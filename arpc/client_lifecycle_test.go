//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package arpc

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/taskpool"
)

// arpcWait is how long a test waits for something that should happen at
// once, generous for slow CI runners under -race.
const arpcWait = 5 * time.Second

// recvArpc waits for a value on ch, or fails the test.
func recvArpc[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(arpcWait):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

// waitArpc polls cond until it holds, or fails the test.
func waitArpc(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(arpcWait)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// hookCoder is a MessageCoder that leaves messages as they are and runs a
// hook, if one is set, on each one, to act at a precise point of a send or a
// receive.
type hookCoder struct {
	encode atomic.Pointer[func(*Client, *Message)]
	decode atomic.Pointer[func(*Client, *Message)]
}

func (h *hookCoder) Encode(c *Client, m *Message) *Message {
	if f := h.encode.Load(); f != nil {
		(*f)(c, m)
	}
	return m
}

func (h *hookCoder) Decode(c *Client, m *Message) *Message {
	if f := h.decode.Load(); f != nil {
		(*f)(c, m)
	}
	return m
}

// TestNewClientDialFailures pins down what NewClient returns when the dialer
// fails at once, when the dial reports failure, and when it reports success
// for a connection that closed before the Client took it; the last stops the
// Client, whose disconnected callback still runs, with no pool to run on.
func TestNewClientDialFailures(t *testing.T) {
	refused := errors.New("refused at once")
	if _, err := NewClient(func(fib.Handler, func(*fib.Connection, error)) error { return refused }, nil); err != refused {
		t.Fatalf("NewClient with a failing dialer = %v", err)
	}
	failed := errors.New("dial failed")
	failing := func(_ fib.Handler, done func(*fib.Connection, error)) error {
		go done(nil, failed)
		return nil
	}
	if _, err := NewClient(failing, nil); err != failed {
		t.Fatalf("NewClient with a failed dial = %v", err)
	}

	handler := NewHandler()
	disconnected := make(chan *Client, 1)
	handler.HandleDisconnected(func(c *Client) { disconnected <- c })
	closedEarly := func(_ fib.Handler, done func(*fib.Connection, error)) error {
		go done(nil, nil)
		return nil
	}
	if _, err := NewClient(closedEarly, handler); err != ErrClientStopped {
		t.Fatalf("NewClient for a connection that closed as it opened = %v", err)
	}
	if c := recvArpc(t, disconnected, "the disconnected callback"); c.CheckState() != ErrClientStopped {
		t.Fatalf("the Client's state is %v", c.CheckState())
	}

	if _, err := NewClientPoolFromDialers(nil, nil); err != ErrClientInvalidPoolDialers {
		t.Fatalf("NewClientPoolFromDialers(nil) = %v", err)
	}
	s := NewServer()
	addr := startServer(t, s)
	good := TCPDialer(startClientEngine(t), "tcp", addr, arpcWait)
	if _, err := NewClientPoolFromDialers([]DialerFunc{good, failing}, nil); err != failed {
		t.Fatalf("NewClientPoolFromDialers with a failing dialer = %v", err)
	}
	waitArpc(t, "the pool's dialed Client to be stopped", func() bool { return s.Accepted() == 1 && s.CurrLoad() == 0 })
}

// TestClientValuesAndRole pins down the values a Client keeps for the
// application, and which side of the connection a Client reports it is.
func TestClientValuesAndRole(t *testing.T) {
	s := NewServer()
	accepted := make(chan *Client, 1)
	s.Handler.HandleConnected(func(c *Client) { accepted <- c })
	c := dial(t, startServer(t, s), nil)
	if !c.IsClient() || c.IsServer() {
		t.Fatal("a dialed Client says it is the server's")
	}
	if sc := recvArpc(t, accepted, "the accepted Client"); sc.IsClient() || !sc.IsServer() {
		t.Fatal("an accepted Client says it was dialed")
	}

	c.Set(nil, 1)
	c.Set("k", nil)
	if _, ok := c.Get("k"); ok {
		t.Fatal("Set stored a nil key or value")
	}
	c.Set("k", "v")
	if v, ok := c.Get("k"); !ok || v != "v" {
		t.Fatalf("Get = %v, %v", v, ok)
	}
	c.Delete("k")
	if _, ok := c.Get("k"); ok {
		t.Fatal("Delete kept the value")
	}
}

// TestPingAndKeepalive pins down that the peer answers a ping with a pong,
// and that Keepalive keeps pinging until the Client stops.
func TestPingAndKeepalive(t *testing.T) {
	s := NewServer()
	pings := make(chan struct{}, 16)
	s.Handler.HandleMessageDone(func(_ *Client, m *Message) {
		if m.Cmd() == CmdPing {
			select {
			case pings <- struct{}{}:
			default:
			}
		}
	})
	handler := NewHandler()
	pongs := make(chan struct{}, 16)
	handler.HandleMessageDone(func(_ *Client, m *Message) {
		if m.Cmd() == CmdPong {
			select {
			case pongs <- struct{}{}:
			default:
			}
		}
	})
	c := dial(t, startServer(t, s), handler)
	c.Ping()
	recvArpc(t, pings, "the ping")
	recvArpc(t, pongs, "the pong")

	c.Keepalive(10 * time.Millisecond)
	for range 2 {
		recvArpc(t, pings, "a keepalive ping")
	}
	c.Stop()
	// Pinging a stopped Client sends nothing, and its Keepalive ends at the
	// next tick.
	c.Ping()
	c.Keepalive(0)
}

// TestPushMsg pins down that PushMsg sends a Message the application built,
// through the coders, which see its values, and that pushing on a stopped
// Client fails and still hands the Message back to OnMessageDone.
func TestPushMsg(t *testing.T) {
	s := NewServer()
	got := make(chan string, 1)
	s.Handler.Handle("/push", func(ctx *Context) { got <- string(ctx.Body()) })
	coder := &hookCoder{}
	traced := make(chan any, 1)
	onEncode := func(_ *Client, m *Message) {
		if v, ok := m.Get("trace"); ok {
			traced <- v
		}
	}
	coder.encode.Store(&onEncode)
	handler := NewHandler()
	handler.UseCoder(coder)
	done := make(chan *Message, 2)
	handler.HandleMessageDone(func(_ *Client, m *Message) {
		if m.Cmd() == CmdNotify {
			done <- m
		}
	})
	c := dial(t, startServer(t, s), handler)

	msg := c.NewMessage(CmdNotify, "/push", "pushed", map[any]any{"trace": "t1"})
	if err := c.PushMsg(msg, TimeForever); err != nil {
		t.Fatal(err)
	}
	if v := recvArpc(t, traced, "the coder"); v != "t1" {
		t.Fatalf("the coder saw trace %v", v)
	}
	if body := recvArpc(t, got, "the pushed notify"); body != "pushed" {
		t.Fatalf("the server got %q", body)
	}
	recvArpc(t, done, "the sent Message's done callback")

	c.Stop()
	late := c.NewMessage(CmdNotify, "/push", "late")
	if err := c.PushMsg(late, TimeZero); err != ErrClientStopped {
		t.Fatalf("PushMsg on a stopped Client = %v", err)
	}
	if m := recvArpc(t, done, "the unsent Message's done callback"); m != late {
		t.Fatal("OnMessageDone got another Message")
	}
}

// TestCallArgumentErrors pins down the errors the calls return for what they
// are given, before or instead of sending anything, and once the Client has
// stopped.
func TestCallArgumentErrors(t *testing.T) {
	s := NewServer()
	s.Handler.Handle("/echo", func(ctx *Context) { ctx.Write(ctx.Body()) })
	s.Handler.Handle("/never", func(*Context) {})
	notified := make(chan string, 1)
	s.Handler.Handle("/notify", func(ctx *Context) { notified <- string(ctx.Body()) })
	c := dial(t, startServer(t, s), nil)
	unencodable := make(chan int)
	noop := func(*Context, error) {}

	if err := c.Call("/echo", nil, nil, -time.Second); err != ErrClientInvalidTimeoutLessThanZero {
		t.Fatalf("Call with a negative timeout = %v", err)
	}
	if err := c.CallAsync("/echo", nil, noop, -time.Second); err != ErrClientInvalidTimeoutLessThanZero {
		t.Fatalf("CallAsync with a negative timeout = %v", err)
	}
	if err := c.Notify("/notify", nil, -time.Second); err != ErrClientInvalidTimeoutLessThanZero {
		t.Fatalf("Notify with a negative timeout = %v", err)
	}
	if err := c.Call("/echo", unencodable, nil, time.Second); err == nil {
		t.Fatal("Call with an unencodable request succeeded")
	}
	if err := c.CallAsync("/echo", unencodable, noop, time.Second); err == nil {
		t.Fatal("CallAsync with an unencodable request succeeded")
	}
	if err := c.Notify("/notify", unencodable, time.Second); err == nil {
		t.Fatal("Notify with an unencodable payload succeeded")
	}

	var rsp string
	if err := c.CallWith(context.Background(), "/echo", "with", &rsp); err != nil || rsp != "with" {
		t.Fatalf("CallWith = %q, %v", rsp, err)
	}
	if err := c.NotifyWith(context.Background(), "/notify", "with"); err != nil {
		t.Fatal(err)
	}
	if body := recvArpc(t, notified, "NotifyWith's notify"); body != "with" {
		t.Fatalf("NotifyWith sent %q", body)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := c.CallContext(ctx, "/never", nil, nil); err != ErrClientTimeout {
		t.Fatalf("CallContext past its deadline = %v", err)
	}

	c.Stop()
	if err := c.CallContext(context.Background(), "/echo", nil, nil); err != ErrClientStopped {
		t.Fatalf("CallContext on a stopped Client = %v", err)
	}
	if err := c.CallAsync("/echo", nil, noop, time.Second); err != ErrClientStopped {
		t.Fatalf("CallAsync on a stopped Client = %v", err)
	}
	if err := c.Notify("/notify", nil, time.Second); err != ErrClientStopped {
		t.Fatalf("Notify on a stopped Client = %v", err)
	}
	if err := c.NotifyContext(context.Background(), "/notify", nil); err != ErrClientStopped {
		t.Fatalf("NotifyContext on a stopped Client = %v", err)
	}
}

// stoppingCodec stops its Client as it encodes a request, which is a Client
// stopping between a call's first check of its state and its registration.
type stoppingCodec struct{ c atomic.Pointer[Client] }

func (sc *stoppingCodec) Marshal(v any) ([]byte, error) {
	if c := sc.c.Load(); c != nil {
		c.Stop()
	}
	return JSONCodec{}.Marshal(v)
}

func (sc *stoppingCodec) Unmarshal(data []byte, v any) error { return JSONCodec{}.Unmarshal(data, v) }

// TestCallStoppedWhileEncoding pins down that a call whose Client stops after
// the call has checked its state, while the request is being encoded, fails
// with ErrClientStopped rather than waiting for a response that cannot come.
func TestCallStoppedWhileEncoding(t *testing.T) {
	s := NewServer()
	s.Handler.Handle("/echo", func(ctx *Context) { ctx.Write(ctx.Body()) })
	addr := startServer(t, s)
	req := &echoReq{Message: "m"}

	codec := &stoppingCodec{}
	c := dial(t, addr, nil)
	c.Codec = codec
	codec.c.Store(c)
	if err := c.Call("/echo", req, nil, time.Second); err != ErrClientStopped {
		t.Fatalf("Call = %v, want ErrClientStopped", err)
	}

	codec = &stoppingCodec{}
	c = dial(t, addr, nil)
	c.Codec = codec
	codec.c.Store(c)
	if err := c.CallAsync("/echo", req, func(*Context, error) { t.Error("the handler of a failed CallAsync ran") }, time.Second); err != ErrClientStopped {
		t.Fatalf("CallAsync = %v, want ErrClientStopped", err)
	}
}

// TestCallConnectionLostWhileSending pins down the calls whose connection
// breaks as their request is being written. Call returns the write's error.
// CallAsync returns it too while the call is still its own; once the lost
// connection has failed the call, its handler reports the loss instead and
// CallAsync returns nil, so the outcome is reported exactly once.
func TestCallConnectionLostWhileSending(t *testing.T) {
	s := NewServer()
	s.Handler.Handle("/echo", func(ctx *Context) { ctx.Write(ctx.Body()) })
	coder := &hookCoder{}
	handler := NewHandler()
	handler.UseCoder(coder)
	handler.SetReconnectInterval(10 * time.Millisecond)
	connected := make(chan struct{}, 8)
	handler.HandleConnected(func(*Client) { connected <- struct{}{} })
	c := dial(t, startServer(t, s), handler)
	recvArpc(t, connected, "the first connection")
	setEncode := func(f func(*Client, *Message)) { coder.encode.Store(&f) }
	once := func(f func(*Client, *Message)) {
		var fired atomic.Bool
		setEncode(func(c *Client, m *Message) {
			if fired.CompareAndSwap(false, true) {
				f(c, m)
			}
		})
	}

	once(func(c *Client, _ *Message) { c.Conn().Close() })
	if err := c.Call("/echo", "x", nil, arpcWait); err == nil || err == ErrClientTimeout {
		t.Fatalf("Call whose connection closed as it wrote = %v", err)
	}
	recvArpc(t, connected, "the reconnection after Call")

	// The connection's loss fails the call before its write does.
	failed := make(chan error, 1)
	once(func(c *Client, _ *Message) {
		c.Conn().Close()
		select {
		case err := <-failed:
			failed <- err
		case <-time.After(arpcWait):
		}
	})
	err := c.CallAsync("/echo", "x", func(_ *Context, err error) { failed <- err }, arpcWait)
	if err != nil {
		t.Fatalf("CallAsync failed by the connection's loss = %v, want nil", err)
	}
	if err := recvArpc(t, failed, "the async handler"); err != ErrClientReconnecting {
		t.Fatalf("the async handler got %v", err)
	}
	recvArpc(t, connected, "the reconnection after the lost CallAsync")

	// The write fails before the connection's loss reaches the call.
	results := make(chan error, 1)
	once(func(c *Client, _ *Message) { _ = c.Conn().CloseWrite() })
	err = c.CallAsync("/echo", "x", func(_ *Context, err error) { results <- err }, arpcWait)
	if err == nil {
		if herr := recvArpc(t, results, "the async handler"); herr == nil {
			t.Fatal("a CallAsync whose write failed succeeded")
		}
	}
	setEncode(func(*Client, *Message) {})
	recvArpc(t, connected, "the reconnection after the failed write")
	var rsp string
	if err := c.Call("/echo", "after", &rsp, arpcWait); err != nil || rsp != "after" {
		t.Fatalf("Call after reconnecting = %q, %v", rsp, err)
	}
}

// TestReconnectStoppedWhileDialing pins down a Client stopped while it
// redials: the connection the dial then opens is refused and closed, its
// close is ignored, and the attempt is not reported as a reconnection.
func TestReconnectStoppedWhileDialing(t *testing.T) {
	s := NewServer()
	s.Handler.Handle("/kick", func(ctx *Context) { ctx.Client.Stop() })
	addr := startServer(t, s)
	engine := startClientEngine(t)
	var client atomic.Pointer[Client]
	var dials atomic.Int32
	dialer := func(h fib.Handler, done func(*fib.Connection, error)) error {
		if dials.Add(1) > 1 {
			client.Load().Stop()
		}
		return engine.DialWithHandler("tcp", addr, arpcWait, h, done)
	}
	handler := NewHandler()
	reconnects := make(chan *ReconnectInfo, 1)
	handler.HandleReconnect(func(_ *Client, info *ReconnectInfo) { reconnects <- info })
	disconnected := make(chan struct{}, 2)
	handler.HandleDisconnected(func(*Client) { disconnected <- struct{}{} })
	c, err := NewClient(dialer, handler)
	if err != nil {
		t.Fatal(err)
	}
	client.Store(c)
	t.Cleanup(c.Stop)
	// The redial stops the Client at once, so the Call may see either.
	if err := c.Call("/kick", nil, nil, arpcWait); err != ErrClientReconnecting && err != ErrClientStopped {
		t.Fatalf("Call that loses the connection = %v", err)
	}
	recvArpc(t, disconnected, "the disconnected callback")
	waitArpc(t, "the refused connection to close", func() bool { return s.Accepted() == 2 && s.CurrLoad() == 0 })
	if err := c.CheckState(); err != ErrClientStopped {
		t.Fatalf("state %v", err)
	}
	select {
	case info := <-reconnects:
		t.Fatalf("a dial refused for a stopped Client was reported: %+v", info)
	default:
	}
}

// TestReconnectStoppedBetweenAttempts pins down that a Client stopped after a
// failed attempt to reconnect makes no further attempt, whether it stops in
// the reconnect callback or while it waits for the next attempt.
func TestReconnectStoppedBetweenAttempts(t *testing.T) {
	for _, inCallback := range []bool{false, true} {
		t.Run(map[bool]string{false: "waiting", true: "callback"}[inCallback], func(t *testing.T) {
			s := NewServer()
			s.Handler.Handle("/kick", func(ctx *Context) { ctx.Client.Stop() })
			addr := startServer(t, s)
			engine := startClientEngine(t)
			var dials atomic.Int32
			dialer := func(h fib.Handler, done func(*fib.Connection, error)) error {
				if dials.Add(1) > 1 {
					return errors.New("refused")
				}
				return engine.DialWithHandler("tcp", addr, arpcWait, h, done)
			}
			handler := NewHandler()
			handler.SetReconnectInterval(50 * time.Millisecond)
			failedAttempt := make(chan struct{}, 4)
			handler.HandleReconnect(func(c *Client, info *ReconnectInfo) {
				if info.Success || info.Err == nil {
					t.Errorf("reconnect %+v", info)
				}
				if inCallback {
					c.Stop()
				}
				failedAttempt <- struct{}{}
			})
			disconnected := make(chan struct{}, 2)
			handler.HandleDisconnected(func(*Client) { disconnected <- struct{}{} })
			c, err := NewClient(dialer, handler)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(c.Stop)
			if err := c.Call("/kick", nil, nil, arpcWait); err != ErrClientReconnecting && err != ErrClientStopped {
				t.Fatalf("Call that loses the connection = %v", err)
			}
			recvArpc(t, failedAttempt, "the failed attempt")
			c.Stop()
			recvArpc(t, disconnected, "the disconnected callback")
			// Give a next attempt, which would be due after the interval,
			// the time to show up.
			time.Sleep(200 * time.Millisecond)
			if n := dials.Load(); n != 2 {
				t.Fatalf("%d dials, want the first and one attempt", n)
			}
		})
	}
}

// TestPendingFailOnStop pins down what a Client's own Stop fails: its
// asynchronous calls get ErrClientStopped, and its Streams, the ones it
// opened and the ones the peer opened, stop receiving.
func TestPendingFailOnStop(t *testing.T) {
	s := NewServer()
	release := make(chan struct{})
	defer close(release)
	s.Handler.Handle("/block", func(ctx *Context) { <-release })
	s.Handler.HandleStream("/client-opened", func(st *Stream) { _ = st.Recv(nil) })
	s.Handler.HandleConnected(func(c *Client) { _ = c.NewStream("/server-opened").Send("hi") })
	handler := NewHandler()
	remote := make(chan *Stream, 1)
	handler.HandleStream("/server-opened", func(st *Stream) { remote <- st }, false)
	c := dial(t, startServer(t, s), handler)
	remoteStream := recvArpc(t, remote, "the stream the server opened")
	var hi string
	if err := remoteStream.Recv(&hi); err != nil || hi != "hi" {
		t.Fatalf("Recv = %q, %v", hi, err)
	}
	local := c.NewStream("/client-opened")
	asyncErr := make(chan error, 1)
	if err := c.CallAsync("/block", nil, func(_ *Context, err error) { asyncErr <- err }, arpcWait); err != nil {
		t.Fatal(err)
	}

	c.Stop()
	if err := recvArpc(t, asyncErr, "the async handler"); err != ErrClientStopped {
		t.Fatalf("the pending async call got %v", err)
	}
	for name, st := range map[string]*Stream{"local": local, "remote": remoteStream} {
		if err := st.Recv(nil); err != io.EOF && err != ErrClientStopped {
			t.Fatalf("Recv on the %s stream of a stopped Client = %v", name, err)
		}
	}
	// Stop closed the stream's sending half, and a send says why.
	if err := local.Send("late"); err != ErrClientStopped {
		t.Fatalf("Send on a stream of a stopped Client = %v, want ErrClientStopped", err)
	}
}

// TestCallStoppedWhileWaiting pins down that a Call waiting for its response
// returns ErrClientStopped as soon as its Client stops.
func TestCallStoppedWhileWaiting(t *testing.T) {
	s := NewServer()
	release := make(chan struct{})
	defer close(release)
	entered := make(chan struct{}, 1)
	s.Handler.Handle("/block", func(ctx *Context) {
		entered <- struct{}{}
		<-release
	})
	c := dial(t, startServer(t, s), nil)
	result := make(chan error, 1)
	go func() { result <- c.Call("/block", nil, nil, arpcWait) }()
	recvArpc(t, entered, "the request")
	c.Stop()
	if err := recvArpc(t, result, "the Call"); err != ErrClientStopped {
		t.Fatalf("Call = %v, want ErrClientStopped", err)
	}
}

// TestLateResponses pins down the responses that arrive once their caller has
// given up: to a timed-out CallAsync, and to a CallContext whose context ends
// just as its response arrives. Each is a session miss.
func TestLateResponses(t *testing.T) {
	s := NewServer()
	release := make(chan struct{})
	s.Handler.Handle("/slow", func(ctx *Context) {
		<-release
		ctx.Write("late")
	})
	s.Handler.Handle("/echo", func(ctx *Context) { ctx.Write(ctx.Body()) })
	coder := &hookCoder{}
	handler := NewHandler()
	handler.UseCoder(coder)
	missed := make(chan string, 2)
	handler.HandleSessionMiss(func(_ *Client, m *Message) { missed <- string(m.Data()) })
	c := dial(t, startServer(t, s), handler)

	timedOut := make(chan error, 1)
	if err := c.CallAsync("/slow", nil, func(_ *Context, err error) { timedOut <- err }, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := recvArpc(t, timedOut, "the async timeout"); err != ErrTimeout {
		t.Fatalf("CallAsync = %v, want ErrTimeout", err)
	}
	close(release)
	if got := recvArpc(t, missed, "the late async response"); got != "late" {
		t.Fatalf("session miss got %q", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	onDecode := func(_ *Client, m *Message) {
		if m.Cmd() == CmdResponse {
			cancel()
		}
	}
	coder.decode.Store(&onDecode)
	// A response quick enough to beat the caller to its wait leaves the
	// caller to choose between it and the ended context.
	err := c.CallContext(ctx, "/echo", "raced", nil)
	if err == nil {
		return
	}
	if err != ErrClientTimeout {
		t.Fatalf("CallContext whose context ended as its response arrived = %v", err)
	}
	if got := recvArpc(t, missed, "the raced response"); got != "raced" {
		t.Fatalf("session miss got %q", got)
	}
}

// rejectingPool turns every task away, as a full pool does.
type rejectingPool struct{ rejected atomic.Int32 }

func (p *rejectingPool) GoTask(taskpool.Task) bool { p.rejected.Add(1); return false }
func (p *rejectingPool) GoTasks(tasks []taskpool.Task) int {
	p.rejected.Add(int32(len(tasks)))
	return 0
}

// TestTaskPoolRejects pins down that what a full pool turns away still runs:
// an asynchronous handler where its connection is read, and a callback on a
// goroutine of its own.
func TestTaskPoolRejects(t *testing.T) {
	s := NewServer()
	pool := &rejectingPool{}
	s.Handler.SetTaskPool(pool)
	s.Handler.Handle("/echo", func(ctx *Context) { ctx.Write(ctx.Body()) })
	connected := make(chan struct{}, 1)
	s.Handler.HandleConnected(func(*Client) { connected <- struct{}{} })
	c := dial(t, startServer(t, s), nil)
	recvArpc(t, connected, "the connected callback")
	var rsp string
	if err := c.Call("/echo", "inline", &rsp, arpcWait); err != nil || rsp != "inline" {
		t.Fatalf("Call = %q, %v", rsp, err)
	}
	if n := pool.rejected.Load(); n < 2 {
		t.Fatalf("the pool turned away %d tasks, want the callback and the handler", n)
	}
}

// TestCallbackPanicRecovered pins down that a panicking callback, here an
// asynchronous call's handler, is logged and leaves its Client working.
func TestCallbackPanicRecovered(t *testing.T) {
	s := NewServer()
	s.Handler.Handle("/echo", func(ctx *Context) { ctx.Write(ctx.Body()) })
	c := dial(t, startServer(t, s), nil)
	panicked := make(chan struct{})
	if err := c.CallAsync("/echo", nil, func(*Context, error) {
		close(panicked)
		panic("callback bug")
	}, arpcWait); err != nil {
		t.Fatal(err)
	}
	recvArpc(t, panicked, "the async handler")
	var rsp string
	if err := c.Call("/echo", "alive", &rsp, arpcWait); err != nil || rsp != "alive" {
		t.Fatalf("Call after a panicking callback = %q, %v", rsp, err)
	}
}

// TestClientPoolAccessors pins down Get, which wraps around, Handler, which
// the Clients share, and Next once no Client is connected.
func TestClientPoolAccessors(t *testing.T) {
	s := NewServer()
	addr := startServer(t, s)
	handler := NewHandler()
	pool, err := NewClientPool(TCPDialer(startClientEngine(t), "tcp", addr, arpcWait), 2, handler)
	if err != nil {
		t.Fatal(err)
	}
	if pool.Get(0) != pool.Get(2) || pool.Get(0) == pool.Get(1) || pool.Handler() != handler {
		t.Fatal("Get or Handler")
	}
	pool.Stop()
	waitArpc(t, "the pool's connections to close", func() bool { return pool.Get(0).Conn() == nil && pool.Get(1).Conn() == nil })
	if c := pool.Next(); c != pool.Get(0) && c != pool.Get(1) {
		t.Fatal("Next with no Client connected returned none of them")
	}
}

// TestClientStateWithoutConnection pins down the errors of a Client that has
// no connection, before and after it stops, and that what follows stopping
// runs once however many ways the Client is ended.
func TestClientStateWithoutConnection(t *testing.T) {
	h := NewHandler()
	var disconnects atomic.Int32
	done := make(chan struct{}, 2)
	h.HandleDisconnected(func(*Client) {
		disconnects.Add(1)
		done <- struct{}{}
	})
	c := newClient(DefaultCodec, h)
	if c.CheckState() != ErrClientReconnecting || c.brokenErr() != ErrClientReconnecting || c.remoteAddr() != "" {
		t.Fatalf("a Client with no connection: state %v, broken %v", c.CheckState(), c.brokenErr())
	}
	c.Stop()
	c.terminate()
	recvArpc(t, done, "the disconnected callback")
	if c.CheckState() != ErrClientStopped || c.brokenErr() != ErrClientStopped {
		t.Fatalf("a stopped Client: state %v, broken %v", c.CheckState(), c.brokenErr())
	}
	select {
	case <-done:
		t.Fatal("the disconnected callback ran twice")
	case <-time.After(50 * time.Millisecond):
	}
}

// TestStopTakesEffectAtOnce pins down that a Client has stopped as soon as
// Stop returns: its connection closes on the event loop afterwards, but
// CheckState and the calls already report ErrClientStopped rather than
// nothing, or the error of writing to a connection that is closing.
func TestStopTakesEffectAtOnce(t *testing.T) {
	s := NewServer()
	s.Handler.Handle("/echo", func(ctx *Context) { ctx.Write(ctx.Body()) })
	addr := startServer(t, s)
	for range 20 {
		c := dial(t, addr, nil)
		c.Stop()
		if err := c.CheckState(); err != ErrClientStopped {
			t.Fatalf("CheckState right after Stop = %v", err)
		}
		if err := c.Notify("/echo", "x", time.Second); err != ErrClientStopped {
			t.Fatalf("Notify right after Stop = %v", err)
		}
		if err := c.NewStream("/s").Send("x"); err != ErrClientStopped {
			t.Fatalf("Stream.Send right after Stop = %v", err)
		}
	}
}

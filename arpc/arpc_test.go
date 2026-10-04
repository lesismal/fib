//go:build linux || darwin || windows

package arpc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/taskpool"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

func TestMain(m *testing.M) {
	SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

// startServer serves s on a loopback port and returns its address.
func startServer(t *testing.T, s *Server) string {
	t.Helper()
	return startHandler(t, s)
}

// startHandler runs handler on an engine listening on a loopback port.
func startHandler(t *testing.T, handler fib.Handler) string {
	t.Helper()
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	engine, err := fib.Bind(config, handler)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := engine.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		<-runDone
		_ = engine.Close()
	})
	return addr.String()
}

// startClientEngine runs an engine with no listener, for dialing.
func startClientEngine(t *testing.T) *fib.Engine {
	t.Helper()
	engine, err := fib.NewEngine(fib.DefaultConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		<-runDone
		_ = engine.Close()
	})
	return engine
}

func dial(t *testing.T, addr string, handler *Handler) *Client {
	t.Helper()
	c, err := Dial(startClientEngine(t), "tcp", addr, 3*time.Second, handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Stop)
	return c
}

type echoReq struct{ Message string }
type echoRsp struct{ Message string }

func TestCall(t *testing.T) {
	s := NewServer()
	s.Handler.Handle("/echo", func(ctx *Context) {
		var str string
		if err := ctx.Bind(&str); err != nil {
			ctx.Error(err)
			return
		}
		ctx.Write(str)
	})
	s.Handler.Handle("/echo/sync", func(ctx *Context) { ctx.Write(ctx.Body()) }, false)
	s.Handler.Handle("/echo/struct", func(ctx *Context) {
		var req echoReq
		if err := ctx.Bind(&req); err != nil {
			ctx.Error(err)
			return
		}
		ctx.Write(&echoRsp{Message: "re: " + req.Message})
	})
	s.Handler.Handle("/fail", func(ctx *Context) { ctx.Error(errors.New("boom")) })
	c := dial(t, startServer(t, s), nil)

	var rsp string
	if err := c.Call("/echo", "hello", &rsp, time.Second); err != nil || rsp != "hello" {
		t.Fatalf("Call /echo = %q, %v", rsp, err)
	}
	var raw []byte
	if err := c.Call("/echo/sync", []byte("bytes"), &raw, time.Second); err != nil || string(raw) != "bytes" {
		t.Fatalf("Call /echo/sync = %q, %v", raw, err)
	}
	var out echoRsp
	if err := c.Call("/echo/struct", &echoReq{Message: "hi"}, &out, time.Second); err != nil || out.Message != "re: hi" {
		t.Fatalf("Call /echo/struct = %+v, %v", out, err)
	}
	if err := c.Call("/fail", nil, nil, time.Second); err == nil || err.Error() != "boom" {
		t.Fatalf("Call /fail = %v, want boom", err)
	}
	if err := c.Call("/missing", nil, nil, time.Second); err == nil || err.Error() != ErrMethodNotFound.Error() {
		t.Fatalf("Call /missing = %v, want %v", err, ErrMethodNotFound)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.CallContext(ctx, "/echo", "ctx", &rsp); err != nil || rsp != "ctx" {
		t.Fatalf("CallContext = %q, %v", rsp, err)
	}
	if err := c.Call("", nil, nil, time.Second); err == nil {
		t.Fatal("Call with an empty method succeeded")
	}
	if err := c.Call("/echo", nil, nil, 0); err != ErrClientInvalidTimeoutZero {
		t.Fatalf("Call with no timeout = %v", err)
	}
}

func TestCallConcurrent(t *testing.T) {
	for _, pool := range []bool{false, true} {
		t.Run(fmt.Sprintf("pool=%v", pool), func(t *testing.T) {
			s := NewServer()
			if pool {
				s.Handler.EnablePool(true)
			}
			s.Handler.Handle("/echo", func(ctx *Context) { ctx.Write(ctx.Body()) })
			s.Handler.Handle("/echo/sync", func(ctx *Context) { ctx.Write(ctx.Body()) }, false)
			handler := NewHandler()
			if pool {
				handler.EnablePool(true)
			}
			c := dial(t, startServer(t, s), handler)
			var wg sync.WaitGroup
			for g := range 16 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range 200 {
						method := "/echo"
						if i%2 == 1 {
							method = "/echo/sync"
						}
						want := fmt.Sprintf("%d-%d-%s", g, i, strings.Repeat("x", i*37))
						var got string
						if err := c.Call(method, want, &got, 5*time.Second); err != nil || got != want {
							t.Errorf("Call %s = %d bytes, %v", method, len(got), err)
							return
						}
					}
				}()
			}
			wg.Wait()
		})
	}
}

func TestCallTimeoutAndSessionMiss(t *testing.T) {
	s := NewServer()
	release := make(chan struct{})
	s.Handler.Handle("/slow", func(ctx *Context) {
		<-release
		ctx.Write("late")
	})
	handler := NewHandler()
	missed := make(chan string, 1)
	handler.HandleSessionMiss(func(c *Client, m *Message) { missed <- string(m.Data()) })
	c := dial(t, startServer(t, s), handler)
	if err := c.Call("/slow", nil, nil, 50*time.Millisecond); err != ErrClientTimeout {
		t.Fatalf("Call = %v, want ErrClientTimeout", err)
	}
	close(release)
	select {
	case got := <-missed:
		if got != "late" {
			t.Fatalf("session miss got %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no session miss for the late response")
	}
}

func TestCallAsync(t *testing.T) {
	s := NewServer()
	s.Handler.Handle("/echo", func(ctx *Context) { ctx.Write(ctx.Body()) })
	s.Handler.Handle("/never", func(ctx *Context) {})
	c := dial(t, startServer(t, s), nil)

	type result struct {
		body string
		err  error
	}
	results := make(chan result, 2)
	err := c.CallAsync("/echo", "async", func(ctx *Context, err error) {
		var body string
		if err == nil {
			err = ctx.Bind(&body)
		}
		results <- result{body, err}
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if r := <-results; r.err != nil || r.body != "async" {
		t.Fatalf("CallAsync = %+v", r)
	}
	if err := c.CallAsync("/never", nil, func(ctx *Context, err error) { results <- result{err: err} }, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if r := <-results; r.err != ErrTimeout {
		t.Fatalf("CallAsync with no response = %v, want ErrTimeout", r.err)
	}
	if err := c.CallAsync("/echo", nil, nil, time.Second); err != ErrClientInvalidAsyncHandler {
		t.Fatalf("CallAsync with no handler = %v", err)
	}
}

func TestNotify(t *testing.T) {
	s := NewServer()
	got := make(chan string, 2)
	s.Handler.Handle("/notify", func(ctx *Context) {
		got <- string(ctx.Body())
		if err := ctx.Write("ignored"); err != ErrContextResponseToNotify {
			t.Errorf("responding to a notify = %v", err)
		}
	})
	c := dial(t, startServer(t, s), nil)
	if err := c.Notify("/notify", "one", time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.NotifyContext(context.Background(), "/notify", "two"); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for range 2 {
		select {
		case s := <-got:
			seen[s] = true
		case <-time.After(3 * time.Second):
			t.Fatal("notify not received")
		}
	}
	if !seen["one"] || !seen["two"] {
		t.Fatalf("received %v", seen)
	}
}

func TestMiddleware(t *testing.T) {
	s := NewServer()
	var mu sync.Mutex
	var trace []string
	record := func(s string) {
		mu.Lock()
		trace = append(trace, s)
		mu.Unlock()
	}
	s.Handler.Use(func(ctx *Context) {
		record("before")
		ctx.Next()
		record("after-next")
	})
	s.Handler.Use(func(ctx *Context) {
		if string(ctx.Body()) == "deny" {
			ctx.Error("denied")
			ctx.Abort()
		}
	})
	s.Handler.Handle("/m", func(ctx *Context) {
		record("handler")
		ctx.Write("ok")
	}, false)
	s.Handler.Use(func(ctx *Context) { record("late") })
	c := dial(t, startServer(t, s), nil)

	var rsp string
	if err := c.Call("/m", "allow", &rsp, time.Second); err != nil || rsp != "ok" {
		t.Fatalf("Call = %q, %v", rsp, err)
	}
	mu.Lock()
	got := strings.Join(trace, ",")
	trace = nil
	mu.Unlock()
	if got != "before,handler,late,after-next" {
		t.Fatalf("trace %s", got)
	}
	if err := c.Call("/m", "deny", &rsp, time.Second); err == nil || err.Error() != "denied" {
		t.Fatalf("Call denied = %v", err)
	}
	mu.Lock()
	got = strings.Join(trace, ",")
	mu.Unlock()
	if got != "before,after-next" {
		t.Fatalf("aborted trace %s", got)
	}
}

type service struct{}

func (service) Hello(ctx context.Context, req *echoReq, rsp *echoRsp) {
	rsp.Message = "hello, " + req.Message
}

func (service) Say(ctx context.Context, req *echoReq, rsp *echoRsp) { rsp.Message = req.Message }

func (s service) SayBinding(ctx *Context) {
	var req echoReq
	var rsp echoRsp
	if err := ctx.Bind(&req); err != nil {
		ctx.Error(err)
		return
	}
	s.Say(ctx, &req, &rsp)
	rsp.Message += " (binding)"
	ctx.Write(&rsp)
}

func (service) Ping(ctx *Context) { ctx.Write("pong") }

func TestRegister(t *testing.T) {
	s := NewServer()
	if err := s.Handler.Register("Svc", service{}); err != nil {
		t.Fatal(err)
	}
	c := dial(t, startServer(t, s), nil)
	var rsp echoRsp
	if err := c.Call("Svc.Hello", &echoReq{Message: "fib"}, &rsp, time.Second); err != nil || rsp.Message != "hello, fib" {
		t.Fatalf("Svc.Hello = %+v, %v", rsp, err)
	}
	if err := c.Call("Svc.Say", &echoReq{Message: "x"}, &rsp, time.Second); err != nil || rsp.Message != "x (binding)" {
		t.Fatalf("Svc.Say = %+v, %v", rsp, err)
	}
	var pong string
	if err := c.Call("Svc.Ping", nil, &pong, time.Second); err != nil || pong != "pong" {
		t.Fatalf("Svc.Ping = %q, %v", pong, err)
	}
	if err := c.Call("Svc.SayBinding", nil, nil, time.Second); err == nil {
		t.Fatal("the Binding method was registered on its own")
	}
}

// TestTwoWay has the server call the client that called it, from a handler on
// the pool.
func TestTwoWay(t *testing.T) {
	s := NewServer()
	s.Handler.Handle("/ask", func(ctx *Context) {
		var answer string
		if err := ctx.Client.Call("/client/answer", ctx.Body(), &answer, time.Second); err != nil {
			ctx.Error(err)
			return
		}
		ctx.Write("client said " + answer)
	})
	handler := NewHandler()
	handler.Handle("/client/answer", func(ctx *Context) { ctx.Write(strings.ToUpper(string(ctx.Body()))) })
	c := dial(t, startServer(t, s), handler)
	var rsp string
	if err := c.Call("/ask", "yes", &rsp, 2*time.Second); err != nil || rsp != "client said YES" {
		t.Fatalf("Call = %q, %v", rsp, err)
	}
}

func TestBroadcastAndCallbacks(t *testing.T) {
	s := NewServer()
	var connected, disconnected atomic.Int32
	joined := make(chan *Client, 3)
	s.Handler.HandleConnected(func(c *Client) {
		connected.Add(1)
		joined <- c
	})
	left := make(chan struct{}, 3)
	s.Handler.HandleDisconnected(func(c *Client) {
		disconnected.Add(1)
		left <- struct{}{}
	})
	addr := startServer(t, s)
	got := make(chan string, 8)
	var clients []*Client
	for i := range 3 {
		handler := NewHandler()
		handler.Handle("/news", func(ctx *Context) { got <- fmt.Sprintf("%d:%s", i, ctx.Body()) })
		clients = append(clients, dial(t, addr, handler))
	}
	for range 3 {
		select {
		case <-joined:
		case <-time.After(3 * time.Second):
			t.Fatal("connected callback missing")
		}
	}
	if n := s.CurrLoad(); n != 3 {
		t.Fatalf("CurrLoad = %d", n)
	}
	s.Broadcast("/news", "flash")
	seen := map[string]bool{}
	for range 3 {
		select {
		case m := <-got:
			seen[m] = true
		case <-time.After(3 * time.Second):
			t.Fatalf("broadcast reached %v", seen)
		}
	}
	if !seen["0:flash"] || !seen["1:flash"] || !seen["2:flash"] {
		t.Fatalf("broadcast reached %v", seen)
	}
	clients[0].Stop()
	select {
	case <-left:
	case <-time.After(3 * time.Second):
		t.Fatal("disconnected callback missing")
	}
	if err := clients[0].Call("/x", nil, nil, time.Second); err != ErrClientStopped {
		t.Fatalf("Call after Stop = %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for s.CurrLoad() != 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := s.CurrLoad(); n != 2 {
		t.Fatalf("CurrLoad after a client left = %d", n)
	}
	s.BroadcastWithFilter("/news", "only", func(c *Client) bool { return true })
	for range 2 {
		select {
		case <-got:
		case <-time.After(3 * time.Second):
			t.Fatal("filtered broadcast missing")
		}
	}
	n := 0
	s.ForEach(func(*Client) { n++ })
	if n != 2 || s.Accepted() != 3 {
		t.Fatalf("ForEach saw %d, Accepted %d", n, s.Accepted())
	}
}

func TestStream(t *testing.T) {
	s := NewServer()
	s.Handler.HandleStream("/upper", func(st *Stream) {
		defer st.CloseSend()
		for {
			var msg string
			if err := st.Recv(&msg); err != nil {
				if err != io.EOF {
					t.Errorf("server Recv: %v", err)
				}
				return
			}
			if err := st.Send(strings.ToUpper(msg)); err != nil {
				t.Errorf("server Send: %v", err)
				return
			}
		}
	})
	c := dial(t, startServer(t, s), nil)
	st := c.NewStream("/upper")
	const n = 50
	go func() {
		for i := range n {
			if err := st.Send(fmt.Sprintf("m%d", i)); err != nil {
				t.Errorf("client Send: %v", err)
			}
		}
		st.CloseSend()
	}()
	for i := range n {
		var got string
		if err := st.Recv(&got); err != nil || got != fmt.Sprintf("M%d", i) {
			t.Fatalf("Recv %d = %q, %v", i, got, err)
		}
	}
	var rest string
	if err := st.Recv(&rest); err != io.EOF {
		t.Fatalf("Recv after the server closed = %q, %v", rest, err)
	}
	if err := st.Send("late"); err != ErrStreamClosedSend {
		t.Fatalf("Send after CloseSend = %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		left := len(c.streamLocal)
		c.mu.Unlock()
		if left == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the closed stream stayed on the client")
}

// TestAsyncPool checks where handlers run: async ones on the engine's stream
// pool by default, so that one blocking holds up nothing else of its
// connection, sync ones where the connection is read, and async ones on the
// pool SetTaskPool names.
func TestAsyncPool(t *testing.T) {
	s := NewServer()
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	s.Handler.Handle("/block", func(ctx *Context) {
		entered <- struct{}{}
		<-release
		ctx.Write("unblocked")
	})
	s.Handler.Handle("/echo", func(ctx *Context) { ctx.Write(ctx.Body()) }, false)
	pools := make(chan string, 1)
	s.Handler.HandleConnected(func(c *Client) {
		if pool, ok := c.taskPool().(*taskpool.TaskPool); ok {
			pools <- pool.Name()
		} else {
			pools <- fmt.Sprintf("%T", c.taskPool())
		}
	})
	c := dial(t, startServer(t, s), nil)
	if name := <-pools; name != "fib-streams" {
		t.Fatalf("handlers run on %s, want fib-streams", name)
	}
	blocked := make(chan error, 1)
	go func() { blocked <- c.Call("/block", nil, nil, 5*time.Second) }()
	<-entered
	var rsp string
	if err := c.Call("/echo", "through", &rsp, time.Second); err != nil || rsp != "through" {
		close(release)
		t.Fatalf("a blocked async handler held up its connection: %q, %v", rsp, err)
	}
	close(release)
	if err := <-blocked; err != nil {
		t.Fatal(err)
	}

	custom := &countingPool{TaskPool: taskpool.New("arpc-test", 4, 64)}
	t.Cleanup(custom.Stop)
	s2 := NewServer()
	s2.Handler.SetTaskPool(custom)
	s2.Handler.Handle("/async", func(ctx *Context) { ctx.Write("a") })
	s2.Handler.Handle("/sync", func(ctx *Context) { ctx.Write("s") }, false)
	s2.Handler.SetAsyncResponse(false)
	s2.Handler.Handle("/sync2", func(ctx *Context) { ctx.Write("s2") })
	c2 := dial(t, startServer(t, s2), nil)
	for _, m := range []string{"/sync", "/sync2", "/async"} {
		if err := c2.Call(m, nil, nil, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if n := custom.n.Load(); n != 1 {
		t.Fatalf("the custom pool ran %d tasks, want 1", n)
	}

	var executed atomic.Int32
	s3 := NewServer()
	s3.Handler.SetAsyncExecutor(func(f func()) {
		executed.Add(1)
		go f()
	})
	s3.Handler.Handle("/async", func(ctx *Context) { ctx.Write("a") })
	c3 := dial(t, startServer(t, s3), nil)
	if err := c3.Call("/async", nil, nil, time.Second); err != nil {
		t.Fatal(err)
	}
	if executed.Load() != 1 {
		t.Fatalf("the executor ran %d tasks", executed.Load())
	}
}

type countingPool struct {
	*taskpool.TaskPool
	n atomic.Int32
}

func (p *countingPool) GoTask(task taskpool.Task) bool {
	p.n.Add(1)
	return p.TaskPool.GoTask(task)
}

func TestReconnect(t *testing.T) {
	s := NewServer()
	s.Handler.Handle("/echo", func(ctx *Context) { ctx.Write(ctx.Body()) })
	s.Handler.Handle("/kick", func(ctx *Context) { ctx.Client.Stop() })
	handler := NewHandler()
	handler.SetReconnectInterval(10 * time.Millisecond)
	connected := make(chan struct{}, 4)
	handler.HandleConnected(func(*Client) { connected <- struct{}{} })
	reconnected := make(chan *ReconnectInfo, 4)
	handler.HandleReconnect(func(_ *Client, info *ReconnectInfo) { reconnected <- info })
	c := dial(t, startServer(t, s), handler)
	<-connected
	if err := c.Call("/kick", nil, nil, time.Second); err != ErrClientReconnecting {
		t.Fatalf("Call that loses the connection = %v, want ErrClientReconnecting", err)
	}
	select {
	case info := <-reconnected:
		if !info.Success || info.Times != 1 {
			t.Fatalf("reconnect %+v", info)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no reconnect")
	}
	<-connected
	var rsp string
	if err := c.Call("/echo", "again", &rsp, time.Second); err != nil || rsp != "again" {
		t.Fatalf("Call after reconnecting = %q, %v", rsp, err)
	}
}

func TestReconnectGivesUp(t *testing.T) {
	s := NewServer()
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	engine, err := fib.Bind(config, s)
	if err != nil {
		t.Fatal(err)
	}
	addr, _ := engine.LocalAddr()
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()

	handler := NewHandler()
	handler.SetReconnectInterval(10 * time.Millisecond)
	handler.SetMaxReconnectTimes(3)
	var attempts atomic.Int32
	handler.HandleReconnect(func(_ *Client, info *ReconnectInfo) {
		if info.Success {
			t.Error("reconnected to a closed server")
		}
		attempts.Add(1)
	})
	stopped := make(chan struct{})
	handler.HandleDisconnected(func(*Client) { close(stopped) })
	c := dial(t, addr.String(), handler)

	engine.Stop()
	<-runDone
	_ = engine.Close()
	// Windows retries a refused connect for about 2 seconds before failing
	// it, so the three attempts take over 6 there.
	select {
	case <-stopped:
	case <-time.After(20 * time.Second):
		t.Fatal("the client never gave up")
	}
	if n := attempts.Load(); n != 3 {
		t.Fatalf("%d attempts, want 3", n)
	}
	if err := c.CheckState(); err != ErrClientStopped {
		t.Fatalf("state %v", err)
	}
}

type xorCoder struct{ key byte }

func (x xorCoder) apply(m *Message) *Message {
	for i := HeadLen + m.MethodLen(); i < len(m.Buffer); i++ {
		m.Buffer[i] ^= x.key
	}
	return m
}
func (x xorCoder) Encode(_ *Client, m *Message) *Message { return x.apply(m) }
func (x xorCoder) Decode(_ *Client, m *Message) *Message { return x.apply(m) }

func TestCoder(t *testing.T) {
	s := NewServer()
	s.Handler.UseCoder(xorCoder{0x5a})
	seen := make(chan string, 1)
	s.Handler.Handle("/echo", func(ctx *Context) {
		seen <- string(ctx.Body())
		ctx.Write(ctx.Body())
	})
	handler := NewHandler()
	handler.UseCoder(xorCoder{0x5a})
	news := make(chan string, 1)
	handler.Handle("/news", func(ctx *Context) { news <- string(ctx.Body()) })
	c := dial(t, startServer(t, s), handler)
	var rsp string
	if err := c.Call("/echo", "secret", &rsp, time.Second); err != nil || rsp != "secret" || <-seen != "secret" {
		t.Fatalf("Call = %q, %v", rsp, err)
	}
	s.Broadcast("/news", "coded")
	if got := <-news; got != "coded" {
		t.Fatalf("broadcast = %q", got)
	}
}

func TestSingleflight(t *testing.T) {
	s := NewServer()
	var calls atomic.Int32
	release := make(chan struct{})
	s.Handler.Handle("/get", func(ctx *Context) {
		calls.Add(1)
		<-release
		ctx.Write(&echoRsp{Message: "shared " + string(ctx.Body())})
	})
	handler := NewHandler()
	handler.Singleflight("/get", func(req any) string { return req.(string) })
	c := dial(t, startServer(t, s), handler)

	const n = 8
	var wg sync.WaitGroup
	errs := make(chan error, 2*n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var rsp echoRsp
			if err := c.Call("/get", "k", &rsp, 3*time.Second); err != nil || rsp.Message != "shared k" {
				errs <- fmt.Errorf("Call = %+v, %v", rsp, err)
			}
		}()
	}
	asyncDone := make(chan string, n)
	for range n {
		err := c.CallAsync("/get", "k", func(ctx *Context, err error) {
			var rsp echoRsp
			if err == nil {
				err = ctx.Bind(&rsp)
			}
			if err != nil {
				errs <- err
			}
			asyncDone <- rsp.Message
		}, 3*time.Second)
		if err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()
	for range n {
		if got := <-asyncDone; got != "shared k" {
			t.Fatalf("async follower got %q", got)
		}
	}
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("server served %d calls, want one blocking and one async", got)
	}
}

func TestTLS(t *testing.T) {
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer()
	s.Handler.Handle("/echo", func(ctx *Context) { ctx.Write(ctx.Body()) })
	addr := startHandler(t, fibtls.NewServer(serverTLS, s))
	c, err := NewClient(TLSDialer(startClientEngine(t), "tcp", addr, 3*time.Second, clientTLS), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	var rsp string
	if err := c.Call("/echo", "over tls", &rsp, 3*time.Second); err != nil || rsp != "over tls" {
		t.Fatalf("Call = %q, %v", rsp, err)
	}
}

func TestMaxLoad(t *testing.T) {
	s := NewServer()
	s.MaxLoad = 1
	s.Handler.Handle("/echo", func(ctx *Context) { ctx.Write(ctx.Body()) })
	addr := startServer(t, s)
	c := dial(t, addr, nil)
	if err := c.Call("/echo", "first", nil, time.Second); err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("a connection over MaxLoad read %v, want EOF", err)
	}
}

// frame builds a message by hand, as github.com/lesismal/arpc lays it out.
func frame(cmd, flags byte, seq uint64, method, data string) []byte {
	b := make([]byte, HeadLen+len(method)+len(data))
	binary.LittleEndian.PutUint32(b, uint32(len(method)+len(data)))
	b[HeaderIndexCmd] = cmd
	b[HeaderIndexFlag] = flags
	b[HeaderIndexMethodLen] = byte(len(method))
	binary.LittleEndian.PutUint64(b[HeaderIndexSeqBegin:], seq)
	copy(b[HeadLen:], method)
	copy(b[HeadLen+len(method):], data)
	return b
}

func readFrame(t *testing.T, conn net.Conn) (head []byte, method, data string) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	head = make([]byte, HeadLen)
	if _, err := io.ReadFull(conn, head); err != nil {
		t.Fatal(err)
	}
	body := make([]byte, binary.LittleEndian.Uint32(head))
	if _, err := io.ReadFull(conn, body); err != nil {
		t.Fatal(err)
	}
	ml := int(head[HeaderIndexMethodLen])
	return head, string(body[:ml]), string(body[ml:])
}

// TestWire speaks the protocol by hand, a byte at a time, so that every
// header and body arrives split across reads.
func TestWire(t *testing.T) {
	s := NewServer()
	s.Handler.Handle("/echo", func(ctx *Context) { ctx.Write(ctx.Body()) }, false)
	s.Handler.SetMaxBodyLen(1024)
	addr := startServer(t, s)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var stream []byte
	stream = append(stream, frame(CmdPing, 0, 0, "", "")...)
	stream = append(stream, frame(CmdRequest, 0, 7, "/echo", "split")...)
	stream = append(stream, frame(CmdRequest, HeaderFlagMaskAsync, 8, "/echo", "")...)
	for _, b := range stream {
		if _, err := conn.Write([]byte{b}); err != nil {
			t.Fatal(err)
		}
	}
	head, _, _ := readFrame(t, conn)
	if head[HeaderIndexCmd] != CmdPong {
		t.Fatalf("ping answered with cmd %d", head[HeaderIndexCmd])
	}
	head, method, data := readFrame(t, conn)
	if head[HeaderIndexCmd] != CmdResponse || binary.LittleEndian.Uint64(head[HeaderIndexSeqBegin:]) != 7 ||
		method != "/echo" || data != "split" || head[HeaderIndexFlag] != 0 {
		t.Fatalf("response %v %q %q", head, method, data)
	}
	head, _, data = readFrame(t, conn)
	if binary.LittleEndian.Uint64(head[HeaderIndexSeqBegin:]) != 8 || head[HeaderIndexFlag] != HeaderFlagMaskAsync || data != "" {
		t.Fatalf("async response %v %q", head, data)
	}

	if _, err := conn.Write(frame(CmdRequest, 0, 9, "/echo", strings.Repeat("x", 2048))); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("a message over MaxBodyLen was served")
	}
}

func TestHandlerPanic(t *testing.T) {
	s := NewServer()
	s.Handler.Handle("/panic", func(ctx *Context) { panic("handler bug") })
	s.Handler.Handle("/panic/sync", func(ctx *Context) { panic("handler bug") }, false)
	s.Handler.Handle("/echo", func(ctx *Context) { ctx.Write(ctx.Body()) })
	c := dial(t, startServer(t, s), nil)
	for _, m := range []string{"/panic", "/panic/sync"} {
		if err := c.Call(m, nil, nil, 100*time.Millisecond); err != ErrClientTimeout {
			t.Fatalf("Call %s = %v", m, err)
		}
	}
	var rsp string
	if err := c.Call("/echo", "alive", &rsp, time.Second); err != nil || rsp != "alive" {
		t.Fatalf("the connection did not survive a panic: %q, %v", rsp, err)
	}
}

func TestClientPool(t *testing.T) {
	s := NewServer()
	s.Handler.Handle("/echo", func(ctx *Context) { ctx.Write(ctx.Body()) })
	addr := startServer(t, s)
	pool, err := NewClientPool(TCPDialer(startClientEngine(t), "tcp", addr, time.Second), 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Stop()
	if pool.Size() != 3 {
		t.Fatalf("Size = %d", pool.Size())
	}
	for i := range 6 {
		var rsp string
		if err := pool.Next().Call("/echo", "p", &rsp, time.Second); err != nil || rsp != "p" {
			t.Fatalf("call %d = %q, %v", i, rsp, err)
		}
	}
	if s.CurrLoad() != 3 {
		t.Fatalf("CurrLoad = %d", s.CurrLoad())
	}
}

func TestServerRun(t *testing.T) {
	s := NewServer()
	s.Handler.Handle("/echo", func(ctx *Context) { ctx.Write(ctx.Body()) })
	left := make(chan struct{}, 1)
	s.Handler.HandleDisconnected(func(*Client) { left <- struct{}{} })
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	served := make(chan error, 1)
	go func() { served <- s.Run(addr) }()
	var c *Client
	deadline := time.Now().Add(3 * time.Second)
	for {
		c, err = Dial(startClientEngine(t), "tcp", addr, time.Second, nil)
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	if err := c.Call("/echo", "run", nil, time.Second); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	select {
	case <-left:
	case <-time.After(3 * time.Second):
		t.Fatal("no disconnected callback once the server's engine closed")
	}
	if n := s.CurrLoad(); n != 0 {
		t.Fatalf("CurrLoad after Shutdown = %d", n)
	}
}

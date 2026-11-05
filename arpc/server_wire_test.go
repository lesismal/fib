//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package arpc

import (
	"context"
	"encoding/binary"
	"net"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
)

// TestContextInHandlers pins down what a handler sees of its Context: the
// error its chain responded with, WriteWithTimeout as Write, the context
// done callback once the chain returns, and, in an asynchronous call's
// handler, Bind returning the error an error response carries.
func TestContextInHandlers(t *testing.T) {
	s := NewServer()
	responded := make(chan any, 2)
	s.Handler.Handle("/timeout-write", func(ctx *Context) { _ = ctx.WriteWithTimeout("written", time.Second) })
	s.Handler.Handle("/fail", func(ctx *Context) { _ = ctx.Error("boom") })
	s.Handler.Use(func(ctx *Context) { responded <- ctx.ResponseError() })
	var done atomic.Int32
	s.Handler.HandleContextDone(func(*Context) { done.Add(1) })
	c := dial(t, startServer(t, s), nil)

	var rsp string
	if err := c.Call("/timeout-write", nil, &rsp, arpcWait); err != nil || rsp != "written" {
		t.Fatalf("Call = %q, %v", rsp, err)
	}
	if r := recvArpc(t, responded, "the middleware"); r != nil {
		t.Fatalf("ResponseError after a Write = %v", r)
	}
	bound := make(chan error, 1)
	if err := c.CallAsync("/fail", nil, func(ctx *Context, err error) {
		var s string
		bound <- ctx.Bind(&s)
	}, arpcWait); err != nil {
		t.Fatal(err)
	}
	if err := recvArpc(t, bound, "the async handler"); err == nil || err.Error() != "boom" {
		t.Fatalf("Bind of an error response = %v", err)
	}
	if r := recvArpc(t, responded, "the middleware"); r != "boom" {
		t.Fatalf("ResponseError after Error = %v", r)
	}
	waitArpc(t, "the context done callbacks", func() bool { return done.Load() == 2 })
}

// TestNotFoundHandlers pins down the response to a method nobody handles:
// ErrMethodNotFound from a Handler with no method at all, and what
// HandleNotFound's handler writes when there is one.
func TestNotFoundHandlers(t *testing.T) {
	bare := NewServer()
	c := dial(t, startServer(t, bare), nil)
	if err := c.Call("/missing", nil, nil, arpcWait); err == nil || err.Error() != ErrMethodNotFound.Error() {
		t.Fatalf("Call to a Server with no method = %v", err)
	}

	s := NewServer()
	s.Handler.HandleNotFound(func(ctx *Context) { ctx.Write("fallback for " + ctx.Message.Method()) })
	c = dial(t, startServer(t, s), nil)
	var rsp string
	if err := c.Call("/missing", nil, &rsp, arpcWait); err != nil || rsp != "fallback for /missing" {
		t.Fatalf("Call = %q, %v", rsp, err)
	}
}

// TestRegisterWithoutName pins down Register with no name, which routes a
// method by its name alone, and the error response of the handler it makes
// for a request it cannot decode.
func TestRegisterWithoutName(t *testing.T) {
	s := NewServer()
	if err := s.Handler.Register("", service{}); err != nil {
		t.Fatal(err)
	}
	c := dial(t, startServer(t, s), nil)
	var rsp echoRsp
	if err := c.Call("Hello", &echoReq{Message: "bare"}, &rsp, arpcWait); err != nil || rsp.Message != "hello, bare" {
		t.Fatalf("Hello = %+v, %v", rsp, err)
	}
	if err := c.Call("Hello", "not json", &rsp, arpcWait); err == nil {
		t.Fatal("a request the handler cannot decode succeeded")
	}
}

// TestWireMalformedMessages speaks the protocol by hand to pin down what the
// receiver does with messages a correct peer never sends. A message whose
// method length is zero or overruns its body, a stream message for no
// Stream, and a response nobody waits for are dropped, and the connection
// goes on; a message of an unknown command closes it.
func TestWireMalformedMessages(t *testing.T) {
	s := NewServer()
	missed := make(chan uint64, 1)
	s.Handler.HandleSessionMiss(func(_ *Client, m *Message) { missed <- m.Seq() })
	addr := startServer(t, s)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	overrun := frame(CmdRequest, 0, 2, "/m", "")
	overrun[HeaderIndexMethodLen] = 10
	for _, b := range [][]byte{
		frame(CmdRequest, 0, 1, "", "no method"),
		overrun,
		frame(CmdStream, 0, 3, "/s", "for no stream"),
		frame(CmdResponse, HeaderFlagMaskAsync, 4, "/r", "unasked"),
		frame(CmdPing, 0, 0, "", ""),
	} {
		if _, err := conn.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	if head, _, _ := readFrame(t, conn); head[HeaderIndexCmd] != CmdPong {
		t.Fatalf("the ping after the malformed messages got cmd %d", head[HeaderIndexCmd])
	}
	if seq := recvArpc(t, missed, "the session miss"); seq != 4 {
		t.Fatalf("session miss of seq %d", seq)
	}

	if _, err := conn.Write(frame(0x3f, 0, 5, "/x", "")); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(arpcWait))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("a message of an unknown command was served")
	}
}

// TestWireCloseMidMessage pins down a connection that closes in the middle
// of a message: the Server ends its Client and lets go of the partial one.
func TestWireCloseMidMessage(t *testing.T) {
	s := NewServer()
	addr := startServer(t, s)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	partial := frame(CmdRequest, 0, 1, "/m", "0123456789")
	binary.LittleEndian.PutUint32(partial, 100)
	if _, err := conn.Write(partial); err != nil {
		t.Fatal(err)
	}
	waitArpc(t, "the connection to be served", func() bool { return s.CurrLoad() == 1 })
	conn.Close()
	waitArpc(t, "the Client to end", func() bool { return s.CurrLoad() == 0 })
}

// TestWireLongBodyGrowsAsItArrives pins down that a message whose header
// announces a long body is not given a buffer of that length before the body
// arrives: one that announces 32 MiB and sends a few bytes costs about
// recvChunk, and the buffer grows as the rest comes, ending up whole, with
// buffers from make or from the pool.
func TestWireLongBodyGrowsAsItArrives(t *testing.T) {
	for _, pool := range []bool{false, true} {
		t.Run(map[bool]string{false: "make", true: "pool"}[pool], func(t *testing.T) {
			testWireLongBody(t, pool)
		})
	}
}

func testWireLongBody(t *testing.T, pool bool) {
	const bodyLen = 32 << 20
	s := NewServer()
	var largest atomic.Int64
	if pool {
		s.Handler.EnablePool(true)
	}
	s.Handler.HandleMalloc(func(size int) []byte {
		for {
			cur := largest.Load()
			if int64(size) <= cur || largest.CompareAndSwap(cur, int64(size)) {
				break
			}
		}
		if pool {
			return bufferpool.Get(size)
		}
		return make([]byte, size)
	})
	got := make(chan string, 1)
	s.Handler.Handle("/big", func(ctx *Context) { got <- string(ctx.Message.Data()) })
	addr := startServer(t, s)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	payload := make([]byte, bodyLen-len("/big"))
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	msg := frame(CmdNotify, 0, 1, "/big", string(payload))
	if _, err := conn.Write(msg[:HeadLen+100]); err != nil {
		t.Fatal(err)
	}
	waitArpc(t, "the header to be read", func() bool { return largest.Load() > 0 })
	if n := largest.Load(); n > int64(HeadLen+recvChunk) {
		t.Fatalf("a header announcing %d bytes allocated %d before the body arrived", bodyLen, n)
	}
	if _, err := conn.Write(msg[HeadLen+100:]); err != nil {
		t.Fatal(err)
	}
	if data := recvArpc(t, got, "the long message"); data != string(payload) {
		t.Fatalf("the long message arrived with %d bytes, not as sent", len(data))
	}
}

// TestPriorityDataIgnored pins down that arpc ignores out-of-band data on
// either side, which it never sends.
func TestPriorityDataIgnored(t *testing.T) {
	NewServer().OnPriorityData(nil, []byte("oob"))
	(&clientHandler{}).OnPriorityData(nil, []byte("oob"))
}

// TestServerStopOnOwnEngine pins down Stop and Shutdown of a Server bound to
// an engine of the caller's: Stop closes the Server's connections, and
// Shutdown, with no Serve to wait for, returns at once.
func TestServerStopOnOwnEngine(t *testing.T) {
	s := NewServer()
	left := make(chan struct{}, 1)
	s.Handler.HandleDisconnected(func(*Client) { left <- struct{}{} })
	connected := make(chan struct{}, 1)
	s.Handler.HandleConnected(func(*Client) { connected <- struct{}{} })
	// A raw connection rather than a Client, which would redial as soon as
	// Stop closed it, onto the engine Stop leaves listening.
	c, err := net.Dial("tcp", startServer(t, s))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	recvArpc(t, connected, "the connected callback")
	if s.Engine() != nil {
		t.Fatal("Engine of a Server Serve did not run")
	}
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	recvArpc(t, left, "the disconnected callback")
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown = %v", err)
	}
	if s.CurrLoad() != 0 {
		t.Fatalf("CurrLoad after Stop = %d", s.CurrLoad())
	}
}

// TestServeErrors pins down Serve with a config it cannot bind, which returns
// the error, and Shutdown whose context ends before Serve returns.
func TestServeErrors(t *testing.T) {
	config := fib.DefaultConfig()
	config.Network = "bogus"
	if err := NewServer().Serve(config, "127.0.0.1:0"); err == nil {
		t.Fatal("Serve on a bogus network succeeded")
	}

	s := NewServer()
	served := make(chan error, 1)
	go func() {
		served <- s.Serve(fib.DefaultConfig(), "127.0.0.1:0")
	}()
	waitArpc(t, "Serve to make its engine", func() bool { return s.Engine() != nil })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// The engine stops on a loop of its own, so Serve has very likely not
	// returned yet; either way, Shutdown must not wait past its context.
	if err := s.Shutdown(ctx); err != nil && err != ErrTimeout {
		t.Fatalf("Shutdown = %v", err)
	}
	if err := recvArpc(t, served, "Serve to return"); err != nil {
		t.Fatal(err)
	}
}

// TestServerZeroValueAndBroadcastSkips pins down a Server made without
// NewServer, which makes its own map of Clients, and the Clients a broadcast
// skips: those its filter turns down, and one whose connection has just
// closed and which has not left the Server yet.
func TestServerZeroValueAndBroadcastSkips(t *testing.T) {
	s := &Server{Codec: DefaultCodec, Handler: NewHandler()}
	// The connected callback follows the Client's joining the Server.
	connected := make(chan struct{}, 1)
	s.Handler.HandleConnected(func(*Client) { connected <- struct{}{} })
	got := make(chan string, 4)
	handler := NewHandler()
	handler.Handle("/news", func(ctx *Context) { got <- string(ctx.Body()) })
	c := dial(t, startServer(t, s), handler)
	recvArpc(t, connected, "the connected callback")

	closing := newClient(DefaultCodec, s.Handler)
	s.mu.Lock()
	s.clients[closing] = struct{}{}
	s.mu.Unlock()
	s.BroadcastWithFilter("/news", "filtered", func(*Client) bool { return false })
	s.Broadcast("/news", "all")
	if body := recvArpc(t, got, "the broadcast"); body != "all" {
		t.Fatalf("the client got %q, which the filter turned down", body)
	}
	s.mu.Lock()
	delete(s.clients, closing)
	s.mu.Unlock()
	c.Stop()
}

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"context"
	"errors"
	stdhttp "net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// cvH2sSync sends a PING and reads until its ack, by when the server has
// handled every frame sent before it.
func cvH2sSync(t *testing.T, tc *h2TestConn) {
	t.Helper()
	tc.write(append(h2AppendFrameHeader(nil, h2FramePing, 0, 0, 8), "cvh2ssyn"...))
	for {
		if f := tc.readUntil(h2FramePing); f.has(h2FlagAck) && string(f.payload) == "cvh2ssyn" {
			return
		}
	}
}

// cvH2sWait takes what a handler reports, or fails the test.
func cvH2sWait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("the handler reported nothing")
		var zero T
		return zero
	}
}

// cvH2sConnect opens an extended CONNECT for "echo" on stream id.
func cvH2sConnect(tc *h2TestConn, id uint32) {
	tc.headers(id, false, ":method", "CONNECT", ":protocol", "echo", ":scheme", "http", ":path", "/", ":authority", "t")
}

// cvH2sTunnel is a TunnelShutdownHandler that ends its side of the tunnel
// when it hears of the shutdown.
type cvH2sTunnel struct{ told chan struct{} }

func (h *cvH2sTunnel) OnTunnelOpen(*Tunnel)         {}
func (h *cvH2sTunnel) OnTunnelData(*Tunnel, []byte) {}
func (h *cvH2sTunnel) OnTunnelClose(*Tunnel, error) {}
func (h *cvH2sTunnel) OnTunnelShutdown(t *Tunnel) {
	t.CloseAfterSend()
	close(h.told)
}

// TestCoverH2ServerRejectedUnderRunningHandler pins down what a handler
// hears once the server has answered its stream itself, here with a 431 for
// trailers too large while the handler of a request whose body streams
// still runs: with the client's window shut the 431's body waits, so the
// stream stays open and answered, an interim response, a whole response and
// a response that streams are each refused as already written, and an
// Upgrade as not upgradable.
func TestCoverH2ServerRejectedUnderRunningHandler(t *testing.T) {
	config := DefaultConfig()
	config.MaxHeaderBytes = 400
	config.StreamRequestBody = true
	config.StreamRequestBodyThreshold = 0
	entered, release := make(chan struct{}), make(chan struct{})
	errs := make(chan []error, 1)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		close(entered)
		<-release
		_, upgradeErr := c.Upgrade("echo", nil, &cvH2sTunnel{})
		interimErr := c.WriteInterim(stdhttp.StatusEarlyHints, nil)
		wholeErr := c.WriteResponse(Response{StatusCode: stdhttp.StatusOK})
		_, _ = c.Write([]byte("x"))
		errs <- []error{upgradeErr, interimErr, wholeErr, c.FlushError()}
	})))
	tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 0})
	tc.headers(1, false, ":method", "POST", ":scheme", "http", ":authority", "test", ":path", "/")
	tc.data(1, false, []byte("x"))
	<-entered
	big := strings.Repeat("v", 150)
	tc.headers(1, true, "x-a", big, "x-b", big, "x-c", big)
	if f := tc.readUntil(h2FrameHeaders); f.fields[":status"] != "431" {
		t.Fatalf("status %q, want 431", f.fields[":status"])
	}
	close(release)
	got := cvH2sWait(t, errs)
	if !errors.Is(got[0], ErrNotUpgradable) {
		t.Errorf("Upgrade: %v, want ErrNotUpgradable", got[0])
	}
	if got[1] == nil || !strings.Contains(got[1].Error(), "after the final one") {
		t.Errorf("WriteInterim: %v", got[1])
	}
	if got[2] == nil || !strings.Contains(got[2].Error(), "already written") {
		t.Errorf("WriteResponse: %v", got[2])
	}
	if !errors.Is(got[3], ErrResponseWritten) {
		t.Errorf("Flush: %v, want ErrResponseWritten", got[3])
	}
	// The stream ends once the handler has returned, and the connection
	// carries on.
	tc.write(h2AppendWindowUpdate(nil, 1, 1<<16))
	for st := tc.stream(1); !st.done; {
		tc.read()
	}
	cvH2sSync(t, tc)
}

// TestCoverH2ServerStatus101RefusedOnFlush pins down that a 101 a handler
// sets on HTTP/2, where it means nothing, is refused when the response
// begins to stream, before anything is sent, and the connection carries on.
// (The stream itself is left open: see
// TestCoverH2ServerFailedFinishResetsStream.)
func TestCoverH2ServerStatus101RefusedOnFlush(t *testing.T) {
	errs := make(chan error, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.WriteHeader(stdhttp.StatusSwitchingProtocols)
		errs <- c.FlushError()
	})))
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/")...)
	if err := cvH2sWait(t, errs); err == nil || !strings.Contains(err.Error(), "invalid status code 101") {
		t.Fatalf("Flush: %v", err)
	}
	cvH2sSync(t, tc)
	if st := tc.stream(1); st.header != nil {
		t.Fatalf("a header was sent: %v", st.header)
	}
}

// TestCoverH2ServerFailedFinishResetsStream checks that a stream whose
// response, left behind by its handler, cannot be sent (status 101, a field
// HTTP/2 cannot carry) is reset rather than left open for its client to wait
// on until the connection ends.
func TestCoverH2ServerFailedFinishResetsStream(t *testing.T) {
	for _, path := range []string{"/101", "/bad-field"} {
		addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
			if c.Request.URL.Path == "/101" {
				c.WriteHeader(stdhttp.StatusSwitchingProtocols)
				return
			}
			c.Header().Set("Bad Name", "x")
			_, _ = c.Write([]byte("hi"))
		})))
		tc := dialH2(t, addr)
		_ = tc.c.SetDeadline(time.Now().Add(5 * time.Second))
		tc.headers(1, true, get(path)...)
		st := tc.stream(1)
		for !st.done {
			tc.read()
		}
		if !st.resetSet || st.reset != H2InternalError || st.header != nil {
			t.Fatalf("%s: reset %v (%v), header %v; want an INTERNAL_ERROR reset and no header", path, st.resetSet, st.reset, st.header)
		}
	}
}

// TestCoverH2ServerTrailersAfterTunnelEnded pins down a tunnel whose side
// this server has ended with END_STREAM, the client ending its own with a
// trailer block, which a tunnel's stream may not carry: the stream is reset
// and forgotten, and the connection serves the next request.
func TestCoverH2ServerTrailersAfterTunnelEnded(t *testing.T) {
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		if c.Request.Method != stdhttp.MethodConnect {
			_ = c.Respond(200, "text/plain", []byte("next"))
			return
		}
		if tun, err := c.Upgrade("echo", nil, &cvH2sTunnel{}); err == nil {
			tun.CloseAfterSend()
		}
	})))
	tc := dialH2(t, addr)
	cvH2sConnect(tc, 1)
	st := tc.stream(1)
	for !st.done {
		tc.read()
	}
	if st.resetSet || st.header[":status"] != "200" {
		t.Fatalf("reset %v, header %v", st.resetSet, st.header)
	}
	tc.headers(1, true, "x-end", "1")
	tc.headers(3, true, get("/next")...)
	if _, body := tc.response(3); string(body) != "next" {
		t.Fatalf("body %q", body)
	}
	// The stream is closed now: more DATA on it is refused.
	tc.data(1, true, []byte("late"))
	for !tc.stream(1).resetSet {
		tc.read()
	}
}

// TestCoverH2ServerTrailersOnOpenTunnel checks that a trailer block a client
// sends on a tunnel's stream while the tunnel is still open both ways resets
// the stream, as RFC 9113 section 8.5 has it, rather than serving the
// CONNECT handler a second time on the same stream.
func TestCoverH2ServerTrailersOnOpenTunnel(t *testing.T) {
	var calls atomic.Int32
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		if c.Request.Method != stdhttp.MethodConnect {
			_ = c.Respond(200, "text/plain", []byte("next"))
			return
		}
		calls.Add(1)
		_, _ = c.Upgrade("echo", nil, &cvH2sTunnel{})
	})))
	tc := dialH2(t, addr)
	cvH2sConnect(tc, 1)
	if f := tc.readUntil(h2FrameHeaders); f.fields[":status"] != "200" {
		t.Fatalf("status %q", f.fields[":status"])
	}
	tc.headers(1, true, "x-end", "1")
	tc.headers(3, true, get("/next")...)
	if _, body := tc.response(3); string(body) != "next" {
		t.Fatalf("body %q", body)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("the CONNECT handler ran %d times", n)
	}
}

// TestCoverH2ServerReadDeadlineStoppedByEnd pins down a read deadline on a
// body that streams from its first byte: the body's end stops its timer, and
// the request is answered as usual.
func TestCoverH2ServerReadDeadlineStoppedByEnd(t *testing.T) {
	config := DefaultConfig()
	config.StreamRequestBody = true
	config.StreamRequestBodyThreshold = 0
	set := make(chan error, 1)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		set <- c.SetReadDeadline(time.Now().Add(time.Hour))
		var got []byte
		c.OnBody(func(data []byte, fin bool, err error) {
			got = append(got, data...)
			if fin || err != nil {
				_ = c.Respond(200, "text/plain", append(got, " "+cvH2sErrString(err)...))
			}
		})
	})))
	tc := dialH2(t, addr)
	tc.headers(1, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", "/", "content-length", "5")
	if err := cvH2sWait(t, set); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	tc.data(1, true, []byte("hello"))
	if _, body := tc.response(1); string(body) != "hello <nil>" {
		t.Fatalf("body %q", body)
	}
}

// TestCoverH2ServerInlineHandlerAnswersBeforeBodyEnds pins down a body that
// begins to stream in the DATA frame that ends it, to a handler run on the
// connection's reader that answers without reading it: the stream is
// finished by the answer, the end of the body finds it gone, and the
// connection carries on.
func TestCoverH2ServerInlineHandlerAnswersBeforeBodyEnds(t *testing.T) {
	config := DefaultConfig()
	config.StreamPool = StreamPoolConfig{Disable: true}
	config.StreamRequestBody = true
	config.StreamRequestBodyThreshold = 4
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		_ = c.Respond(200, "text/plain", []byte("early "+c.Request.URL.Path))
	})))
	tc := dialH2(t, addr)
	tc.headers(1, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", "/a", "content-length", "10")
	tc.data(1, true, []byte("0123456789"))
	if _, body := tc.response(1); string(body) != "early /a" {
		t.Fatalf("body %q", body)
	}
	tc.headers(3, true, get("/b")...)
	if _, body := tc.response(3); string(body) != "early /b" {
		t.Fatalf("body %q", body)
	}
}

// cvH2sErrString is err's text, or <nil>.
func cvH2sErrString(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// TestCoverH2ServerUnpooledHeaders pins down a server that recycles its
// streams but not its headers: each request gets a header of its own, which
// still carries every field.
func TestCoverH2ServerUnpooledHeaders(t *testing.T) {
	if !testReuse {
		t.Skip("FIB_TEST_REUSE=0 turns stream recycling off")
	}
	config := DefaultConfig()
	setReuse(&config, true)
	config.ReuseHeaders = false
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		_ = c.Respond(200, "text/plain", []byte(c.Request.Header.Get("X-A")+c.Request.Header.Get("X-B")))
	})))
	tc := dialH2(t, addr)
	for id := uint32(1); id <= 5; id += 2 {
		tc.headers(id, true, append(get("/"), "x-a", "a", "x-b", "b")...)
		if _, body := tc.response(id); string(body) != "ab" {
			t.Fatalf("stream %d: body %q", id, body)
		}
	}
}

// TestCoverH2ServerShutdownTellsTunnels pins down Shutdown against an HTTP/2
// connection carrying a tunnel: GOAWAY goes out, the tunnel's handler is
// told, ends its side, and once the client ends its own the connection
// closes and Shutdown returns.
func TestCoverH2ServerShutdownTellsTunnels(t *testing.T) {
	tunnel := &cvH2sTunnel{told: make(chan struct{})}
	upgraded := make(chan struct{})
	engine, addr, _ := shutdownServer(t, NewHandler(HandlerFunc(func(c *Context) {
		if _, err := c.Upgrade("echo", nil, tunnel); err == nil {
			close(upgraded)
		}
	})))
	tc := dialH2(t, addr)
	cvH2sConnect(tc, 1)
	<-upgraded
	cvH2sSync(t, tc)
	shut := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		shut <- engine.Shutdown(ctx)
	}()
	cvH2sWait(t, tunnel.told)
	tc.readUntil(h2FrameGoAway)
	st := tc.stream(1)
	for !st.done {
		tc.read()
	}
	tc.data(1, true, nil)
	if err := cvH2sWait(t, shut); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// TestCoverH2ServerBurstPastRetainedBuffer pins down a read that brings more
// frames than a connection keeps a buffer for: they are all handled, and the
// buffer goes back to the pool rather than staying with the connection.
func TestCoverH2ServerBurstPastRetainedBuffer(t *testing.T) {
	addr := cvH2sServe(t, NewHandler(echoHandler()), func(config *fib.Config) {
		config.ReadBufferSize = 1 << 20
	})
	tc := dialH2(t, addr)
	// Extension frames, which the server ignores, of a size that does not
	// divide a read, so that reads end within them too.
	frame := append(h2AppendFrameHeader(nil, 0xf0, 0, 0, 10000), make([]byte, 10000)...)
	burst := make([]byte, 0, 50*len(frame))
	for range 50 {
		burst = append(burst, frame...)
	}
	for range 4 {
		tc.write(burst)
		cvH2sSync(t, tc)
	}
	tc.headers(1, true, get("/after")...)
	if _, body := tc.response(1); string(body) != "GET /after " {
		t.Fatalf("body %q", body)
	}
}

// cvH2sServe is serve with the engine's config adjusted by tune.
func cvH2sServe(t *testing.T, handler *ServerHandler, tune func(*fib.Config)) string {
	t.Helper()
	if !testReuse {
		setReuse(&handler.config, false)
	}
	config := fib.DefaultConfig()
	tune(&config)
	server, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:0", Handler: handler})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	})
	return addr.String()
}

// TestCoverH2ServerBodyEndsAfterHeaderRejection pins down a request whose
// header was too large, refused with a 431 whose body waits on the client's
// window, the client then ending the body it was sending: there is no
// request to serve, and the connection serves the next one.
func TestCoverH2ServerBodyEndsAfterHeaderRejection(t *testing.T) {
	config := DefaultConfig()
	config.MaxHeaderBytes = 400
	served := make(chan string, 2)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		served <- c.Request.URL.Path
		_ = c.Respond(200, "text/plain", nil)
	})))
	tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 0})
	big := strings.Repeat("v", 150)
	tc.headers(1, false, ":method", "POST", ":scheme", "http", ":authority", "test", ":path", "/big",
		"x-a", big, "x-b", big, "x-c", big)
	if f := tc.readUntil(h2FrameHeaders); f.fields[":status"] != "431" {
		t.Fatalf("status %q, want 431", f.fields[":status"])
	}
	tc.data(1, true, []byte("body"))
	tc.headers(3, true, get("/next")...)
	if f := tc.readUntil(h2FrameHeaders); f.streamID != 3 || f.fields[":status"] != "200" {
		t.Fatalf("stream %d status %q", f.streamID, f.fields[":status"])
	}
	if path := cvH2sWait(t, served); path != "/next" {
		t.Fatalf("served %q", path)
	}
}

// A handler with no PingTimeout of its own waits DefaultPingTimeout for a
// PING's answer.
func TestCoverH2ServerDefaultPingTimeout(t *testing.T) {
	if d := (&ServerHandler{}).pingTimeout(); d != DefaultPingTimeout {
		t.Fatalf("pingTimeout %v, want %v", d, DefaultPingTimeout)
	}
}

// OnWritable on a response whose stream the client has reset runs its
// function at once, with why the stream went.
func TestCoverH2ServerOnWritableAfterReset(t *testing.T) {
	cancelled := make(chan struct{})
	writable := make(chan error, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.Retain()
		c.OnCancel(func(error) { close(cancelled) })
		c.WriteHeader(stdhttp.StatusOK)
		_, _ = c.WriteString("streaming")
		c.Flush()
		go func() {
			defer c.Release()
			<-cancelled
			if !c.OnWritable(func(err error) { writable <- err }) {
				writable <- errors.New("OnWritable not armed on a reset stream")
			}
		}()
	})))
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/")...)
	tc.readUntil(h2FrameHeaders)
	tc.write(append(h2AppendFrameHeader(nil, h2FrameRSTStream, 0, 1, 4), 0, 0, 0, byte(H2Cancel)))
	if err := cvH2sWait(t, writable); err == nil {
		t.Fatal("OnWritable ran without the reset's error")
	}
}

// A DATA frame larger than what is left of the connection's window is a
// FLOW_CONTROL_ERROR for the connection, and one larger than its stream's
// window, within the connection's, for the stream. Frames that large need a
// MaxFrameSize past the window: the window never drops below half of itself
// otherwise.
func TestCoverH2ServerDataPastWindows(t *testing.T) {
	post := []string{":method", "POST", ":scheme", "http", ":authority", "test", ":path", "/up"}
	config := DefaultConfig()
	config.HTTP2 = HTTP2Config{ConnWindow: h2DefaultWindow, MaxFrameSize: 1 << 17}
	tc := dialH2(t, serve(t, NewHandlerWithConfig(config, echoHandler())))
	tc.headers(1, false, post...)
	tc.data(1, false, make([]byte, h2DefaultWindow+1))
	if code := h2covGoAway(t, tc); code != H2FlowControlError {
		t.Fatalf("GOAWAY %v, want FLOW_CONTROL_ERROR", code)
	}

	config.HTTP2 = HTTP2Config{StreamWindow: h2DefaultWindow, MaxFrameSize: 1 << 17}
	tc = dialH2(t, serve(t, NewHandlerWithConfig(config, echoHandler())))
	tc.headers(1, false, post...)
	tc.data(1, false, make([]byte, h2DefaultWindow+1))
	if code := h2covReset(t, tc, 1); code != H2FlowControlError {
		t.Fatalf("RST_STREAM %v, want FLOW_CONTROL_ERROR", code)
	}
}

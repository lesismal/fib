//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"errors"
	"net"
	stdhttp "net/http"
	"sync/atomic"
	"testing"
	"time"
)

// h2covTunnel is a TunnelHandler whose open callback is the test's, and
// which counts what arrives.
type h2covTunnel struct {
	open     func(t *Tunnel)
	received atomic.Int64
	closed   chan error
}

func (h *h2covTunnel) OnTunnelOpen(t *Tunnel) {
	if h.open != nil {
		h.open(t)
	}
}
func (h *h2covTunnel) OnTunnelData(_ *Tunnel, data []byte) { h.received.Add(int64(len(data))) }
func (h *h2covTunnel) OnTunnelClose(_ *Tunnel, err error) {
	if h.closed != nil {
		h.closed <- err
	}
}

// h2covConnect opens an extended CONNECT for protocol on stream id.
func h2covConnect(tc *h2TestConn, id uint32, path string) {
	tc.headers(id, false, ":method", "CONNECT", ":protocol", "echo", ":scheme", "http", ":path", path, ":authority", "t")
}

// TestH2TunnelGivesBackWindowAsDataArrives checks that what a tunnel
// receives is given back to the client's window as it arrives, so that a
// tunnel carrying more than a window's worth keeps going.
func TestH2TunnelGivesBackWindowAsDataArrives(t *testing.T) {
	tunnel := &h2covTunnel{}
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		_, _ = c.Upgrade("echo", nil, tunnel)
	})))
	tc := dialH2(t, addr)
	h2covConnect(tc, 1, "/")
	if f := tc.readUntil(h2FrameHeaders); f.fields[":status"] != "200" {
		t.Fatalf("status %v", f.fields)
	}
	chunk := make([]byte, h2DefaultMaxFrameSize)
	sent := 0
	for ; sent < h2StreamWindow/2+len(chunk); sent += len(chunk) {
		tc.data(1, false, chunk)
	}
	for {
		f := tc.read()
		if f.typ == h2FrameWindowUpdate && f.streamID == 1 {
			break
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for tunnel.received.Load() != int64(sent) {
		if time.Now().After(deadline) {
			t.Fatalf("the tunnel took %d of %d bytes", tunnel.received.Load(), sent)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestH2TunnelBeforeUpgrade checks a tunnel's stream before its handler has
// accepted it: more than the tunnel holds resets the stream with
// ENHANCE_YOUR_CALM, a client that has ended its side leaves nothing to
// accept, and a header HTTP/2 cannot carry is refused.
func TestH2TunnelBeforeUpgrade(t *testing.T) {
	config := DefaultConfig()
	config.MaxBodyBytes = 100
	proceed := make(chan struct{}, 2)
	results := make(chan error, 2)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		var header stdhttp.Header
		if c.Request.URL.Path == "/bad-header" {
			header = stdhttp.Header{"Bad Name": {"x"}}
		} else {
			select {
			case <-proceed:
			case <-time.After(10 * time.Second):
			}
		}
		_, err := c.Upgrade("echo", header, &h2covTunnel{})
		results <- err
		if err != nil {
			_ = c.Respond(stdhttp.StatusTeapot, "text/plain", []byte(err.Error()))
		}
	})))
	tc := dialH2(t, addr)

	h2covConnect(tc, 1, "/flood")
	tc.data(1, false, make([]byte, 200))
	if code := h2covReset(t, tc, 1); code != H2EnhanceYourCalm {
		t.Fatalf("flood: reset %v, want %v", code, H2EnhanceYourCalm)
	}
	proceed <- struct{}{}
	if err := h2covWaitErr(t, results); err == nil {
		t.Fatal("a reset tunnel was accepted")
	}

	h2covConnect(tc, 3, "/ended")
	tc.data(3, true, nil)
	h2covSync(t, tc)
	proceed <- struct{}{}
	if err := h2covWaitErr(t, results); !errors.Is(err, errH2StreamClosed) {
		t.Fatalf("ended: %v, want %v", err, errH2StreamClosed)
	}
	if header, _ := tc.response(3); header[":status"] != "418" {
		t.Fatalf("ended: status %s", header[":status"])
	}

	h2covConnect(tc, 5, "/bad-header")
	if err := h2covWaitErr(t, results); err == nil {
		t.Fatal("an invalid header was accepted")
	}
	if header, _ := tc.response(5); header[":status"] != "418" {
		t.Fatalf("bad header: status %s", header[":status"])
	}
}

// TestH2TunnelSendHeldByWindow checks what a tunnel sends past the client's
// window: it waits, later sends queue behind it, a backlog past the limit is
// refused, nothing more may be sent once the tunnel is ending, and when the
// client has ended its side and opens the window, what waited goes with
// END_STREAM and the stream is done.
func TestH2TunnelSendHeldByWindow(t *testing.T) {
	config := DefaultConfig()
	config.MaxBodyBytes = 100
	results := make(chan []error, 1)
	closed := make(chan error, 1)
	tunnel := &h2covTunnel{closed: closed, open: func(t *Tunnel) {
		results <- []error{
			t.Send(bytes.Repeat([]byte("a"), 50)),
			t.Send(bytes.Repeat([]byte("b"), 20)),
			t.Send(make([]byte, 200)),
			t.SendOwned(bytes.Repeat([]byte("c"), 10)),
			func() error { t.CloseAfterSend(); return nil }(),
			t.Send([]byte("late")),
		}
	}}
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		_, _ = c.Upgrade("echo", nil, tunnel)
	})))
	tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 10})
	h2covConnect(tc, 1, "/")
	var errs []error
	select {
	case errs = <-results:
	case <-time.After(10 * time.Second):
		t.Fatal("the tunnel never opened")
	}
	for i, want := range []error{nil, nil, ErrTunnelBacklog, nil, nil, errH2StreamClosed} {
		if !errors.Is(errs[i], want) {
			t.Fatalf("send %d: %v, want %v", i, errs[i], want)
		}
	}
	st := tc.stream(1)
	for len(st.body) < 10 {
		tc.read()
	}
	tc.data(1, true, nil)
	h2covSync(t, tc)
	tc.write(h2AppendWindowUpdate(nil, 1, 1000))
	for !st.done {
		tc.read()
	}
	if want := string(bytes.Repeat([]byte("a"), 50)) + string(bytes.Repeat([]byte("b"), 20)) +
		string(bytes.Repeat([]byte("c"), 10)); string(st.body) != want || st.resetSet {
		t.Fatalf("body %q (reset %v), want %q", st.body, st.resetSet, want)
	}
	if err := h2covWaitErr(t, closed); err == nil {
		t.Fatal("OnTunnelClose(nil)")
	}
}

// TestH2TunnelEndsAndCloses checks the two ways this side ends a tunnel:
// CloseAfterSend ends its side with END_STREAM, leaving the client's open
// until it ends it too, and Close resets the stream with CANCEL.
func TestH2TunnelEndsAndCloses(t *testing.T) {
	tunnel := &h2covTunnel{}
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		tun, err := c.Upgrade("echo", nil, tunnel)
		if err != nil {
			return
		}
		if c.Request.URL.Path == "/close" {
			_ = tun.Close()
		} else {
			tun.CloseAfterSend()
		}
	})))
	tc := dialH2(t, addr)
	h2covConnect(tc, 1, "/end")
	st := tc.stream(1)
	for !st.done {
		tc.read()
	}
	if st.resetSet || st.header[":status"] != "200" {
		t.Fatalf("end: reset %v, header %v", st.resetSet, st.header)
	}
	// The client's side is still open, and takes data until it ends it.
	tc.data(1, false, []byte("more"))
	tc.data(1, true, nil)
	h2covSync(t, tc)

	h2covConnect(tc, 3, "/close")
	if code := h2covReset(t, tc, 3); code != H2Cancel {
		t.Fatalf("close: reset %v, want %v", code, H2Cancel)
	}
	tc.headers(5, true, get("/after")...)
	for !tc.stream(5).done {
		tc.read()
	}
}

// h2PanicTunnel is a TunnelHandler whose OnTunnelClose panics when panics is
// set, after recording that it was called.
type h2PanicTunnel struct {
	h2covTunnel
	panics bool
}

func (h *h2PanicTunnel) OnTunnelClose(t *Tunnel, err error) {
	h.h2covTunnel.OnTunnelClose(t, err)
	if h.panics {
		panic("OnTunnelClose")
	}
}

// h2covWaitClosed reads until the server closes the connection, skipping
// whatever it sends first.
func h2covWaitClosed(t *testing.T, tc *h2TestConn) {
	t.Helper()
	buf := make([]byte, 32<<10)
	for {
		_, err := tc.c.Read(buf)
		if err == nil {
			continue
		}
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatalf("the connection stayed open: %v", err)
		}
		return
	}
}

// TestH2TunnelCallbackPanics checks that a panic in what a tunnel's handler
// runs away from the connection's reader and the handler pool — its
// OnTunnelClose once the client resets the stream, or a NotifySendable
// callback — is the handler's panic, as one in a request's handler is: it
// ends the connection, not the process, which a bare goroutine let it do.
func TestH2TunnelCallbackPanics(t *testing.T) {
	for _, path := range []string{"/close", "/notify"} {
		t.Run(path[1:], func(t *testing.T) {
			tunnel := &h2PanicTunnel{h2covTunnel: h2covTunnel{closed: make(chan error, 1)}, panics: path == "/close"}
			registered := make(chan bool, 1)
			addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
				tun, err := c.Upgrade("echo", nil, tunnel)
				if err != nil {
					registered <- false
					return
				}
				if c.Request.URL.Path == "/notify" {
					// More than the client's window, so that some waits.
					_ = tun.Send(make([]byte, 2*h2DefaultWindow))
					registered <- tun.NotifySendable(1, func(error) { panic("NotifySendable") })
					return
				}
				registered <- true
			})))
			tc := dialH2(t, addr)
			h2covConnect(tc, 1, path)
			if f := tc.readUntil(h2FrameHeaders); f.fields[":status"] != "200" {
				t.Fatalf("status %v", f.fields)
			}
			if !<-registered {
				t.Fatal("the handler could not set up its callback")
			}
			tc.write(h2AppendRSTStream(nil, 1, H2Cancel))
			h2covWaitClosed(t, tc)
			if path == "/close" {
				if err := h2covWaitErr(t, tunnel.closed); err == nil {
					t.Fatal("OnTunnelClose heard no error")
				}
			}
			// The server, and the process, go on.
			tc = dialH2(t, addr)
			tc.headers(1, true, get("/after")...)
			for !tc.stream(1).done {
				tc.read()
			}
		})
	}
}

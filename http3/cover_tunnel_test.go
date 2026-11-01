//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"bytes"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/bufferpool"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/qpack"
)

// ccvTunnel is a TunnelHandler of funcs, each optional.
type ccvTunnel struct {
	open   func(*fibhttp.Tunnel)
	data   func(*fibhttp.Tunnel, []byte)
	closed chan error
}

func (h *ccvTunnel) OnTunnelOpen(t *fibhttp.Tunnel) {
	if h.open != nil {
		h.open(t)
	}
}

func (h *ccvTunnel) OnTunnelData(t *fibhttp.Tunnel, data []byte) {
	if h.data != nil {
		h.data(t, data)
	}
}

func (h *ccvTunnel) OnTunnelClose(_ *fibhttp.Tunnel, err error) {
	if h.closed != nil {
		h.closed <- err
	}
}

// ccvConnect is the HEADERS frame of an extended CONNECT for the protocol
// "ccv" to path (RFC 9220).
func ccvConnect(path string) []byte {
	return headersFrame(
		qpack.HeaderField{Name: ":method", Value: "CONNECT"},
		qpack.HeaderField{Name: ":protocol", Value: "ccv"},
		qpack.HeaderField{Name: ":scheme", Value: "https"},
		qpack.HeaderField{Name: ":authority", Value: "localhost"},
		qpack.HeaderField{Name: ":path", Value: path},
	)
}

// ccvResult is what a handler reports of one of its calls.
type ccvResult struct {
	what string
	err  error
}

// ccvResults is the reports of a test's handlers, in the order they come.
type ccvResults chan ccvResult

func (r ccvResults) report(what string, err error) { r <- ccvResult{what, err} }

// expect waits for the next report, which must be what's, and returns its
// error.
func (r ccvResults) expect(t *testing.T, what string) error {
	t.Helper()
	select {
	case got := <-r:
		if got.what != what {
			t.Fatalf("report %q (%v), want %q", got.what, got.err, what)
		}
		return got.err
	case <-time.After(10 * time.Second):
		t.Fatalf("no report of %q", what)
		return nil
	}
}

// What the HTTP/3 server's side of a tunnel does (RFC 9220): Upgrade refuses
// a request that is not an extended CONNECT, a header it cannot send, and a
// stream the client has reset; it sends the header it is given with its 200.
// The tunnel sends nothing for an empty send and takes over a buffer it is
// given, and closing it resets the stream both ways, once, after which
// sending fails.
func TestCcTunnelServer(t *testing.T) {
	results := make(ccvResults, 16)
	entered := make(chan struct{}, 1)
	tunnelClosed := make(chan error, 1)
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
		switch c.Request.URL.Path {
		case "/plain":
			_, err := c.Upgrade("ccv", nil, &ccvTunnel{})
			results.report("plain", err)
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("plain"))
		case "/bad-header":
			_, err := c.Upgrade("ccv", stdhttp.Header{"Bad Name": {"x"}}, &ccvTunnel{})
			results.report("bad header", err)
			_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", []byte("bad"))
		case "/reset-first":
			cancelled := make(chan struct{})
			c.OnCancel(func(error) { close(cancelled) })
			entered <- struct{}{}
			select {
			case <-cancelled:
			case <-time.After(10 * time.Second):
			}
			_, err := c.Upgrade("ccv", nil, &ccvTunnel{})
			results.report("reset first", err)
		case "/tunnel":
			h := &ccvTunnel{closed: tunnelClosed}
			h.open = func(tn *fibhttp.Tunnel) {
				results.report("send nothing", tn.Send(nil))
				out := bufferpool.Get(5)
				copy(out, "owned")
				results.report("send owned", tn.SendOwned(out))
			}
			h.data = func(tn *fibhttp.Tunnel, data []byte) {
				if string(data) != "close" {
					return
				}
				results.report("close", tn.Close())
				results.report("close again", tn.Close())
				results.report("send after close", tn.Send([]byte("late")))
			}
			_, err := c.Upgrade("ccv", stdhttp.Header{"X-Tunnel": {"yes"}}, h)
			results.report("upgrade", err)
		}
	})
	w := dialH3Wire(t, nil, base)

	id := w.send(t, h3Request{path: "/plain"}, false)
	if err := results.expect(t, "plain"); !errors.Is(err, fibhttp.ErrNotUpgradable) {
		t.Fatalf("a GET upgraded: %v", err)
	}
	if status, body := w.response(id); status != stdhttp.StatusOK || string(body) != "plain" {
		t.Fatalf("plain: %d %q", status, body)
	}

	open := func(path string) uint64 {
		t.Helper()
		s, err := w.qc.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Write(ccvConnect(path), false); err != nil {
			t.Fatal(err)
		}
		return s.ID()
	}

	id = open("/bad-header")
	if err := results.expect(t, "bad header"); err == nil || !strings.Contains(err.Error(), "invalid header name") {
		t.Fatalf("a bad header: %v", err)
	}
	w.await(id, "the refusal", func(st *h3WireStream) bool { return st.status == stdhttp.StatusBadRequest })

	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ccvConnect("/reset-first"), false); err != nil {
		t.Fatal(err)
	}
	<-entered
	s.Reset(uint64(ErrCodeRequestCancelled))
	if err := results.expect(t, "reset first"); !errors.Is(err, errStreamClosed) {
		t.Fatalf("upgrading a reset stream: %v", err)
	}

	tunnel, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := tunnel.Write(ccvConnect("/tunnel"), false); err != nil {
		t.Fatal(err)
	}
	for _, what := range []string{"send nothing", "send owned", "upgrade"} {
		if err := results.expect(t, what); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	w.await(tunnel.ID(), "the tunnel's first data", func(st *h3WireStream) bool {
		return st.status == stdhttp.StatusOK && string(st.body) == "owned"
	})
	if err := tunnel.Write(dataFrame([]byte("close")), false); err != nil {
		t.Fatal(err)
	}
	for _, what := range []string{"close", "close again"} {
		if err := results.expect(t, what); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	if err := results.expect(t, "send after close"); !errors.Is(err, errStreamClosed) {
		t.Fatalf("a send on a closed tunnel: %v", err)
	}
	w.await(tunnel.ID(), "the reset", func(st *h3WireStream) bool { return st.reset })
	select {
	case err := <-tunnelClosed:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("the tunnel closed with %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("OnTunnelClose was not called")
	}
}

// An extended CONNECT whose HEADERS complete once the server has begun
// shutting down, on a stream below the GOAWAY's identifier, is still served,
// its tunnel told of the shutdown from the start; the shutdown then finishes.
func TestCcTunnelOpensWhileRetiring(t *testing.T) {
	pinged := make(chan struct{}, 1)
	engine, base, done := shutdownServer(t, func(c *fibhttp.Context) {
		if c.Request.URL.Path == "/ping" {
			pinged <- struct{}{}
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	})
	r := dialRaw(t, base)
	connect, err := r.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	sr := newStreamReader(t, r, connect)
	request := ccvConnect("/late")
	if err := connect.Write(request[:1], false); err != nil {
		t.Fatal(err)
	}
	// A request on the next stream has the server count both.
	ping, err := r.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	head, _ := h3Request{path: "/ping"}.frames()
	if err := ping.Write(head, true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-pinged:
	case <-time.After(10 * time.Second):
		t.Fatal("the ping was not served")
	}

	shut := startShutdown(engine)
	// GOAWAY naming stream 8, the one after the ping's.
	goAway := append(appendFrameHeader(nil, frameGoAway, 1), 8)
	deadline := time.After(10 * time.Second)
	for {
		sr.mu.Lock()
		seen := bytes.Contains(sr.control, goAway)
		sr.mu.Unlock()
		if seen {
			break
		}
		select {
		case <-sr.arrived:
		case <-deadline:
			t.Fatal("no GOAWAY")
		}
	}
	if err := connect.Write(request[1:], true); err != nil {
		t.Fatal(err)
	}
	typ, payload, ok := sr.frame()
	if !ok || typ != frameHeaders {
		t.Fatalf("frame %d, want HEADERS", typ)
	}
	var decoder qpack.Decoder
	if fields, err := decoder.AppendFields(nil, payload, 1<<16); err != nil || len(fields) == 0 || fields[0].Value != "200" {
		t.Fatalf("response %v %v", fields, err)
	}
	select {
	case <-r.closed:
	case <-time.After(10 * time.Second):
		t.Fatal("the connection did not close")
	}
	awaitShutdown(t, done, shut)
}

// How a response that streams goes wrong on the server: a status
// BeginResponse cannot send, or a header, fails the flush that would begin
// it, and the stream is reset once the handler is done, since no answer can
// follow; a body short of its Content-Length resets the stream rather than
// ending it; a trailer of nothing HTTP/3 can carry is left out; and a
// response that ends before the request body has all arrived asks for no
// more of it with STOP_SENDING.
func TestCcResponseStreamEdges(t *testing.T) {
	results := make(ccvResults, 16)
	base := startServer(t, Config{StreamRequestBody: true}, func(c *fibhttp.Context) {
		switch c.Request.URL.Path {
		case "/status-101":
			c.WriteHeader(stdhttp.StatusSwitchingProtocols)
			_, _ = c.WriteString("x")
			results.report("101", c.FlushError())
		case "/bad-header":
			c.Header()["Bad Name"] = []string{"x"}
			_, _ = c.WriteString("x")
			results.report("bad header", c.FlushError())
		case "/short":
			c.Header().Set("Content-Length", "100")
			_, _ = c.WriteString(strings.Repeat("s", 10))
			results.report("short", c.FlushError())
		case "/bad-trailer":
			c.Header().Set("Trailer", "X-Sum")
			_, _ = c.WriteString("body")
			results.report("bad trailer", c.FlushError())
			c.Header().Set("X-Sum", "bad\x01value")
		case "/early-answer":
			_, _ = c.WriteString("early")
			results.report("early answer", c.FlushError())
		}
	})
	w := dialH3Wire(t, nil, base)

	id := w.send(t, h3Request{path: "/status-101"}, false)
	if err := results.expect(t, "101"); err == nil || !strings.Contains(err.Error(), "invalid status code") {
		t.Fatalf("a 101 began a response: %v", err)
	}
	if st := w.await(id, "the reset", func(st *h3WireStream) bool { return st.reset }); st.status != 0 {
		t.Fatalf("101: status %d", st.status)
	}
	id = w.send(t, h3Request{path: "/bad-header"}, false)
	if err := results.expect(t, "bad header"); err == nil || !strings.Contains(err.Error(), "invalid header name") {
		t.Fatalf("a bad header began a response: %v", err)
	}
	if st := w.await(id, "the reset", func(st *h3WireStream) bool { return st.reset }); st.status != 0 {
		t.Fatalf("bad header: status %d", st.status)
	}

	id = w.send(t, h3Request{path: "/short"}, false)
	if err := results.expect(t, "short"); err != nil {
		t.Fatal(err)
	}
	// The header went out before the reset, but a reset lets the receiver
	// drop what it has not delivered yet (RFC 9000 section 3.2), so whether
	// the status was seen first is up to how the two arrived.
	if st := w.await(id, "the reset", func(st *h3WireStream) bool { return st.reset }); st.status != stdhttp.StatusOK && st.status != 0 {
		t.Fatalf("short: status %d", st.status)
	}

	id = w.send(t, h3Request{path: "/bad-trailer"}, false)
	if err := results.expect(t, "bad trailer"); err != nil {
		t.Fatal(err)
	}
	if status, body := w.response(id); status != stdhttp.StatusOK || string(body) != "body" {
		t.Fatalf("bad trailer: %d %q", status, body)
	}

	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	head, body := h3Request{path: "/early-answer", framing: h3Length, body: make([]byte, 100000)}.frames()
	if err := s.Write(append(head, body[:2000]...), false); err != nil {
		t.Fatal(err)
	}
	if err := results.expect(t, "early answer"); err != nil {
		t.Fatal(err)
	}
	if status, body := w.response(s.ID()); status != stdhttp.StatusOK || string(body) != "early" {
		t.Fatalf("early answer: %d %q", status, body)
	}
}

// A handler on the goroutine that reads the connection, whose writes do not
// wait for the client, has them fail once the write timeout has reset its
// stream.
func TestCcWriteOnTheReaderAfterTimeout(t *testing.T) {
	config := Config{WriteTimeout: 50 * time.Millisecond}
	config.StreamPool = fibhttp.StreamPoolConfig{Disable: true}
	results := make(ccvResults, 1)
	base := startServer(t, config, func(c *fibhttp.Context) {
		_, _ = c.WriteString("x")
		if err := c.FlushError(); err != nil {
			results.report("write", err)
			return
		}
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			if _, err := c.WriteString("y"); err != nil {
				results.report("write", err)
				return
			}
			if err := c.FlushError(); err != nil {
				results.report("write", err)
				return
			}
		}
		results.report("write", nil)
	})
	w := dialH3Wire(t, nil, base)
	w.send(t, h3Request{path: "/"}, false)
	if err := results.expect(t, "write"); err == nil {
		t.Fatal("writes went on past the write timeout")
	}
}

// A retained extended CONNECT switched after its handler has returned, as a
// proxy switches one once its upstream has accepted: the DATA the client
// sent before the 200 is the tunnel's first.
func TestCcTunnelLateUpgrade(t *testing.T) {
	results := make(ccvResults, 4)
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
		c.Retain()
		go func() {
			defer c.Release()
			time.Sleep(200 * time.Millisecond)
			h := &ccvTunnel{data: func(tn *fibhttp.Tunnel, data []byte) {
				results.report("data "+string(data), tn.Send(data))
			}}
			_, err := c.Upgrade("ccv", nil, h)
			results.report("upgrade", err)
		}()
	})
	w := dialH3Wire(t, nil, base)
	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ccvConnect("/late"), false); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(dataFrame([]byte("early")), false); err != nil {
		t.Fatal(err)
	}
	// The queued DATA is handed over as the tunnel opens, inside Upgrade.
	for _, what := range []string{"data early", "upgrade"} {
		if err := results.expect(t, what); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	w.await(s.ID(), "the echo", func(st *h3WireStream) bool {
		return st.status == stdhttp.StatusOK && string(st.body) == "early"
	})
}

// The client sends a CONNECT whose header has :protocol as an extended
// CONNECT (RFC 9220), as net/http's HTTP/2 transport does: the server's
// tunnel takes the body as it is written, and its answer streams back.
func TestClientExtendedConnect(t *testing.T) {
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
		h := &ccvTunnel{data: func(tn *fibhttp.Tunnel, data []byte) { _ = tn.Send(bytes.ToUpper(data)) }}
		if _, err := c.Upgrade("ccv", nil, h); err != nil {
			_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", []byte(err.Error()))
		}
	})
	client := newClient(t, func(c *ClientConfig) {
		c.StreamResponseBody = true
		c.StreamUnknownLengthAtHeader = true
	})
	pr, pw := io.Pipe()
	defer pw.Close()
	req := mustRequest(t, stdhttp.MethodConnect, base+"/chat?x=1", pr)
	req.Header.Set(":protocol", "ccv")
	got := make(chan string, 4)
	answered := make(chan int, 1)
	client.Do(req, func(resp *fibhttp.ClientResponse, err error) {
		if err != nil {
			t.Errorf("CONNECT: %v", err)
			answered <- 0
			return
		}
		answered <- resp.StatusCode
		resp.OnBody(func(data []byte, fin bool, err error) {
			if len(data) > 0 {
				got <- string(data)
			}
		})
	})
	if _, err := pw.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	select {
	case status := <-answered:
		if status != stdhttp.StatusOK {
			t.Fatalf("status %d", status)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no answer")
	}
	select {
	case s := <-got:
		if s != "HELLO" {
			t.Fatalf("echo %q", s)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no echo")
	}
}

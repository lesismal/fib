//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	stdhttp "net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/qpack"
	"github.com/lesismal/fib/http3/quic"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// csWait is how long a test waits for the server to do something it is
// sure to do.
const csWait = 10 * time.Second

// csSingle serves a ServerHandler on an engine that hands it one datagram at
// a time, through OnData, as an engine does with a handler that is not a
// fib.DatagramsHandler: csSingle is not one, though the ServerHandler is. It
// records the connections it saw and those that closed.
type csSingle struct {
	h      *ServerHandler
	mu     sync.Mutex
	conns  []*fib.Connection
	seen   chan *fib.Connection
	closed chan *fib.Connection
}

func newCsSingle(h *ServerHandler) *csSingle {
	return &csSingle{h: h, seen: make(chan *fib.Connection, 64), closed: make(chan *fib.Connection, 64)}
}

func (w *csSingle) OnOpen(*fib.Connection)                 {}
func (w *csSingle) OnPriorityData(*fib.Connection, []byte) {}
func (w *csSingle) OnData(c *fib.Connection, data []byte) {
	w.h.OnData(c, data)
	w.mu.Lock()
	first := !slices.Contains(w.conns, c)
	if first {
		w.conns = append(w.conns, c)
	}
	w.mu.Unlock()
	if first {
		w.seen <- c
	}
}
func (w *csSingle) OnClose(c *fib.Connection, err error) {
	w.h.OnClose(c, err)
	w.closed <- c
}

// awaitSeen waits for the connection of the address addr to take its first
// datagram.
func (w *csSingle) awaitSeen(t *testing.T, addr string) *fib.Connection {
	t.Helper()
	return csAwaitConn(t, w.seen, addr, "datagram")
}

// awaitClosed waits for the connection of the address addr to close.
func (w *csSingle) awaitClosed(t *testing.T, addr string) *fib.Connection {
	t.Helper()
	return csAwaitConn(t, w.closed, addr, "close")
}

func csAwaitConn(t *testing.T, ch chan *fib.Connection, addr, what string) *fib.Connection {
	t.Helper()
	deadline := time.After(csWait)
	for {
		select {
		case c := <-ch:
			if addr == "" || c.RemoteAddr().String() == addr {
				return c
			}
		case <-deadline:
			t.Fatalf("no %s from %s", what, addr)
		}
	}
}

// csHold is csSingle on an engine whose shutdown it ignores, so that a
// connection stays open through it until the ServerHandler closes it.
type csHold struct{ *csSingle }

func (csHold) OnShutdown(*fib.Connection) {}

// csBind starts an engine serving handler on a UDP port of its own.
func csBind(t *testing.T, handler fib.Handler) (*fib.Engine, *net.UDPAddr) {
	t.Helper()
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: handler})
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, engine)
	addr, err := engine.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	return engine, addr
}

// csUDP is a bare UDP socket that sends the server what no QUIC client
// would.
func csUDP(t *testing.T, server *net.UDPAddr) *net.UDPConn {
	t.Helper()
	conn, err := net.DialUDP("udp", nil, server)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func csWrite(t *testing.T, conn *net.UDPConn, datagram []byte) {
	t.Helper()
	if _, err := conn.Write(datagram); err != nil {
		t.Fatal(err)
	}
}

func csRead(t *testing.T, conn *net.UDPConn) []byte {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(csWait))
	buf := make([]byte, 2048)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	return buf[:n]
}

func csRandom(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

// Long header packet types (RFC 9000 section 17.2).
const (
	csInitial   = 0
	csHandshake = 2
)

// csLongPacket is a long header packet of typ and version, padded to size,
// whose payload is noise: enough for the server to tell what it is, not to
// decrypt it.
func csLongPacket(typ byte, version uint32, size int) []byte {
	b := []byte{0xc0 | typ<<4}
	b = binary.BigEndian.AppendUint32(b, version)
	b = append(b, 8)
	b = append(b, csRandom(8)...)
	b = append(b, 8)
	b = append(b, csRandom(8)...)
	if typ == csInitial {
		b = append(b, 0) // no token
	}
	rest := size - len(b) - 2
	b = append(b, byte(0x40|rest>>8), byte(rest))
	return append(b, csRandom(rest)...)
}

// csShortPacket is a short header packet to cid whose payload is noise, in a
// buffer from the pool, as the engine hands datagrams over.
func csShortPacket(cid []byte, size int) []byte {
	b := bufferpool.Get(size)[:size]
	_, _ = rand.Read(b)
	b[0] = 0x40 | b[0]&0x3f
	copy(b[1:], cid)
	return b
}

// csServerConn returns the QUIC connection on the address c serves.
func csServerConn(t *testing.T, c *fib.Connection) *quic.Conn {
	t.Helper()
	peer := c.Attachment().(*udpPeer)
	peer.mu.Lock()
	defer peer.mu.Unlock()
	for _, path := range peer.paths {
		if path.qc != nil {
			return path.qc
		}
	}
	t.Fatal("no connection on the address")
	return nil
}

// The ServerHandler serves a handler that takes datagrams one at a time, as
// it does one that takes them in bursts, and answers on its own what no
// connection takes: a version it does not speak with Version Negotiation
// (RFC 9000 section 6), a packet for a connection it does not have with a
// stateless reset (RFC 9000 section 10.3). An address that never had a
// connection is let go then, and one that had is kept.
func TestCsOneDatagramAtATime(t *testing.T) {
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	// Without a handler, every request is not found.
	h := NewHandler(serverTLS, nil)
	w := newCsSingle(h)
	_, addr := csBind(t, w)
	base := fmt.Sprintf("https://localhost:%d", addr.Port)

	client := newClient(t, nil)
	resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, base+"/nothing", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if body := readBody(t, resp); resp.StatusCode != stdhttp.StatusNotFound || body != "404 page not found\n" {
		t.Fatalf("%d %q, want the default 404", resp.StatusCode, body)
	}
	c1 := w.awaitSeen(t, "")
	qc := csServerConn(t, c1)

	t.Run("burst of two connections", func(t *testing.T) {
		// A burst whose datagrams are for different connections goes to each
		// in its own run: the first, which the connection drops as noise, to
		// the connection, the second, to one the server does not have, is
		// answered with a reset that does not let the address go, since it
		// has a connection.
		h.OnDatagrams(c1, [][]byte{csShortPacket(qc.LocalCID(), 64), csShortPacket(csRandom(8), 64)})
		if c1.Attachment().(*udpPeer).isClosing() {
			t.Fatal("an address with a connection closed for a stray packet")
		}
	})

	t.Run("accept refused", func(t *testing.T) {
		// An address closing since the datagram was looked at takes no new
		// connection, and one whose Initial does not parse leaves no path.
		closing := &udpPeer{c: c1, closing: true}
		h.accept(closing, csLongPacket(csInitial, quic.Version1, 1200))
		if len(closing.paths) != 0 {
			t.Fatal("a closing address took a connection")
		}
		open := &udpPeer{c: c1}
		h.accept(open, []byte{0x40})
		if len(open.paths) != 0 {
			t.Fatal("a datagram that started no connection kept a path")
		}
	})

	t.Run("address closed under a connection", func(t *testing.T) {
		// The connection that sends on the address ends with it.
		_ = c1.Close()
		w.awaitClosed(t, c1.RemoteAddr().String())
		// Its close takes it off the routes.
		for deadline := time.Now().Add(csWait); ; time.Sleep(time.Millisecond) {
			h.routesMu.RLock()
			_, routed := h.routes[string(qc.LocalCID())]
			h.routesMu.RUnlock()
			if !routed {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the connection outlived its address")
			}
		}
		// A connection closed by the time it would start has no control
		// stream to open, and is closed for it, which it is already.
		sc := &serverConn{h: h}
		sc.start(qc)
		if sc.control != nil {
			t.Fatal("a closed connection opened its control stream")
		}
	})

	t.Run("version negotiation", func(t *testing.T) {
		conn := csUDP(t, addr)
		csWrite(t, conn, csLongPacket(csInitial, 0x0a0a0a0a, 1200))
		vn := csRead(t, conn)
		if len(vn) < 7 || vn[0]&0x80 == 0 || binary.BigEndian.Uint32(vn[1:5]) != 0 {
			t.Fatalf("answer %x, want Version Negotiation", vn)
		}
		w.awaitClosed(t, conn.LocalAddr().String())
	})

	t.Run("stateless reset", func(t *testing.T) {
		conn := csUDP(t, addr)
		// A Handshake packet from an address with no connection is dropped,
		// and the address kept, as one from a client that moved would be.
		csWrite(t, conn, csLongPacket(csHandshake, quic.Version1, 100))
		w.awaitSeen(t, conn.LocalAddr().String())
		sent := csShortPacket(csRandom(8), 80)
		csWrite(t, conn, sent)
		reset := csRead(t, conn)
		if reset[0]&0xc0 != 0x40 || len(reset) >= len(sent) {
			t.Fatalf("answer %x, want a stateless reset smaller than the %d bytes it answers", reset, len(sent))
		}
		w.awaitClosed(t, conn.LocalAddr().String())
	})
}

// A server without a certificate starts no connection: the address the
// Initial came from is let go.
func TestCsInitialWithoutCertificate(t *testing.T) {
	w := newCsSingle(NewHandlerWithConfig(Config{}, nil))
	_, addr := csBind(t, w)
	conn := csUDP(t, addr)
	csWrite(t, conn, csLongPacket(csInitial, quic.Version1, 1200))
	w.awaitClosed(t, conn.LocalAddr().String())
}

// No connection starts while the engine shuts down: the Initial of a client
// whose address the engine still has is dropped, and the address let go.
func TestCsInitialDuringShutdown(t *testing.T) {
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(serverTLS, nil)
	w := newCsSingle(h)
	engine, addr := csBind(t, csHold{w})
	conn := csUDP(t, addr)
	csWrite(t, conn, csLongPacket(csHandshake, quic.Version1, 100))
	c := w.awaitSeen(t, conn.LocalAddr().String())

	ctx, cancel := context.WithTimeout(context.Background(), csWait)
	defer cancel()
	shut := make(chan error, 1)
	go func() { shut <- engine.Shutdown(ctx) }()
	for !engine.ShuttingDown() {
		time.Sleep(time.Millisecond)
	}
	csWrite(t, conn, csLongPacket(csInitial, quic.Version1, 1200))
	w.awaitClosed(t, conn.LocalAddr().String())
	if err := <-shut; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	// The address's connection is gone from it, which a late shutdown round
	// or close finds.
	h.OnShutdown(c)
	h.OnClose(c, nil)
}

func TestCsPeerPaths(t *testing.T) {
	h := NewHandlerWithConfig(Config{}, nil)
	closing := &udpPeer{closing: true}
	if h.pathFor(closing, nil, csLongPacket(csInitial, quic.Version1, 1200)) != nil {
		t.Fatal("a closing address took a datagram")
	}
	if closing.add(new(quic.Conn)) != nil || closing.reserve() != nil {
		t.Fatal("a closing address took a path")
	}
	peer := &udpPeer{}
	qc := new(quic.Conn)
	path := peer.add(qc)
	if again := peer.add(qc); again != path || len(peer.paths) != 1 {
		t.Fatal("a connection that came back got a second path")
	}
	if peer.find(nil) != nil {
		t.Fatal("a datagram too short for a connection ID found a path")
	}
}

func TestCsConfigureTLS(t *testing.T) {
	config := ConfigureTLS(nil)
	if !slices.Equal(config.NextProtos, []string{NextProto}) || config.MinVersion != tls.VersionTLS13 {
		t.Fatalf("from nil: %v %x", config.NextProtos, config.MinVersion)
	}
	given := &tls.Config{NextProtos: []string{"h2", NextProto, "http/1.1"}}
	config = ConfigureTLS(given)
	if !slices.Equal(config.NextProtos, []string{NextProto, "h2", "http/1.1"}) {
		t.Fatalf("protocols %v", config.NextProtos)
	}
	if len(given.NextProtos) != 3 || given.NextProtos[0] != "h2" {
		t.Fatal("the config given was changed")
	}
	if got := AltSvc(8443); got != `h3=":8443"; ma=86400` {
		t.Fatalf("AltSvc %q", got)
	}
}

// A TLS config the module's handshake does not do is left to crypto/tls.
func TestCsNewTLSFallback(t *testing.T) {
	h := NewHandlerWithConfig(Config{}, nil)
	config := &tls.Config{GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return nil, nil }}
	if conn := h.newTLS(false)(false, config); conn != nil {
		t.Fatalf("got %T, want none", conn)
	}
	if safeMethod(stdhttp.MethodPost) || !safeMethod(stdhttp.MethodHead) {
		t.Fatal("safeMethod")
	}
}

// The deadlines of a request stream, without a connection under it: what
// they do once the stream has closed, its response has been written, or its
// timer has fired, and moving one that is running.
func TestCsDeadlines(t *testing.T) {
	fired := func() *time.Timer {
		timer := time.NewTimer(0)
		<-timer.C
		return timer
	}
	far := time.Now().Add(time.Hour)

	closed := &requestStream{closed: true, timedOut: true, streamed: &streamedBody{feed: &fibhttp.BodyFeed{}}}
	if err := closed.SetWriteDeadline(far); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("write deadline on a timed out stream: %v", err)
	}
	if err := closed.SetReadDeadline(far); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read deadline on a timed out stream: %v", err)
	}

	answered := &requestStream{responded: true}
	if err := answered.SetWriteDeadline(far); err != nil {
		t.Fatalf("write deadline after the response: %v", err)
	}

	rs := &requestStream{writeTimer: fired(), streamed: &streamedBody{feed: &fibhttp.BodyFeed{}, readTimer: fired()}}
	if err := rs.SetWriteDeadline(far); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("write deadline after the timer fired: %v", err)
	}
	if err := rs.SetReadDeadline(far); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read deadline after the timer fired: %v", err)
	}

	// A running timer moved keeps the reference it held, and one lifted
	// gives it back.
	rs = &requestStream{streamed: &streamedBody{feed: &fibhttp.BodyFeed{}}}
	for _, set := range []func(time.Time) error{rs.SetWriteDeadline, rs.SetReadDeadline} {
		if err := set(far); err != nil {
			t.Fatal(err)
		}
		if err := set(far.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if got := rs.refs.Load(); got != 1 && got != 2 {
			t.Fatalf("refs %d after moving a deadline", got)
		}
	}
	rs.mu.Lock()
	readStopped := rs.stopReadTimerLocked()
	rs.mu.Unlock()
	if !readStopped {
		t.Fatal("the read timer was not running")
	}
	rs.unref()
	if err := rs.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if got := rs.refs.Load(); got != 0 {
		t.Fatalf("refs %d after lifting both deadlines", got)
	}

	// A write timer stopped as it fired leaves the stream alone.
	idle := &requestStream{}
	idle.refs.Store(1)
	idle.writeTimedOut()
	if idle.closed || idle.refs.Load() != 0 {
		t.Fatal("a stopped write timer closed the stream")
	}
}

// WriteInterim refuses a bad header, a response already written and a
// stream already closed before it touches the stream.
func TestCsWriteInterimRefused(t *testing.T) {
	rs := &requestStream{}
	if err := rs.WriteInterim(stdhttp.StatusEarlyHints, stdhttp.Header{"Bad Name": {"x"}}); err == nil {
		t.Fatal("a bad header name was taken")
	}
	rs.responded = true
	if err := rs.WriteInterim(stdhttp.StatusEarlyHints, nil); err == nil || !strings.Contains(err.Error(), "after the final") {
		t.Fatalf("after the response: %v", err)
	}
	rs = &requestStream{closed: true}
	if err := rs.WriteInterim(stdhttp.StatusEarlyHints, nil); !errors.Is(err, errStreamClosed) {
		t.Fatalf("on a closed stream: %v", err)
	}
	if err := rs.Push(nil, "/", nil); !errors.Is(err, stdhttp.ErrNotSupported) {
		t.Fatalf("push: %v", err)
	}
}

// csRequest is the HEADERS frame of a request, with the header fields pairs
// names after its pseudo-header fields.
func csRequest(method, path string, pairs ...string) []byte {
	fields := []qpack.HeaderField{
		{Name: ":method", Value: method},
		{Name: ":scheme", Value: "https"},
		{Name: ":authority", Value: "localhost"},
		{Name: ":path", Value: path},
	}
	for i := 0; i+1 < len(pairs); i += 2 {
		fields = append(fields, qpack.HeaderField{Name: pairs[i], Value: pairs[i+1]})
	}
	return headersFrame(fields...)
}

func csFrames(frames ...[]byte) []byte { return slices.Concat(frames...) }

// csOpen opens a request stream on w and writes data on it.
func csOpen(t *testing.T, w *h3Wire, data []byte, fin bool) *quic.Stream {
	t.Helper()
	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(data, fin); err != nil {
		t.Fatal(err)
	}
	return s
}

func csWriteOn(t *testing.T, s *quic.Stream, data []byte, fin bool) {
	t.Helper()
	if err := s.Write(data, fin); err != nil {
		t.Fatal(err)
	}
}

func csAwaitReset(t *testing.T, w *h3Wire, s *quic.Stream) {
	t.Helper()
	w.await(s.ID(), "reset", func(st *h3WireStream) bool { return st.reset })
}

func csExpectStatus(t *testing.T, w *h3Wire, s *quic.Stream, want int) {
	t.Helper()
	if status, body := w.response(s.ID()); status != want {
		t.Fatalf("stream %d: %d %q, want %d", s.ID(), status, body, want)
	}
}

func csRecv[T any](t *testing.T, ch chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(csWait):
		t.Fatalf("no %s", what)
	}
	var zero T
	return zero
}

// csStreamOf finds the request stream c answers, for a test that breaks it
// under its handler.
func csStreamOf(c *fibhttp.Context) *requestStream {
	peer := c.Conn.Attachment().(*udpPeer)
	peer.mu.Lock()
	defer peer.mu.Unlock()
	for _, path := range peer.paths {
		if path.qc == nil {
			continue
		}
		sc := path.qc.Handler().(*serverConn)
		sc.mu.Lock()
		for _, rs := range sc.streams.all() {
			if rs.req == c.Request {
				sc.mu.Unlock()
				return rs
			}
		}
		sc.mu.Unlock()
	}
	return nil
}

// What a request stream may carry, and how the server answers what it may
// not: a request it refuses gets an error status, a malformed one has its
// stream reset (RFC 9114 section 4.1.2), and a frame that has no place on a
// request stream closes the connection (RFC 9114 section 7.2).
func TestCsRequestStreams(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	holdStarted, holdRelease := make(chan struct{}, 1), make(chan struct{})
	closeDone := make(chan struct{}, 1)
	resetErrs, badErrs := make(chan []error, 1), make(chan []error, 1)
	base := startServer(t, Config{MaxBodyBytes: 16, MaxHeaderBytes: 256}, func(c *fibhttp.Context) {
		switch c.Request.URL.Path {
		case "/tunnel":
			<-release
		case "/hold":
			holdStarted <- struct{}{}
			<-holdRelease
			_ = c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusOK, Close: true})
		case "/close":
			_ = c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusOK, Close: true})
			closeDone <- struct{}{}
		case "/reset":
			// The stream is reset under the handler, which every write then
			// hears of.
			csStreamOf(c).s.Reset(uint64(ErrCodeInternalError))
			resetErrs <- []error{
				c.WriteInterim(stdhttp.StatusEarlyHints, nil),
				c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusOK}),
				c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusOK}),
				c.WriteInterim(stdhttp.StatusEarlyHints, nil),
			}
		case "/bad":
			badErrs <- []error{
				c.WriteResponse(fibhttp.Response{StatusCode: 1000}),
				c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusOK, Header: stdhttp.Header{"Bad Name": {"x"}}}),
				c.WriteInterim(stdhttp.StatusEarlyHints, stdhttp.Header{"Bad Name": {"x"}}),
			}
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
		default:
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
		}
	})
	w := dialH3Wire(t, nil, base)

	// A body past MaxBodyBytes, without a content-length to refuse it by, is
	// refused as it arrives; what follows it in the same delivery is
	// dropped.
	s := csOpen(t, w, csFrames(csRequest("POST", "/big"), dataFrame(make([]byte, 32)),
		headersFrame(qpack.HeaderField{Name: "x-t", Value: "v"})), true)
	csExpectStatus(t, w, s, stdhttp.StatusRequestEntityTooLarge)

	// A header section that decodes past MaxHeaderBytes, though it arrived
	// smaller (RFC 9114 section 4.2.2).
	s = csOpen(t, w, csRequest("GET", "/fields", "x-pad", strings.Repeat("p", 60)), true)
	csExpectStatus(t, w, s, stdhttp.StatusRequestHeaderFieldsTooLarge)

	// An expectation the server cannot meet (RFC 9110 section 10.1.1).
	s = csOpen(t, w, csRequest("GET", "/expect", "expect", "teapot"), true)
	csExpectStatus(t, w, s, stdhttp.StatusExpectationFailed)

	// A frame type the server does not know is skipped (RFC 9114 section 9).
	s = csOpen(t, w, csFrames(controlFrame(0x21, []byte{1, 2}), csRequest("GET", "/ext")), true)
	csExpectStatus(t, w, s, stdhttp.StatusOK)

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{"pseudo-header in the trailer", csFrames(csRequest("GET", "/t"), headersFrame(qpack.HeaderField{Name: ":path", Value: "/x"}))},
		{"content-length not a number", csRequest("POST", "/cl", "content-length", "abc")},
		{"body shorter than its content-length", csFrames(csRequest("POST", "/short", "content-length", "10"), dataFrame(make([]byte, 5)))},
	} {
		s := csOpen(t, w, tc.data, true)
		csAwaitReset(t, w, s)
	}

	// An extended CONNECT whose handler has yet to take the tunnel holds what
	// arrives for it up to MaxBodyBytes; past that the stream is reset, and
	// the tunnel ended.
	s = csOpen(t, w, csFrames(csRequest("CONNECT", "/tunnel", ":protocol", "websocket"), dataFrame(make([]byte, 64))), false)
	csAwaitReset(t, w, s)

	// A handler whose stream is reset under it hears so from every write.
	s = csOpen(t, w, csRequest("GET", "/reset"), true)
	errs := csRecv(t, resetErrs, "writes on the reset stream")
	if !errors.Is(errs[0], errStreamClosed) || !errors.Is(errs[1], errStreamClosed) {
		t.Fatalf("interim %v, response %v, want %v", errs[0], errs[1], errStreamClosed)
	}
	if errs[2] == nil || !strings.Contains(errs[2].Error(), "already written") {
		t.Fatalf("second response: %v", errs[2])
	}
	if errs[3] == nil || !strings.Contains(errs[3].Error(), "after the final") {
		t.Fatalf("interim after the response: %v", errs[3])
	}
	csAwaitReset(t, w, s)

	// A response the server cannot send is refused, and the handler's next
	// goes.
	s = csOpen(t, w, csRequest("GET", "/bad"), true)
	csExpectStatus(t, w, s, stdhttp.StatusOK)
	for i, err := range csRecv(t, badErrs, "the refused responses") {
		if err == nil {
			t.Fatalf("refusal %d: no error", i)
		}
	}

	// GOAWAY (RFC 9114 section 5.2): once a response asks for the connection
	// to close, a request the client opens after is refused, while one it
	// opened before is still served; then the connection closes.
	hold := csOpen(t, w, csRequest("GET", "/hold"), true)
	csRecv(t, holdStarted, "the held request")
	s = csOpen(t, w, csRequest("GET", "/close"), true)
	csRecv(t, closeDone, "the response that closes")
	csExpectStatus(t, w, s, stdhttp.StatusOK)
	// What more arrives on the refused stream is passed over: it once found
	// a nil request stream there, and the connection's goroutine panicked.
	late := csOpen(t, w, csRequest("POST", "/late"), false)
	if err := late.Write(dataFrame([]byte("more")), true); err != nil && !errors.Is(err, quic.ErrClosed) {
		// The refusal's STOP_SENDING may have arrived first, which resets
		// the stream here, and then there is no more to send.
		t.Fatal(err)
	}
	csAwaitReset(t, w, late)
	close(holdRelease)
	csExpectStatus(t, w, hold, stdhttp.StatusOK)

	// Frames that have no place on a request stream close the connection.
	for _, tc := range []struct {
		name string
		data []byte
		code ErrorCode
	}{
		{"ended inside a frame", func() []byte { f := csRequest("GET", "/"); return f[:len(f)-3] }(), ErrCodeFrameError},
		{"SETTINGS", controlFrame(frameSettings, nil), ErrCodeFrameUnexpected},
		{"HTTP/2 PRIORITY", controlFrame(0x02, nil), ErrCodeFrameUnexpected},
		{"HEADERS after trailers", csFrames(csRequest("POST", "/"), headersFrame(qpack.HeaderField{Name: "x-t", Value: "v"}),
			headersFrame(qpack.HeaderField{Name: "x-u", Value: "v"})), ErrCodeFrameUnexpected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := dialRaw(t, base)
			s, err := r.qc.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			_ = s.Write(tc.data, true)
			r.expectClose(t, tc.code)
		})
	}

	// A client's control stream may not end (RFC 9114 section 6.2.1).
	t.Run("control stream reset", func(t *testing.T) {
		r := dialRaw(t, base)
		// A request answered after something was sent on the control stream
		// has the server hold it by then: the stream's type, before a reset
		// that would discard it if it had not gone yet, and a frame of a type
		// the server skips (RFC 9114 section 9), in a delivery of its own.
		roundTrip := func() {
			t.Helper()
			s, err := r.qc.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			sr := newStreamReader(t, r, s)
			_ = s.Write(csRequest("GET", "/"), true)
			if typ, _, ok := sr.frame(); !ok || typ != frameHeaders {
				t.Fatalf("frame %d, want the response's HEADERS", typ)
			}
		}
		control := r.rawControl(t)
		roundTrip()
		if err := control.Write(controlFrame(0x21, []byte{1}), false); err != nil {
			t.Fatal(err)
		}
		roundTrip()
		control.Reset(uint64(ErrCodeNoError))
		r.expectClose(t, ErrCodeClosedCriticalStream)
	})
}

// csBodyEnd is how a streamed body ended for its handler.
type csBodyEnd struct {
	path string
	err  error
}

// A streamed body's handler that set a read deadline, under the server's
// WriteTimeout, has both timers stopped however its request ends: whole,
// malformed, reset by the client, timed out, or with the connection.
func TestCsStreamedBodyTimers(t *testing.T) {
	ready, ended := make(chan string, 8), make(chan csBodyEnd, 8)
	base := startServer(t, Config{StreamRequestBody: true, WriteTimeout: time.Minute}, func(c *fibhttp.Context) {
		path := c.Request.URL.Path
		if err := c.SetReadDeadline(time.Now().Add(time.Hour)); err != nil {
			ended <- csBodyEnd{path, fmt.Errorf("SetReadDeadline: %w", err)}
			return
		}
		if path == "/wtimeout" {
			if err := c.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
				ended <- csBodyEnd{path, fmt.Errorf("SetWriteDeadline: %w", err)}
				return
			}
		}
		c.OnBody(func(_ []byte, fin bool, err error) {
			if !fin && err == nil {
				return
			}
			ended <- csBodyEnd{path, err}
			if err == nil {
				_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("done"))
			}
		})
		ready <- path
	})
	w := dialH3Wire(t, nil, base)
	start := func(w *h3Wire, path string, declared int) *quic.Stream {
		t.Helper()
		s := csOpen(t, w, csFrames(csRequest("POST", path, "content-length", fmt.Sprint(declared)), dataFrame(make([]byte, 2))), false)
		if got := csRecv(t, ready, "handler for "+path); got != path {
			t.Fatalf("handler for %s, want %s", got, path)
		}
		return s
	}
	end := func(path string, wantErr bool) error {
		t.Helper()
		got := csRecv(t, ended, "end of "+path)
		if got.path != path || (got.err != nil) != wantErr {
			t.Fatalf("%s ended with %v, want %s ending with an error: %v", got.path, got.err, path, wantErr)
		}
		return got.err
	}

	s := start(w, "/fin", 10)
	csWriteOn(t, s, dataFrame(make([]byte, 8)), true)
	end("/fin", false)
	csExpectStatus(t, w, s, stdhttp.StatusOK)

	s = start(w, "/long", 5)
	csWriteOn(t, s, dataFrame(make([]byte, 10)), false)
	end("/long", true)
	csAwaitReset(t, w, s)

	s = start(w, "/short", 10)
	csWriteOn(t, s, dataFrame(make([]byte, 2)), true)
	end("/short", true)
	csAwaitReset(t, w, s)

	s = start(w, "/reset", 10)
	s.Reset(uint64(ErrCodeRequestCancelled))
	if err := end("/reset", true); !errors.Is(err, errRequestReset) {
		t.Fatalf("reset body ended with %v", err)
	}

	s = start(w, "/wtimeout", 10)
	if err := end("/wtimeout", true); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("timed out body ended with %v", err)
	}
	csAwaitReset(t, w, s)

	w2 := dialH3Wire(t, nil, base)
	start(w2, "/conn", 10)
	w2.qc.Close(uint64(ErrCodeNoError), "")
	end("/conn", true)
}

// A request that is not safe to replay, in 0-RTT data, is answered 425 Too
// Early (RFC 8470 section 5.2).
func TestCsEarlyUnsafeMethod(t *testing.T) {
	base := startServer(t, Config{EarlyData: true}, func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	})
	sessions := fibtls.NewQUICSessionCache(4)
	config := quic.Config{EarlyData: true, NewTLS: func(_ bool, config *tls.Config) quic.TLSConn {
		if conn := fibtls.NewQUICClientWithSessions(config, sessions); conn != nil {
			return conn
		}
		return nil
	}}
	first := dialH3WireWith(t, nil, base, config)
	csExpectStatus(t, first, csOpen(t, first, csRequest("GET", "/"), true), stdhttp.StatusOK)
	for deadline := time.Now().Add(csWait); sessions.Get("localhost") == nil; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the server sent no ticket")
		}
	}

	var early *quic.Stream
	w := csDialEarly(t, base, config, func(qc *quic.Conn) {
		if !qc.CanSendEarly() {
			return
		}
		s, err := qc.OpenStream()
		if err == nil && s.Write(csRequest("DELETE", "/thing"), true) == nil {
			early = s
		}
	})
	if early == nil {
		t.Fatal("the resumed connection sent no early data")
	}
	csExpectStatus(t, w, early, stdhttp.StatusTooEarly)
}

// csDialEarly is dialH3WireWith, calling early with the connection as soon
// as it is dialed, before its handshake.
func csDialEarly(t *testing.T, base string, config quic.Config, early func(*quic.Conn)) *h3Wire {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	_, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	clientTLS.NextProtos = []string{NextProto}
	config.TLSConfig = clientTLS
	engine := newH3ClientEngine(t)
	w := &h3Wire{t: t, handshake: make(chan struct{}), streams: map[uint64]*h3WireStream{}}
	dialed := make(chan error, 1)
	err = engine.DialWithHandler("udp", "127.0.0.1:"+u.Port(), time.Second, w, func(fc *fib.Connection, err error) {
		if err != nil {
			dialed <- err
			return
		}
		w.mu.Lock()
		w.qc, err = quic.Dial(fc, fc.RemoteAddr(), config, h3WireHandler{w})
		qc := w.qc
		w.mu.Unlock()
		if err == nil {
			early(qc)
		}
		dialed <- err
	})
	if err == nil {
		err = <-dialed
	}
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-w.handshake:
	case <-time.After(csWait):
		t.Fatal("no handshake")
	}
	return w
}

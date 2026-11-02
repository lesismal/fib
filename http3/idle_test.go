//go:build linux || darwin || windows

package http3

import (
	stdhttp "net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/qpack"
	"github.com/lesismal/fib/http3/quic"
	"github.com/lesismal/fib/tlstest"
)

// idleWire is a raw QUIC client that records the server's GOAWAY, each
// request stream's end or reset, and the connection's close.
type idleWire struct {
	t         *testing.T
	mu        sync.Mutex
	qc        *quic.Conn
	handshake chan struct{}
	closed    chan struct{}
	goAway    chan uint64
	control   []byte
	ended     map[uint64]chan bool // true once answered, false once reset
}

func (w *idleWire) OnOpen(*fib.Connection)                 {}
func (w *idleWire) OnPriorityData(*fib.Connection, []byte) {}
func (w *idleWire) OnData(_ *fib.Connection, data []byte) {
	w.mu.Lock()
	qc := w.qc
	w.mu.Unlock()
	if qc != nil {
		qc.HandleDatagram(data)
	}
}
func (w *idleWire) OnClose(*fib.Connection, error) {}

type idleWireHandler struct{ w *idleWire }

func (h idleWireHandler) OnHandshake(*quic.Conn) { close(h.w.handshake) }

func (h idleWireHandler) OnStreamData(s *quic.Stream, data []byte, fin bool) {
	w := h.w
	w.mu.Lock()
	defer w.mu.Unlock()
	if s.Bidirectional() {
		if fin {
			w.endedLocked(s.ID()) <- true
		}
		return
	}
	// The server's control stream: its type, SETTINGS, and, at the end,
	// GOAWAY with the first stream it will not serve.
	if s.ID() != 3 {
		return
	}
	w.control = append(w.control, data...)
	rest := w.control
	_, n := quic.ReadVarint(rest)
	rest = rest[n:]
	for len(rest) > 0 {
		typ, n1 := quic.ReadVarint(rest)
		if n1 <= 0 {
			return
		}
		length, n2 := quic.ReadVarint(rest[n1:])
		if n2 <= 0 || uint64(len(rest[n1+n2:])) < length {
			return
		}
		payload := rest[n1+n2 : n1+n2+int(length)]
		if typ == frameGoAway {
			id, _ := quic.ReadVarint(payload)
			select {
			case w.goAway <- id:
			default:
			}
		}
		rest = rest[n1+n2+int(length):]
	}
}

func (h idleWireHandler) OnStreamReset(s *quic.Stream, _ uint64) {
	h.w.mu.Lock()
	h.w.endedLocked(s.ID()) <- false
	h.w.mu.Unlock()
}
func (h idleWireHandler) OnStopSending(*quic.Stream, uint64) {}
func (h idleWireHandler) OnStreamsAvailable(*quic.Conn)      {}
func (h idleWireHandler) OnClose(*quic.Conn, error)          { close(h.w.closed) }

func (w *idleWire) endedLocked(id uint64) chan bool {
	ch := w.ended[id]
	if ch == nil {
		ch = make(chan bool, 4)
		w.ended[id] = ch
	}
	return ch
}

func (w *idleWire) endOf(id uint64) chan bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.endedLocked(id)
}

// dialIdleWire connects to base with config for the QUIC connection.
func dialIdleWire(t *testing.T, base string, config quic.Config) *idleWire {
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
	w := &idleWire{t: t, handshake: make(chan struct{}), closed: make(chan struct{}), goAway: make(chan uint64, 1),
		ended: map[uint64]chan bool{}}
	dialed := make(chan error, 1)
	err = newH3ClientEngine(t).DialWithHandler("udp", "127.0.0.1:"+u.Port(), time.Second, w, func(fc *fib.Connection, err error) {
		if err != nil {
			dialed <- err
			return
		}
		w.mu.Lock()
		config.TLSConfig = clientTLS
		w.qc, err = quic.Dial(fc, fc.RemoteAddr(), config, idleWireHandler{w})
		w.mu.Unlock()
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
	case <-time.After(15 * time.Second):
		t.Fatal("no handshake")
	}
	return w
}

// request sends a whole GET for path and returns its stream's ID.
func (w *idleWire) request(path string) uint64 {
	w.t.Helper()
	s, err := w.qc.OpenStream()
	if err != nil {
		w.t.Fatal(err)
	}
	head, _ := h3Request{path: path}.frames()
	if err := s.Write(head, true); err != nil {
		w.t.Fatal(err)
	}
	return s.ID()
}

func (w *idleWire) awaitEnd(id uint64) bool {
	w.t.Helper()
	select {
	case answered := <-w.endOf(id):
		return answered
	case <-time.After(10 * time.Second):
		w.t.Fatalf("stream %d neither answered nor reset", id)
		return false
	}
}

// expectRetired waits for the server's GOAWAY naming first, then for the
// connection to close.
func (w *idleWire) expectRetired(first uint64) {
	w.t.Helper()
	select {
	case id := <-w.goAway:
		if id != first {
			w.t.Fatalf("GOAWAY names stream %d, want %d", id, first)
		}
	case <-time.After(10 * time.Second):
		w.t.Fatal("no GOAWAY")
	}
	select {
	case <-w.closed:
	case <-time.After(10 * time.Second):
		w.t.Fatal("the connection stayed open after its GOAWAY")
	}
}

func idleHandler(c *fibhttp.Context) {
	if c.Request.URL.Path == "/slow" {
		time.Sleep(500 * time.Millisecond)
	}
	_ = c.Respond(200, "text/plain", []byte("ok"))
}

// A connection that carries no request for IdleTimeout is retired with
// GOAWAY and closed: one that never asks, one that goes quiet after its
// requests, and one that keeps QUIC alive with pings, which MaxIdleTimeout
// alone would never close. One with a request open longer than that is left
// alone.
func TestIdleTimeout(t *testing.T) {
	const idle = 250 * time.Millisecond
	config := DefaultConfig()
	config.IdleTimeout = idle
	url := startServer(t, config, idleHandler)
	t.Run("fresh", func(t *testing.T) {
		w := dialIdleWire(t, url, quic.Config{})
		start := time.Now()
		w.expectRetired(0)
		if waited := time.Since(start); waited < idle/2 {
			t.Fatalf("retired after %v", waited)
		}
	})
	t.Run("after requests", func(t *testing.T) {
		w := dialIdleWire(t, url, quic.Config{})
		for i := 0; i < 2; i++ {
			if !w.awaitEnd(w.request("/")) {
				t.Fatalf("request %d was reset", i)
			}
		}
		w.expectRetired(8)
	})
	t.Run("keep-alive", func(t *testing.T) {
		w := dialIdleWire(t, url, quic.Config{KeepAlivePeriod: 50 * time.Millisecond})
		w.expectRetired(0)
	})
	t.Run("busy", func(t *testing.T) {
		w := dialIdleWire(t, url, quic.Config{})
		if !w.awaitEnd(w.request("/slow")) {
			t.Fatal("the slow request was reset")
		}
		select {
		case id := <-w.goAway:
			t.Fatalf("GOAWAY (%d) while a request was open", id)
		default:
		}
		w.expectRetired(4)
	})
}

// IdleTimeout falls back to ReadTimeout, and a negative one leaves an idle
// connection open.
func TestIdleTimeoutSettings(t *testing.T) {
	t.Run("fallback", func(t *testing.T) {
		url := startServer(t, Config{ReadTimeout: 250 * time.Millisecond}, idleHandler)
		dialIdleWire(t, url, quic.Config{}).expectRetired(0)
	})
	t.Run("none", func(t *testing.T) {
		config := DefaultConfig()
		config.IdleTimeout = -1
		w := dialIdleWire(t, startServer(t, config, idleHandler), quic.Config{KeepAlivePeriod: 50 * time.Millisecond})
		select {
		case id := <-w.goAway:
			t.Fatalf("GOAWAY (%d) without an IdleTimeout", id)
		case <-w.closed:
			t.Fatal("closed without an IdleTimeout")
		case <-time.After(500 * time.Millisecond):
		}
	})
}

// A request whose header does not all arrive by ReadHeaderTimeout has its
// stream reset, and the connection goes on serving; one that arrives in
// pieces in time is answered.
func TestReadHeaderTimeout(t *testing.T) {
	config := DefaultConfig()
	config.ReadHeaderTimeout = 250 * time.Millisecond
	url := startServer(t, config, idleHandler)
	w := dialIdleWire(t, url, quic.Config{})
	head := headersFrame(qpack.HeaderField{Name: ":method", Value: "GET"}, qpack.HeaderField{Name: ":scheme", Value: "https"},
		qpack.HeaderField{Name: ":authority", Value: "localhost"}, qpack.HeaderField{Name: ":path", Value: "/"})

	stalled, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := stalled.Write(head[:len(head)/2], false); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if w.awaitEnd(stalled.ID()) {
		t.Fatal("a request whose header never finished was answered")
	}
	if waited := time.Since(start); waited < 150*time.Millisecond {
		t.Fatalf("reset after %v", waited)
	}

	pieces, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	for i, piece := range h3Pieces(head, 3) {
		if err := pieces.Write(piece, i == 2); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !w.awaitEnd(pieces.ID()) {
		t.Fatal("a header that arrived in time was reset")
	}
}

// HandshakeTimeout reaches QUIC, and DefaultConfig sets the read timeouts.
func TestHTTP3TimeoutSettings(t *testing.T) {
	config := DefaultConfig()
	if config.IdleTimeout != fibhttp.DefaultIdleTimeout || config.ReadHeaderTimeout != fibhttp.DefaultReadHeaderTimeout ||
		config.ReadBodyTimeout != fibhttp.DefaultReadBodyTimeout {
		t.Fatalf("DefaultConfig timeouts: %+v", config)
	}
	config.HandshakeTimeout = 3 * time.Second
	if h := NewHandlerWithConfig(config, nil); h.quic.HandshakeTimeout != 3*time.Second {
		t.Fatalf("HandshakeTimeout %v", h.quic.HandshakeTimeout)
	}
	if idleTimeout(&Config{ReadTimeout: time.Second}) != time.Second || headerTimeout(&Config{ReadTimeout: time.Second}) != time.Second {
		t.Fatal("the fallback to ReadTimeout")
	}
}

// An idle timer that fires while a request is open, while the connection
// goes away, or after a request stream opened since its idleness began,
// leaves the connection alone, the last putting itself off; and without
// timeouts a request left incomplete arms no timer.
func TestIdleExpiredStale(t *testing.T) {
	config := DefaultConfig()
	config.IdleTimeout = time.Hour
	sc := &serverConn{h: NewHandlerWithConfig(config, nil)}
	sc.idleTimer = time.AfterFunc(time.Hour, func() {})
	defer sc.idleTimer.Stop()
	held := &requestStream{}
	sc.streams.add(0, held)
	sc.idleExpired()
	sc.streams.remove(0, held)
	sc.goingAway = true
	sc.idleExpired()
	sc.goingAway = false
	sc.idleSince, sc.lastRequest = time.Now().Add(-2*time.Hour), time.Now()
	sc.idleExpired()
	if sc.goingAway || !sc.idleSince.Equal(sc.lastRequest) {
		t.Fatal("a stale idle timer retired the connection")
	}

	untimed := &serverConn{h: NewHandlerWithConfig(Config{}, nil)}
	for _, rs := range []*requestStream{{sc: untimed}, {sc: untimed, req: &stdhttp.Request{}}} {
		rs.armPendingTimer()
		if rs.collectTimer != nil {
			t.Fatal("a timer armed without timeouts")
		}
	}
}

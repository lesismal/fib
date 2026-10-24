//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"bytes"
	"net"
	stdhttp "net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/http3/quic"
	"github.com/lesismal/fib/tlstest"
)

// capturePath is a quic.PacketConn that keeps what is sent through it.
type capturePath struct {
	mu   sync.Mutex
	sent [][]byte
}

func (p *capturePath) Send(d []byte) error {
	p.mu.Lock()
	p.sent = append(p.sent, bytes.Clone(d))
	p.mu.Unlock()
	return nil
}

func (p *capturePath) Close() error { return nil }

// nopQUICHandler is a quic.Handler that does nothing.
type nopQUICHandler struct{}

func (nopQUICHandler) OnHandshake(*quic.Conn)                  {}
func (nopQUICHandler) OnStreamData(*quic.Stream, []byte, bool) {}
func (nopQUICHandler) OnStreamReset(*quic.Stream, uint64)      {}
func (nopQUICHandler) OnStopSending(*quic.Stream, uint64)      {}
func (nopQUICHandler) OnStreamsAvailable(*quic.Conn)           {}
func (nopQUICHandler) OnClose(*quic.Conn, error)               {}

// firstFlight returns the datagrams a new client's handshake starts with,
// as a client that never goes on with it would send them: an Initial, of a
// connection ID of its own, in one datagram or more.
func firstFlight(t *testing.T) [][]byte {
	t.Helper()
	_, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	clientTLS.NextProtos = []string{NextProto}
	path := &capturePath{}
	c, err := quic.Dial(path, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, quic.Config{TLSConfig: clientTLS}, nopQUICHandler{})
	if err != nil {
		t.Fatal(err)
	}
	c.Abort(net.ErrClosed)
	path.mu.Lock()
	defer path.mu.Unlock()
	return path.sent
}

// rawUDP is a UDP socket that sends to the server at base.
func rawUDP(t *testing.T, base string) (*net.UDPConn, *net.UDPAddr) {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	server, err := net.ResolveUDPAddr("udp", "127.0.0.1:"+u.Port())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, server
}

// answers reads what the server sends conn for wait, and returns it.
func answers(conn *net.UDPConn, wait time.Duration) [][]byte {
	var got [][]byte
	deadline := time.Now().Add(wait)
	buf := make([]byte, 2048)
	for {
		_ = conn.SetReadDeadline(deadline)
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			return got
		}
		got = append(got, bytes.Clone(buf[:n]))
	}
}

// longDCID is the destination connection ID of a long header packet.
func longDCID(d []byte) string {
	if len(d) < 6 || d[0]&0x80 == 0 || len(d) < 6+int(d[5]) {
		return ""
	}
	return string(d[6 : 6+int(d[5])])
}

// isRetry reports whether d is a version 1 Retry packet.
func isRetry(d []byte) bool {
	return len(d) > 5 && d[0]&0xf0 == 0xf0 && bytes.Equal(d[1:5], []byte{0, 0, 0, 1})
}

// TestRetryPastRetryHandshakes checks that once RetryHandshakes are under
// way, a new client's Initial is answered with a Retry, before any TLS work.
func TestRetryPastRetryHandshakes(t *testing.T) {
	config := DefaultConfig()
	config.RetryHandshakes = 1
	conn, server := rawUDP(t, startServer(t, config, echo))
	for i, want := range []bool{false, true} {
		for _, d := range firstFlight(t) {
			if _, err := conn.WriteToUDP(d, server); err != nil {
				t.Fatal(err)
			}
		}
		got := answers(conn, 300*time.Millisecond)
		if len(got) == 0 {
			t.Fatalf("handshake %d: no answer", i)
		}
		if retried := isRetry(got[0]); retried != want {
			t.Fatalf("handshake %d answered with a Retry: %v, want %v", i, retried, want)
		}
	}
}

// TestRetryEveryClient checks that a server that asks every client for a
// Retry serves a client that answers it.
func TestRetryEveryClient(t *testing.T) {
	config := DefaultConfig()
	config.RetryHandshakes = -1
	base := startServer(t, config, echo)
	client := newClient(t, nil)
	for i := 0; i < 2; i++ {
		resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, base+"/retried", nil)).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if got := readBody(t, resp); got != "GET /retried " {
			t.Fatalf("body %q", got)
		}
	}
	// A raw Initial is answered with a Retry, however few handshakes are
	// under way.
	conn, server := rawUDP(t, base)
	for _, d := range firstFlight(t) {
		_, _ = conn.WriteToUDP(d, server)
	}
	if got := answers(conn, 300*time.Millisecond); len(got) == 0 || !isRetry(got[0]) {
		t.Fatalf("answered with %d datagrams, not a Retry", len(got))
	}
}

// TestMaxHandshakes checks that past MaxHandshakes under way, a new client's
// Initial is dropped: three handshakes started, two answered.
func TestMaxHandshakes(t *testing.T) {
	config := DefaultConfig()
	config.MaxHandshakes = 2
	conn, server := rawUDP(t, startServer(t, config, echo))
	for i := 0; i < 3; i++ {
		for _, d := range firstFlight(t) {
			if _, err := conn.WriteToUDP(d, server); err != nil {
				t.Fatal(err)
			}
		}
		// One at a time, so that which are answered does not depend on the
		// order the server takes them in.
		time.Sleep(50 * time.Millisecond)
	}
	answered := map[string]bool{}
	for _, d := range answers(conn, 500*time.Millisecond) {
		if cid := longDCID(d); cid != "" {
			answered[cid] = true
		}
	}
	if len(answered) != 2 {
		t.Fatalf("%d handshakes answered, want 2", len(answered))
	}
}

// TestStatelessResetKey checks that a server given a stateless reset key
// makes its tokens with it, so that another server with the key resets the
// connections of the first.
func TestStatelessResetKey(t *testing.T) {
	key := quic.NewResetKey()
	config := DefaultConfig()
	config.StatelessResetKey = &key
	h := NewHandlerWithConfig(config, nil)
	if h.quic.ResetKey == nil || *h.quic.ResetKey != key {
		t.Fatal("connections do not make their tokens with the key")
	}
	packet := append([]byte{0x40}, make([]byte, 60)...)
	mine, theirs := h.resetKey.StatelessReset(packet), key.StatelessReset(packet)
	if !bytes.Equal(mine[len(mine)-16:], theirs[len(theirs)-16:]) {
		t.Fatal("the reset token is not the key's")
	}
	other := NewHandlerWithConfig(DefaultConfig(), nil)
	if other.resetKey == key {
		t.Fatal("a handler without a key has the same one")
	}
}

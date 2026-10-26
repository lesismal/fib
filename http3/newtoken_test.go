//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"bytes"
	"crypto/tls"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/http3/quic"
	"github.com/lesismal/fib/tlstest"
)

// tokenHandshakeHandler is a QUIC handler that says when the handshake is
// over, and does nothing else.
type tokenHandshakeHandler struct {
	nopQUICHandler
	once      sync.Once
	handshake chan struct{}
}

func (h *tokenHandshakeHandler) OnHandshake(*quic.Conn) {
	h.once.Do(func() { close(h.handshake) })
}

// sharedUDPPath is one UDP socket shared by the connections of a test, which
// routes what the server sends to the connection it is for. A connection of
// a NEW_TOKEN test has to come from the address an earlier one did for the
// token to be good, so the test keeps the socket as they come and go.
type sharedUDPPath struct {
	sock    *net.UDPConn
	server  *net.UDPAddr
	answers chan []byte

	mu      sync.Mutex
	qc      *quic.Conn
	backlog [][]byte
}

func (p *sharedUDPPath) Send(d []byte) error {
	_, err := p.sock.WriteToUDP(d, p.server)
	return err
}

func (p *sharedUDPPath) Close() error { return nil }

// deliver hands d to qc and then queues it for the test: a datagram the test
// has seen has been handled. Callers hold p.mu across both, so that what the
// test reads is in the order the path took the datagrams: without that, use
// could still be draining the backlog that arrived before Dial when the
// answer to the handshake a Retry starts arrives, and the answer would be
// queued first, as the connection's first answer.
func (p *sharedUDPPath) deliver(qc *quic.Conn, d []byte) {
	qc.HandleDatagram(d)
	select {
	case p.answers <- d:
	default:
	}
}

// use makes qc the connection the path delivers to, and hands it what
// arrived before.
func (p *sharedUDPPath) use(qc *quic.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.qc = qc
	backlog := p.backlog
	p.backlog = nil
	for _, d := range backlog {
		p.deliver(qc, d)
	}
}

// receive takes a datagram from the socket and delivers it: a datagram the
// test has seen has been handled.
func (p *sharedUDPPath) receive(d []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.qc == nil {
		// A connection is between Dial and use.
		if len(p.backlog) < 8 {
			p.backlog = append(p.backlog, d)
		}
		return
	}
	p.deliver(p.qc, d)
}

// waitQuiet waits until the path has been quiet for a while, which is when
// the server has sent everything it had to for the connection.
func (p *sharedUDPPath) waitQuiet(t *testing.T) {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case <-p.answers:
		case <-time.After(150 * time.Millisecond):
			return
		case <-timeout:
			t.Fatal("the path never went quiet")
		}
	}
}

// answerTo waits for the server's first datagram to the connection whose
// local connection ID is cid.
func answerTo(t *testing.T, answers chan []byte, cid []byte) []byte {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case d := <-answers:
			if bytes.Equal(quic.DatagramDCID(d), cid) {
				return d
			}
		case <-timeout:
			t.Fatal("no answer from the server")
		}
	}
}

// tokenTestClientTLS is the TLS configuration of a client of the HTTP/3
// server a test starts.
func tokenTestClientTLS(t *testing.T) *tls.Config {
	t.Helper()
	_, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	clientTLS.NextProtos = []string{NextProto}
	clientTLS.ServerName = "localhost"
	return clientTLS
}

// TestNewTokenSkipsRetry checks that a fib client that comes back to a fib
// server from the same address with the token of its NEW_TOKEN frame is
// taken in without a Retry, where its first connection was retried: a round
// trip saved under load.
func TestNewTokenSkipsRetry(t *testing.T) {
	config := DefaultConfig()
	// Every client without a good token is retried, so that a second
	// connection's first answer tells whether its token was taken.
	config.RetryHandshakes = -1
	base := startServer(t, config, echo)
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	server, err := net.ResolveUDPAddr("udp", "127.0.0.1:"+u.Port())
	if err != nil {
		t.Fatal(err)
	}
	sock, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sock.Close() })
	path := &sharedUDPPath{sock: sock, server: server, answers: make(chan []byte, 64)}
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		buf := make([]byte, 2048)
		for {
			_ = sock.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			n, _, err := sock.ReadFromUDP(buf)
			if err != nil {
				select {
				case <-done:
					return
				default:
					continue
				}
			}
			path.receive(bytes.Clone(buf[:n]))
		}
	}()
	clientTLS := tokenTestClientTLS(t)
	tokens := quic.NewTokenCache()
	configs := func() quic.Config {
		return quic.Config{TLSConfig: clientTLS, TokenCache: tokens, HandshakeTimeout: 10 * time.Second}
	}
	handshake := func() *quic.Conn {
		h := &tokenHandshakeHandler{handshake: make(chan struct{})}
		qc, err := quic.Dial(path, server, configs(), h)
		if err != nil {
			t.Fatal(err)
		}
		path.use(qc)
		if first := answerTo(t, path.answers, qc.LocalCID()); !isRetry(first) {
			t.Fatalf("the first connection was not retried: first answer %x", first[:min(len(first), 20)])
		}
		select {
		case <-h.handshake:
		case <-time.After(10 * time.Second):
			t.Fatal("the handshake timed out")
		}
		// The server sends NEW_TOKEN with HANDSHAKE_DONE, before the
		// handshake is over for the client; wait for what it sent.
		path.waitQuiet(t)
		return qc
	}
	// The first connection is retried, and is given a token: it comes back
	// with it, from its own address.
	first := handshake()
	first.Abort(net.ErrClosed)
	path.waitQuiet(t)
	// The second one sends the token in its first Initial, and is taken in
	// at once: no Retry, so no extra round trip.
	h := &tokenHandshakeHandler{handshake: make(chan struct{})}
	qc, err := quic.Dial(path, server, configs(), h)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { qc.Abort(net.ErrClosed) })
	path.use(qc)
	if first := answerTo(t, path.answers, qc.LocalCID()); isRetry(first) {
		t.Fatal("the second connection from the same address was retried")
	}
	select {
	case <-h.handshake:
	case <-time.After(10 * time.Second):
		t.Fatal("the second handshake timed out")
	}
}

// TestNewTokenWiring checks that an ordinary server and client have
// NEW_TOKEN on, and that Transport.DisableNewTokens turns it off for each.
func TestNewTokenWiring(t *testing.T) {
	h := NewHandlerWithConfig(DefaultConfig(), nil)
	if h.quic.TokenKey == nil {
		t.Fatal("the server has no token key")
	}
	if h.quic.DisableNewTokens {
		t.Fatal("NEW_TOKEN is off by default")
	}
	config := DefaultConfig()
	config.Transport.DisableNewTokens = true
	if h := NewHandlerWithConfig(config, nil); !h.quic.DisableNewTokens {
		t.Fatal("Transport.DisableNewTokens did not reach the server's QUIC config")
	}
	if c := newClient(t, nil); c.tokens == nil {
		t.Fatal("the client keeps the tokens it is sent")
	}
	c := newClient(t, func(config *ClientConfig) {
		config.Transport.DisableNewTokens = true
	})
	if c.tokens != nil {
		t.Fatal("the client keeps tokens with NEW_TOKEN off")
	}
}

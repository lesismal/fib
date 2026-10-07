package quic

import (
	"bytes"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/tlstest"
)

// serverPath is a path of the server's to the client: what it sends
// reaches the client, unless it is a black hole, which drops it.
type serverPath struct {
	addr     *net.UDPAddr
	toClient chan []byte
	black    atomic.Bool
	mu       sync.Mutex
	closed   bool
}

func (p *serverPath) Send(d []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return net.ErrClosed
	}
	if p.black.Load() {
		return nil
	}
	select {
	case p.toClient <- bytes.Clone(d):
	default:
	}
	return nil
}

func (p *serverPath) Close() error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	return nil
}

func (p *serverPath) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// arrival is a datagram reaching the server, and the path it came on.
type arrival struct {
	d    []byte
	path *serverPath
}

// rebindNet is a network whose client's datagrams reach the server on the
// path current names, as a NAT in front of the client would have them, or
// nowhere while it names none. replay, while set, gets a copy of each of
// them first, as an attacker on the path that raced the client would.
type rebindNet struct {
	toServer chan arrival
	toClient chan []byte
	current  atomic.Pointer[serverPath]
	replay   atomic.Pointer[serverPath]
}

// clientSide is the client's PacketConn on a rebindNet.
type clientSide struct{ n *rebindNet }

func (c clientSide) Send(d []byte) error {
	if p := c.n.replay.Load(); p != nil {
		c.n.toServer <- arrival{bytes.Clone(d), p}
	}
	if p := c.n.current.Load(); p != nil {
		c.n.toServer <- arrival{bytes.Clone(d), p}
	}
	return nil
}

func (clientSide) Close() error { return nil }

// pathHandler is a testHandler that counts its path changes.
type pathHandler struct {
	*testHandler
	changes atomic.Int32
}

func (h *pathHandler) OnPathChange(*Conn) { h.changes.Add(1) }

type rebindPair struct {
	n              *rebindNet
	a, b           *serverPath
	client, server *Conn
	clientH        *testHandler
	serverH        *pathHandler
}

// newRebindPair connects a client and a server over a rebindNet whose
// client starts on path a; b is the address a NAT may move it to.
func newRebindPair(t *testing.T) *rebindPair {
	t.Helper()
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	serverTLS.NextProtos = []string{"test"}
	clientTLS.NextProtos = []string{"test"}
	n := &rebindNet{toServer: make(chan arrival, 4096), toClient: make(chan []byte, 4096)}
	p := &rebindPair{n: n, clientH: newTestHandler(), serverH: &pathHandler{testHandler: newTestHandler()},
		a: &serverPath{addr: &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1000}, toClient: n.toClient},
		b: &serverPath{addr: &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 2000}, toClient: n.toClient}}
	p.serverH.onData = func(s *Stream, data []byte, fin bool) { _ = s.Write(data, fin) }
	n.current.Store(p.a)
	done := make(chan struct{})
	var wg sync.WaitGroup
	var serverMu sync.Mutex
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case a := <-n.toServer:
				serverMu.Lock()
				if p.server == nil {
					if IsInitial(a.d) {
						p.server = Accept(p.a, p.a.addr, Config{TLSConfig: serverTLS}, p.serverH, a.d)
					}
					serverMu.Unlock()
					continue
				}
				s := p.server
				serverMu.Unlock()
				s.HandlePathDatagram(a.path, a.path.addr, a.d)
			case <-done:
				return
			}
		}
	}()
	p.client, err = Dial(clientSide{n}, p.a.addr, Config{TLSConfig: clientTLS}, p.clientH)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		defer wg.Done()
		for {
			select {
			case d := <-n.toClient:
				p.client.HandleDatagram(d)
			case <-done:
				return
			}
		}
	}()
	t.Cleanup(func() {
		close(done)
		wg.Wait()
		serverMu.Lock()
		server := p.server
		serverMu.Unlock()
		for _, c := range []*Conn{p.client, server} {
			if c != nil {
				c.Abort(errTestOver)
			}
		}
	})
	for _, h := range []*testHandler{p.clientH, p.serverH.testHandler} {
		select {
		case <-h.handshake:
		case err := <-h.closed:
			t.Fatalf("closed during handshake: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("handshake timed out")
		}
	}
	// The server's goroutine set it before the handshake could end; the
	// lock makes that visible here.
	serverMu.Lock()
	server := p.server
	serverMu.Unlock()
	if server == nil {
		t.Fatal("no server connection")
	}
	return p
}

// echo has the client send msg on a stream of its own and waits for the
// server's echo of it.
func (p *rebindPair) echo(t *testing.T, msg string) {
	t.Helper()
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write([]byte(msg), true); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, p.clientH, s.ID())
	if got := p.clientH.received(s.ID()); string(got) != msg {
		t.Fatalf("echo %q, want %q", got, msg)
	}
}

// waitPath waits for the server to send on want, with the path validated.
func (p *rebindPair) waitPath(t *testing.T, want *serverPath) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		p.server.mu.Lock()
		on, validated := p.server.pc == PacketConn(want), p.server.pathValidated
		p.server.mu.Unlock()
		if on && validated {
			return
		}
	}
	t.Fatalf("the server does not send on %v, validated", want.addr)
}

// waitClosed waits for a path the server left to be closed, which it is
// once the connection's lock is let go.
func waitClosed(t *testing.T, path *serverPath) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !path.isClosed(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the path to %v the server left is still open", path.addr)
		}
	}
}

// waitChanges waits for the server's handler to have heard of n path
// changes, which it does after the change, once the calls are dispatched.
func (p *rebindPair) waitChanges(t *testing.T, n int32) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); p.serverH.changes.Load() != n; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d path changes, want %d", p.serverH.changes.Load(), n)
		}
	}
}

// A client whose address changes under it, as a NAT rebinding changes it,
// is followed there: the server sends on the new path once the client's
// latest packet arrives on it, validates it, and closes the old one.
func TestServerFollowsRebinding(t *testing.T) {
	p := newRebindPair(t)
	p.echo(t, "before")
	p.n.current.Store(p.b)
	p.echo(t, "across")
	p.waitPath(t, p.b)
	waitClosed(t, p.a)
	if got := p.server.RemoteAddr().String(); got != p.b.addr.String() {
		t.Fatalf("RemoteAddr %s, want %s", got, p.b.addr)
	}
	p.waitChanges(t, 1)
	p.echo(t, "after")
}

// A packet replayed from another address ahead of the client's own moves
// the server there, but the client's next packet on its own path moves it
// back, and the path the replay came from is left.
func TestServerReturnsFromReplayedPath(t *testing.T) {
	p := newRebindPair(t)
	p.echo(t, "before")
	p.b.black.Store(true)
	p.n.replay.Store(p.b)
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write([]byte("replayed"), true); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); !p.server.SendsOn(p.b); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the replay did not move the server")
		}
	}
	p.n.replay.Store(nil)
	waitFinished(t, p.clientH, s.ID())
	p.waitPath(t, p.a)
	waitClosed(t, p.b)
	if p.a.isClosed() {
		t.Fatal("the client's own path was closed")
	}
	p.echo(t, "after")
}

// A new path the client does not answer on is given up once its validation
// times out, and the server goes back to the path it left.
func TestServerGivesUpUnansweredPath(t *testing.T) {
	p := newRebindPair(t)
	p.echo(t, "before")
	p.b.black.Store(true)
	p.n.current.Store(p.b)
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write([]byte("unanswered"), true); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(5 * time.Second); !p.server.SendsOn(p.b); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the server did not move")
		}
	}
	// The client goes quiet, so nothing but the timer ends the validation.
	p.n.current.Store(nil)
	p.waitPath(t, p.a)
	waitClosed(t, p.b)
	p.waitChanges(t, 2)
	p.n.current.Store(p.a)
	waitFinished(t, p.clientH, s.ID())
	p.echo(t, "after")
}

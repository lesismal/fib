package utp

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
)

// maxOpenQueue bounds the datagrams held for an address while its fib
// connection opens; uTP sends again what is lost.
const maxOpenQueue = 64

// Socket is uTP on a fib UDP binding: a Mux whose datagrams fib reads,
// each burst of a peer's handed over together and answered with one
// batch (sendmmsg on Linux), and whose connections send through the fib
// connection of their peer's address, opening one with Engine.OpenUDPPeer
// for an address that has sent nothing yet. It accepts the connections
// peers start, as a net.Listener, and dials from the same port, as a
// BitTorrent client does.
//
// Datagrams that are not uTP go to the function OnOther sets, so that
// the DHT and UDP trackers can share the port; WriteTo sends theirs.
//
// Everything is keyed by address, never by fib connection: with fib's
// ReusePort a binding has a socket on every poller, and replies from one
// address may arrive on any of them.
type Socket struct {
	mux *Mux

	engine  *fib.Engine
	binding int
	owned   bool

	mu      sync.RWMutex
	closed  bool
	conns   map[netip.AddrPort]*fib.Connection
	opening map[netip.AddrPort][][]byte

	other   atomic.Pointer[func(from netip.AddrPort, b []byte)]
	closing atomic.Bool
}

// NewSocket returns a Socket to be the Handler of a UDP fib.Binding. It
// answers the peers that reach it at once; it dials, and sends to
// addresses that have sent it nothing, once Attach names its binding.
//
//	s := utp.NewSocket(utp.Config{})
//	engine, _ := fib.New(fib.DefaultConfig(),
//		fib.Binding{Network: "udp", Addr: ":6881", Handler: s})
//	s.Attach(engine, 0)
//	go engine.Run()
//
// Listen does that for an engine that runs already.
func NewSocket(cfg Config) *Socket {
	s := &Socket{
		binding: -1,
		conns:   make(map[netip.AddrPort]*fib.Connection),
		opening: make(map[netip.AddrPort][][]byte),
	}
	s.mux = NewMux(cfg, nil, s.send)
	return s
}

// Attach names the binding the Socket is the Handler of, the index of the
// Binding given to fib.New or returned by Engine.AddBinding, so that it can
// open connections to addresses that have sent it nothing.
func (s *Socket) Attach(engine *fib.Engine, binding int) {
	s.mu.Lock()
	s.engine, s.binding = engine, binding
	s.mu.Unlock()
	if addrs, err := engine.BindingAddrs(binding); err == nil && len(addrs) > 0 {
		s.mux.local.Store(&addrs[0])
	}
}

// Listen serves uTP at addr ("host:port") on a binding of network
// ("udp", "udp4" or "udp6") it adds to engine, which must be running;
// Close removes it. Listen waits for the engine's event loop, so it must
// not be called from a callback that loop runs.
func Listen(engine *fib.Engine, network, addr string, cfg Config) (*Socket, error) {
	s := NewSocket(cfg)
	i, err := engine.AddBinding(fib.Binding{Network: network, Addr: addr, Handler: s})
	if err != nil {
		return nil, err
	}
	s.owned = true
	s.Attach(engine, i)
	return s, nil
}

// Dial connects to the uTP peer at addr ("host:port") from a binding of
// its own that it adds to engine, which must be running, and removes once
// the connection ends. Dial waits for the engine's event loop, so it must
// not be called from a callback that loop runs.
func Dial(engine *fib.Engine, addr string, cfg Config) (*Conn, error) {
	return DialContext(context.Background(), engine, addr, cfg)
}

// DialContext is Dial with a context.
func DialContext(ctx context.Context, engine *fib.Engine, addr string, cfg Config) (*Conn, error) {
	to, err := resolve(ctx, addr)
	if err != nil {
		return nil, err
	}
	network := "udp4"
	if !to.Addr().Unmap().Is4() {
		network = "udp6"
	}
	cfg.AcceptBacklog = -1
	s, err := Listen(engine, network, ":0", cfg)
	if err != nil {
		return nil, err
	}
	c, err := s.mux.Dial(ctx, to)
	if err != nil {
		go s.Close()
		return nil, err
	}
	c.mu.Lock()
	gone := c.state == stateDone && !c.lingering
	if !gone {
		c.onGone = func() { go s.Close() }
	}
	c.mu.Unlock()
	if gone {
		go s.Close()
	}
	return c, nil
}

func resolve(ctx context.Context, addr string) (netip.AddrPort, error) {
	if ap, err := netip.ParseAddrPort(addr); err == nil {
		return ap, nil
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return netip.AddrPort{}, err
	}
	pn, err := net.DefaultResolver.LookupPort(ctx, "udp", port)
	if err != nil {
		return netip.AddrPort{}, err
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return netip.AddrPort{}, err
	}
	return netip.AddrPortFrom(ips[0].Unmap(), uint16(pn)), nil
}

// DialContext connects from the Socket's port to the uTP peer at addr
// ("host:port").
func (s *Socket) DialContext(ctx context.Context, addr string) (*Conn, error) {
	to, err := resolve(ctx, addr)
	if err != nil {
		return nil, err
	}
	return s.mux.Dial(ctx, to)
}

// DialAddrPort connects from the Socket's port to the uTP peer at to.
func (s *Socket) DialAddrPort(ctx context.Context, to netip.AddrPort) (*Conn, error) {
	return s.mux.Dial(ctx, to)
}

// Mux returns the Socket's Mux.
func (s *Socket) Mux() *Mux { return s.mux }

// Accept waits for the next connection a peer starts, as net.Listener.
func (s *Socket) Accept() (net.Conn, error) { return s.mux.Accept() }

// AcceptUTP is Accept returning the *Conn.
func (s *Socket) AcceptUTP() (*Conn, error) { return s.mux.AcceptUTP() }

// Addr returns the binding's address.
func (s *Socket) Addr() net.Addr { return s.mux.Addr() }

// OnOther sets the function given the datagrams that are not uTP, on the
// fib worker that read them, valid until it returns; nil drops them.
func (s *Socket) OnOther(fn func(from netip.AddrPort, b []byte)) {
	if fn == nil {
		s.other.Store(nil)
		return
	}
	s.other.Store(&fn)
}

// WriteTo sends b to the peer at to as a datagram of its own, for the
// protocols that share the port.
func (s *Socket) WriteTo(b []byte, to netip.AddrPort) error {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return net.ErrClosed
	}
	s.send(unmap(to), [][]byte{b})
	return nil
}

// Close resets every connection, and removes the binding Listen added.
func (s *Socket) Close() error {
	if !s.closing.CompareAndSwap(false, true) {
		return nil
	}
	// The connections' RESETs go out before the fib connections are
	// forgotten.
	_ = s.mux.Close()
	s.mu.Lock()
	s.closed = true
	clear(s.conns)
	s.mu.Unlock()
	if s.owned {
		return s.engine.RemoveBinding(s.binding)
	}
	return nil
}

// send sends the datagrams of a connection, or of WriteTo.
func (s *Socket) send(to netip.AddrPort, dgrams [][]byte) {
	s.mu.RLock()
	c := s.conns[to]
	engine := s.engine
	closed := s.closed
	s.mu.RUnlock()
	if c != nil {
		_, err := c.SendDatagrams(dgrams)
		if err == nil || (!errors.Is(err, net.ErrClosed) && !errors.Is(err, syscall.EPIPE)) {
			return
		}
		s.mu.Lock()
		if s.conns[to] == c {
			delete(s.conns, to)
		}
		s.mu.Unlock()
	}
	if engine == nil || closed {
		return
	}
	s.mu.Lock()
	q, opening := s.opening[to]
	for _, d := range dgrams {
		if len(q) < maxOpenQueue {
			q = append(q, append([]byte(nil), d...))
		}
	}
	s.opening[to] = q
	s.mu.Unlock()
	if !opening {
		// OpenUDPPeer waits for the socket's loop, which may be running
		// the callback that sends.
		go s.open(to)
	}
}

func (s *Socket) open(to netip.AddrPort) {
	c, err := s.engine.OpenUDPPeer(s.binding, to)
	s.mu.Lock()
	q := s.opening[to]
	delete(s.opening, to)
	if err == nil && !s.closed {
		if old := s.conns[to]; old != nil {
			c = old
		} else {
			s.conns[to] = c
		}
	}
	s.mu.Unlock()
	if err == nil && len(q) > 0 {
		_, _ = c.SendDatagrams(q)
	}
}

// remember keeps c as the connection of its address.
func (s *Socket) remember(c *fib.Connection, from netip.AddrPort) {
	s.mu.RLock()
	known := s.conns[from] == c
	s.mu.RUnlock()
	if known {
		return
	}
	s.mu.Lock()
	if !s.closed {
		s.conns[from] = c
	}
	s.mu.Unlock()
}

// OnOpen is part of fib.Handler.
func (s *Socket) OnOpen(*fib.Connection) {}

// OnPriorityData is part of fib.Handler.
func (s *Socket) OnPriorityData(*fib.Connection, []byte) {}

// OnData reads a datagram.
func (s *Socket) OnData(c *fib.Connection, b []byte) {
	from := unmap(c.RemoteAddrPort())
	s.remember(c, from)
	if !s.mux.HandleDatagram(from, b) {
		if fn := s.other.Load(); fn != nil {
			(*fn)(from, b)
		}
	}
	s.mux.Flush()
}

// OnDatagrams reads a burst of a peer's datagrams, and answers it once.
func (s *Socket) OnDatagrams(c *fib.Connection, dgrams [][]byte) {
	from := unmap(c.RemoteAddrPort())
	s.remember(c, from)
	other := s.other.Load()
	for _, d := range dgrams {
		if !s.mux.HandleDatagram(from, d) && other != nil {
			(*other)(from, d)
		}
		bufferpool.Put(d)
	}
	s.mux.Flush()
}

// OnClose forgets a connection fib closed; the next datagram for its
// address opens another.
func (s *Socket) OnClose(c *fib.Connection, _ error) {
	from := unmap(c.RemoteAddrPort())
	s.mu.Lock()
	if s.conns[from] == c {
		delete(s.conns, from)
	}
	s.mu.Unlock()
}

// OnEngineClose resets the uTP connections of a peer the engine is
// closing the fib connection of, while it can still tell the peer.
func (s *Socket) OnEngineClose(c *fib.Connection) {
	s.mux.abortAddr(unmap(c.RemoteAddrPort()), net.ErrClosed)
}

var (
	_ fib.Handler            = (*Socket)(nil)
	_ fib.DatagramsHandler   = (*Socket)(nil)
	_ fib.EngineCloseHandler = (*Socket)(nil)
	_ net.Listener           = (*Socket)(nil)
)

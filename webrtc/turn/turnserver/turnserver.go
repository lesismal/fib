//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Package turnserver runs a TURN server (package turn's Server) on a fib
// engine: the listening address is a UDP binding whose handler is the
// Server, and each allocation's relayed address is a UDP binding of its own
// that the Server adds with Engine.AddBinding and removes when the
// allocation ends. Datagrams go to clients and peers through the
// connections of those bindings, opened with Engine.OpenUDPPeer when the
// peer has not spoken first.
//
// A Server is meant to be embedded in a larger service, a WebRTC gateway
// say, that shares the engine with other protocols: everything it does is
// set by Config, whose turn.ServerConfig carries the realm, the AuthFunc,
// the lifetimes, the quotas and the bandwidth hook, and whose own fields
// say where relayed addresses are opened.
//
// The turn.Server core is not safe for concurrent use, and fib runs the
// handlers of different connections on different workers, so a mutex
// serializes every call into it; datagrams are sent after it is released.
package turnserver

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/webrtc/turn"
)

// Config configures a Server.
type Config struct {
	// ServerConfig is the TURN server's: Realm and Auth are required, and
	// Send and AllocateRelay must be left nil, since the Server sets them.
	turn.ServerConfig

	// RelayIP is the address relayed sockets bind to. It must be a
	// specific address, not a wildcard, since it is what clients are
	// told unless PublicIP is set; its family is the family of every
	// allocation.
	RelayIP netip.Addr
	// PublicIP, if set, is the address clients are told instead, for a
	// server behind a 1:1 NAT that maps PublicIP to RelayIP.
	PublicIP netip.Addr
	// PortMin and PortMax bound the relayed ports, as a firewall that
	// opens a range expects. Zero leaves the choice of each port to the
	// kernel.
	PortMin, PortMax uint16
}

// Server is a TURN server running on a fib engine. It is the fib.Handler of
// the listening UDP binding.
type Server struct {
	cfg  Config
	core *turn.Server

	// mu serializes the core, and guards out, which collects the
	// datagrams the core emits until mu is released.
	mu  sync.Mutex
	out []datagram

	engine  *fib.Engine
	binding int

	// connMu guards the connection tables, which handlers running on the
	// event loop (OnOpen, OnClose) touch too: it is never held while
	// waiting for the loop.
	connMu  sync.Mutex
	clients map[netip.AddrPort]*fib.Connection
	locals  map[*fib.Connection]netip.AddrPort

	done chan struct{}
	once sync.Once
	wg   sync.WaitGroup
}

// datagram is one the core emitted: to a client from the listening
// binding if relay is nil, and to a peer from relay's binding otherwise.
type datagram struct {
	relay *relay
	to    netip.AddrPort
	b     []byte
}

var errBadConfig = errors.New("turnserver: Config.Send and Config.AllocateRelay are set by the Server")

// New returns a Server configured by config. It does nothing until Start
// gives it the engine it runs on.
func New(config Config) (*Server, error) {
	if config.Send != nil || config.AllocateRelay != nil {
		return nil, errBadConfig
	}
	if !config.RelayIP.IsValid() || config.RelayIP.IsUnspecified() {
		return nil, errors.New("turnserver: Config.RelayIP must be a specific address")
	}
	if config.PortMin > config.PortMax {
		return nil, errors.New("turnserver: Config.PortMin exceeds PortMax")
	}
	s := &Server{
		cfg:     config,
		clients: make(map[netip.AddrPort]*fib.Connection),
		locals:  make(map[*fib.Connection]netip.AddrPort),
		done:    make(chan struct{}),
	}
	sc := config.ServerConfig
	sc.Send = s.sendToClient
	sc.AllocateRelay = s.allocateRelay
	core, err := turn.NewServer(sc)
	if err != nil {
		return nil, err
	}
	s.core = core
	return s, nil
}

// Listen starts a Server on engine at addr, a "host:port" for a new UDP
// binding of engine's.
func Listen(engine *fib.Engine, addr string, config Config) (*Server, error) {
	s, err := New(config)
	if err != nil {
		return nil, err
	}
	i, err := engine.AddBinding(fib.Binding{Network: "udp", Addr: addr, Handler: s})
	if err != nil {
		return nil, err
	}
	s.Start(engine, i)
	return s, nil
}

// Start has s serve on engine, whose UDP binding at index binding has s as
// its handler, and starts the timer that frees expired allocations. Datagrams
// that arrive before Start are dropped.
func (s *Server) Start(engine *fib.Engine, binding int) {
	s.mu.Lock()
	s.engine, s.binding = engine, binding
	s.mu.Unlock()
	s.wg.Add(1)
	go s.timer()
}

func (s *Server) timer() {
	defer s.wg.Done()
	interval := s.cfg.SweepInterval
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case now := <-t.C:
			s.mu.Lock()
			s.core.HandleTimeout(now)
			out := s.takeOut()
			s.mu.Unlock()
			s.flush(out)
		}
	}
}

// Close stops the server: it frees every allocation, removing its relayed
// binding. The listening binding is the caller's to remove.
func (s *Server) Close() {
	s.once.Do(func() {
		close(s.done)
		s.wg.Wait()
		s.mu.Lock()
		s.core.Close()
		s.mu.Unlock()
	})
}

// Allocations is the number of allocations the server holds.
func (s *Server) Allocations() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.core.Allocations()
}

func (s *Server) takeOut() []datagram {
	out := s.out
	s.out = nil
	return out
}

func (s *Server) sendToClient(t turn.FiveTuple, b []byte) {
	s.out = append(s.out, datagram{to: t.Client, b: append([]byte(nil), b...)})
}

// flush sends the datagrams the core emitted, opening the connection of a
// client or peer that has none on its binding.
func (s *Server) flush(out []datagram) {
	for _, d := range out {
		var c *fib.Connection
		if d.relay == nil {
			c = s.conn(s.clients, s.binding, d.to)
		} else {
			c = s.conn(d.relay.peers, d.relay.binding, d.to)
		}
		if c != nil {
			_ = c.SendOwned(d.b)
		}
	}
}

func (s *Server) conn(table map[netip.AddrPort]*fib.Connection, binding int, to netip.AddrPort) *fib.Connection {
	s.connMu.Lock()
	c := table[to]
	s.connMu.Unlock()
	if c != nil {
		return c
	}
	// OpenUDPPeer waits for the loop, whose OnOpen takes connMu: it is
	// called with connMu released, and its OnOpen records the connection.
	c, err := s.engine.OpenUDPPeer(binding, to)
	if err != nil {
		return nil
	}
	return c
}

// remote is c's peer address, unmapped as the core keys it.
func remote(c *fib.Connection) netip.AddrPort {
	a := c.RemoteAddrPort()
	return netip.AddrPortFrom(a.Addr().Unmap(), a.Port())
}

func addrPort(a net.Addr) netip.AddrPort {
	if u, ok := a.(*net.UDPAddr); ok {
		ap := u.AddrPort()
		return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
	}
	return netip.AddrPort{}
}

// OnOpen records a client's connection, under its address: with
// Config.ReusePort the same client may have one on each poller's socket,
// and the latest serves for sending.
func (s *Server) OnOpen(c *fib.Connection) {
	local := addrPort(c.LocalAddr())
	s.connMu.Lock()
	s.clients[remote(c)] = c
	s.locals[c] = local
	s.connMu.Unlock()
}

// OnData passes a client's datagram to the core.
func (s *Server) OnData(c *fib.Connection, b []byte) {
	s.connMu.Lock()
	local, ok := s.locals[c]
	if !ok {
		local = addrPort(c.LocalAddr())
		s.locals[c] = local
		s.clients[remote(c)] = c
	}
	s.connMu.Unlock()
	s.mu.Lock()
	if s.engine == nil {
		s.mu.Unlock()
		return
	}
	_ = s.core.HandlePacket(turn.FiveTuple{Client: remote(c), Server: local}, b, time.Now())
	out := s.takeOut()
	s.mu.Unlock()
	s.flush(out)
}

// OnPriorityData is not used by UDP.
func (s *Server) OnPriorityData(*fib.Connection, []byte) {}

// OnClose forgets a client's connection.
func (s *Server) OnClose(c *fib.Connection, _ error) {
	s.connMu.Lock()
	if s.clients[remote(c)] == c {
		delete(s.clients, remote(c))
	}
	delete(s.locals, c)
	s.connMu.Unlock()
}

// relay is one relayed address: a UDP binding whose handler it is.
type relay struct {
	s       *Server
	binding int
	addr    netip.AddrPort
	peers   map[netip.AddrPort]*fib.Connection // guarded by s.connMu
	closed  bool                               // guarded by s.mu
}

// allocateRelay opens a relayed address, binding a port in the configured
// range or one the kernel picks. It runs inside the core, under s.mu, on
// the worker of the client's datagram.
func (s *Server) allocateRelay(_ turn.FiveTuple, _ string, ipv6 bool) (turn.Relay, error) {
	if s.engine == nil || ipv6 != s.cfg.RelayIP.Is6() {
		return nil, turn.ErrNoRelay
	}
	r := &relay{s: s, peers: make(map[netip.AddrPort]*fib.Connection)}
	try := func(port uint16) error {
		i, err := s.engine.AddBinding(fib.Binding{
			Network: "udp",
			Addr:    net.JoinHostPort(s.cfg.RelayIP.String(), strconv.Itoa(int(port))),
			Handler: r,
		})
		if err != nil {
			return err
		}
		addrs, err := s.engine.BindingAddrs(i)
		if err != nil || len(addrs) == 0 {
			_ = s.engine.RemoveBinding(i)
			return fmt.Errorf("turnserver: relay binding has no address: %w", err)
		}
		r.binding = i
		r.addr = addrPort(addrs[0])
		if s.cfg.PublicIP.IsValid() {
			r.addr = netip.AddrPortFrom(s.cfg.PublicIP.Unmap(), r.addr.Port())
		}
		return nil
	}
	if s.cfg.PortMax == 0 {
		if err := try(0); err != nil {
			return nil, err
		}
		return r, nil
	}
	// A random start spreads allocations over the range, so a port just
	// freed, which a peer may still be sending to, is not reused at once.
	n := int(s.cfg.PortMax-s.cfg.PortMin) + 1
	start := rand.IntN(n)
	for k := range n {
		if try(s.cfg.PortMin+uint16((start+k)%n)) == nil {
			return r, nil
		}
	}
	return nil, turn.ErrNoRelay
}

func (r *relay) Addr() netip.AddrPort { return r.addr }

func (r *relay) SendTo(peer netip.AddrPort, b []byte) {
	r.s.out = append(r.s.out, datagram{relay: r, to: peer, b: append([]byte(nil), b...)})
}

// Close removes the relay's binding. The core calls it under s.mu, perhaps
// on the worker running OnData for this very binding, so the removal, which
// closes its sockets and connections, runs on a goroutine of its own.
func (r *relay) Close() {
	if r.closed {
		return
	}
	r.closed = true
	e, i := r.s.engine, r.binding
	go func() { _ = e.RemoveBinding(i) }()
}

func (r *relay) OnOpen(c *fib.Connection) {
	r.s.connMu.Lock()
	r.peers[remote(c)] = c
	r.s.connMu.Unlock()
}

func (r *relay) OnData(c *fib.Connection, b []byte) {
	s := r.s
	s.mu.Lock()
	if r.closed {
		s.mu.Unlock()
		return
	}
	_ = s.core.HandleRelayPacket(r.addr, remote(c), b, time.Now())
	out := s.takeOut()
	s.mu.Unlock()
	s.flush(out)
}

func (r *relay) OnPriorityData(*fib.Connection, []byte) {}

func (r *relay) OnClose(c *fib.Connection, _ error) {
	r.s.connMu.Lock()
	if r.peers[remote(c)] == c {
		delete(r.peers, remote(c))
	}
	r.s.connMu.Unlock()
}

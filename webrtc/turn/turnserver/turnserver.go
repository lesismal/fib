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
// A Server never makes a fib worker wait. The turn.Server core is safe for
// concurrent use and relays the datagrams of different allocations in
// parallel, so a worker's OnData runs the core and sends what it emits
// without a lock any other allocation takes; sends are fib's, which queue
// rather than wait. What does wait, Engine.AddBinding to open a relayed
// address and Engine.OpenUDPPeer to reach a peer that has not spoken first,
// runs on a few goroutines of the Server's own, fed by a bounded queue
// (Config.SetupWorkers and SetupQueue): an Allocate is answered once its
// relayed address is bound, and the datagrams for a peer being opened wait
// for it in a short queue of their own. A queue that is full refuses the
// allocation with 508 or drops the datagram rather than wait for room.
//
// A reload changes the core's limits and credentials in place, through
// Core().SetLimits and SetAuth; a server being replaced is stopped with
// Shutdown, which lets its allocations end before it closes.
package turnserver

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/webrtc/turn"
)

// Defaults of the Config fields that bound the setup queue.
const (
	DefaultSetupWorkers = 4
	DefaultSetupQueue   = 1024
)

// maxPeerBacklog is how many datagrams wait for a peer's connection to
// open; more are dropped, as a congested link would.
const maxPeerBacklog = 16

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

	// SetupWorkers is the number of goroutines that bind relayed
	// addresses and open peers' connections, the calls that wait for the
	// engine's loops, DefaultSetupWorkers if zero; SetupQueue bounds the
	// work waiting for them, DefaultSetupQueue if zero.
	SetupWorkers, SetupQueue int
}

// Server is a TURN server running on a fib engine. It is the fib.Handler of
// the listening UDP binding.
type Server struct {
	cfg  Config
	core *turn.Server

	// run is the engine and listening binding Start gave, nil before.
	run atomic.Pointer[running]

	// clients are the connections to clients by their 5-tuple, which
	// OnOpen records on the event loop and OnClose forgets.
	clients sync.Map // turn.FiveTuple -> *fib.Connection

	setup chan func()
	// unsent counts the datagrams dropped for want of a connection to send
	// them on: its open failed, or the setup queue was full.
	unsent atomic.Uint64

	done      chan struct{}
	startOnce sync.Once
	once      sync.Once
	wg        sync.WaitGroup
}

type running struct {
	engine  *fib.Engine
	binding int
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
	if config.SetupWorkers <= 0 {
		config.SetupWorkers = DefaultSetupWorkers
	}
	if config.SetupQueue <= 0 {
		config.SetupQueue = DefaultSetupQueue
	}
	s := &Server{
		cfg:   config,
		setup: make(chan func(), config.SetupQueue),
		done:  make(chan struct{}),
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
// its handler, and starts the timer that frees expired allocations and the
// goroutines that open relayed addresses. Datagrams that arrive before Start
// are dropped. Only the first call counts.
func (s *Server) Start(engine *fib.Engine, binding int) {
	s.startOnce.Do(func() {
		s.wg.Add(1 + s.cfg.SetupWorkers)
		go s.timer()
		for range s.cfg.SetupWorkers {
			go s.worker()
		}
		s.run.Store(&running{engine: engine, binding: binding})
	})
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
			s.core.HandleTimeout(now)
		}
	}
}

func (s *Server) worker() {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			return
		case fn := <-s.setup:
			fn()
		}
	}
}

// enqueue hands fn to the setup goroutines, and reports false, without
// waiting, if their queue is full or the server has closed.
func (s *Server) enqueue(fn func()) bool {
	select {
	case <-s.done:
		return false
	default:
	}
	select {
	case s.setup <- fn:
		return true
	default:
		return false
	}
}

// Close stops the server: it frees every allocation, removing its relayed
// binding. The listening binding is the caller's to remove.
func (s *Server) Close() {
	s.once.Do(func() {
		close(s.done)
		s.wg.Wait()
		s.core.Close()
	})
}

// Shutdown stops the server gracefully: it drains the core, which refuses
// new allocations with 508 while those held go on relaying and refreshing,
// waits until none is left, or ctx is done, and then closes the server as
// Close does. It returns ctx.Err() if ctx ended the wait, and nil
// otherwise. Lifetimes, quotas and credentials are changed in place
// through Core's SetLimits and SetAuth, with no need to shut down.
func (s *Server) Shutdown(ctx context.Context) error {
	s.core.Drain()
	t := time.NewTicker(shutdownPoll)
	defer t.Stop()
	var err error
	for {
		if st := s.core.Stats(); st.Allocations+st.Pending == 0 {
			break
		}
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-t.C:
			continue
		}
		break
	}
	s.Close()
	return err
}

// shutdownPoll is how often Shutdown looks for allocations left.
const shutdownPoll = 20 * time.Millisecond

// Allocations is the number of allocations the server holds.
func (s *Server) Allocations() int { return s.core.Allocations() }

// Core is the turn.Server s runs, for what Config does not cover.
func (s *Server) Core() *turn.Server { return s.core }

// Unsent is the number of datagrams dropped for want of a connection to
// send them on: one that could not be opened, or a setup queue too full to
// open it.
func (s *Server) Unsent() uint64 { return s.unsent.Load() }

func (s *Server) sendToClient(t turn.FiveTuple, b []byte) {
	if c, ok := s.clients.Load(t); ok {
		_ = c.(*fib.Connection).Send(b)
		return
	}
	// A client whose connection fib closed for idleness: open it again,
	// off the worker.
	r := s.run.Load()
	if r == nil {
		return
	}
	d := append([]byte(nil), b...)
	if !s.enqueue(func() {
		c, err := r.engine.OpenUDPPeer(r.binding, t.Client)
		if err != nil {
			s.unsent.Add(1)
			return
		}
		_ = c.SendOwned(d)
	}) {
		s.unsent.Add(1)
	}
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

// tupleOf is the 5-tuple of a client's connection, kept as its attachment.
func tupleOf(c *fib.Connection) turn.FiveTuple {
	if t, ok := c.Attachment().(turn.FiveTuple); ok {
		return t
	}
	return turn.FiveTuple{Client: remote(c), Server: addrPort(c.LocalAddr())}
}

// OnOpen records a client's connection, under its 5-tuple: with
// Config.ReusePort the same client may have one on each poller's socket,
// and the latest serves for sending.
func (s *Server) OnOpen(c *fib.Connection) {
	t := tupleOf(c)
	c.SetAttachment(t)
	s.clients.Store(t, c)
}

// OnData passes a client's datagram to the core.
func (s *Server) OnData(c *fib.Connection, b []byte) {
	if s.run.Load() == nil {
		return
	}
	_ = s.core.HandlePacket(tupleOf(c), b, time.Now())
}

// OnPriorityData is not used by UDP.
func (s *Server) OnPriorityData(*fib.Connection, []byte) {}

// OnClose forgets a client's connection.
func (s *Server) OnClose(c *fib.Connection, _ error) {
	s.clients.CompareAndDelete(tupleOf(c), c)
}

// relay is one relayed address: a UDP binding whose handler it is.
type relay struct {
	s       *Server
	binding int
	addr    netip.AddrPort
	closed  atomic.Bool

	// mu guards peers, the connections to peers by address, and backlog,
	// the datagrams for peers whose connections are being opened. It is
	// the allocation's alone, and never held while waiting.
	mu      sync.Mutex
	peers   map[netip.AddrPort]*fib.Connection
	backlog map[netip.AddrPort][][]byte
}

// allocateRelay opens a relayed address for the core, on a setup
// goroutine, and hands it to done there.
func (s *Server) allocateRelay(_ turn.FiveTuple, _ string, ipv6 bool, done func(turn.Relay, error)) {
	r := s.run.Load()
	if r == nil || ipv6 != s.cfg.RelayIP.Is6() {
		done(nil, turn.ErrNoRelay)
		return
	}
	if !s.enqueue(func() {
		rl, err := s.bindRelay(r.engine)
		if err != nil {
			done(nil, err)
			return
		}
		done(rl, nil)
	}) {
		done(nil, turn.ErrNoRelay)
	}
}

// bindRelay opens a relayed address, binding a port in the configured
// range or one the kernel picks. It waits for the engine's loops, so it
// runs on a setup goroutine.
func (s *Server) bindRelay(engine *fib.Engine) (*relay, error) {
	r := &relay{s: s, peers: make(map[netip.AddrPort]*fib.Connection), backlog: make(map[netip.AddrPort][][]byte)}
	try := func(port uint16) error {
		i, err := engine.AddBinding(fib.Binding{
			Network: "udp",
			Addr:    net.JoinHostPort(s.cfg.RelayIP.String(), strconv.Itoa(int(port))),
			Handler: r,
		})
		if err != nil {
			return err
		}
		addrs, err := engine.BindingAddrs(i)
		if err != nil || len(addrs) == 0 {
			_ = engine.RemoveBinding(i)
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

// SendTo sends b to peer on the peer's connection, or, when the peer has
// none yet, queues it and has a setup goroutine open one.
func (r *relay) SendTo(peer netip.AddrPort, b []byte) {
	r.mu.Lock()
	c := r.peers[peer]
	if c == nil {
		q, opening := r.backlog[peer]
		if len(q) < maxPeerBacklog {
			r.backlog[peer] = append(q, append([]byte(nil), b...))
		} else {
			r.s.unsent.Add(1)
		}
		r.mu.Unlock()
		if !opening && !r.s.enqueue(func() { r.open(peer) }) {
			r.mu.Lock()
			r.s.unsent.Add(uint64(len(r.backlog[peer])))
			delete(r.backlog, peer)
			r.mu.Unlock()
		}
		return
	}
	r.mu.Unlock()
	_ = c.Send(b)
}

// open opens the connection to peer on the relay's binding, on a setup
// goroutine, and sends what waited for it.
func (r *relay) open(peer netip.AddrPort) {
	var c *fib.Connection
	err := net.ErrClosed
	if !r.closed.Load() {
		c, err = r.s.run.Load().engine.OpenUDPPeer(r.binding, peer)
	}
	r.mu.Lock()
	q := r.backlog[peer]
	delete(r.backlog, peer)
	if err == nil && r.peers[peer] == nil {
		r.peers[peer] = c
	}
	r.mu.Unlock()
	if err != nil {
		r.s.unsent.Add(uint64(len(q)))
		return
	}
	for _, d := range q {
		_ = c.SendOwned(d)
	}
}

// Close removes the relay's binding. The core may call it on the worker
// running OnData for this very binding, so the removal, which closes its
// sockets and connections, runs on a goroutine of its own.
func (r *relay) Close() {
	if !r.closed.CompareAndSwap(false, true) {
		return
	}
	e, i := r.s.run.Load().engine, r.binding
	go func() { _ = e.RemoveBinding(i) }()
}

func (r *relay) OnOpen(c *fib.Connection) {
	r.mu.Lock()
	r.peers[remote(c)] = c
	r.mu.Unlock()
}

func (r *relay) OnData(c *fib.Connection, b []byte) {
	if r.closed.Load() {
		return
	}
	_ = r.s.core.HandleRelayPacket(r.addr, remote(c), b, time.Now())
}

func (r *relay) OnPriorityData(*fib.Connection, []byte) {}

func (r *relay) OnClose(c *fib.Connection, _ error) {
	r.mu.Lock()
	if r.peers[remote(c)] == c {
		delete(r.peers, remote(c))
	}
	r.mu.Unlock()
}

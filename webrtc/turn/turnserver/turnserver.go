//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

// Package turnserver runs a TURN server (package turn's Server) on a fib
// engine: the listening address is a UDP binding whose handler is the
// Server, and each allocation's relayed address is a UDP binding of its own
// that the Server adds with Engine.AddBinding and removes when the
// allocation ends. Datagrams go to clients and peers through the
// connections of those bindings, opened with Engine.OpenUDPPeer when the
// peer has not spoken first. Clients may also reach the server over TCP or
// TLS (ListenTCP, ListenTLS, StreamHandler), which RFC 8656 has a client
// behind a firewall that lets only those through do; their allocations
// still relay UDP, and end when their connection closes.
//
// A Server is meant to be embedded in a larger service, a WebRTC gateway
// say, that shares the engine with other protocols: everything it does is
// set by Config, whose turn.ServerConfig carries the realm, the AuthFunc,
// the lifetimes, the quotas, the peer policy and the bandwidth and event
// hooks, and whose own fields say where relayed addresses are opened.
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
	stdtls "crypto/tls"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/webrtc/turn"
)

// Defaults of the Config fields that bound the setup queue.
const (
	DefaultSetupWorkers = 4
	DefaultSetupQueue   = 1024
	// DefaultMaxStreamMessage and DefaultStreamIdleTimeout are those of
	// Config's stream bounds.
	DefaultMaxStreamMessage  = 8 << 10
	DefaultStreamIdleTimeout = 30 * time.Second
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
	// kernel. A port of the range that another socket holds, another
	// allocation's relayed address or anything else's, is skipped, though
	// the engine binds with SO_REUSEPORT (Config.ReusePort) and the kernel
	// would let the relay share it.
	PortMin, PortMax uint16

	// SetupWorkers is the number of goroutines that bind relayed
	// addresses and open peers' connections, the calls that wait for the
	// engine's loops, DefaultSetupWorkers if zero; SetupQueue bounds the
	// work waiting for them, DefaultSetupQueue if zero.
	SetupWorkers, SetupQueue int

	// MaxStreamMessage is the largest message a TCP or TLS client may
	// send, and so what its connection holds of one not yet whole,
	// DefaultMaxStreamMessage if zero; a connection whose next message
	// claims more is closed. TURN's messages are small, a ChannelData
	// message at most the 4-byte header and a datagram.
	MaxStreamMessage int
	// StreamIdleTimeout is how long a TCP or TLS client's connection may
	// stay open without an allocation, before its first Allocate or after
	// its allocation ended, DefaultStreamIdleTimeout if zero. It is checked
	// every SweepInterval.
	StreamIdleTimeout time.Duration
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
	// streams are the TCP and TLS clients' connections, for the idle
	// sweep and Close.
	streams sync.Map // *fib.Connection -> *streamConn

	setup chan func()
	// ports are the ports of the range that this server's relays hold or
	// are binding, under portsMu: two setup goroutines must not both find a
	// port free and bind it, which SO_REUSEPORT would let them.
	portsMu sync.Mutex
	ports   map[uint16]struct{}
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
	if config.MaxStreamMessage <= 0 {
		config.MaxStreamMessage = DefaultMaxStreamMessage
	}
	if config.StreamIdleTimeout <= 0 {
		config.StreamIdleTimeout = DefaultStreamIdleTimeout
	}
	s := &Server{
		cfg:   config,
		setup: make(chan func(), config.SetupQueue),
		done:  make(chan struct{}),
	}
	sc := config.ServerConfig
	// The relay's own addresses are reachable through it on relayed ports
	// alone under the default peer policy.
	sc.OwnAddrs = append(slices.Clip(sc.OwnAddrs), config.RelayIP)
	if config.PublicIP.IsValid() {
		sc.OwnAddrs = append(sc.OwnAddrs, config.PublicIP)
	}
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
			s.sweepStreams(now)
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
// binding, and closes its TCP and TLS clients' connections. The listening
// bindings are the caller's to remove.
func (s *Server) Close() {
	s.once.Do(func() {
		close(s.done)
		s.wg.Wait()
		s.core.Close()
		s.streams.Range(func(c, _ any) bool {
			_ = c.(*fib.Connection).Close()
			return true
		})
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
	if t.Transport != turn.TransportUDP {
		// A stream client's connection has closed: nothing reaches it.
		s.unsent.Add(1)
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
	var ap netip.AddrPort
	switch a := a.(type) {
	case *net.UDPAddr:
		ap = a.AddrPort()
	case *net.TCPAddr:
		ap = a.AddrPort()
	default:
		return ap
	}
	return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
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
	// port is the port of the range the relay holds in s.ports, or 0 for
	// one the kernel picked.
	port   uint16
	closed atomic.Bool
	// ready is set once binding and addr are: a datagram a peer sends to
	// a port just bound may come before then, and is dropped.
	ready atomic.Bool

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
		addr := net.JoinHostPort(s.cfg.RelayIP.String(), strconv.Itoa(int(port)))
		if port != 0 {
			if err := s.claimPort(port, addr); err != nil {
				return err
			}
		}
		i, err := engine.AddBinding(fib.Binding{Network: "udp", Addr: addr, Handler: r})
		if err != nil {
			s.releasePort(port)
			return err
		}
		addrs, err := engine.BindingAddrs(i)
		if err != nil || len(addrs) == 0 {
			_ = engine.RemoveBinding(i)
			s.releasePort(port)
			return fmt.Errorf("turnserver: relay binding has no address: %w", err)
		}
		r.binding, r.port = i, port
		r.addr = addrPort(addrs[0])
		if s.cfg.PublicIP.IsValid() {
			r.addr = netip.AddrPortFrom(s.cfg.PublicIP.Unmap(), r.addr.Port())
		}
		r.ready.Store(true)
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

// errPortHeld is what claimPort reports for a port another of the server's
// relays holds or is binding.
var errPortHeld = errors.New("turnserver: relayed port held by another allocation")

// claimPort reserves port of the range for a relay about to bind addr, and
// fails if another relay of the server holds it or a socket of anything
// else does. The engine binds with SO_REUSEPORT, so that its pollers each
// read an address's datagrams, and the kernel would let the relay join a
// socket on the port that sets it too, splitting the datagrams of both
// between them: a bind without it, which such a socket refuses, tells. The
// reservation covers the window between that bind and the relay's.
func (s *Server) claimPort(port uint16, addr string) error {
	s.portsMu.Lock()
	if _, ok := s.ports[port]; ok {
		s.portsMu.Unlock()
		return errPortHeld
	}
	if s.ports == nil {
		s.ports = make(map[uint16]struct{})
	}
	s.ports[port] = struct{}{}
	s.portsMu.Unlock()
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		s.releasePort(port)
		return err
	}
	_ = pc.Close()
	return nil
}

// releasePort gives back a port claimPort reserved; 0 is none.
func (s *Server) releasePort(port uint16) {
	if port == 0 {
		return
	}
	s.portsMu.Lock()
	delete(s.ports, port)
	s.portsMu.Unlock()
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
	go func() {
		_ = e.RemoveBinding(i)
		// Only once its sockets are closed may another relay bind it.
		r.s.releasePort(r.port)
	}()
}

func (r *relay) OnOpen(c *fib.Connection) {
	r.mu.Lock()
	r.peers[remote(c)] = c
	r.mu.Unlock()
}

func (r *relay) OnData(c *fib.Connection, b []byte) {
	if !r.ready.Load() || r.closed.Load() {
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

// streamConn is what a TCP or TLS client's connection keeps: its 5-tuple,
// the bytes of a message not yet whole, and since when it has had no
// allocation, in Unix nanoseconds.
type streamConn struct {
	tuple     turn.FiveTuple
	buf       []byte
	idleSince atomic.Int64
}

// streamHandler serves TURN over TCP, or over TLS behind fib's tls
// handler: a connection is one client, whose allocation ends when it
// closes.
type streamHandler struct{ s *Server }

// StreamHandler returns the fib.Handler that serves TURN over a TCP
// binding of the engine s runs on, or over TLS, wrapped in fib's tls
// handler: fibtls.NewServer(config, s.StreamHandler()). ListenTCP and
// ListenTLS add such bindings.
func (s *Server) StreamHandler() fib.Handler { return streamHandler{s} }

// ListenTCP has s serve TURN over TCP at addr, a "host:port" for a new TCP
// binding of the engine Start gave it, and returns the binding's index,
// which is the caller's to remove.
func (s *Server) ListenTCP(addr string) (int, error) {
	return s.listenStream(addr, s.StreamHandler())
}

// ListenTLS has s serve TURN over TLS with config at addr, as ListenTCP
// does over TCP; browsers reach such a server as turns:host:443.
func (s *Server) ListenTLS(addr string, config *stdtls.Config) (int, error) {
	return s.listenStream(addr, fibtls.NewServer(config, s.StreamHandler()))
}

func (s *Server) listenStream(addr string, h fib.Handler) (int, error) {
	r := s.run.Load()
	if r == nil {
		return -1, errNotStarted
	}
	return r.engine.AddBinding(fib.Binding{Network: "tcp", Addr: addr, Handler: h})
}

var errNotStarted = errors.New("turnserver: Server not started")

func (h streamHandler) OnOpen(c *fib.Connection) {
	sc := &streamConn{tuple: turn.FiveTuple{Client: remote(c), Server: addrPort(c.LocalAddr()), Transport: turn.TransportTCP}}
	sc.idleSince.Store(time.Now().UnixNano())
	c.SetAttachment(sc)
	h.s.streams.Store(c, sc)
	h.s.clients.Store(sc.tuple, c)
}

// OnData frames what the client sent into STUN and ChannelData messages
// and passes each whole one to the core, keeping the rest for the next
// call. A stream that frames as neither is closed, as is one whose message
// is longer than Config.MaxStreamMessage.
func (h streamHandler) OnData(c *fib.Connection, b []byte) {
	sc, ok := c.Attachment().(*streamConn)
	if !ok || h.s.run.Load() == nil {
		return
	}
	data := b
	if len(sc.buf) > 0 {
		sc.buf = append(sc.buf, b...)
		data = sc.buf
	}
	now := time.Now()
	for {
		n, err := turn.FrameLen(data)
		if err != nil || n > h.s.cfg.MaxStreamMessage {
			_ = c.Close()
			return
		}
		if n == 0 || n > len(data) {
			break
		}
		_ = h.s.core.HandlePacket(sc.tuple, data[:n], now)
		data = data[n:]
	}
	// copy, which append uses, moves overlapping bytes correctly when
	// data is the tail of sc.buf.
	sc.buf = append(sc.buf[:0], data...)
}

func (h streamHandler) OnPriorityData(*fib.Connection, []byte) {}

func (h streamHandler) OnClose(c *fib.Connection, _ error) {
	sc, ok := c.Attachment().(*streamConn)
	if !ok {
		return
	}
	h.s.streams.Delete(c)
	h.s.clients.CompareAndDelete(sc.tuple, c)
	h.s.core.ClientClosed(sc.tuple)
}

// sweepStreams closes the TCP and TLS clients' connections that have gone
// without an allocation for Config.StreamIdleTimeout.
func (s *Server) sweepStreams(now time.Time) {
	s.streams.Range(func(c, v any) bool {
		sc := v.(*streamConn)
		if _, ok := s.core.RelayFor(sc.tuple); ok {
			sc.idleSince.Store(now.UnixNano())
		} else if now.UnixNano()-sc.idleSince.Load() >= int64(s.cfg.StreamIdleTimeout) {
			_ = c.(*fib.Connection).Close()
		}
		return true
	})
}

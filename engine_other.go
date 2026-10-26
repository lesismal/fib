//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd && !windows

package fib

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/internal/netaddr"
	"github.com/lesismal/fib/taskpool"
)

// pollersSupported says Config.IOPollers applies, which it does not to a
// backend that already serves each connection on a goroutine of its own.
const pollersSupported = false

type Engine struct {
	bindingState
	// pollers is always empty here; see pollersSupported.
	pollers []*Engine
	// acceptPlacement is Config.AcceptPlacement, kept only to warn of it.
	acceptPlacement AcceptPlacement
	// accepted counts the connections the bindings accepted, against
	// Config.MaxConns.
	accepted        acceptedConns
	name            string
	logStatus       bool
	listeners       []boundListener
	handler         Handler
	taskPool        TaskPool
	releaseTaskPool func()
	taskWG          sync.WaitGroup
	stopping        atomic.Bool
	// draining says Shutdown has begun: the listeners are closed, no UDP
	// peer is opened, and the connections are left to finish.
	draining atomic.Bool
	// stopped closes when Stop is called, which is what Run waits on when
	// there is no listener to serve, or once Shutdown has closed them.
	stopped   chan struct{}
	closeOnce sync.Once
	// mu guards the connections and the lists of listeners and UDP sockets,
	// which bindings change as they come and go, and running and sweeping.
	mu          sync.Mutex
	connections map[*Connection]struct{}
	// running says Run has started serving the listeners, each on a
	// goroutine of its own that serving counts; a listener added from then
	// on is served at once. serveErr holds the first error a listener
	// failed with, which ends Run.
	running  bool
	serving  sync.WaitGroup
	serveErr chan error
	// sweeping says the timer closing silent UDP peers is armed.
	sweeping       bool
	readers        sync.WaitGroup
	readBufferSize int
	// udpListeners are the engine's UDP sockets, when Config.Network names
	// UDP, and udpIdleTimeout closes their silent peers.
	udpListeners   []*udpListener
	udpIdleTimeout time.Duration
}

// boundListener is a TCP or Unix listener and what serves the connections it
// takes: handler, for the binding-th of the engine's bindings.
type boundListener struct {
	net.Listener
	handler Handler
	binding int
}

// root is the engine the application created, which here is always e.
func (e *Engine) root() *Engine { return e }

// newEngine creates the engine New describes, listening on bindings, which
// bindingsOf has resolved.
func newEngine(config Config, bindings []Binding) (*Engine, error) {
	if err := config.validateTaskPool(); err != nil {
		return nil, err
	}
	if err := validAcceptPlacement(config.AcceptPlacement); err != nil {
		return nil, err
	}
	if config.MaxEvents <= 0 {
		config.MaxEvents = 256
	}
	if config.ReadBufferSize <= 0 {
		config.ReadBufferSize = 16 * 1024
	}
	// Dialed connections are served by the handler each dial names; one
	// that names none gets this, which does nothing.
	var handler Handler = HandlerFuncs{}
	listeners, udpListeners, err := listenBindings(bindings, 0)
	if err != nil {
		return nil, err
	}
	pool, releasePool := acquireTaskPool(config)
	e := &Engine{accepted: acceptedConns{max: int64(config.MaxConns)}, name: engineName(config), logStatus: config.LogStatus, acceptPlacement: config.AcceptPlacement, listeners: listeners, handler: handler, taskPool: pool, releaseTaskPool: releasePool,
		connections: make(map[*Connection]struct{}), stopped: make(chan struct{}), serveErr: make(chan error, 1),
		udpListeners: udpListeners, udpIdleTimeout: udpIdleTimeout(config.UDPIdleTimeout)}
	e.readBufferSize = config.ReadBufferSize
	e.bindConfig = config
	e.bindingCount = len(bindings)
	e.startUDPSweeper()
	return e, nil
}

// listenBindings opens the listeners and UDP sockets of every binding, or
// none of them. The bindings are the engine's first-th and those after it.
func listenBindings(bindings []Binding, first int) ([]boundListener, []*udpListener, error) {
	var listeners []boundListener
	var udpListeners []*udpListener
	fail := func(err error) ([]boundListener, []*udpListener, error) {
		for _, opened := range listeners {
			_ = opened.Close()
		}
		for _, opened := range udpListeners {
			_ = opened.pc.Close()
		}
		return nil, nil, err
	}
	for k, b := range bindings {
		i := first + k
		if netaddr.IsUDP(b.Network) {
			opened, err := listenUDP(b.Network, b.Addrs)
			if err != nil {
				return fail(err)
			}
			for _, l := range opened {
				l.handler, l.binding = b.Handler, i
			}
			udpListeners = append(udpListeners, opened...)
			continue
		}
		for _, addr := range b.Addrs {
			if addr == "" {
				addr = ":0"
			}
			listener, err := net.Listen(b.Network, addr)
			if err != nil {
				return fail(err)
			}
			listeners = append(listeners, boundListener{Listener: listener, handler: b.Handler, binding: i})
		}
	}
	return listeners, udpListeners, nil
}

func (e *Engine) submit(c *Connection) bool {
	e.taskWG.Add(1)
	if !e.taskPool.GoTask(c) {
		e.taskWG.Done()
		return false
	}
	return true
}

// Stats reports what backpressure this server has applied. This backend leaves
// socket I/O to the runtime's own poller, which applies the pushback itself, so
// there is no read interest for the server to pause and the counters stay at
// zero. The method exists so that code written against either backend compiles
// and runs on both.
// OpenConns and ConnsRefused count against Config.MaxConns, as on the other
// backends.
type Stats struct {
	ReadsPausedByWatermark uint64
	ReadsPausedByBudget    uint64
	ReadsResumed           uint64
	PendingBytes           int64
	OpenConns              int64
	ConnsRefused           uint64
}

func (e *Engine) Stats() Stats {
	return Stats{OpenConns: e.accepted.open.Load(), ConnsRefused: e.accepted.refused.Load()}
}

// acceptedConns counts the connections an engine's bindings accepted and
// have not closed, against Config.MaxConns.
type acceptedConns struct {
	max     int64
	open    atomic.Int64
	refused atomic.Uint64
}

// take counts a connection just accepted, or reports false, and counts it
// refused, when MaxConns are open already.
func (a *acceptedConns) take() bool {
	if a.open.Add(1) > a.max && a.max > 0 {
		a.open.Add(-1)
		a.refused.Add(1)
		return false
	}
	return true
}

// leave uncounts an accepted connection that has closed, or that was
// closed before it opened.
func (a *acceptedConns) leave() { a.open.Add(-1) }

// LocalAddr returns the address of the server's first listener.
func (e *Engine) LocalAddr() (*net.TCPAddr, error) {
	addrs, err := e.LocalAddrs()
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, errors.New("fib: engine has no listener")
	}
	return addrs[0], nil
}

// ListenAddrs returns the address of every listener, whatever its network:
// the TCP and Unix listeners first, in configured order, and the UDP ones
// after them, in theirs.
func (e *Engine) ListenAddrs() ([]net.Addr, error) {
	return e.listenAddrsOf(-1), nil
}

// BindingAddrs returns the addresses of the i-th binding, as New and
// AddBinding number them, in the order the binding lists them.
func (e *Engine) BindingAddrs(i int) ([]net.Addr, error) {
	if i < 0 {
		return nil, errNoBinding
	}
	addrs := e.listenAddrsOf(i)
	if len(addrs) == 0 {
		return nil, errNoBinding
	}
	return addrs, nil
}

// listenAddrsOf returns the addresses of binding's listeners, or of every
// listener for a binding below zero.
func (e *Engine) listenAddrsOf(binding int) []net.Addr {
	e.mu.Lock()
	defer e.mu.Unlock()
	addrs := make([]net.Addr, 0, len(e.listeners)+len(e.udpListeners))
	for _, listener := range e.listeners {
		if binding < 0 || listener.binding == binding {
			addrs = append(addrs, listener.Addr())
		}
	}
	for _, l := range e.udpListeners {
		if binding < 0 || l.binding == binding {
			addrs = append(addrs, l.pc.LocalAddr())
		}
	}
	return addrs
}

// LocalAddrs returns one address per TCP listener, in configured order,
// leaving out the Unix ones, which ListenAddrs reports.
func (e *Engine) LocalAddrs() ([]*net.TCPAddr, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	addrs := make([]*net.TCPAddr, 0, len(e.listeners))
	for _, listener := range e.listeners {
		if addr, ok := listener.Addr().(*net.TCPAddr); ok {
			addrs = append(addrs, addr)
		}
	}
	return addrs, nil
}

// Run serves every listener, and those bindings add, until the server is
// stopped. It returns early, with the error, if a listener fails.
func (e *Engine) Run() error {
	e.logRun()
	e.mu.Lock()
	e.running = true
	if !e.stopping.Load() {
		for _, listener := range e.listeners {
			e.startServing(listener)
		}
		for _, l := range e.udpListeners {
			e.startServingUDP(l)
		}
	}
	e.mu.Unlock()
	// A client engine's connections are served by their own readers, and a
	// shut-down one's until Shutdown stops it, so Run only has to last as
	// long as the engine does.
	select {
	case <-e.stopped:
	case err := <-e.serveErr:
		return err
	}
	e.serving.Wait()
	return nil
}

// startServing accepts listener's connections on a goroutine of its own
// until the listener closes. Callers hold mu.
func (e *Engine) startServing(listener boundListener) {
	e.serving.Add(1)
	go func() {
		defer e.serving.Done()
		e.noteServeError(e.serve(listener))
	}()
}

// startServingUDP is startServing for a UDP socket. Callers hold mu.
func (e *Engine) startServingUDP(l *udpListener) {
	e.serving.Add(1)
	go func() {
		defer e.serving.Done()
		e.noteServeError(e.serveUDP(l))
	}()
}

// noteServeError keeps the first error a listener failed with, for Run.
func (e *Engine) noteServeError(err error) {
	if err == nil {
		return
	}
	select {
	case e.serveErr <- err:
	default:
	}
}

func (e *Engine) serve(listener boundListener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if e.stopping.Load() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		if !e.accepted.take() {
			_ = conn.Close()
			continue
		}
		e.adoptWith(conn, listener.handler, nil, false, false, int32(listener.binding+1))
	}
}

// adopt starts serving an established connection: OnOpen, then done if the
// connection was dialed, then its reader. It reports false, and closes conn,
// if the engine has stopped in the meantime.
func (e *Engine) adopt(conn net.Conn, handler Handler, done func(*Connection, error)) bool {
	return e.adoptWith(conn, handler, done, false, false, 0)
}

// adoptWith is adopt for a connection that may be a dialed one, a dialed UDP
// one among them, or one a binding took, whose bindingID is id.
func (e *Engine) adoptWith(conn net.Conn, handler Handler, done func(*Connection, error), udp, dialed bool, id int32) bool {
	c := &Connection{engine: e, handler: handler, conn: conn, udp: udp, dialed: dialed, bindingID: id}
	if addr := conn.LocalAddr(); !udp && addr != nil && addr.Network() == "unix" {
		c.unix = true
	}
	c.fd.Store(-1)
	e.mu.Lock()
	if e.stopping.Load() {
		e.mu.Unlock()
		_ = conn.Close()
		if id != 0 && !udp {
			e.accepted.leave()
		}
		return false
	}
	e.connections[c] = struct{}{}
	e.readers.Add(1)
	e.mu.Unlock()
	c.handler.OnOpen(c)
	if done != nil {
		done(c, nil)
	}
	go e.readConnection(c)
	return true
}

// Dial connects to addr and serves the connection like an accepted one. This
// backend leaves socket I/O to the runtime's poller, so the connect runs on a
// goroutine of its own through net.DialTimeout, and done runs on that
// goroutine rather than on an event loop. Otherwise it behaves as the native
// backends' Dial does: OnOpen and then done on success, done alone with the
// error on failure, and an error from Dial itself, with done never called,
// only when the dial cannot start at all.
func (e *Engine) Dial(network, addr string, timeout time.Duration, done func(*Connection, error)) error {
	return e.DialWithHandler(network, addr, timeout, nil, done)
}

// DialWithHandler is Dial with a handler of the connection's own. A nil
// handler means the engine's.
func (e *Engine) DialWithHandler(network, addr string, timeout time.Duration, handler Handler, done func(*Connection, error)) error {
	if handler == nil {
		handler = e.handler
	}
	switch network {
	case "":
		network = "tcp"
	case "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6", "unix":
	default:
		return &net.OpError{Op: "dial", Net: network, Err: net.UnknownNetworkError(network)}
	}
	if e.stopping.Load() {
		return &net.OpError{Op: "dial", Net: network, Err: net.ErrClosed}
	}
	go func() {
		conn, err := net.DialTimeout(network, addr, timeout)
		if err == nil && !e.adoptWith(conn, handler, done, netaddr.IsUDP(network), true, 0) {
			err = &net.OpError{Op: "dial", Net: network, Addr: conn.RemoteAddr(), Err: net.ErrClosed}
		}
		if err != nil && done != nil {
			done(nil, err)
		}
	}()
	return nil
}
func (e *Engine) readConnection(c *Connection) {
	defer e.readers.Done()
	size := e.readBufferSize
	if c.udp {
		// A datagram larger than the buffer would be truncated.
		size = maxDatagramSize
	}
	buf := bufferpool.Get(size)
	defer bufferpool.Put(buf)
	for {
		if !c.udp && !c.awaitReadable() {
			return
		}
		n, err := c.conn.Read(buf)
		if n > 0 && !c.enqueueData(buf[:n]) {
			return
		}
		if (err != nil || n == 0) && !c.udp && c.readEnded() {
			// CloseRead ended input, and with it the reads; the connection
			// stays open until it is closed.
			return
		}
		if err != nil {
			if c.isLingering() {
				// The peer ended its side, or the linger ran out: either way
				// this is the close the linger put off.
				err = nil
			}
			c.closeWithError(err)
			return
		}
		if n == 0 && !c.udp {
			c.closeWithError(io.EOF)
			return
		}
	}
}
func (e *Engine) finishConnection(c *Connection, err error) {
	c.mu.Lock()
	if c.closeDelivered {
		c.mu.Unlock()
		return
	}
	c.closeDelivered, c.closing, c.scheduled = true, true, false
	c.mu.Unlock()
	_ = c.conn.Close()
	if c.bindingID != 0 && !c.udp {
		e.accepted.leave()
	}
	c.handler.OnClose(c, err)
	// Listed until OnClose has run, so that Shutdown and ShutdownBinding,
	// which count what is listed, return only after it.
	e.mu.Lock()
	delete(e.connections, c)
	e.mu.Unlock()
}

// beginShutdown starts Shutdown: the listeners are closed, which stops them
// taking connections, while the connections there are go on being served.
// The UDP sockets stay open, since the peers' datagrams arrive on them.
func (e *Engine) beginShutdown() {
	if e.draining.Load() {
		return
	}
	// The listeners close before ShuttingDown reports true, so that a client
	// connecting once it does is refused; closing one twice does no harm.
	e.mu.Lock()
	for _, listener := range e.listeners {
		_ = listener.Close()
	}
	e.mu.Unlock()
	e.draining.Store(true)
}

// shutdownRound tells each open connection the engine is shutting down, here
// on the goroutine calling Shutdown, and reports how many of those Shutdown
// waits for remain. With a binding of zero or more, the round is
// ShutdownBinding's, over the connections that binding took.
func (e *Engine) shutdownRound(_ context.Context, binding int) (int, error) {
	if e.stopping.Load() {
		return 0, nil
	}
	e.mu.Lock()
	connections := make([]*Connection, 0, len(e.connections))
	for c := range e.connections {
		if !c.dialed && c.inBinding(binding) {
			connections = append(connections, c)
		}
	}
	e.mu.Unlock()
	for _, c := range connections {
		c.mu.Lock()
		// The handler is read under the lock, which SetHandler takes.
		closing, handler := c.closing, c.handler
		c.mu.Unlock()
		if !closing {
			if h, ok := handler.(ShutdownHandler); ok {
				h.OnShutdown(c)
			}
		}
	}
	return len(connections), nil
}

func (e *Engine) Stop() {
	if e.stopping.CompareAndSwap(false, true) {
		close(e.stopped)
		e.mu.Lock()
		defer e.mu.Unlock()
		for _, listener := range e.listeners {
			_ = listener.Close()
		}
		for _, l := range e.udpListeners {
			_ = l.pc.Close()
		}
	}
}

// Close stops the engine and closes every connection it still has, at once:
// a handler still running finds whatever it sends refused. It releases the
// task pool once no handler is running any more, which may be after it has
// returned; see the native backends' Close.
func (e *Engine) Close() error {
	e.closeOnce.Do(func() {
		e.Stop()
		e.mu.Lock()
		connections := make([]*Connection, 0, len(e.connections))
		for c := range e.connections {
			connections = append(connections, c)
		}
		e.mu.Unlock()
		for _, c := range connections {
			c.Close()
		}
		_ = afterRounds(func() {
			e.readers.Wait()
			e.taskWG.Wait()
		}, func() error {
			e.releaseTaskPool()
			return nil
		})
	})
	return nil
}

var _ io.Closer = (*Engine)(nil)

type Config struct {
	// Name labels the engine in what it logs, and names the task pools its
	// connections run on: "<Name>-workers" for the engine's own and
	// "<Name>-streams" for the HTTP/2 and HTTP/3 handlers. Engines of the
	// same name share both pools; see SharedTaskPool. Empty means
	// DefaultName.
	Name string
	// LogStatus has the engine log a line when it starts serving, with its
	// name, the addresses it listens on, its task pool and its pollers. It is
	// off by default. The task pools' own lines are switched separately, with
	// taskpool.SetLogStatus.
	LogStatus bool
	// Network is the network of every Binding that leaves its own empty;
	// see Binding.Network. Empty means "tcp".
	Network                         string
	Backlog, WorkerCount, MaxEvents int
	// PollBatchSize caps how many events one wait of an epoll, kqueue or
	// IOCP event loop collects. This backend has no loops, and ignores it.
	PollBatchSize            int
	ReadBufferSize           int
	WriteBufferHighWatermark int
	// MaxWriteQueueBytes caps a connection's send queue, as it does on the
	// other backends; see the native Config's field. This backend writes each
	// send before it returns, so it never holds a queue for the limit to
	// bound, and accepts the field only so that one config serves both.
	MaxWriteQueueBytes int64
	UseWritev          bool
	// CoalesceWrites batches the replies a read round produces. This backend
	// reads and writes through the net package, which has no read rounds to
	// batch, and ignores it.
	CoalesceWrites bool
	// SocketSyscalls has a Linux engine read and write its sockets with
	// recvfrom, sendto and sendmsg rather than read, write and writev. This
	// backend reads and writes through the net package, and ignores it.
	SocketSyscalls bool
	// TaskPoolMode picks the scheduler the workers run under. Prefer
	// SetTaskPoolMode over assigning it, so that WorkerCount and MaxEvents
	// follow the mode rather than staying at numbers tuned for the other one.
	TaskPoolMode taskpool.Mode
	// MinWorkerCount is the resident floor a ModeAdaptive pool retires down
	// to, with WorkerCount as the ceiling it grows to. Zero, the default,
	// lets an idle pool retire every worker and start them again when work
	// arrives. The other modes ignore it.
	MinWorkerCount int
	// SharedTaskPool has the engines of one Name run on one task pool. The
	// first of them builds it from its own settings, and those that follow
	// run on it as it is, whatever theirs say.
	SharedTaskPool bool
	// TaskPool, when set, runs the engine's connections instead of a pool the
	// engine builds from the fields above. See SetTaskPool.
	TaskPool TaskPool
	// IOPollers and IOPollerCount split an epoll or kqueue engine across
	// several event loops. This backend has one goroutine per connection
	// already, so both are ignored here, though DefaultConfig sets IOPollers
	// as it does on the other backends: on more than four CPUs.
	IOPollers     bool
	IOPollerCount int
	// EngineLoop gives an epoll or kqueue engine with pollers a loop of its
	// own for its listeners. This backend has no loops, and ignores it.
	EngineLoop bool
	// AcceptPlacement picks the poller an epoll engine's poller hands a
	// connection it accepted to. This backend has no pollers, ignores it,
	// and warns when it is set to other than AcceptFDModulo.
	AcceptPlacement AcceptPlacement
	// ReusePort binds an epoll or kqueue engine's listeners with SO_REUSEPORT.
	// This backend listens through the net package, and ignores it.
	ReusePort bool
	// BalanceReusePort round-robins a reuseport group's connections on an
	// epoll or kqueue engine with pollers. This backend has no pollers, and
	// ignores it.
	BalanceReusePort bool
	// UDPIdleTimeout closes a silent UDP peer's connection. Zero means
	// DefaultUDPIdleTimeout and a negative value keeps peers until closed.
	UDPIdleTimeout time.Duration
	// MaxConns caps the connections the engine's bindings have accepted and
	// not yet closed, as on the other backends: one accepted past it is
	// closed at once and counted in Stats.ConnsRefused. Zero or less means
	// no cap.
	MaxConns int
	// customPoolSizing records that SetPoolSizing pinned the sizing, so that a
	// later SetTaskPoolMode does not overwrite it.
	customPoolSizing bool
}

func DefaultConfig() Config {
	sizing := DefaultPoolSizing(taskpool.ModeAdaptive)
	return Config{Name: DefaultName, Network: "tcp", Backlog: 128, WorkerCount: sizing.WorkerCount, MaxEvents: sizing.MaxEvents, ReadBufferSize: 16 * 1024, WriteBufferHighWatermark: 4 * 1024, UseWritev: true, CoalesceWrites: true, SocketSyscalls: true, TaskPoolMode: taskpool.ModeAdaptive, SharedTaskPool: true, IOPollers: true, ReusePort: true}
}

type portableEvent struct {
	data     []byte
	closeErr error
	closing  bool
}

type connectionAttachment struct{ value any }

type Connection struct {
	engine                             *Engine
	handler                            Handler
	conn                               net.Conn
	fd                                 atomic.Int64
	mu                                 sync.Mutex
	events                             []portableEvent
	scheduled, closing, closeDelivered bool
	// readHeld is an application-driven read pause, set through HoldReads.
	// readWake is what the reader goroutine parks on while it is set.
	readHeld bool
	readWake *sync.Cond
	// readShut and writeShut record CloseRead and CloseWrite. Guarded by mu.
	readShut, writeShut bool
	// lingerFor is what CloseAfterSendLinger asked for, and lingering marks
	// the linger under way, which drops what arrives. Guarded by mu.
	lingerFor time.Duration
	lingering bool
	writeMu   sync.Mutex
	// writeLimit is what SetWriteQueueLimit chose, and writeLimitSet that it
	// did; see the native Connection's. This backend has no send queue for it
	// to cap.
	writeLimit    int64
	writeLimitSet bool
	attachment    atomic.Pointer[connectionAttachment]
	layer         Layer
	// udp marks a connection that exchanges datagrams: a peer of a UDP
	// listener, whose conn is a udpPeerConn, or a dialed UDP socket.
	udp bool
	// unix marks a Unix socket.
	unix bool
	// dialed marks a connection Dial opened, as against one a listener
	// accepted.
	dialed bool
	// bindingID is the index of the Binding that took the connection, plus
	// one, so that zero, a dialed connection's, names none.
	bindingID int32
	// udpActive is when a listener's peer last sent or was sent a datagram,
	// in nanoseconds, for the idle timeout.
	udpActive atomic.Int64
	// udpIdle, when not zero, is a listener's peer's own idle timeout in
	// nanoseconds, in place of the engine's; a negative one keeps the peer.
	udpIdle atomic.Int64
	// peer is the address SetRemoteAddrPort gave, if it gave one.
	peer *net.TCPAddr
}

// SetUDPIdleTimeout sets how long the peer of a UDP listener, which c is, may
// be silent before its connection is closed, in place of
// Config.UDPIdleTimeout. Zero goes back to the engine's, and a negative value
// keeps the peer until it is closed, for a handler that times its peers out
// itself. Peers are looked at as often as the engine's own timeout has them
// looked at, every quarter of it, so a shorter one here may be late by up to
// that, and while the engine's is off none is looked at. On another
// connection it does nothing.
func (c *Connection) SetUDPIdleTimeout(d time.Duration) {
	if c.udp && !c.dialed {
		c.udpIdle.Store(int64(d))
	}
}

// Protocol returns the transport the connection runs over, which it keeps
// for its whole life, after it closes too.
func (c *Connection) Protocol() Protocol {
	switch {
	case c.udp:
		return ProtocolUDP
	case c.unix:
		return ProtocolUnix
	}
	return ProtocolTCP
}

// SetRemoteAddrPort makes ap the peer's address, which RemoteAddrPort and
// RemoteAddr report from then on in place of the socket's: the client's
// that a PROXY protocol header in front of the connection's data names, for
// a server behind a proxy that sends one. RemoteAddr reports it as a
// *net.TCPAddr. Call it before anything else reads the address: from the
// handler that reads the header, before it hands the connection to the one
// that serves it. An address that is not valid or has a zone is ignored, and
// so is any on a UDP connection, whose peer is its datagrams' source.
func (c *Connection) SetRemoteAddrPort(ap netip.AddrPort) {
	if c.udp || !ap.IsValid() || ap.Addr().Zone() != "" {
		return
	}
	c.peer = net.TCPAddrFromAddrPort(netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port()))
}

// RemoteAddr returns the peer's address.
func (c *Connection) RemoteAddr() net.Addr {
	if c.peer != nil {
		return c.peer
	}
	return c.conn.RemoteAddr()
}

// RemoteAddrPort returns the peer's IP address and port, or the zero
// AddrPort for a peer without one, or with an IPv6 zone.
func (c *Connection) RemoteAddrPort() netip.AddrPort {
	if c.peer != nil {
		return c.peer.AddrPort()
	}
	var ap netip.AddrPort
	switch addr := c.conn.RemoteAddr().(type) {
	case *net.TCPAddr:
		if addr.Zone != "" {
			return netip.AddrPort{}
		}
		ap = addr.AddrPort()
	case *net.UDPAddr:
		if addr.Zone != "" {
			return netip.AddrPort{}
		}
		ap = addr.AddrPort()
	default:
		return netip.AddrPort{}
	}
	return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
}

func (c *Connection) FD() int { return int(c.fd.Load()) }

// Close ends the connection. The close is carried out asynchronously, so
// there is no error to report and Close always returns nil, including for a
// connection that is already closing. It satisfies net.Conn and io.Closer.
func (c *Connection) Close() error {
	c.closeWithError(nil)
	return nil
}

func (c *Connection) Attachment() any {
	if value := c.attachment.Load(); value != nil {
		return value.value
	}
	return nil
}

func (c *Connection) SetAttachment(value any) {
	if value == nil {
		c.attachment.Store(nil)
	} else {
		c.attachment.Store(&connectionAttachment{value: value})
	}
}

func (c *Connection) closeWithError(err error) {
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		return
	}
	c.closing = true
	if c.readWake != nil {
		// A reader parked on a hold has to see the close.
		c.readWake.Broadcast()
	}
	c.events = append(c.events, portableEvent{closing: true, closeErr: err})
	submit := !c.scheduled
	c.scheduled = true
	c.mu.Unlock()
	_ = c.conn.Close()
	if submit && !c.engine.submit(c) {
		c.engine.finishConnection(c, err)
	}
}

// HoldReads stops or resumes reading from this connection. See the native
// backends' HoldReads: here the connection's reader goroutine parks while the
// hold is set, which leaves the peer's bytes in the socket.
func (c *Connection) HoldReads(hold bool) {
	if c.udp {
		return
	}
	c.mu.Lock()
	if c.readHeld != hold {
		c.readHeld = hold
		if c.readWake == nil {
			c.readWake = sync.NewCond(&c.mu)
		}
		c.readWake.Broadcast()
	}
	c.mu.Unlock()
}

// NotifyOutput reports false on the portable backend, whose Send writes
// before it returns, so nothing is ever left queued to wait for.
func (c *Connection) NotifyOutput(int64, func(error)) bool { return false }

// NotifyWritable reports false on the portable backend, whose UDP sends go
// through package net, which waits for room itself.
func (c *Connection) NotifyWritable(func()) bool { return false }

// SetWriteBackpressure does nothing on the portable backend, whose reads
// never pause for the connection's own backlog.
func (c *Connection) SetWriteBackpressure(bool) {}

// ReadsHeld reports whether HoldReads is currently holding reads back.
func (c *Connection) ReadsHeld() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readHeld
}

// awaitReadable parks the reader goroutine while reads are held, and reports
// whether reading should go on.
func (c *Connection) awaitReadable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.readHeld && !c.closing {
		if c.readWake == nil {
			c.readWake = sync.NewCond(&c.mu)
		}
		c.readWake.Wait()
	}
	return !c.closing
}

func (c *Connection) Send(data []byte) error {
	if l := c.layer; l != nil {
		return l.Send(data, nil)
	}
	return c.sendRaw(data)
}

// sendRawPooled is sendRaw for a buffer from package bufferpool, which goes
// back to the pool once written: this backend writes before it returns.
func (c *Connection) sendRawPooled(data []byte) error {
	err := c.sendRaw(data)
	bufferpool.Put(data)
	return err
}

// sendRaw writes bytes to the socket below any layer.
func (c *Connection) sendRaw(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	c.mu.Lock()
	closing, writeShut := c.closing, c.writeShut
	c.mu.Unlock()
	if closing || writeShut {
		return io.ErrClosedPipe
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	for len(data) > 0 {
		n, err := c.conn.Write(data)
		if err != nil {
			c.closeWithError(err)
			return err
		}
		if n == 0 {
			c.closeWithError(io.ErrNoProgress)
			return io.ErrNoProgress
		}
		data = data[n:]
	}
	if c.udp {
		c.udpActive.Store(time.Now().UnixNano())
	}
	return nil
}

// Flush does nothing on the portable backend, whose sends have reached the
// socket by the time they return.
func (c *Connection) Flush() error { return nil }

// Corked reports false: this backend writes every send as it is made.
func (c *Connection) Corked() bool { return false }

// SendOwned is equivalent to Send on the synchronous portable backend.
func (c *Connection) SendOwned(data []byte) error { return c.Send(data) }

// SendShared is equivalent to Send on the synchronous portable backend, which
// never keeps data past the call.
func (c *Connection) SendShared(data []byte) error { return c.Send(data) }

// SetWriteQueueLimit caps a connection's send queue, as it does on the other
// backends, where a send past the limit closes the connection with
// ErrSendQueueFull. This backend writes each send before it returns and never
// holds a queue, so the limit never applies; it is accepted, and reported by
// WriteQueueLimit, so that one program serves both.
func (c *Connection) SetWriteQueueLimit(limit int64) {
	c.mu.Lock()
	c.writeLimitSet, c.writeLimit = limit >= 0, max(limit, 0)
	c.mu.Unlock()
}

// WriteQueueLimit reports the limit SetWriteQueueLimit set, or
// Config.MaxWriteQueueBytes when none was, and zero when there is none.
func (c *Connection) WriteQueueLimit() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writeLimitSet {
		return c.writeLimit
	}
	if c.engine == nil {
		return 0
	}
	return c.engine.bindConfig.MaxWriteQueueBytes
}

func (c *Connection) SendParts(first, second []byte) error {
	if l := c.layer; l != nil {
		return l.Send(first, second)
	}
	// A send on this backend has reached the socket by the time it returns,
	// so the joined copy is done with as soon as Send is.
	data := bufferpool.Join(nil, first, second)
	defer bufferpool.Put(data)
	return c.Send(data)
}

// SendPartsShared is equivalent to SendParts on the synchronous portable
// backend, which never keeps either part past the call, and so has no copy to
// spare.
func (c *Connection) SendPartsShared(first, second []byte) error {
	return c.SendParts(first, second)
}

// sendClosed reports whether the connection has stopped accepting sends.
func (c *Connection) sendClosed() bool {
	c.mu.Lock()
	closing := c.closing
	c.mu.Unlock()
	return closing
}

// CloseAfterSendLinger is CloseAfterSend for a connection whose peer may
// still be sending: rather than close with input unread, which makes the
// kernel reset the connection, it shuts the write side once what was sent has
// gone, after the layer's closing message, and goes on reading and dropping
// whatever arrives until the peer ends its own side or d has passed; then it
// closes. A d of zero or less is CloseAfterSend.
func (c *Connection) CloseAfterSendLinger(d time.Duration) {
	if d > 0 && !c.udp {
		c.mu.Lock()
		if c.lingerFor == 0 {
			c.lingerFor = d
		}
		c.mu.Unlock()
	}
	if l := c.layer; l != nil {
		l.CloseAfterSend()
		return
	}
	c.closeAfterSendRaw()
}

// closeAfterSendRaw closes the connection, or begins the linger of
// CloseAfterSendLinger. Sends on this backend have already reached the socket
// by the time they return, so nothing is left to wait for.
func (c *Connection) closeAfterSendRaw() {
	c.mu.Lock()
	d := c.lingerFor
	linger := d > 0 && !c.closing && !c.lingering && !c.readShut
	c.lingerFor = 0
	if linger {
		c.lingering = true
	}
	c.mu.Unlock()
	if !linger || c.CloseWrite() != nil || c.conn.SetReadDeadline(time.Now().Add(d)) != nil {
		c.closeWithError(nil)
	}
}

// isLingering reports whether a lingering close is under way.
func (c *Connection) isLingering() bool {
	c.mu.Lock()
	lingering := c.lingering
	c.mu.Unlock()
	return lingering
}

func (c *Connection) enqueueData(data []byte) bool {
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		return false
	}
	if c.lingering {
		c.mu.Unlock()
		return true
	}
	c.events = append(c.events, portableEvent{data: append([]byte(nil), data...)})
	submit := !c.scheduled
	c.scheduled = true
	c.mu.Unlock()
	if submit && !c.engine.submit(c) {
		c.closeWithError(errors.New("task pool stopped"))
		return false
	}
	return true
}

func (c *Connection) process() {
	defer func() {
		if r := recover(); r != nil {
			c.engine.finishConnection(c, fmt.Errorf("handler panic: %v", r))
		}
	}()
	for {
		c.mu.Lock()
		if len(c.events) == 0 {
			c.scheduled = false
			c.mu.Unlock()
			return
		}
		event := c.events[0]
		c.events[0] = portableEvent{}
		c.events = c.events[1:]
		c.mu.Unlock()
		if event.closing {
			c.engine.finishConnection(c, event.closeErr)
			return
		}
		c.handler.OnData(c, event.data)
	}
}

func (c *Connection) RunTask() {
	defer c.engine.taskWG.Done()
	c.process()
}

// errNoRead is what Read reports on this backend. The native backends let a
// handler take bytes off the socket itself; here the connection's own reader
// goroutine is blocked on that socket, and a read from anywhere else would
// take bytes from under it.
var errNoRead = errors.New("fib: this backend is read through OnData")

// Read is not available on this backend: incoming bytes reach the handler
// through OnData. See the native backends' Read.
func (c *Connection) Read([]byte) (int, error) { return 0, errNoRead }

// Write hands b to Send and reports it all written, since Send copies what it
// is given. It therefore does not wait for the peer.
func (c *Connection) Write(b []byte) (int, error) {
	if err := c.Send(b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// LocalAddr returns the address this side of the connection is bound to.
func (c *Connection) LocalAddr() net.Addr { return c.conn.LocalAddr() }

// SetDeadline sets both the read and the write deadline. This backend reads
// and writes through a net.Conn of its own, so they are that connection's
// deadlines: one that passes fails the read or write it belongs to, which
// closes the connection and hands OnClose the timeout error. A zero time
// removes a deadline.
func (c *Connection) SetDeadline(t time.Time) error { return c.conn.SetDeadline(t) }

// SetReadDeadline closes the connection at t if the peer has not sent
// anything more by then. See SetDeadline.
func (c *Connection) SetReadDeadline(t time.Time) error { return c.conn.SetReadDeadline(t) }

// SetWriteDeadline fails a send that has not reached the socket by t, which
// closes the connection. See SetDeadline.
func (c *Connection) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }

// SendFile sends count bytes of f, starting at offset. The portable backend
// writes synchronously, so the range is read and written before SendFile
// returns, and the caller may close f afterwards. A file shorter than the
// range is an error, since the peer was promised count bytes.
func (c *Connection) SendFile(f File, offset, count int64) error {
	if err := checkSendFileRange(offset, count); err != nil {
		return err
	}
	if c.udp {
		return ErrSendFileDatagram
	}
	return sendFileCopy(c, f, offset, count, c.Send)
}

// SendFileRaw is SendFile below any layer, with each chunk passed through
// sealer, when there is one, on its way. It writes synchronously too.
func (c *Connection) SendFileRaw(f File, offset, count int64, sealer FileSealer) error {
	if err := checkSendFileRange(offset, count); err != nil {
		return err
	}
	if c.udp {
		return ErrSendFileDatagram
	}
	if sealer == nil {
		return sendFileCopy(c, f, offset, count, c.sendRaw)
	}
	return sendFileCopy(c, f, offset, count, func(b []byte) error {
		sealed, err := sealer.Seal(nil, b)
		if err != nil {
			return err
		}
		return c.sendRaw(sealed)
	})
}

// QueuesFiles reports whether the connection has no layer, whose SendFile
// this backend has write the range without one; the range is written before
// SendFile returns either way.
func (c *Connection) QueuesFiles() bool { return !c.udp && c.layer == nil }

// ControlIdle runs nothing and reports false: this backend's socket is the
// net.Conn it wraps, whose writes it cannot hold back.
func (c *Connection) ControlIdle(func(fd uintptr) error) (bool, error) { return false, nil }

// SendRawRecord fails: there is no kernel TLS here.
func (c *Connection) SendRawRecord(uint8, []byte) error { return errNoKernelRecords }

// errNoKernelRecords is what SendRawRecord reports where it cannot send.
var errNoKernelRecords = errors.New("fib: no kernel TLS records here")

// The methods below are net.TCPConn's, beyond those net.Conn already asks
// for. This backend hands each to the net.Conn it wraps, and one that the
// wrapped connection does not have, such as a TCP option on a Unix or UDP
// connection, does nothing and reports no error.

// SetNoDelay turns Nagle's algorithm off, when noDelay is true, or back on.
// The net package starts every TCP connection with it off.
func (c *Connection) SetNoDelay(noDelay bool) error {
	if conn, ok := c.conn.(interface{ SetNoDelay(bool) error }); ok {
		return conn.SetNoDelay(noDelay)
	}
	return nil
}

// SetKeepAlive turns keep-alive probes on or off.
func (c *Connection) SetKeepAlive(keepalive bool) error {
	if conn, ok := c.conn.(interface{ SetKeepAlive(bool) error }); ok {
		return conn.SetKeepAlive(keepalive)
	}
	return nil
}

// SetKeepAlivePeriod is net.TCPConn.SetKeepAlivePeriod.
func (c *Connection) SetKeepAlivePeriod(d time.Duration) error {
	if conn, ok := c.conn.(interface{ SetKeepAlivePeriod(time.Duration) error }); ok {
		return conn.SetKeepAlivePeriod(d)
	}
	return nil
}

// SetKeepAliveConfig is net.TCPConn.SetKeepAliveConfig.
func (c *Connection) SetKeepAliveConfig(config net.KeepAliveConfig) error {
	if conn, ok := c.conn.(interface {
		SetKeepAliveConfig(net.KeepAliveConfig) error
	}); ok {
		return conn.SetKeepAliveConfig(config)
	}
	return nil
}

// SetLinger is net.TCPConn.SetLinger.
func (c *Connection) SetLinger(sec int) error {
	if conn, ok := c.conn.(interface{ SetLinger(int) error }); ok {
		return conn.SetLinger(sec)
	}
	return nil
}

// SetReadBuffer sets the size of the socket's receive buffer.
func (c *Connection) SetReadBuffer(bytes int) error {
	if conn, ok := c.conn.(interface{ SetReadBuffer(int) error }); ok {
		return conn.SetReadBuffer(bytes)
	}
	return nil
}

// SetWriteBuffer sets the size of the socket's send buffer.
func (c *Connection) SetWriteBuffer(bytes int) error {
	if conn, ok := c.conn.(interface{ SetWriteBuffer(int) error }); ok {
		return conn.SetWriteBuffer(bytes)
	}
	return nil
}

// CloseRead shuts down the reading side of the connection. OnData is not
// called again, and the end of input that follows does not close the
// connection, which goes on sending until it is closed. On a connection with
// no reading side to shut, such as a UDP one, it does nothing.
func (c *Connection) CloseRead() error {
	conn, ok := c.conn.(interface{ CloseRead() error })
	if !ok || c.udp {
		return nil
	}
	c.mu.Lock()
	closing, shut := c.closing, c.readShut
	c.readShut = true
	c.mu.Unlock()
	if closing {
		return net.ErrClosed
	}
	if shut {
		return nil
	}
	return conn.CloseRead()
}

// readEnded reports whether CloseRead has ended input.
func (c *Connection) readEnded() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readShut
}

// CloseWrite shuts down the writing side of the connection, after which sends
// are refused. Sends on this backend have reached the socket by the time they
// return, so the peer reads everything sent before it. On a connection with
// no writing side to shut, such as a UDP one, it does nothing.
func (c *Connection) CloseWrite() error {
	conn, ok := c.conn.(interface{ CloseWrite() error })
	if !ok || c.udp {
		return nil
	}
	c.mu.Lock()
	closing, shut := c.closing, c.writeShut
	c.writeShut = true
	c.mu.Unlock()
	if closing {
		return net.ErrClosed
	}
	if shut {
		return nil
	}
	// A send still writing finishes first.
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return conn.CloseWrite()
}

// MultipathTCP is net.TCPConn.MultipathTCP, and false for anything else.
func (c *Connection) MultipathTCP() (bool, error) {
	if conn, ok := c.conn.(interface{ MultipathTCP() (bool, error) }); ok {
		return conn.MultipathTCP()
	}
	return false, nil
}

// SyscallConn returns the wrapped connection's socket for raw access. A
// connection without one, a UDP listener's peer among them, returns an error.
func (c *Connection) SyscallConn() (syscall.RawConn, error) {
	if conn, ok := c.conn.(syscall.Conn); ok {
		return conn.SyscallConn()
	}
	return nil, errNoSocket
}

// File returns a duplicate of the wrapped connection's socket. A connection
// without one, a UDP listener's peer among them, returns an error.
func (c *Connection) File() (*os.File, error) {
	if conn, ok := c.conn.(interface{ File() (*os.File, error) }); ok {
		return conn.File()
	}
	return nil, errNoSocket
}

// udpListener is a UDP socket and the peers it has seen. Each peer is a
// connection whose conn is a udpPeerConn, so it sends and closes like any
// other connection on this backend.
type udpListener struct {
	pc *net.UDPConn
	// handler serves the socket's peers, and binding is the index of the
	// Binding it was opened for.
	handler Handler
	binding int
	// draining says ShutdownBinding is draining the socket's binding, which
	// opens no connection for a peer it does not know.
	draining atomic.Bool
	mu       sync.Mutex
	peers    map[netip.AddrPort]*Connection
}

func listenUDP(network string, addrs []string) ([]*udpListener, error) {
	listeners := make([]*udpListener, 0, len(addrs))
	for _, addr := range addrs {
		if addr == "" {
			addr = ":0"
		}
		resolved, err := net.ResolveUDPAddr(network, addr)
		if err == nil {
			var pc *net.UDPConn
			if pc, err = net.ListenUDP(network, resolved); err == nil {
				listeners = append(listeners, &udpListener{pc: pc, peers: make(map[netip.AddrPort]*Connection)})
				continue
			}
		}
		for _, opened := range listeners {
			_ = opened.pc.Close()
		}
		return nil, err
	}
	return listeners, nil
}

// serveUDP reads a UDP socket until the engine stops, queueing each datagram
// on its peer's connection and opening connections for new peers.
func (e *Engine) serveUDP(l *udpListener) error {
	buf := make([]byte, maxDatagramSize)
	for {
		n, addr, err := l.pc.ReadFromUDPAddrPort(buf)
		if err != nil {
			if e.stopping.Load() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			// A failed receive loses only its own datagram.
			continue
		}
		if c := e.udpPeer(l, addr); c != nil {
			c.udpActive.Store(time.Now().UnixNano())
			c.enqueueData(buf[:n])
		}
	}
}

func (e *Engine) udpPeer(l *udpListener, addr netip.AddrPort) *Connection {
	l.mu.Lock()
	c := l.peers[addr]
	l.mu.Unlock()
	if c != nil {
		return c
	}
	c = &Connection{engine: e, handler: l.handler, conn: &udpPeerConn{l: l, addr: addr}, udp: true,
		bindingID: int32(l.binding + 1)}
	c.fd.Store(-1)
	c.udpActive.Store(time.Now().UnixNano())
	e.mu.Lock()
	if e.stopping.Load() || e.draining.Load() || l.draining.Load() {
		e.mu.Unlock()
		return nil
	}
	e.connections[c] = struct{}{}
	e.mu.Unlock()
	l.mu.Lock()
	l.peers[addr] = c
	l.mu.Unlock()
	c.handler.OnOpen(c)
	return c
}

// udpPeerConn is one peer of a UDP listener, as the connection sees it: writes
// are datagrams to the peer, and closing forgets the peer.
type udpPeerConn struct {
	l    *udpListener
	addr netip.AddrPort
}

func (p *udpPeerConn) Write(b []byte) (int, error) { return p.l.pc.WriteToUDPAddrPort(b, p.addr) }

func (p *udpPeerConn) Read([]byte) (int, error) {
	return 0, errors.New("fib: udp peer is read by its listener")
}

func (p *udpPeerConn) Close() error {
	p.l.mu.Lock()
	if c := p.l.peers[p.addr]; c != nil && c.conn == net.Conn(p) {
		delete(p.l.peers, p.addr)
	}
	p.l.mu.Unlock()
	return nil
}

func (p *udpPeerConn) LocalAddr() net.Addr              { return p.l.pc.LocalAddr() }
func (p *udpPeerConn) RemoteAddr() net.Addr             { return net.UDPAddrFromAddrPort(p.addr) }
func (p *udpPeerConn) SetDeadline(time.Time) error      { return nil }
func (p *udpPeerConn) SetReadDeadline(time.Time) error  { return nil }
func (p *udpPeerConn) SetWriteDeadline(time.Time) error { return nil }

// startUDPSweeper closes peers that have been silent for the idle timeout,
// until the engine stops. A timer does the sweeping, and arms itself again
// after each sweep, so nothing waits on it in between.
func (e *Engine) startUDPSweeper() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.startUDPSweeperLocked()
}

// startUDPSweeperLocked is startUDPSweeper for a caller holding mu. It arms
// the timer once, for the engine's first UDP socket.
func (e *Engine) startUDPSweeperLocked() {
	if e.sweeping || len(e.udpListeners) == 0 || e.udpIdleTimeout <= 0 {
		return
	}
	e.sweeping = true
	interval := udpSweepInterval(e.udpIdleTimeout)
	time.AfterFunc(interval, func() { e.sweepUDP(interval) })
}

// sweepUDP closes the peers that have been silent for the idle timeout, and
// arms the next sweep.
func (e *Engine) sweepUDP(interval time.Duration) {
	select {
	case <-e.stopped:
		return
	default:
	}
	limit := int64(e.udpIdleTimeout)
	now := time.Now().UnixNano()
	var idle []*Connection
	e.mu.Lock()
	listeners := append([]*udpListener(nil), e.udpListeners...)
	e.mu.Unlock()
	for _, l := range listeners {
		l.mu.Lock()
		for _, c := range l.peers {
			l := limit
			if d := c.udpIdle.Load(); d != 0 {
				l = d
			}
			if l > 0 && now-c.udpActive.Load() >= l {
				idle = append(idle, c)
			}
		}
		l.mu.Unlock()
	}
	for _, c := range idle {
		c.closeWithError(ErrUDPIdleTimeout)
	}
	time.AfterFunc(interval, func() { e.sweepUDP(interval) })
}

// LocalUDPAddrs returns one address per UDP listener, in configured order.
func (e *Engine) LocalUDPAddrs() ([]*net.UDPAddr, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	addrs := make([]*net.UDPAddr, 0, len(e.udpListeners))
	for _, l := range e.udpListeners {
		addrs = append(addrs, l.pc.LocalAddr().(*net.UDPAddr))
	}
	return addrs, nil
}

// LocalUDPAddr returns the address of the engine's first UDP listener.
func (e *Engine) LocalUDPAddr() (*net.UDPAddr, error) {
	addrs, err := e.LocalUDPAddrs()
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, errors.New("fib: engine has no listener")
	}
	return addrs[0], nil
}

// acceptPlacements says each connection has a goroutine of its own here,
// which Config.AcceptPlacement has no say in.
func (e *Engine) acceptPlacements() (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.listeners) == 0 {
		return "", false
	}
	return "goroutine-per-connection", false
}

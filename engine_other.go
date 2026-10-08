//go:build !linux && !darwin && !windows

package fib

import (
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
	"github.com/lesismal/fib/taskpool"
)

// pollersSupported says Config.IOPollers applies, which it does not to a
// backend that already serves each connection on a goroutine of its own.
const pollersSupported = false

type Engine struct {
	// pollers is always empty here; see pollersSupported.
	pollers         []*Engine
	name            string
	logStatus       bool
	listeners       []net.Listener
	handler         Handler
	taskPool        TaskPool
	releaseTaskPool func()
	taskWG          sync.WaitGroup
	stopping        atomic.Bool
	// stopped closes when Stop is called, which is what Run waits on when
	// there is no listener to serve.
	stopped        chan struct{}
	closeOnce      sync.Once
	mu             sync.Mutex
	connections    map[*Connection]struct{}
	readers        sync.WaitGroup
	readBufferSize int
	// udpListeners are the engine's UDP sockets, when Config.Network names
	// UDP, and udpIdleTimeout closes their silent peers.
	udpListeners   []*udpListener
	udpIdleTimeout time.Duration
}

// root is the engine the application created, which here is always e.
func (e *Engine) root() *Engine { return e }

// Bind creates an engine that listens on config.Addr, or on every address in
// config.Addrs, and serves the connections it accepts with handler.
func Bind(config Config, handler Handler) (*Engine, error) {
	addrs := config.Addrs
	if len(addrs) == 0 {
		addrs = []string{config.Addr}
	}
	return newEngine(config, handler, addrs)
}

// NewEngine creates an engine with no listeners, for connections it dials.
func NewEngine(config Config, handler Handler) (*Engine, error) {
	return newEngine(config, handler, nil)
}

func newEngine(config Config, handler Handler, addrs []string) (*Engine, error) {
	if err := config.validateTaskPool(); err != nil {
		return nil, err
	}
	if config.MaxEvents <= 0 {
		config.MaxEvents = 256
	}
	if config.ReadBufferSize <= 0 {
		config.ReadBufferSize = 16 * 1024
	}
	if handler == nil {
		handler = HandlerFuncs{}
	}
	network := config.Network
	if network == "" {
		network = "tcp"
	}
	var udpListeners []*udpListener
	if isUDPNetwork(network) {
		var err error
		if udpListeners, err = listenUDP(network, addrs); err != nil {
			return nil, err
		}
		addrs = nil
	}
	listeners := make([]net.Listener, 0, len(addrs))
	for _, addr := range addrs {
		if addr == "" {
			addr = ":0"
		}
		listener, err := net.Listen(network, addr)
		if err != nil {
			for _, opened := range listeners {
				_ = opened.Close()
			}
			return nil, err
		}
		listeners = append(listeners, listener)
	}
	pool, releasePool := acquireTaskPool(config)
	e := &Engine{name: engineName(config), logStatus: config.LogStatus, listeners: listeners, handler: handler, taskPool: pool, releaseTaskPool: releasePool,
		connections: make(map[*Connection]struct{}), stopped: make(chan struct{}),
		udpListeners: udpListeners, udpIdleTimeout: udpIdleTimeout(config.UDPIdleTimeout)}
	e.readBufferSize = config.ReadBufferSize
	e.startUDPSweeper()
	return e, nil
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
type Stats struct {
	ReadsPausedByWatermark uint64
	ReadsPausedByBudget    uint64
	ReadsResumed           uint64
	PendingBytes           int64
}

func (e *Engine) Stats() Stats { return Stats{} }

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

// ListenAddrs returns the address of every listener, in configured order,
// whatever its network.
func (e *Engine) ListenAddrs() ([]net.Addr, error) {
	addrs := make([]net.Addr, 0, len(e.listeners)+len(e.udpListeners))
	for _, listener := range e.listeners {
		addrs = append(addrs, listener.Addr())
	}
	for _, l := range e.udpListeners {
		addrs = append(addrs, l.pc.LocalAddr())
	}
	return addrs, nil
}

// LocalAddrs returns one address per TCP listener, in configured order.
func (e *Engine) LocalAddrs() ([]*net.TCPAddr, error) {
	addrs := make([]*net.TCPAddr, 0, len(e.listeners))
	for _, listener := range e.listeners {
		addr, ok := listener.Addr().(*net.TCPAddr)
		if !ok {
			return nil, errors.New("listener is not TCP")
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

// Run serves every listener until the server is stopped. It returns the first
// error any of them reported.
func (e *Engine) Run() error {
	e.logRun()
	if len(e.listeners) == 0 && len(e.udpListeners) == 0 {
		// A client engine's connections are served by their own readers, so
		// Run only has to last as long as the engine does.
		<-e.stopped
		return nil
	}
	if len(e.listeners) == 1 && len(e.udpListeners) == 0 {
		return e.serve(e.listeners[0])
	}
	errs := make(chan error, len(e.listeners)+len(e.udpListeners))
	var serving sync.WaitGroup
	for _, listener := range e.listeners {
		serving.Add(1)
		go func(listener net.Listener) {
			defer serving.Done()
			errs <- e.serve(listener)
		}(listener)
	}
	for _, l := range e.udpListeners {
		serving.Add(1)
		go func(l *udpListener) {
			defer serving.Done()
			errs <- e.serveUDP(l)
		}(l)
	}
	serving.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) serve(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if e.stopping.Load() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		e.adopt(conn, e.handler, nil)
	}
}

// adopt starts serving an established connection: OnOpen, then done if the
// connection was dialed, then its reader. It reports false, and closes conn,
// if the engine has stopped in the meantime.
func (e *Engine) adopt(conn net.Conn, handler Handler, done func(*Connection, error)) bool {
	return e.adoptWith(conn, handler, done, false, false)
}

// adoptWith is adopt for a connection that may be a dialed one, a dialed UDP
// one among them.
func (e *Engine) adoptWith(conn net.Conn, handler Handler, done func(*Connection, error), udp, dialed bool) bool {
	c := &Connection{engine: e, handler: handler, conn: conn, udp: udp, dialed: dialed}
	if addr := conn.LocalAddr(); !udp && addr != nil && addr.Network() == "unix" {
		c.unix = true
	}
	c.fd.Store(-1)
	e.mu.Lock()
	if e.stopping.Load() {
		e.mu.Unlock()
		_ = conn.Close()
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
		if err == nil && !e.adoptWith(conn, handler, done, isUDPNetwork(network), true) {
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
	e.mu.Lock()
	delete(e.connections, c)
	e.mu.Unlock()
	c.handler.OnClose(c, err)
}
func (e *Engine) Stop() {
	if e.stopping.CompareAndSwap(false, true) {
		close(e.stopped)
		for _, listener := range e.listeners {
			_ = listener.Close()
		}
		for _, l := range e.udpListeners {
			_ = l.pc.Close()
		}
	}
}
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
		e.readers.Wait()
		e.taskWG.Wait()
		e.releaseTaskPool()
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
	// Network and Addr name the listener the way net.Listen does: Network is
	// "tcp", "tcp4" or "tcp6", and Addr is a "host:port" such as ":9000",
	// "127.0.0.1:9000" or "[::1]:9000". An empty Network means "tcp", and an
	// empty Addr means ":0". "unix" listens on a Unix socket at the path Addr.
	// "udp", "udp4" and "udp6" bind UDP sockets, whose peers each become a
	// connection, as on the native backends.
	Network string
	Addr    string
	// Addrs, when it is not empty, is the complete set of addresses to listen
	// on and Addr is ignored; they all share Network. One server spanning
	// several addresses shares its connection table, task pool and buffer pool
	// across all of them.
	Addrs                           []string
	Backlog, WorkerCount, MaxEvents int
	ReadBufferSize                  int
	WriteBufferHighWatermark        int
	UseWritev                       bool
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
	// IOPollers and IOPollerCount split a Linux or macOS engine across
	// several event loops. This backend has one goroutine per connection
	// already, so both are ignored here, though DefaultConfig sets IOPollers
	// as it does on the other backends: on more than four CPUs.
	IOPollers     bool
	IOPollerCount int
	// ReusePort binds a Linux or macOS engine's listeners with SO_REUSEPORT.
	// This backend listens through the net package, and ignores it.
	ReusePort bool
	// UDPIdleTimeout closes a silent UDP peer's connection. Zero means
	// DefaultUDPIdleTimeout and a negative value keeps peers until closed.
	UDPIdleTimeout time.Duration
	// customPoolSizing records that SetPoolSizing pinned the sizing, so that a
	// later SetTaskPoolMode does not overwrite it.
	customPoolSizing bool
}

func DefaultConfig() Config {
	sizing := DefaultPoolSizing(taskpool.ModeAdaptive)
	return Config{Name: DefaultName, Network: "tcp", Addr: ":9000", Backlog: 128, WorkerCount: sizing.WorkerCount, MaxEvents: sizing.MaxEvents, ReadBufferSize: 16 * 1024, WriteBufferHighWatermark: 4 * 1024, UseWritev: true, SocketSyscalls: true, TaskPoolMode: taskpool.ModeAdaptive, SharedTaskPool: true, IOPollers: defaultIOPollers(defaultCPUs())}
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
	writeMu             sync.Mutex
	attachment          atomic.Pointer[connectionAttachment]
	layer               Layer
	// udp marks a connection that exchanges datagrams: a peer of a UDP
	// listener, whose conn is a udpPeerConn, or a dialed UDP socket.
	udp bool
	// unix marks a Unix socket.
	unix bool
	// dialed marks a connection Dial opened, as against one a listener
	// accepted.
	dialed bool
	// udpActive is when a listener's peer last sent or was sent a datagram,
	// in nanoseconds, for the idle timeout.
	udpActive atomic.Int64
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

// RemoteAddr returns the peer's address.
func (c *Connection) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }

// RemoteAddrPort returns the peer's IP address and port, or the zero
// AddrPort for a peer without one, or with an IPv6 zone.
func (c *Connection) RemoteAddrPort() netip.AddrPort {
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

// SendOwned is equivalent to Send on the synchronous portable backend.
func (c *Connection) SendOwned(data []byte) error { return c.Send(data) }

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

// sendClosed reports whether the connection has stopped accepting sends.
func (c *Connection) sendClosed() bool {
	c.mu.Lock()
	closing := c.closing
	c.mu.Unlock()
	return closing
}

// closeAfterSendRaw closes the connection. Sends on this backend have already
// reached the socket by the time they return, so nothing is left to wait for.
func (c *Connection) closeAfterSendRaw() { c.closeWithError(nil) }

func (c *Connection) enqueueData(data []byte) bool {
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		return false
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
	pc    *net.UDPConn
	mu    sync.Mutex
	peers map[netip.AddrPort]*Connection
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
	c = &Connection{engine: e, handler: e.handler, conn: &udpPeerConn{l: l, addr: addr}, udp: true}
	c.fd.Store(-1)
	c.udpActive.Store(time.Now().UnixNano())
	e.mu.Lock()
	if e.stopping.Load() {
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
	if len(e.udpListeners) == 0 || e.udpIdleTimeout <= 0 {
		return
	}
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
	for _, l := range e.udpListeners {
		l.mu.Lock()
		for _, c := range l.peers {
			if now-c.udpActive.Load() >= limit {
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

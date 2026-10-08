//go:build linux || darwin || windows

package fib

import (
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/internal/sys"
	"github.com/lesismal/fib/streampool"
	"github.com/lesismal/fib/taskpool"
)

const (
	maxWritevItems = 64
	// maxWaitBatch caps the buffer one wait for events fills. MaxEvents keeps
	// sizing the task queue; beyond this many events per wait the loop just
	// waits again, so a larger buffer only costs memory per server.
	maxWaitBatch = 1024
	// defaultWriteHighWatermark is the per-connection outbound budget. Two
	// costs sit either side of it. Lower, and ordinary traffic crosses it on
	// every reply, and each crossing costs an eventfd write and an epoll_ctl:
	// at 4KB that churn was 16% of a 100k-connection profile. Higher, and a
	// backlog grows past the buffer size the pool retains: at 64KB against the
	// default 16KB read buffer, a 15-second rate test allocated 13.7GB against
	// the same run's 254MB live. The default sits at that upper end, favouring
	// fewer pauses for bursty peers over allocation under sustained backlog;
	// memory-sensitive deployments with many connections should lower it.
	defaultWriteHighWatermark = 64 << 10
	// defaultMaxPendingBytes is the server-wide outbound budget. It is the
	// bound that actually holds at high connection counts, where the
	// per-connection watermark alone would admit its own limit times the
	// connection count.
	defaultMaxPendingBytes = 1 << 30
)

// Readiness a connection accumulates between rounds. The values are epoll's,
// so the Linux backend passes its events through untranslated; kqueue and IOCP
// translate what they observe into the same bits.
const (
	evIn    uint32 = 0x1
	evPri   uint32 = 0x2
	evOut   uint32 = 0x4
	evErr   uint32 = 0x8
	evHup   uint32 = 0x10
	evRdHup uint32 = 0x2000
	evAll          = evIn | evPri | evOut | evErr | evHup | evRdHup
)

type commandType uint8

const (
	commandRefresh commandType = iota
	commandClose
	commandDial
	commandDialTimeout
	commandUDPSweep
	// commandAccept hands a poller a connection the engine's own loop
	// accepted; see Config.IOPollers.
	commandAccept
	// commandDetach releases the descriptor of a connection whose last
	// round has run its OnClose; see closeConnection.
	commandDetach
)

type command struct {
	kind       commandType
	connection *Connection
	dial       *dialRequest
	err        error
}
type commandBatch struct{ items []command }

// Engine owns the listeners, the event loop, its command queue, and the worker
// pool. The loop itself is platform code: epoll on Linux, kqueue on macOS and
// an I/O completion port on Windows, each embedded here as enginePlatform.
//
// With Config.IOPollers, the engine also owns the pollers its connections are
// served on. Each poller is an Engine of its own with no listeners, whose
// loop, command queue and descriptor table serve only the connections handed
// to it, while the settings, the handler, the task pool and the server-wide
// budget are the parent's.
type Engine struct {
	enginePlatform
	// parent is the engine a poller serves connections for, and nil for an
	// engine the application created.
	parent *Engine
	// pollers are the loops the engine hands its connections to; see
	// Config.IOPollers. Empty when the engine serves them itself.
	pollers            []*Engine
	name               string
	logStatus          bool
	maxEvents          int
	useWritev          bool
	socketSyscalls     bool // see Config.SocketSyscalls
	writeHighWatermark int
	writeLowWatermark  int
	maxPendingBytes    int64
	budgetResumeBytes  int64
	sendBufferSize     int
	handler            Handler
	stopping           atomic.Bool
	// pendingTotal is the outbound total MaxPendingBytes bounds, which an
	// engine's pollers share with it.
	pendingTotal *atomic.Int64
	// Backpressure counters, reported by Stats. They move only when a
	// connection's read interest actually changes, which is rare by design.
	readsPausedByWatermark atomic.Uint64
	readsPausedByBudget    atomic.Uint64
	readsResumed           atomic.Uint64
	commandMu              sync.Mutex
	commands               *commandBatch
	// commandsClosed turns requests away once Close has drained the queue for
	// the last time, since nothing would ever run what arrived after it.
	// Guarded by commandMu.
	commandsClosed bool
	commandPool    sync.Pool
	wakePending    atomic.Bool
	// budgetPaused holds connections whose reads the server-wide budget stopped,
	// waiting to be resumed once it recovers. Event-loop ownership.
	budgetPaused []*Connection
	// budgetResume is the spare list resumeBudgetPaused swaps in while it walks
	// the current one. Event-loop ownership.
	budgetResume []*Connection
	// budgetWaiting says budgetPaused is not empty, for whichever goroutine
	// drains the budget, which with pollers may belong to another loop and
	// has to wake this one; see releaseBudget.
	budgetWaiting atomic.Bool
	// redeliver holds connections the loop itself made runnable, to be
	// scheduled at the end of the round: stalled ones whose read it hands
	// back, and closed ones whose last round is to run OnClose. Event-loop
	// ownership.
	redeliver       []*Connection
	taskPool        TaskPool
	releaseTaskPool func()
	taskWG          sync.WaitGroup
	readBufferSize  int
	closeOnce       sync.Once
	// udpListeners are the engine's UDP sockets, when Config.Network names
	// UDP. udpIdleTimeout closes their silent peers, and udpSweep is the
	// timer that has the loop check for them, nil once it has been stopped.
	// udpSweepMu guards it.
	udpListeners   []*udpListener
	udpIdleTimeout time.Duration
	udpSweepMu     sync.Mutex
	udpSweep       *time.Timer
	// unixPaths are the socket files the engine's Unix listeners created,
	// which it removes when it closes, as net.UnixListener does.
	unixPaths []string
}

// removeUnixPaths removes the socket files the engine created.
func (e *Engine) removeUnixPaths() {
	for _, path := range e.unixPaths {
		_ = os.Remove(path)
	}
	e.unixPaths = nil
}

// noteUnixPath records a socket file a listener just created.
func (e *Engine) noteUnixPath(network, path string) {
	if isUnixNetwork(network) && !isAbstractUnixPath(path) {
		e.unixPaths = append(e.unixPaths, path)
	}
}

// acquireSendBuffer returns an empty outbound buffer from the shared pool,
// with room for a whole round's replies.
//
// One size for every buffer is deliberate, even though the pool has classes
// for every other size. Asking for a buffer shaped to the backlog was measured
// twice and helped neither: a 4KB class left 736MB of buffers about a third
// full, and stepping down to 1KB classes made it worse still at 1181MB,
// because each class retains idle buffers of its own. Resident peak did not
// move for any of them, so the bound that matters is MaxPendingBytes, not the
// shape of the buffers underneath it.
func (e *Engine) acquireSendBuffer() []byte {
	return bufferpool.Get(e.sendBufferSize)[:0]
}

// releaseSendBuffer hands a buffer back. A buffer a single large message grew
// past the size it was taken at goes back too, since it returns to the class
// its capacity belongs to rather than to the one a round's replies are served
// from: an outsized buffer is then reused only by another message that size,
// and cannot leave the ordinary buffers inflated the way one shared pool would.
func (e *Engine) releaseSendBuffer(data []byte) {
	bufferpool.Put(data)
}

// Bind creates an engine that listens on config.Addr, or on every address in
// config.Addrs, and serves the connections it accepts with handler.
func Bind(config Config, handler Handler) (*Engine, error) {
	addrs := config.Addrs
	if len(addrs) == 0 {
		addrs = []string{config.Addr}
	}
	return newEngine(config, handler, addrs)
}

// NewEngine creates an engine with no listeners. Its connections are the ones
// it dials, which makes it a client engine; config.Addr and config.Addrs are
// ignored. handler serves connections dialed with Dial, and may be nil when
// every dial names its own handler through DialWithHandler.
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
	if config.WriteBufferHighWatermark == 0 {
		config.WriteBufferHighWatermark = 4 * 1024
	}
	if config.Backlog <= 0 {
		config.Backlog = sys.DefaultBacklog()
	}
	if handler == nil {
		handler = HandlerFuncs{}
	}
	if preforkChild && len(addrs) > 0 {
		config.ReusePort = true
	}

	e := &Engine{name: engineName(config), logStatus: config.LogStatus, maxEvents: config.MaxEvents,
		useWritev:          config.UseWritev,
		socketSyscalls:     config.SocketSyscalls,
		writeHighWatermark: config.WriteBufferHighWatermark,
		// Resume at a quarter of the budget rather than at the budget itself,
		// so recovery admits a useful amount of work instead of re-pausing on
		// the first reply.
		writeLowWatermark: config.WriteBufferHighWatermark / 4,
		maxPendingBytes:   config.MaxPendingBytes,
		budgetResumeBytes: config.MaxPendingBytes / 4,
		// An outbound buffer is sized to one read round's replies, not to the
		// write watermark. The watermark bounds the bytes a connection may
		// have pending; it does not bound the capacity of the buffer holding
		// them, and a buffer taken at the watermark would be pooled at that
		// size and handed on to the next connection. At high connection
		// counts that capacity, not the pending bytes, is what the process
		// actually pays for: buffers at the watermark held 908MB against
		// 256MB of pending data.
		//
		// The allowance is twice the round size because replies carry framing
		// on top of the bytes that arrived, and buffers are taken at it rather
		// than empty: starting smaller costs a reallocation per round on every
		// connection, and starting from empty costs one per doubling. Empty
		// cost 49.5GB of allocation across a 15-second rate test, and an
		// exact-fit 16KB still cost 12.4GB. Aligning it to a pool class means
		// the buffer a round is given is exactly the class it comes from.
		sendBufferSize: bufferpool.Align(2 * config.ReadBufferSize),
		udpIdleTimeout: udpIdleTimeout(config.UDPIdleTimeout),
		pendingTotal:   new(atomic.Int64),
		handler:        handler}
	e.readBufferSize = config.ReadBufferSize
	e.taskPool, e.releaseTaskPool = acquireTaskPool(config)
	if err := e.open(config, addrs); err != nil {
		e.releaseTaskPool()
		return nil, err
	}
	if err := e.openPollers(config); err != nil {
		_ = e.Close()
		return nil, err
	}
	e.startUDPSweeper()
	for _, p := range e.pollers {
		p.startUDPSweeper()
	}
	return e, nil
}

// openPollers creates the loops Config.IOPollers asks for. A poller is
// created as an engine without listeners and shares everything else with
// this one.
func (e *Engine) openPollers(config Config) error {
	n := pollerCount(config)
	for i := 0; i < n; i++ {
		p := &Engine{parent: e, name: e.name, logStatus: e.logStatus, maxEvents: e.maxEvents, useWritev: e.useWritev,
			socketSyscalls:     e.socketSyscalls,
			writeHighWatermark: e.writeHighWatermark,
			writeLowWatermark:  e.writeLowWatermark, maxPendingBytes: e.maxPendingBytes,
			budgetResumeBytes: e.budgetResumeBytes, sendBufferSize: e.sendBufferSize,
			udpIdleTimeout: e.udpIdleTimeout, pendingTotal: e.pendingTotal, handler: e.handler,
			taskPool: e.taskPool, releaseTaskPool: func() {},
			readBufferSize: e.readBufferSize}
		if err := p.open(config, nil); err != nil {
			return err
		}
		e.pollers = append(e.pollers, p)
	}
	return nil
}

// HandlerPool returns the pool the protocols served on the engine run their
// request handlers on, away from the worker that reads the connection, as
// HTTP/2 and HTTP/3 run theirs: "<Name>-streams", apart from the engine's own
// pool for the reason package streampool gives. It returns nil once
// the engine has closed, and the caller then runs the handler itself.
func (e *Engine) HandlerPool() *taskpool.TaskPool {
	sizing := DefaultStreamPoolSizing(taskpool.ModeAdaptive)
	return streampool.Get(e.root().name, sizing.WorkerCount, sizing.MaxEvents)
}

// root is the engine the application created: this one, or the one this
// poller serves.
func (e *Engine) root() *Engine {
	if e.parent != nil {
		return e.parent
	}
	return e
}

// errNoListener is what LocalAddr reports for an engine made by NewEngine.
var errNoListener = errors.New("fib: engine has no listener")

// LocalAddr returns the address of the server's first listener.
func (e *Engine) LocalAddr() (*net.TCPAddr, error) {
	addrs, err := e.LocalAddrs()
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, errNoListener
	}
	return addrs[0], nil
}

// Stats reports what backpressure this server has applied. It answers the
// question the counters exist for: whether reads were ever paused, and which
// of the two bounds did it, since the two have very different causes.
type Stats struct {
	// ReadsPausedByWatermark counts the times a connection's reads were paused
	// because that connection's own queued output reached
	// WriteBufferHighWatermark. Its peer is not keeping up with its replies.
	ReadsPausedByWatermark uint64
	// ReadsPausedByBudget counts the times a connection's reads were paused
	// because MaxPendingBytes was exhausted across the whole server. Such a
	// connection may be holding almost nothing itself: it is paying for what
	// the others have queued, so a connection well under its own watermark can
	// still be stopped this way.
	ReadsPausedByBudget uint64
	// ReadsResumed counts the pauses that have since been lifted.
	ReadsResumed uint64
	// PendingBytes is the outbound total queued across this server's
	// connections right now that their sockets have not taken, which is what
	// MaxPendingBytes bounds. The replies a read round holds under its cork
	// until it ends count only if its flush leaves them queued.
	PendingBytes int64
}

// Stats covers the engine's pollers too, since they serve its connections.
func (e *Engine) Stats() Stats {
	s := Stats{
		ReadsPausedByWatermark: e.readsPausedByWatermark.Load(),
		ReadsPausedByBudget:    e.readsPausedByBudget.Load(),
		ReadsResumed:           e.readsResumed.Load(),
		PendingBytes:           e.pendingTotal.Load(),
	}
	for _, p := range e.pollers {
		s.ReadsPausedByWatermark += p.readsPausedByWatermark.Load()
		s.ReadsPausedByBudget += p.readsPausedByBudget.Load()
		s.ReadsResumed += p.readsResumed.Load()
	}
	return s
}

// runReady ends one round of the loop: it hands the connections the round made
// runnable to the workers, and then gives connections parked on the
// server-wide budget a chance to resume.
func (e *Engine) runReady(ready []*Connection, tasks []taskpool.Task) ([]*Connection, []taskpool.Task) {
	// Resuming comes first so that a connection it hands a read back to is
	// scheduled in this round rather than whenever the loop next wakes.
	e.resumeBudgetPaused()
	ready = append(ready, e.redeliver...)
	clear(e.redeliver)
	e.redeliver = e.redeliver[:0]
	if len(ready) > 0 {
		tasks = e.submitReady(e.taskPool, ready, tasks[:0])
		// The workers just woken wait on this goroutine's P, and the loop is
		// about to wait for events in a system call that keeps the P out of
		// use: while there is other work, the runtime hands it on only after
		// the call has lasted a while, and the loop then waits for a P of its
		// own behind whatever is runnable. Yielding first runs the workers
		// here straight away. Measured on Linux with 4 Ps serving HTTP/2
		// echoes over 10k connections, it raised throughput by 10% to 25%,
		// depending on the client, from a loop that had spent 40% of its time
		// runnable and waiting for a P after each wait.
		runtime.Gosched()
		clear(ready)
		ready = ready[:0]
	}
	return ready, tasks
}

// resumeBudgetPaused re-arms reads on connections the server-wide budget held
// back, once enough of that budget has drained. They are re-examined rather
// than resumed outright: a connection that has since built a backlog of its own
// stays paused on its own account, and simply goes back on the list.
func (e *Engine) resumeBudgetPaused() {
	if len(e.budgetPaused) == 0 || e.pendingTotal.Load() > e.budgetResumeBytes {
		return
	}
	// Swap in the spare list before refreshing. A connection that is still held
	// back goes straight back onto s.budgetPaused, which therefore must not
	// share an array with the one being iterated.
	waiting := e.budgetPaused
	e.budgetPaused = e.budgetResume[:0]
	for _, c := range waiting {
		// Clear the flag first: it is what stops a connection already on the
		// list from being added twice, so leaving it set would drop a
		// connection that turns out to still need the budget.
		c.mu.Lock()
		c.budgetPaused = false
		c.mu.Unlock()
		e.refreshConnection(c)
	}
	clear(waiting)
	e.budgetResume = waiting
	e.budgetWaiting.Store(len(e.budgetPaused) > 0)
}

// releaseBudget returns n bytes to the server-wide budget. Draining it below
// the level paused connections resume at wakes every loop holding some, since
// the loop whose connection drained it need not be theirs, and a loop whose
// connections are all paused has nothing else to wake it.
//
// A loop that parks a connection sets budgetWaiting before the same round
// checks the budget in resumeBudgetPaused, so a drain lands either before
// that check, which then sees it, or after the flag, which this then sees.
func (e *Engine) releaseBudget(n int64) {
	total := e.pendingTotal.Add(-n)
	if e.maxPendingBytes <= 0 || total > e.budgetResumeBytes || total+n <= e.budgetResumeBytes {
		return
	}
	root := e.root()
	if root.budgetWaiting.Load() {
		root.notify()
	}
	for _, p := range root.pollers {
		if p.budgetWaiting.Load() {
			p.notify()
		}
	}
}

// submitReady hands one round's newly runnable connections to pool in a
// single batch instead of one lock-and-wake cycle per connection. A nil pool,
// which is a pool of workers asked for after the engine closed, takes none.
func (e *Engine) submitReady(pool TaskPool, ready []*Connection, tasks []taskpool.Task) []taskpool.Task {
	for _, c := range ready {
		tasks = append(tasks, c)
	}
	e.taskWG.Add(len(tasks))
	accepted := 0
	if pool != nil {
		accepted = pool.GoTasks(tasks)
	}
	for _, c := range ready[accepted:] {
		e.taskWG.Done()
		c.mu.Lock()
		c.scheduled = false
		closing := c.closePending
		c.closePending = false
		c.mu.Unlock()
		if closing {
			// The round that was to finish the close will not run, so the
			// close is finished here instead.
			c.finishClose()
		} else {
			c.Close()
		}
	}
	clear(tasks)
	return tasks
}

// Stop makes Run return, once it has stopped the engine's pollers too.
func (e *Engine) Stop() {
	e.stopping.Store(true)
	e.notify()
	for _, p := range e.pollers {
		p.Stop()
	}
}

// request queues a command for the event loop. It reports false, and queues
// nothing, once Close has drained the queue for the last time.
func (e *Engine) request(cmd command) bool {
	e.commandMu.Lock()
	if e.commandsClosed {
		e.commandMu.Unlock()
		return false
	}
	if e.commands == nil {
		if pooled := e.commandPool.Get(); pooled != nil {
			e.commands = pooled.(*commandBatch)
		} else {
			e.commands = &commandBatch{}
		}
	}
	e.commands.items = append(e.commands.items, cmd)
	e.commandMu.Unlock()
	e.notify()
	return true
}

// closeCommands drains the command queue one last time and turns away every
// request after it. Close calls it once no worker can queue anything more.
func (e *Engine) closeCommands() {
	e.commandMu.Lock()
	e.commandsClosed = true
	e.commandMu.Unlock()
	e.drainCommands()
	// No loop or worker is left to run the rounds the loop still owes, and
	// closes above all must not lose their OnClose. The closed connections'
	// last rounds run here; the rest are closed with the engine.
	for _, c := range e.redeliver {
		c.mu.Lock()
		closing := c.closePending
		c.mu.Unlock()
		if closing {
			c.process()
		}
	}
	clear(e.redeliver)
	e.redeliver = e.redeliver[:0]
}

// notify wakes the event loop, coalescing requests that arrive before it has
// woken into a single wake-up. A loop that is awake needs no wake-up at all,
// since it looks at its queue before it waits again; see settle.
func (e *Engine) notify() {
	if !e.wakePending.CompareAndSwap(false, true) {
		return
	}
	e.wake()
}

// maxSettleRounds bounds how many times settle goes back to the queue before
// the loop waits, so that commands that keep arriving cannot keep it from the
// sockets.
const maxSettleRounds = 8

// settle ends one pass of the loop: it runs what was queued for the loop
// while it was awake, and the rounds that queues, until nothing is left, and
// then lets the next request wake it again. Callers run on the event loop,
// which marks itself awake, with wakePending, when its wait returns.
//
// While the loop is awake a request costs no wake-up: the rounds a loop runs
// itself queue commands for it as they close connections, the close and
// then the release of the descriptor, and waking itself for each was a
// write to the wake-up descriptor, and another wait that returned at once
// to read it, twice for every connection that ended.
//
// The flag is cleared before the queue is looked at, so a request lands
// either before the look, which sees it, or after the flag, which wakes the
// loop from the wait that follows.
func (e *Engine) settle(ready []*Connection, tasks []taskpool.Task) ([]*Connection, []taskpool.Task) {
	for i := 0; ; i++ {
		e.wakePending.Store(false)
		if !e.commandsQueued() && len(e.redeliver) == 0 {
			return ready, tasks
		}
		if i == maxSettleRounds {
			// What is left runs after the next wait, which this wake-up
			// keeps from blocking.
			e.notify()
			return ready, tasks
		}
		e.wakePending.Store(true)
		e.drainCommands()
		ready, tasks = e.runReady(ready, tasks)
	}
}

// commandsQueued reports whether requests are waiting for the loop.
func (e *Engine) commandsQueued() bool {
	e.commandMu.Lock()
	queued := e.commands != nil && len(e.commands.items) > 0
	e.commandMu.Unlock()
	return queued
}

// drainWake answers a wake-up by running the commands it announced, for a
// loop that has no settle step and so stays asleep between wake-ups, as far
// as notify can tell.
func (e *Engine) drainWake() {
	e.ackWake()
	e.wakePending.Store(false)
	e.drainCommands()
}

// noteEvent folds readiness into the connection and reports whether it needs
// to be scheduled. Actual submission happens once per round in Run.
func (e *Engine) noteEvent(c *Connection, events uint32) *Connection {
	events &= evAll
	if events == 0 {
		// Nothing to fold in, such as kqueue's except filter firing with
		// every read; the lock, which a worker may be holding across a
		// write, is not worth waiting for.
		return nil
	}
	c.mu.Lock()
	if c.closing || c.closed {
		c.mu.Unlock()
		return nil
	}
	if events == evOut && c.sendHead == len(c.sends) && c.pendingEvents == 0 {
		// Write interest is armed for the connection's whole life, so the
		// socket reports itself writable the moment it is registered and again
		// every time it drains. With nothing queued there is nothing for a
		// worker to do, and the edge that does matter, the one after a write
		// stops short, always arrives later. Skipping the wake-up keeps an
		// accept burst from scheduling every new connection a second time.
		c.mu.Unlock()
		return nil
	}
	// Readiness is level information from the connection's point of view.
	// Coalescing duplicate notifications avoids a slice scan and keeps a hot
	// connection from allocating an unbounded event queue while its worker is
	// draining the socket.
	c.pendingEvents |= events
	submit := !c.scheduled
	c.scheduled = true
	c.mu.Unlock()
	if !submit {
		return nil
	}
	return c
}

// drainCommands runs the commands queued for the loop. Callers run on the
// event loop, and acknowledge the wake-up that announced them, if one did,
// themselves.
func (e *Engine) drainCommands() {
	e.commandMu.Lock()
	batch := e.commands
	e.commands = nil
	e.commandMu.Unlock()
	if batch == nil {
		return
	}
	for _, cmd := range batch.items {
		switch cmd.kind {
		case commandClose:
			e.closeConnection(cmd.connection, cmd.err, true)
		case commandDial:
			e.startDial(cmd.dial)
		case commandDialTimeout:
			e.expireDial(cmd.connection, cmd.dial)
		case commandUDPSweep:
			e.sweepUDP()
		case commandAccept:
			e.admitAccepted(cmd.connection)
		case commandDetach:
			e.detach(cmd.connection)
		default:
			e.refreshConnection(cmd.connection)
		}
	}
	for i := range batch.items {
		batch.items[i] = command{}
	}
	if cap(batch.items) <= e.maxEvents {
		batch.items = batch.items[:0]
		e.commandPool.Put(batch)
	}
}

func (e *Engine) refreshConnection(c *Connection) {
	c.mu.Lock()
	usable := !c.closing && !c.closed
	pauseReads, byBudget := c.pauseDecision(c.readPaused)
	changed := c.readPaused != pauseReads
	c.readPaused = pauseReads
	track := usable && pauseReads && byBudget && !c.budgetPaused
	if track {
		c.budgetPaused = true
	}
	if !pauseReads {
		c.budgetPaused = false
	}
	// A stalled read is settled once reads are running again. Resuming them
	// re-arms the backend, and re-arming reports bytes already waiting, so
	// only a connection that was never paused needs its read handed back.
	redeliver := false
	if c.readStalled && !pauseReads {
		c.readStalled = false
		redeliver = usable && !changed
	}
	c.mu.Unlock()
	if redeliver {
		if ready := e.noteEvent(c, evIn); ready != nil {
			e.redeliver = append(e.redeliver, ready)
		}
	}
	if track {
		// A connection held back only by the server-wide budget may have
		// nothing of its own left to flush, so no later event of its own would
		// re-evaluate it. The loop resumes it when the budget recovers.
		e.budgetPaused = append(e.budgetPaused, c)
		e.budgetWaiting.Store(true)
	}
	if changed {
		switch {
		case !pauseReads:
			e.readsResumed.Add(1)
		case byBudget:
			e.readsPausedByBudget.Add(1)
		default:
			e.readsPausedByWatermark.Add(1)
		}
	}
	if usable && changed {
		// Write interest is permanent, so the backend only has to track
		// whether reads are paused while the peer catches up.
		if err := e.setReadPaused(c, pauseReads); err != nil {
			c.closeWithError(err)
		}
	}
}

// closeConnection closes a connection from the loop. The connection stops at
// once: nothing more is read or sent, and its queue and its share of the
// budget are let go. What remains is ordered with its rounds the way an event
// is, as noteEvent orders readiness: the close is folded into the connection
// and a round scheduled for it, unless one already is, and that round runs
// OnClose after whatever it or the round before it was delivering, then has
// the loop release the descriptor. A round still running on a worker thus
// never has OnClose run beside it, nor its descriptor closed, and possibly
// handed to another connection, under it.
//
// Without callback, which is how Close ends what is left once no round can
// run any more, the descriptor is released on the spot and nothing is told.
func (e *Engine) closeConnection(c *Connection, closeErr error, callback bool) {
	if c.dialing != nil {
		// The dialer has not been handed the connection yet, so it is the one
		// to hear about the close, and the handler never hears of it at all.
		if closeErr == nil {
			closeErr = net.ErrClosed
		}
		e.failDial(c, closeErr)
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		if !callback {
			// A close still waiting for its last round, which will now never
			// run.
			e.detach(c)
		}
		return
	}
	c.closed = true
	c.closing = true
	// Record why, so a Read or Write that comes back to the connection is
	// told what happened rather than only that it is closed, and drop the
	// deadline timers, which have nothing left to close.
	c.closeReason = closeErr
	c.stopDeadlinesLocked()
	// Buffers the kernel is still sending from cannot go back to the pool: the
	// next connection to take one would overwrite bytes still on their way out.
	// They are left to the garbage collector instead.
	if !c.writeBusyLocked() {
		for i := c.sendHead; i < len(c.sends); i++ {
			c.releaseItemLocked(&c.sends[i])
		}
	}
	c.sends = nil
	c.sendHead = 0
	c.budgetPaused = false
	// Drop this connection's share of the server-wide budget in the same step
	// that abandons its queue, so a closed connection cannot hold the budget
	// against the ones still running.
	c.pendingBytes.Store(0)
	if charged := c.charged; charged > 0 {
		c.charged = 0
		e.releaseBudget(charged)
	}
	if !callback {
		c.mu.Unlock()
		e.detach(c)
		return
	}
	c.closePending = true
	schedule := !c.scheduled
	c.scheduled = true
	c.mu.Unlock()
	if c.udp != nil && c.udp.listener != nil {
		// A listener's peer has no descriptor of its own to be reused under
		// a round, so it leaves the peer table now: the peer's next datagram
		// opens a fresh connection rather than being dropped on this one
		// while its OnClose is still to run.
		e.detachPeer(c)
	}
	if schedule {
		e.redeliver = append(e.redeliver, c)
	}
}

var _ io.Closer = (*Engine)(nil)

// Config controls listener and worker-pool sizing.
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
	// "127.0.0.1:9000" or "[::1]:9000". A host resolves through the net
	// package, and a zero port asks the kernel to choose one. An empty Network
	// means "tcp", and an empty Addr means ":0", again as net.Listen reads
	// them.
	//
	// "tcp" with no host listens on both families where the kernel has IPv6,
	// "tcp4" and "tcp6" pin it to one. This is the same choice net.Listen
	// makes from the same arguments.
	//
	// "unix" listens on a Unix domain stream socket, with Addr as its path, as
	// net.Listen does: the path must not exist yet, it is removed when the
	// engine closes, and on Linux a leading '@' names an abstract socket.
	// Connections on it behave exactly like TCP ones.
	//
	// "udp", "udp4" and "udp6" bind UDP sockets instead, as net.ListenPacket
	// does. Each peer address that sends a datagram becomes a connection of
	// its own, whose OnData receives one datagram per call and whose Send
	// sends one; see UDPIdleTimeout for how such a connection ends.
	Network string
	Addr    string
	// Addrs, when it is not empty, is the complete set of addresses to listen
	// on and Addr is ignored; they all share Network. One server spanning
	// several addresses shares a single event loop, descriptor table, task
	// pool and buffer pool across all of them, where a server per address
	// gives each its own copy of all four.
	Addrs          []string
	Backlog        int
	WorkerCount    int
	MaxEvents      int
	ReadBufferSize int
	// WriteBufferHighWatermark pauses socket reads while at least this many
	// bytes are waiting to be written. This bounds userspace buffering while
	// TCP backpressure catches up. Set below zero to disable write backpressure.
	//
	// Crossing it is not free: the worker hands a command to the event loop,
	// which costs an eventfd write and an epoll_ctl. A watermark near the
	// message size makes every reply a crossing, so keep it well above one
	// round's worth of output.
	WriteBufferHighWatermark int
	// MaxPendingBytes caps the bytes this server may hold across all of its
	// connections waiting for their sockets. WriteBufferHighWatermark bounds a
	// single connection, which at 100k connections still admits a per-server
	// total of watermark*100k; this is the bound on the sum. Reads pause on
	// every connection while the budget is exhausted. Zero means unlimited.
	MaxPendingBytes int64
	UseWritev       bool
	// SocketSyscalls has connections read and write their sockets with
	// recvfrom, sendto and sendmsg rather than read, write and writev. Both
	// sets end in the same socket code, but read, write and writev reach it
	// through the VFS, which on every call checks the file's access mode,
	// runs the security module's file permission hook and notifies fsnotify.
	// Where a security module mediates file access, as AppArmor does in a
	// Docker container, that is a large share of a busy server's time:
	// go-websocket-benchmark's echo in such a container (50k connections, 8
	// server CPUs) served 611k messages a second at 634% CPU through read and
	// write, and 639k at 507% through the socket calls. A descriptor that is
	// not a socket falls back to read and write.
	//
	// DefaultConfig sets it. Only Linux honours it, and not on 386, which
	// reaches the socket calls only through socketcall, nor in a race, memory
	// or address sanitizer build.
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
	// IOPollers splits the engine across several event loops. The engine's
	// own loop is left to accept connections and serve its UDP sockets, and
	// every connection it accepts is handed to one of IOPollerCount further
	// loops, the one its descriptor picks modulo their number, which then
	// waits on that connection for the rest of its life. Connections the
	// engine dials, over TCP, UDP or a Unix socket, go to those loops the
	// same way. A UDP listener's peers share its one socket, so they stay on
	// the engine's own loop, unless ReusePort gives each poller a socket of
	// its own.
	//
	// Spreading the connections spreads the loops' own work, the waits,
	// registrations and wake-ups, over several cores. With or without it, a
	// loop only waits for events, and reads the UDP sockets peers share, and
	// hands the connections they make runnable to the engine's pool of
	// workers, the one TaskPoolMode,
	// WorkerCount and SharedTaskPool describe or SetTaskPool supplies: every
	// round, its reads and the OnData and OnClose they call, runs on a
	// worker, so a callback that takes a while holds up its own connection
	// and no other. The handlers of HTTP/2 and HTTP/3 requests run on a pool
	// of their own, apart from the workers that read their connections; see
	// Engine.HandlerPool.
	//
	// DefaultConfig sets it on more than four CPUs, counting GOMAXPROCS where
	// it is below runtime.NumCPU, and leaves it off on four or fewer, where
	// one loop serves as many requests as a poller would.
	// Without it, the engine serves listeners and connections alike on its
	// one loop. Setting it explicitly gives an engine pollers on any number
	// of CPUs.
	//
	// Only the Linux and macOS backends have pollers; on Windows the engine
	// keeps its single loop, as if this were unset.
	IOPollers bool
	// IOPollerCount is how many loops IOPollers creates. Zero or less means
	// the CPUs divided by four, rounded down, and at least one, up to 32
	// CPUs, and half the CPUs on more: 1 up to 7 CPUs, 2 on 8, 4 on 16, 8 on
	// 32, 32 on 64. The CPUs are runtime.NumCPU, or GOMAXPROCS where that is
	// lower.
	//
	// A loop only waits for events and hands them on, which costs a small
	// share of what the workers spend on them, so one loop keeps up with the
	// workers of many CPUs, and each further loop competes with the workers
	// for the same Ps: after every round a loop yields to the workers it woke
	// and then waits behind them for a P, so each loop's rounds gather fewer
	// connections, and requests wait longer to be read. HTTP/2 echoes over
	// 10k connections on 4 CPUs measured 411k requests/s with one poller and
	// 399k with four, whose 99th percentile latency rose from 55ms to 64ms.
	// More loops pay off where the loops' own work, accepting and registering
	// connections and waking for them, is what runs short, as it may with
	// ReusePort and many connections arriving at once.
	IOPollerCount int
	// ReusePort binds the engine's listeners with SO_REUSEPORT, so that
	// other sockets that set it too, in this process or another of the same
	// user, may listen on the same address and share its connections, or
	// its datagrams.
	//
	// Under IOPollers on Linux the pollers accept TCP connections themselves
	// whether or not it is set: each poller listens on a socket of its own,
	// bound to the engine's address with SO_REUSEPORT, and accepts the
	// connections the kernel spreads onto that socket by a hash of their
	// addresses. The engine's own loop accepting every connection and waking
	// the poller it hands it to capped how fast connections were accepted
	// however many cores there were to serve them: HttpArena's limited-conn
	// profile, ten requests a connection over 4096 at a time on 64 CPUs,
	// served 0.94M requests a second that way and 1.65M with the pollers
	// accepting. What it gives up is balance: a connection stays with the
	// poller its hash picked, however busy that poller is. Without
	// ReusePort the engine first claims the address with a socket bound
	// without SO_REUSEPORT, so that an address another socket holds is
	// refused with EADDRINUSE as it always was, and only a socket that sets
	// SO_REUSEPORT itself can join the engine's afterwards. Unix sockets, and
	// the other platforms, keep the engine's loop accepting.
	//
	// On a UDP address it has the pollers read datagrams in the same way,
	// which they do only with it set: each poller reads a
	// socket of its own bound there, and the kernel hands each datagram to
	// one of the sockets by a hash of its source and destination addresses,
	// waking only the poller that reads it. A peer's datagrams therefore all
	// reach one poller, which keeps its connection, its OnData and its idle
	// timeout; without it the engine's own loop reads every peer. The hash
	// holds only while the sockets sharing the address stay the same, so a
	// socket another process binds there moves some peers to it, and a peer
	// whose address changes reaches whichever poller its new address hashes
	// to, as a new peer; package http3 finds its QUIC connection there by
	// its connection ID.
	//
	// In a child of package prefork it is always set, since the children
	// all listen on the same addresses.
	ReusePort bool
	// UDPIdleTimeout closes a UDP peer's connection once the peer has neither
	// sent nor been sent a datagram for this long, since UDP has no close of
	// its own to end it. OnClose receives ErrUDPIdleTimeout. Zero means
	// DefaultUDPIdleTimeout and a negative value keeps peers until they are
	// closed. Dialed UDP connections are never timed out.
	UDPIdleTimeout time.Duration
	// customPoolSizing records that SetPoolSizing pinned the sizing, so that a
	// later SetTaskPoolMode does not overwrite it.
	customPoolSizing bool
}

func DefaultConfig() Config {
	sizing := DefaultPoolSizing(taskpool.ModeAdaptive)
	return Config{Name: DefaultName, Network: "tcp", Addr: ":9000", Backlog: sys.DefaultBacklog(), WorkerCount: sizing.WorkerCount,
		MaxEvents: sizing.MaxEvents, ReadBufferSize: 16 * 1024,
		WriteBufferHighWatermark: defaultWriteHighWatermark, MaxPendingBytes: defaultMaxPendingBytes,
		UseWritev: true, SocketSyscalls: true, TaskPoolMode: taskpool.ModeAdaptive, SharedTaskPool: true,
		IOPollers: defaultIOPollers(defaultCPUs())}
}

// UDP rides on the same connections, handlers and workers as TCP. What is
// different is who reads the socket and what a connection is.
//
// A listening UDP socket has no connections of its own, so the engine makes
// one per peer address: the first datagram from an address opens a
// connection for it, OnOpen runs, and every datagram from that address is
// then that connection's input. The connection closes when it is closed, or
// when the peer has been silent for Config.UDPIdleTimeout. A dialed UDP
// connection is a socket connected to one peer and is simply that peer's.
//
// The event loop reads UDP sockets itself and queues each datagram on its
// connection, and a worker hands the queue to OnData one datagram per call.
// Reading on the loop is what lets many peers share one socket without any of
// them reading another's datagrams, and it keeps datagram boundaries intact:
// OnData receives exactly one datagram, and each Send sends exactly one. It
// is the one read a loop makes; every OnData runs on a worker. Handing the
// socket's reads to a worker as well, one at a time, was measured slower:
// HTTP/3 over 1000 connections on 50 ports, three CPUs, served 270k
// multiplexed requests/s against 289k with the loop reading, and 349k
// echoes/s against 372k with ReusePort, with 15% more memory.
//
// Sends go straight to the socket and are never queued. A datagram the socket
// has no room for is dropped and Send reports the error, which is what UDP
// does anyway when a router has no room for it, so there is no backpressure
// and no write watermark to pause reads on.

// udpState is what a UDP connection keeps on top of an ordinary one.
type udpState struct {
	udpPlatform
	// listener is the socket a peer's datagrams arrive on and its replies
	// leave from, or nil for a dialed connection, which has its own socket.
	listener *udpListener
	// sa and key are a peer's address, as the socket calls take it and as the
	// listener's peer table is keyed. raddr is the same for RemoteAddr.
	sa    syscall.Sockaddr
	key   netip.AddrPort
	raddr *net.UDPAddr
	// queue holds datagrams the loop has read and the handler has not seen
	// yet, from head on. Guarded by the connection's mu. spare is the array
	// the queue last handed to a DatagramsHandler, kept for the next swap.
	queue [][]byte
	head  int
	spare [][]byte
	// lastActive is when the peer last sent or was sent a datagram, in
	// nanoseconds, for the idle timeout.
	lastActive atomic.Int64
}

// udpListener is a bound UDP socket and the peers it has seen.
type udpListener struct {
	udpListenerPlatform
	// peers maps an address to its open connection. Event-loop ownership.
	peers map[netip.AddrPort]*Connection
}

// rawSockaddrKey reads the table key of the peer a receive left its address
// in, and the scope of an IPv6 one, which the key leaves out. It reads the
// raw address where the kernel wrote it rather than through a
// syscall.Sockaddr, which would be an allocation for every datagram when only
// a new peer needs one.
func rawSockaddrKey(rsa *syscall.RawSockaddrAny) (key netip.AddrPort, zone uint32, ok bool) {
	switch rsa.Addr.Family {
	case syscall.AF_INET:
		pp := (*syscall.RawSockaddrInet4)(unsafe.Pointer(rsa))
		port := (*[2]byte)(unsafe.Pointer(&pp.Port))
		return netip.AddrPortFrom(netip.AddrFrom4(pp.Addr), uint16(port[0])<<8|uint16(port[1])), 0, true
	case syscall.AF_INET6:
		pp := (*syscall.RawSockaddrInet6)(unsafe.Pointer(rsa))
		port := (*[2]byte)(unsafe.Pointer(&pp.Port))
		return netip.AddrPortFrom(netip.AddrFrom16(pp.Addr), uint16(port[0])<<8|uint16(port[1])), pp.Scope_id, true
	}
	return netip.AddrPort{}, 0, false
}

// keySockaddr is the socket address a peer's replies are sent to: the
// address its key was read from.
func keySockaddr(key netip.AddrPort, zone uint32) syscall.Sockaddr {
	if addr := key.Addr(); addr.Is4() {
		return &syscall.SockaddrInet4{Port: int(key.Port()), Addr: addr.As4()}
	}
	return &syscall.SockaddrInet6{Port: int(key.Port()), ZoneId: zone, Addr: key.Addr().As16()}
}

func sockaddrToUDPAddr(sa syscall.Sockaddr) *net.UDPAddr {
	addr, err := sockaddrToTCPAddr(sa)
	if err != nil {
		return nil
	}
	return &net.UDPAddr{IP: addr.IP, Port: addr.Port, Zone: addr.Zone}
}

// udpPeer returns the connection for the peer whose address a receive left
// in from, opening one if this is the first datagram from it. It returns nil
// once the engine is stopping. Callers run on the event loop.
func (e *Engine) udpPeer(l *udpListener, from *syscall.RawSockaddrAny) *Connection {
	key, zone, ok := rawSockaddrKey(from)
	if !ok {
		return nil
	}
	if c := l.peers[key]; c != nil {
		return c
	}
	if e.stopping.Load() {
		return nil
	}
	sa := keySockaddr(key, zone)
	c := &Connection{engine: e, handler: e.handler,
		udp: &udpState{listener: l, sa: sa, key: key, raddr: sockaddrToUDPAddr(sa)}}
	c.initUDPPeer()
	c.udp.lastActive.Store(time.Now().UnixNano())
	l.peers[key] = c
	c.handler.OnOpen(c)
	return c
}

// deliverDatagram queues a copy of one datagram on its connection and reports
// the connection if that made it runnable. The copy is a buffer from package
// bufferpool, which a handler done with it may give back. now, in Unix
// nanoseconds, is when the datagram was read, which a read shares between
// the datagrams it took rather than ask the clock for each. Callers run on
// the event loop.
func (e *Engine) deliverDatagram(c *Connection, data []byte, now int64) *Connection {
	u := c.udp
	c.mu.Lock()
	if c.closing || c.closed || len(u.queue)-u.head >= maxQueuedDatagrams {
		c.mu.Unlock()
		return nil
	}
	if u.head == len(u.queue) {
		u.queue = u.queue[:0]
		u.head = 0
	}
	datagram := bufferpool.Get(len(data))
	copy(datagram, data)
	u.queue = append(u.queue, datagram)
	c.mu.Unlock()
	u.lastActive.Store(now)
	return e.noteEvent(c, evIn)
}

// drainDatagrams hands every queued datagram to the handler, one per OnData,
// or all of them at once to a DatagramsHandler.
func (c *Connection) drainDatagrams() {
	u := c.udp
	if h, ok := c.handler.(DatagramsHandler); ok {
		for {
			c.mu.Lock()
			if u.head == len(u.queue) || c.closing || c.closed {
				c.mu.Unlock()
				return
			}
			queue := u.queue
			batch := queue[u.head:]
			// The loop queues what arrives meanwhile on the spare array, so
			// the batch is the handler's alone while it runs.
			u.queue, u.head, u.spare = u.spare[:0], 0, nil
			c.mu.Unlock()
			h.OnDatagrams(c, batch)
			clear(queue)
			c.mu.Lock()
			if u.spare == nil {
				u.spare = queue[:0]
			}
			c.mu.Unlock()
		}
	}
	for {
		c.mu.Lock()
		if u.head == len(u.queue) || c.closing || c.closed {
			c.mu.Unlock()
			return
		}
		data := u.queue[u.head]
		u.queue[u.head] = nil
		u.head++
		c.mu.Unlock()
		c.handler.OnData(c, data)
	}
}

// sendDatagram sends data as one datagram, or drops it and reports why.
func (c *Connection) sendDatagram(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.closed || c.closeAfterSend {
		return syscall.EPIPE
	}
	err := c.sysSendDatagram(data)
	if err == nil {
		c.udp.lastActive.Store(time.Now().UnixNano())
	}
	return err
}

// SendBatch sends each of datagrams as a datagram of its own, in order, as
// that many Sends would, but on Linux and macOS with one system call for as
// many of them as it can: sendmmsg, or sendmsg_x. A datagram the socket has
// no room for is dropped, with those after it, and SendBatch reports why;
// those before it were sent. On a connection that is not UDP it Sends each
// in turn.
func (c *Connection) SendBatch(datagrams [][]byte) error {
	batch := c.udp != nil
	for _, d := range datagrams {
		// Send sends nothing for an empty datagram, where a batch would
		// send an empty one.
		batch = batch && len(d) > 0
	}
	if !batch {
		for _, d := range datagrams {
			if err := c.Send(d); err != nil {
				return err
			}
		}
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.closed || c.closeAfterSend {
		return syscall.EPIPE
	}
	err := c.sysSendDatagrams(datagrams)
	if err == nil {
		c.udp.lastActive.Store(time.Now().UnixNano())
	}
	return err
}

// sendDatagramParts sends two parts as one datagram.
func (c *Connection) sendDatagramParts(first, second []byte) error {
	if len(second) == 0 {
		return c.sendDatagram(first)
	}
	if len(first) == 0 {
		return c.sendDatagram(second)
	}
	// sendDatagram hands the bytes to the socket before it returns, so the
	// joined copy goes straight back to the pool.
	data := bufferpool.Join(nil, first, second)
	defer bufferpool.Put(data)
	return c.sendDatagram(data)
}

// detachPeer drops a closed peer from its listener's table. The socket is the
// listener's, so there is nothing to close. Callers run on the event loop.
func (e *Engine) detachPeer(c *Connection) {
	u := c.udp
	if u.listener.peers[u.key] == c {
		delete(u.listener.peers, u.key)
	}
}

// startUDPSweeper arms the timer that has the loop look for idle peers. The
// timer arms itself again each time it fires, so nothing waits on it in
// between.
func (e *Engine) startUDPSweeper() {
	if len(e.udpListeners) == 0 || e.udpIdleTimeout <= 0 {
		return
	}
	interval := udpSweepInterval(e.udpIdleTimeout)
	e.udpSweepMu.Lock()
	defer e.udpSweepMu.Unlock()
	e.udpSweep = time.AfterFunc(interval, func() {
		if e.stopping.Load() || !e.request(command{kind: commandUDPSweep}) {
			return
		}
		e.udpSweepMu.Lock()
		if e.udpSweep != nil {
			e.udpSweep.Reset(interval)
		}
		e.udpSweepMu.Unlock()
	})
}

func (e *Engine) stopUDPSweeper() {
	e.udpSweepMu.Lock()
	if e.udpSweep != nil {
		e.udpSweep.Stop()
		e.udpSweep = nil
	}
	e.udpSweepMu.Unlock()
}

// sweepUDP closes the peers that have been silent for the idle timeout.
// Callers run on the event loop.
func (e *Engine) sweepUDP() {
	limit := int64(e.udpIdleTimeout)
	if limit <= 0 {
		return
	}
	now := time.Now().UnixNano()
	for _, l := range e.udpListeners {
		for _, c := range l.peers {
			if now-c.udp.lastActive.Load() >= limit {
				e.closeConnection(c, ErrUDPIdleTimeout, true)
			}
		}
	}
}

// closeUDPPeers closes every peer without a callback, as Close does for TCP
// connections.
func (e *Engine) closeUDPPeers() {
	for _, l := range e.udpListeners {
		for _, c := range l.peers {
			e.closeConnection(c, nil, false)
		}
	}
}

// LocalUDPAddrs returns one address per UDP listener, in configured order.
// Ports left at zero report the port the kernel chose.
func (e *Engine) LocalUDPAddrs() ([]*net.UDPAddr, error) {
	listeners := e.udpListeners
	if len(listeners) == 0 && len(e.pollers) > 0 {
		// The pollers read the engine's addresses themselves, and the first
		// holds the sockets the engine bound; see Config.ReusePort.
		listeners = e.pollers[0].udpListeners
	}
	addrs := make([]*net.UDPAddr, 0, len(listeners))
	for _, l := range listeners {
		sa, err := l.sockname()
		if err != nil {
			return nil, err
		}
		addr := sockaddrToUDPAddr(sa)
		if addr == nil {
			return nil, syscall.EAFNOSUPPORT
		}
		addrs = append(addrs, addr)
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
		return nil, errNoListener
	}
	return addrs[0], nil
}

// errUDPRead is what Read reports on a UDP connection, whose datagrams the
// event loop reads and delivers whole.
var errUDPRead = errors.New("fib: a UDP connection is read through OnData")

// Protocol returns the transport the connection runs over, which it keeps
// for its whole life, after it closes too.
func (c *Connection) Protocol() Protocol {
	switch {
	case c.udp != nil:
		return ProtocolUDP
	case c.unix:
		return ProtocolUnix
	}
	return ProtocolTCP
}

// RemoteAddrPort returns the peer's IP address and port, the zero AddrPort
// for a Unix socket, for an IPv6 peer with a zone, which RemoteAddr reports
// with its zone, and once the socket is gone. An IPv4 peer of an IPv6
// socket is reported as the IPv4 address it is, as net.TCPAddr's String
// shows it. For a connection the engine accepted it is the address accept
// returned, which takes no system call and no allocation to report.
func (c *Connection) RemoteAddrPort() netip.AddrPort {
	if c.peer.IsValid() {
		return c.peer
	}
	if c.udp != nil {
		if c.udp.raddr == nil || c.udp.raddr.Zone != "" {
			return netip.AddrPort{}
		}
		ap := c.udp.raddr.AddrPort()
		return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
	}
	if c.unix {
		return netip.AddrPort{}
	}
	sa, err := c.peerSockaddr()
	if err != nil {
		return netip.AddrPort{}
	}
	return sys.SockaddrAddrPort(sa)
}

// RemoteAddr returns the peer's address: a *net.UDPAddr for a UDP
// connection, a *net.UnixAddr for a Unix socket, whose name is empty when the
// peer never bound one, and a *net.TCPAddr otherwise. It returns nil once the
// socket is gone.
func (c *Connection) RemoteAddr() net.Addr {
	if c.udp != nil {
		return c.udp.raddr
	}
	sa, err := c.peerSockaddr()
	if err != nil {
		return nil
	}
	return sockaddrToAddr(sa)
}

//go:build linux || darwin || windows

package fib

import (
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/bufferpool"
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
	maxEvents          int
	useWritev          bool
	inlineHandlers     bool
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
	// inlineTasks says taskPool runs tasks on the loop that submits them.
	inlineTasks bool
	// workers is the pool of workers connections that ask for one run on
	// while the engine's own rounds run on its loops; see workerPool. The
	// application's engine keeps it for its pollers too.
	workers workerPool
	// offload collects a round's connections bound for workers. Event-loop
	// ownership.
	offload        []*Connection
	taskWG         sync.WaitGroup
	readBufferSize int
	closeOnce      sync.Once
	// udpListeners are the engine's UDP sockets, when Config.Network names
	// UDP. udpIdleTimeout closes their silent peers, and udpSweepDone stops
	// the ticker that checks for them.
	udpListeners   []*udpListener
	udpIdleTimeout time.Duration
	udpSweepDone   chan struct{}
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
		config.Backlog = defaultBacklog()
	}
	if handler == nil {
		handler = HandlerFuncs{}
	}

	e := &Engine{name: engineName(config), maxEvents: config.MaxEvents,
		useWritev: config.UseWritev, inlineHandlers: config.InlineHandlers,
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
	e.workers.config = config
	e.taskPool, e.releaseTaskPool = acquireTaskPool(config)
	if pool, ok := e.taskPool.(*taskpool.TaskPool); ok {
		e.inlineTasks = pool.Mode() == taskpool.ModeInline
	}
	if err := e.open(config, addrs); err != nil {
		e.releaseTaskPool()
		return nil, err
	}
	if err := e.openPollers(config); err != nil {
		_ = e.Close()
		return nil, err
	}
	e.startUDPSweeper()
	return e, nil
}

// openPollers creates the loops Config.IOPollers asks for. A poller is
// created as an engine without listeners and shares everything else with
// this one.
func (e *Engine) openPollers(config Config) error {
	n := pollerCount(config)
	for i := 0; i < n; i++ {
		p := &Engine{parent: e, name: e.name, maxEvents: e.maxEvents, useWritev: e.useWritev,
			inlineHandlers: e.inlineHandlers, writeHighWatermark: e.writeHighWatermark,
			writeLowWatermark: e.writeLowWatermark, maxPendingBytes: e.maxPendingBytes,
			budgetResumeBytes: e.budgetResumeBytes, sendBufferSize: e.sendBufferSize,
			udpIdleTimeout: e.udpIdleTimeout, pendingTotal: e.pendingTotal, handler: e.handler,
			taskPool: e.taskPool, releaseTaskPool: func() {}, inlineTasks: e.inlineTasks,
			readBufferSize: e.readBufferSize}
		if err := p.open(config, nil); err != nil {
			return err
		}
		e.pollers = append(e.pollers, p)
	}
	return nil
}

// workerPool is an engine's pool of workers for connections that ask for one
// while the engine runs rounds on its loops. It is built the first time one
// asks, since an engine that serves nothing that does has no use for it.
type workerPool struct {
	// config is what the pool is built from.
	config Config
	// ready holds the pool once it is built, for the loops to read without
	// the lock.
	ready   atomic.Pointer[builtWorkerPool]
	mu      sync.Mutex
	release func()
	// closed turns building away once the engine has released the pool.
	closed bool
}

type builtWorkerPool struct{ pool TaskPool }

// workerPool returns the pool the rounds of connections that asked for
// workers run on, or nil once the engine has closed. It is the engine's own
// pool unless that one runs rounds on the loops.
func (e *Engine) workerPool() TaskPool {
	if !e.inlineTasks && !e.inlineHandlers {
		return e.taskPool
	}
	w := &e.root().workers
	if built := w.ready.Load(); built != nil {
		return built.pool
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if built := w.ready.Load(); built != nil || w.closed {
		if built == nil {
			return nil
		}
		return built.pool
	}
	pool, release := acquireWorkerPool(w.config)
	w.release = release
	w.ready.Store(&builtWorkerPool{pool: pool})
	return pool
}

// releaseWorkerPool lets go of the pool of workers, if one was built, and
// turns away building one after it.
func (e *Engine) releaseWorkerPool() {
	w := &e.workers
	w.mu.Lock()
	w.closed = true
	release := w.release
	w.release = nil
	w.mu.Unlock()
	if release != nil {
		release()
	}
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
	// connections right now, which is what MaxPendingBytes bounds.
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
// runnable to the workers, or runs them in place when handlers are inline, and
// then gives connections parked on the server-wide budget a chance to resume.
func (e *Engine) runReady(ready []*Connection, tasks []taskpool.Task) ([]*Connection, []taskpool.Task) {
	// Resuming comes first so that a connection it hands a read back to is
	// scheduled in this round rather than whenever the loop next wakes.
	e.resumeBudgetPaused()
	ready = append(ready, e.redeliver...)
	clear(e.redeliver)
	e.redeliver = e.redeliver[:0]
	if len(ready) > 0 {
		all := len(ready)
		// yield says workers were just woken, rather than every round run
		// here.
		yield := false
		if e.inlineHandlers || e.inlineTasks {
			// Connections that asked for workers leave the loop even so.
			ready, tasks, yield = e.submitToWorkers(ready, tasks)
		}
		if e.inlineHandlers {
			for _, c := range ready {
				c.process()
			}
		} else if len(ready) > 0 {
			tasks = e.submitReady(e.taskPool, ready, tasks[:0])
			yield = yield || !e.inlineTasks
		}
		if yield {
			// The workers just woken wait on this goroutine's P, and the loop
			// is about to wait for events in a system call that keeps the P
			// out of use: while there is other work, the runtime hands it on
			// only after the call has lasted a while, and the loop then waits
			// for a P of its own behind whatever is runnable. Yielding first
			// runs the workers here straight away. Measured on Linux with 4
			// Ps serving HTTP/2 echoes over 10k connections, it raised
			// throughput by 10% to 25%, depending on the client, from a loop
			// that had spent 40% of its time runnable and waiting for a P
			// after each wait.
			runtime.Gosched()
		}
		ready = ready[:all]
		clear(ready)
		ready = ready[:0]
	}
	return ready, tasks
}

// submitToWorkers hands the connections of a round that asked for workers to
// the pool of them, and reports the rest, which the loop runs, compacted at
// the front of ready, and whether there were any to hand over.
func (e *Engine) submitToWorkers(ready []*Connection, tasks []taskpool.Task) ([]*Connection, []taskpool.Task, bool) {
	kept := ready[:0]
	for _, c := range ready {
		if c.onWorkers.Load() {
			e.offload = append(e.offload, c)
		} else {
			kept = append(kept, c)
		}
	}
	if len(e.offload) == 0 {
		return kept, tasks, false
	}
	tasks = e.submitReady(e.workerPool(), e.offload, tasks[:0])
	clear(e.offload)
	e.offload = e.offload[:0]
	return kept, tasks, true
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
// woken into a single wake-up.
func (e *Engine) notify() {
	if !e.wakePending.CompareAndSwap(false, true) {
		return
	}
	e.wake()
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

func (e *Engine) drainCommands() {
	e.ackWake()
	e.wakePending.Store(false)
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
	if pending := c.pendingBytes.Swap(0); pending > 0 {
		e.releaseBudget(pending)
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
	if schedule {
		e.redeliver = append(e.redeliver, c)
	}
}

var _ io.Closer = (*Engine)(nil)

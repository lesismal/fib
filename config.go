package fib

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/lesismal/fib/internal/udpbatch"
	"github.com/lesismal/fib/streampool"
	"github.com/lesismal/fib/taskpool"
)

// DefaultName is the Name an engine takes when its Config sets none.
const DefaultName = "fib"

// TaskPool runs the rounds the event loop schedules. *taskpool.TaskPool
// implements it; set Config.TaskPool to supply a different one.
//
// Each task must eventually run exactly once. GoTasks accepts a prefix of its
// argument and reports how long that prefix is; the engine closes the
// connections of the tasks it did not accept, so a pool that is stopping may
// refuse work, but one that is merely busy should queue it rather than refuse.
// A task may block for as long as the handler it calls does, so a pool that
// runs tasks on a fixed set of goroutines bounds how many connections progress
// at once.
type TaskPool interface {
	GoTask(task taskpool.Task) bool
	GoTasks(tasks []taskpool.Task) int
}

// PoolSizing is how many workers a task pool may run and how many tasks it may
// hold waiting for them. How many events an event loop collects in one round
// is Config.PollBatchSize, apart from either.
type PoolSizing struct {
	WorkerCount int
	MaxEvents   int
}

// Pools are oversubscribed relative to the cores they run on, because a worker
// drives its connection's read and write syscalls itself and so spends much of
// a round inside the kernel rather than on a P. Sizing a pool to GOMAXPROCS
// leaves cores idle whenever its workers are in flight.
//
// Every mode that has workers grows and shrinks them with the load, so the
// worker count is a ceiling rather than a population: raising it costs
// nothing until the load actually climbs to it, and lowering it throttles a
// burst that the cores could have absorbed. The default is set high for that
// reason.
//
// ModeElastic forks a worker per submission while it has capacity and retires
// it after a short idle linger.
//
// The default's floor follows the cores rather than GOMAXPROCS, since it is
// sized from GOMAXPROCS and the floor is what keeps a lowered GOMAXPROCS from
// throttling the pool below what the machine's cores can keep busy. It is
// elasticMinWorkersPerCPU workers per core, where a fixed floor would hand a
// two-core machine 5000 workers per core.
//
// ModeAdaptive and ModeAdaptiveChan keep parked workers between tasks and
// grow and shrink the population with the load, and take the same default.
// The floor they retire down to is Config.MinWorkerCount.
//
// GOMAXPROCS is best left at the runtime's default, the cores: raising it
// above them served HTTP/2 over 10k connections 3% to 17% fewer multiplexed
// requests on 3, 4 and 6 cores, with echoes no faster; see the note on
// GOMAXPROCS in docs/guide.zh-CN.md.
const (
	elasticWorkersPerCPU = 1000
	// elasticMinWorkersPerCPU is the floor per core; see above.
	elasticMinWorkersPerCPU = 20

	// The queue holds connections the loop has made runnable but no worker has
	// picked up yet, so it is sized from the pool rather than independently,
	// within bounds that keep a small pool's queue usable and a large pool's
	// queue from growing far wider than a round can fill. Under ModeAdaptive
	// the queue's memory follows its backlog, so a bound costs nothing until
	// the queue reaches it.
	eventsPerWorker = 10
	minMaxEvents    = 10000
	maxMaxEvents    = 100000

	// streamPoolFactor is how much wider the pool HTTP/2 and HTTP/3 run
	// their request handlers on is than the widest engine pool of the same
	// Name. There is one such pool for each Name, shared by every HTTP/2 and
	// HTTP/3 server whose connections come from engines of that Name, and it
	// is never one an engine runs on; see package streampool for
	// the deadlock sharing an engine's pool would invite, and
	// Engine.HandlerPool.
	//
	// The two pools do different work. An engine worker holds its connection
	// for one round: it reads the socket, hands what came in to the protocol,
	// and gives the round back, so the work is bounded by the syscalls it
	// makes. A handler runs the application, which may wait on a database, a
	// file or another server for as long as that takes, and one multiplexed
	// connection can have hundreds of requests open at once where a TCP
	// connection has one round. Sizing the handler pool like the engine's
	// would make it the bottleneck it exists to remove, so it is sized above
	// it; under ModeAdaptive, which it takes, a ceiling costs nothing until
	// the load climbs to it.
	streamPoolFactor = 2
)

// DefaultPoolSizing reports the sizing mode is tuned for, which is what
// DefaultConfig and SetTaskPoolMode install. Every mode now takes the same
// one; the parameter stays so that a mode tuned otherwise can be told apart.
func DefaultPoolSizing(mode taskpool.Mode) PoolSizing {
	workerCount := max(runtime.GOMAXPROCS(0)*elasticWorkersPerCPU, runtime.NumCPU()*elasticMinWorkersPerCPU)
	return poolSizing(workerCount)
}

// DefaultStreamPoolSizing reports the sizing of the pool HTTP/2 and HTTP/3
// run their request handlers on while no engine is running: twice what
// DefaultPoolSizing reports for the engine. Once engines run, the pool's
// ceiling is twice the widest of their pools instead, whatever their mode;
// see streamPoolFactor.
func DefaultStreamPoolSizing(mode taskpool.Mode) PoolSizing {
	return poolSizing(DefaultPoolSizing(mode).WorkerCount * streamPoolFactor)
}

// poolSizing pairs a worker count with the queue that goes with it.
func poolSizing(workerCount int) PoolSizing {
	maxEvents := workerCount * eventsPerWorker
	if maxEvents < minMaxEvents {
		maxEvents = minMaxEvents
	} else if maxEvents > maxMaxEvents {
		maxEvents = maxMaxEvents
	}
	return PoolSizing{WorkerCount: workerCount, MaxEvents: maxEvents}
}

// SetTaskPoolMode switches the task pool mode and moves the pool sizing to the
// one that mode is tuned for, since what a worker count buys differs between
// them. Sizing pinned by SetPoolSizing is left alone, so the two may be called
// in either order.
func (c *Config) SetTaskPoolMode(mode taskpool.Mode) *Config {
	c.TaskPoolMode = mode
	if !c.customPoolSizing {
		sizing := DefaultPoolSizing(mode)
		c.WorkerCount = sizing.WorkerCount
		c.MaxEvents = sizing.MaxEvents
	}
	return c
}

// SetTaskPool makes the engine run its connections on pool instead of a pool
// of its own. The engine never stops a pool supplied this way: the caller owns
// it, may share it between engines and with other work, and stops it after
// every engine using it has been closed. TaskPoolMode, WorkerCount,
// SharedTaskPool and the pool sizing no longer apply. Passing nil goes back
// to the built-in pool.
func (c *Config) SetTaskPool(pool TaskPool) *Config {
	c.TaskPool = pool
	return c
}

// errInlinePool is what an engine asked to run on an inline pool reports: its
// loops only wait for events and hand the connections those make runnable to
// workers, and never run a connection's round themselves.
var errInlinePool = errors.New("fib: an engine runs its connections on workers, not on an inline pool")

// validateTaskPool checks the settings that describe the built-in pool, which
// mean nothing when the caller supplies one.
func (c *Config) validateTaskPool() error {
	if c.TaskPool != nil {
		if p, ok := c.TaskPool.(*taskpool.TaskPool); ok && p.Mode() == taskpool.ModeInline {
			return errInlinePool
		}
		return nil
	}
	if c.WorkerCount <= 0 {
		return errors.New("worker count must be greater than zero")
	}
	if !c.TaskPoolMode.Valid() {
		return fmt.Errorf("invalid task pool mode %d", c.TaskPoolMode)
	}
	if c.TaskPoolMode == taskpool.ModeInline {
		return errInlinePool
	}
	if c.TaskPoolMode.Adaptive() && (c.MinWorkerCount < 0 || c.MinWorkerCount > c.WorkerCount) {
		return fmt.Errorf("min worker count %d must be between zero and the worker count %d",
			c.MinWorkerCount, c.WorkerCount)
	}
	return nil
}

// AcceptPlacement says which poller serves a connection a poller accepts on
// a listening socket of its own; see Config.AcceptPlacement.
type AcceptPlacement int

const (
	// AcceptFDModulo, the default, hands the connection to the poller its
	// descriptor picks modulo the number of pollers.
	AcceptFDModulo AcceptPlacement = iota
	// AcceptKernelHash has the poller that accepted a connection serve it,
	// so the kernel's hash of the connection's addresses, which picked the
	// socket, picks the poller.
	AcceptKernelHash
)

func (p AcceptPlacement) String() string {
	switch p {
	case AcceptFDModulo:
		return "fd-modulo"
	case AcceptKernelHash:
		return "kernel-hash"
	}
	return fmt.Sprintf("AcceptPlacement(%d)", int(p))
}

// validAcceptPlacement rejects a Config.AcceptPlacement no constant names.
func validAcceptPlacement(p AcceptPlacement) error {
	if p != AcceptKernelHash && p != AcceptFDModulo {
		return fmt.Errorf("invalid accept placement %d", int(p))
	}
	return nil
}

// engineLoop reports whether config gives an engine an event loop of its
// own: always without pollers, which leave it the only loop, and with them
// only when Config.EngineLoop asks for one.
func engineLoop(config Config) bool {
	return config.EngineLoop || pollerCount(config) == 0
}

// pollerCount reports how many loops config has an engine hand its
// connections to, or zero when it keeps them on its own loop.
func pollerCount(config Config) int {
	if !config.IOPollers || !pollersSupported {
		return 0
	}
	if config.IOPollerCount > 0 {
		return config.IOPollerCount
	}
	return defaultPollerCount(defaultCPUs())
}

// defaultCPUs is how many CPUs the defaults that depend on the machine's
// size go by: the cores the process may run on, or GOMAXPROCS when that is
// lower, as it is in a child of package prefork, which serves on a few Ps
// of a machine its siblings share, or where the runtime has lowered it to a
// container's CPU quota. Pollers beyond the Ps there are to run them, and
// the workers they feed, would only wait for one another.
func defaultCPUs() int { return min(runtime.NumCPU(), runtime.GOMAXPROCS(0)) }

// cpusPerPoller is how many CPUs one poller waits for by default. A poller
// only waits for events and hands the connections they make runnable to the
// workers, which costs a small share of what the workers then spend on them:
// profiled at 0.2us a request against 4.5us on the workers, for WebSocket and
// HTTP/2 echoes over 10k connections, and every loop competes with the
// workers for the same Ps.
const cpusPerPoller = 8

// onePollerCPUs and twoPollerCPUs are the most CPUs on which the default
// creates one poller and two. A lone poller is the one thread every
// readiness and every accepted connection goes through: on 8 CPUs of a
// Ryzen 7800X3D, HttpArena's limited-conn profile, ten requests a
// connection over 4096 at a time, served 705k requests/s with one poller,
// which left the CPUs 18% idle, and 827k with two.
const (
	onePollerCPUs = 4
	twoPollerCPUs = 16
)

// defaultPollerCount is how many pollers IOPollers creates on cpus CPUs when
// IOPollerCount leaves it to the engine: one on onePollerCPUs or fewer, two
// on up to twoPollerCPUs, and cpus divided by cpusPerPoller, rounded down,
// on more.
func defaultPollerCount(cpus int) int {
	switch {
	case cpus <= onePollerCPUs:
		return 1
	case cpus <= twoPollerCPUs:
		return 2
	}
	return cpus / cpusPerPoller
}

// SetPoolSizing pins the pool sizing to the caller's own numbers, which a later
// SetTaskPoolMode then keeps. A value that is not positive leaves that field at
// what it already held.
//
// WorkerCount is a ceiling on forked goroutines under ModeElastic and on
// parked ones under ModeAdaptive and ModeAdaptiveChan; DefaultPoolSizing
// documents what each mode does with it.
func (c *Config) SetPoolSizing(workerCount, maxEvents int) *Config {
	if workerCount > 0 {
		c.WorkerCount = workerCount
	}
	if maxEvents > 0 {
		c.MaxEvents = maxEvents
	}
	c.customPoolSizing = true
	return c
}

// DefaultUDPIdleTimeout is how long a UDP peer may stay silent before its
// connection is closed, when Config.UDPIdleTimeout is left at zero.
const DefaultUDPIdleTimeout = 60 * time.Second

// ErrUDPIdleTimeout is what OnClose receives for a UDP peer that neither sent
// nor was sent anything for Config.UDPIdleTimeout.
var ErrUDPIdleTimeout = errors.New("fib: udp peer idle timeout")

const (
	// maxDatagramSize holds the largest UDP payload, so no datagram is ever
	// truncated on the way in.
	maxDatagramSize = udpbatch.MaxDatagramSize
	// maxQueuedDatagrams bounds the datagrams a connection holds for its
	// handler. Beyond it new ones are dropped, as the kernel drops them when
	// a socket's receive buffer is full: a peer that sends faster than its
	// handler keeps up loses datagrams instead of growing memory.
	maxQueuedDatagrams = 1024
	// maxDatagramsPerRound bounds the datagrams one socket may deliver in a
	// single round of the event loop, so a flooded socket cannot starve the
	// others. UDP sockets are level-triggered, so what is left is picked up in
	// the next round.
	maxDatagramsPerRound = 256
)

// udpIdleTimeout reads Config.UDPIdleTimeout: zero is the default, and a
// negative value turns the timeout off, which is reported as zero.
func udpIdleTimeout(configured time.Duration) time.Duration {
	switch {
	case configured == 0:
		return DefaultUDPIdleTimeout
	case configured < 0:
		return 0
	}
	return configured
}

// udpSweepInterval is how often idle peers are looked for: often enough that
// a peer outlives its timeout by at most a quarter of it.
func udpSweepInterval(timeout time.Duration) time.Duration {
	return max(timeout/4, 10*time.Millisecond)
}

// engineName is the name config gives its engine.
func engineName(config Config) string {
	if config.Name == "" {
		return DefaultName
	}
	return config.Name
}

// taskPoolName is the name of the pool an engine named engine builds for its
// connections.
func taskPoolName(engine string) string { return engine + "-workers" }

// newTaskPool builds the pool config describes.
func newTaskPool(config Config) *taskpool.TaskPool {
	name := taskPoolName(engineName(config))
	adaptive := taskpool.AdaptiveConfig{
		Name: name, MinWorkers: config.MinWorkerCount, MaxWorkers: config.WorkerCount, QueueSize: config.MaxEvents,
	}
	switch config.TaskPoolMode {
	case taskpool.ModeAdaptive:
		return taskpool.NewAdaptive(adaptive)
	case taskpool.ModeAdaptiveChan:
		return taskpool.NewAdaptiveChan(adaptive)
	}
	return taskpool.NewWithMode(name, config.TaskPoolMode, config.WorkerCount, config.MaxEvents)
}

type sharedTaskPoolEntry struct {
	pool *taskpool.TaskPool
	// workers is the WorkerCount the pool was built with, which the engines
	// sharing it run on whatever theirs say.
	workers int
	refs    int
}

// sharedTaskPools holds the pools engines share, by the pool's name.
var sharedTaskPools = struct {
	sync.Mutex
	entries map[string]*sharedTaskPoolEntry
}{entries: make(map[string]*sharedTaskPoolEntry)}

func acquireTaskPool(config Config) (TaskPool, func()) {
	if config.TaskPool != nil {
		// The caller owns a pool it supplied, so releasing it is a no-op.
		return config.TaskPool, func() {}
	}
	engine := engineName(config)
	if !config.SharedTaskPool {
		// The pool HTTP/2 and HTTP/3 run their handlers on has to stay wider
		// than every engine pool that feeds it; see streamPoolFactor.
		releaseStreams := streampool.Require(engine, config.WorkerCount*streamPoolFactor)
		pool := newTaskPool(config)
		return pool, func() {
			pool.Stop()
			releaseStreams()
		}
	}
	name := taskPoolName(engine)
	sharedTaskPools.Lock()
	entry := sharedTaskPools.entries[name]
	if entry == nil {
		entry = &sharedTaskPoolEntry{pool: newTaskPool(config), workers: config.WorkerCount}
		sharedTaskPools.entries[name] = entry
	}
	entry.refs++
	sharedTaskPools.Unlock()
	releaseStreams := streampool.Require(engine, entry.workers*streamPoolFactor)
	var once sync.Once
	return entry.pool, func() {
		once.Do(func() {
			sharedTaskPools.Lock()
			entry.refs--
			last := entry.refs == 0
			if last {
				delete(sharedTaskPools.entries, name)
			}
			sharedTaskPools.Unlock()
			if last {
				entry.pool.Stop()
			}
			releaseStreams()
		})
	}
}

package fib

import (
	"errors"
	"fmt"
	"runtime"

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
// hold waiting for them. MaxEvents also sizes the epoll batch the event loop
// collects in one round.
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
	// queue from being allocated far wider than a round can fill.
	eventsPerWorker = 10
	minMaxEvents    = 10000
	maxMaxEvents    = 100000

	// streamPoolFactor is how much wider the pool HTTP/2 and HTTP/3 run
	// their request handlers on is than the widest engine pool of the same
	// Name. There is one such pool for each Name, shared by every HTTP/2 and
	// HTTP/3 server whose connections come from engines of that Name, and it
	// is never one an engine runs on; see package internal/streampool for
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

// pollerCount reports how many loops config has an engine hand its
// connections to, or zero when it keeps them on its own loop.
func pollerCount(config Config) int {
	if !config.IOPollers || !pollersSupported {
		return 0
	}
	if config.IOPollerCount > 0 {
		return config.IOPollerCount
	}
	return defaultPollerCount(runtime.NumCPU())
}

// cpusPerPoller is how many CPUs one poller waits for by default. A poller
// only waits for events and hands the connections they make runnable to the
// workers, which costs a small share of what the workers then spend on them:
// profiled at 0.2us a request against 4.5us on the workers, for WebSocket and
// HTTP/2 echoes over 10k connections. One poller then keeps up with the
// workers of about 20 CPUs, and one for every 8 leaves each loop around 40%
// busy, where rounds still gather many events and wait little for a P.
// Every loop more than that competes with the workers for the same Ps: on 3
// and 4 CPUs, one poller served as many requests as none or more, and one per
// CPU served 2% to 6% fewer with a 99th percentile 10% to 15% higher.
const cpusPerPoller = 8

// defaultPollerCount is how many pollers IOPollers creates on cpus CPUs when
// IOPollerCount leaves it to the engine: one for every cpusPerPoller of them,
// and at least one.
func defaultPollerCount(cpus int) int {
	return max(1, (cpus+cpusPerPoller-1)/cpusPerPoller)
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

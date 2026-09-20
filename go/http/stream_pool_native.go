//go:build linux || darwin || windows

package http

import (
	"fmt"
	"sync"
	"sync/atomic"

	fib "github.com/lesismal/fib/go"
	"github.com/lesismal/fib/go/taskpool"
)

// StreamPool runs the request handlers of a multiplexed protocol away from
// the goroutines that read its connections. Each connection keeps a
// StreamGate of its own and passes it to Run for every request.
//
// A nil *StreamPool runs every request on the calling goroutine, which is
// what NewStreamPool returns for a configuration that asks for no pool, so
// a server never has to test for one.
type StreamPool struct {
	pool fib.TaskPool
	// limit is StreamPoolConfig.MaxConcurrentHandlers.
	limit int
}

// NewStreamPool returns the pool config describes, or nil when config leaves
// the handlers on the goroutine that reads their connection.
func NewStreamPool(config StreamPoolConfig) *StreamPool {
	if config.Disable || config.MaxConcurrentHandlers == 1 {
		return nil
	}
	pool := config.TaskPool
	if pool == nil {
		pool = sharedStreamPool(config)
	}
	return &StreamPool{pool: pool, limit: config.MaxConcurrentHandlers}
}

// StreamGate is one connection's share of a StreamPool: it counts the
// requests of that connection the pool is running, which is what a
// per-connection limit is measured against. A connection keeps one, by
// value, and passes it to every Run.
type StreamGate struct {
	running atomic.Int64
}

// Running is how many of the connection's requests the pool is running now.
// The handler on the connection's own reader, which a connection at its
// limit runs there, is not one of them.
func (g *StreamGate) Running() int { return int(g.running.Load()) }

// Run serves one request. It hands fn to the pool while the connection is
// under its limit, and runs fn itself once the connection is at it, which is
// what enforces the limit: the caller is the goroutine that reads the
// connection, and it reads nothing more until fn returns.
//
// conn is flushed after a request the pool ran, since that response was
// written outside the read round that would otherwise carry it to the
// socket; a request run here is left to the caller's own round.
func (p *StreamPool) Run(gate *StreamGate, conn *fib.Connection, fn func()) {
	if p == nil {
		fn()
		return
	}
	if p.limit > 0 {
		for {
			running := gate.running.Load()
			if running+1 >= int64(p.limit) {
				fn()
				return
			}
			if gate.running.CompareAndSwap(running, running+1) {
				break
			}
		}
	} else {
		gate.running.Add(1)
	}
	task := &streamTask{gate: gate, conn: conn, fn: fn}
	if !p.pool.GoTask(task) {
		// The pool has stopped taking work; the request is served here.
		task.run()
	}
}

// streamTask is one request on the pool.
type streamTask struct {
	gate *StreamGate
	conn *fib.Connection
	fn   func()
}

func (t *streamTask) RunTask() { t.run() }

// run serves the request and gives back the connection's slot. A handler
// that panics ends its connection, as one that panics on the connection's
// own worker does, since the engine is not there to do it: the panic reaches
// the pool afterwards, where a panic handler can see it.
func (t *streamTask) run() {
	defer func() {
		t.gate.running.Add(-1)
		if recovered := recover(); recovered != nil {
			if t.conn != nil {
				t.conn.CloseWithError(fmt.Errorf("handler panic: %v", recovered))
			}
			panic(recovered)
		}
		if t.conn != nil {
			_ = t.conn.Flush()
		}
	}()
	t.fn()
}

// streamPoolKey is the sizing that decides which shared pool a server gets.
type streamPoolKey struct {
	minWorkers, maxWorkers, queueSize int
}

var sharedStreamPools = struct {
	sync.Mutex
	pools map[streamPoolKey]*taskpool.TaskPool
}{pools: make(map[streamPoolKey]*taskpool.TaskPool)}

// sharedStreamPool is the pool every server asking for config's sizing
// shares, built on the first ask. See StreamPoolConfig.MaxWorkers for why it
// is shared and why it is never stopped.
func sharedStreamPool(config StreamPoolConfig) *taskpool.TaskPool {
	key := streamPoolKey{
		minWorkers: config.MinWorkers,
		maxWorkers: config.MaxWorkers,
		queueSize:  config.QueueSize,
	}
	if key.maxWorkers <= 0 || key.queueSize <= 0 {
		sizing := fib.DefaultStreamPoolSizing(taskpool.ModeAdaptive)
		if key.maxWorkers <= 0 {
			key.maxWorkers = sizing.WorkerCount
		}
		if key.queueSize <= 0 {
			key.queueSize = sizing.MaxEvents
		}
	}
	if key.minWorkers < 0 {
		key.minWorkers = 0
	}
	key.minWorkers = min(key.minWorkers, key.maxWorkers)

	sharedStreamPools.Lock()
	defer sharedStreamPools.Unlock()
	pool := sharedStreamPools.pools[key]
	if pool == nil {
		if key.minWorkers > 0 {
			pool = taskpool.NewAdaptive(taskpool.AdaptiveConfig{
				MinWorkers: key.minWorkers, MaxWorkers: key.maxWorkers, QueueSize: key.queueSize,
			})
		} else {
			// The pool's own floor, which NewAdaptive would read as none.
			pool = taskpool.NewWithMode(taskpool.ModeAdaptive, key.maxWorkers, key.queueSize)
		}
		sharedStreamPools.pools[key] = pool
	}
	return pool
}

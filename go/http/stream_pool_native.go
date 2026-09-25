//go:build linux || darwin || windows

package http

import (
	"fmt"
	"sync/atomic"

	fib "github.com/lesismal/fib/go"
	"github.com/lesismal/fib/go/internal/streampool"
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
	pool *taskpool.TaskPool
	// limit is StreamPoolConfig.MaxConcurrentHandlers.
	limit int
}

// NewStreamPool returns the pool config describes, or nil when config leaves
// the handlers on the goroutine that reads their connection.
func NewStreamPool(config StreamPoolConfig) *StreamPool {
	if config.Disable || config.MaxConcurrentHandlers == 1 {
		return nil
	}
	sizing := fib.DefaultStreamPoolSizing(taskpool.ModeAdaptive)
	return &StreamPool{pool: streampool.Get(sizing.WorkerCount, sizing.MaxEvents),
		limit: config.MaxConcurrentHandlers}
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
	if !p.admit(gate) {
		fn()
		return
	}
	p.submit(&streamTask{gate: gate, conn: conn, fn: fn})
}

// Serve serves the request r holds with handler, as Run does a function that
// calls Serve with r's Context, but without allocating: what the pool keeps
// of the request while it waits for a worker is in r. Get the Context from
// r.Context first.
func (p *StreamPool) Serve(gate *StreamGate, conn *fib.Connection, handler Handler, r *StreamRequest) {
	if !p.admit(gate) {
		Serve(handler, &r.context)
		return
	}
	r.task = streamTask{gate: gate, conn: conn, handler: handler, context: &r.context}
	p.submit(&r.task)
}

// StreamBatch holds requests of one connection that Queue has admitted to a
// StreamPool, until Submit hands them to its workers together. A burst of
// requests handed over as one batch lands on one shard of the pool, in
// order, and is taken off it by the same few workers, so its responses are
// written close together, which a protocol that packs them into packets
// makes use of; handed over one at a time, they go to shards whose queues
// differ, and finish as far apart as those queues are. A connection keeps
// one batch, by value, used only by the goroutine that reads it.
type StreamBatch struct {
	tasks []taskpool.Task
}

// Queue is Serve for a request that goes to the pool with the others queued
// on b, once Submit is called. A request the connection's limit keeps off the
// pool is served at once, as Serve serves it, after those queued ahead of it
// are submitted.
func (p *StreamPool) Queue(b *StreamBatch, gate *StreamGate, conn *fib.Connection, handler Handler, r *StreamRequest) {
	if !p.admit(gate) {
		p.Submit(b)
		Serve(handler, &r.context)
		return
	}
	r.task = streamTask{gate: gate, conn: conn, handler: handler, context: &r.context}
	b.tasks = append(b.tasks, &r.task)
}

// Submit hands the requests queued on b to the pool, in order.
func (p *StreamPool) Submit(b *StreamBatch) {
	if len(b.tasks) == 0 {
		return
	}
	n := p.pool.GoTasks(b.tasks)
	for _, task := range b.tasks[n:] {
		// The pool has stopped taking work; the request is served here.
		task.(*streamTask).run()
	}
	clear(b.tasks)
	b.tasks = b.tasks[:0]
}

// admit reports whether a request of the connection gate counts for goes to
// the pool, counting it if so, or has to be served by the caller: when there
// is no pool, or the connection is at its limit.
func (p *StreamPool) admit(gate *StreamGate) bool {
	if p == nil {
		return false
	}
	if p.limit <= 0 {
		gate.running.Add(1)
		return true
	}
	for {
		running := gate.running.Load()
		if running+1 >= int64(p.limit) {
			return false
		}
		if gate.running.CompareAndSwap(running, running+1) {
			return true
		}
	}
}

// submit hands an admitted request to the pool.
func (p *StreamPool) submit(task *streamTask) {
	if !p.pool.GoTask(task) {
		// The pool has stopped taking work; the request is served here.
		task.run()
	}
}

// streamTask is one request on the pool: fn, or handler serving context.
type streamTask struct {
	gate    *StreamGate
	conn    *fib.Connection
	fn      func()
	handler Handler
	context *Context
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
	if t.fn != nil {
		t.fn()
		return
	}
	serveRequest(t.handler, t.context)
}

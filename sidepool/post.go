package sidepool

import (
	"sync"

	"github.com/lesismal/fib/taskpool"
)

// notifyWorkers is the notify pool's ceiling. What it runs is a callback
// that hands a waiter the room it waited for, which sends what it can and
// returns.
const notifyWorkers = 4096

var notify lazyPool

// Notify returns the pool Post runs its callbacks on. Nothing but Post
// submits to it, which is what lets the callbacks it could not queue be run
// by the ones it did.
func Notify() *taskpool.TaskPool { return notify.get("fib-notify", notifyWorkers) }

// Post runs fn on the notify pool without waiting and without failing, for a
// caller that may not wait, such as an event loop or a goroutine holding a
// lock that fn takes. A callback that finds the pool's queue full waits in a
// list of Post's own instead, which every callback the pool runs empties
// before it lets its worker go, so no goroutine is started for it.
func Post(fn func()) { post(Notify(), &overflow, fn) }

var overflow backlog

// backlog is the callbacks Post could not queue.
type backlog struct {
	mu  sync.Mutex
	fns []func()
}

// post is Post on pool, with b as its backlog.
//
// Each callback runs through pool.Call, which recovers a panic in it as the
// pool does any task's, reporting it to the pool's panic handler or the log:
// one that panics must not keep the backlog behind it from being emptied,
// which nothing else would do until the next callback ran.
func post(pool *taskpool.TaskPool, b *backlog, fn func()) {
	if pool.TryGo(func() { pool.Call(fn); b.drain(pool) }) {
		return
	}
	b.mu.Lock()
	b.fns = append(b.fns, fn)
	b.mu.Unlock()
	// The queue was full, and the callbacks in it each empty the backlog
	// once they have run. Those that were queued then may all have run
	// before fn joined it, though, so one more look is asked for: a pool
	// that takes it runs it, and one whose queue is still full has, in that
	// queue, callbacks that start after fn joined, and look then.
	pool.TryGo(func() { b.drain(pool) })
}

// drain runs the callbacks waiting in the backlog until it is empty, each
// through pool.Call.
func (b *backlog) drain(pool *taskpool.TaskPool) {
	for {
		b.mu.Lock()
		if len(b.fns) == 0 {
			b.mu.Unlock()
			return
		}
		fn := b.fns[0]
		b.fns[0] = nil
		b.fns = b.fns[1:]
		b.mu.Unlock()
		pool.Call(fn)
	}
}

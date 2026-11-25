//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package core

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/taskpool"
)

// handRetry is how long a loop waits before it offers a pool again the
// functions it turned away with its queue full.
const handRetry = time.Millisecond

// handItem is a function a loop is to run on a pool.
type handItem struct {
	pool TaskPool
	fn   func()
}

// handQueue holds what a loop has yet to hand on: the functions a full pool
// turned away, in the order they came, and whether a retry is due. Its
// lock is taken by the loop, and by the timer that retries once the loop
// has closed.
type handQueue struct {
	mu      sync.Mutex
	pending []handItem
	armed   bool
}

// handNext spreads the hands of an engine with no loop of its own over its
// pollers.
var handNext atomic.Uint32

// Hand runs fn on pool, submitted by the engine's event loop rather than by
// the caller.
//
// It is for code that may itself be running on pool: a task of a pool must
// never submit to its own pool, since a pool whose every worker waits in a
// submission for room in its own queue has none left to make room (see
// taskpool.TaskPool.Go). Hand only queues fn for the loop, which never waits:
// it offers fn to pool with TryGo and, when the queue is full, keeps it and
// offers it again shortly, behind what that loop already keeps. So the
// caller does not wait either, the pool's own workers never submit to it,
// and the loop is never held up by the pool.
//
// fn never runs on the caller's goroutine, nor on the loop. If pool has
// stopped, fn runs on a goroutine of its own. Hand reports false, and fn
// does not run, once the engine has closed.
//
// An engine with no loop of its own spreads its hands over its pollers, so
// the functions it is handed keep no order between them; Connection.Hand,
// which goes through the loop that serves the connection, keeps the order
// of what it is handed to any one pool.
func (e *Engine) Hand(pool TaskPool, fn func()) bool {
	loop := e.handLoop()
	if loop == nil {
		return false
	}
	return loop.handVia(pool, fn)
}

// Hand is Engine.Hand on the loop that serves c.
func (c *Connection) Hand(pool TaskPool, fn func()) bool {
	return c.engine.handVia(pool, fn)
}

// handLoop picks the loop Hand goes through: the engine's own, or one of
// its pollers when it has none.
func (e *Engine) handLoop() *Engine {
	root := e.root()
	if !root.noLoop {
		return root
	}
	if n := len(root.pollers); n > 0 {
		return root.pollers[int(handNext.Add(1)%uint32(n))]
	}
	return nil
}

// handVia queues fn on e's loop, which must have one.
func (e *Engine) handVia(pool TaskPool, fn func()) bool {
	return e.request(command{kind: commandRun, run: func() { e.handOn(pool, fn) }})
}

// handOn offers fn to pool, on the loop. A function that finds others
// waiting before it, or the queue full, waits its turn.
func (e *Engine) handOn(pool TaskPool, fn func()) {
	h := &e.hand
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.pending) == 0 && offerHand(pool, fn) {
		return
	}
	h.pending = append(h.pending, handItem{pool: pool, fn: fn})
	e.armHandLocked()
}

// offerHand offers fn to pool without waiting, and reports whether fn was
// taken: by the pool, or, once the pool has stopped, by a goroutine of its
// own. A pool that is no taskpool.TaskPool, such as one an application
// supplies, has no TryGo: a goroutine of its own submits fn, and may wait,
// which holds up no loop and no worker of any pool.
func offerHand(pool TaskPool, fn func()) bool {
	tp, ok := pool.(*taskpool.TaskPool)
	if !ok {
		go func() {
			if !pool.GoTask(handTask(fn)) {
				fn()
			}
		}()
		return true
	}
	if tp.TryGo(fn) {
		return true
	}
	if tp.Stopped() {
		go tp.Call(fn)
		return true
	}
	return false
}

// handTask is a function handed to a pool as a Task.
type handTask func()

func (t handTask) RunTask() { t() }

// armHandLocked has the loop offer what waits again after handRetry, or,
// once the loop has closed, hands it to goroutines of their own.
func (e *Engine) armHandLocked() {
	h := &e.hand
	if h.armed {
		return
	}
	h.armed = true
	time.AfterFunc(handRetry, func() {
		if !e.request(command{kind: commandRun, run: e.retryHand}) {
			e.abandonHand()
		}
	})
}

// retryHand offers the waiting functions again, in order, on the loop, and
// keeps the first one turned away and those behind it.
func (e *Engine) retryHand() {
	h := &e.hand
	h.mu.Lock()
	defer h.mu.Unlock()
	h.armed = false
	taken := 0
	for _, item := range h.pending {
		if !offerHand(item.pool, item.fn) {
			break
		}
		taken++
	}
	rest := copy(h.pending, h.pending[taken:])
	clear(h.pending[rest:])
	h.pending = h.pending[:rest]
	if rest > 0 {
		e.armHandLocked()
	}
}

// abandonHand runs what still waits on goroutines of its own, once the loop
// that was to hand it on has closed.
func (e *Engine) abandonHand() {
	h := &e.hand
	h.mu.Lock()
	pending := h.pending
	h.pending = nil
	h.armed = false
	h.mu.Unlock()
	for _, item := range pending {
		if tp, ok := item.pool.(*taskpool.TaskPool); ok {
			go tp.Call(item.fn)
		} else {
			go item.fn()
		}
	}
}

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package core

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/log"
	"github.com/lesismal/fib/taskpool"
)

// handRetry is how long a loop waits before it offers a pool again the
// functions it turned away with its queue full.
const handRetry = time.Millisecond

// handPool is what a loop has yet to hand one pool: the functions that pool
// turned away with its queue full, in the order they came.
type handPool struct {
	pool *taskpool.TaskPool
	fns  []func()
}

// handQueue holds what a loop has yet to hand on, one list for each pool
// that turned something away, so that a pool whose queue is full holds up
// only what is for that pool: what the loop is handed for another goes on
// at once. Its lock is taken by the loop, by the timer that retries once the
// loop has closed, and by TryHand.
//
// Only a taskpool.TaskPool turns a function away; one an application
// supplies has a goroutine of its own submit to it (see offerHand), so
// nothing waits here for it.
type handQueue struct {
	mu      sync.Mutex
	pools   []handPool
	waiting int
	armed   bool
	// level is how many of handWarnAt the loop has risen past and not yet
	// left, and warned when it last logged at each.
	level  int
	warned [len(handWarnAt)]time.Time

	// What Stats reads, without the lock: how many functions wait now,
	// the most that have, the TryHand calls turned away, and the warnings
	// logged and held back.
	queued    atomic.Int64
	maxQueued atomic.Int64
	rejected  atomic.Uint64
	warnings  atomic.Uint64
	// rejectLogged is when a TryHand turned away was last logged, in
	// nanoseconds since the Unix epoch.
	rejectLogged atomic.Int64
}

// handWarnAt are the numbers of functions waiting on one loop at which it
// warns, as the number rises past each; a level warns again once the number
// has fallen below half of it. What waits here is mostly notifications, the
// ends of requests and of connections, which are never turned away, so the
// numbers are what an engine taking on much more than its pools run looks
// like rather than a limit.
var handWarnAt = [...]int{1 << 10, 1 << 14, 1 << 17}

// handLogEvery is the least time between two lines a loop logs about one
// level of handWarnAt, or about the TryHand calls it turns away.
const handLogEvery = 10 * time.Second

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
// offers it again shortly, behind what that loop already keeps for pool. So
// the caller does not wait either, the pool's own workers never submit to
// it, and the loop is never held up by the pool. A pool that is full holds up
// only what is handed to it: what the loop is handed for another pool goes
// on at once.
//
// Hand never turns fn away while the engine runs, so it is for what must
// run, such as telling a waiter that what it waits on has ended. Work a busy
// process may refuse, such as a lookup before a dial, goes through TryHand.
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

// TryHand is Hand for work a busy process may turn away, such as a lookup
// before a dial: it reports ErrPoolBusy, and fn does not run, when pool's
// queue is full or the loop already keeps functions that pool turned away,
// and an error wrapping net.ErrClosed once the engine has closed. A caller
// turned away fails what it was asked to do with the error, which the
// protocol it serves reports as it reports an overload: a 503 for an HTTP
// request, an error for a dial.
//
// Each turn-away is counted in Stats.HandRejected and logged, at most once
// every handLogEvery for a loop.
func (e *Engine) TryHand(pool TaskPool, fn func()) error {
	loop := e.handLoop()
	if loop == nil {
		return errLoopClosed
	}
	return loop.tryHandVia(pool, fn)
}

// TryHand is Engine.TryHand on the loop that serves c.
func (c *Connection) TryHand(pool TaskPool, fn func()) error {
	return c.engine.tryHandVia(pool, fn)
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

// tryHandVia is TryHand on e's loop, which must have one.
func (e *Engine) tryHandVia(pool TaskPool, fn func()) error {
	if tp, ok := pool.(*taskpool.TaskPool); ok && e.handBusy(tp) {
		e.rejectHand(tp)
		return ErrPoolBusy
	}
	if !e.handVia(pool, fn) {
		return errLoopClosed
	}
	return nil
}

// handBusy reports whether tp would turn away what is handed to it now: its
// queue is full, or the loop keeps functions it turned away already.
func (e *Engine) handBusy(tp *taskpool.TaskPool) bool {
	if queued, size := tp.Queued(); size > 0 && queued >= size {
		return true
	}
	h := &e.hand
	if h.queued.Load() == 0 {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.find(tp) >= 0
}

// rejectHand counts a TryHand turned away for tp, and logs it unless the
// loop logged one less than handLogEvery ago.
func (e *Engine) rejectHand(tp *taskpool.TaskPool) {
	h := &e.hand
	n := h.rejected.Add(1)
	now := time.Now().UnixNano()
	if last := h.rejectLogged.Load(); now-last < int64(handLogEvery) || !h.rejectLogged.CompareAndSwap(last, now) {
		return
	}
	queued, size := tp.Queued()
	log.Warn("fib: pool busy, work turned away", "engine", e.root().name, "pool", tp.Name(),
		"queued", queued, "queueSize", size, "workers", tp.Workers(),
		"handWaiting", h.queued.Load(), "rejected", n)
}

// handOn offers fn to pool, on the loop. A function that finds others
// waiting for the same pool, or its queue full, waits its turn; one for a
// pool nothing waits for is offered at once.
func (e *Engine) handOn(pool TaskPool, fn func()) {
	h := &e.hand
	h.mu.Lock()
	defer h.mu.Unlock()
	tp, ok := pool.(*taskpool.TaskPool)
	i := -1
	if ok && h.waiting > 0 {
		i = h.find(tp)
	}
	if i < 0 && offerHand(pool, fn) {
		return
	}
	if i < 0 {
		h.pools = append(h.pools, handPool{pool: tp})
		i = len(h.pools) - 1
	}
	h.pools[i].fns = append(h.pools[i].fns, fn)
	h.waiting++
	e.noteHandLocked()
	e.armHandLocked()
}

// find returns the index of tp's list in h.pools, or -1. A loop hands to a
// few pools at most, so a scan is all it takes.
func (h *handQueue) find(tp *taskpool.TaskPool) int {
	for i := range h.pools {
		if h.pools[i].pool == tp {
			return i
		}
	}
	return -1
}

// noteHandLocked publishes how many functions wait for Stats, and warns as
// the number rises past each of handWarnAt.
func (e *Engine) noteHandLocked() {
	h := &e.hand
	n := h.waiting
	h.queued.Store(int64(n))
	if int64(n) > h.maxQueued.Load() {
		h.maxQueued.Store(int64(n))
	}
	for h.level > 0 && n < handWarnAt[h.level-1]/2 {
		h.level--
	}
	for h.level < len(handWarnAt) && n >= handWarnAt[h.level] {
		at := h.level
		h.level++
		h.warnings.Add(1)
		now := time.Now()
		if now.Sub(h.warned[at]) < handLogEvery {
			continue
		}
		h.warned[at] = now
		pools := make([]any, 0, 2*len(h.pools))
		for _, p := range h.pools {
			pools = append(pools, p.pool.Name(), len(p.fns))
		}
		log.Warn("fib: functions waiting for full pools", "engine", e.root().name,
			"waiting", n, "level", handWarnAt[at], "maxWaiting", h.maxQueued.Load(),
			"pools", fmt.Sprint(pools...))
	}
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

// retryHand offers the waiting functions again, on the loop: each pool's
// in order, stopping at the first that pool turns away, and keeps that one
// and those behind it.
func (e *Engine) retryHand() {
	h := &e.hand
	h.mu.Lock()
	defer h.mu.Unlock()
	h.armed = false
	kept := h.pools[:0]
	waiting := 0
	for _, p := range h.pools {
		taken := 0
		for _, fn := range p.fns {
			if !offerHand(p.pool, fn) {
				break
			}
			taken++
		}
		rest := copy(p.fns, p.fns[taken:])
		clear(p.fns[rest:])
		p.fns = p.fns[:rest]
		if rest > 0 {
			kept = append(kept, p)
			waiting += rest
		}
	}
	clear(h.pools[len(kept):])
	h.pools = kept
	h.waiting = waiting
	e.noteHandLocked()
	if waiting > 0 {
		e.armHandLocked()
	}
}

// abandonHand runs what still waits on goroutines of its own, once the loop
// that was to hand it on has closed.
func (e *Engine) abandonHand() {
	h := &e.hand
	h.mu.Lock()
	pools := h.pools
	h.pools = nil
	h.waiting = 0
	h.armed = false
	e.noteHandLocked()
	h.mu.Unlock()
	for _, p := range pools {
		for _, fn := range p.fns {
			go p.pool.Call(fn)
		}
	}
}

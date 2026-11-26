package sidepool

import (
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/log"
	"github.com/lesismal/fib/taskpool"
)

const (
	// notifyWorkers is the notify pool's ceiling. What it runs is a callback
	// that hands a waiter the room it waited for, which sends what it can and
	// returns.
	notifyWorkers = 4096
	// notifyQueue is what waits in the notify pool's queue at most, a line
	// it is not meant to reach: what Post runs is a notification, a waiter's
	// wake-up or the end of something, and each connection, stream or waiter
	// has a few at a time, so the queue holds as many as the work an engine
	// has taken on gives rise to. The queue takes memory only as deep as it
	// has been (see taskpool), so a large one costs nothing while it is not
	// used.
	notifyQueue = 1 << 17
)

var notify lazyPool

// Notify returns the pool Post runs its callbacks on. Nothing but Post
// submits to it.
func Notify() *taskpool.TaskPool { return pools[4].get() }

// Post runs fn on the notify pool without waiting and without failing, for a
// caller that may not wait, such as an event loop or a goroutine holding a
// lock that fn takes.
//
// What Post runs is a notification: it wakes a waiter or tells it that what
// it waits on has ended, and nothing else would. So it is never dropped:
// should the notify pool's queue be full, which takes notifyQueue callbacks
// waiting at once, fn runs on a goroutine of its own instead, which is
// counted (see PostSpilled) and logged. Work a busy process may turn away
// is not posted; it goes to a pool that may refuse it, such as through
// fib.Engine.TryHand.
//
// A callback Post runs may call Post again, which submits to the notify pool
// from one of its own workers. That is safe because Post never waits: a pool
// stops for good only when every worker waits for room in its own queue
// (see taskpool.TaskPool.Go), and none of Post's ever does. See
// TestPoolSubmissions.
func Post(fn func()) { post(Notify(), fn) }

// spilled counts the callbacks Post ran on goroutines of their own, and
// spillLogged when it last logged one, in nanoseconds since the Unix epoch.
var (
	spilled     atomic.Uint64
	spillLogged atomic.Int64
)

// spillLogEvery is the least time between two lines logged for callbacks
// Post ran on goroutines of their own.
const spillLogEvery = 10 * time.Second

// PostSpilled reports how many callbacks Post has run on goroutines of their
// own because the notify pool's queue was full: a process that sees it grow
// has more notifications waiting than the pool can run.
func PostSpilled() uint64 { return spilled.Load() }

// post is Post on pool. A callback runs through the pool's panic recovery
// wherever it runs.
func post(pool *taskpool.TaskPool, fn func()) {
	if pool.TryGo(fn) {
		return
	}
	n := spilled.Add(1)
	now := time.Now().UnixNano()
	if last := spillLogged.Load(); now-last >= int64(spillLogEvery) && spillLogged.CompareAndSwap(last, now) {
		queued, size := pool.Queued()
		log.Error("sidepool: notify queue full, running callbacks on their own goroutines",
			"pool", pool.Name(), "queued", queued, "queueSize", size, "workers", pool.Workers(), "spilled", n)
	}
	go pool.Call(fn)
}

//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd && !windows

package core

import "github.com/lesismal/fib/taskpool"

// Hand runs fn on pool, submitted by a goroutine of its own rather than by
// the caller, which may be running on pool: a task of a pool must never
// submit to its own pool (see taskpool.TaskPool.Go). This backend has no
// event loop to submit it, so the goroutine does, and may wait for room. If
// pool has stopped, fn runs on that goroutine. It always reports true.
func (e *Engine) Hand(pool TaskPool, fn func()) bool {
	go func() {
		if !pool.GoTask(handTask(fn)) {
			fn()
		}
	}()
	return true
}

// Hand is Engine.Hand for the engine that serves c.
func (c *Connection) Hand(pool TaskPool, fn func()) bool {
	return c.engine.Hand(pool, fn)
}

// TryHand is Hand for work a busy process may turn away: it reports
// ErrPoolBusy, and fn does not run, when pool's queue is full. Each
// turn-away is counted in Stats.HandRejected.
func (e *Engine) TryHand(pool TaskPool, fn func()) error {
	if tp, ok := pool.(*taskpool.TaskPool); ok {
		if queued, size := tp.Queued(); size > 0 && queued >= size {
			e.handRejected.Add(1)
			return ErrPoolBusy
		}
	}
	e.Hand(pool, fn)
	return nil
}

// TryHand is Engine.TryHand for the engine that serves c.
func (c *Connection) TryHand(pool TaskPool, fn func()) error {
	return c.engine.TryHand(pool, fn)
}

// handTask is a function handed to a pool as a Task.
type handTask func()

func (t handTask) RunTask() { t() }

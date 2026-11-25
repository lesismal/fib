//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd && !windows

package core

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

// handTask is a function handed to a pool as a Task.
type handTask func()

func (t handTask) RunTask() { t() }

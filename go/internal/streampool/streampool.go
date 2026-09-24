// Package streampool holds the one pool that HTTP/2 and HTTP/3 run their
// request handlers on, and keeps its ceiling above the pools of the engines
// that feed it.
//
// The pool is internal so that it cannot be handed to an engine as its own:
// an engine worker that frames a request submits the handler here, and a
// submission to a full queue waits for a worker of this pool to take one off.
// Were the two pools one, every worker could end up waiting in a submission
// with none left to drain the queue, and the event loop, which submits to the
// same queue, would stop behind them. Kept apart, the wait only runs one way:
// an engine worker may wait on a handler worker, which never waits on an
// engine worker, since a handler is given its whole request body and its
// response is sent as the flow-control windows allow rather than waited on.
package streampool

import (
	"sync"

	"github.com/lesismal/fib/go/taskpool"
)

type sharedPool struct {
	sync.Mutex
	pool *taskpool.TaskPool
	// ceiling is what the pool's ceiling is now, or will be when it is built.
	ceiling int
	// fallback is the ceiling while no engine has asked for one, which Get
	// supplies; zero until the first Get.
	fallback int
	// required counts the engines asking for each ceiling.
	required map[int]int
}

var shared = &sharedPool{required: make(map[int]int)}

// Require records that an engine needs the pool's ceiling to be at least
// ceiling, and returns the function that withdraws that once the engine is
// done with its pool. The ceiling is the largest any engine asks for, so it
// shrinks again as the engines asking for more go away.
func Require(ceiling int) (release func()) {
	shared.Lock()
	shared.required[ceiling]++
	shared.resizeLocked()
	shared.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			shared.Lock()
			if shared.required[ceiling]--; shared.required[ceiling] == 0 {
				delete(shared.required, ceiling)
			}
			shared.resizeLocked()
			shared.Unlock()
		})
	}
}

// Get returns the pool, building it on the first call with a queue of
// queueSize. fallback is the ceiling it takes while no engine has asked for
// one. The pool is never stopped: a handler has no close of its own, so there
// is no moment at which the last server using it is known to be done.
func Get(fallback, queueSize int) *taskpool.TaskPool {
	shared.Lock()
	defer shared.Unlock()
	if shared.pool == nil {
		shared.fallback = fallback
		shared.ceiling = shared.ceilingLocked()
		shared.pool = taskpool.NewAdaptive(taskpool.AdaptiveConfig{
			MinWorkers: taskpool.DefaultMinWorkers(shared.ceiling),
			MaxWorkers: shared.ceiling,
			QueueSize:  queueSize,
		})
	}
	return shared.pool
}

// Ceiling reports the pool's ceiling, or the one it will be built with.
func Ceiling() int {
	shared.Lock()
	defer shared.Unlock()
	return shared.ceilingLocked()
}

// resizeLocked moves a built pool's ceiling, and its floor with it, to what
// the engines now ask for.
func (s *sharedPool) resizeLocked() {
	ceiling := s.ceilingLocked()
	if s.pool == nil || ceiling == s.ceiling || ceiling <= 0 {
		return
	}
	s.ceiling = ceiling
	s.pool.Resize(taskpool.DefaultMinWorkers(ceiling), ceiling)
}

// ceilingLocked is the largest ceiling an engine asks for, or the fallback
// when none does.
func (s *sharedPool) ceilingLocked() int {
	ceiling := 0
	for c := range s.required {
		ceiling = max(ceiling, c)
	}
	if ceiling == 0 {
		ceiling = s.fallback
	}
	return ceiling
}

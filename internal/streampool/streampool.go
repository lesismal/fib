// Package streampool holds the pools that HTTP/2 and HTTP/3 run their
// request handlers on, one for each engine name, and keeps each one's ceiling
// above the pools of the engines of that name that feed it.
//
// A stream pool is internal so that it cannot be handed to an engine as its
// own: an engine worker that frames a request submits the handler here, and a
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

	"github.com/lesismal/fib/taskpool"
)

// Name is the name of the stream pool that engine's connections run their
// handlers on.
func Name(engine string) string { return engine + "-streams" }

type sharedPool struct {
	pool *taskpool.TaskPool
	// ceiling is what the pool's ceiling is now, or will be when it is built.
	ceiling int
	// fallback is the ceiling while no engine has asked for one, which Get
	// supplies; zero until the first Get.
	fallback int
	// required counts the engines asking for each ceiling.
	required map[int]int
}

var shared = struct {
	sync.Mutex
	pools map[string]*sharedPool
}{pools: make(map[string]*sharedPool)}

// entryLocked returns engine's pool entry, making it if there is none.
func entryLocked(engine string) *sharedPool {
	s := shared.pools[engine]
	if s == nil {
		s = &sharedPool{required: make(map[int]int)}
		shared.pools[engine] = s
	}
	return s
}

// Require records that an engine named engine needs its stream pool's
// ceiling to be at least ceiling, and returns the function that withdraws
// that once the engine is done with its pool. The ceiling is the largest any
// engine of that name asks for, so it shrinks again as the engines asking for
// more go away. Once the last of them has gone the pool is stopped, without
// waiting: a handler has no close of its own, so its workers leave once the
// handlers still running return, and a request that comes after is served by
// its reader.
func Require(engine string, ceiling int) (release func()) {
	shared.Lock()
	s := entryLocked(engine)
	s.required[ceiling]++
	s.resizeLocked()
	shared.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			shared.Lock()
			if s.required[ceiling]--; s.required[ceiling] == 0 {
				delete(s.required, ceiling)
			}
			var stop *taskpool.TaskPool
			if len(s.required) == 0 {
				stop = s.pool
				delete(shared.pools, engine)
			} else {
				s.resizeLocked()
			}
			shared.Unlock()
			if stop != nil {
				go stop.Stop()
			}
		})
	}
}

// Get returns engine's stream pool, building it on the first call with a
// queue of queueSize. fallback is the ceiling it takes while no engine of
// that name has asked for one. A pool no engine asked for is never stopped.
func Get(engine string, fallback, queueSize int) *taskpool.TaskPool {
	shared.Lock()
	defer shared.Unlock()
	s := entryLocked(engine)
	if s.pool == nil {
		s.fallback = fallback
		s.ceiling = s.ceilingLocked()
		s.pool = taskpool.NewAdaptive(taskpool.AdaptiveConfig{
			Name:       Name(engine),
			MaxWorkers: s.ceiling,
			QueueSize:  queueSize,
		})
	}
	return s.pool
}

// Ceiling reports the ceiling of engine's stream pool, or the one it will be
// built with.
func Ceiling(engine string) int {
	shared.Lock()
	defer shared.Unlock()
	if s := shared.pools[engine]; s != nil {
		return s.ceilingLocked()
	}
	return 0
}

// resizeLocked moves a built pool's ceiling to what the engines now ask for.
// Its floor stays at zero.
func (s *sharedPool) resizeLocked() {
	ceiling := s.ceilingLocked()
	if s.pool == nil || ceiling == s.ceiling || ceiling <= 0 {
		return
	}
	s.ceiling = ceiling
	s.pool.Resize(0, ceiling)
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

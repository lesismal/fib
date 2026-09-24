package fib

import (
	"sync"

	"github.com/lesismal/fib/go/internal/streampool"
	"github.com/lesismal/fib/go/taskpool"
)

type taskPoolKey struct {
	mode       taskpool.Mode
	minWorkers int
	workers    int
	queueSize  int
}

// newTaskPool builds the pool a key describes.
func newTaskPool(key taskPoolKey) *taskpool.TaskPool {
	if key.mode == taskpool.ModeAdaptive && key.minWorkers > 0 {
		return taskpool.NewAdaptive(taskpool.AdaptiveConfig{
			MinWorkers: key.minWorkers, MaxWorkers: key.workers, QueueSize: key.queueSize,
		})
	}
	return taskpool.NewWithMode(key.mode, key.workers, key.queueSize)
}

type sharedTaskPoolEntry struct {
	pool *taskpool.TaskPool
	refs int
}

var sharedTaskPools = struct {
	sync.Mutex
	entries map[taskPoolKey]*sharedTaskPoolEntry
}{entries: make(map[taskPoolKey]*sharedTaskPoolEntry)}

func acquireTaskPool(config Config) (TaskPool, func()) {
	if config.TaskPool != nil {
		// The caller owns a pool it supplied, so releasing it is a no-op.
		return config.TaskPool, func() {}
	}
	key := taskPoolKey{mode: config.TaskPoolMode, workers: config.WorkerCount, queueSize: config.MaxEvents}
	if key.mode == taskpool.ModeAdaptive {
		key.minWorkers = config.MinWorkerCount
	}
	// The pool HTTP/2 and HTTP/3 run their handlers on has to stay wider than
	// every engine pool that feeds it; see streamPoolFactor.
	releaseStreams := streampool.Require(key.workers * streamPoolFactor)
	if !config.SharedTaskPool {
		pool := newTaskPool(key)
		return pool, func() {
			pool.Stop()
			releaseStreams()
		}
	}
	sharedTaskPools.Lock()
	entry := sharedTaskPools.entries[key]
	if entry == nil {
		entry = &sharedTaskPoolEntry{pool: newTaskPool(key)}
		sharedTaskPools.entries[key] = entry
	}
	entry.refs++
	sharedTaskPools.Unlock()
	var once sync.Once
	return entry.pool, func() {
		once.Do(func() {
			sharedTaskPools.Lock()
			entry.refs--
			last := entry.refs == 0
			if last {
				delete(sharedTaskPools.entries, key)
			}
			sharedTaskPools.Unlock()
			if last {
				entry.pool.Stop()
			}
			releaseStreams()
		})
	}
}

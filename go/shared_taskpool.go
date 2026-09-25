package fib

import (
	"sync"

	"github.com/lesismal/fib/go/internal/streampool"
	"github.com/lesismal/fib/go/taskpool"
)

// engineName is the name config gives its engine.
func engineName(config Config) string {
	if config.Name == "" {
		return DefaultName
	}
	return config.Name
}

// taskPoolName is the name of the pool an engine named engine builds for its
// connections.
func taskPoolName(engine string) string { return engine + "-workers" }

// newTaskPool builds the pool config describes.
func newTaskPool(config Config) *taskpool.TaskPool {
	name := taskPoolName(engineName(config))
	if config.TaskPoolMode == taskpool.ModeAdaptive && config.MinWorkerCount > 0 {
		return taskpool.NewAdaptive(taskpool.AdaptiveConfig{
			Name: name, MinWorkers: config.MinWorkerCount, MaxWorkers: config.WorkerCount, QueueSize: config.MaxEvents,
		})
	}
	return taskpool.NewWithMode(name, config.TaskPoolMode, config.WorkerCount, config.MaxEvents)
}

type sharedTaskPoolEntry struct {
	pool *taskpool.TaskPool
	// workers is the WorkerCount the pool was built with, which the engines
	// sharing it run on whatever theirs say.
	workers int
	refs    int
}

// sharedTaskPools holds the pools engines share, by the pool's name.
var sharedTaskPools = struct {
	sync.Mutex
	entries map[string]*sharedTaskPoolEntry
}{entries: make(map[string]*sharedTaskPoolEntry)}

func acquireTaskPool(config Config) (TaskPool, func()) {
	if config.TaskPool != nil {
		// The caller owns a pool it supplied, so releasing it is a no-op.
		return config.TaskPool, func() {}
	}
	engine := engineName(config)
	if !config.SharedTaskPool {
		// The pool HTTP/2 and HTTP/3 run their handlers on has to stay wider
		// than every engine pool that feeds it; see streamPoolFactor.
		releaseStreams := streampool.Require(engine, config.WorkerCount*streamPoolFactor)
		pool := newTaskPool(config)
		return pool, func() {
			pool.Stop()
			releaseStreams()
		}
	}
	name := taskPoolName(engine)
	sharedTaskPools.Lock()
	entry := sharedTaskPools.entries[name]
	if entry == nil {
		entry = &sharedTaskPoolEntry{pool: newTaskPool(config), workers: config.WorkerCount}
		sharedTaskPools.entries[name] = entry
	}
	entry.refs++
	sharedTaskPools.Unlock()
	releaseStreams := streampool.Require(engine, entry.workers*streamPoolFactor)
	var once sync.Once
	return entry.pool, func() {
		once.Do(func() {
			sharedTaskPools.Lock()
			entry.refs--
			last := entry.refs == 0
			if last {
				delete(sharedTaskPools.entries, name)
			}
			sharedTaskPools.Unlock()
			if last {
				entry.pool.Stop()
			}
			releaseStreams()
		})
	}
}

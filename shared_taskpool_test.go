package fib

import (
	"testing"

	"github.com/lesismal/fib/internal/streampool"
	"github.com/lesismal/fib/taskpool"
)

func TestSharedTaskPoolReferenceLifecycle(t *testing.T) {
	// A name of its own keeps the pools other tests' engines left running out
	// of what this one counts.
	config := Config{Name: "lifecycle", WorkerCount: 1, MaxEvents: 4, TaskPoolMode: taskpool.ModeCond, SharedTaskPool: true}
	first, releaseFirst := acquireTaskPool(config)
	second, releaseSecond := acquireTaskPool(config)
	if first != second {
		t.Fatal("identical configurations did not share a task pool")
	}
	releaseFirst()
	done := make(chan struct{}, 1)
	if !second.(*taskpool.TaskPool).Go(func() { done <- struct{}{} }) {
		t.Fatal("shared pool stopped before its final owner released it")
	}
	<-done
	releaseSecond()
	sharedTaskPools.Lock()
	_, remaining := sharedTaskPools.entries["lifecycle-workers"]
	sharedTaskPools.Unlock()
	if remaining {
		t.Fatal("the shared pool's entry outlived its final owner")
	}
}

// An adaptive pool starts at the floor MinWorkerCount sets. Engines are
// given one pool by name rather than by settings: one of the same name shares
// the first one's pool whatever its floor, and one of another name does not.
func TestAdaptiveTaskPoolHonoursMinWorkerCount(t *testing.T) {
	config := Config{Name: "floor", WorkerCount: 64, MinWorkerCount: 3, MaxEvents: 64,
		TaskPoolMode: taskpool.ModeAdaptive, SharedTaskPool: true}
	pool, release := acquireTaskPool(config)
	defer release()
	if got := pool.(*taskpool.TaskPool).Workers(); got != 3 {
		t.Fatalf("Workers() = %d, want the floor of 3", got)
	}
	if got := pool.(*taskpool.TaskPool).Name(); got != "floor-workers" {
		t.Fatalf("Name() = %q, want floor-workers", got)
	}
	config.MinWorkerCount = 5
	same, releaseSame := acquireTaskPool(config)
	defer releaseSame()
	if same != pool {
		t.Fatal("engines of one name were not shared a pool")
	}
	config.Name = "floor-other"
	other, releaseOther := acquireTaskPool(config)
	defer releaseOther()
	if other == pool {
		t.Fatal("engines of different names were shared a pool")
	}
}

// An engine's HTTP/2 and HTTP/3 pool is never its own, and its ceiling
// follows the widest engine pool of that name running, at twice it, falling
// back once that engine's pool is released.
func TestStreamPoolStaysWiderThanEnginePools(t *testing.T) {
	sizing := DefaultStreamPoolSizing(taskpool.ModeAdaptive)
	const engine = "stream-ceiling"
	// Wider than the fallback, so that it is the engine that decides the
	// ceiling.
	workers := 1 << 24
	for _, shared := range []bool{true, false} {
		streams := streampool.Get(engine, sizing.WorkerCount, sizing.MaxEvents)
		config := Config{Name: engine, WorkerCount: workers, MaxEvents: 64,
			TaskPoolMode: taskpool.ModeAdaptive, SharedTaskPool: shared}
		pool, release := acquireTaskPool(config)
		if pool == TaskPool(streams) {
			t.Fatalf("shared=%v: the engine was given the stream pool", shared)
		}
		if got, want := streampool.Ceiling(engine), 2*workers; got != want {
			t.Fatalf("shared=%v: stream pool ceiling = %d, want %d", shared, got, want)
		}
		release()
		release()
		if got := streampool.Ceiling(engine); got >= 2*workers {
			t.Fatalf("shared=%v: stream pool ceiling = %d after the engine released its pool", shared, got)
		}
	}
}

// An engine takes DefaultName when its Config names none.
func TestEngineNameDefaultsToFib(t *testing.T) {
	if got := engineName(Config{}); got != DefaultName {
		t.Fatalf("engineName(Config{}) = %q, want %q", got, DefaultName)
	}
	if got := DefaultConfig().Name; got != DefaultName {
		t.Fatalf("DefaultConfig().Name = %q, want %q", got, DefaultName)
	}
	pool, release := acquireTaskPool(Config{WorkerCount: 1, MaxEvents: 4, TaskPoolMode: taskpool.ModeCond})
	defer release()
	if got := pool.(*taskpool.TaskPool).Name(); got != "fib-workers" {
		t.Fatalf("Name() = %q, want fib-workers", got)
	}
}

package fib

import (
	"testing"

	"github.com/lesismal/fib/go/internal/streampool"
	"github.com/lesismal/fib/go/taskpool"
)

func TestSharedTaskPoolReferenceLifecycle(t *testing.T) {
	config := Config{WorkerCount: 1, MaxEvents: 4, TaskPoolMode: taskpool.ModeCond, SharedTaskPool: true}
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
	remaining := len(sharedTaskPools.entries)
	sharedTaskPools.Unlock()
	if remaining != 0 {
		t.Fatalf("shared pool entries remaining = %d", remaining)
	}
}

// An adaptive pool starts at the floor MinWorkerCount sets, and pools that
// differ only in that floor are not shared, since sharing would hand one of
// them the other's floor.
func TestAdaptiveTaskPoolHonoursMinWorkerCount(t *testing.T) {
	config := Config{WorkerCount: 64, MinWorkerCount: 3, MaxEvents: 64,
		TaskPoolMode: taskpool.ModeAdaptive, SharedTaskPool: true}
	pool, release := acquireTaskPool(config)
	defer release()
	if got := pool.(*taskpool.TaskPool).Workers(); got != 3 {
		t.Fatalf("Workers() = %d, want the floor of 3", got)
	}
	config.MinWorkerCount = 5
	other, releaseOther := acquireTaskPool(config)
	defer releaseOther()
	if other == pool {
		t.Fatal("pools with different floors were shared")
	}
}

// The pool HTTP/2 and HTTP/3 serve handlers on is never an engine's, and its
// ceiling follows the widest engine pool running, at twice it, falling back once that engine's pool is released.
func TestStreamPoolStaysWiderThanEnginePools(t *testing.T) {
	sizing := DefaultStreamPoolSizing(taskpool.ModeAdaptive)
	streams := streampool.Get(sizing.WorkerCount, sizing.MaxEvents)
	// Wider than any engine another test may have left running, so that it
	// is this one that decides the ceiling.
	workers := 1 << 24
	for _, shared := range []bool{true, false} {
		config := Config{WorkerCount: workers, MaxEvents: 64,
			TaskPoolMode: taskpool.ModeAdaptive, SharedTaskPool: shared}
		pool, release := acquireTaskPool(config)
		if pool == TaskPool(streams) {
			t.Fatalf("shared=%v: the engine was given the stream pool", shared)
		}
		if got, want := streampool.Ceiling(), 2*workers; got != want {
			t.Fatalf("shared=%v: stream pool ceiling = %d, want %d", shared, got, want)
		}
		release()
		release()
		if got := streampool.Ceiling(); got >= 2*workers {
			t.Fatalf("shared=%v: stream pool ceiling = %d after the engine released its pool", shared, got)
		}
	}
}

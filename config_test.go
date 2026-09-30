//go:build linux

package fib

import (
	"runtime"
	"testing"

	"github.com/lesismal/fib/taskpool"
)

func TestDefaultConfigPoolSizing(t *testing.T) {
	config := DefaultConfig()
	// The exact sizing is DefaultPoolSizing's business and is tuned by
	// measurement; restating its arithmetic here would only pin the test to
	// whatever the constants happen to be. What DefaultConfig owes is that it
	// reports that sizing rather than one of its own.
	want := DefaultPoolSizing(config.TaskPoolMode)
	if config.WorkerCount != want.WorkerCount {
		t.Fatalf("WorkerCount = %d, want %d", config.WorkerCount, want.WorkerCount)
	}
	if config.MaxEvents != want.MaxEvents {
		t.Fatalf("MaxEvents = %d, want %d", config.MaxEvents, want.MaxEvents)
	}
	// Workers are deliberately oversubscribed relative to cores: each one runs
	// its connection's syscalls inline, so a pool the size of GOMAXPROCS leaves
	// cores idle whenever workers are in the kernel.
	if config.WorkerCount <= runtime.GOMAXPROCS(0) {
		t.Fatalf("WorkerCount = %d, want more than GOMAXPROCS %d", config.WorkerCount, runtime.GOMAXPROCS(0))
	}
	// The event batch sizes the task queue, which has to be able to hold a
	// round's worth of runnable connections; a queue narrower than the pool
	// would make the event loop wait on workers it has already woken.
	if config.MaxEvents < config.WorkerCount {
		t.Fatalf("MaxEvents = %d, want at least WorkerCount %d", config.MaxEvents, config.WorkerCount)
	}
	if !config.UseWritev {
		t.Fatal("UseWritev = false, want adaptive writev enabled by default")
	}
	if config.WriteBufferHighWatermark != defaultWriteHighWatermark {
		t.Fatalf("WriteBufferHighWatermark = %d, want %d", config.WriteBufferHighWatermark, defaultWriteHighWatermark)
	}
	if config.MaxPendingBytes != defaultMaxPendingBytes {
		t.Fatalf("MaxPendingBytes = %d, want %d", config.MaxPendingBytes, defaultMaxPendingBytes)
	}
	// The server-wide budget has to sit well above a single connection's, or it
	// would pause every connection as soon as one of them backed up.
	if config.MaxPendingBytes <= int64(config.WriteBufferHighWatermark) {
		t.Fatalf("MaxPendingBytes = %d, want more than the per-connection watermark %d",
			config.MaxPendingBytes, config.WriteBufferHighWatermark)
	}
	if config.TaskPoolMode != taskpool.ModeAdaptive {
		t.Fatalf("TaskPoolMode = %v, want adaptive", config.TaskPoolMode)
	}
	if !config.SharedTaskPool {
		t.Fatal("SharedTaskPool = false, want shared workers by default")
	}
}

// The pool a multiplexed protocol runs its handlers on is fed by the engine's
// own, and a handler waits on the application where an engine worker only
// waits on the kernel, so it has to be the wider of the two.
func TestDefaultStreamPoolSizingExceedsTheEnginePool(t *testing.T) {
	for _, mode := range []taskpool.Mode{taskpool.ModeElastic, taskpool.ModeAdaptive, taskpool.ModeAdaptiveChan} {
		engine := DefaultPoolSizing(mode)
		streams := DefaultStreamPoolSizing(mode)
		if streams.WorkerCount <= engine.WorkerCount {
			t.Fatalf("%v: stream pool WorkerCount = %d, want more than the engine's %d",
				mode, streams.WorkerCount, engine.WorkerCount)
		}
		if streams.MaxEvents < streams.WorkerCount && streams.MaxEvents != maxMaxEvents {
			t.Fatalf("%v: stream pool MaxEvents = %d, want at least WorkerCount %d",
				mode, streams.MaxEvents, streams.WorkerCount)
		}
	}
}

// Every mode's worker count is a ceiling, so they share one default, and its
// queue stays within the bounds poolSizing keeps it to.
func TestDefaultPoolSizingIsSharedByModes(t *testing.T) {
	want := DefaultPoolSizing(taskpool.ModeAdaptive)
	if want.WorkerCount <= runtime.GOMAXPROCS(0) {
		t.Fatalf("WorkerCount = %d, want more than GOMAXPROCS %d", want.WorkerCount, runtime.GOMAXPROCS(0))
	}
	if want.MaxEvents < minMaxEvents || want.MaxEvents > maxMaxEvents {
		t.Fatalf("MaxEvents = %d, want within [%d, %d]", want.MaxEvents, minMaxEvents, maxMaxEvents)
	}
	for _, mode := range []taskpool.Mode{taskpool.ModeElastic, taskpool.ModeAdaptiveChan} {
		if got := DefaultPoolSizing(mode); got != want {
			t.Fatalf("%v sizing = %+v, want adaptive's %+v", mode, got, want)
		}
	}
}

// The elastic floor follows the cores, so a GOMAXPROCS lowered below them
// still leaves each core its share of workers, and a small machine is not
// handed a floor sized for a large one.
func TestElasticFloorFollowsCores(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	floor := runtime.NumCPU() * elasticMinWorkersPerCPU
	want := max(elasticWorkersPerCPU, floor)
	for _, mode := range []taskpool.Mode{taskpool.ModeElastic, taskpool.ModeAdaptive, taskpool.ModeAdaptiveChan} {
		if got := DefaultPoolSizing(mode).WorkerCount; got != want {
			t.Fatalf("%v WorkerCount at GOMAXPROCS 1 = %d, want %d", mode, got, want)
		}
	}
}

// Switching the mode has to carry the sizing with it, or a config keeps numbers
// that no mode's default gave it.
func TestSetTaskPoolModeMovesSizing(t *testing.T) {
	config := DefaultConfig()
	config.WorkerCount, config.MaxEvents = 7, 70
	config.SetTaskPoolMode(taskpool.ModeElastic)
	want := DefaultPoolSizing(taskpool.ModeElastic)
	if config.TaskPoolMode != taskpool.ModeElastic {
		t.Fatalf("TaskPoolMode = %v, want elastic", config.TaskPoolMode)
	}
	if config.WorkerCount != want.WorkerCount || config.MaxEvents != want.MaxEvents {
		t.Fatalf("sizing = %d/%d, want elastic's %d/%d",
			config.WorkerCount, config.MaxEvents, want.WorkerCount, want.MaxEvents)
	}
}

// Sizing the caller chose is theirs to keep, whichever order the two setters
// are called in.
func TestSetPoolSizingSurvivesModeSwitch(t *testing.T) {
	const workers, events = 512, 4096
	for _, order := range []string{"sizing first", "mode first"} {
		t.Run(order, func(t *testing.T) {
			config := DefaultConfig()
			if order == "sizing first" {
				config.SetPoolSizing(workers, events)
				config.SetTaskPoolMode(taskpool.ModeAdaptiveChan)
			} else {
				config.SetTaskPoolMode(taskpool.ModeAdaptiveChan)
				config.SetPoolSizing(workers, events)
			}
			if config.WorkerCount != workers || config.MaxEvents != events {
				t.Fatalf("sizing = %d/%d, want the pinned %d/%d",
					config.WorkerCount, config.MaxEvents, workers, events)
			}
			// And it still holds across a further switch.
			config.SetTaskPoolMode(taskpool.ModeElastic)
			if config.WorkerCount != workers || config.MaxEvents != events {
				t.Fatalf("after switching modes sizing = %d/%d, want the pinned %d/%d",
					config.WorkerCount, config.MaxEvents, workers, events)
			}
		})
	}
}

// A field left at zero means "leave it", so that one of the two can be tuned
// without having to restate the other.
func TestSetPoolSizingKeepsFieldsLeftUnset(t *testing.T) {
	config := DefaultConfig()
	events := config.MaxEvents
	config.SetPoolSizing(64, 0)
	if config.WorkerCount != 64 {
		t.Fatalf("WorkerCount = %d, want 64", config.WorkerCount)
	}
	if config.MaxEvents != events {
		t.Fatalf("MaxEvents = %d, want it left at %d", config.MaxEvents, events)
	}
}

// The engine gives itself one poller for every four CPUs, rounded down, and at
// least one.
func TestDefaultPollerCount(t *testing.T) {
	for cpus, want := range map[int]int{1: 1, 3: 1, 4: 1, 7: 1, 8: 2, 9: 2, 16: 4, 64: 16, 96: 24} {
		if got := defaultPollerCount(cpus); got != want {
			t.Errorf("%d CPUs: %d pollers, want %d", cpus, got, want)
		}
	}
}

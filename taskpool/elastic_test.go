package taskpool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A worker that runs out of tasks lingers for the next one, and retires once
// idleLinger passes with none.
func TestElasticWorkerLingersThenRetires(t *testing.T) {
	tp := NewWithMode("test", ModeElastic, 4, 4)
	defer tp.Stop()
	done := make(chan struct{})
	tp.Go(func() { close(done) })
	<-done
	if got := tp.Workers(); got != 1 {
		t.Fatalf("Workers() = %d right after a task, want the lingering 1", got)
	}
	waitFor(t, "the lingering worker to retire", func() bool { return tp.Workers() == 0 })
}

// Sequential submissions reuse the lingering worker instead of forking.
func TestElasticReusesLingeringWorker(t *testing.T) {
	tp := NewWithMode("test", ModeElastic, 8, 8)
	defer tp.Stop()
	for i := 0; i < 100; i++ {
		done := make(chan struct{})
		tp.Go(func() { close(done) })
		<-done
		// Let the worker reach its wait so the next submission finds it idle.
		time.Sleep(time.Millisecond)
	}
	if got := tp.Workers(); got != 1 {
		t.Fatalf("Workers() = %d after sequential tasks, want 1", got)
	}
}

// Tasks queued while lingering workers hold every slot run at once rather
// than after the linger.
func TestElasticQueuedTaskDoesNotWaitOutLinger(t *testing.T) {
	const n = 4
	tp := NewWithMode("test", ModeElastic, n, 64)
	defer tp.Stop()
	var wg sync.WaitGroup
	wg.Add(n)
	release := make(chan struct{})
	for i := 0; i < n; i++ {
		tp.Go(func() { wg.Done(); <-release })
	}
	wg.Wait()
	close(release)
	// Let the workers reach their wait, holding every slot.
	time.Sleep(5 * time.Millisecond)
	start := time.Now()
	wg.Add(32)
	for i := 0; i < 32; i++ {
		tp.Go(wg.Done)
	}
	wg.Wait()
	if elapsed := time.Since(start); elapsed >= idleLinger {
		t.Fatalf("queued tasks took %v, want less than the %v linger", elapsed, idleLinger)
	}
}

// The workers never run more tasks at once than maxConcurrent.
func TestElasticBoundsConcurrency(t *testing.T) {
	const limit = 4
	tp := NewWithMode("test", ModeElastic, limit, 16)
	var running, peak atomic.Int64
	for i := 0; i < 2000; i++ {
		tp.Go(func() {
			n := running.Add(1)
			for {
				old := peak.Load()
				if n <= old || peak.CompareAndSwap(old, n) {
					break
				}
			}
			time.Sleep(10 * time.Microsecond)
			running.Add(-1)
		})
	}
	tp.Stop()
	if got := peak.Load(); got > limit {
		t.Fatalf("peak concurrency = %d, want at most %d", got, limit)
	}
	if got := tp.Workers(); got != 0 {
		t.Fatalf("Workers() = %d after Stop, want 0", got)
	}
}

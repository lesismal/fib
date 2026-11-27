package taskpool

import (
	"runtime"
	"testing"
	"time"
)

// autoSampling switches automatic sampling on for the test, and back off
// after it.
func autoSampling(t *testing.T) {
	t.Helper()
	SetAutoSample(true)
	t.Cleanup(func() { SetAutoSample(false) })
}

// holdWorker runs a task on tp that holds its one worker until the returned
// func is called.
func holdWorker(t *testing.T, tp *TaskPool) (release func()) {
	t.Helper()
	held := make(chan struct{})
	started := make(chan struct{})
	if !tp.Go(func() { close(started); <-held }) {
		t.Fatal("the holding task was turned away")
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the holding task did not start")
	}
	return func() { close(held) }
}

func watcherRunning() bool {
	watcher.mu.Lock()
	defer watcher.mu.Unlock()
	return watcher.running
}

// TestQueueStatsWarnings fills a pool's queue behind a worker it holds and
// checks that QueueStats counts the TryGo turned away, the deepest queue,
// and a warning at every level as the queue rises past it, and that a level
// warns again only once the queue has fallen well below it.
func TestQueueStatsWarnings(t *testing.T) {
	autoSampling(t)
	tp := NewAdaptive(AdaptiveConfig{Name: "watch-test", MaxWorkers: 1, QueueSize: 20})
	defer tp.Stop()
	release := holdWorker(t, tp)
	_, size := tp.Queued()
	if size < 20 {
		t.Fatalf("queue size %d, want at least 20", size)
	}
	queued := 0
	for tp.TryGo(func() {}) {
		queued++
		if queued > 4*size {
			t.Fatal("the queue never filled")
		}
	}
	s := tp.QueueStats()
	if s.Rejected != 1 || s.MaxQueued != size || s.Queued != queued {
		t.Fatalf("stats %+v after %d queued in %d, want 1 rejected and the queue full", s, queued, size)
	}
	if !tp.watch.armed.Load() {
		t.Fatal("a full queue did not arm the watch")
	}
	// The watcher's own samples may have come first; take what they left.
	before := tp.QueueStats().Warnings
	after := tp.Sample().Warnings
	for i := range WarnLevels {
		if after[i] < 1 {
			t.Fatalf("no warning at %d%% with the queue full: %v", WarnLevels[i], after)
		}
	}
	// A second look at the same full queue warns of nothing new.
	if again := tp.Sample().Warnings; again != after {
		t.Fatalf("a full queue looked at twice warned again: %v, then %v (before %v)", after, again, before)
	}
	release()
	waitFor(t, "the queue drains", func() bool { q, _ := tp.Queued(); return q == 0 })
	tp.Sample()
	tp.watch.mu.Lock()
	level := tp.watch.level
	tp.watch.mu.Unlock()
	if level != -1 {
		t.Fatalf("level %d with the queue empty, want -1", level)
	}
	if got := tp.QueueStats().MaxQueued; got != size {
		t.Fatalf("MaxQueued %d once drained, want %d kept", got, size)
	}
}

// TestWatchArmsOnlyWhenDeep checks that a shallow queue leaves the watch
// alone, that a submission that takes the queue to the first level arms it,
// that the watcher disarms it once the queue has calmed and leaves once no
// pool is armed, and that a deep queue arms it again after.
func TestWatchArmsOnlyWhenDeep(t *testing.T) {
	autoSampling(t)
	tp := NewAdaptive(AdaptiveConfig{Name: "watch-arm", MaxWorkers: 1, QueueSize: 20})
	defer tp.Stop()
	for round := 0; round < 2; round++ {
		release := holdWorker(t, tp)
		deep := deepDepth(20)
		for i := 1; i < int(deep); i++ {
			if !tp.TryGo(func() {}) {
				t.Fatalf("task %d turned away", i)
			}
		}
		if tp.watch.armed.Load() {
			t.Fatalf("round %d: %d of 20 queued armed the watch", round, deep-1)
		}
		if !tp.TryGo(func() {}) {
			t.Fatal("the task that makes the queue deep was turned away")
		}
		if !tp.watch.armed.Load() {
			t.Fatalf("round %d: %d of 20 queued did not arm the watch", round, deep)
		}
		if !watcherRunning() {
			t.Fatal("the watch is armed and no watcher runs")
		}
		if got := tp.QueueStats().MaxQueued; got < int(deep) {
			t.Fatalf("MaxQueued %d, want at least %d", got, deep)
		}
		release()
		waitFor(t, "the watch is disarmed", func() bool { return !tp.watch.armed.Load() })
	}
	// Pools other tests left armed are stopped, and disarmed at the next
	// sample.
	waitFor(t, "the watcher leaves", func() bool { return !watcherRunning() })
}

// TestWatchChanBackends checks that the channel-backed pools arm the watch
// when their queue is deep.
func TestWatchChanBackends(t *testing.T) {
	autoSampling(t)
	for _, mode := range []Mode{ModeElastic, ModeAdaptiveChan} {
		t.Run(mode.String(), func(t *testing.T) {
			tp := NewWithMode("watch-chan", mode, 1, 4)
			defer tp.Stop()
			release := holdWorker(t, tp)
			defer release()
			if tp.watch.armed.Load() {
				t.Fatal("an empty queue armed the watch")
			}
			// Elastic's dispatcher may hold one of them, waiting for the
			// worker; the other three leave the queue at least half full.
			for i := 0; i < 4; i++ {
				if !tp.Go(func() {}) {
					t.Fatalf("task %d turned away", i)
				}
			}
			if !tp.watch.armed.Load() {
				q, size := tp.Queued()
				t.Fatalf("%d of %d queued did not arm the watch", q, size)
			}
		})
	}
}

// TestAutoSampleOff checks that, with automatic sampling off as it is by
// default, neither a deep queue nor a TryGo turned away arms the watch or
// starts the watcher, and that a sample taken by hand counts the warnings
// and the deepest queue the watcher would have.
func TestAutoSampleOff(t *testing.T) {
	if AutoSample() {
		t.Fatal("automatic sampling is on by default")
	}
	tp := NewAdaptive(AdaptiveConfig{Name: "watch-off", MaxWorkers: 1, QueueSize: 20})
	defer tp.Stop()
	release := holdWorker(t, tp)
	_, size := tp.Queued()
	queued := 0
	for tp.TryGo(func() {}) {
		queued++
		if queued > 4*size {
			t.Fatal("the queue never filled")
		}
	}
	if tp.watch.armed.Load() {
		t.Fatal("a full queue armed the watch with automatic sampling off")
	}
	s := tp.QueueStats()
	if s.Rejected != 1 || s.MaxQueued != size || s.Warnings != [len(WarnLevels)]uint64{} {
		t.Fatalf("stats %+v before any sample, want 1 rejected, the queue full and no warnings", s)
	}
	s = tp.Sample()
	for i := range WarnLevels {
		if s.Warnings[i] != 1 {
			t.Fatalf("warnings %v after a sample of a full queue, want one at every level", s.Warnings)
		}
	}
	if again := tp.Sample().Warnings; again != s.Warnings {
		t.Fatalf("a full queue sampled twice warned again: %v, then %v", s.Warnings, again)
	}
	if tp.watch.armed.Load() {
		t.Fatal("a sample by hand armed the watch")
	}
	release()
	waitFor(t, "the queue drains", func() bool { q, _ := tp.Queued(); return q == 0 })
}

// TestAutoSampleSwitchedOff checks that switching automatic sampling off
// disarms the pools armed and lets the watcher leave.
func TestAutoSampleSwitchedOff(t *testing.T) {
	autoSampling(t)
	tp := NewAdaptive(AdaptiveConfig{Name: "watch-switch", MaxWorkers: 1, QueueSize: 20})
	defer tp.Stop()
	release := holdWorker(t, tp)
	defer release()
	for i := 0; i < int(deepDepth(20)); i++ {
		if !tp.TryGo(func() {}) {
			t.Fatalf("task %d turned away", i)
		}
	}
	if !tp.watch.armed.Load() || !watcherRunning() {
		t.Fatal("a deep queue did not arm the watch")
	}
	SetAutoSample(false)
	waitFor(t, "the watch is disarmed", func() bool { return !tp.watch.armed.Load() })
	waitFor(t, "the watcher leaves", func() bool { return !watcherRunning() })
}

// TestSampleAll checks that Sample reports every pool built and not yet
// stopped, with what tells it from the others, and drops a pool once it has
// stopped or been collected.
func TestSampleAll(t *testing.T) {
	find := func(id uint64) (PoolStats, bool) {
		for _, s := range Sample() {
			if s.ID == id {
				return s, true
			}
		}
		return PoolStats{}, false
	}
	tp := NewWithMode("sample-all", ModeAdaptiveChan, 2, 8)
	release := holdWorker(t, tp)
	tp.Go(func() {})
	s, ok := find(tp.executor.id)
	if !ok {
		t.Fatal("Sample left out a running pool")
	}
	if s.Name != "sample-all" || s.Mode != ModeAdaptiveChan || s.Workers < 1 || s.QueueSize != 8 || s.MaxQueued < 1 {
		t.Fatalf("Sample reported %+v", s)
	}
	release()
	tp.Stop()
	if _, ok := find(tp.executor.id); ok {
		t.Fatal("Sample reported a stopped pool")
	}

	id := NewInline("sample-dropped").executor.id
	waitFor(t, "a pool let go of is dropped", func() bool {
		runtime.GC()
		_, ok := find(id)
		return !ok
	})
}

// fakeQueue is a queueReporter whose depth a test sets.
type fakeQueue struct{ queued, size, fullest int }

func (f *fakeQueue) queueLen() (int, int)        { return f.queued, f.size }
func (f *fakeQueue) queueState() (int, int, int) { return f.queued, f.size, f.fullest }
func (f *fakeQueue) grownPast() int              { return 0 }

// TestWatchCalms checks when the watcher finds a pool calm: below the first
// level less the hysteresis for releaseCalm, or below the first level with no
// level held for releaseQuiet; never while a shard is deep or a level is held.
func TestWatchCalms(t *testing.T) {
	autoSampling(t)
	for _, c := range []struct {
		name            string
		queued, fullest int
		held            bool // the queue rose to the first level just before
		want            time.Duration
	}{
		{"calm", 30, 30, false, releaseCalm},
		{"quiet", 45, 45, false, releaseQuiet},
		{"deep shard", 30, 60, false, -1},
		{"level held", 45, 45, true, -1},
		{"level left", 35, 35, true, releaseCalm},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeQueue{size: 100}
			w := newQueueWatch(&executor{name: "fake"})
			w.queue, w.workers = f, func() int { return 1 }
			start := time.Now()
			if c.held {
				f.queued, f.fullest = 60, 60
				if w.sample(start) {
					t.Fatal("a queue past the first level calmed")
				}
			}
			f.queued, f.fullest = c.queued, c.fullest
			step := watchInterval
			for d := time.Duration(0); d <= 2*releaseQuiet; d += step {
				calmed := w.sample(start.Add(step + d))
				if calmed != (c.want >= 0 && d >= c.want) {
					t.Fatalf("after %v calm, sample says calmed %v, want calmed after %v", d, calmed, c.want)
				}
				if calmed {
					return
				}
			}
			if c.want >= 0 {
				t.Fatalf("never calmed, want after %v", c.want)
			}
		})
	}
}

// TestLevelOf checks the levels a queue reaches, with and without the slack
// that keeps a level until the queue has fallen well below it.
func TestLevelOf(t *testing.T) {
	for _, c := range []struct{ queued, size, slack, want int }{
		{0, 100, 0, -1},
		{49, 100, 0, -1},
		{50, 100, 0, 0},
		{80, 100, 0, 1},
		{99, 100, 0, 2},
		{100, 100, 0, 3},
		{41, 100, warnHysteresis, 0},
		{39, 100, warnHysteresis, -1},
	} {
		if got := levelOf(c.queued, c.size, c.slack); got != c.want {
			t.Errorf("levelOf(%d, %d, %d) = %d, want %d", c.queued, c.size, c.slack, got, c.want)
		}
	}
}

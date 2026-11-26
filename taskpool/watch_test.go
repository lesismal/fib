package taskpool

import (
	"testing"
	"time"
)

// TestQueueStatsWarnings fills a pool's queue behind a worker it holds and
// checks that QueueStats counts the TryGo turned away, the deepest queue,
// and a warning at every level as the queue rises past it, and that a level
// warns again only once the queue has fallen well below it.
func TestQueueStatsWarnings(t *testing.T) {
	tp := NewAdaptive(AdaptiveConfig{Name: "watch-test", MaxWorkers: 1, QueueSize: 20})
	defer tp.Stop()
	release := make(chan struct{})
	started := make(chan struct{})
	if !tp.TryGo(func() { close(started); <-release }) {
		t.Fatal("the first task was turned away")
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the first task did not start")
	}
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
	// The watcher's own samples may have come first; take what they left.
	before := tp.QueueStats().Warnings
	tp.sample(time.Now())
	after := tp.QueueStats().Warnings
	for i := range WarnLevels {
		if after[i] < 1 {
			t.Fatalf("no warning at %d%% with the queue full: %v", WarnLevels[i], after)
		}
	}
	// A second look at the same full queue warns of nothing new.
	tp.sample(time.Now())
	if again := tp.QueueStats().Warnings; again != after {
		t.Fatalf("a full queue looked at twice warned again: %v, then %v (before %v)", after, again, before)
	}
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for q, _ := tp.Queued(); q > 0; q, _ = tp.Queued() {
		if time.Now().After(deadline) {
			t.Fatalf("%d tasks still queued", q)
		}
		time.Sleep(time.Millisecond)
	}
	tp.sample(time.Now())
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

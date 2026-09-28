package taskpool

import (
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These are adaptive_test.go's tests of the growth, shrink and resize
// mechanism, run against ModeAdaptiveChan. The ones about shards are left out:
// the chan pool has none.

func TestAdaptiveChanDefaultFloorIsZero(t *testing.T) {
	tp := NewWithMode("test", ModeAdaptiveChan, 64, 16)
	defer tp.Stop()
	if got := tp.Workers(); got != 0 {
		t.Fatalf("Workers() = %d right after NewWithMode, want 0", got)
	}
	done := make(chan struct{})
	tp.Go(func() { close(done) })
	<-done
}

func TestAdaptiveChanStartsAtFloor(t *testing.T) {
	tp := NewAdaptiveChan(AdaptiveConfig{Name: "test", MinWorkers: 5, MaxWorkers: 64, QueueSize: 64})
	defer tp.Stop()
	if got := tp.Workers(); got != 5 {
		t.Fatalf("Workers() = %d right after NewAdaptiveChan, want the floor of 5", got)
	}
}

func TestAdaptiveChanGrowsToCeilingAndNoFurther(t *testing.T) {
	const ceiling = 16
	tp := NewAdaptiveChan(AdaptiveConfig{Name: "test", MinWorkers: 1, MaxWorkers: ceiling, QueueSize: 64})
	defer tp.Stop()
	b := newBlocker()
	defer b.open()
	for i := 0; i < 3*ceiling; i++ {
		if !tp.Go(b.task()) {
			t.Fatal("task rejected before Stop")
		}
	}
	waitFor(t, "the pool to reach its ceiling", func() bool { return b.running.Load() == ceiling })
	if got := tp.Workers(); got != ceiling {
		t.Fatalf("Workers() = %d under load, want %d", got, ceiling)
	}
	b.open()
	b.done.Wait()
	if got := b.peak.Load(); got != ceiling {
		t.Fatalf("peak concurrency = %d, want %d", got, ceiling)
	}
}

func TestAdaptiveChanShrinksBackToFloor(t *testing.T) {
	const floor, ceiling = 2, 32
	tp := NewAdaptiveChan(AdaptiveConfig{Name: "test", MinWorkers: floor, MaxWorkers: ceiling, QueueSize: 64,
		ShrinkInterval: 10 * time.Millisecond})
	defer tp.Stop()
	b := newBlocker()
	defer b.open()
	for i := 0; i < ceiling; i++ {
		tp.Go(b.task())
	}
	waitFor(t, "the burst to run", func() bool { return b.running.Load() == ceiling })
	b.open()
	b.done.Wait()
	waitFor(t, "the pool to shrink to its floor", func() bool { return tp.Workers() == floor })
	time.Sleep(50 * time.Millisecond)
	if got := tp.Workers(); got != floor {
		t.Fatalf("Workers() = %d after idling at the floor, want %d", got, floor)
	}
}

func TestAdaptiveChanZeroFloorRestartsWorkers(t *testing.T) {
	tp := NewAdaptiveChan(AdaptiveConfig{Name: "test", MinWorkers: 0, MaxWorkers: 4, QueueSize: 16,
		ShrinkInterval: 10 * time.Millisecond})
	defer tp.Stop()
	var ran atomic.Int64
	for i := 0; i < 8; i++ {
		tp.Go(func() { ran.Add(1) })
	}
	waitFor(t, "the first tasks to run", func() bool { return ran.Load() == 8 })
	waitFor(t, "every worker to retire", func() bool { return tp.Workers() == 0 })
	done := make(chan struct{})
	tp.Go(func() { close(done) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a task submitted to a pool with no workers never ran")
	}
}

func TestAdaptiveChanResize(t *testing.T) {
	tp := NewAdaptiveChan(AdaptiveConfig{Name: "test", MinWorkers: 1, MaxWorkers: 8, QueueSize: 64,
		ShrinkInterval: 10 * time.Millisecond})
	defer tp.Stop()

	// Raising the floor starts workers at once.
	if !tp.Resize(6, 8) {
		t.Fatal("Resize refused on an adaptive pool")
	}
	if got := tp.Workers(); got != 6 {
		t.Fatalf("Workers() = %d after raising the floor to 6", got)
	}

	// Raising the ceiling lets the load grow the pool past the old one.
	tp.Resize(1, 24)
	b := newBlocker()
	defer b.open()
	for i := 0; i < 24; i++ {
		tp.Go(b.task())
	}
	waitFor(t, "the pool to grow to the new ceiling", func() bool { return b.running.Load() == 24 })

	// Lowering the ceiling under load retires the busy workers over it as
	// they finish, and the queued tasks then run within the new ceiling.
	tp.Resize(1, 4)
	b2 := newBlocker()
	defer b2.open()
	for i := 0; i < 16; i++ {
		tp.Go(b2.task())
	}
	b.open()
	b.done.Wait()
	waitFor(t, "the pool to drop to the lowered ceiling", func() bool { return tp.Workers() <= 4 })
	waitFor(t, "queued tasks to start", func() bool { return b2.running.Load() == 4 })
	b2.open()
	b2.done.Wait()
	if got := b2.peak.Load(); got > 4 {
		t.Fatalf("peak concurrency after lowering the ceiling = %d, want at most 4", got)
	}
}

func TestAdaptiveChanEachTaskRunsOnceWhileResizing(t *testing.T) {
	tp := NewAdaptiveChan(AdaptiveConfig{Name: "test", MinWorkers: 0, MaxWorkers: 64, QueueSize: 32,
		ShrinkInterval: time.Millisecond})
	const producers, perProducer = 8, 2000
	var counts [producers * perProducer]atomic.Int64
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			batch := make([]Task, 0, 16)
			for i := 0; i < perProducer; i++ {
				index := base + i
				task := taskFunc(func() { counts[index].Add(1) })
				if i%3 == 0 {
					if !tp.GoTask(task) {
						t.Errorf("task %d rejected", index)
					}
					continue
				}
				batch = append(batch, task)
				if len(batch) == cap(batch) {
					if n := tp.GoTasks(batch); n != len(batch) {
						t.Errorf("batch accepted %d of %d", n, len(batch))
					}
					batch = batch[:0]
				}
				if i%500 == 0 {
					time.Sleep(2 * time.Millisecond)
				}
			}
			if n := tp.GoTasks(batch); n != len(batch) {
				t.Errorf("batch accepted %d of %d", n, len(batch))
			}
		}(p * perProducer)
	}
	stopResizing := make(chan struct{})
	resized := make(chan struct{})
	go func() {
		defer close(resized)
		for i := 0; ; i++ {
			select {
			case <-stopResizing:
				return
			default:
			}
			tp.Resize(i%4, 8+i%57)
			time.Sleep(500 * time.Microsecond)
		}
	}()
	wg.Wait()
	close(stopResizing)
	<-resized
	tp.Stop()
	for i := range counts {
		if got := counts[i].Load(); got != 1 {
			t.Fatalf("task %d ran %d times", i, got)
		}
	}
	if got := tp.Workers(); got != 0 {
		t.Fatalf("Workers() = %d after Stop", got)
	}
}

func TestAdaptiveChanNeverStrandsATask(t *testing.T) {
	deadline := time.Now().Add(2 * time.Second)
	for iter := 0; time.Now().Before(deadline); iter++ {
		rng := rand.New(rand.NewPCG(uint64(iter), 1))
		ceiling := 1 + rng.IntN(40)
		tp := NewAdaptiveChan(AdaptiveConfig{Name: "test", MinWorkers: rng.IntN(ceiling + 1), MaxWorkers: ceiling,
			QueueSize: 1 + rng.IntN(8), ShrinkInterval: time.Millisecond})
		var ran, want atomic.Int64
		stopResizing := make(chan struct{})
		resized := make(chan struct{})
		go func() {
			defer close(resized)
			for i := 0; ; i++ {
				select {
				case <-stopResizing:
					return
				default:
				}
				m := 1 + i%40
				tp.Resize(i%(m+1), m)
				time.Sleep(time.Duration(i%200) * time.Microsecond)
			}
		}()
		var producers sync.WaitGroup
		for g := 0; g < 4; g++ {
			producers.Add(1)
			go func(seed uint64) {
				defer producers.Done()
				rng := rand.New(rand.NewPCG(seed, 2))
				for i := 0; i < 200; i++ {
					block := rng.IntN(10) == 0
					run := func() {
						if block {
							time.Sleep(20 * time.Microsecond)
						}
						ran.Add(1)
					}
					if rng.IntN(2) == 0 {
						want.Add(1)
						tp.Go(run)
					} else {
						tasks := make([]Task, 1+rng.IntN(6))
						for j := range tasks {
							tasks[j] = taskFunc(run)
						}
						want.Add(int64(len(tasks)))
						tp.GoTasks(tasks)
					}
				}
			}(uint64(iter*4 + g))
		}
		producers.Wait()
		close(stopResizing)
		<-resized
		waitFor(t, "every task to run", func() bool { return ran.Load() == want.Load() })
		stopped := make(chan struct{})
		go func() { tp.Stop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d: Stop did not return", iter)
		}
		if got := tp.Workers(); got != 0 {
			t.Fatalf("iteration %d: Workers() = %d after Stop", iter, got)
		}
	}
}

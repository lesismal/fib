package taskpool

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

type indexTask int

func (indexTask) RunTask() {}

// Producers push through rings of every small size while consumers race to
// pop. Every task must come out exactly once: a ring of one cell used to let
// the producer write over a task a consumer had claimed but not yet read.
// A producer or consumer that finds the ring full or empty yields: spinning
// in place, it would hold a lone P until preemption took it off, 10ms a time,
// and with GOMAXPROCS 1 the test would take minutes.
//
// The larger limits start from a segment of two cells, so that the queue grows
// while producers claim in it and consumers drain the segments it left.
func TestTaskRingDeliversEachTaskOnce(t *testing.T) {
	for _, limit := range []int{1, 2, 3, 5, 8, 100, 1000} {
		const tasks, producers, consumers = 200000, 3, 4
		r := newTaskRingFrom(limit, 2)
		var seen [tasks]atomic.Int32
		var taken atomic.Int64
		var wg sync.WaitGroup
		for c := 0; c < consumers; c++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for taken.Load() < tasks {
					if task, ok := r.pop(); ok {
						seen[task.(indexTask)].Add(1)
						taken.Add(1)
					} else {
						runtime.Gosched()
					}
				}
			}()
		}
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := p; i < tasks; i += producers {
					for !r.push(indexTask(i)) {
						runtime.Gosched()
					}
				}
			}()
		}
		wg.Wait()
		for i := range seen {
			if got := seen[i].Load(); got != 1 {
				t.Fatalf("limit %d: task %d came out %d times", limit, i, got)
			}
		}
	}
}

func TestTaskRingHoldsItsLimit(t *testing.T) {
	r := newTaskRing(3)
	for i := 0; i < 3; i++ {
		if !r.push(indexTask(i)) {
			t.Fatalf("push %d refused below the limit", i)
		}
	}
	if r.push(indexTask(3)) {
		t.Fatal("push accepted past the limit")
	}
	if task, ok := r.pop(); !ok || task.(indexTask) != 0 {
		t.Fatalf("pop = %v, %v, want the oldest task", task, ok)
	}
	if !r.push(indexTask(3)) {
		t.Fatal("push refused after a pop made room")
	}
	if got := r.len(); got != 3 {
		t.Fatalf("len = %d, want 3", got)
	}
}

// A new ring writes none of its cells, so that their pages, and under the
// race detector the sync objects its atomics cost, come only as the queue
// reaches them; and a zeroed cell is free for its first lap.
func TestTaskRingStartsUntouched(t *testing.T) {
	r := newTaskRing(1000)
	cells := r.tail.Load().cells
	for i := range cells {
		if seq := cells[i].seq.Load(); seq != 0 {
			t.Fatalf("cell %d starts at seq %d", i, seq)
		}
	}
	// Many laps of a ring of four, through each position's cell in turn.
	r = newTaskRing(4)
	for lap := 0; lap < 1000; lap++ {
		for i := 0; i < 4; i++ {
			if !r.push(indexTask(lap*4 + i)) {
				t.Fatalf("lap %d: push %d refused", lap, i)
			}
		}
		if r.push(indexTask(-1)) {
			t.Fatalf("lap %d: push past the limit taken", lap)
		}
		for i := 0; i < 4; i++ {
			task, ok := r.pop()
			if !ok || task.(indexTask) != indexTask(lap*4+i) {
				t.Fatalf("lap %d: pop %d gave %v, %v", lap, i, task, ok)
			}
		}
		if _, ok := r.pop(); ok {
			t.Fatalf("lap %d: pop from an empty ring", lap)
		}
	}
}

// A queue starts small and grows only as far as its backlog takes it, so that
// a pool with a limit of 100,000 tasks costs a few kilobytes while it is
// never more than a few tasks deep.
func TestTaskRingGrowsWithItsBacklog(t *testing.T) {
	r := newTaskRing(100000)
	if got := len(r.tail.Load().cells); got != ringStartCells {
		t.Fatalf("a new queue has %d cells, want %d", got, ringStartCells)
	}
	// Many laps of a shallow queue leave it as it was.
	for i := 0; i < 10*ringStartCells; i++ {
		if !r.push(indexTask(i)) {
			t.Fatalf("push %d refused", i)
		}
		if task, ok := r.pop(); !ok || task.(indexTask) != indexTask(i) {
			t.Fatalf("pop %d = %v, %v", i, task, ok)
		}
	}
	if r.head.Load() != r.tail.Load() || len(r.tail.Load().cells) != ringStartCells {
		t.Fatal("a queue never more than one task deep grew")
	}
	// A backlog grows it, and its tasks come out in order across the
	// segments.
	const backlog = 5000
	for i := 0; i < backlog; i++ {
		if !r.push(indexTask(i)) {
			t.Fatalf("push %d refused below the limit", i)
		}
	}
	if got := r.len(); got != backlog {
		t.Fatalf("len = %d, want %d", got, backlog)
	}
	// The segments of 256 to 2,048 cells hold 3,840 of them.
	if got := len(r.tail.Load().cells); got != 4096 {
		t.Fatalf("a backlog of %d grew the queue to %d cells, want 4096", backlog, got)
	}
	for i := 0; i < backlog; i++ {
		if task, ok := r.pop(); !ok || task.(indexTask) != indexTask(i) {
			t.Fatalf("pop %d = %v, %v", i, task, ok)
		}
	}
	if _, ok := r.pop(); ok {
		t.Fatal("pop from an empty queue")
	}
	if r.head.Load() != r.tail.Load() {
		t.Fatal("the drained segments are still linked")
	}
}

// The tasks in the segments a queue grew out of count against its limit
// until they are taken.
func TestTaskRingHoldsItsLimitWhileGrowing(t *testing.T) {
	r := newTaskRingFrom(10, 2)
	for i := 0; i < 10; i++ {
		if !r.push(indexTask(i)) {
			t.Fatalf("push %d refused below the limit", i)
		}
	}
	if r.push(indexTask(10)) {
		t.Fatal("push accepted past the limit")
	}
	if got := r.len(); got != 10 {
		t.Fatalf("len = %d, want 10", got)
	}
	if r.head.Load() == r.tail.Load() {
		t.Fatal("the queue did not grow")
	}
	if task, ok := r.pop(); !ok || task.(indexTask) != 0 {
		t.Fatalf("pop = %v, %v, want the oldest task", task, ok)
	}
	if !r.push(indexTask(10)) {
		t.Fatal("push refused after a pop made room")
	}
	if r.push(indexTask(11)) {
		t.Fatal("push accepted past the limit")
	}
	for i := 1; i <= 10; i++ {
		if task, ok := r.pop(); !ok || task.(indexTask) != indexTask(i) {
			t.Fatalf("pop %d = %v, %v", i, task, ok)
		}
	}
	// Segments of 2, 4 and 8 cells hold the 10 tasks.
	if got := len(r.tail.Load().cells); got != 8 {
		t.Fatalf("the queue grew to %d cells, want 8", got)
	}
}

func BenchmarkTaskRingPushPop(b *testing.B) {
	r := newTaskRing(1024)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for !r.push(indexTask(0)) {
			}
			for {
				if _, ok := r.pop(); ok {
					break
				}
			}
		}
	})
}

func BenchmarkNewTaskRing(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = newTaskRing(100000)
	}
}

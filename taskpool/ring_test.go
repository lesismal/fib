package taskpool

import (
	"sync"
	"sync/atomic"
	"testing"
)

type indexTask int

func (indexTask) RunTask() {}

// One producer pushes through rings of every small size while consumers race
// to pop. Every task must come out exactly once: a ring of one cell used to
// let the producer write over a task a consumer had claimed but not yet read.
func TestTaskRingDeliversEachTaskOnce(t *testing.T) {
	for _, limit := range []int{1, 2, 3, 5, 8} {
		const tasks, consumers = 200000, 4
		r := newTaskRing(limit)
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
					}
				}
			}()
		}
		for i := 0; i < tasks; i++ {
			for !r.push(indexTask(i)) {
			}
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

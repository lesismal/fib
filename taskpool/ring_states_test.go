package taskpool

import "testing"

// A producer that finds the segment it filled already grown past by another
// producer tries again in the new one, rather than growing the queue twice.
func TestTaskRingGrowAfterAnotherProducer(t *testing.T) {
	r := newTaskRingFrom(100, 2)
	first := r.tail.Load()
	for i := 0; i < 3; i++ {
		if !r.push(indexTask(i)) {
			t.Fatalf("push %d refused", i)
		}
	}
	grown := r.tail.Load()
	if grown == first {
		t.Fatal("the queue did not grow")
	}
	if !r.grow(first) || r.tail.Load() != grown {
		t.Fatal("growing from a segment already grown past made another")
	}
}

// A consumer stops at a position its producer has claimed but not yet
// published, even once the queue has grown past it, and takes the task once
// it is published.
func TestTaskRingWaitsForAnUnpublishedTask(t *testing.T) {
	r := newTaskRingFrom(100, 2)
	s := r.tail.Load()
	// A producer claims position 0, and has yet to write its task.
	if !s.tail.CompareAndSwap(0, 1) {
		t.Fatal("claim")
	}
	if !r.grow(s) || r.tail.Load() == s {
		t.Fatal("the queue did not grow")
	}
	if !r.push(indexTask(2)) {
		t.Fatal("push into the new segment refused")
	}
	if task, ok := r.pop(); ok {
		t.Fatalf("popped %v past an unpublished task", task)
	}
	cell := &s.cells[0]
	cell.task = indexTask(1)
	s.setSeq(cell, 0, 1)
	for want := indexTask(1); want <= 2; want++ {
		if task, ok := r.pop(); !ok || task != want {
			t.Fatalf("pop = %v, %v; want %v", task, ok, want)
		}
	}
	if _, ok := r.pop(); ok {
		t.Fatal("pop from an empty queue")
	}
}

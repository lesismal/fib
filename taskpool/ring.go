package taskpool

import "sync/atomic"

// cacheLinePad keeps the counters that different cores write apart, so that
// a consumer taking the head does not invalidate the line a producer is
// writing the tail on.
type cacheLinePad [128]byte

type ringCell struct {
	// seq is which lap the cell is on. It equals the position the cell will
	// next be written at while it is free, that position plus one once a task
	// is published in it, and the position plus the ring's size once a
	// consumer has taken the task and handed the cell back.
	seq  atomic.Uint64
	task Task
}

// taskRing is a bounded queue for any number of producers and consumers,
// after Dmitry Vyukov's bounded MPMC queue. Producers claim a position with a
// compare and swap on the tail and consumers one on the head, so that neither
// a submitter nor a worker going back for its next task waits behind a lock.
type taskRing struct {
	cells []ringCell
	mask  uint64
	// limit is how many tasks the ring holds, which may be below the number
	// of cells: the cells are a power of two so that a position maps to its
	// cell with a mask.
	limit uint64
	_     cacheLinePad
	head  atomic.Uint64
	_     cacheLinePad
	tail  atomic.Uint64
	_     cacheLinePad
}

func newTaskRing(limit int) *taskRing {
	limit = max(limit, 1)
	// Two cells at least: with one, a cell holding the task at position n
	// and a free cell waiting for position n+1 carry the same seq, and the
	// producer would write over a task a consumer has claimed but not read.
	size := 2
	for size < limit {
		size <<= 1
	}
	r := &taskRing{cells: make([]ringCell, size), mask: uint64(size - 1), limit: uint64(limit)}
	for i := range r.cells {
		r.cells[i].seq.Store(uint64(i))
	}
	return r
}

// push publishes task and reports whether there was room for it. A producer
// claims a position before it publishes the task there, so for a moment the
// tail is past a task that is not in its cell yet; see pop.
func (r *taskRing) push(task Task) bool {
	for {
		tail := r.tail.Load()
		head := r.head.Load()
		if head > tail {
			// The tail moved on since it was read.
			continue
		}
		if tail-head >= r.limit {
			return false
		}
		cell := &r.cells[tail&r.mask]
		seq := cell.seq.Load()
		if seq != tail {
			if seq < tail {
				// The consumer that took the task a lap ago has not handed
				// the cell back yet.
				return false
			}
			// Another producer claimed this position first.
			continue
		}
		if !r.tail.CompareAndSwap(tail, tail+1) {
			continue
		}
		cell.task = task
		cell.seq.Store(tail + 1)
		return true
	}
}

// pop takes the oldest task, or reports that there is none. A position a
// producer has claimed but not yet published counts as none: that producer
// sends a worker to the queue once it has published, as every producer does.
func (r *taskRing) pop() (Task, bool) {
	for {
		head := r.head.Load()
		cell := &r.cells[head&r.mask]
		seq := cell.seq.Load()
		if seq != head+1 {
			if seq == head {
				// Empty, or its producer is still publishing.
				return nil, false
			}
			// Another consumer took this position first.
			continue
		}
		if !r.head.CompareAndSwap(head, head+1) {
			continue
		}
		task := cell.task
		cell.task = nil
		cell.seq.Store(head + r.mask + 1)
		return task, true
	}
}

// len is how many positions are claimed and not yet taken, including any
// whose task is still being published. It is exact only when nothing is
// moving, which is all its callers need: each one that acts on it checks
// again after.
func (r *taskRing) len() int {
	tail := r.tail.Load()
	head := r.head.Load()
	if head > tail {
		return 0
	}
	return int(tail - head)
}

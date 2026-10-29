package taskpool

import (
	"sync"
	"sync/atomic"
)

// cacheLinePad keeps the counters that different cores write apart, so that
// a consumer taking the head does not invalidate the line a producer is
// writing the tail on.
type cacheLinePad [128]byte

// ringStartCells is how many cells a queue starts with. A queue grows by
// doubling only when it fills, so that its memory follows the deepest backlog
// it has had rather than its limit: a limit of 100,000 tasks was 2.4 MB of
// cells per pool, every one of them touched once the positions had lapped the
// ring, while a queue a moderate load keeps a few tasks deep never grows past
// this.
const ringStartCells = 256

// closedBit marks a segment's tail once a larger segment has taken over from
// it: no producer claims a position in it after that, and once its consumers
// have taken what was claimed before, they move on to the next.
const closedBit = 1 << 63

type ringCell struct {
	// seq is which lap the cell is on, less the cell's index; see seqOf. The
	// full value equals the position the cell will next be written at while
	// it is free, that position plus one once a task is published in it, and
	// the position plus the ring's size once a consumer has taken the task and
	// handed the cell back.
	//
	// It is kept less the index so that a zeroed cell is already free for its
	// first position, and a new segment writes none of its cells: under the
	// race detector each address an atomic touches costs a sync object of a
	// few hundred bytes in the detector's own memory, and outside it the
	// cells' pages are not touched until the queue reaches them.
	seq  atomic.Uint64
	task Task
}

// taskRing is a bounded queue for any number of producers and consumers. Its
// tasks sit in segments, each after Dmitry Vyukov's bounded MPMC queue:
// producers claim a position with a compare and swap on the tail and
// consumers one on the head, so that neither a submitter nor a worker going
// back for its next task waits behind a lock.
//
// A queue starts with one small segment. A producer that finds it full while
// the queue is below its limit closes it and links a segment twice its size,
// under a lock that only growing takes; producers then claim in the new one,
// while consumers finish the old one before they move on. Segments never
// shrink, so a queue grows at most a few times in its life.
type taskRing struct {
	// head is the oldest segment, which consumers take from, and tail the
	// newest, which producers claim in. They are the same segment except
	// while an older one drains.
	head atomic.Pointer[ringSegment]
	tail atomic.Pointer[ringSegment]
	// limit is how many tasks the queue holds, which may be below the number
	// of cells it grows to: the cells are a power of two so that a position
	// maps to its cell with a mask.
	limit  uint64
	growMu sync.Mutex
}

type ringSegment struct {
	cells []ringCell
	mask  uint64
	// room is how many tasks the segment holds: its cells, or the queue's
	// limit if that is fewer.
	room uint64
	// base is the queue's position of the segment's first, where the one
	// before it closed: positions run on across segments, so that the queue's
	// length is its newest tail less its oldest head. Within a segment they
	// count from zero, as its cells' seq does.
	base uint64
	// next is the segment that took over from this one, once it is closed.
	next atomic.Pointer[ringSegment]
	_    cacheLinePad
	head atomic.Uint64
	_    cacheLinePad
	// tail carries closedBit once the segment is closed.
	tail atomic.Uint64
	_    cacheLinePad
}

func newTaskRing(limit int) *taskRing { return newTaskRingFrom(limit, ringStartCells) }

func newTaskRingFrom(limit, start int) *taskRing {
	r := new(taskRing)
	r.init(limit, start)
	return r
}

// init makes r a queue of limit whose first segment has start cells, or as
// many as limit takes if that is fewer. A pool keeps its queue in place
// rather than behind a pointer, so that a task is no further from its
// submitter than the one segment it sits in.
func (r *taskRing) init(limit, start int) {
	limit = max(limit, 1)
	r.limit = uint64(limit)
	s := newRingSegment(min(start, limit), r.limit, 0)
	r.head.Store(s)
	r.tail.Store(s)
}

// newRingSegment builds a segment of at least n cells for a queue of limit,
// starting at the queue's position base.
func newRingSegment(n int, limit, base uint64) *ringSegment {
	// Two cells at least: with one, a cell holding the task at position n
	// and a free cell waiting for position n+1 carry the same seq, and the
	// producer would write over a task a consumer has claimed but not read.
	size := 2
	for size < n {
		size <<= 1
	}
	return &ringSegment{cells: make([]ringCell, size), mask: uint64(size - 1), room: min(uint64(size), limit), base: base}
}

// seqOf reads the full seq of the cell for pos.
func (s *ringSegment) seqOf(cell *ringCell, pos uint64) uint64 {
	return cell.seq.Load() + pos&s.mask
}

// setSeq stores seq for the cell for pos.
func (s *ringSegment) setSeq(cell *ringCell, pos, seq uint64) {
	cell.seq.Store(seq - pos&s.mask)
}

// push publishes task and reports whether there was room for it. A producer
// claims a position before it publishes the task there, so for a moment the
// tail is past a task that is not in its cell yet; see pop.
//
// A segment that is full, or closed, sends the producer to advance, which
// finds it the segment to claim in next; the common case, one segment with
// room, takes no call.
func (r *taskRing) push(task Task) bool {
	s := r.tail.Load()
	room := s.room
	if r.head.Load() != s {
		room = r.room(s)
	}
	for {
		tail := s.tail.Load()
		head := s.head.Load()
		if head > tail {
			// The tail moved on since it was read.
			continue
		}
		// A closed segment's tail carries closedBit, which puts it past any
		// room.
		if tail-head >= room {
			if s, room = r.advance(s); s == nil {
				return false
			}
			continue
		}
		cell := &s.cells[tail&s.mask]
		seq := s.seqOf(cell, tail)
		if seq != tail {
			if seq < tail {
				// The consumer that took the task a lap ago has not handed
				// the cell back yet.
				if s, room = r.advance(s); s == nil {
					return false
				}
			}
			// Otherwise another producer claimed this position first.
			continue
		}
		if !s.tail.CompareAndSwap(tail, tail+1) {
			continue
		}
		cell.task = task
		s.setSeq(cell, tail, tail+1)
		return true
	}
}

// room is how many positions s, the newest segment, may have claimed: its
// own, less what the older segments still hold, since those count against
// the queue's limit until they are taken.
func (r *taskRing) room(s *ringSegment) uint64 {
	h := r.head.Load()
	head := h.base + h.head.Load()
	if head >= s.base {
		return s.room
	}
	if older := s.base - head; older < r.limit {
		return min(s.room, r.limit-older)
	}
	return 0
}

// advance finds the segment a producer that found s full or closed claims in
// next, and its room, growing the queue if s was full and the limit allows;
// or nil if the queue is full.
func (r *taskRing) advance(s *ringSegment) (*ringSegment, uint64) {
	if s.tail.Load()&closedBit != 0 {
		if r.tail.Load() == s {
			// The producer growing the queue closed s and has yet to
			// publish the next segment; it holds the lock until it has.
			r.growMu.Lock()
			//lint:ignore SA2001 taking the lock is the wait
			r.growMu.Unlock()
		}
	} else if !r.grow(s) {
		return nil, 0
	}
	s = r.tail.Load()
	return s, r.room(s)
}

// grow links a segment twice the size of s, the queue's newest, once s is
// full, and reports whether a producer should try again: false when the
// queue is at its limit or s is as large as the limit makes a segment.
func (r *taskRing) grow(s *ringSegment) bool {
	if uint64(len(s.cells)) >= r.limit && r.tail.Load() == s {
		// Full at its largest: a queue at its limit, as any submitter that
		// waits for room finds it, takes no lock.
		return false
	}
	r.growMu.Lock()
	defer r.growMu.Unlock()
	if r.tail.Load() != s {
		// Another producer grew it first.
		return true
	}
	if uint64(r.len()) >= r.limit {
		return false
	}
	// Closed first, so that the next segment starts where s ends; producers
	// that find s closed meanwhile wait for the lock. Linked before it is
	// the tail, so that a consumer can reach every task a producer queues
	// in it.
	end := s.close()
	next := newRingSegment(int(min(uint64(len(s.cells))*2, r.limit)), r.limit, s.base+end)
	s.next.Store(next)
	r.tail.Store(next)
	return true
}

// close stops producers from claiming positions in s, and returns its tail.
func (s *ringSegment) close() uint64 {
	for {
		tail := s.tail.Load()
		if s.tail.CompareAndSwap(tail, tail|closedBit) {
			return tail
		}
	}
}

// pop takes the oldest task, or reports that there is none. A position a
// producer has claimed but not yet published counts as none: that producer
// sends a worker to the queue once it has published, as every producer does.
// Consumers move on from a segment once it is closed and everything claimed
// in it is taken, so a task queued in a newer segment may wait behind such a
// producer too.
func (r *taskRing) pop() (Task, bool) {
	s := r.head.Load()
	for {
		head := s.head.Load()
		cell := &s.cells[head&s.mask]
		seq := s.seqOf(cell, head)
		if seq != head+1 {
			if seq == head {
				// Empty, or its producer is still publishing.
				next := s.next.Load()
				if next == nil {
					return nil, false
				}
				if s = r.moveOn(s, next); s == nil {
					return nil, false
				}
			}
			// Otherwise another consumer took this position first.
			continue
		}
		if !s.head.CompareAndSwap(head, head+1) {
			continue
		}
		task := cell.task
		cell.task = nil
		s.setSeq(cell, head, head+s.mask+1)
		return task, true
	}
}

// moveOn makes next the oldest segment once everything claimed in s, closed
// and found empty, has been taken, and returns the oldest segment then; or
// nil while a producer has yet to publish in s.
func (r *taskRing) moveOn(s, next *ringSegment) *ringSegment {
	if s.head.Load() != s.tail.Load()&^closedBit {
		return nil
	}
	r.head.CompareAndSwap(s, next)
	return r.head.Load()
}

// len is how many positions are claimed and not yet taken, including any
// whose task is still being published. It is exact only when nothing is
// moving, which is all its callers need: each one that acts on it checks
// again after.
func (r *taskRing) len() int {
	h, t := r.head.Load(), r.tail.Load()
	head := h.base + h.head.Load()
	tail := t.base + t.tail.Load()&^closedBit
	if head > tail {
		return 0
	}
	return int(tail - head)
}

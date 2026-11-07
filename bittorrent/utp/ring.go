package utp

// ring holds values by 16-bit sequence number, within a range of them
// starting at a base the caller tracks: the packets in flight, or those
// that arrived ahead of the next one expected. It grows by doubling to
// cover the range, which is at most a thousand or so.
type ring[T any] struct {
	slots []slot[T]
}

type slot[T any] struct {
	seq  uint16
	used bool
	v    T
}

// get returns the value at seq.
func (r *ring[T]) get(seq uint16) (T, bool) {
	if len(r.slots) > 0 {
		if s := &r.slots[int(seq)&(len(r.slots)-1)]; s.used && s.seq == seq {
			return s.v, true
		}
	}
	var zero T
	return zero, false
}

// put stores v at seq, which is at most 32767 past base, the oldest
// sequence number the ring may hold.
func (r *ring[T]) put(seq uint16, v T, base uint16) {
	need := int(seq-base) + 1
	if len(r.slots) < need {
		size := max(len(r.slots), 16)
		for size < need {
			size *= 2
		}
		old := r.slots
		r.slots = make([]slot[T], size)
		for _, s := range old {
			if s.used {
				r.slots[int(s.seq)&(size-1)] = s
			}
		}
	}
	r.slots[int(seq)&(len(r.slots)-1)] = slot[T]{seq: seq, used: true, v: v}
}

// del removes the value at seq.
func (r *ring[T]) del(seq uint16) {
	if len(r.slots) > 0 {
		if s := &r.slots[int(seq)&(len(r.slots)-1)]; s.used && s.seq == seq {
			*s = slot[T]{}
		}
	}
}

// reset removes every value, keeping the room.
func (r *ring[T]) reset() { clear(r.slots) }

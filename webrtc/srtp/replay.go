package srtp

// replayWindow is the replay list of RFC 3711 section 3.3.2: which of the
// size indices up to the highest one received have been seen, kept as a
// ring of bits indexed by the packet index modulo a multiple of 64.
type replayWindow struct {
	bits    []uint64
	size    uint64
	top     uint64
	started bool
}

func (w *replayWindow) init(size int) {
	w.size = uint64(size)
	w.bits = make([]uint64, (size+63)/64)
}

func (w *replayWindow) bit(i uint64) (word int, mask uint64) {
	i %= uint64(len(w.bits)) * 64
	return int(i / 64), 1 << (i % 64)
}

// check tells whether a packet with the index may be accepted: it is
// newer than any received, or inside the window and not received yet.
func (w *replayWindow) check(index uint64) bool {
	if !w.started || index > w.top {
		return true
	}
	if w.top-index >= w.size {
		return false
	}
	word, mask := w.bit(index)
	return w.bits[word]&mask == 0
}

// accept records an index check allowed, once the packet authenticated.
func (w *replayWindow) accept(index uint64) {
	ring := uint64(len(w.bits)) * 64
	switch {
	case !w.started:
		w.started = true
		clear(w.bits)
		w.top = index
	case index > w.top:
		if index-w.top >= ring {
			clear(w.bits)
		} else {
			for i := w.top + 1; i < index; i++ {
				word, mask := w.bit(i)
				w.bits[word] &^= mask
			}
		}
		w.top = index
	}
	word, mask := w.bit(index)
	w.bits[word] |= mask
}

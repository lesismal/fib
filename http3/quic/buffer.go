package quic

import (
	"bytes"
	"sort"

	"github.com/lesismal/fib/bufferpool"
)

// span is the byte range [off, end) of a stream.
type span struct{ off, end uint64 }

// addSpan merges s into spans, which are sorted and disjoint.
func addSpan(spans []span, s span) []span {
	if s.end <= s.off {
		return spans
	}
	i := sort.Search(len(spans), func(i int) bool { return spans[i].end >= s.off })
	j := i
	for j < len(spans) && spans[j].off <= s.end {
		s.off = min(s.off, spans[j].off)
		s.end = max(s.end, spans[j].end)
		j++
	}
	if i == j {
		spans = append(spans, span{})
		copy(spans[i+1:], spans[i:])
		spans[i] = s
		return spans
	}
	spans[i] = s
	return append(spans[:i+1], spans[j:]...)
}

// sendPiece is a run of a send buffer's bytes: the bytes from off on that it
// holds still, and mem, the buffer from the pool they are in, if they are the
// pool's. A run the caller handed over is kept as it is, so mem is nil for it
// and it is the caller's to keep until the peer has acknowledged it.
type sendPiece struct {
	off  uint64
	data []byte
	mem  []byte
}

// sendBuffer holds what a stream, or a packet number space's CRYPTO stream,
// has been given to send, until the peer acknowledges it.
//
// It is a list of runs rather than one buffer, so that a message the caller
// gave whole is kept as it is, rather than copied into a buffer of the send
// buffer's own: see writeParts. What is copied in grows in a buffer from the
// pool, as one buffer does, and goes back to the pool once acknowledged. What
// is sent is copied into packets as they are built, so a run holds nothing
// but the bytes the peer has yet to acknowledge.
type sendBuffer struct {
	// base is the offset of the first byte the peer has yet to acknowledge,
	// everything before it having been acknowledged, and size is how many
	// bytes from base on the buffer holds.
	base uint64
	size uint64
	// pieces holds those bytes, in order: pieces[0] holds the bytes from base
	// on, and each later one continues where the one before it ends. The
	// first few are kept in spare, which the stream carries with it, so that
	// a message of a part or two allocates nothing for them.
	pieces []sendPiece
	spare  [2]sendPiece
	// cur is the piece next is in, so that what is sent in order does not
	// search the pieces again. onAck keeps it right as pieces go.
	cur int
	// next is the offset of the first byte never sent.
	next uint64
	// lost are sent ranges to send again, and acked the acknowledged ones
	// beyond base.
	lost  []span
	acked []span
}

func (b *sendBuffer) end() uint64 { return b.base + b.size }

// add appends a run to the buffer, in the room the stream carries before it
// takes to the heap for one.
func (b *sendBuffer) add(p sendPiece) {
	if b.pieces == nil {
		b.pieces = b.spare[:0]
	}
	b.pieces = append(b.pieces, p)
}

// write copies p in, as one buffer of the send buffer's own: into the spare
// room of the last run where it is the pool's, in a larger buffer where that
// is not enough, so that a stream written in many small pieces does not keep
// a buffer for each.
func (b *sendBuffer) write(p []byte) {
	if len(p) == 0 {
		return
	}
	if i := len(b.pieces) - 1; i >= 0 && b.pieces[i].mem != nil {
		t := &b.pieces[i]
		if len(t.data)+len(p) <= cap(t.data) {
			t.data = append(t.data, p...)
			b.size += uint64(len(p))
			return
		}
		grown := bufferpool.Get(len(t.data) + len(p))[:len(t.data)]
		copy(grown, t.data)
		bufferpool.Put(t.mem)
		t.data, t.mem = append(grown, p...), grown
		b.size += uint64(len(p))
		return
	}
	mem := bufferpool.Get(len(p))[:len(p)]
	copy(mem, p)
	b.add(sendPiece{off: b.end(), data: mem, mem: mem})
	b.size += uint64(len(p))
}

// adopt appends p, a buffer from the pool that the send buffer takes over, to
// what it holds: kept as the buffer itself, which saves the copy, whether or
// not something else is waiting in it.
func (b *sendBuffer) adopt(p []byte) {
	if len(p) == 0 {
		bufferpool.Put(p)
		return
	}
	b.add(sendPiece{off: b.end(), data: p, mem: p})
	b.size += uint64(len(p))
}

// writeParts appends parts, in order, as the send buffer's own runs rather
// than copying them into one: a message given whole, its header and its body
// as separate parts, costs the memory of the body once and not twice. An
// owned part goes back to the pool once acknowledged; one that is not is the
// caller's until then, and must not be changed in the meantime.
func (b *sendBuffer) writeParts(parts []Part) {
	for _, p := range parts {
		if len(p.Data) == 0 {
			if p.Owned {
				bufferpool.Put(p.Data)
			}
			continue
		}
		piece := sendPiece{off: b.end(), data: p.Data}
		if p.Owned {
			piece.mem = p.Data
		}
		b.add(piece)
		b.size += uint64(len(p.Data))
	}
}

// hasLost reports whether some sent data needs sending again.
func (b *sendBuffer) hasLost() bool {
	for len(b.lost) > 0 && b.lost[0].end <= b.base {
		b.lost = b.lost[1:]
	}
	return len(b.lost) > 0
}

// runAt returns the bytes of the run holding off, from off on, which end
// where that run does: a frame is built from one run, so that what it holds
// is never joined first.
func (b *sendBuffer) runAt(off uint64) []byte {
	for i := range b.pieces {
		p := &b.pieces[i]
		if end := p.off + uint64(len(p.data)); end > off {
			return p.data[off-p.off : end-p.off]
		}
	}
	return nil
}

// popLost takes up to n bytes of the first lost range.
func (b *sendBuffer) popLost(n uint64) (uint64, []byte) {
	if n == 0 {
		return 0, nil
	}
	for b.hasLost() {
		s := &b.lost[0]
		s.off = max(s.off, b.base)
		off := s.off
		data := b.runAt(off)
		if len(data) == 0 {
			// Nothing kept there: the range is not sent again.
			s.off = s.end
		} else {
			if uint64(len(data)) > n {
				data = data[:n]
			}
			if end := s.end - off; uint64(len(data)) > end {
				data = data[:end]
			}
			s.off = off + uint64(len(data))
			if len(data) > 0 {
				if s.off >= s.end {
					b.lost = b.lost[1:]
				}
				return off, data
			}
		}
		if s.off >= s.end {
			b.lost = b.lost[1:]
		}
	}
	return 0, nil
}

// popNew takes up to n bytes never sent before.
func (b *sendBuffer) popNew(n uint64) (uint64, []byte) {
	off := b.next
	if off >= b.end() || n == 0 {
		return off, nil
	}
	for b.cur < len(b.pieces) && b.pieces[b.cur].off+uint64(len(b.pieces[b.cur].data)) <= off {
		b.cur++
	}
	if b.cur == len(b.pieces) {
		return off, nil
	}
	p := &b.pieces[b.cur]
	data := p.data[off-p.off:]
	if uint64(len(data)) > n {
		data = data[:n]
	}
	b.next = off + uint64(len(data))
	return off, data
}

// onAck records that [off, off+n) arrived, and lets go of what now needs no
// keeping.
func (b *sendBuffer) onAck(off, n uint64) {
	var newBase uint64
	if len(b.acked) == 0 && off <= b.base {
		// Acknowledged in order, which is how nearly everything is: the
		// range moves the base on without being recorded.
		newBase = off + n
	} else {
		b.acked = addSpan(b.acked, span{off, off + n})
		if len(b.acked) == 0 || b.acked[0].off > b.base {
			return
		}
		newBase = b.acked[0].end
		b.acked = b.acked[1:]
	}
	if newBase > b.end() {
		newBase = b.end()
	}
	if newBase <= b.base {
		return
	}
	b.dropTo(newBase)
}

// dropTo lets go of the bytes before off: each run they take is dropped, and
// the run they end in is cut, its buffer kept for the pool.
func (b *sendBuffer) dropTo(off uint64) {
	drop := 0
	for drop < len(b.pieces) {
		p := &b.pieces[drop]
		end := p.off + uint64(len(p.data))
		if end > off {
			break
		}
		if p.mem != nil {
			bufferpool.Put(p.mem)
		}
		p.data, p.mem = nil, nil
		drop++
	}
	b.pieces = b.pieces[drop:]
	b.cur = max(b.cur-drop, 0)
	if len(b.pieces) > 0 && b.pieces[0].off < off {
		p := &b.pieces[0]
		p.data = p.data[off-p.off:]
		p.off = off
	}
	b.size -= min(off-b.base, b.size)
	b.base = off
	if len(b.pieces) == 0 {
		// Let the list go rather than grow it from its tail forever.
		b.pieces = b.spare[:0]
		b.cur = 0
	}
}

// release gives back what the pool holds of the buffer, for a stream that is
// reset or done with: nothing in it is sent again.
func (b *sendBuffer) release() {
	for i := range b.pieces {
		if b.pieces[i].mem != nil {
			bufferpool.Put(b.pieces[i].mem)
		}
		b.pieces[i] = sendPiece{}
	}
	b.pieces, b.size = b.spare[:0], 0
	// The peer may have seen what was sent: the final size is where next got
	// to, and nothing from there on is held.
	b.base = b.next
	b.cur = 0
	b.lost, b.acked = nil, nil
}

// onLost queues [off, off+n) to be sent again, if it is still wanted.
func (b *sendBuffer) onLost(off, n uint64) {
	end := off + n
	off = max(off, b.base)
	if end <= off {
		return
	}
	b.lost = addSpan(b.lost, span{off, end})
}

// allAcked reports whether everything written has been acknowledged.
func (b *sendBuffer) allAcked() bool { return len(b.pieces) == 0 }

// segment is received data waiting for what comes before it.
type segment struct {
	off  uint64
	data []byte
}

func (s *segment) end() uint64 { return s.off + uint64(len(s.data)) }

// minRecvSegments is the fewest segments a receive buffer holds before it
// gives up on its peer; see recvSegmentLimit.
const minRecvSegments = 1024

// recvSegmentLimit is how many segments a receive buffer over window bytes
// may hold: one for every half kilobyte of it, so that a peer losing every
// other full packet stays well inside the limit, and never fewer than
// minRecvSegments. Each segment holds a byte at least, and the window
// bounds the bytes already; what the limit bounds is the cost of the
// segments themselves, which a peer sending a byte here and a byte there
// would otherwise make one of for every other byte of the window.
func recvSegmentLimit(window uint64) int {
	return int(min(max(window>>9, minRecvSegments), 1<<20))
}

// recvBuffer reassembles a stream from frames that may arrive out of order,
// overlap or repeat.
type recvBuffer struct {
	// offset is how far the data has been delivered in order.
	offset uint64
	// segments are ascending and disjoint, and all past offset: a byte is
	// held once however often it arrives, so what they hold is bounded by
	// the flow control window that bounds the frames' ends.
	segments []segment
	// buffered is how many bytes the segments hold.
	buffered int
}

// take hands on a frame's data at once, less what has been delivered already,
// when it continues the stream where delivery left off and nothing waits
// ahead of it, which is how nearly every frame arrives. Otherwise it stores
// the data for pop and returns nil. It reports false when storing the data
// would take more than limit segments, which only a peer cutting its data
// into ever more pieces asks for.
func (r *recvBuffer) take(off uint64, data []byte, limit int) ([]byte, bool) {
	if len(r.segments) > 0 || off > r.offset {
		return nil, r.push(off, data, limit)
	}
	end := off + uint64(len(data))
	if end <= r.offset {
		return nil, true
	}
	data = data[r.offset-off:]
	r.offset = end
	return data, true
}

// push stores a copy of what a frame's data adds to what has been delivered
// and stored already, since it outlives the datagram it arrived in; pop then
// hands it on in order. A stored segment keeps what it holds, and segments
// the data covers whole give way to it, so a frame sent again, or ever larger
// ones over the same bytes, hold them no more than once. Data that continues
// a segment is added to it. It reports false, storing nothing, when the data
// would make more than limit segments.
func (r *recvBuffer) push(off uint64, data []byte, limit int) bool {
	end := off + uint64(len(data))
	if end <= r.offset || len(data) == 0 {
		return true
	}
	if off < r.offset {
		data = data[r.offset-off:]
		off = r.offset
	}
	segs := r.segments
	// i is the first segment that ends past off.
	i := sort.Search(len(segs), func(i int) bool { return segs[i].end() > off })
	if i < len(segs) && segs[i].off <= off {
		// The data starts inside segment i, which has those bytes already.
		e := segs[i].end()
		if e >= end {
			return true
		}
		data = data[e-off:]
		off = e
		i++
	}
	// Segments i up to j lie inside [off, end), and give way to the data.
	j := i
	for j < len(segs) && segs[j].end() <= end {
		j++
	}
	if j < len(segs) && segs[j].off < end {
		// The data ends inside segment j, which has those bytes already.
		end = segs[j].off
		data = data[:end-off]
		if len(data) == 0 {
			return true
		}
	}
	for k := i; k < j; k++ {
		r.buffered -= len(segs[k].data)
	}
	switch {
	case i > 0 && segs[i-1].end() == off:
		segs[i-1].data = append(segs[i-1].data, data...)
		r.segments = cutSegments(segs, i, j)
	case i < j:
		segs[i] = segment{off: off, data: bytes.Clone(data)}
		r.segments = cutSegments(segs, i+1, j)
	default:
		if len(segs) >= limit {
			return false
		}
		segs = append(segs, segment{})
		copy(segs[i+1:], segs[i:])
		segs[i] = segment{off: off, data: bytes.Clone(data)}
		r.segments = segs
	}
	r.buffered += len(data)
	return true
}

// cutSegments drops segs[i:j], whose bytes the caller has taken out of
// buffered, and lets go of what they held.
func cutSegments(segs []segment, i, j int) []segment {
	if i == j {
		return segs
	}
	n := len(segs)
	segs = append(segs[:i], segs[j:]...)
	clear(segs[len(segs):n])
	return segs
}

// pop returns the next data in order, or nil when there is a gap first.
func (r *recvBuffer) pop() []byte {
	if len(r.segments) == 0 || r.segments[0].off > r.offset {
		return nil
	}
	s := r.segments[0]
	r.segments[0] = segment{}
	r.segments = r.segments[1:]
	r.buffered -= len(s.data)
	data := s.data[r.offset-s.off:]
	r.offset = s.end()
	return data
}

// pnRange is the packet numbers [lo, hi].
type pnRange struct{ lo, hi uint64 }

// maxAckRanges bounds the ranges kept for acknowledgement; the oldest go
// first, and the peer has long since given up on them.
const maxAckRanges = 32

// rangeSet is the packet numbers received in a space, as ascending, disjoint
// and non-adjacent ranges.
type rangeSet []pnRange

// add records pn, reporting false if it was already there or is too old to
// tell.
func (s *rangeSet) add(pn uint64) bool {
	r := *s
	if len(r) > 0 && pn < r[0].lo && len(r) == maxAckRanges {
		return false
	}
	i := sort.Search(len(r), func(i int) bool { return r[i].hi+1 >= pn })
	if i < len(r) && r[i].lo <= pn && pn <= r[i].hi {
		return false
	}
	switch {
	case i < len(r) && r[i].hi+1 == pn:
		r[i].hi = pn
		if i+1 < len(r) && r[i+1].lo == pn+1 {
			r[i].hi = r[i+1].hi
			r = append(r[:i+1], r[i+2:]...)
		}
	case i < len(r) && r[i].lo == pn+1:
		r[i].lo = pn
	default:
		r = append(r, pnRange{})
		copy(r[i+1:], r[i:])
		r[i] = pnRange{pn, pn}
	}
	if len(r) > maxAckRanges {
		r = r[1:]
	}
	*s = r
	return true
}

func (s rangeSet) largest() uint64 { return s[len(s)-1].hi }

// forget drops the ranges wholly below pn, but never the last one, which
// packet numbers are decoded against.
func (s *rangeSet) forget(pn uint64) {
	r := *s
	i := 0
	for i < len(r)-1 && r[i].hi < pn {
		i++
	}
	*s = r[i:]
}

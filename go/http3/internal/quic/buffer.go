package quic

import (
	"bytes"
	"sort"

	"github.com/lesismal/fib/go/bufferpool"
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

// sendBuffer holds what a stream, or a packet number space's CRYPTO stream,
// has been given to send, until the peer acknowledges it.
type sendBuffer struct {
	// buf holds the bytes from offset base on; everything before base has
	// been acknowledged. It is a window of mem, an array from the buffer
	// pool, which goes back to it once everything written is acknowledged:
	// what is sent is copied into packets as they are built, so nothing
	// else holds on to it.
	base uint64
	buf  []byte
	mem  []byte
	// next is the offset of the first byte never sent.
	next uint64
	// lost are sent ranges to send again, and acked the acknowledged ones
	// beyond base.
	lost  []span
	acked []span
}

func (b *sendBuffer) end() uint64 { return b.base + uint64(len(b.buf)) }

func (b *sendBuffer) write(p []byte) {
	if len(b.buf)+len(p) > cap(b.buf) {
		grown := bufferpool.Get(len(b.buf) + len(p))[:len(b.buf)]
		copy(grown, b.buf)
		bufferpool.Put(b.mem)
		b.mem, b.buf = grown, grown
	}
	b.buf = append(b.buf, p...)
}

// adopt appends p, a buffer from the pool that the send buffer takes over:
// kept as the buffer itself when nothing else is waiting in it, which saves
// the copy, and copied and given back otherwise.
func (b *sendBuffer) adopt(p []byte) {
	if len(b.buf) > 0 || len(p) == 0 {
		b.write(p)
		bufferpool.Put(p)
		return
	}
	bufferpool.Put(b.mem)
	b.mem, b.buf = p, p
}

// hasLost reports whether some sent data needs sending again.
func (b *sendBuffer) hasLost() bool {
	for len(b.lost) > 0 && b.lost[0].end <= b.base {
		b.lost = b.lost[1:]
	}
	return len(b.lost) > 0
}

// popLost takes up to n bytes of the first lost range.
func (b *sendBuffer) popLost(n uint64) (uint64, []byte) {
	if !b.hasLost() || n == 0 {
		return 0, nil
	}
	s := &b.lost[0]
	s.off = max(s.off, b.base)
	off := s.off
	end := min(s.end, off+n)
	data := b.buf[off-b.base : end-b.base]
	s.off = end
	if s.off >= s.end {
		b.lost = b.lost[1:]
	}
	return off, data
}

// popNew takes up to n bytes never sent before.
func (b *sendBuffer) popNew(n uint64) (uint64, []byte) {
	off := b.next
	end := min(b.end(), off+n)
	b.next = end
	return off, b.buf[off-b.base : end-b.base]
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
	if newBase <= b.base {
		return
	}
	b.buf = b.buf[newBase-b.base:]
	b.base = newBase
	if len(b.buf) == 0 {
		// Let the array go rather than grow it from its tail forever.
		bufferpool.Put(b.mem)
		b.mem, b.buf = nil, nil
	}
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
func (b *sendBuffer) allAcked() bool { return len(b.buf) == 0 }

// segment is received data waiting for what comes before it.
type segment struct {
	off  uint64
	data []byte
}

// recvBuffer reassembles a stream from frames that may arrive out of order,
// overlap or repeat.
type recvBuffer struct {
	// offset is how far the data has been delivered in order.
	offset   uint64
	segments []segment
	// buffered is how many bytes the segments hold.
	buffered int
}

// take hands on a frame's data at once, less what has been delivered already,
// when it continues the stream where delivery left off and nothing waits
// ahead of it, which is how nearly every frame arrives. Otherwise it stores
// the data for pop and returns nil.
func (r *recvBuffer) take(off uint64, data []byte) []byte {
	if len(r.segments) > 0 || off > r.offset {
		r.push(off, data)
		return nil
	}
	end := off + uint64(len(data))
	if end <= r.offset {
		return nil
	}
	data = data[r.offset-off:]
	r.offset = end
	return data
}

// push stores a copy of a frame's data, less what has been delivered
// already, since it outlives the datagram it arrived in; pop then hands it
// on in order.
func (r *recvBuffer) push(off uint64, data []byte) {
	end := off + uint64(len(data))
	if end <= r.offset || len(data) == 0 {
		return
	}
	if off < r.offset {
		data = data[r.offset-off:]
		off = r.offset
	}
	i := sort.Search(len(r.segments), func(i int) bool { return r.segments[i].off > off })
	r.segments = append(r.segments, segment{})
	copy(r.segments[i+1:], r.segments[i:])
	r.segments[i] = segment{off: off, data: bytes.Clone(data)}
	r.buffered += len(data)
}

// pop returns the next data in order, or nil when there is a gap first.
func (r *recvBuffer) pop() []byte {
	for len(r.segments) > 0 {
		s := r.segments[0]
		if s.off > r.offset {
			return nil
		}
		r.segments = r.segments[1:]
		r.buffered -= len(s.data)
		end := s.off + uint64(len(s.data))
		if end <= r.offset {
			continue
		}
		data := s.data[r.offset-s.off:]
		r.offset = end
		return data
	}
	return nil
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

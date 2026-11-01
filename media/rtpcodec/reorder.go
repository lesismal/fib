package rtpcodec

import (
	"time"

	"github.com/lesismal/fib/webrtc/rtp"
)

// Defaults of a Reorder buffer.
const (
	DefaultReorderSize  = 128
	DefaultReorderDelay = 100 * time.Millisecond
)

// Reorder puts the RTP packets of one source that arrive over UDP back in
// sequence order, waiting a little for the ones missing: a packet that
// arrives after a gap is held until the gap fills, until Size packets are
// held, or until it has waited Delay, and then the gap is given up as lost
// and the packets after it go on. Packets that come too late, or twice, are
// dropped. It is sans-IO: the caller gives it the time with each packet,
// and calls Expire at NextTimeout. It is not safe for concurrent use.
type Reorder struct {
	size  int
	delay time.Duration

	started bool
	next    uint16 // the sequence number expected next
	// slots hold the packets after a gap, by sequence number modulo size.
	slots []reorderSlot
	held  int
	// oldest is when the first packet held arrived.
	oldest time.Time
	out    rtp.Packet
}

type reorderSlot struct {
	used    bool
	seq     uint16
	arrived time.Time
	data    []byte
}

// NewReorder returns a Reorder that holds up to size packets, for up to
// delay; zero means DefaultReorderSize or DefaultReorderDelay. The size is
// rounded up to a power of two, at most 32768.
func NewReorder(size int, delay time.Duration) *Reorder {
	if size <= 0 {
		size = DefaultReorderSize
	}
	// A power of two divides 2^16, so the slots of the sequence numbers a
	// window holds stay distinct across the wrap.
	n := 2
	for n < size && n < 1<<15 {
		n <<= 1
	}
	size = n
	if delay <= 0 {
		delay = DefaultReorderDelay
	}
	return &Reorder{size: size, delay: delay, slots: make([]reorderSlot, size)}
}

// Push takes packet p, which arrived at now, and calls emit with every
// packet that is now in order, p among them. A packet held is copied, so p
// need only be valid during the call; the packets emit gets are valid
// during the emit call.
func (r *Reorder) Push(now time.Time, p *rtp.Packet, emit func(*rtp.Packet)) {
	seq := p.SequenceNumber
	if !r.started {
		r.started, r.next = true, seq
	}
	diff := int16(seq - r.next)
	switch {
	case diff < 0:
		// Late or duplicate. A sender that restarted its numbering sends
		// far behind for good; after a buffer's worth, follow it.
		if -int(diff) < 8*r.size {
			return
		}
		r.skipAll(emit)
		r.next = seq
		fallthrough
	case diff == 0:
		r.next = seq + 1
		emit(p)
		r.drain(emit)
		return
	}
	if int(diff) >= r.size {
		// Too far ahead to hold: give up the gap and what is held.
		r.skipAll(emit)
		r.next = seq + 1
		emit(p)
		return
	}
	i := int(seq) % r.size
	if r.slots[i].used {
		return
	}
	b, err := p.Append(r.slots[i].data[:0])
	if err != nil {
		return
	}
	if r.held == 0 {
		r.oldest = now
	}
	r.slots[i] = reorderSlot{used: true, seq: seq, arrived: now, data: b}
	r.held++
	if r.held >= r.size-1 {
		r.skipGap(emit)
	}
}

// NextTimeout returns when Expire is next due, and false while nothing is
// held.
func (r *Reorder) NextTimeout() (time.Time, bool) {
	if r.held == 0 {
		return time.Time{}, false
	}
	return r.oldest.Add(r.delay), true
}

// Expire gives up the gaps that held packets for longer than the delay by
// now, emitting the packets after them.
func (r *Reorder) Expire(now time.Time, emit func(*rtp.Packet)) {
	for r.held > 0 && !now.Before(r.oldest.Add(r.delay)) {
		r.skipGap(emit)
	}
}

// skipGap moves past the gap before the first packet held, and emits from
// there.
func (r *Reorder) skipGap(emit func(*rtp.Packet)) {
	for range r.size {
		if r.slots[int(r.next)%r.size].used && r.slots[int(r.next)%r.size].seq == r.next {
			break
		}
		r.next++
	}
	r.drain(emit)
}

// skipAll emits everything held, in order, gaps and all.
func (r *Reorder) skipAll(emit func(*rtp.Packet)) {
	for r.held > 0 {
		r.skipGap(emit)
	}
}

// drain emits the held packets that continue the sequence.
func (r *Reorder) drain(emit func(*rtp.Packet)) {
	for r.held > 0 {
		s := &r.slots[int(r.next)%r.size]
		if !s.used || s.seq != r.next {
			break
		}
		s.used = false
		r.held--
		r.next++
		if r.out.Unmarshal(s.data) == nil {
			emit(&r.out)
		}
	}
	if r.held > 0 {
		// The oldest of what is still held.
		r.oldest = time.Time{}
		for i := range r.slots {
			if s := &r.slots[i]; s.used && (r.oldest.IsZero() || s.arrived.Before(r.oldest)) {
				r.oldest = s.arrived
			}
		}
	}
}

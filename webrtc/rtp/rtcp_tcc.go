package rtp

import (
	"encoding/binary"
	"slices"
	"time"
)

// The status symbols of transport-wide feedback: whether each packet
// arrived, and the size of the delta its arrival time is given by.
const (
	TCCNotReceived uint8 = 0
	TCCSmallDelta  uint8 = 1
	TCCLargeDelta  uint8 = 2
)

// TCCDeltaUnit is the unit of TransportCC's arrival deltas, and
// TCCReferenceUnit that of its reference time.
const (
	TCCDeltaUnit     = 250 * time.Microsecond
	TCCReferenceUnit = 64 * time.Millisecond
)

// TransportCC is the transport-wide congestion control feedback of
// draft-holmer-rmcat-transport-wide-cc-extensions-01 section 3.1, an RTPFB
// that tells the sender when each packet it numbered arrived.
type TransportCC struct {
	SenderSSRC uint32
	MediaSSRC  uint32
	// BaseSeq is the transport-wide sequence number of the first packet
	// the feedback covers.
	BaseSeq uint16
	// ReferenceTime is a 24-bit time in TCCReferenceUnit, from an epoch
	// of the receiver's choosing, the first delta counts from.
	ReferenceTime uint32
	// FeedbackCount numbers the feedback packets the receiver sends.
	FeedbackCount uint8
	// Status holds a symbol for each packet from BaseSeq on: TCCNotReceived,
	// TCCSmallDelta or TCCLargeDelta. It is reused from one Unmarshal to
	// the next.
	Status []uint8
	// Deltas holds, for each packet that arrived, in order, its arrival
	// time less that of the one before, or less ReferenceTime for the
	// first, in TCCDeltaUnit. A small delta is 0 to 255; a large one any
	// other. It is reused from one Unmarshal to the next.
	Deltas []int16
}

func (p *TransportCC) Unmarshal(b []byte) error {
	sender, media, fci, err := feedbackBody(b, TypeRTPFB, FormatTCC)
	if err != nil {
		return err
	}
	if len(fci) < 8 {
		return ErrRTCPFormat
	}
	p.SenderSSRC, p.MediaSSRC = sender, media
	p.BaseSeq = binary.BigEndian.Uint16(fci)
	count := int(binary.BigEndian.Uint16(fci[2:]))
	p.ReferenceTime = uint32(fci[4])<<16 | uint32(fci[5])<<8 | uint32(fci[6])
	p.FeedbackCount = fci[7]
	off := 8
	p.Status = p.Status[:0]
	for len(p.Status) < count {
		if off+2 > len(fci) {
			return ErrRTCPFormat
		}
		chunk := binary.BigEndian.Uint16(fci[off:])
		off += 2
		left := count - len(p.Status)
		switch {
		case chunk&0x8000 == 0:
			symbol := uint8(chunk >> 13 & 3)
			if symbol > TCCLargeDelta {
				return ErrRTCPFormat
			}
			n := min(int(chunk&0x1fff), left)
			for range n {
				p.Status = append(p.Status, symbol)
			}
		case chunk&0x4000 == 0:
			for i := range min(14, left) {
				symbol := uint8(chunk >> (13 - i) & 1)
				p.Status = append(p.Status, symbol)
			}
		default:
			for i := range min(7, left) {
				symbol := uint8(chunk >> (12 - 2*i) & 3)
				if symbol > TCCLargeDelta {
					return ErrRTCPFormat
				}
				p.Status = append(p.Status, symbol)
			}
		}
	}
	p.Deltas = p.Deltas[:0]
	for _, symbol := range p.Status {
		switch symbol {
		case TCCSmallDelta:
			if off >= len(fci) {
				return ErrRTCPFormat
			}
			p.Deltas = append(p.Deltas, int16(fci[off]))
			off++
		case TCCLargeDelta:
			if off+2 > len(fci) {
				return ErrRTCPFormat
			}
			p.Deltas = append(p.Deltas, int16(binary.BigEndian.Uint16(fci[off:])))
			off += 2
		}
	}
	return nil
}

// tccChunk returns the next packet status chunk for the symbols in s and
// how many it covers: a run length chunk for a run of 14 or more, or one
// that reaches the end; otherwise a status vector of fourteen 1-bit
// symbols when it can, or of seven 2-bit ones.
func tccChunk(s []uint8) (uint16, int) {
	run := 1
	for run < len(s) && run < 0x1fff && s[run] == s[0] {
		run++
	}
	if run >= 14 || run == len(s) {
		return uint16(s[0])<<13 | uint16(run), run
	}
	n := min(14, len(s))
	if !slices.Contains(s[:n], TCCLargeDelta) {
		chunk := uint16(0x8000)
		for i, symbol := range s[:n] {
			chunk |= uint16(symbol) << (13 - i)
		}
		return chunk, n
	}
	n = min(7, len(s))
	chunk := uint16(0xc000)
	for i, symbol := range s[:n] {
		chunk |= uint16(symbol) << (12 - 2*i)
	}
	return chunk, n
}

func (p *TransportCC) fciSize() int {
	n := 8
	for s := p.Status; len(s) > 0; {
		_, used := tccChunk(s)
		s = s[used:]
		n += 2
	}
	for _, symbol := range p.Status {
		n += int(symbol)
	}
	return n
}

func (p *TransportCC) MarshalSize() int {
	return RTCPHeaderSize + 8 + (p.fciSize()+3)&^3
}

// Append appends the feedback, padded to a multiple of 4 bytes with the P
// bit set as libwebrtc does. Each delta must fit the symbol it has.
func (p *TransportCC) Append(dst []byte) ([]byte, error) {
	if len(p.Status) > 0xffff {
		return dst, ErrRTCPTooMany
	}
	received := 0
	for _, symbol := range p.Status {
		if symbol > TCCLargeDelta {
			return dst, ErrRTCPFormat
		}
		if symbol != TCCNotReceived {
			if received >= len(p.Deltas) {
				return dst, ErrRTCPFormat
			}
			if d := p.Deltas[received]; symbol == TCCSmallDelta && (d < 0 || d > 255) {
				return dst, ErrRTCPFormat
			}
			received++
		}
	}
	if received != len(p.Deltas) {
		return dst, ErrRTCPFormat
	}
	// At most 0xffff symbols, two bytes of delta each and a chunk for each
	// seven, take some 150KB, well within what the length field counts.
	fci := p.fciSize()
	pad := (4 - fci%4) % 4
	start := len(dst)
	dst = appendFeedbackHeader(dst, TypeRTPFB, FormatTCC, p.SenderSSRC, p.MediaSSRC, fci+pad)
	if pad > 0 {
		dst[start] |= 0x20
	}
	dst = binary.BigEndian.AppendUint16(dst, p.BaseSeq)
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(p.Status)))
	dst = append(dst, byte(p.ReferenceTime>>16), byte(p.ReferenceTime>>8), byte(p.ReferenceTime), p.FeedbackCount)
	for s := p.Status; len(s) > 0; {
		chunk, used := tccChunk(s)
		dst = binary.BigEndian.AppendUint16(dst, chunk)
		s = s[used:]
	}
	received = 0
	for _, symbol := range p.Status {
		switch symbol {
		case TCCSmallDelta:
			dst = append(dst, byte(p.Deltas[received]))
			received++
		case TCCLargeDelta:
			dst = binary.BigEndian.AppendUint16(dst, uint16(p.Deltas[received]))
			received++
		}
	}
	if pad > 0 {
		for range pad - 1 {
			dst = append(dst, 0)
		}
		dst = append(dst, byte(pad))
	}
	return dst, nil
}

// All yields the transport-wide sequence number of each packet the
// feedback covers and its arrival time, counted from ReferenceTime's epoch,
// or -1 for one that did not arrive.
func (p *TransportCC) All(yield func(seq uint16, arrival time.Duration) bool) {
	at := time.Duration(p.ReferenceTime) * TCCReferenceUnit
	received := 0
	for i, symbol := range p.Status {
		arrival := time.Duration(-1)
		if symbol != TCCNotReceived && received < len(p.Deltas) {
			at += time.Duration(p.Deltas[received]) * TCCDeltaUnit
			arrival = at
			received++
		}
		if !yield(p.BaseSeq+uint16(i), arrival) {
			return
		}
	}
}

// TransportCCRecorder records when packets with a transport-wide sequence
// number arrived and builds the TransportCC feedback that reports them.
type TransportCCRecorder struct {
	SenderSSRC uint32
	MediaSSRC  uint32
	// MaxPacketSize bounds each feedback packet; 0 means 1200 bytes.
	MaxPacketSize int

	unwrap   SeqUnwrapper
	arrivals []tccArrival
	// next is the first sequence number not yet reported; packets before
	// it that arrive late are dropped.
	next     int64
	started  bool
	fbCount  uint8
	feedback TransportCC
}

type tccArrival struct {
	seq int64
	at  time.Duration
}

// Record notes that the packet with the sequence number arrived at the
// time, a nonnegative duration from an epoch the recorder keeps for its
// life, such as the time it was created.
func (r *TransportCCRecorder) Record(seq uint16, arrival time.Duration) {
	s := r.unwrap.Unwrap(seq)
	if r.started && s < r.next {
		return
	}
	r.arrivals = append(r.arrivals, tccArrival{seq: s, at: max(arrival, 0)})
}

// Pending tells whether packets have arrived that no feedback reported.
func (r *TransportCCRecorder) Pending() bool { return len(r.arrivals) > 0 }

// AppendFeedback appends feedback for every packet recorded since the last
// call, as many TransportCC packets as that takes, and forgets them.
func (r *TransportCCRecorder) AppendFeedback(dst []byte) ([]byte, error) {
	if len(r.arrivals) == 0 {
		return dst, nil
	}
	slices.SortStableFunc(r.arrivals, func(a, b tccArrival) int {
		switch {
		case a.seq < b.seq:
			return -1
		case a.seq > b.seq:
			return 1
		}
		return 0
	})
	r.arrivals = slices.CompactFunc(r.arrivals, func(a, b tccArrival) bool { return a.seq == b.seq })
	maxSize := r.MaxPacketSize
	if maxSize <= 0 {
		maxSize = 1200
	}
	fb := &r.feedback
	for i := 0; i < len(r.arrivals); {
		first := r.arrivals[i]
		fb.SenderSSRC, fb.MediaSSRC = r.SenderSSRC, r.MediaSSRC
		fb.BaseSeq = uint16(first.seq)
		ref := first.at / TCCReferenceUnit
		fb.ReferenceTime = uint32(ref) & 0xffffff
		fb.FeedbackCount = r.fbCount
		fb.Status = fb.Status[:0]
		fb.Deltas = fb.Deltas[:0]
		prev := ref * TCCReferenceUnit
		seq := first.seq
		// size counts the status symbols and deltas so far, as an upper
		// bound of the chunks they take: one chunk per 7 symbols.
		size := RTCPHeaderSize + 16
		for i < len(r.arrivals) {
			a := r.arrivals[i]
			gap := int(a.seq - seq)
			if len(fb.Status)+gap+1 > 0xffff {
				break
			}
			delta := a.at - prev
			if delta >= 0 {
				delta = (delta + TCCDeltaUnit/2) / TCCDeltaUnit
			} else {
				delta = (delta - TCCDeltaUnit/2) / TCCDeltaUnit
			}
			if delta < -1<<15 || delta >= 1<<15 {
				break
			}
			symbol, deltaSize := TCCSmallDelta, 1
			if delta < 0 || delta > 255 {
				symbol, deltaSize = TCCLargeDelta, 2
			}
			grow := deltaSize + 2*((len(fb.Status)+gap+1+6)/7-(len(fb.Status)+6)/7)
			if gap > 14 {
				// A run of missing packets takes a run length chunk or two.
				grow = deltaSize + 4 + 2*((gap+0x1ffe)/0x1fff)
			}
			if len(fb.Deltas) > 0 && size+grow > maxSize {
				break
			}
			size += grow
			for range gap {
				fb.Status = append(fb.Status, TCCNotReceived)
			}
			fb.Status = append(fb.Status, symbol)
			fb.Deltas = append(fb.Deltas, int16(delta))
			prev += time.Duration(delta) * TCCDeltaUnit
			seq = a.seq + 1
			i++
		}
		var err error
		if dst, err = fb.Append(dst); err != nil {
			return dst, err
		}
		r.fbCount++
		r.next = seq
	}
	r.started = true
	r.arrivals = r.arrivals[:0]
	return dst, nil
}

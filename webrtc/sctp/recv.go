package sctp

import (
	"encoding/binary"
	"fmt"
	"time"
)

// rchunk is a received DATA chunk waiting for the rest of its message or
// for its turn; data is a pooled copy. A run of fragments that arrived in
// order and were reassembled together is one rchunk covering TSNs tsn
// through last.
type rchunk struct {
	tsn   uint32
	last  uint32
	ppid  uint32
	ssn   uint16
	flags uint8
	data  []byte
}

// tsnRange is a run of TSNs received past the cumulative point.
type tsnRange struct{ start, end uint32 }

type receiver struct {
	// peerCum is the cumulative TSN this side acknowledges.
	peerCum uint32
	gaps    []tsnRange
	dups    []uint32
	// held is how many bytes of data wait in the streams' queues.
	held int

	sackNow     bool
	sackAt      time.Time
	unackedPkts int
	reasm       []byte
	// spare holds the buffers of a message being delivered.
	spare [][]byte
}

// maxGapSpan bounds how far past the cumulative point a TSN is taken,
// which keeps every gap block's offsets in 16 bits.
const maxGapSpan = 65535

const maxDups = 32

// chunkOverhead is what each chunk waiting in a stream's queue counts
// toward the receive window besides its data, for its buffer and entry, as
// usrsctp counts each chunk's control overhead. It keeps a peer sending
// tiny chunks from filling the window with many more entries than bytes.
const chunkOverhead = 256

func (a *Association) handleData(flags uint8, v []byte) error {
	if len(v) < 12 {
		return ErrMalformed
	}
	switch a.state {
	case StateEstablished, StateShutdownPending, StateShutdownSent:
	default:
		return nil
	}
	tsn := binary.BigEndian.Uint32(v)
	sid := binary.BigEndian.Uint16(v[4:])
	ssn := binary.BigEndian.Uint16(v[6:])
	ppid := binary.BigEndian.Uint32(v[8:])
	data := v[12:]
	if len(data) == 0 {
		a.abort(fmt.Errorf("%w: DATA without user data", ErrProtocol), causeNoUserData)
		return nil
	}
	if a.received(tsn) {
		if len(a.dups) < maxDups {
			a.dups = append(a.dups, tsn)
		}
		a.sackNow = true
		return nil
	}
	if tsn-a.peerCum > maxGapSpan {
		return nil
	}
	var st *stream
	if sid < a.inStreams {
		st = a.stream(sid)
	}
	whole := flags&(flagBegin|flagEnd) == flagBegin|flagEnd
	// A chunk delivered straight from the packet, or on a stream that is
	// not there, holds nothing. Any other is held and must fit the window:
	// past it a chunk is dropped unacknowledged and the peer sends it
	// again. The next TSN may go over, so a full window still moves while
	// a message larger than it is reassembled, but no further than one
	// such message: a peer that keeps sending chunks it cannot have
	// delivered, each the next TSN, holds no more than that.
	if st != nil && !(whole && (flags&flagUnordered != 0 || ssn == st.nextSSN)) {
		need := a.held + len(data) + chunkOverhead
		if need > a.cfg.ReceiveWindow && (tsn != a.peerCum+1 || need > a.cfg.ReceiveWindow+a.cfg.MaxMessageSize) {
			return nil
		}
	}
	a.mark(tsn)
	if st == nil {
		return nil
	}
	if whole {
		// A whole message: unordered ones and the next ordered one are
		// delivered straight from the packet.
		if flags&flagUnordered != 0 {
			a.deliver(sid, ppid, data)
			return nil
		}
		if ssn == st.nextSSN {
			st.nextSSN++
			st.headN, st.headBytes = 0, 0
			a.deliver(sid, ppid, data)
			if len(st.ordered) > 0 && !a.closed {
				a.deliverOrdered(sid, st)
			}
			return nil
		}
	}
	if len(data) > a.cfg.MaxMessageSize {
		a.abort(fmt.Errorf("%w: a chunk of %d bytes", ErrMessageTooLarge, len(data)), causeProtocolViolation)
		return nil
	}
	a.held += len(data)
	if a.assemble(sid, st, tsn, ssn, ppid, flags, data) {
		return nil
	}
	c := rchunk{tsn: tsn, last: tsn, ppid: ppid, ssn: ssn, flags: flags, data: a.pool.get(len(data))}
	copy(c.data, data)
	a.queue(sid, st, c)
	return nil
}

// assemble reassembles a message whose fragments arrive in order straight
// into the stream's buffer, one copy for the whole message rather than one
// pooled buffer per chunk. It reports whether it took the chunk; when one
// arrives out of turn, what it has gathered joins the stream's queue.
func (a *Association) assemble(sid uint16, st *stream, tsn uint32, ssn uint16, ppid uint32, flags uint8, data []byte) bool {
	unordered := flags&flagUnordered != 0
	if st.asmActive {
		if tsn != st.asm.last+1 || flags&flagBegin != 0 || unordered != (st.asm.flags&flagUnordered != 0) ||
			!unordered && ssn != st.asm.ssn {
			a.spill(sid, st)
			return false
		}
		if len(st.asm.data)+len(data) > a.cfg.MaxMessageSize {
			a.abort(fmt.Errorf("%w: over %d bytes", ErrMessageTooLarge, a.cfg.MaxMessageSize), causeProtocolViolation)
			return true
		}
		st.asm.data = a.pool.grow(st.asm.data, len(data))
		st.asm.data = append(st.asm.data, data...)
		st.asm.last = tsn
		if flags&flagEnd == 0 {
			return true
		}
		st.asmActive = false
		msg := st.asm.data
		st.asm.data = nil
		a.held -= len(msg)
		if !unordered {
			st.nextSSN++
			st.headN, st.headBytes = 0, 0
		}
		a.deliver(sid, st.asm.ppid, msg)
		a.pool.put(msg)
		if !unordered && len(st.ordered) > 0 && !a.closed {
			a.deliverOrdered(sid, st)
		}
		return true
	}
	if flags&(flagBegin|flagEnd) != flagBegin || !unordered && (ssn != st.nextSSN || len(st.ordered) > 0) {
		return false
	}
	st.asmActive = true
	st.asm = rchunk{tsn: tsn, last: tsn, ppid: ppid, ssn: ssn, flags: flags,
		data: append(a.pool.get(max(4*len(data), 4096))[:0], data...)}
	return true
}

// spill moves a partly assembled message into the stream's queue, where
// the general reassembly takes it from there.
func (a *Association) spill(sid uint16, st *stream) {
	if !st.asmActive {
		return
	}
	st.asmActive = false
	c := st.asm
	st.asm = rchunk{}
	a.queue(sid, st, c)
}

// queue files a chunk in its stream's ordered or unordered queue and
// delivers what that completes. Each queued chunk counts chunkOverhead
// toward held besides its data.
func (a *Association) queue(sid uint16, st *stream, c rchunk) {
	a.held += chunkOverhead
	if c.flags&flagUnordered != 0 {
		q := st.unordered
		// Chunks mostly arrive in order: try the end before searching.
		i := len(q)
		if i > 0 && tsnGT(q[i-1].tsn, c.tsn) {
			lo, hi := 0, i-1
			for lo < hi {
				m := int(uint(lo+hi) >> 1)
				if tsnGT(q[m].tsn, c.tsn) {
					hi = m
				} else {
					lo = m + 1
				}
			}
			i = lo
		}
		st.unordered = insertChunk(st.unordered, i, c)
		a.deliverUnordered(sid, st, i)
		return
	}
	key := c.ssn - st.nextSSN
	after := func(p *rchunk) bool {
		pk := p.ssn - st.nextSSN
		return pk > key || pk == key && tsnGT(p.tsn, c.tsn)
	}
	q := st.ordered
	i := len(q)
	if i > 0 && after(&q[i-1]) {
		lo, hi := 0, i-1
		for lo < hi {
			m := int(uint(lo+hi) >> 1)
			if after(&q[m]) {
				hi = m
			} else {
				lo = m + 1
			}
		}
		i = lo
	}
	if i < st.headN {
		st.headN, st.headBytes = 0, 0
	}
	st.ordered = insertChunk(st.ordered, i, c)
	a.deliverOrdered(sid, st)
}

func insertChunk(q []rchunk, i int, c rchunk) []rchunk {
	q = append(q, rchunk{})
	copy(q[i+1:], q[i:])
	q[i] = c
	return q
}

// received reports whether tsn has arrived before.
func (a *Association) received(tsn uint32) bool {
	if !tsnGT(tsn, a.peerCum) {
		return true
	}
	for _, g := range a.gaps {
		if tsnLT(tsn, g.start) {
			return false
		}
		if tsnLE(tsn, g.end) {
			return true
		}
	}
	return false
}

// mark records tsn as received, merging it into the cumulative point or
// the gap blocks.
func (a *Association) mark(tsn uint32) {
	if tsn == a.peerCum+1 {
		a.peerCum = tsn
		if len(a.gaps) > 0 && a.gaps[0].start == tsn+1 {
			a.peerCum = a.gaps[0].end
			n := copy(a.gaps, a.gaps[1:])
			a.gaps = a.gaps[:n]
		}
		return
	}
	for i := range a.gaps {
		g := &a.gaps[i]
		if tsnLT(tsn, g.start) {
			if tsn+1 == g.start {
				g.start = tsn
				if i > 0 && a.gaps[i-1].end+1 == tsn {
					a.gaps[i-1].end = g.end
					a.gaps = append(a.gaps[:i], a.gaps[i+1:]...)
				}
				return
			}
			if i > 0 && a.gaps[i-1].end+1 == tsn {
				a.gaps[i-1].end = tsn
				return
			}
			a.gaps = append(a.gaps, tsnRange{})
			copy(a.gaps[i+1:], a.gaps[i:])
			a.gaps[i] = tsnRange{tsn, tsn}
			return
		}
	}
	if n := len(a.gaps); n > 0 && a.gaps[n-1].end+1 == tsn {
		a.gaps[n-1].end = tsn
		return
	}
	a.gaps = append(a.gaps, tsnRange{tsn, tsn})
}

// deliver hands one whole message to its data channel or OnMessage.
func (a *Association) deliver(sid uint16, ppid uint32, data []byte) {
	st := a.streams[sid]
	if ppid == PPIDDCEP || st.ch != nil {
		a.channelMessage(sid, st, ppid, data)
		return
	}
	if a.cfg.OnMessage != nil {
		a.cfg.OnMessage(sid, ppid, data)
	}
}

// deliverOrdered delivers the messages at the head of st's ordered queue
// that are whole and next in sequence.
func (a *Association) deliverOrdered(sid uint16, st *stream) {
	for len(st.ordered) > 0 && !a.closed {
		q := st.ordered
		if q[0].ssn != st.nextSSN || q[0].flags&flagBegin == 0 {
			return
		}
		if st.headN == 0 {
			st.headN, st.headBytes = 1, len(q[0].data)
		}
		for q[st.headN-1].flags&flagEnd == 0 {
			if st.headN >= len(q) {
				return
			}
			p, c := &q[st.headN-1], &q[st.headN]
			if c.ssn != st.nextSSN || c.tsn != p.last+1 || c.flags&flagBegin != 0 {
				return
			}
			st.headN++
			st.headBytes += len(c.data)
			if st.headBytes > a.cfg.MaxMessageSize {
				a.abort(fmt.Errorf("%w: over %d bytes", ErrMessageTooLarge, a.cfg.MaxMessageSize), causeProtocolViolation)
				return
			}
		}
		n := st.headN
		st.headN, st.headBytes = 0, 0
		st.nextSSN++
		a.deliverRun(sid, &st.ordered, 0, n)
	}
}

// deliverUnordered delivers the unordered message the chunk at i belongs
// to, if all of it is there.
func (a *Association) deliverUnordered(sid uint16, st *stream, i int) {
	q := st.unordered
	s := i
	for q[s].flags&flagBegin == 0 {
		if s == 0 || q[s-1].last+1 != q[s].tsn || q[s-1].flags&flagEnd != 0 {
			return
		}
		s--
	}
	e := i
	size := 0
	for q[e].flags&flagEnd == 0 {
		if e+1 >= len(q) || q[e+1].tsn != q[e].last+1 || q[e+1].flags&flagBegin != 0 {
			return
		}
		size += len(q[e].data)
		if size > a.cfg.MaxMessageSize {
			a.abort(fmt.Errorf("%w: over %d bytes", ErrMessageTooLarge, a.cfg.MaxMessageSize), causeProtocolViolation)
			return
		}
		e++
	}
	a.deliverRun(sid, &st.unordered, s, e+1)
}

// deliverRun delivers the message made of (*q)[s:e] and takes it out of
// the queue.
func (a *Association) deliverRun(sid uint16, q *[]rchunk, s, e int) {
	run := (*q)[s:e]
	ppid := run[0].ppid
	var msg []byte
	size := 0
	for i := range run {
		size += len(run[i].data)
	}
	if len(run) == 1 {
		msg = run[0].data
	} else {
		if cap(a.reasm) < size {
			a.reasm = make([]byte, 0, size)
		}
		msg = a.reasm[:0]
		for i := range run {
			msg = append(msg, run[i].data...)
		}
	}
	a.held -= size
	// The chunks leave the queue before the callback, which may send but
	// cannot see the queue.
	bufs := a.takeRun(q, s, e)
	a.deliver(sid, ppid, msg)
	for _, b := range bufs {
		a.pool.put(b)
	}
	a.spare = bufs[:0]
}

// takeRun removes (*q)[s:e] and returns its buffers, for recycling once the
// message built from them is delivered.
func (a *Association) takeRun(q *[]rchunk, s, e int) [][]byte {
	bufs := a.spare[:0]
	a.held -= (e - s) * chunkOverhead
	for i := s; i < e; i++ {
		bufs = append(bufs, (*q)[i].data)
	}
	n := s + copy((*q)[s:], (*q)[e:])
	clear((*q)[n:])
	*q = (*q)[:n]
	return bufs
}

// afterData decides when to acknowledge the DATA a packet carried: at once
// when TSNs are missing or duplicated, or were and no longer are,
// otherwise with every second packet or after SACKDelay (RFC 9260 section
// 6.2).
func (a *Association) afterData(gapsBefore bool) {
	a.unackedPkts++
	if gapsBefore || len(a.gaps) > 0 || len(a.dups) > 0 || a.unackedPkts >= 2 {
		a.sackNow = true
		return
	}
	if a.sackAt.IsZero() {
		a.sackAt = a.now.Add(a.cfg.SACKDelay)
	}
}

// rwndLeft is the receiver window to advertise.
func (a *Association) rwndLeft() uint32 {
	return uint32(max(a.cfg.ReceiveWindow-a.held, 0))
}

func (a *Association) appendSack() {
	a.sackNow = false
	a.sackAt = time.Time{}
	a.unackedPkts = 0
	space := (a.mtu - headerSize - chunkHeaderSize - 12) / 4
	ng := min(len(a.gaps), space)
	nd := min(len(a.dups), space-ng)
	b := a.reserve(chunkSack, 0, 12+4*(ng+nd))
	binary.BigEndian.PutUint32(b, a.peerCum)
	binary.BigEndian.PutUint32(b[4:], a.rwndLeft())
	binary.BigEndian.PutUint16(b[8:], uint16(ng))
	binary.BigEndian.PutUint16(b[10:], uint16(nd))
	p := b[12:]
	for _, g := range a.gaps[:ng] {
		binary.BigEndian.PutUint16(p, uint16(g.start-a.peerCum))
		binary.BigEndian.PutUint16(p[2:], uint16(g.end-a.peerCum))
		p = p[4:]
	}
	for _, d := range a.dups[:nd] {
		binary.BigEndian.PutUint32(p, d)
		p = p[4:]
	}
	a.dups = a.dups[:0]
}

// handleForwardTSN skips what the peer gave up on (RFC 3758 section 3.6).
func (a *Association) handleForwardTSN(v []byte) error {
	if len(v) < 4 || len(v)%4 != 0 {
		return ErrMalformed
	}
	if a.state < StateEstablished {
		return nil
	}
	a.sackNow = true
	cum := binary.BigEndian.Uint32(v)
	if !tsnGT(cum, a.peerCum) {
		return nil
	}
	if cum-a.peerCum > 1<<30 {
		return fmt.Errorf("%w: FORWARD TSN %d far past %d", ErrProtocol, cum, a.peerCum)
	}
	a.peerCum = cum
	for sid, st := range a.streams {
		if st != nil && st.asmActive {
			a.spill(uint16(sid), st)
		}
	}
	n := 0
	for _, g := range a.gaps {
		if tsnLE(g.end, cum) {
			continue
		}
		if tsnLE(g.start, cum+1) {
			a.peerCum = g.end
			continue
		}
		a.gaps[n] = g
		n++
	}
	a.gaps = a.gaps[:n]
	cum = a.peerCum

	// Ordered streams named: deliver the whole messages up to the SSN
	// skipped to, drop the rest, and move on past it.
	for p := v[4:]; len(p) >= 4; p = p[4:] {
		sid := binary.BigEndian.Uint16(p)
		ssn := binary.BigEndian.Uint16(p[2:])
		if sid >= a.inStreams {
			continue
		}
		st := a.stream(sid)
		if !ssnLE(st.nextSSN, ssn) {
			continue
		}
		st.headN, st.headBytes = 0, 0
		for len(st.ordered) > 0 && !a.closed && ssnLE(st.ordered[0].ssn, ssn) {
			q := st.ordered
			head := q[0].ssn
			e := 1
			whole := q[0].flags&flagBegin != 0
			for whole && q[e-1].flags&flagEnd == 0 {
				if e >= len(q) || q[e].ssn != head || q[e].tsn != q[e-1].last+1 {
					whole = false
					break
				}
				e++
			}
			if whole {
				st.nextSSN = head + 1
				a.deliverRun(sid, &st.ordered, 0, e)
				continue
			}
			for e = 0; e < len(q) && q[e].ssn == head; e++ {
				a.held -= len(q[e].data)
			}
			for _, b := range a.takeRun(&st.ordered, 0, e) {
				a.pool.put(b)
			}
		}
		st.nextSSN = ssn + 1
		a.deliverOrdered(sid, st)
	}
	// Unordered fragments at or below the new point never complete.
	for sid, st := range a.streams {
		if st == nil || len(st.unordered) == 0 {
			continue
		}
		e := 0
		for e < len(st.unordered) && tsnLE(st.unordered[e].last, cum) {
			a.held -= len(st.unordered[e].data)
			e++
		}
		for _, b := range a.takeRun(&a.streams[sid].unordered, 0, e) {
			a.pool.put(b)
		}
	}
	return nil
}

package quic

import "github.com/lesismal/fib/go/bufferpool"

// Stream is one QUIC stream. Its sending side is written with Write and
// ended with Close or Reset; what arrives on its receiving side goes to the
// connection handler's OnStreamData, in order.
//
// Stream methods are safe to call from any goroutine.
type Stream struct {
	conn *Conn
	id   uint64
	// Context is for the application, which the stream never touches.
	Context any

	// Sending side. sendMax is the peer's limit on its offsets.
	hasSend   bool
	send      sendBuffer
	sendMax   uint64
	finQueued bool
	finSent   bool
	finAcked  bool
	// Resetting the sending side: resetPending while RESET_STREAM waits to
	// go out, reset from when it is decided, resetAcked once the peer has
	// it.
	reset        bool
	resetPending bool
	resetAcked   bool
	resetCode    uint64
	// finalSize is the sending side's final size once it is known.
	finalSize uint64

	// Receiving side. recvMax is the limit given to the peer, recvHigh the
	// highest offset it has reached, and recvFin its final size if known.
	hasRecv        bool
	recv           recvBuffer
	recvMax        uint64
	recvHigh       uint64
	recvFin        int64
	recvDone       bool
	peerReset      bool
	maxDataPending bool
	stopPending    bool
	stopSent       bool
	stopCode       uint64
	// recvRead is how far the stream counts as read for flow control,
	// which recv.offset alone is not once arriving data is discarded.
	recvRead uint64

	queued bool
	done   bool
	// released is set once the application is done with the stream, and
	// its handler calls stop (see WriteFinal). Only the goroutine making
	// them uses it.
	released bool
}

// ID is the stream's ID.
func (s *Stream) ID() uint64 { return s.id }

// Conn is the connection the stream belongs to.
func (s *Stream) Conn() *Conn { return s.conn }

func isBidi(id uint64) bool           { return id&2 == 0 }
func isClientStream(id uint64) bool   { return id&1 == 0 }
func (s *Stream) local() bool         { return isClientStream(s.id) == s.conn.isClient }
func (s *Stream) Bidirectional() bool { return isBidi(s.id) }

// Write queues p to be sent, and with fin set ends the sending side after
// it. The stream keeps a copy, so p may be reused at once. Data is sent as
// flow and congestion control allow, which Write does not wait for.
func (s *Stream) Write(p []byte, fin bool) error { return s.write(p, fin, false) }

// WriteOwned is Write for p that is a buffer from package bufferpool, which
// the stream takes over, whether or not the write succeeds, rather than copy:
// the caller must not use p afterwards. A stream with nothing waiting to be
// sent keeps p as it is until it has been acknowledged.
func (s *Stream) WriteOwned(p []byte, fin bool) error { return s.write(p, fin, true) }

// WriteFinal is WriteOwned for the last of what the stream sends, which ends
// its sending side, and it tells the connection that the application is done
// with the stream: the handler is called for it no more, and its Context is
// dropped, so that what the Context refers to need not live as long as the
// stream, which is until the peer has acknowledged all it was sent. That
// takes effect in order with the handler's calls. With expected set it
// is the write an ExpectWrite announced, which it ends as WriteDone would:
// what the write adds and what was held for it then leave together, rather
// than the write first holding them back for a write that is its own.
func (s *Stream) WriteFinal(p []byte, expected bool) error {
	c := s.conn
	c.mu.Lock()
	err := s.writeLocked(p, true, true)
	if err == nil {
		c.wantFlush = true
	}
	c.events = append(c.events, event{kind: evRelease, stream: s})
	if expected && c.awaited > 0 {
		c.awaited--
	}
	c.mu.Unlock()
	c.dispatch()
	return err
}

func (s *Stream) write(p []byte, fin, owned bool) error {
	c := s.conn
	c.mu.Lock()
	err := s.writeLocked(p, fin, owned)
	if err == nil {
		c.wantFlush = true
	}
	c.mu.Unlock()
	c.dispatch()
	return err
}

func (s *Stream) writeLocked(p []byte, fin, owned bool) error {
	switch {
	case s.conn.closed, !s.hasSend || s.finQueued || s.reset:
		if owned {
			bufferpool.Put(p)
		}
		return ErrClosed
	}
	if owned {
		s.send.adopt(p)
	} else {
		s.send.write(p)
	}
	if fin {
		s.finQueued = true
		s.finalSize = s.send.end()
	}
	s.conn.queueStream(s)
	return nil
}

// Close ends the sending side once what has been written is sent.
func (s *Stream) Close() error { return s.Write(nil, true) }

// Reset abandons the sending side, telling the peer with code.
func (s *Stream) Reset(code uint64) {
	c := s.conn
	c.mu.Lock()
	if !c.closed {
		s.resetLocked(code)
		c.wantFlush = true
	}
	c.mu.Unlock()
	c.dispatch()
}

func (s *Stream) resetLocked(code uint64) {
	if !s.hasSend || s.reset || s.finAcked {
		return
	}
	s.reset = true
	s.resetPending = true
	s.resetCode = code
	// The final size is what the peer may have seen, however little of it
	// arrived.
	s.finalSize = s.send.next
	s.send = sendBuffer{base: s.send.next, next: s.send.next}
	s.conn.queueStream(s)
}

// StopSending asks the peer to stop sending, with code, and discards what
// arrives from then on.
func (s *Stream) StopSending(code uint64) {
	c := s.conn
	c.mu.Lock()
	if !c.closed {
		s.stopSendingLocked(code)
		c.wantFlush = true
	}
	c.mu.Unlock()
	c.dispatch()
}

func (s *Stream) stopSendingLocked(code uint64) {
	if !s.hasRecv || s.recvDone || s.stopSent {
		return
	}
	s.stopSent = true
	s.stopPending = true
	s.stopCode = code
	s.recv = recvBuffer{offset: s.recv.offset}
	s.conn.queueStream(s)
}

// sendCredit is how many new bytes the stream may send now.
func (s *Stream) sendCredit() uint64 {
	c := s.conn
	if s.send.next >= s.sendMax || c.sentData >= c.peerMaxData {
		return 0
	}
	return min(s.sendMax-s.send.next, c.peerMaxData-c.sentData)
}

// sendable reports whether the stream has a frame to send now.
func (s *Stream) sendable() bool {
	if s.stopPending || s.maxDataPending || s.resetPending {
		return true
	}
	if s.reset || !s.hasSend {
		return false
	}
	if s.send.hasLost() {
		return true
	}
	if s.send.next < s.send.end() {
		return s.sendCredit() > 0
	}
	return s.finQueued && !s.finSent
}

// appendFrames adds the stream's frames to a packet payload, up to max.
func (s *Stream) appendFrames(b []byte, max int, p *sentPacket) []byte {
	c := s.conn
	if s.stopPending {
		if f := appendStopSending(nil, s.id, s.stopCode); len(b)+len(f) <= max {
			b = append(b, f...)
			s.stopPending = false
			p.add(sentFrame{kind: sfStopSending, stream: s})
		}
	}
	if s.maxDataPending {
		if f := appendMaxStreamData(nil, s.id, s.recvMax); len(b)+len(f) <= max {
			b = append(b, f...)
			s.maxDataPending = false
			p.add(sentFrame{kind: sfMaxStreamData, stream: s})
		}
	}
	if s.resetPending {
		if f := appendResetStream(nil, s.id, s.resetCode, s.finalSize); len(b)+len(f) <= max {
			b = append(b, f...)
			s.resetPending = false
			p.add(sentFrame{kind: sfResetStream, stream: s})
		}
	}
	if s.reset || !s.hasSend {
		return b
	}
	idLen := VarintLen(s.id)
	for s.send.hasLost() {
		off := s.send.lost[0].off
		avail := max - len(b) - 1 - idLen - VarintLen(off) - 2
		if avail <= 0 {
			return b
		}
		off, data := s.send.popLost(uint64(avail))
		b = s.appendStreamFrame(b, off, data, p)
	}
	for s.send.next < s.send.end() {
		credit := s.sendCredit()
		avail := max - len(b) - 1 - idLen - VarintLen(s.send.next) - 2
		if credit == 0 || avail <= 0 {
			return b
		}
		off, data := s.send.popNew(min(uint64(avail), credit))
		c.sentData += uint64(len(data))
		b = s.appendStreamFrame(b, off, data, p)
	}
	if s.finQueued && !s.finSent {
		if avail := max - len(b) - 1 - idLen - VarintLen(s.send.next) - 1; avail >= 0 {
			b = s.appendStreamFrame(b, s.send.next, nil, p)
		}
	}
	return b
}

func (s *Stream) appendStreamFrame(b []byte, off uint64, data []byte, p *sentPacket) []byte {
	end := off + uint64(len(data))
	fin := s.finQueued && !s.finSent && end == s.finalSize
	typ := byte(frameStream | 0x02)
	if off > 0 {
		typ |= 0x04
	}
	if fin {
		typ |= 0x01
		s.finSent = true
	}
	b = append(b, typ)
	b = AppendVarint(b, s.id)
	if off > 0 {
		b = AppendVarint(b, off)
	}
	b = AppendVarint(b, uint64(len(data)))
	b = append(b, data...)
	p.add(sentFrame{kind: sfStream, stream: s, off: off, n: uint64(len(data)), fin: fin})
	return b
}

func (s *Stream) onDataAcked(off, n uint64, fin bool) {
	if s.reset {
		return
	}
	s.send.onAck(off, n)
	if fin {
		s.finAcked = true
	}
	s.conn.maybeFinish(s)
}

func (s *Stream) onDataLost(off, n uint64, fin bool) {
	if s.reset || s.done {
		return
	}
	s.send.onLost(off, n)
	if fin && !s.finAcked {
		s.finSent = false
	}
	s.conn.queueStream(s)
}

func (s *Stream) onResetAcked() {
	s.resetAcked = true
	s.conn.maybeFinish(s)
}

func (s *Stream) onResetLost() {
	if !s.resetAcked && !s.done {
		s.resetPending = true
		s.conn.queueStream(s)
	}
}

func (s *Stream) onMaxDataLost() {
	if !s.recvDone && !s.done && s.recvFin < 0 {
		s.maxDataPending = true
		s.conn.queueStream(s)
	}
}

func (s *Stream) onStopSendingLost() {
	if !s.recvDone && !s.done {
		s.stopPending = true
		s.conn.queueStream(s)
	}
}

// sendDone reports whether the sending side needs nothing more.
func (s *Stream) sendDone() bool {
	if !s.hasSend {
		return true
	}
	if s.reset {
		return s.resetAcked
	}
	return s.finAcked && s.send.allAcked()
}

// onStreamFrame takes in a STREAM frame.
func (s *Stream) onStreamFrame(off uint64, data []byte, fin bool) error {
	c := s.conn
	end := off + uint64(len(data))
	if end > maxVarint {
		return transportErr(errFrameEncoding, "stream offset too large")
	}
	if s.recvFin >= 0 && (end > uint64(s.recvFin) || fin && end != uint64(s.recvFin)) {
		return transportErr(errFinalSize, "data beyond the final size")
	}
	if fin {
		if end < s.recvHigh {
			return transportErr(errFinalSize, "final size below data received")
		}
		s.recvFin = int64(end)
	}
	if end > s.recvMax {
		return transportErr(errFlowControl, "stream flow control exceeded")
	}
	if end > s.recvHigh {
		c.recvData += end - s.recvHigh
		s.recvHigh = end
		if c.recvData > c.recvMaxData {
			return transportErr(errFlowControl, "connection flow control exceeded")
		}
	}
	if s.recvDone || s.peerReset || s.stopSent {
		// Nobody reads it, so it counts as read at once.
		c.consumed(s, end)
		if s.stopSent && s.recvFin >= 0 && !s.recvDone {
			s.recvDone = true
			c.maybeFinish(s)
		}
		return nil
	}
	if chunk := s.recv.take(off, data); chunk != nil {
		s.deliver(chunk, false)
	}
	for {
		chunk := s.recv.pop()
		if chunk == nil {
			break
		}
		s.deliver(chunk, false)
	}
	if s.recvFin >= 0 && s.recv.offset == uint64(s.recvFin) && !s.recvDone {
		s.recvDone = true
		s.deliver(nil, true)
		c.maybeFinish(s)
	}
	return nil
}

// deliver hands data to the application. The window it frees is given back
// to the peer as it is delivered, since the application takes the data
// whole and bounds what it keeps itself.
func (s *Stream) deliver(data []byte, fin bool) {
	c := s.conn
	c.consumed(s, s.recv.offset)
	c.events = append(c.events, event{kind: evStreamData, stream: s, data: data, fin: fin})
}

// consumed records that the stream has been read up to offset, and gives
// the peer more room when half of the window has been used.
func (c *Conn) consumed(s *Stream, offset uint64) {
	if offset > s.recvRead {
		c.recvConsumed += offset - s.recvRead
		s.recvRead = offset
	}
	if s.recvFin < 0 && !s.recvDone && s.recvMax-offset < c.config.StreamReceiveWindow/2 {
		s.recvMax = offset + c.config.StreamReceiveWindow
		s.maxDataPending = true
		c.queueStream(s)
	}
	if c.recvMaxData-c.recvConsumed < c.config.ConnReceiveWindow/2 {
		c.recvMaxData = c.recvConsumed + c.config.ConnReceiveWindow
		c.maxDataPending = true
	}
}

func (s *Stream) onResetStream(code, finalSize uint64) error {
	c := s.conn
	if s.recvFin >= 0 && finalSize != uint64(s.recvFin) || finalSize < s.recvHigh {
		return transportErr(errFinalSize, "reset with a different final size")
	}
	if finalSize > s.recvMax {
		return transportErr(errFlowControl, "stream flow control exceeded")
	}
	if finalSize > s.recvHigh {
		c.recvData += finalSize - s.recvHigh
		s.recvHigh = finalSize
		if c.recvData > c.recvMaxData {
			return transportErr(errFlowControl, "connection flow control exceeded")
		}
	}
	s.recvFin = int64(finalSize)
	c.consumed(s, finalSize)
	if s.recvDone || s.peerReset {
		return nil
	}
	s.peerReset = true
	s.maxDataPending = false
	s.recv = recvBuffer{offset: s.recv.offset}
	if !s.stopSent {
		c.events = append(c.events, event{kind: evStreamReset, stream: s, code: code})
	}
	c.maybeFinish(s)
	return nil
}

func (s *Stream) onStopSending(code uint64) {
	c := s.conn
	if s.reset || s.finAcked {
		return
	}
	s.resetLocked(code)
	c.events = append(c.events, event{kind: evStopSending, stream: s, code: code})
}

// recvFinished reports whether the receiving side needs nothing more.
func (s *Stream) recvFinished() bool { return !s.hasRecv || s.recvDone || s.peerReset }

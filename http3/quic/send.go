package quic

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/lesismal/fib/bufferpool"
)

// packetPlan is a packet whose frames are chosen but which is not yet
// sealed, so that the last packet of a datagram can still be padded.
type packetPlan struct {
	space   int
	pn      uint64
	pnLen   int
	payload []byte
	sp      *sentPacket
	// padded says the payload has PADDING, which makes the packet count as
	// in flight even without ack-eliciting frames.
	padded bool
	// early says a client sends it as 0-RTT data.
	early bool
}

// headerLen is the length of a packet header in a space.
func (c *Conn) headerLen(space, pnLen int, early bool) int {
	if space == spaceApp && early {
		return 1 + 4 + 1 + len(c.dcid) + 1 + len(c.scid) + 2 + pnLen
	}
	if space == spaceApp {
		return 1 + len(c.dcid) + pnLen
	}
	n := 1 + 4 + 1 + len(c.dcid) + 1 + len(c.scid) + 2 + pnLen
	if space == spaceInitial {
		n += VarintLen(uint64(len(c.token))) + len(c.token)
	}
	return n
}

func (c *Conn) packetSize(p *packetPlan) int {
	return c.headerLen(p.space, p.pnLen, p.early) + len(p.payload) + aeadOverhead
}

// sealPlan appends the sealed packet to out.
func (c *Conn) sealPlan(out []byte, p *packetPlan) []byte {
	start := len(out)
	s := &c.spaces[p.space]
	tx := s.tx
	if p.early {
		tx = c.earlyTx
	}
	if p.space == spaceApp && !p.early {
		first := byte(0x40) | byte(p.pnLen-1)
		if c.txGen&1 == 1 {
			first |= 0x04
		}
		out = append(out, first)
		out = append(out, c.dcid...)
	} else {
		typ := byte(packetInitial)
		if p.space == spaceHandshake {
			typ = packetHandshake
		}
		if p.early {
			typ = packet0RTT
		}
		out = append(out, 0xc0|typ<<4|byte(p.pnLen-1))
		out = binary.BigEndian.AppendUint32(out, Version1)
		out = append(out, byte(len(c.dcid)))
		out = append(out, c.dcid...)
		out = append(out, byte(len(c.scid)))
		out = append(out, c.scid...)
		if p.space == spaceInitial {
			out = AppendVarint(out, uint64(len(c.token)))
			out = append(out, c.token...)
		}
		out = appendVarint2(out, uint64(p.pnLen+len(p.payload)+aeadOverhead))
	}
	pnOffset := len(out) - start
	for i := p.pnLen - 1; i >= 0; i-- {
		out = append(out, byte(p.pn>>(8*i)))
	}
	out = append(out, p.payload...)
	if cap(out)-len(out) < aeadOverhead {
		grown := make([]byte, len(out), len(out)+aeadOverhead+maxDatagram)
		copy(grown, out)
		out = grown
	}
	sealed := tx.seal(out[start:], pnOffset, p.pnLen, p.pn)
	return out[:start+len(sealed)]
}

// sendBufferSize is the memory a round of sending builds its datagrams in:
// room for a burst of full packets, and one of the buffer pool's classes.
const sendBufferSize = 32 << 10

// maxRoundDatagrams bounds the datagrams a round of sending sends.
const maxRoundDatagrams = 64

// sendRoundLocked builds what there is to send and the limits allow, as much
// of it as a round's buffer holds, sets the timer for what comes next, and
// sends the datagrams with the lock released. It leaves wantFlush set when it
// stopped for want of room rather than of something to send. A round that
// stops for pacing, or finds the socket full, leaves sendAt set instead, for
// the timer to go on with. Callers are the sending goroutine (see flush) and
// hold c.mu, which they hold again when it returns.
func (c *Conn) sendRoundLocked() {
	if c.blocked != nil && !c.sendBlockedLocked() {
		return
	}
	// One buffer for the round: datagrams from its start, and at its end a
	// slot for each space to build its packet's payload in, since the
	// packets of a datagram are all chosen before any of them is sealed.
	mem := bufferpool.Get(sendBufferSize)
	scratch := mem[sendBufferSize-numSpaces*maxDatagramLimit:]
	buf := mem[: 0 : sendBufferSize-numSpaces*maxDatagramLimit]
	lens := &c.roundLens
	n := 0
	now := c.now()
	c.maybeUpdateKeys(now)
	c.paced = false
	c.sendAt = time.Time{}
	for !c.closed {
		if n == len(lens) || cap(buf)-len(buf) < c.maxDatagram {
			c.wantFlush = true
			break
		}
		if size := c.mtuProbeDue(now); size > 0 && cap(buf)-len(buf) >= size {
			// A probe ends its round, the last datagram of the batch, so
			// that a socket refusing it for its size, as one that knows the
			// interface's MTU does, refuses nothing after it. What else
			// there is goes in the next round.
			start := len(buf)
			buf = c.appendMTUProbe(buf, scratch, size, now)
			c.bytesSent += uint64(len(buf) - start)
			lens[n] = uint16(len(buf) - start)
			n++
			c.wantFlush = true
			break
		}
		start := len(buf)
		buf = c.appendDatagram(buf, scratch, now)
		if len(buf) == start {
			break
		}
		c.bytesSent += uint64(len(buf) - start)
		if !c.pathValidated {
			c.pathSent += uint64(len(buf) - start)
		}
		lens[n] = uint16(len(buf) - start)
		n++
	}
	c.flushDeadline = time.Time{}
	if c.paced && !c.wantFlush && c.sendableLocked() {
		c.sendAt = c.paceNext(now)
	}
	if !c.closed {
		c.setLossDetectionTimer(now)
		c.armTimerLocked(now)
	}
	if n > 0 {
		// The path is read with the lock held, since a datagram handled
		// meanwhile may move the connection to another.
		pc := c.pc
		c.mu.Unlock()
		sent, err := sendDatagrams(pc, buf, lens[:n])
		c.mu.Lock()
		if sent < n && wouldBlock(err) && !c.closed {
			c.keepBlockedLocked(mem, sent, n)
			return
		}
	}
	bufferpool.Put(mem)
}

// keepBlockedLocked keeps the datagrams of a round from the sent-th of n on,
// which the socket had no room for, to send again once the timer says to.
// The packets in them were counted as sent when they were built, and are
// sent late rather than lost. Callers are the sending goroutine and hold
// c.mu.
func (c *Conn) keepBlockedLocked(mem []byte, sent, n int) {
	off := c.blockedOff
	for _, l := range c.roundLens[:sent] {
		off += int(l)
	}
	copy(c.roundLens[:], c.roundLens[sent:n])
	c.blocked, c.blockedOff, c.blockedN = mem, off, n-sent
	// More waiting to be built waits too: the timer sends it once the
	// socket has taken what it refused.
	c.wantFlush = false
	now := c.now()
	c.sendAt = now.Add(blockedRetry)
	c.armTimerLocked(now)
}

// sendBlockedLocked sends again what a round left for a full socket, and
// reports whether the socket took all of it. Callers are the sending
// goroutine and hold c.mu.
func (c *Conn) sendBlockedLocked() bool {
	mem, n := c.blocked, c.blockedN
	c.blocked = nil
	if c.closed {
		bufferpool.Put(mem)
		return false
	}
	pc := c.pc
	c.mu.Unlock()
	sent, err := sendDatagrams(pc, mem[c.blockedOff:], c.roundLens[:n])
	c.mu.Lock()
	if sent < n && wouldBlock(err) && !c.closed {
		c.keepBlockedLocked(mem, sent, n)
		return false
	}
	c.blockedOff, c.blockedN = 0, 0
	bufferpool.Put(mem)
	return !c.closed
}

// roundDatagrams are the slices a batched send lists a round's datagrams in:
// an array on the stack would be moved to the heap for every batch, since a
// BatchSender is an interface, and one kept by each connection would be as
// large as the rest of an idle connection.
var roundDatagrams = sync.Pool{New: func() any { return new([maxRoundDatagrams][]byte) }}

// sendDatagrams sends the datagrams laid one after another in buf through
// pc, with one call when the path takes several at once, and reports how
// many went: those before the one that failed, or all of them when the path
// cannot tell, since they are then as good as lost. Callers are the sending
// goroutine, without c.mu.
func sendDatagrams(pc PacketConn, buf []byte, lens []uint16) (int, error) {
	if len(lens) > 1 {
		ds, counted := pc.(DatagramsSender)
		b, batched := pc.(BatchSender)
		if counted || batched {
			datagrams := roundDatagrams.Get().(*[maxRoundDatagrams][]byte)
			for i, n := range lens {
				datagrams[i], buf = buf[:n], buf[n:]
			}
			sent, err := len(lens), error(nil)
			if counted {
				sent, err = ds.SendDatagrams(datagrams[:len(lens)])
			} else {
				err = b.SendBatch(datagrams[:len(lens)])
			}
			clear(datagrams[:len(lens)])
			roundDatagrams.Put(datagrams)
			return sent, err
		}
	}
	for i, n := range lens {
		if err := pc.Send(buf[:n]); err != nil && wouldBlock(err) {
			return i, err
		}
		buf = buf[n:]
	}
	return len(lens), nil
}

// appendDatagram appends one datagram to out, with a packet from each space
// that has something to send, and returns out as it was when none has. out
// has room for c.maxDatagram more bytes, and scratch for numSpaces payloads of
// maxDatagramLimit bytes.
func (c *Conn) appendDatagram(out, scratch []byte, now time.Time) []byte {
	limit := c.maxDatagram
	if !c.isClient && !c.addressValidated {
		// Until the client's address is proven, a server sends at most
		// three times what it received (RFC 9000 section 8.1).
		if c.bytesSent+uint64(limit) > 3*c.bytesRecv {
			return out
		}
	}
	if !c.pathValidated {
		// Nor to a client's new address, until it is proven (RFC 9000
		// section 9.3). The datagrams are cut short to fit rather than held
		// back, since the PATH_CHALLENGE that proves it has to go.
		budget := pathBudget(c.pathRecv, c.pathSent)
		if budget < minPathDatagram {
			return out
		}
		limit = min(limit, budget)
	}
	congestionOK := c.cc.canSend()
	if congestionOK && !c.paceAllows(now) {
		congestionOK = false
		c.paced = true
	}
	var plans [numSpaces]packetPlan
	n, size := 0, 0
	for space := range c.spaces {
		s := &c.spaces[space]
		// A client with early keys and no 1-RTT ones yet sends 0-RTT packets
		// in the application space.
		early := space == spaceApp && s.tx == nil && c.earlyTx != nil
		if s.tx == nil && !early || s.discarded {
			continue
		}
		slot := scratch[space*maxDatagramLimit : space*maxDatagramLimit : (space+1)*maxDatagramLimit]
		plan, ok := c.buildPacket(space, limit-size, congestionOK || s.probes > 0, now, slot, early)
		if !ok {
			continue
		}
		plans[n] = plan
		n++
		size += c.packetSize(&plan)
	}
	if n == 0 {
		return out
	}
	pad := false
	for i := 0; i < n; i++ {
		if plans[i].space == spaceInitial && (c.isClient || plans[i].sp.ackEliciting) {
			// Datagrams with Initial packets are padded out so that the
			// server's replies cannot amplify much (RFC 9000 section 14.1).
			pad = true
		}
	}
	if pad && size < minInitialDatagram {
		last := &plans[n-1]
		last.payload = append(last.payload, make([]byte, minInitialDatagram-size)...)
		last.padded = true
		size = minInitialDatagram
	}
	for i := 0; i < n; i++ {
		out = c.sealPlan(out, &plans[i])
	}
	for i := 0; i < n; i++ {
		p := &plans[i]
		p.sp.time = now
		p.sp.size = c.packetSize(p)
		p.sp.inFlight = p.sp.ackEliciting || p.padded
		if p.sp.inFlight {
			c.paceBudget -= p.sp.size
		}
		c.onPacketSent(p.space, p.sp)
	}
	return out
}

// buildPacket chooses the frames for a packet in space of at most room
// bytes, building its payload in scratch, which has room for all of it.
// mayElicit says whether congestion control allows ack-eliciting frames;
// without them, only an acknowledgement that is due goes out.
func (c *Conn) buildPacket(space, room int, mayElicit bool, now time.Time, scratch []byte, early bool) (packetPlan, bool) {
	s := &c.spaces[space]
	pn := s.nextPN
	pl := pnLen(pn, s.largestAcked)
	max := room - c.headerLen(space, pl, early) - aeadOverhead
	if max < 16 {
		return packetPlan{}, false
	}
	ackDue := s.ackPending && (s.ackNow || !s.ackDeadline.IsZero() && !now.Before(s.ackDeadline))
	if early {
		// There is nothing to acknowledge before the server has been heard
		// from, and only streams are sent.
		ackDue = false
	}
	if !ackDue && s.probes == 0 && (!mayElicit || !c.framesWaiting(space)) {
		// An acknowledgement that is not due goes out only with something
		// else, and there is nothing else: building it would be for nothing.
		return packetPlan{}, false
	}
	sp := c.newSentPacket(pn)
	sp.early = early
	payload := scratch[:0]
	withAck := false
	// A handshake packet carries the acknowledgement again even when it has
	// already been sent: the datagram that had it may be the one that was
	// lost, and until the peer hears it has no round-trip time to go by, so
	// it sends its flight again only as its probe timeout doubles from the
	// one second it starts at. There are few such packets and most are padded
	// anyway, so it costs nothing.
	if !early && (s.ackPending || space != spaceApp) && len(s.recv) > 0 {
		if ack := c.appendAck(payload, s, now); len(ack) <= max/2 {
			payload = ack
			withAck = true
			sp.ackOf = s.recv.largest() + 1
		}
	}
	if mayElicit {
		if early {
			payload = c.appendStreams(payload, max, sp)
		} else {
			payload = c.appendFrames(payload, space, max, sp)
		}
	}
	if s.probes > 0 && !sp.ackEliciting && len(payload) < max {
		payload = append(payload, framePing)
		sp.ackEliciting = true
	}
	if !sp.ackEliciting && !(withAck && ackDue) {
		c.releaseSent(sp)
		return packetPlan{}, false
	}
	if withAck {
		s.ackPending = false
		s.ackNow = false
		s.unackedEliciting = 0
		s.ackDeadline = time.Time{}
	}
	if sp.ackEliciting && s.probes > 0 {
		s.probes--
	}
	// Header protection samples four bytes past the packet number's start.
	padded := false
	if short := 4 - pl - len(payload); short > 0 {
		payload = append(payload, make([]byte, short)...)
		padded = true
	}
	s.nextPN++
	return packetPlan{space: space, pn: pn, pnLen: pl, payload: payload, sp: sp, padded: padded, early: early}, true
}

// framesWaiting reports whether a space may have ack-eliciting frames to
// send, which appendFrames finds out for sure: false means it has none.
func (c *Conn) framesWaiting(space int) bool {
	cs := &c.spaces[space].cryptoSend
	if cs.next < cs.end() || cs.hasLost() {
		return true
	}
	return space == spaceApp && (len(c.sendQueue) > 0 || c.handshakeDonePending || c.challengePending || c.maxDataPending ||
		c.maxStreamsBidiPending || c.maxStreamsUniPending || len(c.pathResponses) > 0 ||
		len(c.retireCIDs) > 0 || c.cidsPending || c.blockedPending() || c.pingPending)
}

// appendFrames adds a space's ack-eliciting frames, up to max bytes of
// payload.
func (c *Conn) appendFrames(b []byte, space, max int, sp *sentPacket) []byte {
	if space == spaceApp {
		b = c.appendControlFrames(b, max, sp)
	}
	b = c.appendCrypto(b, space, max, sp)
	if space == spaceApp {
		b = c.appendStreams(b, max, sp)
	}
	return b
}

func (c *Conn) appendControlFrames(b []byte, max int, sp *sentPacket) []byte {
	fits := func(n int) bool { return len(b)+n <= max }
	if c.handshakeDonePending && fits(1) {
		b = append(b, frameHandshakeDone)
		c.handshakeDonePending = false
		sp.add(sentFrame{kind: sfHandshakeDone})
	}
	if c.maxDataPending && fits(1+VarintLen(c.recvMaxData)) {
		b = append(b, frameMaxData)
		b = AppendVarint(b, c.recvMaxData)
		c.maxDataPending = false
		sp.add(sentFrame{kind: sfMaxData})
	}
	if c.maxStreamsBidiPending && fits(1+VarintLen(c.maxBidi)) {
		b = append(b, frameMaxStreamsBidi)
		b = AppendVarint(b, c.maxBidi)
		c.maxStreamsBidiPending = false
		sp.add(sentFrame{kind: sfMaxStreamsBidi})
	}
	if c.maxStreamsUniPending && fits(1+VarintLen(c.maxUni)) {
		b = append(b, frameMaxStreamsUni)
		b = AppendVarint(b, c.maxUni)
		c.maxStreamsUniPending = false
		sp.add(sentFrame{kind: sfMaxStreamsUni})
	}
	if c.challengePending && fits(9) {
		b = append(b, framePathChallenge)
		b = append(b, c.challenge[:]...)
		c.challengePending = false
		sp.add(sentFrame{kind: sfPathChallenge})
	}
	for len(c.pathResponses) > 0 && fits(9) {
		b = append(b, framePathResponse)
		b = append(b, c.pathResponses[0][:]...)
		c.pathResponses = c.pathResponses[1:]
		sp.ackEliciting = true
	}
	b = c.appendNewCIDs(b, max, sp)
	b = c.appendBlocked(b, max, sp)
	for len(c.retireCIDs) > 0 && fits(1+VarintLen(c.retireCIDs[0])) {
		seq := c.retireCIDs[0]
		c.retireCIDs = c.retireCIDs[1:]
		b = append(b, frameRetireConnectionID)
		b = AppendVarint(b, seq)
		sp.add(sentFrame{kind: sfRetireCID, off: seq})
	}
	if c.pingPending && fits(1) {
		b = append(b, framePing)
		c.pingPending = false
		sp.ackEliciting = true
	}
	return b
}

func (c *Conn) appendCrypto(b []byte, space, max int, sp *sentPacket) []byte {
	cs := &c.spaces[space].cryptoSend
	for {
		lost := cs.hasLost()
		if !lost && cs.next >= cs.end() {
			return b
		}
		off := cs.next
		if lost {
			off = max64(cs.lost[0].off, cs.base)
		}
		avail := max - len(b) - 1 - VarintLen(off) - 2
		if avail <= 0 {
			return b
		}
		var data []byte
		if lost {
			off, data = cs.popLost(uint64(avail))
		} else {
			off, data = cs.popNew(uint64(avail))
		}
		b = append(b, frameCrypto)
		b = AppendVarint(b, off)
		b = AppendVarint(b, uint64(len(data)))
		b = append(b, data...)
		sp.add(sentFrame{kind: sfCrypto, off: off, n: uint64(len(data))})
	}
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// appendStreams adds stream frames, taking the queued streams in turn so
// that one busy stream does not starve the rest.
func (c *Conn) appendStreams(b []byte, max int, sp *sentPacket) []byte {
	i := 0
	for i < len(c.sendQueue) && max-len(b) > 16 {
		s := c.sendQueue[i]
		if !s.done {
			b = s.appendFrames(b, max, sp)
		}
		if s.done || !s.sendable() {
			s.queued = false
			c.sendQueue = append(c.sendQueue[:i], c.sendQueue[i+1:]...)
			continue
		}
		i++
	}
	if len(c.sendQueue) > 1 && i > 0 {
		// The stream that filled the packet goes to the back of the line.
		first := c.sendQueue[0]
		copy(c.sendQueue, c.sendQueue[1:])
		c.sendQueue[len(c.sendQueue)-1] = first
	}
	return b
}

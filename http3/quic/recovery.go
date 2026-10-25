package quic

import (
	"time"
)

// Loss detection follows RFC 9002; congestion control is in congestion.go.

const (
	packetThreshold  = 3
	timerGranularity = time.Millisecond
	initialRTT       = 333 * time.Millisecond
	// minCongestionWindow and initialCongestionWindow are in bytes.
	minCongestionWindow     = 2 * maxDatagram
	initialCongestionWindow = 10 * maxDatagram
	// maxProbes is how many probe packets a PTO sends.
	maxProbes = 2
)

// What a sent packet carried that loss or acknowledgement matters to.
const (
	sfCrypto = iota
	sfStream
	sfMaxData
	sfMaxStreamData
	sfMaxStreamsBidi
	sfMaxStreamsUni
	sfResetStream
	sfStopSending
	sfHandshakeDone
	sfRetireCID
	sfPathChallenge
	sfDataBlocked
	sfStreamDataBlocked
	sfStreamsBlockedBidi
	sfStreamsBlockedUni
)

type sentFrame struct {
	kind   uint8
	fin    bool
	stream *Stream
	off, n uint64
}

type sentPacket struct {
	pn uint64
	// ackOf is one more than the largest packet number the packet's ACK
	// frame acknowledged, or zero if it carried none.
	ackOf        uint64
	time         time.Time
	size         int
	ackEliciting bool
	inFlight     bool
	// early says the packet was sent as 0-RTT data.
	early bool
	// mtuProbe says the packet is a probe of path MTU discovery.
	mtuProbe bool
	frames   []sentFrame
}

// maxFreeSent bounds the sent packets a connection keeps for reuse, and
// maxFreeFrames the frames one of them keeps room for: records a connection
// has in flight at once come back and go out again, and what is kept past
// that is memory for every connection.
const (
	maxFreeSent   = 16
	maxFreeFrames = 16
)

// newSentPacket returns a record for the packet numbered pn, reusing one
// that was acknowledged or declared lost when there is one: every packet
// sent needs one, and most of them are done with an RTT later. Callers hold
// c.mu.
func (c *Conn) newSentPacket(pn uint64) *sentPacket {
	n := len(c.freeSent)
	if n == 0 {
		return &sentPacket{pn: pn}
	}
	p := c.freeSent[n-1]
	c.freeSent[n-1] = nil
	c.freeSent = c.freeSent[:n-1]
	p.pn = pn
	return p
}

// releaseSent takes back a record nothing refers to any more. Callers hold
// c.mu.
func (c *Conn) releaseSent(p *sentPacket) {
	if len(c.freeSent) >= maxFreeSent {
		return
	}
	frames := p.frames[:0]
	if cap(frames) > maxFreeFrames {
		frames = nil
	} else {
		clear(p.frames)
	}
	*p = sentPacket{frames: frames}
	c.freeSent = append(c.freeSent, p)
}

type rttStats struct {
	latest, smoothed, rttvar, min time.Duration
	hasSample                     bool
	// firstAt is when the first sample was taken: lost packets sent before
	// it say nothing of persistent congestion (RFC 9002 section 7.6.2).
	firstAt time.Time
}

func newRTTStats() rttStats {
	return rttStats{smoothed: initialRTT, rttvar: initialRTT / 2}
}

func (r *rttStats) update(latest, ackDelay time.Duration) {
	r.latest = latest
	if !r.hasSample {
		r.hasSample = true
		r.min = latest
		r.smoothed = latest
		r.rttvar = latest / 2
		return
	}
	r.min = min(r.min, latest)
	adjusted := latest
	if latest >= r.min+ackDelay {
		adjusted = latest - ackDelay
	}
	diff := r.smoothed - adjusted
	if diff < 0 {
		diff = -diff
	}
	r.rttvar = (3*r.rttvar + diff) / 4
	r.smoothed = (7*r.smoothed + adjusted) / 8
}

// pto is the probe timeout before backoff, without the peer's ack delay.
func (r *rttStats) pto() time.Duration {
	return r.smoothed + max(4*r.rttvar, timerGranularity)
}

// peerMaxAckDelay is how long the peer may hold back an acknowledgement.
func (c *Conn) peerMaxAckDelay() time.Duration {
	if c.peerParams != nil {
		return c.peerParams.maxAckDelay
	}
	return 25 * time.Millisecond
}

// onPacketSent records a packet as it leaves.
func (c *Conn) onPacketSent(space int, p *sentPacket) {
	s := &c.spaces[space]
	s.sent.push(p)
	if p.ackEliciting {
		s.lastAckElicitingSent = p.time
		s.ackElicitingInFlight++
		if !c.sentSinceRecv {
			c.sentSinceRecv = true
			c.lastActivity = p.time
		}
	}
	if p.inFlight {
		c.cc.bytesInFlight += uint64(p.size)
	}
	if space == spaceApp {
		c.txPhasePackets++
	}
	if space == spaceHandshake && c.isClient && !c.spaces[spaceInitial].discarded {
		// A client stops using Initial packets once it sends a Handshake
		// one (RFC 9001 section 4.9.1).
		c.discardSpace(spaceInitial)
	}
}

// onAckReceived processes an ACK frame's ranges, largest first.
func (c *Conn) onAckReceived(space int, ranges []pnRange, ackDelay time.Duration, now time.Time) error {
	s := &c.spaces[space]
	largest := ranges[0].hi
	if largest >= s.nextPN {
		return transportErr(errProtocolViolation, "acknowledgement of an unsent packet")
	}
	if int64(largest) > s.largestAcked {
		s.largestAcked = int64(largest)
	}
	// The ranges, smallest first, go only through the slots of packets
	// still waiting: what an ACK frame says again of packets acknowledged
	// before costs nothing.
	acked := c.ackedScratch[:0]
	for i := len(ranges) - 1; i >= 0; i-- {
		lo, hi := max(ranges[i].lo, s.sent.base), min(ranges[i].hi+1, s.sent.end())
		for pn := lo; pn < hi; pn++ {
			if p := s.sent.take(pn); p != nil {
				acked = append(acked, p)
			}
		}
	}
	s.sent.trim()
	if len(acked) == 0 {
		return nil
	}
	c.ackedNew = true
	defer func() {
		for _, p := range acked {
			c.releaseSent(p)
		}
		clear(acked)
		c.ackedScratch = acked[:0]
	}()
	newest := acked[len(acked)-1]
	eliciting, oneRTT := false, false
	for _, p := range acked {
		eliciting = eliciting || p.ackEliciting
		oneRTT = oneRTT || !p.early
	}
	if newest.pn == largest && eliciting {
		if space == spaceInitial {
			ackDelay = 0
		} else if c.handshakeConfirmed {
			ackDelay = min(ackDelay, c.peerMaxAckDelay())
		}
		if !c.rtt.hasSample {
			c.rtt.firstAt = now
		}
		c.rtt.update(now.Sub(newest.time), ackDelay)
	}
	// Whether the window was in use is a matter of what was in flight
	// before this acknowledgement, and before what it shows lost.
	limited := c.cc.limited(c.cc.bytesInFlight, c.paced)
	if space == spaceApp {
		// What the acknowledgement says of the path's MTU comes first: a
		// large packet acknowledged makes those lost before it no black
		// hole.
		for _, p := range acked {
			c.mtuAckedLocked(p, now)
		}
	}
	c.detectLost(space, now)
	for _, p := range acked {
		if p.ackEliciting {
			s.ackElicitingInFlight--
		}
		if p.inFlight {
			c.cc.onAcked(p, now, c.rtt.smoothed, limited)
		}
		for i := range p.frames {
			c.frameAcked(space, &p.frames[i])
		}
		if p.ackOf > s.ackFloor+1 {
			// The peer has the ACK frame, so what it acknowledged need not
			// be again: every ACK would otherwise carry every range still
			// kept, which a peer that skips packet numbers keeps many of.
			s.ackFloor = p.ackOf - 1
			s.recv.forget(s.ackFloor)
		}
	}
	if space == spaceApp && largest >= c.txPhaseFirstPN {
		// A key update may follow an acknowledged packet of this phase.
		c.txPhaseAcked = true
	}
	if space == spaceApp && c.isClient && !c.handshakeConfirmed && oneRTT {
		// An acknowledged 1-RTT packet confirms the handshake as well as
		// HANDSHAKE_DONE would (RFC 9001 section 4.1.2).
		c.confirmHandshake()
	}
	if c.peerCompletedAddressValidation() {
		c.ptoCount = 0
	}
	// What is acknowledged no longer needs probing (RFC 9002 appendix
	// A.7): the timer the probe timeout of the packets was set for is
	// moved now, rather than by the next round of sending, which a handler
	// that is still being called holds back. One that is due sooner than
	// the timer is set for, a packet's loss by time, sets it.
	c.setLossDetectionTimer(now)
	if !c.lossDeadline.IsZero() && (c.timerAt.IsZero() || c.lossDeadline.Before(c.timerAt)) {
		c.armTimerLocked(now)
	}
	return nil
}

// detectLost declares packets lost that are too far behind the largest
// acknowledged one, by count or by time.
func (c *Conn) detectLost(space int, now time.Time) {
	s := &c.spaces[space]
	s.lossTime = time.Time{}
	if s.largestAcked < 0 {
		return
	}
	lossDelay := max(9*max(c.rtt.latest, c.rtt.smoothed)/8, timerGranularity)
	lostSendTime := now.Add(-lossDelay)
	// Only packets up to the largest acknowledged can be lost, and only
	// those are looked at.
	lost := c.lostScratch[:0]
	q := &s.sent
	end := min(uint64(s.largestAcked)+1, q.end())
	for pn := q.base; pn < end; pn++ {
		p := *q.slot(pn)
		if p == nil {
			continue
		}
		if !p.time.After(lostSendTime) || uint64(s.largestAcked) >= p.pn+packetThreshold {
			lost = append(lost, q.take(pn))
			continue
		}
		if t := p.time.Add(lossDelay); s.lossTime.IsZero() || t.Before(s.lossTime) {
			s.lossTime = t
		}
	}
	q.trim()
	if len(lost) == 0 {
		return
	}
	defer func() {
		for _, p := range lost {
			c.releaseSent(p)
		}
		clear(lost)
		c.lostScratch = lost[:0]
	}()
	var latest time.Time
	for _, p := range lost {
		if p.ackEliciting {
			s.ackElicitingInFlight--
		}
		if p.inFlight {
			c.cc.bytesInFlight -= uint64(p.size)
			if !p.mtuProbe {
				// A probe lost for its size says nothing of congestion
				// (RFC 9000 section 14.4).
				latest = p.time
			}
		}
		if space == spaceApp {
			c.mtuLostLocked(p, now)
		}
		for i := range p.frames {
			c.frameLost(space, &p.frames[i])
		}
	}
	if space == spaceApp {
		c.mtuCheckBlackHoleLocked(now)
	}
	if !latest.IsZero() {
		c.cc.onCongestion(latest, now)
		if c.persistentCongestion(lost) {
			c.cc.onPersistentCongestion()
		}
	}
}

// persistentCongestion reports whether packets just declared lost, in
// packet number order, establish persistent congestion (RFC 9002 section
// 7.6): two ack-eliciting ones sent further apart than three probe timeouts
// without backoff, both after the first round-trip sample, with every packet
// numbered between them lost as well. A packet number missing between two
// lost ones was acknowledged, or declared lost before; either ends the run,
// so that an acknowledgement in between is never missed.
func (c *Conn) persistentCongestion(lost []*sentPacket) bool {
	if !c.rtt.hasSample {
		return false
	}
	span := (c.rtt.smoothed + max(4*c.rtt.rttvar, timerGranularity) + c.peerMaxAckDelay()) * persistentCongestionThreshold
	var first *sentPacket
	for i, p := range lost {
		if i > 0 && p.pn != lost[i-1].pn+1 {
			first = nil
		}
		if !p.ackEliciting || p.mtuProbe {
			continue
		}
		if p.time.Before(c.rtt.firstAt) {
			first = nil
			continue
		}
		if first == nil {
			first = p
		} else if p.time.Sub(first.time) > span {
			return true
		}
	}
	return false
}

func (c *Conn) frameAcked(space int, f *sentFrame) {
	switch f.kind {
	case sfCrypto:
		c.spaces[space].cryptoSend.onAck(f.off, f.n)
	case sfStream:
		f.stream.onDataAcked(f.off, f.n, f.fin)
	case sfResetStream:
		f.stream.onResetAcked()
	}
}

func (c *Conn) frameLost(space int, f *sentFrame) {
	switch f.kind {
	case sfCrypto:
		c.spaces[space].cryptoSend.onLost(f.off, f.n)
	case sfStream:
		f.stream.onDataLost(f.off, f.n, f.fin)
	case sfMaxData:
		c.maxDataPending = true
	case sfMaxStreamData:
		f.stream.onMaxDataLost()
	case sfMaxStreamsBidi:
		c.maxStreamsBidiPending = true
	case sfMaxStreamsUni:
		c.maxStreamsUniPending = true
	case sfResetStream:
		f.stream.onResetLost()
	case sfStopSending:
		f.stream.onStopSendingLost()
	case sfHandshakeDone:
		c.handshakeDonePending = true
	case sfRetireCID:
		c.retireCIDs = append(c.retireCIDs, f.off)
	case sfDataBlocked:
		c.dataBlocked.lost(f.off, c.peerMaxData)
	case sfStreamsBlockedBidi:
		c.streamsBlocked[0].lost(f.off, c.peerMaxBidi)
	case sfStreamsBlockedUni:
		c.streamsBlocked[1].lost(f.off, c.peerMaxUni)
	case sfStreamDataBlocked:
		f.stream.onBlockedLost(f.off)
	case sfPathChallenge:
		// The challenge of the path being validated goes again, whichever
		// path the lost one was for.
		c.challengePending = !c.pathValidated
	}
}

// peerCompletedAddressValidation reports whether the peer can no longer be
// stuck waiting for this side to prove its address, which only a client
// has to (RFC 9002 section 6.2.2.1).
func (c *Conn) peerCompletedAddressValidation() bool {
	return !c.isClient || c.handshakeConfirmed || c.spaces[spaceHandshake].largestAcked >= 0
}

func (c *Conn) atAmplificationLimit() bool {
	return !c.isClient && (!c.addressValidated && c.bytesSent+maxDatagram > 3*c.bytesRecv ||
		!c.pathValidated && pathBudget(c.pathRecv, c.pathSent) < minPathDatagram)
}

// lossTimeSpace is the space with the earliest time-threshold loss timer.
func (c *Conn) lossTimeSpace() (time.Time, int) {
	var t time.Time
	space := 0
	for i := range c.spaces {
		if lt := c.spaces[i].lossTime; !lt.IsZero() && (t.IsZero() || lt.Before(t)) {
			t, space = lt, i
		}
	}
	return t, space
}

func (c *Conn) ackElicitingInFlight() bool {
	for i := range c.spaces {
		if c.spaces[i].ackElicitingInFlight > 0 {
			return true
		}
	}
	return false
}

// ptoTimeSpace is when the probe timeout fires and for which space.
func (c *Conn) ptoTimeSpace(now time.Time) (time.Time, int) {
	duration := c.rtt.pto() << c.ptoCount
	if !c.ackElicitingInFlight() {
		if c.spaces[spaceHandshake].tx != nil {
			return now.Add(duration), spaceHandshake
		}
		return now.Add(duration), spaceInitial
	}
	var t time.Time
	space := spaceInitial
	for i := range c.spaces {
		s := &c.spaces[i]
		if s.ackElicitingInFlight == 0 {
			continue
		}
		d := duration
		if i == spaceApp {
			// 1-RTT packets wait for the handshake to be confirmed, unless
			// they are all that is left in flight: the handshake's own
			// packets may have been declared lost and be waiting behind
			// them for room in the congestion window.
			if !c.handshakeConfirmed && !t.IsZero() {
				break
			}
			if c.handshakeConfirmed {
				d += c.peerMaxAckDelay() << c.ptoCount
			}
		}
		if pt := s.lastAckElicitingSent.Add(d); t.IsZero() || pt.Before(t) {
			t, space = pt, i
		}
	}
	return t, space
}

// setLossDetectionTimer works out when loss detection next has something to
// do.
func (c *Conn) setLossDetectionTimer(now time.Time) {
	if t, _ := c.lossTimeSpace(); !t.IsZero() {
		c.lossDeadline = t
		return
	}
	if c.atAmplificationLimit() || !c.ackElicitingInFlight() && c.peerCompletedAddressValidation() {
		c.lossDeadline = time.Time{}
		return
	}
	c.lossDeadline, _ = c.ptoTimeSpace(now)
}

func (c *Conn) onLossDetectionTimeout(now time.Time) {
	if t, space := c.lossTimeSpace(); !t.IsZero() {
		c.detectLost(space, now)
		return
	}
	if !c.ackElicitingInFlight() {
		// The client's handshake packets may all have arrived while the
		// server, not yet sure of its address, waits: prompt it.
		if c.spaces[spaceHandshake].tx != nil {
			c.spaces[spaceHandshake].probes = 1
		} else {
			c.spaces[spaceInitial].probes = 1
		}
	} else {
		_, space := c.ptoTimeSpace(now)
		if space == spaceApp && c.handshakeConfirmed && c.ptoCount+1 >= mtuBlackHolePTOs && c.maxDatagram > maxDatagram {
			// Nothing sent in that long has been acknowledged: the path may
			// have stopped carrying large datagrams, and the probes go
			// with the size every path carries.
			c.mtuBlackHoleLocked(now)
		}
		c.probe(space)
	}
	c.ptoCount++
}

// probe gets a space ready to send probe packets, carrying again what its
// oldest packets in flight carried, since that is what the peer most likely
// lacks.
func (c *Conn) probe(space int) {
	s := &c.spaces[space]
	s.probes = maxProbes
	// Handshake data waiting to be sent again goes out with the probes,
	// whatever the congestion window says, or the handshake could wait
	// forever behind packets that cannot be acknowledged without it.
	for i := spaceInitial; i < spaceApp; i++ {
		hs := &c.spaces[i]
		if i != space && hs.tx != nil && !hs.discarded && (hs.cryptoSend.hasLost() || hs.cryptoSend.next < hs.cryptoSend.end()) {
			hs.probes = max(hs.probes, 1)
		}
	}
	n := 0
	s.sent.each(s.sent.end(), func(p *sentPacket) bool {
		if !p.ackEliciting {
			return true
		}
		for i := range p.frames {
			c.frameLost(space, &p.frames[i])
		}
		n++
		return space != spaceApp || n < maxProbes
	})
}

// discardSpace drops a space's keys and what it had in flight, once the
// handshake has moved past it.
func (c *Conn) discardSpace(space int) {
	s := &c.spaces[space]
	if s.discarded {
		return
	}
	s.sent.each(s.sent.end(), func(p *sentPacket) bool {
		if p.inFlight {
			c.cc.bytesInFlight -= uint64(p.size)
		}
		return true
	})
	*s = pnSpace{discarded: true, largestAcked: s.largestAcked}
	c.ptoCount = 0
}

// rejectEarlyLocked is what a client does when the server turns its 0-RTT data
// away, or a Retry makes it start over: the early packets are given up on,
// without the congestion response a loss gets, and what they carried is
// sent again. Callers hold c.mu.
func (c *Conn) resendEarlyLocked() {
	s := &c.spaces[spaceApp]
	for pn := s.sent.base; pn < s.sent.end(); pn++ {
		p := *s.sent.slot(pn)
		if p == nil || !p.early {
			continue
		}
		s.sent.take(pn)
		if p.ackEliciting {
			s.ackElicitingInFlight--
		}
		if p.inFlight {
			c.cc.bytesInFlight -= uint64(p.size)
		}
		for i := range p.frames {
			c.frameLost(spaceApp, &p.frames[i])
		}
		c.releaseSent(p)
	}
	s.sent.trim()
	c.wantFlush = true
}

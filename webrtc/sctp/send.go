package sctp

import (
	"encoding/binary"
	"fmt"
	"time"
)

// PPIDs of RFC 8831 section 8 and the DCEP's of RFC 8832.
const (
	PPIDDCEP        = 50
	PPIDString      = 51
	PPIDBinary      = 53
	PPIDStringEmpty = 56
	PPIDBinaryEmpty = 57
)

// PR is a partial reliability policy (RFC 3758, and RFC 7496's policies
// as RFC 8831 uses them).
type PR uint8

const (
	// PRNone retransmits until the peer has the message.
	PRNone PR = iota
	// PRRetransmits gives a message up after Limit retransmissions.
	PRRetransmits
	// PRLifetime gives a message up Limit milliseconds after it was sent.
	PRLifetime
)

// SendOptions says how a message is to be sent. The zero value is
// ordered and reliable.
type SendOptions struct {
	Unordered bool
	PR        PR
	Limit     uint32
}

// outMsg is a message queued to be sent; its chunks share its data.
type outMsg struct {
	data      []byte
	stream    uint16
	ppid      uint32
	ssn       uint16
	unordered bool
	// off is how much of data has been made into chunks.
	off int
	// maxRtx is how many times a chunk may be retransmitted, or -1.
	maxRtx int
	expire time.Time
	// outstanding counts its chunks still in the retransmission queue.
	outstanding int
	queued      bool
	abandoned   bool
}

// outChunk is a DATA chunk sent and not yet cumulatively acknowledged. The
// queue holds them by value, one per TSN, so sending allocates nothing.
type outChunk struct {
	msg      *outMsg
	off, n   int32
	flags    uint8
	nsent    uint8
	missing  uint8
	acked    bool
	inFlight bool
	rtx      bool
	fastDone bool
	abandon  bool
	sentAt   time.Time
}

type sender struct {
	pending  []*outMsg
	pendHead int
	freeMsgs []*outMsg

	// infl[inflHead] has TSN cumAck+1, and nextTSN follows the last.
	infl     []outChunk
	inflHead int
	cumAck   uint32
	nextTSN  uint32

	flight   int
	cwnd     int
	ssthresh int
	pba      int
	rwnd     int

	fastRecovery bool
	fastExit     uint32
	rtxCount     int
	fastBurst    bool
	t3           time.Time

	// prCount counts queued messages under a partial reliability policy,
	// so reliable traffic never looks for messages to give up.
	prCount    int
	fwdSentFor uint32
	fwdPairs   []uint32
}

func (s *sender) init(tsn uint32, mtu int) {
	s.cumAck = tsn - 1
	s.nextTSN = tsn
	s.fwdSentFor = tsn - 1
	s.cwnd = min(4*mtu, max(2*mtu, 4380))
}

// Send queues a message on stream with payload protocol id ppid. data is
// copied. Messages queued before the association is up go out once it is.
func (a *Association) Send(stream uint16, ppid uint32, data []byte, opts SendOptions) error {
	return a.send(stream, ppid, data, "", opts)
}

func (a *Association) send(id uint16, ppid uint32, data []byte, str string, opts SendOptions) error {
	if a.closed || a.state > StateEstablished {
		return ErrClosed
	}
	n := len(data) + len(str)
	if n == 0 {
		return ErrEmptyMessage
	}
	if m := a.cfg.RemoteMaxMessageSize; m > 0 && n > m {
		return fmt.Errorf("%w: %d bytes, the peer takes %d", ErrMessageTooLarge, n, m)
	}
	if a.state == StateEstablished && id >= a.outStreams {
		return fmt.Errorf("%w: %d of %d", ErrInvalidStream, id, a.outStreams)
	}
	st := a.stream(id)
	if st.resetting {
		return ErrStreamClosing
	}
	m := a.newMsg()
	m.data = a.pool.get(n)
	copy(m.data[copy(m.data, data):], str)
	m.stream, m.ppid, m.unordered = id, ppid, opts.Unordered
	m.maxRtx = -1
	now := a.cfg.Now()
	switch opts.PR {
	case PRRetransmits:
		m.maxRtx = int(opts.Limit)
		a.prCount++
	case PRLifetime:
		m.expire = now.Add(time.Duration(opts.Limit) * time.Millisecond)
		a.prCount++
	}
	m.queued = true
	a.pending = append(a.pending, m)
	st.queued++
	st.buffered += n
	if a.depth == 0 {
		a.enter(now)
		a.exit()
	}
	return nil
}

func (a *Association) newMsg() *outMsg {
	if n := len(a.freeMsgs); n > 0 {
		m := a.freeMsgs[n-1]
		a.freeMsgs = a.freeMsgs[:n-1]
		return m
	}
	return &outMsg{}
}

// releaseMsg recycles a message nothing refers to any more.
func (a *Association) releaseMsg(m *outMsg) {
	if m.queued || m.outstanding > 0 {
		return
	}
	if m.maxRtx >= 0 || !m.expire.IsZero() {
		a.prCount--
	}
	a.pool.put(m.data)
	*m = outMsg{}
	if len(a.freeMsgs) < 4096 {
		a.freeMsgs = append(a.freeMsgs, m)
	}
}

func (a *Association) hasPending() bool { return a.pendHead < len(a.pending) }

// outstanding reports whether any TSN sent awaits its cumulative ack.
func (a *Association) outstanding() bool { return a.inflHead < len(a.infl) }

func (a *Association) wantsToSend() bool {
	return a.rtxCount > 0 || (a.hasPending() && a.flight < a.cwnd)
}

// popPending takes the head message off the queue.
func (a *Association) popPending() {
	m := a.pending[a.pendHead]
	a.pending[a.pendHead] = nil
	a.pendHead++
	if a.pendHead == len(a.pending) {
		a.pending = a.pending[:0]
		a.pendHead = 0
	} else if a.pendHead >= 64 && a.pendHead*2 >= len(a.pending) {
		n := copy(a.pending, a.pending[a.pendHead:])
		clear(a.pending[n:])
		a.pending = a.pending[:n]
		a.pendHead = 0
	}
	m.queued = false
	a.stream(m.stream).queued--
	a.releaseMsg(m)
}

// bufferedDone takes n bytes off stream id's buffered amount and tells its
// data channel when that falls to its low-water mark.
func (a *Association) bufferedDone(id uint16, n int) {
	st := a.stream(id)
	before := st.buffered
	st.buffered -= n
	if ch := st.ch; ch != nil && before > ch.lowWater && st.buffered <= ch.lowWater && ch.OnBufferedAmountLow != nil {
		ch.OnBufferedAmountLow()
	}
}

// expired reports whether m's lifetime is over, which only matters when
// the peer can be told to skip it.
func (a *Association) expired(m *outMsg) bool {
	return !m.expire.IsZero() && a.peerForwardTSN && !a.now.Before(m.expire)
}

// flushData sends retransmissions, then new data, as the windows allow.
func (a *Association) flushData() {
	if a.state == StateShutdownAckSent {
		return
	}
	if a.prCount > 0 {
		a.abandonExpired()
	}
	if a.peerForwardTSN {
		a.maybeForwardTSN()
	}
	if a.rtxCount > 0 {
		a.sendRetransmissions()
	}
	if a.rtxCount > 0 || a.closed {
		return
	}
	for a.hasPending() && a.flight < a.cwnd && !a.closed {
		m := a.pending[a.pendHead]
		if m.abandoned || a.expired(m) {
			if !m.abandoned {
				a.abandon(m)
			}
			a.popPending()
			continue
		}
		n := min(len(m.data)-m.off, a.maxPayload)
		if n > a.rwnd && a.flight > 0 {
			break
		}
		st := a.stream(m.stream)
		flags := uint8(0)
		if m.off == 0 {
			flags |= flagBegin
			if !m.unordered {
				m.ssn = st.ssn
				st.ssn++
			}
		}
		if m.unordered {
			flags |= flagUnordered
		}
		if m.off+n == len(m.data) {
			flags |= flagEnd
		}
		a.infl = append(a.infl, outChunk{msg: m, off: int32(m.off), n: int32(n), flags: flags})
		c := &a.infl[len(a.infl)-1]
		m.off += n
		m.outstanding++
		a.nextTSN++
		a.transmit(c, a.nextTSN-1)
		if m.off == len(m.data) {
			a.popPending()
		}
	}
}

// transmit writes chunk c with TSN tsn into the packet being built.
func (a *Association) transmit(c *outChunk, tsn uint32) {
	m := c.msg
	b := a.reserve(chunkData, c.flags, dataHeaderSize-chunkHeaderSize+int(c.n))
	binary.BigEndian.PutUint32(b, tsn)
	binary.BigEndian.PutUint16(b[4:], m.stream)
	if c.flags&flagUnordered == 0 {
		binary.BigEndian.PutUint16(b[6:], m.ssn)
	} else {
		b[6], b[7] = 0, 0
	}
	binary.BigEndian.PutUint32(b[8:], m.ppid)
	copy(b[12:], m.data[c.off:c.off+c.n])
	if c.nsent < 255 {
		c.nsent++
	}
	c.sentAt = a.now
	c.inFlight = true
	c.missing = 0
	a.flight += int(c.n)
	a.rwnd = max(a.rwnd-int(c.n), 0)
	if a.t3.IsZero() {
		a.t3 = a.now.Add(a.rto)
	}
}

// sendRetransmissions resends chunks marked lost, oldest first. Those fast
// retransmit marked go out in one packet whatever the congestion window
// says (RFC 9260 section 7.2.4); the rest wait for room in it.
func (a *Association) sendRetransmissions() {
	burst := a.fastBurst
	a.fastBurst = false
	if burst && a.room() < dataHeaderSize+a.maxPayload {
		a.sendPacket()
	}
	for i := a.inflHead; i < len(a.infl) && a.rtxCount > 0 && !a.closed; i++ {
		c := &a.infl[i]
		if !c.rtx {
			continue
		}
		m := c.msg
		if m.maxRtx >= 0 && int(c.nsent) > m.maxRtx && a.peerForwardTSN || a.expired(m) {
			a.abandon(m)
			continue
		}
		fits := a.room() >= dataHeaderSize+int(c.n)
		if burst {
			if !fits {
				burst = false
			}
		}
		if !burst && a.flight >= a.cwnd {
			return
		}
		c.rtx = false
		a.rtxCount--
		a.transmit(c, a.cumAck+1+uint32(i-a.inflHead))
	}
}

// abandon gives message m up (RFC 3758 section 3.5): its chunks leave the
// flight and the peer is told to skip them with FORWARD TSN.
func (a *Association) abandon(m *outMsg) {
	if m.abandoned {
		return
	}
	m.abandoned = true
	for i := a.inflHead; i < len(a.infl) && m.outstanding > 0; i++ {
		c := &a.infl[i]
		if c.msg != m || c.abandon {
			continue
		}
		c.abandon = true
		if c.inFlight {
			c.inFlight = false
			a.flight -= int(c.n)
		}
		if c.rtx {
			c.rtx = false
			a.rtxCount--
		}
		if !c.acked {
			a.bufferedDone(m.stream, int(c.n))
		}
	}
	if m.queued && m.off < len(m.data) {
		a.bufferedDone(m.stream, len(m.data)-m.off)
		m.off = len(m.data)
	}
}

func (a *Association) abandonExpired() {
	if !a.peerForwardTSN {
		return
	}
	for i := a.inflHead; i < len(a.infl); i++ {
		c := &a.infl[i]
		if !c.acked && !c.abandon && a.expired(c.msg) {
			a.abandon(c.msg)
		}
	}
}

// maybeForwardTSN sends FORWARD TSN when abandoned chunks lie right past
// the cumulative ack point and the peer has not been told of them yet.
func (a *Association) maybeForwardTSN() {
	adv := a.cumAck
	for i := a.inflHead; i < len(a.infl) && a.infl[i].abandon; i++ {
		adv++
	}
	if adv == a.cumAck || adv == a.fwdSentFor {
		return
	}
	a.fwdSentFor = adv
	// One stream and SSN for each ordered stream skipped, its highest.
	pairs := a.fwdPairs[:0]
	for i := a.inflHead; i < a.inflHead+int(adv-a.cumAck); i++ {
		c := &a.infl[i]
		if c.flags&flagUnordered != 0 {
			continue
		}
		p := uint32(c.msg.stream)<<16 | uint32(c.msg.ssn)
		found := false
		for j, q := range pairs {
			if q>>16 == p>>16 {
				if ssnLE(uint16(q), uint16(p)) {
					pairs[j] = p
				}
				found = true
				break
			}
		}
		if !found {
			pairs = append(pairs, p)
		}
	}
	a.fwdPairs = pairs
	maxPairs := (a.mtu - headerSize - chunkHeaderSize - 4) / 4
	if len(pairs) > maxPairs {
		pairs = pairs[:maxPairs]
	}
	b := a.reserve(chunkForwardTSN, 0, 4+4*len(pairs))
	binary.BigEndian.PutUint32(b, adv)
	for i, p := range pairs {
		binary.BigEndian.PutUint32(b[4+4*i:], p)
	}
}

// handleSack processes the peer's SACK (RFC 9260 section 6.2.1 and 7.2).
func (a *Association) handleSack(v []byte) error {
	var s sackChunk
	if err := parseSack(v, &s); err != nil {
		return err
	}
	if a.state < StateEstablished {
		return nil
	}
	if tsnLT(s.cum, a.cumAck) {
		return nil
	}
	if !tsnLT(s.cum, a.nextTSN) {
		a.abort(fmt.Errorf("%w: SACK of TSN %d not sent", ErrProtocol, s.cum), causeProtocolViolation)
		return nil
	}
	flightBefore := a.flight
	oldCum := a.cumAck
	acked := a.ackUpTo(s.cum)

	// Gap blocks: chunks the peer has past the cumulative point.
	var htna uint32
	gotHTNA := false
	count := uint32(len(a.infl) - a.inflHead)
	for g := s.gaps; len(g) >= 4; g = g[4:] {
		start := uint32(binary.BigEndian.Uint16(g))
		end := uint32(binary.BigEndian.Uint16(g[2:]))
		if start == 0 || end < start {
			continue
		}
		for off := start; off <= end && off <= count; off++ {
			c := &a.infl[a.inflHead+int(off)-1]
			if c.acked || c.abandon {
				continue
			}
			acked += a.ackChunk(c)
			htna = s.cum + off
			gotHTNA = true
		}
	}

	// Fast retransmit: chunks the peer missed three times over.
	if gotHTNA {
		for i := a.inflHead; i < len(a.infl); i++ {
			tsn := a.cumAck + 1 + uint32(i-a.inflHead)
			if !tsnLT(tsn, htna) {
				break
			}
			c := &a.infl[i]
			if c.acked || c.abandon || c.rtx || !c.inFlight || c.fastDone {
				continue
			}
			c.missing++
			if c.missing < 3 {
				continue
			}
			c.fastDone = true
			c.rtx = true
			c.inFlight = false
			a.flight -= int(c.n)
			a.rtxCount++
			a.fastBurst = true
			if !a.fastRecovery {
				a.fastRecovery = true
				a.fastExit = a.nextTSN - 1
				a.ssthresh = max(a.cwnd/2, 4*a.mtu)
				a.cwnd = a.ssthresh
				a.pba = 0
			}
		}
	}

	cumMoved := s.cum != oldCum
	if a.fastRecovery && !tsnLT(a.cumAck, a.fastExit) {
		a.fastRecovery = false
	}
	if cumMoved && !a.fastRecovery && acked > 0 {
		full := flightBefore >= a.cwnd || a.hasPending()
		if a.cwnd <= a.ssthresh {
			if full {
				a.cwnd += min(acked, a.mtu)
			}
		} else {
			a.pba += acked
			if a.pba >= a.cwnd && full {
				a.pba -= a.cwnd
				a.cwnd += a.mtu
			}
		}
	}
	a.rwnd = max(int(s.rwnd)-a.flight, 0)
	if !a.outstanding() {
		a.t3 = time.Time{}
	} else if cumMoved {
		a.t3 = a.now.Add(a.rto)
	}
	return nil
}

// ackUpTo moves the cumulative ack point to cum, which SACK and SHUTDOWN
// both carry, and returns the bytes it newly acknowledged.
func (a *Association) ackUpTo(cum uint32) int {
	if !tsnGT(cum, a.cumAck) || !tsnLT(cum, a.nextTSN) {
		return 0
	}
	acked := 0
	k := int(cum - a.cumAck)
	rttDone := false
	for i := a.inflHead; i < a.inflHead+k; i++ {
		c := &a.infl[i]
		if !c.acked && !c.abandon {
			if !rttDone && c.nsent == 1 {
				// Karn's rule: only chunks sent once time the path.
				a.rttSample(a.now.Sub(c.sentAt))
				rttDone = true
			}
			acked += a.ackChunk(c)
		}
		if c.rtx {
			c.rtx = false
			a.rtxCount--
		}
		if c.inFlight {
			c.inFlight = false
			a.flight -= int(c.n)
		}
		m := c.msg
		*c = outChunk{}
		m.outstanding--
		a.releaseMsg(m)
	}
	a.inflHead += k
	a.cumAck = cum
	if tsnLT(a.fwdSentFor, cum) {
		a.fwdSentFor = cum
	}
	if a.inflHead == len(a.infl) {
		a.infl = a.infl[:0]
		a.inflHead = 0
	} else if a.inflHead >= 256 && a.inflHead*2 >= len(a.infl) {
		n := copy(a.infl, a.infl[a.inflHead:])
		clear(a.infl[n:])
		a.infl = a.infl[:n]
		a.inflHead = 0
	}
	a.errorCount = 0
	return acked
}

// ackChunk marks c acknowledged and returns its size.
func (a *Association) ackChunk(c *outChunk) int {
	c.acked = true
	if c.inFlight {
		c.inFlight = false
		a.flight -= int(c.n)
	}
	if c.rtx {
		c.rtx = false
		a.rtxCount--
	}
	a.bufferedDone(c.msg.stream, int(c.n))
	return int(c.n)
}

// onT3 handles a retransmission timeout (RFC 9260 section 6.3.3): the
// window collapses to one MTU, RTO doubles and every chunk outstanding is
// sent again.
func (a *Association) onT3() {
	a.t3 = time.Time{}
	if !a.outstanding() {
		return
	}
	a.errorCount++
	if a.errorCount > a.cfg.MaxRetransmits {
		a.terminate(ErrTimeout)
		return
	}
	a.ssthresh = max(a.cwnd/2, 4*a.mtu)
	a.cwnd = a.mtu
	a.pba = 0
	a.fastRecovery = false
	a.rto = min(2*a.rto, a.cfg.RTOMax)
	for i := a.inflHead; i < len(a.infl); i++ {
		c := &a.infl[i]
		if c.acked || c.abandon {
			continue
		}
		if c.inFlight {
			c.inFlight = false
			a.flight -= int(c.n)
		}
		if !c.rtx {
			c.rtx = true
			a.rtxCount++
		}
	}
	// Resend FORWARD TSN too, should the last one have been lost.
	a.fwdSentFor = a.cumAck
	a.t3 = a.now.Add(a.rto)
}

// BufferedAmount is how many bytes queued on stream await the peer's
// acknowledgement.
func (a *Association) BufferedAmount(stream uint16) int {
	if int(stream) < len(a.streams) && a.streams[stream] != nil {
		return a.streams[stream].buffered
	}
	return 0
}

package quic

import "time"

// Path MTU discovery is DPLPMTUD (RFC 8899), as RFC 9000 section 14.3 has
// QUIC do it: once the handshake is confirmed, the connection sends probes,
// packets of a PING padded out to a size it wants to know about, and a probe
// that is acknowledged shows the path carries datagrams of its size, which
// the connection then sends. A probe that is lost again and again shows the
// path does not; the sizes in between are searched by halves, and once the
// search is over, it starts again from the top every mtuRaiseInterval, in
// case the path has changed for the better.
//
// A probe is ack-eliciting and counts in flight like any packet, but its
// loss is no sign of congestion, so it neither shrinks the congestion window
// nor counts towards persistent congestion, and it carries nothing that has
// to be sent again. Probes are only worth anything on a path that drops a
// datagram too large for it rather than fragment it, so discovery runs only
// on a path whose PacketConn is a DontFragmenter that agrees to that.
//
// A path that stops carrying the size the connection has settled on, a
// route that changed under it, say, is a black hole for most of what it
// sends. A connection sending datagrams larger than maxDatagram, the size
// every path carries, goes back to it when packets larger than it are lost
// while none of those sent after them is acknowledged, or when its probe
// timeout fires mtuBlackHolePTOs times in a row; discovery then starts again
// a while later. This is done whether the larger size was discovered or set
// with Config.MaxDatagramSize.

// DontFragmenter is a PacketConn that can have its datagrams sent whole or
// not at all: with the Don't Fragment bit over IPv4, and never fragmented by
// the sending host over IPv6. fib.Connection is one.
type DontFragmenter interface {
	// SetDontFragment stops the datagrams sent through the PacketConn from
	// being fragmented, and reports why it cannot when it cannot. It may be
	// called again for each connection that sends through it.
	SetDontFragment() error
}

const (
	// mtuProbeAttempts is how many probes of one size are lost before the
	// path is taken not to carry it: MAX_PROBES of RFC 8899.
	mtuProbeAttempts = 3
	// mtuSearchStep ends a search once the largest size known to be carried
	// and the smallest known not to be are this close.
	mtuSearchStep = 20
	// mtuRaiseInterval is how long a search that ended below the largest
	// size it looked for waits before it looks again: PMTU_RAISE_TIMER of
	// RFC 8899.
	mtuRaiseInterval = 10 * time.Minute
	// mtuBlackHoleWait is how long discovery waits after a black hole
	// before it searches again.
	mtuBlackHoleWait = time.Minute
	// mtuBlackHoleLosses is how many lost packets larger than maxDatagram,
	// with nothing larger than it acknowledged since they were sent, make a
	// black hole.
	mtuBlackHoleLosses = 3
	// mtuBlackHolePTOs is how many probe timeouts in a row make a black
	// hole of a path the connection sends large datagrams on: a path that
	// carries nothing larger than maxDatagram acknowledges nothing a
	// connection whose packets are all larger sends.
	mtuBlackHolePTOs = 3
)

// mtuState is a connection's path MTU discovery, and its black hole
// detection.
type mtuState struct {
	// on says discovery runs on the path; limit is the largest size it
	// looks for.
	on    bool
	limit int
	// lo is the largest size the path is known to carry, and hi the
	// smallest it is known not to, or limit+1.
	lo, hi int
	// probe is the size of the next probe, or of the one in flight, and zero
	// when there is none to send; at, when set, is when it may go.
	probe int
	at    time.Time
	// inFlight says the probe has been sent, as packet probePN, and fails
	// how many probes of its size were lost before it.
	inFlight bool
	probePN  uint64
	fails    int
	// largeAcked is when the newest acknowledged packet larger than
	// maxDatagram was sent, and largeLost how many larger packets sent since
	// then have been lost.
	largeAcked time.Time
	largeLost  int
}

// startMTUDiscoveryLocked starts path MTU discovery on the path the
// connection sends on, if the config, the peer and the path allow it, once
// the handshake is confirmed or the connection has settled on a new path.
// Callers hold c.mu.
func (c *Conn) startMTUDiscoveryLocked(now time.Time) {
	c.mtu = mtuState{largeAcked: now}
	if c.config.DisablePathMTUDiscovery || c.peerParams == nil || !c.handshakeConfirmed {
		return
	}
	limit := int(min(uint64(c.config.MaxProbeDatagramSize), c.peerParams.maxUDPPayloadSize))
	if limit <= c.maxDatagram {
		return
	}
	if df, ok := c.pc.(DontFragmenter); !ok || df.SetDontFragment() != nil {
		return
	}
	m := &c.mtu
	m.on, m.limit = true, limit
	m.lo, m.hi = c.maxDatagram, limit+1
	// Most paths carry what the limit allows, an Ethernet one with nothing
	// in the way: the first probe asks about it, and a search follows only
	// if it is lost.
	m.probe = limit
	c.wantFlush = true
}

// stopMTUDiscoveryLocked stops discovery, as the connection moves to a path
// it knows nothing about yet. A probe still in flight is forgotten. Callers
// hold c.mu.
func (c *Conn) stopMTUDiscoveryLocked(now time.Time) {
	c.mtu = mtuState{largeAcked: now}
}

// mtuProbeDue returns the size of the probe to send now, or zero. Callers
// hold c.mu.
func (c *Conn) mtuProbeDue(now time.Time) int {
	m := &c.mtu
	if !m.on || m.probe == 0 || m.inFlight || !m.at.IsZero() && now.Before(m.at) {
		return 0
	}
	if !c.pathValidated || c.spaces[spaceApp].tx == nil || c.closeWhenDone != nil {
		return 0
	}
	if c.cc.bytesInFlight+uint64(m.probe) > c.cc.cwnd || !c.paceAllows(now) {
		return 0
	}
	return m.probe
}

// mtuProbeTimer is when a probe that waits for its time may go, or zero.
// Callers hold c.mu.
func (c *Conn) mtuProbeTimer() time.Time {
	if m := &c.mtu; m.on && m.probe > 0 && !m.inFlight {
		return m.at
	}
	return time.Time{}
}

// appendMTUProbe appends a probe of size bytes to out, which has room for
// it: a 1-RTT packet of a PING and PADDING, built in scratch. Callers hold
// c.mu.
func (c *Conn) appendMTUProbe(out, scratch []byte, size int, now time.Time) []byte {
	s := &c.spaces[spaceApp]
	pn := s.nextPN
	pl := pnLen(pn, s.largestAcked)
	payload := scratch[:size-c.headerLen(spaceApp, pl, false)-aeadOverhead]
	payload[0] = framePing
	clear(payload[1:])
	sp := c.newSentPacket(pn)
	sp.ackEliciting = true
	sp.mtuProbe = true
	s.nextPN++
	plan := packetPlan{space: spaceApp, pn: pn, pnLen: pl, payload: payload, sp: sp, padded: true}
	out = c.sealPlan(out, &plan)
	sp.time = now
	sp.size = c.packetSize(&plan)
	sp.inFlight = true
	c.paceBudget -= sp.size
	c.onPacketSent(spaceApp, sp)
	m := &c.mtu
	m.inFlight, m.probePN, m.at = true, pn, time.Time{}
	return out
}

// mtuAckedLocked learns from a packet that was acknowledged: a probe's size
// is carried, and so is that of a large packet, which says the path is no
// black hole. Callers hold c.mu.
func (c *Conn) mtuAckedLocked(p *sentPacket, now time.Time) {
	m := &c.mtu
	if p.size > maxDatagram && p.time.After(m.largeAcked) {
		m.largeAcked = p.time
		m.largeLost = 0
	}
	if !p.mtuProbe || !m.inFlight || p.pn != m.probePN {
		return
	}
	m.inFlight, m.fails = false, 0
	m.lo = max(m.lo, p.size)
	c.setMaxDatagramLocked(max(c.maxDatagram, p.size))
	c.nextMTUProbeLocked(now)
}

// mtuLostLocked learns from a packet that was declared lost: a probe's size
// may not be carried, and a large packet lost may be a black hole. Callers
// hold c.mu.
func (c *Conn) mtuLostLocked(p *sentPacket, now time.Time) {
	m := &c.mtu
	if p.mtuProbe {
		if !m.inFlight || p.pn != m.probePN {
			return
		}
		m.inFlight = false
		if m.fails++; m.fails < mtuProbeAttempts {
			// The same size again: one probe lost is as likely the
			// network's doing as the size's.
			c.wantFlush = true
			return
		}
		m.fails = 0
		m.hi = min(m.hi, p.size)
		c.nextMTUProbeLocked(now)
		return
	}
	if p.ackEliciting && p.size > maxDatagram && p.time.After(m.largeAcked) {
		m.largeLost++
	}
}

// mtuCheckBlackHoleLocked falls back to maxDatagram once the large packets
// lost make the path a black hole for them. Callers hold c.mu.
func (c *Conn) mtuCheckBlackHoleLocked(now time.Time) {
	if c.mtu.largeLost >= mtuBlackHoleLosses && c.maxDatagram > maxDatagram {
		c.mtuBlackHoleLocked(now)
	}
}

// mtuBlackHoleLocked has the connection send no datagram larger than
// maxDatagram, which every path carries, and discovery search again from
// there a while later. Callers hold c.mu.
func (c *Conn) mtuBlackHoleLocked(now time.Time) {
	c.setMaxDatagramLocked(maxDatagram)
	m := &c.mtu
	m.largeAcked, m.largeLost = now, 0
	if !m.on {
		return
	}
	m.lo, m.hi = maxDatagram, m.limit+1
	m.inFlight, m.fails = false, 0
	m.probe, m.at = m.limit, now.Add(mtuBlackHoleWait)
}

// nextMTUProbeLocked picks the size to probe next, halfway between what is
// known to be carried and what is known not to be, or ends the search when
// they are close, to start it again from the top mtuRaiseInterval later
// unless the path carries the limit. Callers hold c.mu.
func (c *Conn) nextMTUProbeLocked(now time.Time) {
	m := &c.mtu
	if m.lo >= m.limit {
		m.probe = 0
		return
	}
	if m.hi-m.lo < mtuSearchStep {
		m.hi = m.limit + 1
		m.probe, m.at = m.limit, now.Add(mtuRaiseInterval)
		return
	}
	m.probe, m.at = (m.lo+m.hi)/2, time.Time{}
	c.wantFlush = true
}

// setMaxDatagramLocked changes the largest datagram the connection sends,
// as the handshake, a new path or path MTU discovery settles on a size. The
// congestion controller counts its window in that size, and keeps its own
// copy: it follows from here, minimum window included. Callers hold c.mu.
func (c *Conn) setMaxDatagramLocked(size int) {
	c.maxDatagram = size
	c.cc.setMaxDatagramSize(size)
}

// DatagramSize returns the size of the largest datagram the connection
// sends now: maxDatagram until the handshake is over, then what the config
// and the peer allow and path MTU discovery found the path to carry.
func (c *Conn) DatagramSize() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxDatagram
}

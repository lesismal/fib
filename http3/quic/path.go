package quic

import (
	"crypto/rand"
	"net"
	"slices"
	"time"
)

// minPathDatagram is the smallest datagram worth sending on a path the
// amplification limit holds back: room for a short header, a PATH_CHALLENGE
// or an acknowledgement, and the AEAD's tag.
const minPathDatagram = 64

// pathBudget is how many more bytes may go to an unvalidated path that
// recv bytes arrived on and sent went to.
func pathBudget(recv, sent uint64) int {
	if limit := 3 * recv; limit > sent {
		return int(min(limit-sent, 1<<30))
	}
	return 0
}

// migrateLocked moves a server's connection to the path the packet being
// handled arrived on, which proved to be the client's latest (see
// HandlePathDatagram). The handler hears of the move before the calls the
// packet queued from the event at first on, so that a request the packet
// carries is seen from the address it came from. Callers hold c.mu.
func (c *Conn) migrateLocked(now time.Time, first int) {
	pc, remote := c.rxPC, c.rxRemote
	c.events = slices.Insert(c.events, first, event{kind: evPath})
	if pc == c.prevPC {
		// Back to the path the connection was validating away from, which
		// was validated already: the one it tried is left.
		c.giveUpLocked(c.pc)
		c.pc, c.remote = c.prevPC, c.prevRemote
		c.endValidationLocked()
		return
	}
	if c.pathValidated {
		c.prevPC, c.prevRemote = c.pc, c.remote
	} else {
		// A path that was still being validated is left for this one, and
		// the last validated path stays the one to go back to.
		c.giveUpLocked(c.pc)
	}
	if !sameHost(c.remote, remote) {
		// What was learned of the old path says nothing of a new one,
		// unless only the port changed (RFC 9000 section 9.4). Packets in
		// flight on the old path stay counted until they are acknowledged
		// or lost.
		c.rtt = newRTTStats()
		c.cc.cwnd, c.cc.ssthresh, c.cc.recoveryStart = initialCongestionWindow, ^uint64(0), now
	}
	c.pc, c.remote = pc, remote
	c.pathValidated = false
	c.pathRecv, c.pathSent = 0, 0
	// The new path is not known to carry the datagram size the old one
	// did, so it gets the size every path carries.
	c.maxDatagram = maxDatagram
	_, _ = rand.Read(c.challenge[:])
	c.challengePending = true
	// Three times the larger of the current PTO and the one for a new path
	// (RFC 9000 section 8.2.4).
	initial := newRTTStats()
	c.pathDeadline = now.Add(3 * max(c.rtt.pto(), initial.pto()))
}

// pathValidLocked ends a validation the client answered: the connection
// stays on the new path, and the one it left goes. Callers hold c.mu.
func (c *Conn) pathValidLocked() {
	if c.prevPC != nil {
		c.giveUpLocked(c.prevPC)
	}
	c.endValidationLocked()
	if c.peerParams != nil {
		c.maxDatagram = int(min(uint64(c.config.MaxDatagramSize), c.peerParams.maxUDPPayloadSize))
	}
}

// pathFailedLocked ends a validation the client did not answer in time: the
// connection goes back to the path it left, and the new one goes (RFC 9000
// section 9.3.2). Callers hold c.mu.
func (c *Conn) pathFailedLocked() {
	if c.prevPC == nil {
		c.terminateLocked(ErrPathValidation)
		return
	}
	c.giveUpLocked(c.pc)
	c.pc, c.remote = c.prevPC, c.prevRemote
	c.events = append(c.events, event{kind: evPath})
	c.endValidationLocked()
}

// endValidationLocked leaves the connection on a validated path. Callers
// hold c.mu.
func (c *Conn) endValidationLocked() {
	c.prevPC, c.prevRemote = nil, nil
	c.pathValidated = true
	c.challengePending = false
	c.pathDeadline = time.Time{}
	c.pathRecv, c.pathSent = 0, 0
}

// giveUpLocked leaves pc for good; it is closed once the lock is let go.
// Callers hold c.mu.
func (c *Conn) giveUpLocked(pc PacketConn) {
	if pc != nil {
		c.givenUp = append(c.givenUp, pc)
	}
}

// takeGivenUpLocked returns the paths to close. Callers hold c.mu.
func (c *Conn) takeGivenUpLocked() []PacketConn {
	givenUp := c.givenUp
	c.givenUp = nil
	return givenUp
}

// closePaths closes paths a connection has left. Callers hold no lock.
func closePaths(paths []PacketConn) {
	for _, pc := range paths {
		_ = pc.Close()
	}
}

// sameHost reports whether two addresses differ in their ports alone, or not
// at all.
func sameHost(a, b net.Addr) bool {
	ua, ok := a.(*net.UDPAddr)
	ub, ok2 := b.(*net.UDPAddr)
	return ok && ok2 && ua.IP.Equal(ub.IP) && ua.Zone == ub.Zone
}

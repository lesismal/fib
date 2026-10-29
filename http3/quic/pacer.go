package quic

import (
	"errors"
	"syscall"
	"time"
)

// Pacing spreads what the congestion window allows over the round trip
// rather than sending it at once (RFC 9002 section 7.7): a burst of a whole
// window leaves faster than the path drains it, and the queue it builds on
// the way is where datagrams are dropped. It is a token bucket: the budget
// fills at 1.25 congestion windows a smoothed round trip, the margin RFC 9002
// suggests so that a round trip that varies does not leave the window
// unused, up to a burst of maxBurstPackets datagrams or of two
// minPacingDelay's worth, whichever is more; a datagram in flight takes its
// size from it. A round of sending that finds the budget spent stops, and
// the connection's timer resumes it once the budget has a datagram again.
//
// Until the first round-trip sample there is nothing to pace by, and the
// first flight is the initial window anyway; probes and acknowledgements are
// not held back. A round trip shorter than minPacingDelay paces as one of
// minPacingDelay, which the burst covers more than twice over: pacing there
// would hold nothing back, and one of zero, which a coarse clock measures
// on a fast path, would make the rate infinite.
const (
	// pacingGain is N of RFC 9002 section 7.7, in quarters.
	pacingGain = 5
	// maxBurstPackets is the least burst pacing allows: the initial window,
	// in datagrams of the path's size.
	maxBurstPackets = 10
)

// pacingRate is what pacing allows, in bytes a second.
func (c *Conn) pacingRate() float64 {
	return float64(c.cc.cwnd) * pacingGain / 4 / max(c.rtt.smoothed, minPacingDelay).Seconds()
}

// paceAllows fills the pacing budget for the time since it was last
// filled and reports whether a full datagram fits in it. Callers hold c.mu.
func (c *Conn) paceAllows(now time.Time) bool {
	if !c.rtt.hasSample {
		return true
	}
	if !c.paceLast.IsZero() && !now.After(c.paceLast) {
		// The round's datagrams after its first find the budget as the
		// first left it: no time has passed to fill it.
		c.paceLast = now
		return c.paceBudget >= c.maxDatagram
	}
	rate := c.pacingRate()
	burst := max(float64(maxBurstPackets*c.maxDatagram), rate*2*minPacingDelay.Seconds())
	if c.paceLast.IsZero() {
		c.paceBudget = int(burst)
	} else if elapsed := now.Sub(c.paceLast); elapsed > 0 {
		c.paceBudget = int(min(float64(c.paceBudget)+rate*elapsed.Seconds(), burst))
	}
	c.paceLast = now
	return c.paceBudget >= c.maxDatagram
}

// paceNext is when the pacing budget has a full datagram again, at least
// minPacingDelay away. Callers hold c.mu.
func (c *Conn) paceNext(now time.Time) time.Time {
	need := float64(c.maxDatagram - c.paceBudget)
	d := time.Duration(need / c.pacingRate() * float64(time.Second))
	return now.Add(max(d, minPacingDelay))
}

// blockedRetry is how long a round of sending that found the socket full
// waits before it tries again when nothing will say the socket has room:
// a path that is no WritableNotifier, or cannot tell on its system, and a
// full queue past the socket, which ENOBUFS reports (see awaitRoomLocked).
const blockedRetry = time.Millisecond

// wouldBlock reports whether a send failed only because the socket had no
// room: EAGAIN, or ENOBUFS, which macOS answers for a full UDP socket and
// Linux for a full device queue, or their Windows counterparts (see
// socketFull).
func wouldBlock(err error) bool {
	return errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.ENOBUFS) || socketFull(err)
}

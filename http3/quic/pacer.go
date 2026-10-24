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
// not held back.
const (
	// pacingGain is N of RFC 9002 section 7.7, in quarters.
	pacingGain = 5
	// maxBurstPackets is the least burst pacing allows: the initial window.
	maxBurstPackets = 10
	// minPacingDelay is the shortest wait for the budget: a timer set any
	// closer would fire late anyway, and the burst covers twice it.
	minPacingDelay = time.Millisecond
)

// pacingRate is what pacing allows, in bytes a second.
func (c *Conn) pacingRate() float64 {
	return float64(c.cc.cwnd) * pacingGain / 4 / c.rtt.smoothed.Seconds()
}

// paceAllows fills the pacing budget for the time since it was last
// filled and reports whether a full datagram fits in it. Callers hold c.mu.
func (c *Conn) paceAllows(now time.Time) bool {
	if !c.rtt.hasSample {
		return true
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
// waits before it tries again: the engine says when a socket can be read,
// but not when a full one can be written again.
const blockedRetry = time.Millisecond

// wouldBlock reports whether a send failed only because the socket had no
// room: EAGAIN, or ENOBUFS, which macOS answers for a full UDP socket and
// Linux for a full device queue.
func wouldBlock(err error) bool {
	return errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.ENOBUFS)
}

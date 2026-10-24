package quic

import (
	"math"
	"strconv"
	"time"
)

// CongestionControl names a congestion controller (see Config).
type CongestionControl int

const (
	// CUBIC is RFC 9438's controller, the default: after a loss its window
	// shrinks to 0.7 of what it was rather than half, and it grows back
	// along a cubic curve whose plateau is the window the loss came at,
	// quickly at first and then slowly near it, and as quickly as NewReno
	// at least.
	CUBIC CongestionControl = iota
	// NewReno is RFC 9002 section 7's controller: half the window after a
	// loss, and a datagram more a round trip after that.
	NewReno
)

func (cc CongestionControl) String() string {
	switch cc {
	case CUBIC:
		return "CUBIC"
	case NewReno:
		return "NewReno"
	}
	return "CongestionControl(" + strconv.Itoa(int(cc)) + ")"
}

// CUBIC's constants (RFC 9438 section 4): C, in segments a second cubed,
// and β, the window kept after a loss.
const (
	cubicC    = 0.4
	cubicBeta = 0.7
	// cubicAlpha is the Reno-friendly region's growth, in segments a round
	// trip, which with β makes it as fair to NewReno as NewReno is to
	// itself (RFC 9438 section 4.3).
	cubicAlpha = 3 * (1 - cubicBeta) / (1 + cubicBeta)
	// persistentCongestionThreshold is how many probe timeouts, without
	// backoff, lost packets have to span for congestion to be persistent
	// (RFC 9002 section 7.6.1).
	persistentCongestionThreshold = 3
	// maxBurstLimited is how much of the window may be left unused, in
	// datagrams, before the sender counts as not using it: a round's last
	// datagram, or what pacing holds back, leave a little.
	maxBurstLimited = 3
)

// congestion is the connection's congestion controller: the window, with
// slow start and the recovery period (RFC 9002 section 7) common to both
// algorithms, and how the window grows in congestion avoidance and shrinks
// after a loss particular to each.
type congestion struct {
	algo                          CongestionControl
	cwnd, ssthresh, bytesInFlight uint64
	recoveryStart                 time.Time

	// CUBIC's state (RFC 9438 section 4): wMax is the window the last loss
	// came at, less after fast convergence; cwndPrior the window right
	// before the reduction; epoch the start of the congestion avoidance
	// stage, zero until its first acknowledgement, and k how long the curve
	// takes from the window then to wMax; wEst
	// the Reno-friendly estimate; and grown the fraction of a byte that
	// growth has not yet added to the window. Windows are in bytes.
	wMax, cwndPrior, wEst, grown float64
	epoch                        time.Time
	k                            float64
}

func newCongestion(algo CongestionControl) congestion {
	return congestion{algo: algo, cwnd: initialCongestionWindow, ssthresh: ^uint64(0)}
}

// resetPath starts the window afresh for a path that has nothing in common
// with the last one, keeping what is in flight on that one counted.
func (cc *congestion) resetPath(now time.Time) {
	*cc = congestion{algo: cc.algo, cwnd: initialCongestionWindow, ssthresh: ^uint64(0),
		bytesInFlight: cc.bytesInFlight, recoveryStart: now}
}

func (cc *congestion) canSend() bool { return cc.bytesInFlight+maxDatagram <= cc.cwnd }

// limited reports whether a sender that had prior bytes in flight when an
// acknowledgement came was using the window, which only then grows: a window
// grown while the application had less to send than it allows would let a
// burst of all of it go at once, later, on the strength of nothing (RFC 9002
// section 7.8). In slow start, half the window is enough, since it doubles
// a round trip; and a sender that pacing held back was not the one holding
// back.
func (cc *congestion) limited(prior uint64, paced bool) bool {
	if paced || prior+maxBurstLimited*maxDatagram >= cc.cwnd {
		return true
	}
	return cc.cwnd < cc.ssthresh && prior > cc.cwnd/2
}

// onAcked takes a packet in flight off the count, and grows the window if
// the packet was sent after the last recovery period began and the window
// was in use; rtt is the smoothed round-trip time.
func (cc *congestion) onAcked(p *sentPacket, now time.Time, rtt time.Duration, limited bool) {
	cc.bytesInFlight -= uint64(p.size)
	if !p.time.After(cc.recoveryStart) {
		return
	}
	if !limited {
		// CUBIC's curve is a function of time, which assumes the window in
		// use all along: the stage starts again once it is (RFC 9438
		// section 5.8).
		cc.epoch = time.Time{}
		return
	}
	if cc.cwnd < cc.ssthresh {
		cc.cwnd += uint64(p.size)
		return
	}
	if cc.algo == NewReno {
		cc.cwnd += maxDatagram * uint64(p.size) / cc.cwnd
		return
	}
	cc.cubicAcked(float64(p.size), now, rtt)
}

// cubicTarget is W_cubic(t) of RFC 9438's figure 1, in bytes, t seconds
// into the congestion avoidance stage.
func (cc *congestion) cubicTarget(t float64) float64 {
	d := t - cc.k
	return cubicC*maxDatagram*d*d*d + cc.wMax
}

// cubicAcked grows the window in congestion avoidance for acked bytes
// (RFC 9438 sections 4.2 to 4.4).
func (cc *congestion) cubicAcked(acked float64, now time.Time, rtt time.Duration) {
	cwnd := float64(cc.cwnd)
	if cc.epoch.IsZero() {
		cc.epoch, cc.wEst = now, cwnd
		if cc.wMax <= cwnd {
			// Past the plateau already, out of slow start or after a
			// timeout: the curve starts at its inflection, here.
			cc.wMax, cc.k = cwnd, 0
		} else {
			cc.k = math.Cbrt((cc.wMax - cwnd) / (cubicC * maxDatagram))
		}
	}
	t := now.Sub(cc.epoch).Seconds()
	// The Reno-friendly estimate grows by α segments a round trip, and by
	// one, NewReno's rate, once it is past the window the loss came at.
	alpha := cubicAlpha
	if cc.wEst >= cc.cwndPrior {
		alpha = 1
	}
	cc.wEst += alpha * maxDatagram * acked / cwnd
	var grow float64
	if cc.cubicTarget(t) < cc.wEst {
		grow = cc.wEst - cwnd
	} else {
		// Concave or convex region: towards where the curve is a round
		// trip from now, by at most half the window a round trip.
		target := min(max(cc.cubicTarget(t+rtt.Seconds()), cwnd), 1.5*cwnd)
		grow = (target - cwnd) * acked / cwnd
	}
	if grow <= 0 {
		return
	}
	cc.grown += grow
	whole := math.Floor(cc.grown)
	cc.cwnd += uint64(whole)
	cc.grown -= whole
}

// onCongestion starts a recovery period for a loss of a packet sent at
// sent, unless one began since: the window shrinks once per round trip of
// losses.
func (cc *congestion) onCongestion(sent, now time.Time) {
	if !sent.After(cc.recoveryStart) {
		return
	}
	cc.recoveryStart = now
	if cc.algo == NewReno {
		cc.ssthresh = max(cc.cwnd/2, minCongestionWindow)
		cc.cwnd = cc.ssthresh
		return
	}
	cwnd := float64(cc.cwnd)
	cc.cwndPrior = cwnd
	if cwnd < cc.wMax {
		// Fast convergence: a window lost below the last plateau gives
		// room to a flow that has just come, and the next plateau is lower
		// still (RFC 9438 section 4.7).
		cc.wMax = cwnd * (1 + cubicBeta) / 2
	} else {
		cc.wMax = cwnd
	}
	cc.ssthresh = max(uint64(cwnd*cubicBeta), minCongestionWindow)
	cc.cwnd = cc.ssthresh
	cc.epoch, cc.grown = time.Time{}, 0
}

// onPersistentCongestion collapses the window to the minimum, as a timeout
// would: nothing in flight for the length of several probe timeouts got
// through (RFC 9002 section 7.6.2). Slow start takes it back up to the
// threshold the losses set, and CUBIC's curve starts afresh from there
// (RFC 9438 section 4.8).
func (cc *congestion) onPersistentCongestion() {
	cc.cwnd = minCongestionWindow
	cc.recoveryStart = time.Time{}
	cc.wMax, cc.epoch, cc.grown = 0, time.Time{}, 0
}

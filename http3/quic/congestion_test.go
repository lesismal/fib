package quic

import (
	"math"
	"testing"
	"time"
)

// segs is n datagrams' worth of window, in bytes.
func segs(n float64) uint64 { return uint64(n * maxDatagram) }

// ackSegment acknowledges one full datagram sent just before now.
func ackSegment(cc *congestion, now time.Time, rtt time.Duration) {
	cc.bytesInFlight += maxDatagram
	cc.onAcked(&sentPacket{size: maxDatagram, time: now.Add(-time.Microsecond)}, now, rtt, true)
}

// A loss leaves CUBIC 0.7 of its window, with that as the threshold and the
// window the loss came at as the plateau; a loss below the plateau lowers
// it further, to halfway between the window and 0.7 of it (fast
// convergence); NewReno halves. Each shrinks once a recovery period.
func TestCubicReduction(t *testing.T) {
	now := time.Now()
	cc := newCongestion(CUBIC)
	cc.cwnd, cc.ssthresh = segs(100), segs(50)
	cc.onCongestion(now.Add(-time.Millisecond), now)
	if cc.cwnd != segs(70) || cc.ssthresh != segs(70) || cc.wMax != float64(segs(100)) || cc.cwndPrior != float64(segs(100)) {
		t.Fatalf("after a loss at 100 segments: cwnd %d, ssthresh %d, wMax %v", cc.cwnd, cc.ssthresh, cc.wMax)
	}
	cc.onCongestion(now.Add(-time.Millisecond), now.Add(time.Millisecond))
	if cc.cwnd != segs(70) {
		t.Fatalf("a loss from before the recovery period shrank the window to %d", cc.cwnd)
	}
	cc.cwnd = segs(80)
	later := now.Add(time.Second)
	cc.onCongestion(later.Add(-time.Millisecond), later)
	if want := float64(segs(80)) * 1.7 / 2; cc.wMax != want || cc.cwnd != segs(56) {
		t.Fatalf("after a loss below the plateau: wMax %v, want %v; cwnd %d, want %d", cc.wMax, want, cc.cwnd, segs(56))
	}
	cc.cwnd = segs(2)
	cc.onCongestion(later.Add(time.Millisecond), later.Add(2*time.Millisecond))
	if cc.cwnd != minCongestionWindow {
		t.Fatalf("window %d below the minimum", cc.cwnd)
	}

	nr := newCongestion(NewReno)
	nr.cwnd = segs(100)
	nr.onCongestion(now.Add(-time.Millisecond), now)
	if nr.cwnd != segs(50) || nr.ssthresh != segs(50) {
		t.Fatalf("NewReno after a loss: cwnd %d, ssthresh %d", nr.cwnd, nr.ssthresh)
	}
}

// In congestion avoidance CUBIC's window follows W_cubic(t) = C(t-K)^3 +
// W_max (RFC 9438 figure 1), with K = cbrt((W_max - cwnd_epoch)/C) (figure
// 2), growing each acknowledgement by (W_cubic(t+RTT) - cwnd)/cwnd segments
// for each one acknowledged (section 4.4), and reaches the plateau K
// seconds after the reduction.
func TestCubicGrowth(t *testing.T) {
	const rtt = 100 * time.Millisecond
	start := time.Now()
	cc := newCongestion(CUBIC)
	cc.cwnd = segs(100)
	cc.onCongestion(start.Add(-time.Millisecond), start)
	ackSegment(&cc, start.Add(time.Millisecond), rtt)
	k := math.Cbrt((100 - 70) / cubicC)
	if math.Abs(cc.k-k) > 1e-9 {
		t.Fatalf("K %v, want %v", cc.k, k)
	}
	// One acknowledgement a second into the stage.
	at := cc.epoch.Add(time.Second)
	cwnd := float64(cc.cwnd)
	wCubic := func(t float64) float64 { return (cubicC*math.Pow(t-k, 3) + 100) * maxDatagram }
	want := cwnd + cc.grown + (wCubic(1+rtt.Seconds())-cwnd)*maxDatagram/cwnd
	ackSegment(&cc, at, rtt)
	if got := float64(cc.cwnd) + cc.grown; math.Abs(got-want) > 1e-6 {
		t.Fatalf("window %v a second in, want %v", got, want)
	}
	// From the reduction again, a window's worth acknowledged every round
	// trip until K: the window
	// follows the curve, a few segments behind it at most, since each
	// acknowledgement closes 1/cwnd of the gap to where it is a round trip
	// on; and it reaches the plateau at K, never past it.
	cc = newCongestion(CUBIC)
	cc.cwnd = segs(100)
	cc.onCongestion(start.Add(-time.Millisecond), start)
	ackSegment(&cc, start.Add(time.Millisecond), rtt)
	for now := cc.epoch; now.Sub(cc.epoch) < time.Duration(k*float64(time.Second)); now = now.Add(rtt) {
		for n := cc.cwnd / maxDatagram; n > 0; n-- {
			ackSegment(&cc, now, rtt)
		}
		elapsed := now.Sub(cc.epoch).Seconds()
		if w := float64(cc.cwnd); w > wCubic(elapsed+rtt.Seconds())+maxDatagram || w < wCubic(elapsed)-4*maxDatagram {
			t.Fatalf("window %.0f %.2fs in, off the curve at %.0f", w, elapsed, wCubic(elapsed))
		}
	}
	if w := float64(cc.cwnd); w > float64(segs(100))+maxDatagram || w < float64(segs(98)) {
		t.Fatalf("window %.1f segments at K, want the plateau of 100", w/maxDatagram)
	}
	// Past the plateau it probes ever faster (the convex region), but by at
	// most half the window a round trip.
	before := cc.cwnd
	now := cc.epoch.Add(time.Duration((k + 3) * float64(time.Second)))
	for n := cc.cwnd / maxDatagram; n > 0; n-- {
		ackSegment(&cc, now, rtt)
	}
	if cc.cwnd <= before || cc.cwnd > before*3/2+maxDatagram {
		t.Fatalf("window %d a round trip past the plateau, from %d", cc.cwnd, before)
	}
}

// Where the curve is slower than NewReno would be, which on a short round
// trip it is, the window follows W_est, which grows α = 3(1-β)/(1+β)
// segments a round trip until it reaches the window the loss came at and
// one a round trip after that (RFC 9438 section 4.3).
func TestCubicRenoFriendly(t *testing.T) {
	const rtt = time.Millisecond
	start := time.Now()
	cc := newCongestion(CUBIC)
	cc.cwnd = segs(100)
	cc.onCongestion(start.Add(-time.Millisecond), start)
	// After a timeout-like start (the plateau below the window) the curve
	// begins at its inflection, flat.
	cc.wMax = 0
	now := start.Add(time.Millisecond)
	ackSegment(&cc, now, rtt)
	if cc.k != 0 || cc.wMax != float64(segs(70)) {
		t.Fatalf("K %v, plateau %v from a window of %d", cc.k, cc.wMax, cc.cwnd)
	}
	from := float64(segs(70))
	for n := 1; n < 70; n++ {
		ackSegment(&cc, now, rtt)
	}
	// 70 segments acknowledged at a window of about 70 grow W_est by about α.
	if got := (float64(cc.cwnd) + cc.grown - from) / maxDatagram; math.Abs(got-cubicAlpha) > 0.02 {
		t.Fatalf("grew %.3f segments in a round trip, want α = %.3f", got, cubicAlpha)
	}
	cc.cwndPrior = cc.wEst
	before := float64(cc.cwnd) + cc.grown
	for n := cc.cwnd / maxDatagram; n > 0; n-- {
		ackSegment(&cc, now, rtt)
	}
	if got := (float64(cc.cwnd) + cc.grown - before) / maxDatagram; math.Abs(got-1) > 0.02 {
		t.Fatalf("grew %.3f segments in a round trip past the old window, want 1", got)
	}
}

// The window grows only while it is in use: in congestion avoidance, within
// a few datagrams of full, or held back by pacing; in slow start, half full.
// An acknowledgement while it is not starts CUBIC's stage again.
func TestCongestionAppLimited(t *testing.T) {
	now := time.Now()
	for _, algo := range []CongestionControl{CUBIC, NewReno} {
		cc := newCongestion(algo)
		cc.cwnd, cc.ssthresh = segs(100), segs(50)
		if cc.limited(segs(50), false) || !cc.limited(segs(97), false) || !cc.limited(segs(10), true) {
			t.Fatalf("%v: limited wrong in congestion avoidance", algo)
		}
		cc.epoch = now
		before := cc.cwnd
		cc.bytesInFlight = segs(10)
		cc.onAcked(&sentPacket{size: maxDatagram, time: now}, now.Add(time.Millisecond), time.Millisecond, cc.limited(segs(10), false))
		if cc.cwnd != before || !cc.epoch.IsZero() {
			t.Fatalf("%v: window grew to %d (epoch %v) with a tenth of it in use", algo, cc.cwnd, cc.epoch)
		}
		cc.ssthresh = ^uint64(0)
		if !cc.limited(segs(51), false) || cc.limited(segs(49), false) {
			t.Fatalf("%v: limited wrong in slow start", algo)
		}
		cc.bytesInFlight = segs(60)
		cc.onAcked(&sentPacket{size: maxDatagram, time: now}, now.Add(time.Millisecond), time.Millisecond, cc.limited(segs(60), false))
		if cc.cwnd != before+maxDatagram {
			t.Fatalf("%v: slow start grew the window to %d", algo, cc.cwnd)
		}
	}
}

// Packets lost over more than three probe timeouts, with none acknowledged
// between them and the first sent after the first round-trip sample, are
// persistent congestion, and the window collapses to the minimum (RFC 9002
// section 7.6).
func TestPersistentCongestion(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.rtt = rttStats{smoothed: 10 * time.Millisecond, rttvar: 2 * time.Millisecond, hasSample: true, firstAt: now}
	span := (c.rtt.smoothed + 4*c.rtt.rttvar + c.peerMaxAckDelay()) * persistentCongestionThreshold
	lostAt := func(pns ...uint64) []*sentPacket {
		var lost []*sentPacket
		for i, pn := range pns {
			at := now.Add(time.Millisecond + time.Duration(i)*(span/time.Duration(len(pns)-1)+time.Millisecond))
			lost = append(lost, &sentPacket{pn: pn, time: at, ackEliciting: true, inFlight: true, size: maxDatagram})
		}
		return lost
	}
	if !c.persistentCongestion(lostAt(5, 6, 7, 8)) {
		t.Fatal("a run of losses over the span was not persistent congestion")
	}
	if c.persistentCongestion(lostAt(5, 6, 8, 9)) {
		t.Fatal("losses with a packet acknowledged between them were persistent congestion")
	}
	short := lostAt(5, 6)
	short[1].time = short[0].time.Add(span - time.Millisecond)
	if c.persistentCongestion(short) {
		t.Fatal("losses within the span were persistent congestion")
	}
	early := lostAt(5, 6, 7)
	early[0].time = now.Add(-time.Second)
	early[1].time = now.Add(-time.Second)
	if c.persistentCongestion(early) {
		t.Fatal("losses sent before the first sample were persistent congestion")
	}
	c.rtt.hasSample = false
	if c.persistentCongestion(lostAt(5, 6, 7, 8)) {
		t.Fatal("persistent congestion without a round-trip sample")
	}

	// Through loss detection: the window collapses.
	c.rtt.hasSample = true
	s := &c.spaces[spaceApp]
	c.cc.cwnd = segs(100)
	inFlight := c.cc.bytesInFlight
	lost := lostAt(0, 1, 2, 3)
	for _, p := range lost {
		c.onPacketSent(spaceApp, p)
	}
	s.nextPN = 5
	s.largestAcked = 4
	c.detectLost(spaceApp, lost[3].time.Add(time.Second))
	if c.cc.cwnd != minCongestionWindow || c.cc.bytesInFlight != inFlight {
		t.Fatalf("window %d, %d bytes in flight after persistent congestion", c.cc.cwnd, c.cc.bytesInFlight)
	}
}

// Config.CongestionControl picks the controller, CUBIC by default.
func TestCongestionControlConfig(t *testing.T) {
	for _, algo := range []CongestionControl{CUBIC, NewReno} {
		c := newConn(fuzzPacketConn{}, testAddr, Config{CongestionControl: algo}, fuzzHandler{}, true)
		if c.cc.algo != algo || c.cc.cwnd != initialCongestionWindow {
			t.Fatalf("asked for %v, got %v with a window of %d", algo, c.cc.algo, c.cc.cwnd)
		}
	}
}

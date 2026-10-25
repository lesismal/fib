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

// The window is bytes, counted in the path's datagram size: RFC 9002
// section 7.2's initial window is ten datagrams, 14720 bytes at most and two
// datagrams at least, and the minimum window is two. A path known to carry
// 1452-byte datagrams starts with 14520 bytes rather than the 12000 a
// 1200-byte unit would give it, and a window chosen in bytes keeps those
// bytes when the path carries larger datagrams.
func TestWindowInBytes(t *testing.T) {
	for _, tc := range []struct {
		size             int
		initial, minimum uint64
	}{
		{0, 12000, 2400}, // no size yet is the 1200 bytes every path carries
		{maxDatagram, 12000, 2400},
		{1452, 14520, 2904},
		{1500, 14720, 3000}, // ten datagrams would be more than RFC 9002's cap
	} {
		cc := newCongestion(CUBIC, tc.size)
		wantSize := uint64(max(tc.size, maxDatagram))
		if cc.cwnd != tc.initial || cc.maxDatagramSize != wantSize {
			t.Errorf("%d-byte datagrams: window %d of %d-byte units, want %d of %d",
				tc.size, cc.cwnd, cc.maxDatagramSize, tc.initial, wantSize)
		}
		if got := minWindow(tc.size); got != tc.minimum {
			t.Errorf("%d-byte datagrams: minimum window %d, want %d", tc.size, got, tc.minimum)
		}
	}
	// A window the controller chose keeps its bytes as the path grows; only
	// one under the new minimum is raised to it.
	cc := newCongestion(NewReno, maxDatagram)
	cc.cwnd = 2400
	cc.setMaxDatagramSize(1452)
	if cc.cwnd != 2904 || cc.maxDatagramSize != 1452 {
		t.Fatalf("a 2400-byte window became %d in %d-byte units, want the 2904 minimum",
			cc.cwnd, cc.maxDatagramSize)
	}
	cc.cwnd = 20000
	cc.setMaxDatagramSize(1452)
	if cc.cwnd != 20000 {
		t.Fatalf("a 20000-byte window became %d when the datagram size grew", cc.cwnd)
	}
	cc.setMaxDatagramSize(0)
	if cc.maxDatagramSize != maxDatagram {
		t.Fatalf("a zero datagram size counted the window in %d bytes", cc.maxDatagramSize)
	}
	// Ten datagrams of the path's size fit exactly in the initial window,
	// and an eleventh does not: what is in flight is bytes.
	cc = newCongestion(CUBIC, 1452)
	cc.bytesInFlight = 9 * 1452
	if !cc.canSend() {
		t.Fatal("the tenth 1452-byte datagram did not fit in 14520 bytes")
	}
	cc.bytesInFlight += 1452
	if cc.canSend() {
		t.Fatal("an eleventh 1452-byte datagram fit in 14520 bytes")
	}
}

// NewReno's congestion-avoidance increase is one datagram of the path's size
// a round trip (RFC 9002 section 7.3.1): a window's worth acknowledged in
// 1452-byte datagrams grows the window by 1452 bytes, not the 1200 of the
// size every path carries.
func TestNewRenoGrowsByThePathsDatagram(t *testing.T) {
	for _, size := range []int{maxDatagram, 1452} {
		now := time.Now()
		cc := newCongestion(NewReno, size)
		cc.cwnd = uint64(20 * size)
		cc.ssthresh = cc.cwnd / 2
		cc.bytesInFlight = cc.cwnd
		before := cc.cwnd
		cc.onAcked(&sentPacket{size: int(cc.cwnd), time: now}, now, time.Millisecond, true)
		if got := cc.cwnd - before; got != uint64(size) {
			t.Fatalf("%d-byte datagrams: a window acknowledged grew the window by %d, want %d",
				size, got, size)
		}
	}
}

// CUBIC's β and its curve are in the same byte window: a loss leaves 0.7 of
// the bytes whatever the datagram size, K is still the cube root of the
// datagrams the window is below the plateau, and W_est grows by α datagrams
// of the path's size a round trip (RFC 9438 sections 4.2 and 4.3).
func TestCubicWindowInBytes(t *testing.T) {
	for _, size := range []int{maxDatagram, 1452} {
		now := time.Now()
		cc := newCongestion(CUBIC, size)
		cc.cwnd = uint64(100 * size)
		cc.onCongestion(now.Add(-time.Millisecond), now)
		if want := uint64(70 * size); cc.cwnd != want || cc.ssthresh != want {
			t.Fatalf("%d-byte datagrams: window %d and threshold %d after a loss at %d, want %d",
				size, cc.cwnd, cc.ssthresh, 100*size, want)
		}
		// 30 datagrams below the plateau: K is where it was in datagrams.
		at := now.Add(time.Millisecond)
		cc.bytesInFlight = cc.cwnd
		cc.onAcked(&sentPacket{size: int(cc.cwnd), time: at}, at, time.Millisecond, true)
		if want := math.Cbrt(30 / cubicC); math.Abs(cc.k-want) > 1e-9 {
			t.Errorf("%d-byte datagrams: K %v, want %v", size, cc.k, want)
		}
		// The Reno-friendly estimate below the window the loss came at
		// grows by α datagrams of the path's size a round trip.
		cc = newCongestion(CUBIC, size)
		cc.cwnd = uint64(100 * size)
		cc.onCongestion(now.Add(-time.Millisecond), now)
		cc.cwndPrior = float64(200 * size)
		cc.cwnd = uint64(100 * size)
		cc.bytesInFlight = cc.cwnd
		from := float64(cc.cwnd)
		cc.onAcked(&sentPacket{size: int(cc.cwnd), time: at}, at, time.Millisecond, true)
		if got, want := cc.wEst-from, cubicAlpha*float64(size); math.Abs(got-want) > 1e-6 {
			t.Errorf("%d-byte datagrams: W_est grew %.3f bytes a round trip, want α*%d = %.3f",
				size, got, size, want)
		}
	}
}

// A loss leaves CUBIC 0.7 of its window, with that as the threshold and the
// window the loss came at as the plateau; a loss below the plateau lowers it
// further, to halfway between the window and 0.7 of it (fast
// convergence); NewReno halves. Each shrinks once a recovery period.
func TestCubicReduction(t *testing.T) {
	now := time.Now()
	cc := newCongestion(CUBIC, maxDatagram)
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
	if cc.cwnd != minWindow(maxDatagram) {
		t.Fatalf("window %d below the minimum", cc.cwnd)
	}

	nr := newCongestion(NewReno, maxDatagram)
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
	cc := newCongestion(CUBIC, maxDatagram)
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
	cc = newCongestion(CUBIC, maxDatagram)
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
	cc := newCongestion(CUBIC, maxDatagram)
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
		cc := newCongestion(algo, maxDatagram)
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
	if c.cc.cwnd != minWindow(maxDatagram) || c.cc.bytesInFlight != inFlight {
		t.Fatalf("window %d, %d bytes in flight after persistent congestion", c.cc.cwnd, c.cc.bytesInFlight)
	}
}

// Persistent congestion is a matter of packet numbers and time, whatever
// sizes the lost packets were, and the window it collapses to is the minimum
// for the datagram size the path now carries: 2904 bytes at 1452, and 2400
// once a black hole has put the connection back on 1200 (RFC 9002 sections
// 7.2 and 7.6).
func TestPersistentCongestionMixedDatagramSizes(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.setMaxDatagramLocked(1452)
	c.rtt = rttStats{smoothed: 10 * time.Millisecond, rttvar: 2 * time.Millisecond, hasSample: true, firstAt: now}
	span := (c.rtt.smoothed + 4*c.rtt.rttvar + c.peerMaxAckDelay()) * persistentCongestionThreshold
	sizes := []int{1200, 1452, 1200, 1452}
	var lost []*sentPacket
	for i, size := range sizes {
		at := now.Add(time.Millisecond + time.Duration(i)*(span/3+time.Millisecond))
		lost = append(lost, &sentPacket{pn: uint64(i), time: at, ackEliciting: true, inFlight: true, size: size})
	}
	if !c.persistentCongestion(lost) {
		t.Fatal("losses of mixed datagram sizes over the span were not persistent congestion")
	}
	s := &c.spaces[spaceApp]
	c.cc.cwnd = 100 * 1452
	for _, p := range lost {
		c.onPacketSent(spaceApp, p)
	}
	s.nextPN, s.largestAcked = uint64(len(lost)), int64(len(lost))-1
	c.detectLost(spaceApp, lost[len(lost)-1].time.Add(time.Second))
	if want := minWindow(1452); c.cc.cwnd != want {
		t.Fatalf("window %d after persistent congestion in 1452-byte datagrams, want %d", c.cc.cwnd, want)
	}
	// A black hole takes the path back to 1200: the minimum follows it.
	c.setMaxDatagramLocked(maxDatagram)
	c.cc.onPersistentCongestion()
	if want := minWindow(maxDatagram); c.cc.cwnd != want {
		t.Fatalf("window %d after persistent congestion in 1200-byte datagrams, want %d", c.cc.cwnd, want)
	}
}

// Config.CongestionControl picks the controller, CUBIC by default.
func TestCongestionControlConfig(t *testing.T) {
	for _, algo := range []CongestionControl{CUBIC, NewReno} {
		c := newConn(fuzzPacketConn{}, testAddr, Config{CongestionControl: algo}, fuzzHandler{}, true)
		if c.cc.algo != algo || c.cc.cwnd != initialWindow(maxDatagram) {
			t.Fatalf("asked for %v, got %v with a window of %d", algo, c.cc.algo, c.cc.cwnd)
		}
	}
}

// hsStart is a connection in the initial slow start, with HyStart++'s state
// set as a test needs it.
func hsStart() congestion {
	cc := newCongestion(CUBIC, maxDatagram)
	cc.cwnd, cc.ssthresh = segs(10), ^uint64(0)
	cc.hy = hystart{started: true, roundEnd: 1000}
	return cc
}

// hsSample gives HyStart++ one round trip sample, with the round kept open:
// the largest packet acknowledged is well below the round's end.
func hsSample(cc *congestion, rtt time.Duration) {
	cc.hystartAck(rtt, true, 1, 2000)
}

// hystartThreshold is the last round's minimum round trip over the divisor,
// between 4 and 16 milliseconds (RFC 9406 section 4.3).
func TestHyStartThreshold(t *testing.T) {
	for _, tc := range []struct{ last, want time.Duration }{
		{time.Millisecond, 4 * time.Millisecond},
		{16 * time.Millisecond, 4 * time.Millisecond},
		{40 * time.Millisecond, 5 * time.Millisecond},
		{100 * time.Millisecond, 12500 * time.Microsecond},
		{time.Second, 16 * time.Millisecond},
	} {
		if got := hystartThreshold(tc.last); got != tc.want {
			t.Errorf("threshold of a %v round trip = %v, want %v", tc.last, got, tc.want)
		}
	}
}

// A round's samples are judged against the minimum round trip of the round
// before, and only once the round has enough of them: fewer do not end slow
// start however slow they are, and a rise under the threshold does not
// either.
func TestHyStartNeedsEnoughSamplesAndADelay(t *testing.T) {
	for _, tc := range []struct {
		name    string
		last    time.Duration
		samples int
		sample  time.Duration
		want    bool
	}{
		{"a delay, with the samples", 10 * time.Millisecond, hystartSamples, 15 * time.Millisecond, true},
		{"too few samples", 10 * time.Millisecond, hystartSamples - 1, time.Second, false},
		{"a delay under the threshold", 10 * time.Millisecond, hystartSamples, 13999 * time.Microsecond, false},
		{"a delay at the threshold", 10 * time.Millisecond, hystartSamples, 14 * time.Millisecond, true},
	} {
		cc := hsStart()
		cc.hy.lastMin = tc.last
		for i := 0; i < tc.samples; i++ {
			hsSample(&cc, tc.sample)
		}
		if got := cc.hy.inCSS; got != tc.want {
			t.Errorf("%s: conservative phase %v, want %v", tc.name, got, tc.want)
		}
		if tc.want && (!cc.hy.inCSS || cc.hy.baseline != tc.sample || cc.hy.cssRounds != 1) {
			t.Errorf("%s: baseline %v, rounds %d", tc.name, cc.hy.baseline, cc.hy.cssRounds)
		}
	}
}

// A round ends when the packet it was sent up to is acknowledged: the round
// minimum becomes the last round's, and the samples start over (RFC 9406
// section 4.2).
func TestHyStartRoundEnd(t *testing.T) {
	cc := hsStart()
	cc.hy.roundEnd = 10
	for i := 0; i < hystartSamples; i++ {
		hsSample(&cc, 10*time.Millisecond)
	}
	if cc.hy.samples != hystartSamples || cc.hy.lastMin != 0 {
		t.Fatalf("before the round ended: samples %d, last minimum %v", cc.hy.samples, cc.hy.lastMin)
	}
	// The packet the round was sent up to, with the next round's packets
	// already sent.
	cc.hystartAck(10*time.Millisecond, true, 10, 20)
	if cc.hy.lastMin != 10*time.Millisecond || cc.hy.samples != 1 || cc.hy.roundEnd != 19 {
		t.Fatalf("after the round ended: last minimum %v, samples %d, round end %d",
			cc.hy.lastMin, cc.hy.samples, cc.hy.roundEnd)
	}
}

// The conservative phase grows by a quarter of the bytes slow start would,
// keeping the fraction of a byte not yet added rather than losing it or
// adding it twice.
func TestHyStartConservativeGrowth(t *testing.T) {
	cc := hsStart()
	cc.hy.inCSS = true
	before := cc.cwnd
	for i := 0; i < 2; i++ {
		cc.bytesInFlight += 1002
		cc.onAcked(&sentPacket{size: 1002, time: time.Now()}, time.Now(), time.Millisecond, true)
	}
	if got := cc.cwnd - before; got != 501 {
		t.Fatalf("two 1002-byte acknowledgements grew the window by %d, want 501", got)
	}
}

// A round whose minimum round trip is back under the baseline means the
// delay was jitter, not a full path: slow start resumes, and grows by whole
// packets again.
func TestHyStartResumesSlowStart(t *testing.T) {
	cc := hsStart()
	cc.hy.inCSS, cc.hy.cssRounds, cc.hy.baseline = true, 2, 15*time.Millisecond
	for i := 0; i < hystartSamples; i++ {
		hsSample(&cc, 10*time.Millisecond)
	}
	if cc.hy.inCSS || cc.hy.baseline != 0 {
		t.Fatalf("slow start did not resume: inCSS %v, baseline %v", cc.hy.inCSS, cc.hy.baseline)
	}
	before := cc.cwnd
	cc.bytesInFlight += maxDatagram
	cc.onAcked(&sentPacket{size: maxDatagram, time: time.Now()}, time.Now(), time.Millisecond, true)
	if got := cc.cwnd - before; got != maxDatagram {
		t.Fatalf("slow start grew by %d, want a whole packet's %d", got, maxDatagram)
	}
}

// The conservative phase lasts at most five rounds, the one it began in
// included, and then the window is the threshold: congestion avoidance, with
// CUBIC's curve starting at the window as its plateau.
func TestHyStartExitsAfterFiveRounds(t *testing.T) {
	cc := hsStart()
	cc.hy.roundEnd, cc.hy.lastMin, cc.hy.curMin = 10, 10*time.Millisecond, 15*time.Millisecond
	cc.hy.inCSS, cc.hy.cssRounds, cc.hy.baseline, cc.hy.samples = true, hystartRounds, 15*time.Millisecond, hystartSamples
	// The round it began in ended, and that is one more than it allows.
	cc.hystartAck(15*time.Millisecond, true, 10, 20)
	if cc.hy.inCSS {
		t.Fatalf("the conservative phase went on for %d rounds", cc.hy.cssRounds)
	}
	if cc.ssthresh != cc.cwnd {
		t.Fatalf("threshold %d, window %d: congestion avoidance did not begin", cc.ssthresh, cc.cwnd)
	}
	if cc.wMax != float64(cc.cwnd) {
		t.Fatalf("CUBIC's plateau %v, want the window %d", cc.wMax, cc.cwnd)
	}
}

// A loss sets the threshold, and HyStart++ has no part after it: loss
// recovery's threshold is the exit point then (RFC 9406 section 4.3).
func TestHyStartOffAfterLoss(t *testing.T) {
	cc := hsStart()
	cc.onCongestion(time.Now().Add(-time.Millisecond), time.Now())
	roundEnd, lastMin := cc.hy.roundEnd, cc.hy.lastMin
	for i := 0; i < 2*hystartSamples; i++ {
		hsSample(&cc, 100*time.Millisecond)
	}
	if cc.hy.inCSS || cc.hy.roundEnd != roundEnd || cc.hy.lastMin != lastMin {
		t.Fatalf("HyStart++ went on after a loss: inCSS %v, round end %d, last minimum %v",
			cc.hy.inCSS, cc.hy.roundEnd, cc.hy.lastMin)
	}
}

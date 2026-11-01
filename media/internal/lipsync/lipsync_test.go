package lipsync

import (
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/webrtc/rtp"
)

func testSR(t *testing.T, wall time.Time, ts uint32) []byte {
	t.Helper()
	b, err := rtp.AppendRTCP(nil,
		&rtp.SenderReport{SSRC: 1, NTPTime: rtp.NTPTime(wall), RTPTime: ts},
		&rtp.SourceDescription{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestSessionOrigin(t *testing.T) {
	wall := time.Unix(1700000000, 0)
	var none Clock
	var l Session
	if at := l.Origin(none, 5, 8000, 7*time.Second); at != 7*time.Second || l.set {
		t.Fatal("no report: fallback, no anchor", at)
	}
	a := Clock{OK: true, NTP: wall, RTP: 1000}
	if at := l.Origin(a, 1000, 0, time.Second); at != time.Second || l.set {
		t.Fatal("no clock rate: fallback", at)
	}
	// The first stream with a report anchors at its fallback.
	if at := l.Origin(a, 1000+8000, 8000, 3*time.Second); at != 3*time.Second || !l.set {
		t.Fatal("anchor", at)
	}
	// RTP timestamps before the report's, and wrapping, count backwards.
	b := Clock{OK: true, NTP: wall.Add(500 * time.Millisecond), RTP: 10}
	if at := l.Origin(b, 10-90000+1<<32, 90000, 9*time.Second); at != 1500*time.Millisecond {
		t.Fatal("placed", at)
	}
	// Reports placing a stream before zero or too far off are ignored.
	far := Clock{OK: true, NTP: wall.Add(-time.Hour), RTP: 0}
	if at := l.Origin(far, 0, 8000, time.Second); at != time.Second {
		t.Fatal("before zero", at)
	}
	late := Clock{OK: true, NTP: wall.Add(time.Hour), RTP: 0}
	if at := l.Origin(late, 0, 8000, time.Second); at != time.Second {
		t.Fatal("too late", at)
	}
	early := Clock{OK: true, NTP: wall.Add(-time.Second), RTP: 0}
	if at := l.Origin(early, 0, 8000, 20*time.Second); at != 20*time.Second {
		t.Fatal("too early", at)
	}
}

func TestClockUpdate(t *testing.T) {
	var c Clock
	rr, _ := rtp.AppendRTCP(nil, &rtp.ReceiverReport{SSRC: 1})
	c.Update(rr)
	c.Update([]byte{0x80, 200, 0, 9})             // length past the end
	c.Update([]byte{0x80, 200, 0, 1, 0, 0, 0, 1}) // too short a report
	zero, _ := rtp.AppendRTCP(nil, &rtp.SenderReport{SSRC: 1})
	c.Update(zero)
	if c.OK {
		t.Fatal("no sender report taken", c)
	}
	wall := time.Unix(1700000000, 0)
	c.Update(append(rr, testSR(t, wall, 77)...))
	if !c.OK || c.RTP != 77 || !c.NTP.Equal(wall) {
		t.Fatal(c)
	}
}

// simStream is one stream of a simulated sender: frames of dur sampled
// from wall time w0 on, the first with RTP timestamp ts0, arriving delay
// late plus a jitter of up to 15 ms.
type simStream struct {
	rate  int
	dur   time.Duration
	ts0   uint32
	delay time.Duration
	tr    Track
	// srEvery is how often the sender reports, from srFrom on; zero for
	// never.
	srEvery, srFrom time.Duration
	n               int
	nextSR          time.Duration
	// The depacketizer: the first packet's timestamp is media time origin.
	started bool
	ts      uint32
	ext     int64
	origin  time.Duration
	lastDTS time.Duration
	out     map[time.Duration]time.Duration // sample time -> PTS out
}

func (s *simStream) tsOf(at time.Duration) uint32 {
	return s.ts0 + uint32(av.ToTicks(at, s.rate))
}

// event is the next thing the stream sends, at sender time at: a frame,
// or a report when sr.
func (s *simStream) next() (at time.Duration, sr bool) {
	frame := time.Duration(s.n) * s.dur
	if s.srEvery > 0 && s.nextSR < frame {
		return s.nextSR, true
	}
	return frame, false
}

// simulate runs the streams for span of sender time, the wall clock of
// the reports jumping by jump at jumpAt, and checks each stream's times go
// forward.
func simulate(t *testing.T, l *Live, span time.Duration, jump, jumpAt time.Duration, streams ...*simStream) {
	t.Helper()
	w0 := time.Unix(1700000000, 0)
	for _, s := range streams {
		s.nextSR = s.srFrom
		s.out = make(map[time.Duration]time.Duration)
	}
	type ev struct {
		arrive time.Duration
		s      *simStream
		at     time.Duration
		sr     bool
	}
	var evs []ev
	for _, s := range streams {
		for {
			at, sr := s.next()
			if at > span {
				break
			}
			arrive := at
			if sr {
				s.nextSR += s.srEvery
			} else {
				s.n++
				arrive += s.delay + time.Duration(uint32(s.n)*2654435761%15)*time.Millisecond
			}
			evs = append(evs, ev{arrive, s, at, sr})
		}
	}
	// In order of arrival, as a reorder buffer has the packets.
	for i := 1; i < len(evs); i++ {
		for j := i; j > 0 && evs[j].arrive < evs[j-1].arrive; j-- {
			evs[j], evs[j-1] = evs[j-1], evs[j]
		}
	}
	for _, e := range evs {
		s := e.s
		if e.sr {
			wall := w0.Add(e.at)
			if e.at >= jumpAt {
				wall = wall.Add(jump)
			}
			l.Report(&s.tr, wall, s.tsOf(e.at))
			continue
		}
		ts := s.tsOf(e.at)
		if !s.started {
			s.started, s.ts, s.origin = true, ts, time.Second+e.arrive
			l.Start(&s.tr, ts, s.origin, s.rate)
		}
		s.tr.Next(ts)
		s.ext += int64(int32(ts - s.ts))
		s.ts = ts
		at := s.origin + av.FromTicks(s.ext, s.rate)
		pkt := av.Packet{DTS: at, PTS: at}
		s.tr.Shift(&pkt)
		if pkt.DTS <= s.lastDTS {
			t.Fatalf("times went back: %v after %v", pkt.DTS, s.lastDTS)
		}
		s.lastDTS = pkt.DTS
		s.out[e.at] = pkt.PTS
	}
}

// skew returns how far b's sample of sender time at is presented after
// a's.
func skew(a, b *simStream, at time.Duration) time.Duration {
	return b.out[at] - a.out[at]
}

func near(d, want time.Duration) bool { return absDiff(d, want) <= time.Millisecond }

// Two streams whose packets arrive 250 ms apart, with jitter, and whose
// timestamps wrap, are lined up once both have reported, the video's
// first report coming seconds late; before that, they are timed from
// arrival.
func TestLiveSkewJitterWrap(t *testing.T) {
	var l Live
	audio := &simStream{rate: 48000, dur: 20 * time.Millisecond, ts0: 1<<32 - 48000, delay: 20 * time.Millisecond,
		srEvery: time.Second, srFrom: 500 * time.Millisecond}
	video := &simStream{rate: 90000, dur: 40 * time.Millisecond, ts0: 1<<32 - 9000, delay: 270 * time.Millisecond,
		srEvery: time.Second, srFrom: 3 * time.Second}
	simulate(t, &l, 10*time.Second, 0, 0, audio, video)
	if d := skew(audio, video, 200*time.Millisecond); d < 200*time.Millisecond {
		t.Fatalf("before the reports, video %v after audio", d)
	}
	for at := 5 * time.Second; at <= 10*time.Second; at += 200 * time.Millisecond {
		if d := skew(audio, video, at); !near(d, 0) {
			t.Fatalf("at %v video %v after audio", at, d)
		}
	}
}

// A sender that never reports keeps the times of arrival, as does a
// stream alone with its reports.
func TestLiveNoReports(t *testing.T) {
	var l Live
	audio := &simStream{rate: 48000, dur: 20 * time.Millisecond, ts0: 5, delay: 20 * time.Millisecond}
	video := &simStream{rate: 90000, dur: 40 * time.Millisecond, ts0: 7, delay: 270 * time.Millisecond}
	simulate(t, &l, 3*time.Second, 0, 0, audio, video)
	audio2 := &simStream{rate: 48000, dur: 20 * time.Millisecond, ts0: 5, delay: 20 * time.Millisecond,
		srEvery: 500 * time.Millisecond}
	var l2 Live
	simulate(t, &l2, 3*time.Second, 0, 0, audio2)
	for at := 2 * time.Second; at <= 3*time.Second; at += 200 * time.Millisecond {
		if d := skew(audio, video, at); d < 250*time.Millisecond {
			t.Fatalf("no reports: video %v after audio", d)
		}
		if audio2.out[at] != audio.out[at]+audio2.out[0]-audio.out[0] {
			t.Fatal("a lone stream moved")
		}
	}
	if audio.tr.shift != 0 || video.tr.shift != 0 || audio2.tr.shift != 0 {
		t.Fatal("shifted")
	}
}

// A jump of the sender's wall clock, which each stream's reports show in
// turn, leaves the streams where they are, whichever reports it first.
func TestLiveClockJump(t *testing.T) {
	for _, videoFirst := range []bool{false, true} {
		var l Live
		audio := &simStream{rate: 48000, dur: 20 * time.Millisecond, ts0: 1, delay: 20 * time.Millisecond,
			srEvery: time.Second, srFrom: 200 * time.Millisecond}
		video := &simStream{rate: 90000, dur: 40 * time.Millisecond, ts0: 2, delay: 220 * time.Millisecond,
			srEvery: time.Second, srFrom: 700 * time.Millisecond}
		jumpAt := 4100 * time.Millisecond // the audio's report first
		if videoFirst {
			jumpAt = 4600 * time.Millisecond
		}
		simulate(t, &l, 10*time.Second, 3*time.Second, jumpAt, audio, video)
		for at := 2 * time.Second; at <= 10*time.Second; at += 200 * time.Millisecond {
			if d := skew(audio, video, at); !near(d, 0) {
				t.Fatalf("video first %v: at %v video %v after audio", videoFirst, at, d)
			}
		}
	}
}

// A move larger than Tolerance, as when a stream's RTP timestamps jump, is
// taken when the next report confirms it; reports placing a stream too
// far off are ignored.
func TestLiveConfirm(t *testing.T) {
	var l Live
	w := time.Unix(1700000000, 0)
	var a, b Track
	l.Start(&a, 0, time.Second, 8000)
	l.Start(&b, 0, time.Second, 8000)
	l.Report(&a, w, 0)
	l.Report(&b, w.Add(50*time.Millisecond), 0)
	if b.target != 50*time.Millisecond {
		t.Fatal("first placement", b.target)
	}
	l.Report(&b, w.Add(time.Second), 0)
	if b.target != 50*time.Millisecond || !b.hasPending {
		t.Fatal("unconfirmed move taken", b.target)
	}
	l.Report(&b, w.Add(time.Second+10*time.Millisecond), 0)
	if b.target != 1010*time.Millisecond || b.hasPending {
		t.Fatal("confirmed move not taken", b.target)
	}
	l.Report(&b, w.Add(time.Hour), 0)
	l.Report(&b, w.Add(-2*time.Second), 0)
	if b.target != 1010*time.Millisecond {
		t.Fatal("far-off report taken", b.target)
	}
	// A stream started with no clock rate is never placed.
	var c Track
	l.Report(&c, w, 0)
	l.Start(&c, 0, time.Second, 0)
	if c.placed {
		t.Fatal("placed with no clock rate")
	}
}

// A stream that starts after the session anchored, its report already in,
// starts in its place at once; restarting the anchor's stream hands the
// anchor to the next stream to report.
func TestLiveLateStart(t *testing.T) {
	var l Live
	w := time.Unix(1700000000, 0)
	var a, b Track
	l.Start(&a, 1000, time.Second, 8000)
	l.Report(&a, w, 1000)
	l.Report(&b, w.Add(2*time.Second), 90000*7)
	// b's first packet arrives 2.3 s after its sample of a's start.
	l.Start(&b, 90000*7, 3300*time.Millisecond, 90000)
	pkt := av.Packet{DTS: 3300 * time.Millisecond, PTS: 3300 * time.Millisecond}
	b.Shift(&pkt)
	if pkt.PTS != 3*time.Second {
		t.Fatal("late stream at", pkt.PTS)
	}
	// The anchor's stream restarting, its timestamps carrying on, is
	// placed where its report has it, and keeps the anchor.
	l.Start(&a, 1000+8000*4, 5*time.Second, 8000)
	if l.anchor != &a || a.target != 0 {
		t.Fatal("restarted anchor", a.target)
	}
	// Restarted at another clock rate it has no report: the next stream
	// to report carries the anchor on.
	l.Start(&a, 0, 6*time.Second, 48000)
	if l.anchor != nil || a.clock.OK {
		t.Fatal("restarted stream kept the anchor or its report")
	}
	l.Report(&b, w.Add(3*time.Second), 90000*8)
	if l.anchor != &b || b.target != -300*time.Millisecond || l.at != 4*time.Second {
		t.Fatal("anchor not handed on", b.target, l.at)
	}
	l.Report(&b, w.Add(4*time.Second), 90000*9)
	if b.target != -300*time.Millisecond || l.at != 5*time.Second {
		t.Fatal("anchor not moved along", b.target, l.at)
	}
}

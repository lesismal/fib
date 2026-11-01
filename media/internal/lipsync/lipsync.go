// Package lipsync lines up the RTP streams of one sender by its RTCP
// sender reports, as RFC 3550 section 6.4.1 has receivers do: the ingests
// of package rtsp and package rtc share it.
//
// Every stream's RTP timestamps start at a random offset of their own, so
// on their own they say nothing of how the streams line up; the arrival of
// their first packets, the fallback, is off by however differently the
// sender's encoders and queues delayed them, a quarter of a second and more
// for a video encoder's lookahead. A sender report maps a stream's
// timestamps to the sender's wall clock, which all its streams share.
package lipsync

import (
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/webrtc/rtp"
)

// MaxSkew bounds how far sender reports may move a stream from where it
// would be without them: reports further off than this come from a sender
// whose clocks cannot be trusted, and are ignored.
const MaxSkew = 10 * time.Second

// Tolerance is how far a stream's place may move from one sender report to
// the next and still be taken at once. Clocks drift by a few parts per
// million; a larger move is a jump of one of the sender's clocks, which is
// taken only when the stream's next report confirms it, so that a jump of
// the wall clock, which the streams' reports show one after the other, is
// never taken from one stream's report before the others show it.
const Tolerance = 100 * time.Millisecond

// Clock is what the latest sender report of a stream says: RTP timestamp
// RTP was sampled at the sender's wall-clock time NTP.
type Clock struct {
	OK  bool
	NTP time.Time
	RTP uint32
}

// Update takes the sender report in the compound RTCP packet b, if there
// is one. Its SSRC is not checked: a stream's RTCP channel or port carries
// that stream's reports only.
func (c *Clock) Update(b []byte) {
	for len(b) > 0 {
		h, pkt, rest, err := rtp.NextRTCP(b)
		if err != nil {
			return
		}
		var sr rtp.SenderReport
		if h.Type == rtp.TypeSR && sr.Unmarshal(pkt) == nil && sr.NTPTime != 0 {
			c.OK, c.NTP, c.RTP = true, rtp.TimeFromNTP(sr.NTPTime), sr.RTPTime
		}
		b = rest
	}
}

// Wall returns the wall-clock time RTP timestamp ts, of a clock of
// clockRate Hz, was sampled at; ts is taken as within 2^31 ticks of the
// report's.
func (c *Clock) Wall(ts uint32, clockRate int) time.Time {
	return c.NTP.Add(av.FromTicks(int64(int32(ts-c.RTP)), clockRate))
}

// Session lines the streams of one session up as each starts: the first
// stream to start with a sender report anchors the session, and the others
// start where their reports place them from it. A stream that starts
// before its first report keeps its fallback.
type Session struct {
	set bool
	ntp time.Time
	at  time.Duration
}

// Origin returns the media time of a stream's first RTP packet, timestamp
// ts at clockRate: fallback, unless the stream's sender report and an
// anchor set by another stream place it.
func (l *Session) Origin(sr Clock, ts uint32, clockRate int, fallback time.Duration) time.Duration {
	if !sr.OK || clockRate <= 0 {
		return fallback
	}
	wall := sr.Wall(ts, clockRate)
	if !l.set {
		l.set, l.ntp, l.at = true, wall, fallback
		return fallback
	}
	at := l.at + wall.Sub(l.ntp)
	if at < 0 || at-fallback > MaxSkew || fallback-at > MaxSkew {
		return fallback
	}
	return at
}

// Live lines the streams of a live session up for as long as it runs,
// from each sender report as it comes, for a sender whose first reports
// may come well after its first packets, as a browser's do. Each stream is
// timed from the arrival of its first packet, as without reports, and then
// shifted by the difference its reports show from an anchor: the stream
// that reported first, whose every report moves the anchor along with it.
// A shift is slewed in, at most half the time between two packets at each,
// so a stream's times never go back and never jump; a stream with no
// reports, or a session with one stream, keeps its times.
//
// It is not safe for concurrent use.
type Live struct {
	// Sender time ntp is media time at; anchor is the stream whose
	// reports move it along, nil after that stream restarted.
	set    bool
	anchor *Track
	ntp    time.Time
	at     time.Duration
}

// Track is one stream of a Live session.
type Track struct {
	rate    int
	started bool
	origin  time.Duration
	lastTS  uint32
	ext     int64
	clock   Clock
	// shift is added to the stream's times, and moves towards target;
	// pending is a target held until the next report confirms it.
	shift, target time.Duration
	placed        bool
	pending       time.Duration
	hasPending    bool
	last          time.Duration
	hasLast       bool
}

// Start starts (or restarts) t: RTP timestamp ts, of a clock of clockRate
// Hz, is media time at, as the stream's depacketizer is given it; t
// follows the timestamps from there as the depacketizer does. A report
// the stream sent before is applied now, so a stream that starts later
// than the others starts in its place; a stream that restarts, as on a
// change of codec, is placed again.
func (l *Live) Start(t *Track, ts uint32, at time.Duration, clockRate int) {
	if l.anchor == t {
		l.anchor = nil
	}
	if t.rate != 0 && t.rate != clockRate {
		t.clock = Clock{}
	}
	*t = Track{rate: clockRate, started: clockRate > 0, origin: at, lastTS: ts, clock: t.clock}
	if t.started && t.clock.OK {
		l.place(t)
	}
}

// Next follows the stream's RTP timestamps: it is given that of each
// packet the depacketizer takes.
func (t *Track) Next(ts uint32) {
	t.ext += int64(int32(ts - t.lastTS))
	t.lastTS = ts
}

// time returns the media time of RTP timestamp ts, before the shift.
func (t *Track) time(ts uint32) time.Duration {
	return t.origin + av.FromTicks(t.ext+int64(int32(ts-t.lastTS)), t.rate)
}

// Report takes a sender report of t's stream: RTP timestamp rtpTime was
// sampled at wall-clock time ntp.
func (l *Live) Report(t *Track, ntp time.Time, rtpTime uint32) {
	t.clock = Clock{OK: true, NTP: ntp, RTP: rtpTime}
	if t.started {
		l.place(t)
	}
}

// place moves the anchor along with its stream's report, or sets the
// shift t's report asks for from the anchor.
func (l *Live) place(t *Track) {
	m := t.time(t.clock.RTP)
	if !l.set || l.anchor == t {
		// Where the stream is going stays where it is.
		l.set, l.anchor, l.ntp, l.at = true, t, t.clock.NTP, m+t.target
		t.placed, t.hasPending = true, false
		return
	}
	target := l.at + t.clock.NTP.Sub(l.ntp) - m
	if target > MaxSkew || target < -MaxSkew || m+target < 0 {
		return
	}
	if t.placed && absDiff(target, t.target) > Tolerance &&
		(!t.hasPending || absDiff(target, t.pending) > Tolerance) {
		t.pending, t.hasPending = target, true
		return
	}
	t.target, t.placed, t.hasPending = target, true, false
	if l.anchor == nil {
		// The anchor's stream restarted: this one carries it on.
		l.anchor, l.ntp, l.at = t, t.clock.NTP, m+target
	}
}

func absDiff(a, b time.Duration) time.Duration {
	if a > b {
		return a - b
	}
	return b - a
}

// Shift moves pkt, a packet of t's stream as the depacketizer gave it, to
// its place in the session.
func (t *Track) Shift(pkt *av.Packet) {
	if d := t.target - t.shift; d != 0 {
		if t.hasLast {
			room := max((pkt.DTS-t.last)/2, 0)
			d = max(min(d, room), -room)
		}
		t.shift += d
	}
	if !t.hasLast || pkt.DTS > t.last {
		t.last, t.hasLast = pkt.DTS, true
	}
	pkt.DTS += t.shift
	pkt.PTS += t.shift
}

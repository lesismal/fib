package rtsp

import (
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/webrtc/rtp"
)

// maxLipSyncSkew bounds how far sender reports may move a stream's start
// from where it would start without them: reports further off than this
// come from a sender whose clocks cannot be trusted, and are ignored.
const maxLipSyncSkew = 10 * time.Second

// senderClock is what the latest sender report of a stream says: RTP
// timestamp rtp was sampled at the sender's wall-clock time ntp.
type senderClock struct {
	ok  bool
	ntp time.Time
	rtp uint32
}

// update takes the sender report in the compound RTCP packet b, if there
// is one. Its SSRC is not checked: a stream's RTCP channel or port carries
// that stream's reports only.
func (c *senderClock) update(b []byte) {
	for len(b) > 0 {
		h, pkt, rest, err := rtp.NextRTCP(b)
		if err != nil {
			return
		}
		var sr rtp.SenderReport
		if h.Type == rtp.TypeSR && sr.Unmarshal(pkt) == nil && sr.NTPTime != 0 {
			c.ok, c.ntp, c.rtp = true, rtp.TimeFromNTP(sr.NTPTime), sr.RTPTime
		}
		b = rest
	}
}

// lipSync lines up the streams of one session by their senders' clocks,
// as RFC 3550 section 6.4.1 has receivers do. Every stream's RTP
// timestamps start at a random offset of their own, so on their own they
// say nothing of how the streams line up; the arrival of their first
// packets, the fallback, is off by however differently the sender's
// encoders and queues delayed them, a quarter of a second and more for a
// video encoder's lookahead. A sender report maps a stream's timestamps to
// the sender's wall clock, which all its streams share: the first stream
// to start with one anchors the session, and the others start where their
// reports place them from it.
type lipSync struct {
	set bool
	ntp time.Time
	at  time.Duration
}

// origin returns the media time of a stream's first RTP packet, timestamp
// ts at clockRate: fallback, unless the stream's sender report and an
// anchor set by another stream place it.
func (l *lipSync) origin(sr senderClock, ts uint32, clockRate int, fallback time.Duration) time.Duration {
	if !sr.ok || clockRate <= 0 {
		return fallback
	}
	wall := sr.ntp.Add(av.FromTicks(int64(int32(ts-sr.rtp)), clockRate))
	if !l.set {
		l.set, l.ntp, l.at = true, wall, fallback
		return fallback
	}
	at := l.at + wall.Sub(l.ntp)
	if at < 0 || at-fallback > maxLipSyncSkew || fallback-at > maxLipSyncSkew {
		return fallback
	}
	return at
}

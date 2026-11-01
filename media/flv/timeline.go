package flv

import "time"

// now is the clock of Timeline, a variable for tests.
var now = time.Now

// maxTimelineJump is how far past the last timestamp a packet after
// Restart may land and still be taken as the same clock carrying on.
const maxTimelineJump = 10 * time.Second

// minRestartGap is the least a new clock starts after the last timestamp.
const minRestartGap = 20 * time.Millisecond

// Timeline turns the decode times of a hub stream's packets into the
// millisecond timestamps of FLV tags and RTMP messages, which players
// need to increase per track. A subscription outlives its publisher for
// as long as it waits for the next one, and the next one's clock may
// start anywhere: an RTMP publisher's from 0 again. Without Timeline the
// timestamps a player saw would jump back, which ffmpeg reports for every
// packet after it and flv.js and MSE players stall on.
//
// After Restart, the next packet whose time is behind the last timestamp,
// or more than ten seconds past it, starts a new clock after it by as long
// as the stream was quiet (20 ms to ten seconds), so that timestamps keep
// pace with the wall clock players wait by, and frames of the two clocks
// never share a timestamp. A packet of a track behind that track's last
// timestamp, as audio interleaved a little behind the keyframe a stream
// started at, is given the last one.
//
// The zero Timeline starts at the first packet's own time; with
// FromZero, the first packet is at 0, as FLV files start.
type Timeline struct {
	FromZero bool

	started, restart bool
	// offset is added to a packet's time in milliseconds.
	offset int64
	// last is the largest timestamp given, and lastVideo and lastAudio
	// those of each track; lastAt is when the last packet was stamped.
	last, lastVideo, lastAudio int64
	lastAt                     time.Time
}

// Restart says the packets that come next may follow a new clock: the
// publisher changed.
func (t *Timeline) Restart() { t.restart = t.started }

// Last returns the largest timestamp given, 0 before any.
func (t *Timeline) Last() uint32 { return uint32(t.last) }

// Stamp returns the timestamp of a video or audio packet at dts.
func (t *Timeline) Stamp(video bool, dts time.Duration) uint32 {
	ms := int64(Millis(dts))
	if dts < 0 {
		ms = -int64(Millis(-dts))
	}
	switch {
	case !t.started:
		t.started = true
		if t.FromZero {
			t.offset = -ms
		}
	case t.restart:
		t.restart = false
		if ts := ms + t.offset; ts <= t.last || ts > t.last+maxTimelineJump.Milliseconds() {
			gap := max(min(now().Sub(t.lastAt), maxTimelineJump), minRestartGap)
			t.offset = t.last + gap.Milliseconds() - ms
		}
	}
	t.lastAt = now()
	ts := max(ms+t.offset, 0)
	last := &t.lastAudio
	if video {
		last = &t.lastVideo
	}
	ts = max(ts, *last)
	*last = ts
	t.last = max(t.last, ts)
	return uint32(ts)
}

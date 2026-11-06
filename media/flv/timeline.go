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
// After Restart, the next packet whose time is not past the last timestamp
// of its own track (video or audio), or is more than ten seconds past it,
// starts a new clock after the last timestamp of either by as long as the
// stream was quiet (20 ms to ten seconds), so that timestamps keep pace
// with the wall clock players wait by, and frames of the two clocks never
// share a timestamp. A packet that carries on its track's timestamps
// carries on the clock, though it may be behind the other track's last
// one: a publisher whose tracks change on the same clock (SetTracks), with
// B-frames, sends video decoded before the last audio it sent ends, and is
// not moved on. The first packet of a track the stream had none of before
// is checked against the last timestamp of either instead. A packet of a track behind that
// track's last timestamp, as audio interleaved a little behind the
// keyframe a stream started at, is given the last one.
//
// The zero Timeline starts at the first packet's own time; with
// FromZero, the first packet is at 0, as FLV files start.
type Timeline struct {
	FromZero bool

	started, restart bool
	// offset is added to a packet's time in milliseconds.
	offset int64
	// last is the largest timestamp given, and lastVideo and lastAudio
	// those of each track, which sawVideo and sawAudio say were given;
	// lastAt is when the last packet was stamped.
	last, lastVideo, lastAudio int64
	sawVideo, sawAudio         bool
	lastAt                     time.Time
}

// Restart says the packets that come next may follow a new clock: the
// publisher changed.
func (t *Timeline) Restart() { t.restart = t.started }

// Last returns the largest timestamp given, 0 before any.
func (t *Timeline) Last() uint32 { return uint32(t.last) }

// LastOf returns the last timestamp given to video or audio, or Last when
// that track has had none: where the sequence header of a track that
// changed goes, so that it comes no later than the track's packets after
// it.
func (t *Timeline) LastOf(video bool) uint32 {
	if last, saw := t.track(video); *saw {
		return uint32(*last)
	}
	return uint32(t.last)
}

// HeaderStamp returns the timestamp of a tag of type typ the Encoder's
// Headers gives: LastOf its track for a sequence header, Last for
// metadata.
func (t *Timeline) HeaderStamp(typ uint8) uint32 {
	switch typ {
	case TagVideo:
		return t.LastOf(true)
	case TagAudio:
		return t.LastOf(false)
	}
	return t.Last()
}

// track returns the last timestamp of video or audio, and whether it was
// given.
func (t *Timeline) track(video bool) (*int64, *bool) {
	if video {
		return &t.lastVideo, &t.sawVideo
	}
	return &t.lastAudio, &t.sawAudio
}

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
		ref := t.last
		if last, saw := t.track(video); *saw {
			ref = *last
		}
		if ts := ms + t.offset; ts <= ref || ts > ref+maxTimelineJump.Milliseconds() {
			gap := max(min(now().Sub(t.lastAt), maxTimelineJump), minRestartGap)
			t.offset = t.last + gap.Milliseconds() - ms
		}
	}
	t.lastAt = now()
	ts := max(ms+t.offset, 0)
	last, saw := t.track(video)
	ts = max(ts, *last)
	*last, *saw = ts, true
	t.last = max(t.last, ts)
	return uint32(ts)
}

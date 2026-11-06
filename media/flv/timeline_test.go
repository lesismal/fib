package flv

import (
	"slices"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
)

func TestTimeline(t *testing.T) {
	ms := time.Millisecond
	stamp := func(tl *Timeline, video bool, dts time.Duration, want uint32) {
		t.Helper()
		if got := tl.Stamp(video, dts); got != want {
			t.Fatalf("%v at %v: %d, want %d", video, dts, got, want)
		}
	}
	// From zero: audio a little behind the first keyframe is held at 0.
	tl := Timeline{FromZero: true}
	tl.Restart() // before any packet: nothing to restart
	stamp(&tl, true, 1000*ms, 0)
	stamp(&tl, false, 990*ms, 0)
	stamp(&tl, false, 1010*ms, 10)
	stamp(&tl, true, 1040*ms, 40)
	// A packet of a track behind its own last one gets the last one.
	stamp(&tl, true, 1030*ms, 40)
	// A codec change with the clock carrying on, even with a gap.
	tl.Restart()
	stamp(&tl, true, 6000*ms, 5000)
	// A republish restarting the clock at 0: just after the last.
	tl.Restart()
	stamp(&tl, false, 0, 5020)
	stamp(&tl, true, 10*ms, 5030)
	// And one whose clock is hours ahead.
	tl.Restart()
	stamp(&tl, true, 3*time.Hour, 5050)
	stamp(&tl, false, 3*time.Hour+20*ms, 5070)
	if tl.Last() != 5070 {
		t.Fatal(tl.Last())
	}
	// Absolute: the publisher's own times, negative ones at 0.
	var abs Timeline
	stamp(&abs, false, -5*ms, 0)
	stamp(&abs, true, 2*time.Second, 2000)
	abs.Restart()
	stamp(&abs, true, 0, 2020)
}

// A new clock starts after the last timestamp by as long as the stream
// was quiet.
func TestTimelineQuiet(t *testing.T) {
	at := time.Unix(1000, 0)
	now = func() time.Time { return at }
	defer func() { now = time.Now }()
	var tl Timeline
	tl.Stamp(true, 8*time.Second)
	at = at.Add(2500 * time.Millisecond)
	tl.Restart()
	if ts := tl.Stamp(true, 0); ts != 10500 {
		t.Fatal(ts)
	}
	at = at.Add(time.Hour)
	tl.Restart()
	if ts := tl.Stamp(true, 0); ts != 20500 {
		t.Fatal("capped at ten seconds:", ts)
	}
}

// An FLV player of a stream that is republished sees timestamps go on.
func TestMuxerRepublish(t *testing.T) {
	tracks := []*av.Track{{Codec: av.CodecPCMA, SampleRate: 8000, Channels: 1}}
	m := NewMuxer(EncoderConfig{})
	b, _ := m.AppendHeader(nil, tracks)
	for _, d := range []time.Duration{5 * time.Second, 5020 * time.Millisecond} {
		b, _ = m.AppendPacket(b, &av.Packet{DTS: d, PTS: d, Keyframe: true, Data: []byte{1}})
	}
	b, _ = m.AppendTracks(b, tracks)
	b, _ = m.AppendPacket(b, &av.Packet{Keyframe: true, Data: []byte{2}})
	var times []uint32
	err := NewDemuxer(DemuxerConfig{}).Feed(b, func(tag Tag) error {
		if tag.Type == TagAudio {
			times = append(times, tag.Timestamp)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(times) != 3 || times[0] != 0 || times[1] != 20 || times[2] != 40 {
		t.Fatal(times)
	}
}

// New tracks on the same clock carry on each track's timestamps: B-frame
// video decoded before the last audio sent ends is not moved on, as a
// publisher changing its tracks (SetTracks) sends it; a clock that starts
// again still is.
func TestTimelineSameClock(t *testing.T) {
	ms := time.Millisecond
	stamp := func(tl *Timeline, video bool, dts time.Duration, want uint32) {
		t.Helper()
		if got := tl.Stamp(video, dts); got != want {
			t.Fatalf("%v at %v: %d, want %d", video, dts, got, want)
		}
	}
	tl := Timeline{FromZero: true}
	stamp(&tl, true, 920*ms, 0)
	stamp(&tl, false, 1000*ms, 80)
	stamp(&tl, true, 1880*ms, 960)
	stamp(&tl, false, 1980*ms, 1060)
	if tl.LastOf(true) != 960 || tl.LastOf(false) != 1060 || tl.Last() != 1060 {
		t.Fatal(tl.LastOf(true), tl.LastOf(false), tl.Last())
	}
	if tl.HeaderStamp(TagVideo) != 960 || tl.HeaderStamp(TagAudio) != 1060 || tl.HeaderStamp(TagScript) != 1060 {
		t.Fatal("header stamps")
	}
	// The next item: its keyframe is behind the last audio, past the
	// last video.
	tl.Restart()
	stamp(&tl, true, 1920*ms, 1000)
	stamp(&tl, false, 2000*ms, 1080)
	stamp(&tl, true, 1960*ms, 1040)
	// Audio first works the same.
	tl.Restart()
	stamp(&tl, false, 2020*ms, 1100)
	stamp(&tl, true, 2000*ms, 1080)
	// A republish whose clock starts again: video at or before its own
	// last starts a new clock after the last of either.
	tl.Restart()
	stamp(&tl, true, 1080*ms, 1120)
	stamp(&tl, false, 1100*ms, 1140)
	// So does a clock more than ten seconds on.
	tl.Restart()
	stamp(&tl, false, time.Hour, 1160)
	// A track the stream had none of is checked against the last of
	// either.
	var one Timeline
	stamp(&one, false, 5*time.Second, 5000)
	if one.LastOf(true) != 5000 || one.HeaderStamp(TagVideo) != 5000 {
		t.Fatal("no video yet:", one.LastOf(true))
	}
	one.Restart()
	stamp(&one, true, 4990*ms, 5020)
}

// An FLV player of a stream whose tracks change on the same clock sees
// the video's timestamps go on as they were, the sequence header no later
// than the frames after it.
func TestMuxerSameClock(t *testing.T) {
	tracks := []*av.Track{{Codec: av.CodecH264, Config: avcConfig(t)}, {Codec: av.CodecPCMA, SampleRate: 8000, Channels: 1}}
	m := NewMuxer(EncoderConfig{})
	b, _ := m.AppendHeader(nil, tracks)
	frame := func(key bool) []byte {
		if key {
			return []byte{0, 0, 0, 2, 0x65, 1}
		}
		return []byte{0, 0, 0, 2, 0x41, 1}
	}
	write := func(track int, dts time.Duration, key bool) {
		t.Helper()
		pkt := &av.Packet{Track: track, DTS: dts, PTS: dts, Keyframe: true, Data: []byte{1}}
		if track == 0 {
			pkt.PTS, pkt.Keyframe, pkt.Data = dts+80*time.Millisecond, key, frame(key)
		}
		var err error
		if b, err = m.AppendPacket(b, pkt); err != nil {
			t.Fatal(err)
		}
	}
	ms := time.Millisecond
	write(0, 920*ms, true)
	write(1, 1000*ms, false)
	write(0, 1880*ms, false)
	write(1, 1980*ms, false)
	next := []*av.Track{{Codec: av.CodecH264, Config: avcConfig(t)}, tracks[1]}
	b, _ = m.AppendTracks(b, next)
	write(0, 1920*ms, true)
	write(1, 2000*ms, false)
	type tag struct {
		typ uint8
		ts  uint32
	}
	var tags []tag
	err := NewDemuxer(DemuxerConfig{}).Feed(b, func(tg Tag) error {
		tags = append(tags, tag{tg.Type, tg.Timestamp})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var video []uint32
	for _, tg := range tags {
		if tg.typ == TagVideo {
			video = append(video, tg.ts)
		}
	}
	// Header, two frames, header, frame: 0 to 960, then on at 1000.
	if want := []uint32{0, 0, 960, 960, 1000}; !slices.Equal(video, want) {
		t.Fatalf("video tags at %v, want %v", video, want)
	}
}

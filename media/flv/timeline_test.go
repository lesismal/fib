package flv

import (
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

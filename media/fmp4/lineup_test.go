package fmp4

import (
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
)

// A stream joined at a keyframe has audio a little earlier than the
// keyframe come after it (a B-frame encoder's video DTS trails its PTS):
// the audio keeps its place before the video rather than starting with it,
// which played it a third of a second late for good.
func TestWriterLinesTracksUp(t *testing.T) {
	ms := time.Millisecond
	w, err := NewWriter([]*av.Track{h264Track(t), aacTrack(t, 48000)})
	if err != nil {
		t.Fatal(err)
	}
	init, _ := ParseInit(w.Init())
	w.WritePacket(pkt(0, 1000, 80, true, "v0"))
	w.WritePacket(pkt(0, 1040, 80, false, "v1"))
	w.WritePacket(pkt(1, 660, 0, true, "a0"))
	w.WritePacket(pkt(1, 681, 0, true, "a1"))
	f, _ := w.Flush(nil, pkt(0, 1080, 0, false, "v2"))
	_, pkts, _, err := init.ParseFragment(f)
	if err != nil {
		t.Fatal(err)
	}
	starts := map[int]time.Duration{}
	for _, p := range pkts {
		if _, ok := starts[p.Track]; !ok {
			starts[p.Track] = p.DTS
		}
	}
	if starts[0] != 340*ms || starts[1] != 0 {
		t.Fatalf("video at %v, audio at %v: want 340ms, 0", starts[0], starts[1])
	}

	// Once a fragment is out, or more than maxGap back, a late track can
	// only start at 0.
	for _, flushed := range []bool{true, false} {
		w, _ := NewWriter([]*av.Track{h264Track(t), aacTrack(t, 48000)})
		init, _ := ParseInit(w.Init())
		w.WritePacket(pkt(0, 20000, 0, true, "v0"))
		dts := 19500
		if flushed {
			_, _ = w.Flush(nil, pkt(0, 20040, 0, false, "v1"))
		} else {
			dts = 20000 - 11000
		}
		w.WritePacket(pkt(0, 20040, 0, false, "v1"))
		w.WritePacket(pkt(1, dts, 0, true, "a0"))
		f, _ := w.Flush(nil, nil)
		_, pkts, _, err := init.ParseFragment(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range pkts {
			if p.Track == 1 && p.DTS != 0 {
				t.Fatalf("flushed %v: audio at %v", flushed, p.DTS)
			}
		}
	}
}

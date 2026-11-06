package fmp4

import (
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
)

// A Writer carrying on another's timeline starts where it ended, its
// tracks lined up as from 0.
func TestWriterStartAt(t *testing.T) {
	w, _ := NewWriter([]*av.Track{h264Track(t), aacTrack(t, 48000)})
	if w.End() != 0 {
		t.Fatal(w.End())
	}
	w.WritePacket(pkt(0, 100, 0, true, "v0"))
	w.WritePacket(pkt(0, 140, 0, false, "v1"))
	_, _ = w.Flush(nil, nil)
	end := w.End()
	if end != 80*time.Millisecond {
		t.Fatal("end", end)
	}
	w2, _ := NewWriter([]*av.Track{h264Track(t), aacTrack(t, 48000)})
	w2.StartAt(-time.Second) // ignored
	w2.StartAt(end)
	init, _ := ParseInit(w2.Init())
	w2.WritePacket(pkt(0, 0, 0, true, "v0"))
	w2.WritePacket(pkt(1, -20, 0, true, "a0"))
	w2.StartAt(time.Hour) // too late
	f, _ := w2.Flush(nil, nil)
	_, pkts, _, err := init.ParseFragment(f)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pkts {
		want := end + 20*time.Millisecond
		if p.Track == 1 {
			want = end
		}
		if p.DTS != want {
			t.Fatalf("track %d at %v, want %v", p.Track, p.DTS, want)
		}
	}
}

// A Writer following another's Offset gives each track the decode times
// the other would have, as the stream's clock runs on: its first video,
// decoded before the other's last audio, is not moved past it.
func TestWriterFollow(t *testing.T) {
	ms := time.Millisecond
	w, _ := NewWriter([]*av.Track{h264Track(t), aacTrack(t, 48000)})
	if _, ok := w.Offset(); ok {
		t.Fatal("offset before a packet")
	}
	w.WritePacket(pkt(0, 1000, 80, true, "v0"))
	w.WritePacket(pkt(1, 980, 0, true, "a0"))
	w.WritePacket(pkt(0, 1040, 80, false, "v1"))
	w.WritePacket(pkt(1, 1060, 0, true, "a1"))
	_, _ = w.Flush(nil, nil)
	// The audio, earlier, starts the decode times.
	off, ok := w.Offset()
	if !ok || off != -980*ms {
		t.Fatal("offset", off, ok)
	}
	w2, _ := NewWriter([]*av.Track{h264Track(t), aacTrack(t, 48000)})
	w2.StartAt(time.Hour)
	w2.Follow(off) // the last call before a packet wins
	init, _ := ParseInit(w2.Init())
	w2.WritePacket(pkt(0, 1080, 80, true, "v2"))
	w2.WritePacket(pkt(1, 1100, 0, true, "a2"))
	w2.Follow(time.Hour) // too late
	if o, ok := w2.Offset(); !ok || o != off {
		t.Fatal("offset", o, ok)
	}
	f, _ := w2.Flush(nil, nil)
	_, pkts, _, err := init.ParseFragment(f)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pkts {
		want := 100 * ms
		if p.Track == 1 {
			want = 120 * ms
		}
		if p.DTS != want {
			t.Fatalf("track %d at %v, want %v", p.Track, p.DTS, want)
		}
	}
	// A decode time that would be negative is 0.
	w3, _ := NewWriter([]*av.Track{h264Track(t), aacTrack(t, 48000)})
	w3.Follow(-2 * time.Second)
	init, _ = ParseInit(w3.Init())
	w3.WritePacket(pkt(0, 1000, 0, true, "v0"))
	f, _ = w3.Flush(nil, nil)
	if _, pkts, _, err = init.ParseFragment(f); err != nil || len(pkts) != 1 || pkts[0].DTS != 0 {
		t.Fatal(pkts, err)
	}
}

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

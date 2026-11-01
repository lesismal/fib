package hls

import (
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/fmp4"
)

// After a republish restarting the publisher's clock, fMP4 decode times
// carry on from where they ended rather than going back: ffmpeg keeps its
// MP4 demuxer across the discontinuity, the init section being the same,
// and discarded the first segment after it.
func TestMuxerDiscontinuityTimeline(t *testing.T) {
	m := NewMuxer(Config{PlaylistSize: 10, KeepSegments: -1})
	tracks := []*av.Track{h264Track(t), aacTrack(t)}
	m.OnTracks(tracks)
	feed(m, (&source{video: true, audio: 1, gop: 50}).until(4100*time.Millisecond))
	m.OnTracks(tracks)
	feed(m, (&source{video: true, audio: 1, gop: 50}).until(4100*time.Millisecond))
	m.OnTracks(nil)
	p := media(t, m)
	if p.discs != 1 || len(p.maps) != 1 || len(p.segs) < 4 {
		t.Fatalf("%+v", p)
	}
	b, ok := m.File(p.maps[0])
	if !ok {
		t.Fatal("no init")
	}
	init, err := fmp4.ParseInit(b)
	if err != nil {
		t.Fatal(err)
	}
	var last [2]time.Duration
	var seen [2]bool
	for _, name := range p.segs {
		seg, ok := m.File(name)
		if !ok {
			t.Fatal("no", name)
		}
		for len(seg) > 0 {
			_, pkts, rest, err := init.ParseFragment(seg)
			if err != nil {
				t.Fatal(name, err)
			}
			for _, pkt := range pkts {
				if seen[pkt.Track] && pkt.DTS <= last[pkt.Track] {
					t.Fatalf("%s: track %d back from %v to %v", name, pkt.Track, last[pkt.Track], pkt.DTS)
				}
				seen[pkt.Track], last[pkt.Track] = true, pkt.DTS
			}
			seg = rest
		}
	}
	if last[0] < 7*time.Second {
		t.Fatal("timeline ends at", last[0])
	}
}

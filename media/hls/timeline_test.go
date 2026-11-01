package hls

import (
	"cmp"
	"slices"
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

// After a republish restarting the publisher's clock, MPEG-TS times carry
// on from where they ended too: ffmpeg's correction of a transport
// stream's jump back at a discontinuity left one audio decode time
// repeating the last before it. The second publisher's first audio is
// behind its first keyframe, which the writer holds packets to see, so
// that no track starts before where the last configuration ended.
func TestMuxerDiscontinuityTimelineTS(t *testing.T) {
	m := NewMuxer(Config{Format: MPEGTS, PlaylistSize: 10, KeepSegments: -1})
	tracks := []*av.Track{h264Track(t), aacTrack(t)}
	m.OnTracks(tracks)
	feed(m, avcc((&source{video: true, audio: 1, gop: 50}).until(4100*time.Millisecond)))
	m.OnTracks(tracks)
	second := avcc((&source{video: true, audio: 1, gop: 50}).until(4100 * time.Millisecond))
	for _, p := range second {
		if p.Track == 0 {
			p.DTS += 60 * time.Millisecond
			p.PTS += 60 * time.Millisecond
		}
	}
	slices.SortStableFunc(second, func(a, b *av.Packet) int { return cmp.Compare(a.DTS, b.DTS) })
	k := slices.IndexFunc(second, func(p *av.Packet) bool { return p.Track == 0 })
	second = append(append([]*av.Packet{second[k]}, second[:k]...), second[k+1:]...)
	feed(m, second)
	m.OnTracks(nil)
	p := media(t, m)
	if p.discs != 1 || len(p.maps) != 0 || len(p.segs) < 4 {
		t.Fatalf("%+v", p)
	}
	// One demuxer reads the segments in order, as a player does; each
	// segment is counted by itself, as the one demuxer gives up the
	// frame a stream's end left open at the discontinuity.
	var all []byte
	var n [2]int
	for _, name := range p.segs {
		seg, ok := m.File(name)
		if !ok {
			t.Fatal("no", name)
		}
		all = append(all, seg...)
		_, pkts := demux(t, seg)
		for _, pkt := range pkts {
			n[pkt.Track]++
		}
	}
	_, pkts := demux(t, all)
	var last [2]time.Duration
	var seen [2]bool
	for _, pkt := range pkts {
		if seen[pkt.Track] && pkt.DTS <= last[pkt.Track] {
			t.Fatalf("track %d back from %v to %v", pkt.Track, last[pkt.Track], pkt.DTS)
		}
		seen[pkt.Track], last[pkt.Track] = true, pkt.DTS
	}
	// Nothing was lost to the hold, and the timeline ran on.
	if n[0] != 2*103 || n[1] < 2*190 {
		t.Fatal("packets", n)
	}
	if last[0] < 8*time.Second {
		t.Fatal("timeline ends at", last[0])
	}
}

// A writer carrying on another's timeline holds no more than its bounds
// before it starts, and a Flush starts it with what it holds; its End
// counts a track whose step is not known yet as running on a little.
func TestTSWriterHold(t *testing.T) {
	tracks := []*av.Track{h264Track(t), aacTrack(t)}
	w, err := MPEGTS.New(tracks)
	if err != nil {
		t.Fatal(err)
	}
	tw := w.(*tsWriter)
	if tw.End() != 0 {
		t.Fatal(tw.End())
	}
	video := avcc((&source{video: true, audio: -1, gop: 50}).until(2 * time.Second))
	tw.StartAt(-time.Second) // ignored
	tw.StartAt(5 * time.Second)
	// Video only: held until a second past the first packet.
	for i, p := range video[:25] {
		tw.WritePacket(p)
		if !tw.holding {
			t.Fatal("released after", i)
		}
	}
	if tw.Buffered() != 25*len(video[0].Data) {
		t.Fatal("buffered", tw.Buffered())
	}
	tw.WritePacket(video[25])
	if tw.holding || tw.End() != 6*time.Second+40*time.Millisecond {
		t.Fatal("not released at a second, end", tw.End())
	}
	tw.StartAt(time.Hour) // too late

	// A Flush releases what is held, and a track with one packet ends a
	// millisecond on.
	w, _ = MPEGTS.New(tracks)
	tw = w.(*tsWriter)
	tw.StartAt(time.Second)
	tw.WritePacket(video[0])
	out, independent := tw.Flush(nil, nil)
	if tw.holding || len(out) == 0 || !independent || tw.End() != time.Second+tsMinStep {
		t.Fatal("flush", len(out), independent, tw.End())
	}
	_, pkts := demux(t, out)
	if len(pkts) != 1 {
		t.Fatal("packets", pkts)
	}
	// Flushing with nothing held is fine.
	w, _ = MPEGTS.New(tracks)
	tw = w.(*tsWriter)
	tw.StartAt(time.Second)
	tw.release()
	if tw.holding {
		t.Fatal("holding")
	}

	// The count bound: packets of one track that do not move on.
	w, _ = MPEGTS.New(tracks)
	tw = w.(*tsWriter)
	tw.StartAt(time.Second)
	for range tsMaxHeld {
		tw.WritePacket(&av.Packet{Track: 1, Keyframe: true, Data: []byte{1, 2}})
	}
	if tw.holding {
		t.Fatal("held past the count bound")
	}
}

package hls

import (
	"bufio"
	"bytes"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/fmp4"
)

// channelItems feeds m two items of a channel on one clock, each with
// tracks of its own, as fit plays a library: 25 fps B-frame video with a
// keyframe a second, and AAC starting with the item's first picture, so
// that the second item's first audio comes 11 ms after the first's last.
func channelItems(t *testing.T, m *Muxer) {
	t.Helper()
	tracks := []*av.Track{h264Track(t), aacTrack(t)}
	for _, base := range []time.Duration{time.Second, 5 * time.Second} {
		next := slices.Clone(tracks)
		v := *next[0]
		next[0] = &v
		m.OnTracks(next)
		feed(m, bframes((&source{video: true, audio: 1, gop: 25, base: base}).until(base+4*time.Second)))
	}
	m.OnTracks(nil)
}

// fmp4Segment is what hls.js reads of a whole fMP4 segment: whether a
// discontinuity comes before it, and its packets.
type fmp4Segment struct {
	name string
	disc bool
	pkts []*av.Packet
}

// fmp4Segments reads the segments m lists, in order.
func fmp4Segments(t *testing.T, m *Muxer) []fmp4Segment {
	t.Helper()
	b, ok := m.MediaPlaylist(nil)
	if !ok {
		t.Fatal("no playlist")
	}
	var segs []fmp4Segment
	var init *fmp4.Init
	disc := false
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		l := sc.Text()
		switch {
		case l == "#EXT-X-DISCONTINUITY":
			disc = true
		case strings.HasPrefix(l, "#EXT-X-MAP:URI=\""):
			data, ok := m.File(strings.TrimSuffix(strings.TrimPrefix(l, "#EXT-X-MAP:URI=\""), "\""))
			if !ok {
				t.Fatal("no init:", l)
			}
			var err error
			if init, err = fmp4.ParseInit(data); err != nil {
				t.Fatal(err)
			}
		case l != "" && !strings.HasPrefix(l, "#"):
			data, ok := m.File(l)
			if !ok {
				t.Fatal("no", l)
			}
			s := fmp4Segment{name: l, disc: disc}
			disc = false
			for len(data) > 0 {
				_, pkts, rest, err := init.ParseFragment(data)
				if err != nil {
					t.Fatal(l, err)
				}
				s.pkts = append(s.pkts, pkts...)
				data = rest
			}
			segs = append(segs, s)
		}
	}
	return segs
}

// A segment's tracks begin at its keyframe: the audio decoding before it,
// though the fMP4 writer holds its last packet until the next one tells its
// duration, is in the segment before.
func TestMuxerSegmentStartsAtKeyframe(t *testing.T) {
	for _, part := range []time.Duration{0, 250 * time.Millisecond} {
		m := NewMuxer(Config{TargetDuration: time.Second, PartDuration: part, PlaylistSize: 20, KeepSegments: -1})
		channelItems(t, m)
		segs := fmp4Segments(t, m)
		if len(segs) < 6 {
			t.Fatalf("%d segments", len(segs))
		}
		audio := 0
		for _, s := range segs {
			if len(s.pkts) == 0 || s.pkts[0].Track != 0 || !s.pkts[0].Keyframe {
				t.Fatalf("part %v, %s: does not start with a keyframe", part, s.name)
			}
			for _, p := range s.pkts {
				if p.DTS < s.pkts[0].DTS {
					t.Fatalf("part %v, %s: track %d at %v, before its keyframe at %v", part, s.name, p.Track, p.DTS, s.pkts[0].DTS)
				}
				audio += p.Track
			}
		}
		// None is lost: 2 items of 4 s of 1024-sample frames at 48 kHz.
		if want := 2 * 188; audio != want {
			t.Fatalf("part %v: %d audio packets, want %d", part, audio, want)
		}
	}
}

// hls.js, reading whole fMP4 segments, places the first segment after a
// discontinuity where the one before it ends: its earliest decode time
// plus its video's duration. Chrome, given the segments of an item with
// new tracks there, finds every track's decode times running on, none of
// them going back, and plays the new item's video from its keyframe:
// before, each segment's audio began a frame before its keyframe, the new
// item came that much early, its first audio decoded before the last one
// before it, and Chrome dropped the video up to the next keyframe.
func TestMuxerHLSJSPlacement(t *testing.T) {
	m := NewMuxer(Config{TargetDuration: time.Second, PlaylistSize: 20, KeepSegments: -1})
	channelItems(t, m)
	segs := fmp4Segments(t, m)
	const frame = 40 * time.Millisecond
	var initPTS, start, dur time.Duration
	var last [2]time.Duration
	var seen [2]bool
	discs := 0
	for i, s := range segs {
		first := s.pkts[0].DTS
		var lastVideo time.Duration
		for _, p := range s.pkts {
			first = min(first, p.DTS)
			if p.Track == 0 {
				lastVideo = p.DTS
			}
		}
		switch {
		case i == 0:
			initPTS = first
		case s.disc:
			discs++
			initPTS = first - (start + dur)
		}
		start = first - initPTS
		// Every frame of the source is 40 ms long, its last one's too.
		dur = lastVideo + frame - s.pkts[0].DTS
		for _, p := range s.pkts {
			at := p.DTS - initPTS
			if seen[p.Track] && at <= last[p.Track] {
				t.Fatalf("%s: track %d decodes at %v, after %v", s.name, p.Track, at, last[p.Track])
			}
			last[p.Track], seen[p.Track] = at, true
		}
	}
	if discs != 1 {
		t.Fatalf("%d discontinuities", discs)
	}
}

// checkCuts checks that segs hold n video and audio packets, each segment
// starting with a keyframe that no packet decodes before, and lasting at
// most max. The last segment may hold the last packet of a track that
// stopped before it, which the fMP4 writer keeps until the stream ends.
func checkCuts(t *testing.T, segs []fmp4Segment, n [2]int, max time.Duration) {
	t.Helper()
	var got [2]int
	for i, s := range segs {
		if len(s.pkts) == 0 || s.pkts[0].Track != 0 || !s.pkts[0].Keyframe {
			t.Fatalf("%s: does not start with a keyframe", s.name)
		}
		for j, p := range s.pkts {
			stopped := i == len(segs)-1 && p.Track == 1 && !slices.ContainsFunc(s.pkts[j+1:], func(q *av.Packet) bool { return q.Track == 1 }) &&
				p.DTS < s.pkts[0].DTS-max
			if p.DTS < s.pkts[0].DTS && !stopped {
				t.Fatalf("%s: track %d at %v, before its keyframe at %v", s.name, p.Track, p.DTS, s.pkts[0].DTS)
			}
			got[p.Track]++
		}
		if i+1 < len(segs) {
			if d := segs[i+1].pkts[0].DTS - s.pkts[0].DTS; d > max {
				t.Fatalf("%s: %v long", s.name, d)
			}
		}
	}
	if got != n {
		t.Fatalf("%v packets, want %v", got, n)
	}
}

// count counts pkts' video and audio packets.
func count(pkts []*av.Packet) (n [2]int) {
	for _, p := range pkts {
		n[p.Track]++
	}
	return n
}

// Audio arriving late, after the video it decodes with, as a publisher
// interleaving coarsely sends it, or 60 fps video, whose next frame comes
// before the audio reaching the keyframe: the video after the keyframe
// waits with the cut. Neither loses a packet, nor do parts.
func TestMuxerCutWaitsForAudio(t *testing.T) {
	for _, tc := range []struct {
		name  string
		late  time.Duration
		frame int
	}{{"late audio", 120 * time.Millisecond, 0}, {"60 fps", 0, 60}} {
		for _, part := range []time.Duration{0, 200 * time.Millisecond} {
			pkts := (&source{video: true, audio: 1, gop: 25, base: time.Second}).until(9 * time.Second)
			if tc.frame > 0 {
				pkts = sixty(pkts)
			}
			key := func(p *av.Packet) time.Duration {
				if p.Track == 1 {
					return p.DTS + tc.late
				}
				return p.DTS
			}
			slices.SortStableFunc(pkts, func(a, b *av.Packet) int { return int(key(a) - key(b)) })
			m := NewMuxer(Config{TargetDuration: time.Second, PartDuration: part, PlaylistSize: 20, KeepSegments: -1})
			m.OnTracks([]*av.Track{h264Track(t), aacTrack(t)})
			feed(m, pkts)
			m.Close()
			t.Logf("%s, part %v", tc.name, part)
			checkCuts(t, fmp4Segments(t, m), count(pkts), 1100*time.Millisecond)
		}
	}
}

// sixty makes the video of pkts 60 fps, a keyframe a second, over the same
// time, and orders the packets by decode time.
func sixty(pkts []*av.Packet) []*av.Packet {
	var end time.Duration
	var data []byte
	for _, p := range pkts {
		if p.Track == 0 {
			end, data = max(end, p.DTS), p.Data
		}
	}
	// The video first, so that it goes before the audio of the same time.
	var out []*av.Packet
	for i := 0; time.Second+time.Duration(i)*time.Second/60 <= end; i++ {
		at := time.Second + time.Duration(i)*time.Second/60
		out = append(out, &av.Packet{Track: 0, DTS: at, PTS: at, Keyframe: i%60 == 0, Data: data})
	}
	for _, p := range pkts {
		if p.Track == 1 {
			out = append(out, p)
		}
	}
	slices.SortStableFunc(out, func(a, b *av.Packet) int { return int(a.DTS - b.DTS) })
	return out
}

// An audio track that stops does not hold the cuts up, and a stream that
// ends, or changes its tracks, while a cut waits keeps the keyframe in its
// last segment.
func TestMuxerCutEdges(t *testing.T) {
	pkts := (&source{video: true, audio: 1, gop: 25, base: time.Second}).until(9 * time.Second)
	pkts = slices.DeleteFunc(pkts, func(p *av.Packet) bool { return p.Track == 1 && p.DTS > 3*time.Second })
	m := NewMuxer(Config{TargetDuration: time.Second, PlaylistSize: 20, KeepSegments: -1})
	m.OnTracks([]*av.Track{h264Track(t), aacTrack(t)})
	feed(m, pkts)
	m.Close()
	segs := fmp4Segments(t, m)
	checkCuts(t, segs, count(pkts), time.Second)

	for _, end := range []func(*Muxer){(*Muxer).Close, func(m *Muxer) { m.OnTracks(nil) }} {
		// The last packet is a keyframe, with the audio before it.
		pkts := (&source{video: true, audio: 1, gop: 25, base: time.Second}).until(5 * time.Second)
		for pkts[len(pkts)-1].Track == 1 {
			pkts = pkts[:len(pkts)-1]
		}
		pkts = append(pkts, &av.Packet{Track: 0, DTS: 5 * time.Second, PTS: 5 * time.Second, Keyframe: true, Data: pkts[0].Data})
		m := NewMuxer(Config{TargetDuration: time.Second, PlaylistSize: 20, KeepSegments: -1})
		m.OnTracks([]*av.Track{h264Track(t), aacTrack(t)})
		feed(m, pkts)
		if m.cutKey == nil {
			t.Fatal("the cut does not wait")
		}
		end(m)
		segs := fmp4Segments(t, m)
		checkCuts(t, segs, count(pkts), 1100*time.Millisecond)
		last := segs[len(segs)-1].pkts
		var key *av.Packet
		for _, p := range last {
			if p.Track == 0 {
				key = p
			}
		}
		if !key.Keyframe || key.DTS-last[0].DTS != time.Second {
			t.Fatalf("the last segment's video ends at %v, keyframe %v", key.DTS-last[0].DTS, key.Keyframe)
		}
	}
}

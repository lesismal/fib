package hls

import (
	"cmp"
	"slices"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/fmp4"
	"github.com/lesismal/fib/media/mpegts"
)

// bframes makes the video (track 0) of pkts B-frame video, decoded 80 ms
// before it is shown, and orders them by decode time, as ffmpeg sends them.
func bframes(pkts []*av.Packet) []*av.Packet {
	for _, p := range pkts {
		if p.Track == 0 {
			p.DTS -= 80 * time.Millisecond
		}
	}
	slices.SortStableFunc(pkts, func(a, b *av.Packet) int { return cmp.Compare(a.DTS, b.DTS) })
	return pkts
}

// sameClockItems feeds m a channel of two items on one clock, the second
// with tracks of its own, then a republish whose clock starts at 0 again,
// and returns the counts of video and audio packets in each of the three.
func sameClockItems(t *testing.T, m *Muxer, ts bool) [3][2]int {
	t.Helper()
	tracks := []*av.Track{h264Track(t), aacTrack(t)}
	prep := func(p []*av.Packet) []*av.Packet {
		if ts {
			p = avcc(p)
		}
		return bframes(p)
	}
	src := &source{video: true, audio: 1, gop: 50, base: time.Second}
	items := [3][]*av.Packet{prep(src.until(5 * time.Second)), prep(src.until(9 * time.Second))}
	items[2] = prep((&source{video: true, audio: 1, gop: 50}).until(4 * time.Second))
	// The second item's keyframe is decoded before the first's last
	// audio, as B-frames make it.
	if items[1][0].Track != 0 || !items[1][0].Keyframe || items[1][0].DTS >= items[0][len(items[0])-1].DTS {
		t.Fatal("not a channel's items")
	}
	var n [3][2]int
	for i, item := range items {
		next := slices.Clone(tracks)
		v := *next[0]
		next[0] = &v
		m.OnTracks(next)
		feed(m, item)
		for _, p := range item {
			n[i][p.Track]++
		}
	}
	m.OnTracks(nil)
	return n
}

// checkSameClock checks the packets of the segments, in order: each
// track's decode times run on by its packets' steps through the second
// item (n counts them), with no jump where its tracks changed, then go on,
// never back, after the republish.
func checkSameClock(t *testing.T, pkts []*av.Packet, n [3][2]int) {
	t.Helper()
	var times [2][]time.Duration
	for _, p := range pkts {
		times[p.Track] = append(times[p.Track], p.DTS)
	}
	steps := [2][2]time.Duration{{40 * time.Millisecond, 40 * time.Millisecond}, {21 * time.Millisecond, 22 * time.Millisecond}}
	for tr := range 2 {
		same := n[0][tr] + n[1][tr]
		if len(times[tr]) != same+n[2][tr] {
			t.Fatalf("track %d: %d packets, want %d", tr, len(times[tr]), same+n[2][tr])
		}
		for i := 1; i < same; i++ {
			if d := times[tr][i] - times[tr][i-1]; d < steps[tr][0] || d > steps[tr][1] {
				t.Fatalf("track %d, packet %d: a step of %v", tr, i, d)
			}
		}
		for i := same; i < len(times[tr]); i++ {
			if times[tr][i] <= times[tr][i-1] {
				t.Fatalf("track %d, packet %d: back from %v to %v", tr, i, times[tr][i-1], times[tr][i])
			}
		}
	}
}

// New tracks on the same clock, as a channel's next item or a codec change
// makes, start a new timeline after a discontinuity whose fMP4 decode
// times go on as they ran: the next item's B-frame video, decoded before
// the last item's audio ends, is not moved on past it. A republish
// restarting the clock still carries on from where the timeline ended.
func TestMuxerSameClock(t *testing.T) {
	m := NewMuxer(Config{PlaylistSize: 20, KeepSegments: -1})
	n := sameClockItems(t, m, false)
	p := media(t, m)
	if p.discs != 2 || len(p.maps) != 1 {
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
	var pkts []*av.Packet
	for _, name := range p.segs {
		seg, ok := m.File(name)
		if !ok {
			t.Fatal("no", name)
		}
		for len(seg) > 0 {
			_, got, rest, err := init.ParseFragment(seg)
			if err != nil {
				t.Fatal(name, err)
			}
			pkts = append(pkts, got...)
			seg = rest
		}
	}
	checkSameClock(t, pkts, n)
}

// So do MPEG-TS segments' times.
func TestMuxerSameClockTS(t *testing.T) {
	m := NewMuxer(Config{Format: MPEGTS, PlaylistSize: 20, KeepSegments: -1})
	n := sameClockItems(t, m, true)
	p := media(t, m)
	if p.discs != 2 {
		t.Fatalf("%+v", p)
	}
	// One demuxer reads the segments in order, as a player does, flushed
	// at the end of each, where it would give up the frame a
	// configuration's end left open.
	var pkts []*av.Packet
	d := mpegts.NewDemuxer(mpegts.DemuxerConfig{
		OnTracks: func([]*av.Track) {},
		OnPacket: func(p *av.Packet) { pkts = append(pkts, p) },
	})
	for _, name := range p.segs {
		seg, ok := m.File(name)
		if !ok {
			t.Fatal("no", name)
		}
		d.Write(seg)
		d.Flush()
	}
	checkSameClock(t, pkts, n)
}

package hls

import (
	"bufio"
	"bytes"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
)

// place is where a playlist puts a segment: its discontinuity sequence and
// the initialization section in force, "" for none.
type place struct {
	cc   int
	init string
}

// places returns the place of every segment a Low-Latency media playlist
// lists whole, by its parts, or by its preload hint, by msn, as hls.js makes
// them: the segment to come, whose parts are listed or which only the hint
// names, has the tags in force at the end of the playlist (its fragment
// hint), and a player keeps what it made of it once the segment is listed
// (hls.js's mergeDetails keeps the initialization section). hint is the msn
// of the segment to come.
func places(t testing.TB, b []byte) (out map[uint64]place, hint uint64) {
	t.Helper()
	out = map[uint64]place{}
	var msn uint64
	var cur place
	parts := false
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		l := sc.Text()
		k, v, _ := strings.Cut(l, ":")
		uri := ""
		if _, u, ok := strings.Cut(v, "URI=\""); ok {
			uri, _, _ = strings.Cut(u, "\"")
		}
		switch {
		case k == "#EXT-X-MEDIA-SEQUENCE":
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				t.Fatal(l)
			}
			msn = n
		case k == "#EXT-X-DISCONTINUITY-SEQUENCE":
			n, err := strconv.Atoi(v)
			if err != nil {
				t.Fatal(l)
			}
			cur.cc = n
		case l == "#EXT-X-DISCONTINUITY":
			cur.cc++
		case k == "#EXT-X-MAP":
			cur.init = uri
		case k == "#EXT-X-PART":
			if got := msnOf(t, uri); got != msn {
				t.Fatalf("part %s listed as segment %d: %s", uri, msn, b)
			}
			parts = true
		case k == "#EXT-X-PRELOAD-HINT":
			// The hint names the next part: the first of the segment to
			// come, or the next of its parts.
			if got := msnOf(t, uri); got != msn && (got != msn+1 || !parts) {
				t.Fatalf("hint %s after segment %d: %s", uri, msn, b)
			}
		case l != "" && !strings.HasPrefix(l, "#"):
			out[msn] = cur
			msn++
			parts = false
		}
	}
	out[msn] = cur
	return out, msn
}

// trackPlaces checks that every playlist of m puts every segment where the
// ones before did, and remembers where.
type trackPlaces struct {
	m      *Muxer
	seen   map[uint64]place
	hinted map[uint64]bool // segments seen to come, before any part of them
}

func (tp *trackPlaces) check(t testing.TB) {
	t.Helper()
	b, ok := tp.m.MediaPlaylist(nil)
	if !ok {
		return
	}
	got, hint := places(t, b)
	if !bytes.Contains(b, []byte("URI=\"part-"+tp.m.session+"-"+strconv.FormatUint(hint, 10)+"-0")) ||
		bytes.Contains(b, []byte("#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"part-"+tp.m.session+"-"+strconv.FormatUint(hint, 10)+"-0")) {
		// Nothing of it listed yet, only hinted.
		tp.hinted[hint] = true
	}
	for msn, p := range got {
		if was, ok := tp.seen[msn]; ok && was != p {
			t.Fatalf("segment %d at %+v, earlier at %+v:\n%s", msn, p, was, b)
		}
		tp.seen[msn] = p
	}
}

func (tp *trackPlaces) feed(t testing.TB, pkts []*av.Packet) {
	t.Helper()
	for _, p := range pkts {
		tp.m.OnPacket(p)
		tp.check(t)
	}
}

func (tp *trackPlaces) tracks(t testing.TB, tracks []*av.Track) {
	t.Helper()
	tp.m.OnTracks(tracks)
	tp.check(t)
}

// llTrackChanges plays a stream whose tracks change (a new initialization
// section), which is republished with the same tracks, and whose clock
// jumps, through m in Low-Latency HLS, checking its playlist after every
// packet, and returns where its segments were.
func llTrackChanges(t *testing.T, m *Muxer) *trackPlaces {
	tp := &trackPlaces{m: m, seen: map[uint64]place{}, hinted: map[uint64]bool{}}
	tp.tracks(t, []*av.Track{h264Track(t), aacTrack(t)})
	src := &source{video: true, audio: 1, gop: 25}
	tp.feed(t, src.until(3100*time.Millisecond))
	// New tracks on the same clock, as a channel's next item.
	tp.tracks(t, []*av.Track{h265Track(t), aacTrack(t)})
	src = &source{video: true, audio: 1, gop: 25, base: 3200 * time.Millisecond}
	tp.feed(t, src.until(6300*time.Millisecond))
	// The same tracks again, the clock starting over: a discontinuity, the
	// same initialization section.
	tp.tracks(t, []*av.Track{h265Track(t), aacTrack(t)})
	src = &source{video: true, audio: 1, gop: 25}
	tp.feed(t, src.until(3100*time.Millisecond))
	// The clock jumps forward.
	src.base += time.Minute
	tp.feed(t, src.until(time.Minute+6100*time.Millisecond))
	return tp
}

// The segment after a discontinuity is placed after it, under its
// initialization section, from the first playlist hinting it: hls.js
// appended the parts of a segment whose tracks changed after the last
// initialization section, and failed (bufferAppendError) once the media
// element had failed to decode them, though whole segments played.
func TestMuxerLLHLSHintAfterDiscontinuity(t *testing.T) {
	m := NewMuxer(Config{TargetDuration: time.Second, PartDuration: 250 * time.Millisecond, PlaylistSize: 6})
	tp := llTrackChanges(t, m)
	ccs, inits := map[int]bool{}, map[string]bool{}
	for _, p := range tp.seen {
		ccs[p.cc], inits[p.init] = true, true
	}
	if len(ccs) != 4 || len(inits) != 2 {
		t.Fatalf("%d discontinuity sequences, %d initialization sections: %+v", len(ccs), len(inits), tp.seen)
	}
	// The segments after each discontinuity were hinted before they began.
	changes := 0
	for msn := range tp.hinted {
		if prev, ok := tp.seen[msn-1]; ok && prev != tp.seen[msn] {
			changes++
		}
	}
	if changes != 3 {
		t.Fatalf("%d discontinuities hinted: %v", changes, tp.hinted)
	}
	// Right after new tracks, before their first packet, the playlist ends
	// with the discontinuity and the new map before the hint.
	m.OnTracks([]*av.Track{h264Track(t), aacTrack(t)})
	b, _ := m.MediaPlaylist(nil)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	tail := lines[len(lines)-3:]
	if tail[0] != "#EXT-X-DISCONTINUITY" || !strings.HasPrefix(tail[1], "#EXT-X-MAP:URI=\"init-") || !strings.HasPrefix(tail[2], "#EXT-X-PRELOAD-HINT:") {
		t.Fatalf("%s", b)
	}
	// Once ended, nothing is hinted, nor the segment that was to come.
	m.Close()
	b, _ = m.MediaPlaylist(nil)
	if !bytes.HasSuffix(b, []byte("seg-"+m.session+"-"+strconv.FormatUint(m.next-1, 10)+".m4s\n#EXT-X-ENDLIST\n")) {
		t.Fatalf("%s", b)
	}
}

// MPEG-TS has no initialization sections: the discontinuities alone.
func TestMuxerLLHLSHintAfterDiscontinuityTS(t *testing.T) {
	m := NewMuxer(Config{Format: MPEGTS, TargetDuration: time.Second, PartDuration: 250 * time.Millisecond, PlaylistSize: 6})
	tp := llTrackChanges(t, m)
	ccs := map[int]bool{}
	for _, p := range tp.seen {
		if p.init != "" {
			t.Fatalf("%+v", p)
		}
		ccs[p.cc] = true
	}
	if len(ccs) != 4 {
		t.Fatalf("%+v", tp.seen)
	}
}

// Encrypted, the hint after a new initialization section has the key in
// force named again after its EXT-X-MAP, and no key of a later period.
func TestMuxerLLHLSHintAfterDiscontinuityEncrypted(t *testing.T) {
	tw := newTwin(Config{TargetDuration: time.Second, PartDuration: 250 * time.Millisecond, PlaylistSize: 6}, 2)
	tp := llTrackChanges(t, tw.enc)
	clear := llTrackChanges(t, tw.clear)
	if len(tp.seen) != len(clear.seen) || len(tp.hinted) != len(clear.hinted) {
		t.Fatalf("%+v\n%+v", tp.seen, clear.seen)
	}
	tw.check(t)
	tw.tracks([]*av.Track{h264Track(t), aacTrack(t)})
	tw.check(t)
	b, _ := tw.enc.MediaPlaylist(nil)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	tail := strings.Join(lines[len(lines)-5:], "\n")
	epoch := (tw.enc.next - 1) / 2
	want := "#EXT-X-DISCONTINUITY\n#EXT-X-KEY:METHOD=NONE\n#EXT-X-MAP:URI=\"init-" + tw.enc.session + "-3.mp4\"\n" +
		"#EXT-X-KEY:METHOD=AES-128,URI=\"" + keyURI(tw.enc.session, epoch) + "\"\n" +
		"#EXT-X-PRELOAD-HINT:TYPE=PART,URI=\"" + string(tw.enc.partName(nil, tw.enc.next, 0)) + "\""
	if tail != want {
		t.Fatalf("%s\nwant tail\n%s", b, want)
	}
}

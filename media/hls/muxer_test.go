package hls

import (
	"bufio"
	"bytes"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/aac"
	"github.com/lesismal/fib/media/codec/av1"
	"github.com/lesismal/fib/media/fmp4"
)

func readFile(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func h264Track(t testing.TB) *av.Track {
	return &av.Track{Codec: av.CodecH264, Config: readFile(t, "h264_high_720.avcc")}
}

func h265Track(t testing.TB) *av.Track {
	return &av.Track{Codec: av.CodecH265, Config: readFile(t, "h265_main_720.hvcc")}
}

// av1Track is the 160x120 profile 0 stream of the av1 package's fixture.
func av1Track(t testing.TB) *av.Track {
	b, err := os.ReadFile("../codec/av1/testdata/av1.obu")
	if err != nil {
		t.Fatal(err)
	}
	for o := range av1.SplitOBUs(b) {
		if o.Type() == av1.OBUSequenceHeader {
			c, err := av1.NewConfig(av1.AppendOBU(nil, o, true))
			if err != nil {
				t.Fatal(err)
			}
			return &av.Track{Codec: av.CodecAV1, Config: c.Append(nil)}
		}
	}
	t.Fatal("no sequence header")
	return nil
}

func aacTrack(t testing.TB) *av.Track {
	c := aac.Config{ObjectType: aac.ObjectTypeLC, SampleRate: 48000, ChannelConfig: 2}
	b, err := c.Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	return &av.Track{Codec: av.CodecAAC, Config: b, SampleRate: 48000, Channels: 2}
}

// source makes a stream of 25 fps video with a keyframe every gop frames
// (track 0, when video) and 48 kHz AAC (track audio, when >= 0).
type source struct {
	video  bool
	audio  int
	gop    int
	frame  int
	sample int
	base   time.Duration
}

// until returns the packets up to t, interleaved by DTS.
func (s *source) until(t time.Duration) []*av.Packet {
	var out []*av.Packet
	for {
		vt := s.base + time.Duration(s.frame)*40*time.Millisecond
		at := s.base + av.FromTicks(int64(s.sample)*1024, 48000)
		if (!s.video || vt >= t) && (s.audio < 0 || at >= t) {
			return out
		}
		if s.video && (s.audio < 0 || vt <= at) && vt < t {
			out = append(out, &av.Packet{Track: 0, DTS: vt, PTS: vt, Keyframe: s.frame%s.gop == 0, Data: bytes.Repeat([]byte{byte(s.frame)}, 100)})
			s.frame++
			continue
		}
		out = append(out, &av.Packet{Track: s.audio, DTS: at, PTS: at, Keyframe: true, Data: bytes.Repeat([]byte{byte(s.sample)}, 20)})
		s.sample++
	}
}

func feed(m *Muxer, pkts []*av.Packet) {
	for _, p := range pkts {
		m.OnPacket(p)
	}
}

// playlist is a parsed media playlist.
type playlist struct {
	tags  map[string]string
	segs  []string
	durs  []float64
	parts []string
	discs int
	maps  []string
	hint  string
	ended bool
}

func parsePlaylist(t testing.TB, b []byte) *playlist {
	t.Helper()
	p := &playlist{tags: map[string]string{}}
	sc := bufio.NewScanner(bytes.NewReader(b))
	if !sc.Scan() || sc.Text() != "#EXTM3U" {
		t.Fatalf("no #EXTM3U: %s", b)
	}
	for sc.Scan() {
		l := sc.Text()
		k, v, _ := strings.Cut(l, ":")
		switch {
		case k == "#EXTINF":
			d, err := strconv.ParseFloat(strings.TrimSuffix(v, ","), 64)
			if err != nil {
				t.Fatal(l)
			}
			p.durs = append(p.durs, d)
		case k == "#EXT-X-PART":
			_, uri, _ := strings.Cut(v, "URI=\"")
			uri, _, _ = strings.Cut(uri, "\"")
			p.parts = append(p.parts, uri)
		case k == "#EXT-X-MAP":
			p.maps = append(p.maps, strings.Trim(strings.TrimPrefix(v, "URI="), "\""))
		case k == "#EXT-X-PRELOAD-HINT":
			_, uri, _ := strings.Cut(v, "URI=\"")
			p.hint = strings.TrimSuffix(uri, "\"")
		case l == "#EXT-X-DISCONTINUITY":
			p.discs++
		case l == "#EXT-X-ENDLIST":
			p.ended = true
		case strings.HasPrefix(l, "#"):
			p.tags[k] = v
		default:
			p.segs = append(p.segs, l)
		}
	}
	if len(p.segs) != len(p.durs) {
		t.Fatalf("%d segments, %d EXTINF", len(p.segs), len(p.durs))
	}
	return p
}

func media(t testing.TB, m *Muxer) *playlist {
	t.Helper()
	b, ok := m.MediaPlaylist(nil)
	if !ok {
		t.Fatal("no playlist")
	}
	return parsePlaylist(t, b)
}

// readAll reads the init section and every listed segment back into
// packets.
func readAll(t testing.TB, m *Muxer, p *playlist) []*av.Packet {
	t.Helper()
	initSeg, ok := m.File(p.maps[0])
	if !ok {
		t.Fatal(p.maps[0])
	}
	init, err := fmp4.ParseInit(initSeg)
	if err != nil {
		t.Fatal(err)
	}
	var out []*av.Packet
	for _, name := range p.segs {
		data, ok := m.File(name)
		if !ok {
			t.Fatal(name)
		}
		for len(data) > 0 {
			var pkts []*av.Packet
			_, pkts, data, err = init.ParseFragment(data)
			if err != nil {
				t.Fatal(name, err)
			}
			out = append(out, pkts...)
		}
	}
	return out
}

func TestMuxerSegments(t *testing.T) {
	m := NewMuxer(Config{PlaylistSize: 3, KeepSegments: 1})
	if _, ok := m.MediaPlaylist(nil); ok {
		t.Fatal("playlist before tracks")
	}
	m.OnTracks([]*av.Track{h264Track(t), aacTrack(t), {Codec: av.CodecPCMU}})
	src := &source{video: true, audio: 1, gop: 50}
	// Audio before the first keyframe is left out: start mid-GOP.
	src.frame = 10
	feed(m, src.until(time.Second))
	if _, ok := m.MediaPlaylist(nil); ok {
		t.Fatal("playlist before a segment")
	}
	feed(m, []*av.Packet{{Track: 2, Data: []byte{1}}, {Track: 9}, {Track: -1}})
	feed(m, src.until(13*time.Second))
	p := media(t, m)
	if p.tags["#EXT-X-VERSION"] != "7" || p.tags["#EXT-X-TARGETDURATION"] != "2" || len(p.segs) != 3 || len(p.maps) != 1 || p.ended || p.discs != 0 {
		t.Fatalf("%+v", p)
	}
	// Segments 0..4 are complete (the first starts at 2 s); 0 is gone, 1
	// is kept after leaving the playlist.
	if p.tags["#EXT-X-MEDIA-SEQUENCE"] != "2" {
		t.Fatal(p.tags["#EXT-X-MEDIA-SEQUENCE"])
	}
	for _, d := range p.durs {
		if d != 2 {
			t.Fatal(p.durs)
		}
	}
	kept := strings.Replace(p.segs[0], "-2.m4s", "-1.m4s", 1)
	gone := strings.Replace(p.segs[0], "-2.m4s", "-0.m4s", 1)
	if _, ok := m.File(kept); !ok {
		t.Fatal("kept segment gone")
	}
	if _, ok := m.File(gone); ok {
		t.Fatal("old segment kept")
	}
	pkts := readAll(t, m, p)
	var v, a int
	for _, pk := range pkts {
		if pk.Track == 0 {
			v++
		} else {
			a++
		}
	}
	// Three segments of 2 s: 150 frames, and about 281 AAC frames.
	if v != 150 || a < 278 || a > 284 {
		t.Fatal(v, a)
	}
	if !pkts[0].Keyframe {
		t.Fatal("segment does not start with a keyframe")
	}
	b, ok := m.MultivariantPlaylist(nil, "media.m3u8")
	if !ok || !bytes.Contains(b, []byte(`CODECS="avc1.64001f,mp4a.40.2",RESOLUTION=1280x720`)) || !bytes.Contains(b, []byte("\nmedia.m3u8\n")) {
		t.Fatalf("%s", b)
	}
	st := m.status()
	if !st.hasAV || st.next != 5 || !st.any || st.ended {
		t.Fatalf("%+v", st)
	}
	if m.Bytes() <= 0 {
		t.Fatal("no bytes")
	}
	m.OnClose(errors.New("gone"))
	m.Close()
	if !m.Ended() {
		t.Fatal("not ended")
	}
	p = media(t, m)
	if !p.ended {
		t.Fatal("no ENDLIST")
	}
	// Nothing changes after the end.
	m.OnTracks(nil)
	feed(m, src.until(20*time.Second))
	if q := media(t, m); len(q.segs) != len(p.segs) {
		t.Fatal("changed after the end")
	}
}

func TestMuxerAudioOnly(t *testing.T) {
	m := NewMuxer(Config{TargetDuration: 1500 * time.Millisecond, KeepSegments: -1, PlaylistSize: 2})
	m.OnTracks([]*av.Track{aacTrack(t)})
	src := &source{audio: 0}
	feed(m, src.until(10*time.Second))
	p := media(t, m)
	// The target is 1.5 s rounded up; segments are cut at the first
	// packet 1.5 s on, about 1.51 s.
	if p.tags["#EXT-X-TARGETDURATION"] != "2" || len(p.segs) != 2 {
		t.Fatalf("%+v", p)
	}
	for _, d := range p.durs {
		if d < 1.5 || d > 1.53 {
			t.Fatal(p.durs)
		}
	}
	if m.status().hasAV {
		t.Fatal("audio-only has AV")
	}
	if b, _ := m.MultivariantPlaylist(nil, "x"); bytes.Contains(b, []byte("RESOLUTION")) {
		t.Fatalf("%s", b)
	}
}

func TestMuxerLongGOP(t *testing.T) {
	// Keyframes every 3 s against a 2 s target: segments of 3 s raise the
	// target duration.
	m := NewMuxer(Config{})
	m.OnTracks([]*av.Track{h264Track(t)})
	src := &source{video: true, audio: -1, gop: 75}
	feed(m, src.until(10*time.Second))
	p := media(t, m)
	if p.tags["#EXT-X-TARGETDURATION"] != "3" || p.durs[0] != 3 {
		t.Fatalf("%+v", p)
	}
}

func TestMuxerDiscontinuity(t *testing.T) {
	m := NewMuxer(Config{PlaylistSize: 4, KeepSegments: -1})
	tracks := []*av.Track{h264Track(t), aacTrack(t)}
	m.OnTracks(tracks)
	src := &source{video: true, audio: 1, gop: 50}
	feed(m, src.until(5*time.Second))
	// A republish with the same tracks and restarting timestamps: same
	// init, a discontinuity.
	m.OnTracks(tracks)
	src = &source{video: true, audio: 1, gop: 50}
	feed(m, src.until(4100*time.Millisecond))
	p := media(t, m)
	if p.discs != 1 || len(p.maps) != 1 || len(p.segs) != 4 {
		t.Fatalf("%+v", p)
	}
	// The segment cut short by the republish lasts until its last packet
	// and one more packet's gap.
	if p.durs[1] < 0.9 || p.durs[1] > 1.05 {
		t.Fatal(p.durs)
	}
	// A codec change: a new init, named in a new EXT-X-MAP.
	m.OnTracks([]*av.Track{h265Track(t), aacTrack(t)})
	src = &source{video: true, audio: 1, gop: 50, base: time.Hour}
	feed(m, src.until(time.Hour+4100*time.Millisecond))
	p = media(t, m)
	// The first republish's discontinuity has left the playlist.
	if len(p.maps) != 2 || p.maps[0] == p.maps[1] || p.discs != 1 || p.tags["#EXT-X-DISCONTINUITY-SEQUENCE"] != "1" {
		t.Fatalf("%+v", p)
	}
	// A jump in the clock: forward by a minute.
	src.base += time.Minute
	feed(m, src.until(time.Hour+time.Minute+8100*time.Millisecond))
	p = media(t, m)
	if p.tags["#EXT-X-DISCONTINUITY-SEQUENCE"] != "1" || p.discs != 2 || len(p.maps) != 1 || p.durs[2] > 0.2 {
		t.Fatalf("%+v", p)
	}
	// The first init is gone with its segments; the second stays.
	first := strings.Replace(p.maps[0], "-2.mp4", "-1.mp4", 1)
	if _, ok := m.File(first); ok {
		t.Fatal("unused init kept")
	}
	if _, ok := m.File(p.maps[0]); !ok {
		t.Fatal("init gone")
	}
	// Tracks no format carries: packets are ignored until a usable
	// configuration comes.
	m.OnTracks([]*av.Track{{Codec: av.CodecPCMA}})
	feed(m, []*av.Packet{{Track: 0, Keyframe: true, Data: []byte{1}}})
	if st := m.status(); st.codecs != "" || st.hasAV {
		t.Fatalf("%+v", st)
	}
}

func TestMuxerMaxBytes(t *testing.T) {
	// 4 KB: segments are cut at 1 KB without keyframes, and old ones go.
	m := NewMuxer(Config{MaxBytes: 4096, PlaylistSize: 10})
	m.OnTracks([]*av.Track{h264Track(t)})
	src := &source{video: true, audio: -1, gop: 1000}
	feed(m, src.until(10*time.Second))
	p := media(t, m)
	if len(p.segs) < 2 || m.Bytes() > 4096+2048 {
		t.Fatal(len(p.segs), m.Bytes())
	}
	for _, d := range p.durs {
		if d > 0.5 {
			t.Fatal(p.durs)
		}
	}
}

func TestMuxerLLHLS(t *testing.T) {
	m := NewMuxer(Config{PartDuration: 500 * time.Millisecond, PlaylistSize: 4})
	m.OnTracks([]*av.Track{h264Track(t), aacTrack(t)})
	src := &source{video: true, audio: 1, gop: 50}
	feed(m, src.until(6600*time.Millisecond))
	p := media(t, m)
	if p.tags["#EXT-X-SERVER-CONTROL"] != "CAN-BLOCK-RELOAD=YES,PART-HOLD-BACK=1.500" || p.tags["#EXT-X-PART-INF"] != "PART-TARGET=0.500" {
		t.Fatalf("%+v", p.tags)
	}
	if len(p.segs) != 3 || len(p.parts) < 12 || p.hint == "" {
		t.Fatalf("%+v", p)
	}
	b, _ := m.MediaPlaylist(nil)
	if !bytes.Contains(b, []byte("INDEPENDENT=YES")) {
		t.Fatalf("%s", b)
	}
	// Every part is there, and the parts of a segment make it up.
	var whole []byte
	for _, name := range p.parts {
		data, ok := m.File(name)
		if !ok {
			t.Fatal(name)
		}
		if strings.Contains(name, "-0-") {
			whole = append(whole, data...)
		}
	}
	seg0, _ := m.File(p.segs[0])
	if !bytes.Equal(whole, seg0) {
		t.Fatal("parts do not make up the segment")
	}
	// The hint names the next part, which is worth waiting for, unlike one
	// further on.
	msn, part, ok := m.fileWait(p.hint)
	if !ok || msn != 3 || part != 1 {
		t.Fatal(msn, part, ok)
	}
	if _, _, ok := m.fileWait(strings.Replace(p.hint, "-3-", "-5-", 1)); ok {
		t.Fatal("waits for a far part")
	}
	nextSeg := strings.Replace(p.segs[0], "-0.m4s", "-3.m4s", 1)
	if msn, part, ok := m.fileWait(nextSeg); !ok || msn != 3 || part != -1 {
		t.Fatal(msn, part, ok)
	}
	if _, _, ok := m.fileWait(p.maps[0]); ok {
		t.Fatal("waits for an init")
	}
	if _, ok := m.File(p.hint); ok {
		t.Fatal("hinted part exists")
	}
	if m.ready(msn, part) || !m.ready(2, -1) || !m.ready(msn, part-1) || m.ready(msn, -1) || m.ready(4, 0) {
		t.Fatal("ready")
	}
	// A waiter for the hinted part is woken when it comes.
	woke := make(chan struct{})
	w, err := m.wait(msn, part, func() { close(woke) })
	if err != nil || w == nil {
		t.Fatal(w, err)
	}
	other, _ := m.wait(9, -1, func() { t.Error("woken") })
	if w2, err := m.wait(2, -1, nil); w2 != nil || err != nil {
		t.Fatal("waits for what is there")
	}
	feed(m, src.until(7*time.Second))
	<-woke
	if !m.cancel(other) || m.cancel(other) || m.cancel(w) {
		t.Fatal("cancel")
	}
	if _, ok := m.File(p.hint); !ok {
		t.Fatal("hinted part missing")
	}
	// After the end, the playlist neither hints nor lists an open segment's
	// parts beyond what was written.
	m.Close()
	if p := media(t, m); p.hint != "" || !p.ended {
		t.Fatalf("%+v", p)
	}
	if !m.ready(100, 5) {
		t.Fatal("ended not ready")
	}
	if _, _, ok := m.fileWait(p.hint); ok {
		t.Fatal("waits after the end")
	}
}

func TestMuxerWaiters(t *testing.T) {
	m := NewMuxer(Config{MaxWaiters: 2})
	m.OnTracks([]*av.Track{h264Track(t)})
	var mu sync.Mutex
	woken := 0
	fn := func() { mu.Lock(); woken++; mu.Unlock() }
	if w, err := m.wait(-1, -1, fn); w == nil || err != nil {
		t.Fatal(err)
	}
	if w, err := m.wait(0, -1, fn); w == nil || err != nil {
		t.Fatal(err)
	}
	if _, err := m.wait(0, -1, fn); err != ErrTooManyWaiters {
		t.Fatal(err)
	}
	src := &source{video: true, audio: -1, gop: 50}
	feed(m, src.until(2100*time.Millisecond))
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := woken
		mu.Unlock()
		if n == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestNames(t *testing.T) {
	m := NewMuxer(Config{})
	s := m.session
	for _, name := range []string{
		"", "x", "init-" + s + "-1.m4s", "seg-" + s + "-1.mp4", "seg-other-1.m4s",
		"seg-" + s + "-x.m4s", "seg-" + s + "-1-2.m4s", "part-" + s + "-1.m4s",
		"part-" + s + "-1-x.m4s", "part-" + s + "-1--1.m4s",
	} {
		if kind, _, _ := m.parseName(name); kind != fileNone {
			t.Fatal(name, kind)
		}
		if _, ok := m.File(name); ok {
			t.Fatal(name)
		}
	}
	if kind, a, b := m.parseName("part-" + s + "-7-3.m4s"); kind != filePart || a != 7 || b != 3 {
		t.Fatal(kind, a, b)
	}
	if kind, a, _ := m.parseName("init-" + s + "-2.mp4"); kind != fileInit || a != 2 {
		t.Fatal(kind, a)
	}
	if m.segment(0) != nil {
		t.Fatal("segment")
	}
	// Two Muxers never share names.
	if NewMuxer(Config{}).session == s {
		t.Fatal("same session")
	}
}

func TestCodecString(t *testing.T) {
	he := aac.Config{ObjectType: aac.ObjectTypeLC, SampleRate: 24000, ChannelConfig: 2, ExtObjectType: aac.ObjectTypeSBR, ExtSampleRate: 48000}
	heb, _ := he.Append(nil)
	for _, c := range []struct {
		t    *av.Track
		want string
	}{
		{nil, ""},
		{h264Track(t), "avc1.64001f"},
		{h265Track(t), "hvc1.1.6.L93.90"},
		{aacTrack(t), "mp4a.40.2"},
		{&av.Track{Codec: av.CodecAAC, Config: heb}, "mp4a.40.5"},
		{&av.Track{Codec: av.CodecAAC}, "mp4a.40.2"},
		{&av.Track{Codec: av.CodecOpus}, "opus"},
		{&av.Track{Codec: av.CodecMP3}, "mp4a.40.34"},
		{&av.Track{Codec: av.CodecPCMU}, ""},
		{&av.Track{Codec: av.CodecH264}, ""},
		{&av.Track{Codec: av.CodecH265}, ""},
		{&av.Track{Codec: av.CodecAV1, Config: []byte{0x81, 0x08, 0x0c, 0}}, "av01.0.08M.08"},
		{&av.Track{Codec: av.CodecAV1, Config: []byte{0x81, 0x4d, 0xc0, 0}}, "av01.2.13H.10"},
		{&av.Track{Codec: av.CodecAV1, Config: []byte{0x81, 0x4d, 0xe0, 0}}, "av01.2.13H.12"},
		{&av.Track{Codec: av.CodecAV1, Config: []byte{0x81}}, ""},
		{&av.Track{Codec: av.CodecAV1, Config: []byte{0x81, 0x08, 0x0c}}, ""},
		{&av.Track{Codec: av.CodecAV1, Config: []byte{0x82, 0x08, 0x0c, 0}}, ""},
		{av1Track(t), "av01.0.00M.08"},
	} {
		if got := CodecString(c.t); got != c.want {
			t.Errorf("%+v: %q, want %q", c.t, got, c.want)
		}
	}
	for _, c := range []struct {
		t    *av.Track
		w, h int
	}{
		{h264Track(t), 1280, 720},
		{h265Track(t), 1280, 720},
		{&av.Track{Codec: av.CodecH264, Width: 3, Height: 4}, 3, 4},
		{&av.Track{Codec: av.CodecAV1}, 0, 0},
		{av1Track(t), 160, 120},
	} {
		if w, h := videoSize(c.t); w != c.w || h != c.h {
			t.Errorf("%+v: %dx%d", c.t, w, h)
		}
	}
}

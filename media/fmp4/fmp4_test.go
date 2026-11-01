package fmp4

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/aac"
	"github.com/lesismal/fib/media/codec/h264"
	"github.com/lesismal/fib/media/codec/opus"
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

func aacTrack(t testing.TB, rate int) *av.Track {
	c := aac.Config{ObjectType: aac.ObjectTypeLC, SampleRate: rate, ChannelConfig: 2}
	b, err := c.Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	return &av.Track{Codec: av.CodecAAC, Config: b, SampleRate: rate, Channels: 2}
}

func opusTrack(t testing.TB, h *opus.Head) *av.Track {
	if h == nil {
		return &av.Track{Codec: av.CodecOpus, Channels: 2, SampleRate: 48000}
	}
	b, err := h.Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	return &av.Track{Codec: av.CodecOpus, Config: b, Channels: int(h.Channels), SampleRate: 48000}
}

// av1Config is an AV1CodecConfigurationRecord of main profile, level 4.0,
// 8-bit 4:2:0, without OBUs.
var av1Config = []byte{0x81, 0x08, 0x0c, 0x00}

func TestNextBox(t *testing.T) {
	b, at := begin(nil, "free")
	b = append(b, 1, 2, 3)
	b = end(b, at)
	box, rest, err := NextBox(append(b, 9))
	if err != nil || box.Type.String() != "free" || !bytes.Equal(box.Data, []byte{1, 2, 3}) || box.Size() != 11 || len(rest) != 1 {
		t.Fatal(box, rest, err)
	}
	// A size of 0 runs to the end.
	zero := []byte{0, 0, 0, 0, 'm', 'd', 'a', 't', 7, 7}
	if box, rest, err := NextBox(zero); err != nil || len(box.Data) != 2 || len(rest) != 0 {
		t.Fatal(box, rest, err)
	}
	// A 64-bit largesize.
	large := []byte{0, 0, 0, 1, 'm', 'd', 'a', 't', 0, 0, 0, 0, 0, 0, 0, 17, 5}
	if box, _, err := NextBox(large); err != nil || box.HeaderSize != 16 || !bytes.Equal(box.Data, []byte{5}) {
		t.Fatal(box, err)
	}
	for _, bad := range [][]byte{
		{0, 0, 0},
		{0, 0, 0, 1, 'm', 'd', 'a', 't', 0},
		{0, 0, 0, 7, 'f', 'r', 'e', 'e'},
		{0, 0, 0, 9, 'f', 'r', 'e', 'e'},
		{0, 0, 0, 1, 'm', 'd', 'a', 't', 0, 0, 0, 0, 0, 0, 0, 8},
	} {
		if _, _, err := NextBox(bad); err == nil {
			t.Fatalf("%x parsed", bad)
		}
	}
	n := 0
	for _, err := range Boxes(append(b, 0, 0)) {
		n++
		if n == 2 && err == nil {
			t.Fatal("trailing bytes parsed")
		}
	}
	for range Boxes(append(b, b...)) {
		break
	}
	if n != 2 {
		t.Fatal(n)
	}
}

func TestAppendInitErrors(t *testing.T) {
	if _, err := AppendInit(nil, nil); err != ErrNoTracks {
		t.Fatal(err)
	}
	bad := []*av.Track{
		nil,
		{Codec: av.CodecPCMU},
		{Codec: av.CodecH264, Config: []byte{1}},
		{Codec: av.CodecH265, Config: []byte{1}},
		{Codec: av.CodecAV1, Config: []byte{1, 2, 3, 4}},
		{Codec: av.CodecAV1, Config: []byte{0x82, 0x08, 0x0c, 0}},
		{Codec: av.CodecAAC, Config: []byte{0xff}},
		{Codec: av.CodecOpus, Config: []byte("OpusHead")},
	}
	for _, tr := range bad {
		if _, err := AppendInit(nil, []*av.Track{tr}); err == nil {
			t.Fatalf("%+v accepted", tr)
		}
	}
	if _, err := NewWriter(bad); err != ErrNoTracks {
		t.Fatal(err)
	}
	for _, c := range []av.CodecID{av.CodecH264, av.CodecH265, av.CodecAV1, av.CodecAAC, av.CodecOpus} {
		if !Supported(c) {
			t.Fatal(c)
		}
	}
	if Supported(av.CodecMP3) {
		t.Fatal("mp3")
	}
}

func TestInitRoundTrip(t *testing.T) {
	head := &opus.Head{Channels: 6, PreSkip: 312, InputSampleRate: 44100, OutputGain: -3, MappingFamily: 1, StreamCount: 4, CoupledCount: 2, ChannelMapping: []byte{0, 4, 1, 2, 3, 5}}
	tracks := []*av.Track{
		h264Track(t),
		h265Track(t),
		{Codec: av.CodecAV1, Config: av1Config, Width: 640, Height: 360},
		aacTrack(t, 44100),
		opusTrack(t, head),
		opusTrack(t, nil),
		aacTrack(t, 96000),
	}
	b, err := AppendInit([]byte("x"), tracks)
	if err != nil {
		t.Fatal(err)
	}
	b = b[1:]
	ftyp, _, err := NextBox(b)
	if err != nil || ftyp.Type.String() != "ftyp" || string(ftyp.Data[:4]) != "iso5" || !bytes.Contains(ftyp.Data, []byte("iso6mp41")) {
		t.Fatal(ftyp, err)
	}
	init, err := ParseInit(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(init.Tracks) != len(tracks) {
		t.Fatal(len(init.Tracks))
	}
	wantScale := []uint32{90000, 90000, 90000, 44100, 48000, 48000, 96000}
	for i, it := range init.Tracks {
		if it.ID != uint32(i+1) || it.Timescale != wantScale[i] || it.Track.Codec != tracks[i].Codec {
			t.Fatalf("%d: %+v %+v", i, it, it.Track)
		}
	}
	if !bytes.Equal(init.Tracks[0].Track.Config, tracks[0].Config) || init.Tracks[0].Track.Width != 1280 || init.Tracks[0].Track.Height != 720 {
		t.Fatalf("%+v", init.Tracks[0].Track)
	}
	if !bytes.Equal(init.Tracks[1].Track.Config, tracks[1].Config) || init.Tracks[1].Track.Width != 1280 {
		t.Fatalf("%+v", init.Tracks[1].Track)
	}
	if !bytes.Equal(init.Tracks[2].Track.Config, av1Config) || init.Tracks[2].Track.Width != 640 {
		t.Fatalf("%+v", init.Tracks[2].Track)
	}
	if a := init.Tracks[3].Track; !bytes.Equal(a.Config, tracks[3].Config) || a.SampleRate != 44100 || a.Channels != 2 {
		t.Fatalf("%+v", a)
	}
	var h opus.Head
	if err := h.Unmarshal(init.Tracks[4].Track.Config); err != nil || h.Channels != 6 || h.OutputGain != -3 || !bytes.Equal(h.ChannelMapping, head.ChannelMapping) || h.InputSampleRate != 44100 {
		t.Fatalf("%+v %v", h, err)
	}
	if err := h.Unmarshal(init.Tracks[5].Track.Config); err != nil || h.Channels != 2 || h.PreSkip != 312 {
		t.Fatalf("%+v %v", h, err)
	}
	// 96 kHz does not fit the sample entry's 16.16 rate; the timescale and
	// the config carry it.
	if a := init.Tracks[6].Track; a.SampleRate != 96000 {
		t.Fatalf("%+v", a)
	}
}

func TestLengthSizeRewritten(t *testing.T) {
	var c h264.AVCConfig
	if err := c.Unmarshal(readFile(t, "h264_high_720.avcc")); err != nil {
		t.Fatal(err)
	}
	c.LengthSize = 2
	two, _ := c.Append(nil)
	b, err := AppendInit(nil, []*av.Track{{Codec: av.CodecH264, Config: two, Width: 320, Height: 240}})
	if err != nil {
		t.Fatal(err)
	}
	init, err := ParseInit(b)
	if err != nil {
		t.Fatal(err)
	}
	var got h264.AVCConfig
	if err := got.Unmarshal(init.Tracks[0].Track.Config); err != nil || got.LengthSize != 4 || init.Tracks[0].Track.Width != 320 {
		t.Fatal(got, err)
	}

	var hc = h265Track(t)
	raw := bytes.Clone(hc.Config)
	raw[21] = raw[21]&^3 | 1 // lengthSizeMinusOne = 1
	b, err = AppendInit(nil, []*av.Track{{Codec: av.CodecH265, Config: raw}})
	if err != nil {
		t.Fatal(err)
	}
	init, err = ParseInit(b)
	if err != nil || init.Tracks[0].Track.Config[21]&3 != 3 {
		t.Fatal(err)
	}
}

// pkt makes a packet of track at dts ms with a cts of cts ms.
func pkt(track int, dts, cts int, key bool, data string) *av.Packet {
	d := time.Duration(dts) * time.Millisecond
	return &av.Packet{Track: track, DTS: d, PTS: d + time.Duration(cts)*time.Millisecond, Keyframe: key, Data: []byte(data)}
}

func TestWriterRoundTrip(t *testing.T) {
	tracks := []*av.Track{h264Track(t), {Codec: av.CodecPCMA}, aacTrack(t, 48000)}
	w, err := NewWriter(tracks)
	if err != nil {
		t.Fatal(err)
	}
	if got := w.Tracks(); len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Fatal(got)
	}
	if w.Timescale(0) != 90000 || w.Timescale(2) != 48000 || w.Timescale(1) != 0 || w.Timescale(9) != 0 {
		t.Fatal("timescales")
	}
	init, err := ParseInit(w.Init())
	if err != nil {
		t.Fatal(err)
	}
	// Video at 25 fps with B-frames (I P B B), audio every 20 ms.
	var in []*av.Packet
	cts := []int{80, 200, 40, 40}
	for i := range 8 {
		in = append(in, pkt(0, 1000+i*40, cts[i%4], i%4 == 0, "v"+string(rune('a'+i))))
		in = append(in, pkt(2, 1000+i*40, 0, true, "a"+string(rune('a'+i))))
		in = append(in, pkt(2, 1000+i*40+20, 0, true, "A"+string(rune('a'+i))))
		in = append(in, pkt(1, 1000+i*40, 0, true, "ignored"))
	}
	w.WritePacket(pkt(7, 0, 0, true, "ignored"))
	w.WritePacket(nil)

	var frags [][]byte
	var syncs []bool
	for i, p := range in {
		if p.Track == 0 && p.Keyframe && i > 0 {
			f, sync := w.Flush(nil, p)
			frags, syncs = append(frags, f), append(syncs, sync)
		}
		w.WritePacket(p)
	}
	if w.Buffered() == 0 {
		t.Fatal("nothing buffered")
	}
	f, sync := w.Flush(nil, nil)
	frags, syncs = append(frags, f), append(syncs, sync)
	if w.Buffered() != 0 {
		t.Fatal(w.Buffered())
	}
	if f, sync := w.Flush(nil, nil); len(f) != 0 || sync {
		t.Fatal("empty flush wrote")
	}
	if len(frags) != 2 || !syncs[0] || !syncs[1] {
		t.Fatal(len(frags), syncs)
	}
	var out []*av.Packet
	for i, f := range frags {
		seq, pkts, rest, err := init.ParseFragment(f)
		if err != nil || seq != uint32(i+1) || len(rest) != 0 {
			t.Fatal(seq, err, len(rest))
		}
		out = append(out, pkts...)
	}
	// The first fragment ends before the second keyframe: four video
	// frames and the audio before 1160 ms but the last frame, held.
	_, first, _, _ := init.ParseFragment(frags[0])
	nv, na := 0, 0
	for _, p := range first {
		if p.Track == 0 {
			nv++
		} else {
			na++
		}
	}
	if nv != 4 || na != 7 {
		t.Fatal(nv, na)
	}
	var wantV, wantA, gotV, gotA []*av.Packet
	for _, p := range in {
		switch p.Track {
		case 0:
			wantV = append(wantV, p)
		case 2:
			wantA = append(wantA, p)
		}
	}
	for _, p := range out {
		if p.Track == 0 {
			gotV = append(gotV, p)
		} else if p.Track == 1 {
			gotA = append(gotA, p)
		} else {
			t.Fatal(p.Track)
		}
	}
	check := func(want, got []*av.Packet, ts int) {
		t.Helper()
		if len(want) != len(got) {
			t.Fatal(len(want), len(got))
		}
		for i := range want {
			w, g := want[i], got[i]
			if av.ToTicks(g.DTS, ts) != av.ToTicks(w.DTS-time.Second, ts) || av.ToTicks(g.PTS-g.DTS, ts) != av.ToTicks(w.PTS-w.DTS, ts) || g.Keyframe != w.Keyframe || !bytes.Equal(g.Data, w.Data) {
				t.Fatalf("%d: %+v != %+v", i, g, w)
			}
		}
	}
	check(wantV, gotV, 90000)
	check(wantA, gotA, 48000)
}

func TestWriterDurations(t *testing.T) {
	tracks := []*av.Track{h264Track(t), aacTrack(t, 44100), opusTrack(t, nil), h265Track(t)}
	w, err := NewWriter(tracks)
	if err != nil {
		t.Fatal(err)
	}
	init, _ := ParseInit(w.Init())
	// Track 1 starts before track 0, moving the start of decode times back
	// to it; track 0 jumps back, then forward by more than maxGap: both keep
	// the previous duration.
	w.WritePacket(pkt(0, 100, 0, true, "v0"))
	w.WritePacket(pkt(1, 50, 0, true, "a0"))
	w.WritePacket(pkt(0, 140, 0, false, "v1"))
	w.WritePacket(pkt(0, 120, 0, false, "v2"))
	w.WritePacket(pkt(0, 20000, 0, false, "v3"))
	w.WritePacket(pkt(0, 20040, 0, false, "v4"))
	// An Opus packet of 20 ms (config 3: SILK NB 60 ms? no: TOC 0x08 is
	// 20 ms SILK NB), then the end.
	w.WritePacket(&av.Packet{Track: 2, DTS: 200 * time.Millisecond, PTS: 200 * time.Millisecond, Keyframe: true, Data: []byte{0x08, 1}})
	// An Opus packet whose duration cannot be read falls back on 20 ms.
	w.WritePacket(&av.Packet{Track: 2, DTS: 300 * time.Millisecond, PTS: 300 * time.Millisecond, Data: []byte{}})
	w.WritePacket(&av.Packet{Track: 2, DTS: 400 * time.Millisecond, PTS: 400 * time.Millisecond, Data: []byte{}})
	w.WritePacket(pkt(3, 100, 0, true, "h"))
	f, _ := w.Flush(nil, nil)
	_, pkts, _, err := init.ParseFragment(f)
	if err != nil {
		t.Fatal(err)
	}
	var v, a, o, h []time.Duration
	for _, p := range pkts {
		switch p.Track {
		case 0:
			v = append(v, p.DTS)
		case 1:
			a = append(a, p.DTS)
		case 2:
			o = append(o, p.DTS)
		case 3:
			h = append(h, p.DTS)
		}
	}
	ms := time.Millisecond
	if len(v) != 5 || v[0] != 50*ms || v[1] != 90*ms || v[2] != 130*ms || v[3] != 170*ms || v[4] != 210*ms {
		t.Fatal(v)
	}
	if len(a) != 1 || a[0] != 0 {
		t.Fatal(a)
	}
	if len(o) != 3 || o[0] != 150*ms || o[1] != 250*ms || o[2] != 350*ms {
		t.Fatal(o)
	}
	if len(h) != 1 || h[0] != 50*ms {
		t.Fatal(h)
	}
	// The durations of last samples: a 30 fps frame for video with no
	// history, an AAC frame, the Opus packet's own.
	frag, _ := init.ParseFragmentDurations(f)
	if frag[3][0] != frameTicks || frag[1][0] != 1024 || frag[2][0] != 4800 || frag[2][2] != 4800 {
		t.Fatal(frag)
	}
	w2, _ := NewWriter([]*av.Track{opusTrack(t, nil)})
	init2, _ := ParseInit(w2.Init())
	w2.WritePacket(&av.Packet{DTS: 0, Data: []byte{0x08, 1}})
	f, _ = w2.Flush(nil, nil)
	if d, _ := init2.ParseFragmentDurations(f); d[0][0] != 960 {
		t.Fatal(d)
	}
	w2.WritePacket(&av.Packet{DTS: time.Second, Data: []byte{}})
	f, _ = w2.Flush(nil, nil)
	if d, _ := init2.ParseFragmentDurations(f); d[0][0] != 960 {
		t.Fatal(d)
	}
	w3, _ := NewWriter([]*av.Track{opusTrack(t, nil)})
	w3.WritePacket(&av.Packet{DTS: 0, Data: []byte{}})
	f, _ = w3.Flush(nil, nil)
	if d, _ := init2.ParseFragmentDurations(f); d[0][0] != 960 {
		t.Fatal(d)
	}
	// A composition offset past 32 bits is dropped.
	w4, _ := NewWriter([]*av.Track{h264Track(t)})
	w4.WritePacket(&av.Packet{DTS: 0, PTS: 100000 * time.Second, Keyframe: true})
	f, _ = w4.Flush(nil, nil)
	_, pkts, _, _ = init2.ParseFragment(f)
	if len(pkts) != 1 || pkts[0].PTS != pkts[0].DTS {
		t.Fatal(pkts)
	}
}

// ParseFragmentDurations returns the sample durations of each track of the
// fragment f, for tests.
func (init *Init) ParseFragmentDurations(f []byte) (map[int][]uint32, error) {
	_, pkts, _, err := init.ParseFragment(f)
	if err != nil {
		return nil, err
	}
	out := map[int][]uint32{}
	last := map[int]*av.Packet{}
	for _, p := range pkts {
		if q := last[p.Track]; q != nil {
			ts := int(init.Tracks[p.Track].Timescale)
			out[p.Track] = append(out[p.Track], uint32(av.ToTicks(p.DTS, ts)-av.ToTicks(q.DTS, ts)))
		}
		last[p.Track] = p
	}
	// The last sample's duration is read from the trun directly.
	moof, _, _ := NextBox(f)
	for box := range Boxes(moof.Data) {
		if box.Type.String() != "traf" {
			continue
		}
		tfhd, _ := child(box.Data, "tfhd")
		id := binary.BigEndian.Uint32(tfhd[4:])
		trun, _ := child(box.Data, "trun")
		_, flags, p, _ := fullHeader(trun)
		n := int(binary.BigEndian.Uint32(p))
		per := 8
		if flags&trunFlags != 0 {
			per = 16
		}
		dur := binary.BigEndian.Uint32(p[8+(n-1)*per:])
		i := init.index(id)
		out[i] = append(out[i], dur)
	}
	return out, nil
}

func TestStypSidx(t *testing.T) {
	w, _ := NewWriter([]*av.Track{aacTrack(t, 48000)})
	init, _ := ParseInit(w.Init())
	w.WritePacket(pkt(0, 0, 0, true, "aa"))
	f, _ := w.Flush(nil, nil)
	sidx := Sidx{ReferenceID: 1, Timescale: 48000, EarliestPresentationTime: 5, References: []SidxReference{{Size: uint32(len(f)), Duration: 1024, StartsWithSAP: true}, {Size: 1}}}
	seg := sidx.Append(AppendStyp(nil))
	seg = append(seg, f...)
	box, rest, _ := NextBox(seg)
	if box.Type.String() != "styp" {
		t.Fatal(box.Type)
	}
	box, _, _ = NextBox(rest)
	if box.Type.String() != "sidx" || len(box.Data) != 4+28+2*12 {
		t.Fatal(box.Type, len(box.Data))
	}
	_, pkts, _, err := init.ParseFragment(seg)
	if err != nil || len(pkts) != 1 || string(pkts[0].Data) != "aa" {
		t.Fatal(pkts, err)
	}
}

func TestParseErrors(t *testing.T) {
	tracks := []*av.Track{h264Track(t), aacTrack(t, 48000), opusTrack(t, nil)}
	w, _ := NewWriter(tracks)
	initSeg := w.Init()
	init, err := ParseInit(initSeg)
	if err != nil {
		t.Fatal(err)
	}
	w.WritePacket(pkt(0, 0, 40, true, "video"))
	w.WritePacket(pkt(1, 0, 0, true, "audio"))
	w.WritePacket(pkt(2, 0, 0, true, "\x08opus"))
	frag, _ := w.Flush(nil, nil)
	// Every truncation of either fails cleanly or parses; none panics.
	for n := range initSeg {
		_, _ = ParseInit(initSeg[:n])
	}
	for n := range frag {
		_, _, _, _ = init.ParseFragment(frag[:n])
	}
	// So does every single-byte corruption.
	for i := range initSeg {
		b := bytes.Clone(initSeg)
		b[i] ^= 0xff
		_, _ = ParseInit(b)
	}
	for i := range frag {
		b := bytes.Clone(frag)
		b[i] ^= 0xff
		_, _, _, _ = init.ParseFragment(b)
	}
	if _, err := ParseInit(nil); err != ErrNoMoov {
		t.Fatal(err)
	}
	empty, at := begin(nil, "moov")
	empty = end(empty, at)
	if _, err := ParseInit(empty); err != ErrNoTracks {
		t.Fatal(err)
	}
	if _, _, _, err := init.ParseFragment(nil); err != ErrNoMoof {
		t.Fatal(err)
	}
	// A fragment of a track the init segment does not have.
	other, _ := ParseInit(mustInit(t, []*av.Track{aacTrack(t, 48000)}))
	if _, _, _, err := other.ParseFragment(frag); !errors.Is(err, ErrTrack) {
		t.Fatal(err)
	}
	// Truncated mdat: sample data outside the fragment.
	if _, _, _, err := init.ParseFragment(frag[:len(frag)-1]); err == nil {
		t.Fatal("short mdat parsed")
	}
}

func mustInit(t testing.TB, tracks []*av.Track) []byte {
	b, err := AppendInit(nil, tracks)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestReadForeign reads boxes as other writers lay them out: version 1
// tkhd and mdhd, tfhd defaults and a base data offset, a version 0 tfdt and
// trun, first-sample flags, an esds with optional ES fields, and an unknown
// sample entry.
func TestReadForeign(t *testing.T) {
	asc := aacTrack(t, 48000).Config
	es := []byte{0, 1, 0x80 | 0x40 | 0x20, 0, 2, 1, 'u', 0, 3}
	dcd := append([]byte{0x40, 0x15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, 0x05, byte(len(asc)))
	dcd = append(dcd, asc...)
	es = append(es, 0x04, byte(len(dcd)))
	es = append(es, dcd...)
	esdsPayload := append([]byte{0, 0, 0, 0, 0x03, byte(len(es))}, es...)

	b, moov := begin(nil, "moov")
	b = trak(b, 1, 1, 1000, "mp4a", esdsPayload)
	b = trak(b, 2, 0, 90000, "avc1", nil)
	b = trak(b, 3, 0, 90000, "zzzz", nil)
	b, mvex := begin(b, "mvex")
	b, at := beginFull(b, "trex", 0, 0)
	b = u32(b, 1)
	b = u32(b, 1)
	b = u32(b, 10)
	b = u32(b, 3)
	b = u32(b, NonSyncSampleFlags)
	b = end(b, at)
	b = end(b, mvex)
	b = end(b, moov)
	init, err := ParseInit(b)
	if err != nil {
		t.Fatal(err)
	}
	a := init.Tracks[0]
	if a.ID != 1 || a.Timescale != 1000 || a.Track.Codec != av.CodecAAC || !bytes.Equal(a.Track.Config, asc) || a.DefaultSize != 3 || a.Track.SampleRate != 48000 {
		t.Fatalf("%+v %+v", a, a.Track)
	}
	if init.Tracks[1].Track.Codec != av.CodecUnknown || init.Tracks[2].Track.Codec != av.CodecUnknown {
		t.Fatal("unknown entries")
	}

	// A fragment with tfhd defaults (duration 20, size 2) and a base data
	// offset, a v0 tfdt and a v0 trun with first-sample flags.
	f, moof := begin(nil, "moof")
	f, traf := begin(f, "traf")
	f, at = beginFull(f, "tfhd", 0, 0x01|0x02|0x08|0x10|0x20)
	f = u32(f, 1)
	f = u64(f, 0)
	f = u32(f, 1)
	f = u32(f, 20)
	f = u32(f, 2)
	f = u32(f, NonSyncSampleFlags)
	f = end(f, at)
	f, at = beginFull(f, "tfdt", 0, 0)
	f = u32(f, 100)
	f = end(f, at)
	f, at = beginFull(f, "trun", 0, 0x01|0x04|0x800)
	f = u32(f, 2)
	dataAt := len(f)
	f = u32(f, 0)
	f = u32(f, SyncSampleFlags)
	f = u32(f, 5)
	f = u32(f, 0)
	f = end(f, at)
	f = end(f, traf)
	f = end(f, moof)
	binary.BigEndian.PutUint32(f[dataAt:], uint32(len(f)+8))
	f = u32(f, 12)
	f = append(f, "mdat"...)
	f = append(f, "abcd"...)
	_, pkts, _, err := init.ParseFragment(f)
	if err != nil || len(pkts) != 2 {
		t.Fatal(pkts, err)
	}
	if !pkts[0].Keyframe || pkts[1].Keyframe || string(pkts[0].Data) != "ab" || string(pkts[1].Data) != "cd" || pkts[0].DTS != 100*time.Millisecond || pkts[1].DTS != 120*time.Millisecond || pkts[0].PTS != 105*time.Millisecond {
		t.Fatalf("%+v %+v", pkts[0], pkts[1])
	}
	// A negative data offset.
	g := bytes.Clone(f)
	binary.BigEndian.PutUint32(g[dataAt:], 0xffffff00)
	if _, _, _, err := init.ParseFragment(g); err != ErrSampleData {
		t.Fatal(err)
	}
}

// trak builds a trak with a tkhd and mdhd of version v and one sample entry
// of format, with children after its fixed fields.
func trak(b []byte, id uint32, v uint8, timescale uint32, format string, children []byte) []byte {
	b, trak := begin(b, "trak")
	b, at := beginFull(b, "tkhd", v, 3)
	if v == 1 {
		b = zeros(b, 16)
	} else {
		b = zeros(b, 8)
	}
	b = u32(b, id)
	b = zeros(b, 60)
	b = end(b, at)
	b, mdia := begin(b, "mdia")
	b, at = beginFull(b, "mdhd", v, 0)
	if v == 1 {
		b = zeros(b, 16)
	} else {
		b = zeros(b, 8)
	}
	b = u32(b, timescale)
	b = zeros(b, 8)
	b = end(b, at)
	b, minf := begin(b, "minf")
	b, stbl := begin(b, "stbl")
	b, at = beginFull(b, "stsd", 0, 0)
	b = u32(b, 1)
	b, e := begin(b, format)
	if format == "mp4a" {
		b = zeros(b, 28)
		var esds int
		b, esds = begin(b, "esds")
		b = append(b, children...)
		b = end(b, esds)
	}
	b = end(b, e)
	b = end(b, at)
	b = end(b, stbl)
	b = end(b, minf)
	b = end(b, mdia)
	return end(b, trak)
}

func TestFragmentEmptyTracks(t *testing.T) {
	f := Fragment{Sequence: 9, Tracks: []TrackFragment{{TrackID: 1}, {TrackID: 2, Samples: []Sample{{Duration: 1, Data: []byte("x")}}}}}
	b, err := f.Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	init, _ := ParseInit(mustInit(t, []*av.Track{aacTrack(t, 48000), aacTrack(t, 48000)}))
	seq, pkts, _, err := init.ParseFragment(b)
	if err != nil || seq != 9 || len(pkts) != 1 || pkts[0].Track != 1 {
		t.Fatal(seq, pkts, err)
	}
}

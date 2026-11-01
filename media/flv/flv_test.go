package flv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/lesismal/fib/media/amf"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/opus"
)

func readFixture(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type copiedTag struct {
	Type      uint8
	Timestamp uint32
	Data      []byte
}

func demuxAll(t testing.TB, b []byte, chunk int) ([]copiedTag, Header) {
	t.Helper()
	d := NewDemuxer(DemuxerConfig{})
	var tags []copiedTag
	fn := func(tag Tag) error {
		tags = append(tags, copiedTag{tag.Type, tag.Timestamp, append([]byte(nil), tag.Data...)})
		return nil
	}
	for len(b) > 0 {
		n := min(chunk, len(b))
		if err := d.Feed(b[:n], fn); err != nil {
			t.Fatal(err)
		}
		b = b[n:]
	}
	h, ok := d.Header()
	if !ok {
		t.Fatal("no header")
	}
	return tags, h
}

type collected struct {
	tracks  [][]*av.Track
	packets []*av.Packet
}

func decodeTags(t testing.TB, tags []copiedTag) *collected {
	t.Helper()
	c := &collected{}
	d := NewDecoder(DecoderConfig{
		OnTracks: func(tracks []*av.Track) { c.tracks = append(c.tracks, tracks) },
		OnPacket: func(pkt *av.Packet) { c.packets = append(c.packets, pkt) },
	})
	for _, tag := range tags {
		if err := d.WriteTag(tag.Type, tag.Timestamp, tag.Data); err != nil {
			t.Fatalf("tag %d at %d: %v", tag.Type, tag.Timestamp, err)
		}
	}
	d.Flush()
	return c
}

func TestFixtures(t *testing.T) {
	cases := []struct {
		file   string
		tracks []av.Track
		header Header
	}{
		{"h264_aac.flv", []av.Track{{Codec: av.CodecH264, Width: 160, Height: 120}, {Codec: av.CodecAAC, SampleRate: 44100, Channels: 1}}, Header{true, true}},
		{"hevc_aac.flv", []av.Track{{Codec: av.CodecH265, Width: 160, Height: 120}, {Codec: av.CodecAAC, SampleRate: 44100, Channels: 1}}, Header{true, true}},
		{"av1.flv", []av.Track{{Codec: av.CodecAV1}}, Header{false, true}},
		{"mp3.flv", []av.Track{{Codec: av.CodecMP3, SampleRate: 44100, Channels: 1}}, Header{true, false}},
		{"pcma.flv", []av.Track{{Codec: av.CodecPCMA, SampleRate: 8000, Channels: 1}}, Header{true, false}},
	}
	for _, tc := range cases {
		t.Run(tc.file, func(t *testing.T) {
			b := readFixture(t, tc.file)
			whole, h := demuxAll(t, b, len(b))
			if h != tc.header {
				t.Fatalf("header %+v", h)
			}
			for _, chunk := range []int{1, 7, 100, 4096} {
				pieces, _ := demuxAll(t, b, chunk)
				if !reflect.DeepEqual(pieces, whole) {
					t.Fatalf("chunk %d demuxes differently", chunk)
				}
			}
			c := decodeTags(t, whole)
			if len(c.tracks) != 1 {
				t.Fatalf("%d OnTracks", len(c.tracks))
			}
			tracks := c.tracks[0]
			if len(tracks) != len(tc.tracks) {
				t.Fatalf("tracks %+v", tracks)
			}
			for i, want := range tc.tracks {
				got := *tracks[i]
				got.Config = nil
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("track %d: %+v want %+v", i, got, want)
				}
			}
			if len(c.packets) < 3 {
				t.Fatalf("%d packets", len(c.packets))
			}
			last := map[int]time.Duration{}
			reordered := false
			for _, p := range c.packets {
				if p.DTS < last[p.Track] {
					t.Fatalf("DTS goes back on track %d", p.Track)
				}
				last[p.Track] = p.DTS
				if p.PTS < p.DTS {
					t.Fatal("PTS before DTS")
				}
				if p.PTS != p.DTS {
					reordered = true
				}
				if len(p.Data) == 0 {
					t.Fatal("empty packet")
				}
			}
			if tc.file != "av1.flv" && tracks[0].Codec.IsVideo() && !reordered {
				t.Fatal("no B-frames found")
			}
			if tracks[0].Codec.IsVideo() && !c.packets[0].Keyframe {
				t.Fatal("first video packet is not a keyframe")
			}
			roundTrip(t, tracks, c.packets, EncoderConfig{})
			roundTrip(t, tracks, c.packets, EncoderConfig{LegacyHEVC: true})
		})
	}
}

// roundTrip muxes packets, demuxes and decodes the file, and checks the
// stream comes back the same.
func roundTrip(t *testing.T, tracks []*av.Track, packets []*av.Packet, config EncoderConfig) {
	t.Helper()
	m := NewMuxer(config)
	out, dropped := m.AppendHeader(nil, tracks)
	if len(dropped) != 0 {
		t.Fatalf("dropped %v", dropped)
	}
	for _, p := range packets {
		var err error
		if out, err = m.AppendPacket(out, p); err != nil {
			t.Fatal(err)
		}
	}
	tags, _ := demuxAll(t, out, 333)
	c := decodeTags(t, tags)
	if len(c.tracks) != 1 || len(c.tracks[0]) != len(tracks) {
		t.Fatalf("tracks %+v", c.tracks)
	}
	for i := range tracks {
		if !reflect.DeepEqual(c.tracks[0][i], tracks[i]) {
			t.Fatalf("track %d: %+v want %+v", i, c.tracks[0][i], tracks[i])
		}
	}
	if len(c.packets) != len(packets) {
		t.Fatalf("%d packets, want %d", len(c.packets), len(packets))
	}
	base := packets[0].DTS.Round(time.Millisecond)
	for i, p := range c.packets {
		w := packets[i]
		if p.Track != w.Track || p.Keyframe != w.Keyframe || !bytes.Equal(p.Data, w.Data) ||
			p.DTS != w.DTS.Round(time.Millisecond)-base || p.CTS() != w.CTS().Truncate(time.Millisecond) {
			t.Fatalf("packet %d: %+v want %+v", i, p, w)
		}
	}
}

func TestOpusRoundTrip(t *testing.T) {
	head, _ := (&opus.Head{Channels: 2, PreSkip: 312, InputSampleRate: 48000}).Append(nil)
	for _, config := range [][]byte{head, nil} {
		tracks := []*av.Track{{Codec: av.CodecOpus, Config: config, SampleRate: 48000, Channels: 2}}
		var packets []*av.Packet
		for i := range 10 {
			d := time.Duration(i) * 20 * time.Millisecond
			packets = append(packets, &av.Packet{DTS: d, PTS: d, Keyframe: true, Data: []byte{0xfc, byte(i)}})
		}
		roundTrip(t, tracks, packets, EncoderConfig{})
	}
}

func TestPCMURoundTrip(t *testing.T) {
	tracks := []*av.Track{{Codec: av.CodecPCMU, SampleRate: 8000, Channels: 2}}
	var packets []*av.Packet
	for i := range 3 {
		d := time.Duration(i) * 20 * time.Millisecond
		packets = append(packets, &av.Packet{DTS: d, PTS: d, Keyframe: true, Data: []byte{1, 2, byte(i)}})
	}
	roundTrip(t, tracks, packets, EncoderConfig{})
}

func TestEncoderSkipsTracks(t *testing.T) {
	e := NewEncoder(EncoderConfig{})
	dropped := e.SetTracks([]*av.Track{
		nil,
		{Codec: av.CodecH264}, // no config
		{Codec: av.CodecAAC},  // no config
		{Codec: av.CodecH264, Config: []byte{1}},
		{Codec: av.CodecH265, Config: []byte{1}},
		{Codec: av.CodecPCMA},
		{Codec: av.CodecOpus},
		{Codec: av.CodecUnknown},
	})
	if !reflect.DeepEqual(dropped, []int{0, 1, 2, 4, 6, 7}) {
		t.Fatal(dropped)
	}
	v, a := e.Tracks()
	if v.Codec != av.CodecH264 || a.Codec != av.CodecPCMA {
		t.Fatal(v, a)
	}
	b, typ, err := e.AppendPacket(nil, &av.Packet{Track: 4})
	if len(b) != 0 || typ != 0 || err != nil {
		t.Fatal("packet of a dropped track encoded")
	}
	e.SetTracks(nil)
	if b, typ, _ := e.AppendPacket(nil, &av.Packet{Track: -1}); len(b) != 0 || typ != 0 {
		t.Fatal("packet encoded without tracks")
	}
	var names []uint8
	e.SetTracks([]*av.Track{{Codec: av.CodecPCMU, SampleRate: 8000, Channels: 1}})
	e.Headers(func(typ uint8, body []byte) { names = append(names, typ) })
	if !reflect.DeepEqual(names, []uint8{TagScript}) {
		t.Fatal(names)
	}
	m := NewMuxer(EncoderConfig{})
	if m.Encoder() == nil {
		t.Fatal("no encoder")
	}
	if !Carries(av.CodecMP3) || Carries(av.CodecUnknown) {
		t.Fatal("Carries")
	}
}

func TestMetaData(t *testing.T) {
	e := NewEncoder(EncoderConfig{LegacyHEVC: true})
	e.SetTracks([]*av.Track{{Codec: av.CodecH265, Config: []byte{1}, Width: 640, Height: 360}, {Codec: av.CodecAAC, Config: []byte{0x12, 0x10}, SampleRate: 44100, Channels: 2}})
	name, args, err := ParseScript(e.AppendMetaData(nil))
	if err != nil || name != "onMetaData" {
		t.Fatal(name, err)
	}
	meta := MetaData(args)
	if w, _ := meta.Number("width"); w != 640 {
		t.Fatal(meta)
	}
	if id, _ := meta.Number("videocodecid"); id != VideoCodecHEVC {
		t.Fatal(id)
	}
	if s, _ := meta.Get("stereo"); s != true {
		t.Fatal(meta)
	}
	for _, c := range []av.CodecID{av.CodecH265, av.CodecAV1, av.CodecPCMA, av.CodecPCMU, av.CodecMP3, av.CodecOpus} {
		e := NewEncoder(EncoderConfig{})
		_ = e.codecID(&av.Track{Codec: c})
	}
	// @setDataFrame wraps the call an RTMP publisher makes.
	b, _ := amf.Append(nil, "@setDataFrame", "onMetaData", amf.Object{{Key: "width", Value: 1.0}})
	name, args, err = ParseScript(b)
	if err != nil || name != "onMetaData" || MetaData(args) == nil {
		t.Fatal(name, args, err)
	}
	if MetaData(nil) != nil || MetaData([]any{1.0}) != nil {
		t.Fatal("MetaData of nothing")
	}
	if _, _, err := ParseScript(nil); err == nil {
		t.Fatal("empty script parsed")
	}
	if _, _, err := ParseScript([]byte{0x20}); err == nil {
		t.Fatal("bad script parsed")
	}
}

func TestVideoTags(t *testing.T) {
	// Legacy AVC with a negative composition offset.
	b, err := AppendVideoTag(nil, &VideoTag{FrameType: FrameKey, Codec: av.CodecH264, PacketType: PacketCodedFrames, CTS: -40, Data: []byte{9}})
	if err != nil {
		t.Fatal(err)
	}
	v, err := ParseVideoTag(b)
	if err != nil || v.CTS != -40 || v.Codec != av.CodecH264 || v.Enhanced || !bytes.Equal(v.Data, []byte{9}) {
		t.Fatalf("%+v %v", v, err)
	}
	// Enhanced HEVC, with and without composition offsets, with a ModEx
	// nanosecond offset.
	for _, cts := range []int32{0, 33} {
		b, _ = AppendVideoTag(nil, &VideoTag{Enhanced: true, FrameType: FrameInter, Codec: av.CodecH265, PacketType: PacketCodedFrames, CTS: cts, Nanos: 500000, Data: []byte{1, 2}})
		v, err = ParseVideoTag(b)
		if err != nil || v.CTS != cts || v.Nanos != 500000 || v.PacketType != PacketCodedFrames || v.FourCC != FourCCHEVC {
			t.Fatalf("%+v %v", v, err)
		}
	}
	// AV1 and the enhanced AVC FourCC.
	b, _ = AppendVideoTag(nil, &VideoTag{Codec: av.CodecAV1, PacketType: PacketCodedFrames, CTS: 5, Data: []byte{1}})
	if v, _ = ParseVideoTag(b); v.Codec != av.CodecAV1 || v.CTS != 0 {
		t.Fatalf("%+v", v)
	}
	b = append([]byte{0x80 | FrameKey<<4 | PacketCodedFrames}, "avc1\x00\x00\x10\x07"...)
	if v, _ = ParseVideoTag(b); v.Codec != av.CodecH264 || v.CTS != 16 || len(v.Data) != 1 {
		t.Fatalf("%+v", v)
	}
	if _, err := AppendVideoTag(nil, &VideoTag{Codec: av.CodecAAC}); !errors.Is(err, ErrCodec) {
		t.Fatal(err)
	}
	// Legacy HEVC packet types past SequenceEnd are written as frames.
	b, _ = AppendVideoTag(nil, &VideoTag{Codec: av.CodecH265, PacketType: PacketCodedFramesX})
	if b[0]&0x0f != VideoCodecHEVC || b[1] != PacketCodedFrames {
		t.Fatalf("%x", b)
	}
	// Commands, legacy and enhanced.
	if v, _ = ParseVideoTag([]byte{FrameCommand<<4 | VideoCodecAVC, 1}); v.Command != 1 || v.PacketType != PacketMetadata {
		t.Fatalf("%+v", v)
	}
	if v, _ = ParseVideoTag([]byte{0x80 | FrameCommand<<4 | PacketCodedFrames, 1}); v.Command != 1 {
		t.Fatalf("%+v", v)
	}
	// A two-byte ModEx size.
	b = []byte{0x80 | FrameKey<<4 | PacketModEx, 0xff, 0x00, 0x02, 0, 0, 1, PacketCodedFramesX}
	b = append(b, "hvc1"...)
	if v, err = ParseVideoTag(b); err != nil || v.Nanos != 1 {
		t.Fatalf("%+v %v", v, err)
	}
	// Other legacy codecs pass through unknown.
	if v, _ = ParseVideoTag([]byte{FrameKey<<4 | 2, 5}); v.Codec != av.CodecUnknown || len(v.Data) != 1 {
		t.Fatalf("%+v", v)
	}
	if _, err := ParseVideoTag([]byte{0x80 | PacketMultitrack}); !errors.Is(err, ErrMultitrack) {
		t.Fatal(err)
	}
	bad := [][]byte{
		{},
		{FrameKey<<4 | VideoCodecAVC, 0},
		{FrameKey<<4 | VideoCodecAVC, 3, 0, 0, 0},
		{0x80 | FrameKey<<4 | PacketModEx},
		{0x80 | FrameKey<<4 | PacketModEx, 0xff, 0},
		{0x80 | FrameKey<<4 | PacketModEx, 2, 0, 0},
		{0x80 | FrameCommand<<4 | PacketCodedFrames},
		{0x80 | FrameKey<<4 | PacketCodedFrames, 'h', 'v'},
		{0x80 | FrameKey<<4 | PacketCodedFrames, 'h', 'v', 'c', '1', 0},
	}
	for _, b := range bad {
		if _, err := ParseVideoTag(b); err == nil {
			t.Errorf("%x parsed", b)
		}
	}
}

func TestAudioTags(t *testing.T) {
	for _, c := range []av.CodecID{av.CodecPCMA, av.CodecPCMU, av.CodecMP3} {
		b, err := AppendAudioTag(nil, &AudioTag{Codec: c, SoundRate: 3, SoundSize: 1, SoundType: 1, Data: []byte{7}})
		if err != nil {
			t.Fatal(err)
		}
		a, err := ParseAudioTag(b)
		if err != nil || a.Codec != c || a.SoundRate != 3 || a.SoundType != 1 || !bytes.Equal(a.Data, []byte{7}) {
			t.Fatalf("%+v %v", a, err)
		}
	}
	if _, err := AppendAudioTag(nil, &AudioTag{Codec: av.CodecH264}); !errors.Is(err, ErrCodec) {
		t.Fatal(err)
	}
	// Enhanced AAC (mp4a) and MP3 FourCCs, and a ModEx prefix.
	b := append([]byte{SoundExHeader<<4 | PacketSequenceStart}, "mp4a\x12\x10"...)
	if a, _ := ParseAudioTag(b); a.Codec != av.CodecAAC || a.PacketType != PacketSequenceStart {
		t.Fatalf("%+v", a)
	}
	b = append([]byte{SoundExHeader<<4 | PacketModEx, 2, 0, 0, 9, PacketCodedFrames}, ".mp3\x01"...)
	if a, err := ParseAudioTag(b); err != nil || a.Codec != av.CodecMP3 || a.Nanos != 9 {
		t.Fatalf("%+v %v", a, err)
	}
	if _, err := ParseAudioTag([]byte{SoundExHeader<<4 | audioPacketMultitrack}); !errors.Is(err, ErrMultitrack) {
		t.Fatal(err)
	}
	bad := [][]byte{
		{},
		{SoundAAC << 4},
		{SoundAAC << 4, 2},
		{SoundExHeader<<4 | PacketModEx},
		{SoundExHeader << 4, 'O'},
	}
	for _, b := range bad {
		if _, err := ParseAudioTag(b); err == nil {
			t.Errorf("%x parsed", b)
		}
	}
	if soundRate(5000) != 0 || soundRate(11025) != 1 || soundRate(22050) != 2 || soundRate(48000) != 3 {
		t.Fatal("soundRate")
	}
}

func TestDemuxerErrors(t *testing.T) {
	fn := func(Tag) error { return nil }
	if err := NewDemuxer(DemuxerConfig{}).Feed([]byte("FLX\x01\x05\x00\x00\x00\x09"), fn); !errors.Is(err, ErrHeader) {
		t.Fatal(err)
	}
	if err := NewDemuxer(DemuxerConfig{}).Feed([]byte("FLV\x01\x05\x00\x00\x00\x02"), fn); !errors.Is(err, ErrHeader) {
		t.Fatal(err)
	}
	// A header with extra bytes before the first tag.
	d := NewDemuxer(DemuxerConfig{})
	b := []byte("FLV\x01\x05\x00\x00\x00\x0bxx\x00\x00\x00\x00")
	b = AppendTag(b, TagAudio, 1, []byte{SoundPCMA << 4, 1})
	n := 0
	if err := d.Feed(b, func(tag Tag) error { n++; return nil }); err != nil || n != 1 {
		t.Fatal(err, n)
	}
	tag := AppendTag(nil, TagVideo, 0, make([]byte, 100))
	d = NewDemuxer(DemuxerConfig{NoHeader: true, MaxTagSize: 99})
	if err := d.Feed(tag, fn); !errors.Is(err, ErrTagTooLarge) {
		t.Fatal(err)
	}
	d = NewDemuxer(DemuxerConfig{NoHeader: true, MaxTagSize: 99})
	if err := d.Feed(tag[:5], fn); err != nil {
		t.Fatal(err)
	}
	if err := d.Feed(tag[5:], fn); !errors.Is(err, ErrTagTooLarge) {
		t.Fatal(err)
	}
	for _, typ := range []byte{1, TagVideo | 0x20} {
		bad := AppendTag(nil, typ, 0, nil)
		if err := NewDemuxer(DemuxerConfig{NoHeader: true}).Feed(bad, fn); !errors.Is(err, ErrTagType) {
			t.Fatal(err)
		}
	}
	stop := errors.New("stop")
	stopFn := func(Tag) error { return stop }
	if err := NewDemuxer(DemuxerConfig{NoHeader: true}).Feed(tag, stopFn); err != stop {
		t.Fatal(err)
	}
	d = NewDemuxer(DemuxerConfig{NoHeader: true})
	_ = d.Feed(tag[:20], stopFn)
	if err := d.Feed(tag[20:], stopFn); err != stop {
		t.Fatal(err)
	}
	if _, ok := NewDemuxer(DemuxerConfig{}).Header(); ok {
		t.Fatal("header before any input")
	}
}

func TestMillis(t *testing.T) {
	if Millis(1499*time.Microsecond) != 1 || Millis(1500*time.Microsecond) != 2 {
		t.Fatal("Millis rounds")
	}
	if Millis(time.Duration(1<<32+5)*time.Millisecond) != 5 {
		t.Fatal("Millis wraps")
	}
}

// avcConfig is a real AVCDecoderConfigurationRecord, 160x120.
func avcConfig(t testing.TB) []byte {
	tags, _ := demuxAll(t, readFixture(t, "h264_aac.flv"), 1<<20)
	for _, tag := range tags {
		if tag.Type == TagVideo {
			v, _ := ParseVideoTag(tag.Data)
			if v.PacketType == PacketSequenceStart {
				return v.Data
			}
		}
	}
	t.Fatal("no AVC config")
	return nil
}

func videoBody(t testing.TB, v VideoTag) []byte {
	b, err := AppendVideoTag(nil, &v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDecoderEvents(t *testing.T) {
	config := avcConfig(t)
	var tracks [][]*av.Track
	var packets []*av.Packet
	d := NewDecoder(DecoderConfig{
		OnTracks:   func(tr []*av.Track) { tracks = append(tracks, tr) },
		OnPacket:   func(p *av.Packet) { packets = append(packets, p) },
		MaxPending: 3,
	})
	meta, _ := amf.Append(nil, "onMetaData", amf.ECMAArray{{Key: "videocodecid", Value: 7.0}, {Key: "audiocodecid", Value: 10.0}})
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(d.WriteTag(TagScript, 0, meta))
	if d.MetaData() == nil {
		t.Fatal("metadata")
	}
	// A frame before the sequence header is dropped.
	frame := func(key bool, extra ...byte) []byte {
		ft := uint8(FrameInter)
		if key {
			ft = FrameKey
		}
		data := append([]byte{0, 0, 0, 2, 9, 0xf0, 0, 0, 0, 2, 0x65, 1}, extra...) // AUD, then IDR
		return videoBody(t, VideoTag{FrameType: ft, Codec: av.CodecH264, PacketType: PacketCodedFrames, Data: data})
	}
	must(d.WriteTag(TagVideo, 0, frame(true)))
	must(d.WriteTag(TagVideo, 0, videoBody(t, VideoTag{FrameType: FrameKey, Codec: av.CodecH264, PacketType: PacketSequenceStart, Data: config})))
	// The same sequence header again is no change.
	must(d.WriteTag(TagVideo, 0, videoBody(t, VideoTag{FrameType: FrameKey, Codec: av.CodecH264, PacketType: PacketSequenceStart, Data: config})))
	must(d.WriteTag(TagVideo, 0, frame(true)))
	must(d.WriteTag(TagVideo, 40, frame(false)))
	if len(tracks) != 0 {
		t.Fatal("tracks before audio")
	}
	// The third pending packet reaches MaxPending.
	must(d.WriteTag(TagVideo, 80, frame(false)))
	if len(tracks) != 1 || len(tracks[0]) != 1 || len(packets) != 3 {
		t.Fatalf("%d tracks %d packets", len(tracks), len(packets))
	}
	if !bytes.Equal(packets[0].Data, []byte{0, 0, 0, 2, 0x65, 1}) {
		t.Fatalf("AUD kept: %x", packets[0].Data)
	}
	// Audio arriving late is a new track.
	must(d.WriteTag(TagAudio, 90, []byte{SoundAAC<<4 | 0xf, 0, 0x12, 0x10}))
	if len(tracks) != 2 || len(tracks[1]) != 2 || tracks[1][1].Codec != av.CodecAAC {
		t.Fatalf("%+v", tracks)
	}
	must(d.WriteTag(TagAudio, 100, []byte{SoundAAC<<4 | 0xf, 1, 5}))
	if p := packets[len(packets)-1]; p.Track != 1 || p.DTS != 100*time.Millisecond {
		t.Fatalf("%+v", p)
	}
	// A different config replaces the track in place.
	must(d.WriteTag(TagAudio, 100, []byte{SoundAAC<<4 | 0xf, 0, 0x11, 0x90}))
	if len(tracks) != 3 || tracks[2][1].SampleRate != 48000 || tracks[2][0] != tracks[1][0] {
		t.Fatalf("%+v", tracks[2][1])
	}
	if len(d.Tracks()) != 2 {
		t.Fatal("Tracks")
	}
	// Timestamps unwrap past 32 bits.
	must(d.WriteTag(TagVideo, 0xffffff00, frame(true)))
	must(d.WriteTag(TagVideo, 0x00000010, frame(false)))
	n := len(packets)
	if packets[n-1].DTS-packets[n-2].DTS != 0x110*time.Millisecond {
		t.Fatalf("wrap: %v %v", packets[n-2].DTS, packets[n-1].DTS)
	}
	// What is dropped quietly.
	for _, body := range [][]byte{
		{FrameKey<<4 | 2, 0},                      // a legacy codec av lacks
		{FrameCommand<<4 | VideoCodecAVC, 0},      // a command
		{0x80 | PacketMultitrack},                 // multitrack
		{FrameKey<<4 | VideoCodecAVC, 2, 0, 0, 0}, // sequence end
		videoBody(t, VideoTag{FrameType: FrameKey, Codec: av.CodecH265, PacketType: PacketCodedFrames, Data: []byte{0, 0, 0, 1, 2}}),
		videoBody(t, VideoTag{FrameType: FrameKey, Codec: av.CodecH264, PacketType: PacketCodedFrames, Data: []byte{0, 0, 0, 9, 1}}),       // bad AVCC
		videoBody(t, VideoTag{FrameType: FrameKey, Codec: av.CodecH264, PacketType: PacketCodedFrames, Data: []byte{0, 0, 0, 2, 9, 0xf0}}), // only an AUD
	} {
		before := len(packets)
		must(d.WriteTag(TagVideo, 200, body))
		if len(packets) != before {
			t.Fatalf("%x made a packet", body)
		}
	}
	for _, body := range [][]byte{
		{0xb0, 0}, // Speex
		{SoundExHeader<<4 | audioPacketMultitrack}, // multitrack
		{SoundExHeader<<4 | PacketSequenceEnd, 'O', 'p', 'u', 's'},
		{SoundPCMA << 4},       // a new codec, but empty
		{SoundAAC<<4 | 0xf, 1}, // empty AAC frame
		append([]byte{SoundExHeader<<4 | PacketSequenceStart}, ".mp3"...), // no sequence start for MP3
	} {
		before := len(packets)
		must(d.WriteTag(TagAudio, 200, body))
		if len(packets) != before {
			t.Fatalf("%x made a packet", body)
		}
	}
	errs := []struct {
		typ  uint8
		body []byte
	}{
		{TagVideo, []byte{}},
		{TagAudio, []byte{}},
		{TagScript, []byte{}},
		{3, nil},
		{TagVideo, videoBody(t, VideoTag{Codec: av.CodecH264, PacketType: PacketSequenceStart, Data: []byte{1}})},
		{TagVideo, videoBody(t, VideoTag{Codec: av.CodecH265, PacketType: PacketSequenceStart, Data: []byte{1}})},
		{TagAudio, []byte{SoundAAC << 4, 0, 0}},
		{TagAudio, append([]byte{SoundExHeader<<4 | PacketSequenceStart}, "Opus\x01"...)},
	}
	for _, e := range errs {
		if err := d.WriteTag(e.typ, 0, e.body); err == nil {
			t.Errorf("%d %x decoded", e.typ, e.body)
		}
	}
}

func TestDecoderHold(t *testing.T) {
	// Without metadata, an audio-only stream starts once MaxHold has
	// passed.
	var tracks [][]*av.Track
	var packets int
	d := NewDecoder(DecoderConfig{
		OnTracks: func(tr []*av.Track) { tracks = append(tracks, tr) },
		OnPacket: func(*av.Packet) { packets++ },
		MaxHold:  100 * time.Millisecond,
	})
	for i := range 6 {
		_ = d.WriteTag(TagAudio, uint32(i*20), []byte{SoundPCMU << 4, 1})
	}
	if len(tracks) != 1 || packets != 6 {
		t.Fatalf("%d %d", len(tracks), packets)
	}
	d.Flush() // nothing left
	// Metadata announcing audio and video, video never coming: Flush
	// starts with what came.
	d = NewDecoder(DecoderConfig{OnTracks: func(tr []*av.Track) { tracks = append(tracks, tr) }})
	meta, _ := amf.Append(nil, "onMetaData", amf.ECMAArray{{Key: "videocodecid", Value: float64(FourCCHEVC)}, {Key: "audiocodecid", Value: float64(FourCCOpus)}})
	_ = d.WriteTag(TagScript, 0, meta)
	_ = d.WriteTag(TagScript, 0, meta) // a second one changes nothing
	_ = d.WriteTag(TagAudio, 0, append([]byte{SoundExHeader<<4 | PacketCodedFrames}, "Opus\xfc"...))
	if len(tracks) != 1 {
		t.Fatal("started without video")
	}
	d.Flush()
	if len(tracks) != 2 || tracks[1][0].Codec != av.CodecOpus {
		t.Fatal("Flush")
	}
	// The hold never keeps more than MaxPending packets of no track.
	d = NewDecoder(DecoderConfig{MaxPending: 1})
	d.pending = append(d.pending, &av.Packet{})
	d.checkReady(false)
	if len(d.pending) != 0 {
		t.Fatal("pending kept")
	}
	// A packet of a kind whose track went away before the start is
	// dropped.
	d = NewDecoder(DecoderConfig{})
	d.audio = &av.Track{Codec: av.CodecPCMA}
	d.pending = []*av.Packet{{Track: 0}, {Track: 1}}
	d.Flush()
}

func TestDecoderLengthSize(t *testing.T) {
	// A config with 2-byte NAL unit lengths.
	config := avcConfig(t)
	c := append([]byte(nil), config...)
	c[4] = c[4]&^3 | 1
	var got *av.Packet
	d := NewDecoder(DecoderConfig{OnPacket: func(p *av.Packet) { got = p }})
	_ = d.WriteTag(TagVideo, 0, videoBody(t, VideoTag{FrameType: FrameKey, Codec: av.CodecH264, PacketType: PacketSequenceStart, Data: c}))
	_ = d.WriteTag(TagVideo, 0, videoBody(t, VideoTag{FrameType: FrameKey, Codec: av.CodecH264, PacketType: PacketCodedFrames, Data: []byte{0, 2, 0x65, 1}}))
	d.Flush()
	if got == nil || !bytes.Equal(got.Data, []byte{0, 0, 0, 2, 0x65, 1}) {
		t.Fatalf("%+v", got)
	}
	if audLen(av.CodecH265, []byte{0, 0, 0, 2, 35 << 1, 1}) != 6 || audLen(av.CodecH264, nil) != 0 {
		t.Fatal("audLen")
	}
	if tr, err := audioTrack(&AudioTag{Codec: av.CodecMP3, SoundFormat: SoundExHeader}, nil); err != nil || tr.SampleRate != 0 {
		t.Fatal(tr, err)
	}
}

func TestAppendTag(t *testing.T) {
	b := AppendTag(nil, TagVideo, 0x01020304, []byte{1, 2, 3})
	if len(b) != TagHeaderSize+3+4 || binary.BigEndian.Uint32(b[len(b)-4:]) != TagHeaderSize+3 {
		t.Fatalf("%x", b)
	}
	tag := parseTag(b)
	if tag.Timestamp != 0x01020304 || tag.Type != TagVideo {
		t.Fatalf("%+v", tag)
	}
	if h := AppendHeader(nil, Header{}); h[4] != 0 || len(h) != HeaderSize+4 {
		t.Fatalf("%x", h)
	}
}

func TestMuxerTrackChange(t *testing.T) {
	m := NewMuxer(EncoderConfig{})
	tracks := []*av.Track{{Codec: av.CodecPCMA, SampleRate: 8000, Channels: 1}}
	b, _ := m.AppendHeader(nil, tracks)
	b, _ = m.AppendPacket(b, &av.Packet{DTS: time.Second, Data: []byte{1}})
	// An earlier packet is clamped to the start.
	b, _ = m.AppendPacket(b, &av.Packet{DTS: 0, Data: []byte{1}})
	b, _ = m.AppendPacket(b, &av.Packet{DTS: 1100 * time.Millisecond, Data: []byte{1}})
	b, _ = m.AppendTracks(b, []*av.Track{{Codec: av.CodecPCMU, SampleRate: 8000, Channels: 1}})
	b, _ = m.AppendPacket(b, &av.Packet{Track: 1, Data: []byte{1}}) // no such track
	tags, _ := demuxAll(t, b, len(b))
	var ts []uint32
	for _, tag := range tags {
		ts = append(ts, tag.Timestamp)
	}
	if !reflect.DeepEqual(ts, []uint32{0, 0, 0, 100, 100}) {
		t.Fatal(ts)
	}
	m2 := NewMuxer(EncoderConfig{})
	b, _ = m2.AppendTracks(nil, tracks)
	if len(b) == 0 {
		t.Fatal("AppendTracks before start")
	}
	// An encoding failure leaves the buffer as it was.
	m3 := NewMuxer(EncoderConfig{})
	m3.AppendHeader(nil, []*av.Track{{Codec: av.CodecH264, Config: []byte{1}}})
	m3.enc.tracks[0] = &av.Track{Codec: av.CodecAAC}
	m3.enc.audio, m3.enc.video = 0, -1
	m3.enc.tracks[0] = &av.Track{Codec: av.CodecUnknown}
	if out, err := m3.AppendPacket([]byte{1}, &av.Packet{}); err == nil || len(out) != 1 {
		t.Fatal(out, err)
	}
}

package rtpcodec

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/aac"
	"github.com/lesismal/fib/media/codec/h264"
	"github.com/lesismal/fib/media/codec/h265"
	"github.com/lesismal/fib/webrtc/rtp"
	"github.com/lesismal/fib/webrtc/sdp"
)

// The fixtures in testdata are what ffmpeg 5.1 sends with "-f rtp" (each
// packet as a 2-byte length and the packet) and the SDP it writes with
// -sdp_file: 2.2 s of 320x240 testsrc at 25 fps in H.264 (x264, B-frames,
// parameter sets in band only) and H.265 (x265, B-frames), 2 s of a sine
// in AAC (mpeg4-generic and MP4A-LATM), Opus and PCMA.

func readPackets(t testing.TB, name string) [][]byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name+".rtp"))
	if err != nil {
		t.Fatal(err)
	}
	var pkts [][]byte
	for len(b) >= 2 {
		n := int(binary.BigEndian.Uint16(b))
		pkts = append(pkts, b[2:2+n])
		b = b[2+n:]
	}
	return pkts
}

func readFormat(t testing.TB, name string) *Format {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name+".sdp"))
	if err != nil {
		t.Fatal(err)
	}
	sd, err := sdp.Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	fs := ParseSession(sd)
	if len(fs) != 1 {
		t.Fatalf("%s: %d formats", name, len(fs))
	}
	return fs[0]
}

// depacketizeAll runs packets through a new Depacketizer of f and returns
// what it emits.
func depacketizeAll(t testing.TB, f *Format, pkts [][]byte) ([]*av.Packet, *Depacketizer) {
	t.Helper()
	d, err := NewDepacketizer(f, DepacketizerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	var out []*av.Packet
	var p rtp.Packet
	for _, b := range pkts {
		if err := p.Unmarshal(b); err != nil {
			t.Fatal(err)
		}
		if err := d.Depacketize(&p, func(pkt *av.Packet) { out = append(out, pkt) }); err != nil {
			t.Fatalf("depacketize: %v", err)
		}
	}
	d.Flush(func(pkt *av.Packet) { out = append(out, pkt) })
	return out, d
}

// checkTimes checks that decode times rise, presentation times are never
// before them, and that the span is about want.
func checkTimes(t *testing.T, name string, pkts []*av.Packet, span time.Duration) {
	t.Helper()
	var last time.Duration
	lo, hi := pkts[0].PTS, pkts[0].PTS
	for i, p := range pkts {
		if p.DTS < last || p.PTS < p.DTS {
			t.Fatalf("%s: packet %d dts %v pts %v after dts %v", name, i, p.DTS, p.PTS, last)
		}
		last = p.DTS
		lo, hi = min(lo, p.PTS), max(hi, p.PTS)
	}
	if got := hi - lo; got < span-span/5 || got > span+span/5 {
		t.Errorf("%s: span %v, want about %v", name, got, span)
	}
}

func TestFixtureH264(t *testing.T) {
	f := readFormat(t, "h264")
	if f.Track.Codec != av.CodecH264 || f.Track.Config != nil || f.PacketizationMode != 1 || f.ClockRate != 90000 {
		t.Fatalf("format %+v", f)
	}
	pkts := readPackets(t, "h264")
	out, d := depacketizeAll(t, f, pkts)
	track := d.Track()
	if track == f.Track || track.Width != 320 || track.Height != 240 || track.Config == nil {
		t.Fatalf("track %+v", track)
	}
	var cfg h264.AVCConfig
	if err := cfg.Unmarshal(track.Config); err != nil || len(cfg.SPS) != 1 || len(cfg.PPS) != 1 {
		t.Fatalf("config %v %+v", err, cfg)
	}
	if len(out) < 50 || !out[0].Keyframe {
		t.Fatalf("%d access units, first keyframe %v", len(out), out[0].Keyframe)
	}
	keys := 0
	reordered := false
	for _, p := range out {
		if p.Keyframe {
			keys++
		}
		if p.PTS != p.DTS {
			reordered = true
		}
		for nal := range h264.SplitAVCC(p.Data, 4) {
			if typ := h264.Type(nal); typ == h264.NALSPS || typ == h264.NALPPS || typ == h264.NALAUD {
				t.Fatalf("%v left in the access unit", typ)
			}
		}
	}
	if keys < 2 || !reordered {
		t.Errorf("%d keyframes, reordered %v", keys, reordered)
	}
	checkTimes(t, "h264", out, 2*time.Second)
	roundTrip(t, NewFormatOf(t, track, 96), out, 1400)
	roundTrip(t, NewFormatOf(t, track, 96), out, 200)
}

func TestFixtureH265(t *testing.T) {
	f := readFormat(t, "h265")
	pkts := readPackets(t, "h265")
	out, d := depacketizeAll(t, f, pkts)
	track := d.Track()
	if track.Width != 320 || track.Height != 240 {
		t.Fatalf("track %+v", track)
	}
	var cfg h265.HEVCConfig
	if err := cfg.Unmarshal(track.Config); err != nil || len(cfg.NALUs(h265.NALVPS)) != 1 {
		t.Fatalf("config %v", err)
	}
	if len(out) < 50 || !out[0].Keyframe {
		t.Fatalf("%d access units", len(out))
	}
	checkTimes(t, "h265", out, 2*time.Second)
	roundTrip(t, NewFormatOf(t, track, 96), out, 1400)
	roundTrip(t, NewFormatOf(t, track, 96), out, 100)
}

func TestFixtureAAC(t *testing.T) {
	f := readFormat(t, "aac")
	if f.Track.SampleRate != 48000 || f.Track.Channels != 2 || f.SizeLength != 13 {
		t.Fatalf("format %+v %+v", f, f.Track)
	}
	out, _ := depacketizeAll(t, f, readPackets(t, "aac"))
	if n := len(out); n < 90 || n > 96 {
		t.Fatalf("%d access units", n)
	}
	for i := 1; i < len(out); i++ {
		if d := out[i].PTS - out[i-1].PTS; d < 21333330 || d > 21333340 {
			t.Fatalf("frame %d after %v", i, d)
		}
	}
	nf, err := NewFormat(f.Track, 97)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, nf, out, 1400)
	roundTrip(t, nf, out, 100)
	lf, err := NewLATMFormat(f.Track, 98)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip(t, lf, out, 1400)
	roundTrip(t, lf, out, 100)
}

func TestFixtureLATM(t *testing.T) {
	f := readFormat(t, "latm")
	if !f.LATM || f.CPresent || f.Track.SampleRate != 44100 || f.Track.Channels != 1 {
		t.Fatalf("format %+v %+v", f, f.Track)
	}
	var cfg aac.Config
	if err := cfg.Unmarshal(f.Track.Config); err != nil || cfg.ObjectType != aac.ObjectTypeLC {
		t.Fatalf("config %v %+v", err, cfg)
	}
	out, _ := depacketizeAll(t, f, readPackets(t, "latm"))
	if n := len(out); n < 80 || n > 90 {
		t.Fatalf("%d access units", n)
	}
	checkTimes(t, "latm", out, 2*time.Second)
}

func TestFixtureOpus(t *testing.T) {
	f := readFormat(t, "opus")
	if f.Track.Codec != av.CodecOpus || f.Track.Channels != 2 || f.PayloadType != 111 {
		t.Fatalf("format %+v", f.Track)
	}
	out, _ := depacketizeAll(t, f, readPackets(t, "opus"))
	if n := len(out); n < 95 || n > 105 {
		t.Fatalf("%d frames", n)
	}
	checkTimes(t, "opus", out, 2*time.Second)
	roundTrip(t, NewFormatOf(t, f.Track, 111), out, 1400)
}

func TestFixturePCMA(t *testing.T) {
	f := readFormat(t, "pcma")
	if f.Track.Codec != av.CodecPCMA || f.ClockRate != 8000 || f.PayloadType != PayloadTypePCMA {
		t.Fatalf("format %+v", f.Track)
	}
	out, _ := depacketizeAll(t, f, readPackets(t, "pcma"))
	total := 0
	for _, p := range out {
		total += len(p.Data)
	}
	if total < 15000 || total > 17000 {
		t.Fatalf("%d samples", total)
	}
	checkTimes(t, "pcma", out, 2*time.Second)
	roundTrip(t, NewFormatOf(t, f.Track, 0), out, 1400)
}

func NewFormatOf(t *testing.T, track *av.Track, pt uint8) *Format {
	t.Helper()
	f, err := NewFormat(track, pt)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// roundTrip packetizes access units with a small MTU and depacketizes
// them again: the data and times must come back as they were.
func roundTrip(t *testing.T, f *Format, in []*av.Packet, mtu int) {
	t.Helper()
	p, err := NewPacketizer(f, PacketizerConfig{MTU: mtu})
	if err != nil {
		t.Fatal(err)
	}
	// Through SDP and back, as a receiver would get the format.
	md, err := f.Media()
	if err != nil {
		t.Fatal(err)
	}
	rf, err := ParseMedia(md)
	if err != nil {
		t.Fatalf("%v: %s", err, md.Lines)
	}
	d, err := NewDepacketizer(rf, DepacketizerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	var out []*av.Packet
	var q rtp.Packet
	for _, pkt := range in {
		err := p.Packetize(pkt, func(rp *rtp.Packet) error {
			if n := rp.MarshalSize(); n > mtu && f.Track.Codec != av.CodecOpus {
				t.Fatalf("packet of %d bytes, MTU %d", n, mtu)
			}
			b, err := rp.Append(nil)
			if err != nil {
				return err
			}
			if err := q.Unmarshal(b); err != nil {
				return err
			}
			return d.Depacketize(&q, func(pkt *av.Packet) { out = append(out, pkt) })
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	d.Flush(func(pkt *av.Packet) { out = append(out, pkt) })
	if f.Track.Codec == av.CodecPCMA || f.Track.Codec == av.CodecPCMU {
		var a, b []byte
		for _, p := range in {
			a = append(a, p.Data...)
		}
		for _, p := range out {
			b = append(b, p.Data...)
		}
		if !bytes.Equal(a, b) {
			t.Fatalf("samples differ: %d vs %d", len(a), len(b))
		}
		return
	}
	if len(out) != len(in) {
		t.Fatalf("%v mtu %d: %d packets in, %d out", f.Track.Codec, mtu, len(in), len(out))
	}
	origin := in[0].PTS
	for i := range in {
		if !bytes.Equal(in[i].Data, out[i].Data) || in[i].Keyframe != out[i].Keyframe {
			t.Fatalf("%v mtu %d: packet %d differs", f.Track.Codec, mtu, i)
		}
		// One tick of rounding either way.
		if dt := (out[i].PTS - out[0].PTS) - (in[i].PTS - origin); dt < -time.Millisecond || dt > time.Millisecond {
			t.Fatalf("%v: packet %d pts %v, want %v", f.Track.Codec, i, out[i].PTS-out[0].PTS, in[i].PTS-origin)
		}
	}
	if f.LATM {
		// A StreamMuxConfig of version 0 carries the AudioSpecificConfig
		// without its length, so what follows the GASpecificConfig, the
		// backward-compatible SBR signalling ffmpeg writes, is left out.
		var a, b aac.Config
		if a.Unmarshal(d.Track().Config) != nil || b.Unmarshal(f.Track.Config) != nil ||
			a.ObjectType != b.ObjectType || a.SampleRate != b.SampleRate || a.ChannelConfig != b.ChannelConfig {
			t.Fatalf("LATM config %x, want %x", d.Track().Config, f.Track.Config)
		}
	} else if !bytes.Equal(d.Track().Config, f.Track.Config) {
		t.Fatalf("%v: config %x, want %x", f.Track.Codec, d.Track().Config, f.Track.Config)
	}
}

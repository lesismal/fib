package rtpcodec

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/av1"
	"github.com/lesismal/fib/webrtc/rtp"
	"github.com/lesismal/fib/webrtc/sdp"
)

// av1Fixture returns the track and temporal units of testdata/av1.obu,
// what ffmpeg 7.1 wrote with "-c:v libaom-av1 -cpu-used 8 -g 10 -f obu":
// 1.2 s of 160x120 testsrc at 25 fps, a keyframe every 10 frames, with
// hidden frames and frame headers that show them.
func av1Fixture(t testing.TB) (*av.Track, []*av.Packet) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "av1.obu"))
	if err != nil {
		t.Fatal(err)
	}
	var tus [][]byte
	for o := range av1.SplitOBUs(b) {
		if o.Type() == av1.OBUTemporalDelimiter {
			tus = append(tus, nil)
			continue
		}
		tus[len(tus)-1] = av1.AppendOBU(tus[len(tus)-1], o, true)
	}
	cfg, err := av1.NewConfig(tus[0])
	if err != nil {
		t.Fatal(err)
	}
	track := &av.Track{Codec: av.CodecAV1, Config: cfg.Append(nil), Width: 160, Height: 120}
	var pkts []*av.Packet
	for i, tu := range tus {
		ts := time.Duration(i) * 40 * time.Millisecond
		pkts = append(pkts, &av.Packet{DTS: ts, PTS: ts, Keyframe: av1.IsKeyframe(tu), Data: tu})
	}
	return track, pkts
}

// av1Packets packetizes temporal units and returns the RTP packets.
func av1Packets(t testing.TB, f *Format, mtu int, pkts ...*av.Packet) []*rtp.Packet {
	t.Helper()
	p, err := NewPacketizer(f, PacketizerConfig{MTU: mtu, Sequence: 1, Timestamp: 1})
	if err != nil {
		t.Fatal(err)
	}
	var out []*rtp.Packet
	for _, pkt := range pkts {
		err := p.Packetize(pkt, func(rp *rtp.Packet) error {
			if rp.MarshalSize() > mtu {
				t.Fatalf("packet of %d bytes, MTU %d", rp.MarshalSize(), mtu)
			}
			q := *rp
			q.Payload = bytes.Clone(rp.Payload)
			out = append(out, &q)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func TestAV1RoundTrip(t *testing.T) {
	track, pkts := av1Fixture(t)
	if keys := pkts[0].Keyframe && pkts[10].Keyframe && pkts[20].Keyframe && !pkts[1].Keyframe; !keys {
		t.Fatal("fixture keyframes")
	}
	f := NewFormatOf(t, track, 45)
	for _, mtu := range []int{DefaultMTU, 300, 100, minMTU} {
		roundTrip(t, f, pkts, mtu)
	}
	md, err := f.Media()
	if err != nil {
		t.Fatal(err)
	}
	if s := mediaText(md); !strings.Contains(s, "a=rtpmap:45 AV1/90000") ||
		!strings.Contains(s, "a=fmtp:45 level-idx=0;profile=0;tier=0") {
		t.Fatal(s)
	}
	// What the receiver learns of the track: its config and size.
	rf, _ := ParseMedia(md)
	d := mustDepacketizer(t, rf)
	out, errs := feed(d, av1Packets(t, f, DefaultMTU, pkts...)...)
	if len(out) != len(pkts) || len(errs) != 0 {
		t.Fatal(len(out), errs)
	}
	if tr := d.Track(); !bytes.Equal(tr.Config, track.Config) || tr.Width != 160 || tr.Height != 120 {
		t.Fatalf("%+v", tr)
	}
	for i, p := range out {
		if p.DTS != p.PTS || p.PTS != out[0].PTS+time.Duration(i)*40*time.Millisecond {
			t.Fatalf("%d: %v %v", i, p.DTS, p.PTS)
		}
	}
	// A format with no config writes no fmtp.
	bare, _ := (&Format{PayloadType: 45, ClockRate: 90000, Track: &av.Track{Codec: av.CodecAV1}}).Media()
	if s := mediaText(bare); strings.Contains(s, "fmtp") {
		t.Fatal(s)
	}
}

// av1Header reads the aggregation header of a payload.
func av1Header(b []byte) (z, y bool, w int, n bool) {
	return b[0]&av1Z != 0, b[0]&av1Y != 0, int(b[0] >> 4 & 3), b[0]&av1N != 0
}

func TestAV1Packetize(t *testing.T) {
	track, pkts := av1Fixture(t)
	f := NewFormatOf(t, track, 45)
	// The first temporal unit is a keyframe of a sequence header and a
	// frame over 1200 bytes: N on the first packet, the sequence header
	// aggregated with the start of the frame, then the rest of it.
	rps := av1Packets(t, f, 1000, pkts[0])
	if len(rps) != 2 {
		t.Fatal(len(rps))
	}
	z, y, w, n := av1Header(rps[0].Payload)
	if z || !y || w != 2 || !n || rps[0].Marker {
		t.Fatal("first packet", z, y, w, n)
	}
	if size, k := av1.ReadLEB128(rps[0].Payload[1:]); size != 11 || k != 1 || av1.Type(rps[0].Payload[2:]) != av1.OBUSequenceHeader ||
		rps[0].Payload[2]&2 != 0 {
		t.Fatal("sequence header element without its size field")
	}
	z, y, w, n = av1Header(rps[1].Payload)
	if !z || y || w != 1 || n || !rps[1].Marker {
		t.Fatal("second packet", z, y, w, n)
	}
	// Four frames aggregate into one packet whose elements all have
	// lengths.
	rps = av1Packets(t, f, DefaultMTU, pkts[1])
	if _, _, w, _ := av1Header(rps[0].Payload); len(rps) != 1 || w != 0 {
		t.Fatal(len(rps), w)
	}
	// A frame that does not fit beside others goes in a packet of its own
	// when too little room is left to fragment it.
	big := &av.Packet{Data: av1.AppendOBU(nil, av1.OBU{Header: []byte{0x30}, Payload: make([]byte, 30)}, true)}
	big.Data = append(big.Data, av1.AppendOBU(nil, av1.OBU{Header: []byte{0x30}, Payload: make([]byte, 40)}, true)...)
	rps = av1Packets(t, f, rtp.HeaderSize+40, big)
	if len(rps) != 3 {
		t.Fatal(len(rps))
	}
	if z, y, w, _ := av1Header(rps[1].Payload); z || !y || w != 1 {
		t.Fatal(z, y, w)
	}

	// A keyframe without a sequence header gets the track's, and the
	// temporal delimiters, tile lists and padding are left out.
	var noSeq []byte
	for o := range av1.SplitOBUs(pkts[10].Data) {
		if o.Type() != av1.OBUSequenceHeader {
			noSeq = av1.AppendOBU(noSeq, o, true)
		}
	}
	extra := []byte{0x12, 0x00, 0x42, 0x01, 0x00, 0x7a, 0x01, 0xff}
	d := mustDepacketizer(t, f)
	out, _ := feed(d, av1Packets(t, f, DefaultMTU, &av.Packet{Keyframe: true, Data: append(bytes.Clone(extra), noSeq...)})...)
	if len(out) != 1 || !bytes.Equal(out[0].Data, pkts[10].Data) || !out[0].Keyframe {
		t.Fatal("sequence header not inserted")
	}
	// Nothing to send, and data that is not OBUs.
	if rps := av1Packets(t, f, DefaultMTU, &av.Packet{Data: extra}); len(rps) != 0 {
		t.Fatal("empty temporal unit sent")
	}
	p, _ := NewPacketizer(f, PacketizerConfig{})
	if err := p.Packetize(&av.Packet{Data: []byte{0x32, 9}}, func(*rtp.Packet) error { return nil }); err != ErrPacket {
		t.Fatal(err)
	}
	stop := errors.New("stop")
	for _, mtu := range []int{100, DefaultMTU} {
		p, _ := NewPacketizer(f, PacketizerConfig{MTU: mtu})
		if err := p.Packetize(pkts[0], func(*rtp.Packet) error { return stop }); err != stop {
			t.Fatal(err)
		}
		p, _ = NewPacketizer(f, PacketizerConfig{MTU: rtp.HeaderSize + 40})
		if err := p.Packetize(big, func(*rtp.Packet) error { return stop }); err != stop {
			t.Fatal(err)
		}
	}
	// A track without a usable config sends no sequence header.
	p, _ = NewPacketizer(&Format{PayloadType: 45, ClockRate: 90000, Track: &av.Track{Codec: av.CodecAV1}}, PacketizerConfig{})
	var n1 int
	_ = p.Packetize(&av.Packet{Keyframe: true, Data: noSeq}, func(rp *rtp.Packet) error { n1 += len(rp.Payload); return nil })
	if n1 >= len(noSeq) {
		t.Fatal(n1)
	}
}

func TestAV1Loss(t *testing.T) {
	track, pkts := av1Fixture(t)
	f := NewFormatOf(t, track, 45)
	rps := av1Packets(t, f, 100, pkts...)
	// Lose the third packet of the second temporal unit: it and the ones
	// after it wait for the keyframe of the eleventh.
	var lost int
	for i, rp := range rps {
		if rp.Timestamp == rps[0].Timestamp+3600 {
			lost = i + 2
			break
		}
	}
	d := mustDepacketizer(t, &Format{PayloadType: 45, ClockRate: 90000, Track: &av.Track{Codec: av.CodecAV1}})
	out, errs := feed(d, append(rps[:lost:lost], rps[lost+1:]...)...)
	if len(errs) != 0 || len(out) != 30-9 || d.Lost() != 1 {
		t.Fatal(len(out), errs, d.Lost())
	}
	if !out[1].Keyframe || out[1].PTS != out[0].PTS+400*time.Millisecond {
		t.Fatal("did not resume at the keyframe")
	}

	// Before the first sequence header nothing is emitted, the start of a
	// stream joined in the middle of a fragment included.
	d = mustDepacketizer(t, &Format{PayloadType: 45, ClockRate: 90000, Track: &av.Track{Codec: av.CodecAV1}})
	out, _ = feed(d, rps[1:]...)
	if len(out) != 20 || out[0].PTS != 400*time.Millisecond {
		t.Fatal(len(out))
	}

	// A fragment whose end never comes though no packet is lost, and a
	// lost marker: the timestamp change ends the temporal unit.
	frame := av1.AppendOBU(nil, av1.OBU{Header: []byte{0x30}, Payload: []byte{0x10, 1, 2}}, false)
	key := append(av1.AppendOBU(nil, seqOBU(t, track), false), frame...)
	d = mustDepacketizer(t, f)
	out, _ = feed(d,
		rtpPacket(45, 1, 0, false, append([]byte{0x40 | 0x10}, frame[:2]...)),
		rtpPacket(45, 2, 0, true, append([]byte{0x10}, frame...)),
		rtpPacket(45, 3, 3000, false, append([]byte{0x20, byte(len(key) - len(frame))}, key...)),
		rtpPacket(45, 4, 6000, false, []byte{0x10, 0x30, 0x30, 1, 2}),
	)
	if len(out) != 1 || out[0].PTS != av.FromTicks(3000, 90000) {
		t.Fatal(len(out))
	}
	d.Flush(func(p *av.Packet) { out = append(out, p) })
	if len(out) != 2 || !out[0].Keyframe || out[1].Keyframe {
		t.Fatal("flush", len(out))
	}
	d.Flush(func(*av.Packet) { t.Fatal("flushed twice") })
}

// seqOBU returns the sequence header of a track's config as an OBU.
func seqOBU(t *testing.T, track *av.Track) av1.OBU {
	t.Helper()
	o, _, err := av1.NextOBU(mustOBU(t, track))
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func mediaText(md *sdp.MediaDescription) string {
	var lines []string
	for _, l := range md.Lines {
		lines = append(lines, l.String())
	}
	return strings.Join(lines, "\n")
}

// mustOBU returns the sequence header of a track's config.
func mustOBU(t *testing.T, track *av.Track) []byte {
	t.Helper()
	var c av1.Config
	if err := c.Unmarshal(track.Config); err != nil {
		t.Fatal(err)
	}
	return c.SequenceHeader()
}

func TestAV1Depacketize(t *testing.T) {
	track, _ := av1Fixture(t)
	seq := mustOBU(t, track)
	frame := func(b byte) []byte {
		return av1.AppendOBU(nil, av1.OBU{Header: []byte{0x30}, Payload: []byte{b, 1, 2}}, false)
	}
	bare := &Format{PayloadType: 45, ClockRate: 90000, Track: &av.Track{Codec: av.CodecAV1}}
	// OBU elements with their size fields, several OBUs in one element, a
	// temporal delimiter and padding: the same temporal unit comes out.
	sized := append(bytes.Clone(seq), av1.AppendOBU(nil, av1.OBU{Header: []byte{0x30}, Payload: []byte{0x10, 1, 2}}, true)...)
	pad := []byte{0x7a, 0x01, 0xff}
	d := mustDepacketizer(t, bare)
	out, errs := feed(d,
		rtpPacket(45, 1, 0, true, append(append([]byte{0x30, 0x02, 0x12, 0x00, byte(len(sized))}, sized...), pad...)),
		rtpPacket(45, 2, 0, true, nil),
	)
	if len(errs) != 0 || len(out) != 1 || !bytes.Equal(out[0].Data, sized) || !out[0].Keyframe {
		t.Fatal(errs, len(out))
	}

	// A new sequence header replaces the track; one that does not parse
	// leaves it.
	profile1 := []byte{0x0a, 0x0d, 0x20, 0x00, 0x00, 0x03, 0xb4, 0xff, 0x73, 0x6b, 0xe4, 0x80, 0x86, 0x80, 0x10}
	badSeq := []byte{0x0a, 0x01, 0x00}
	before := d.Track()
	out, _ = feed(d, rtpPacket(45, 3, 3000, true, append(append([]byte{0x10}, badSeq...), frame(0x10)...)))
	if len(out) != 1 || d.Track() != before {
		t.Fatal("bad sequence header changed the track")
	}
	out, _ = feed(d, rtpPacket(45, 4, 6000, true, append(append([]byte{0x20, byte(len(profile1))}, profile1...), frame(0x10)...)))
	if len(out) != 1 || d.Track() == before || d.Track().Config[1]>>5 != 1 {
		t.Fatal("new sequence header")
	}

	// Malformed payloads: a length past the end, a bad LEB128, a bad OBU
	// header, in a whole element and at the end of a fragment.
	for i, payload := range [][]byte{
		{0x00, 0x05, 0x30},
		{0x00, 0x80},
		{0x10, 0x80},
	} {
		d := mustDepacketizer(t, bare)
		if _, errs := feed(d, rtpPacket(45, 1, 0, true, payload)); len(errs) != 1 {
			t.Fatalf("%d: %v", i, errs)
		}
	}
	d = mustDepacketizer(t, bare)
	if _, errs := feed(d, rtpPacket(45, 1, 0, false, []byte{0x50, 0x80}), rtpPacket(45, 2, 0, true, []byte{0x90, 0x80})); len(errs) != 1 {
		t.Fatal(errs)
	}

	// Bounds: a temporal unit, a fragment and a fragment start over
	// MaxAccessUnit are dropped.
	small, err := NewDepacketizer(bare, DepacketizerConfig{MaxAccessUnit: 64})
	if err != nil {
		t.Fatal(err)
	}
	big := av1.AppendOBU(nil, av1.OBU{Header: []byte{0x30}, Payload: make([]byte, 50)}, false)
	key := append(av1.AppendOBU(nil, seqOBU(t, track), false), frame(0x10)...)
	out, errs = feed(small,
		rtpPacket(45, 1, 0, true, append([]byte{0x20, byte(len(key) - 4)}, key...)),
		rtpPacket(45, 2, 3000, true, append(append(append([]byte{0x00, byte(len(big))}, big...), byte(len(big))), big...)),
		rtpPacket(45, 3, 6000, false, append([]byte{0x50}, big...)),
		rtpPacket(45, 4, 6000, true, append([]byte{0x90}, big...)),
		rtpPacket(45, 5, 9000, true, append([]byte{0x50}, make([]byte, 70)...)),
	)
	if len(out) != 1 || len(errs) != 2 {
		t.Fatal(len(out), errs)
	}
}

func TestAV1Format(t *testing.T) {
	for _, fmtp := range []string{"profile=3", "level-idx=x", "tier=2"} {
		md := sdp.NewMedia("video", 0, "RTP/AVP")
		md.AddCodec(sdp.Codec{PayloadType: 45, Name: "AV1", ClockRate: 90000, Fmtp: fmtp})
		if _, err := ParseMedia(md); err != ErrFormat {
			t.Fatalf("%s: %v", fmtp, err)
		}
	}
	md := sdp.NewMedia("video", 0, "RTP/AVP")
	md.AddCodec(sdp.Codec{PayloadType: 45, Name: "AV1", ClockRate: 90000, Fmtp: "level-idx=5;profile=0;tier=0"})
	f, err := ParseMedia(md)
	if err != nil || f.Track.Codec != av.CodecAV1 || f.ClockRate != 90000 || f.Track.Config != nil {
		t.Fatal(f, err)
	}
}

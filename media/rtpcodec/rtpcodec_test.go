package rtpcodec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/aac"
	"github.com/lesismal/fib/media/codec/h264"
	"github.com/lesismal/fib/media/codec/h265"
	"github.com/lesismal/fib/media/internal/bitio"
	"github.com/lesismal/fib/webrtc/rtp"
	"github.com/lesismal/fib/webrtc/sdp"
)

var (
	fixtureOnce sync.Once
	h264Fix     struct {
		track *av.Track
		aus   []*av.Packet
	}
	h265Fix struct {
		track *av.Track
		aus   []*av.Packet
	}
)

// fixtures returns the tracks and access units of the H.264 and H.265
// captures.
func fixtures(t testing.TB) {
	fixtureOnce.Do(func() {
		out, d := depacketizeAll(t, readFormat(t, "h264"), readPackets(t, "h264"))
		h264Fix.track, h264Fix.aus = d.Track(), out
		out, d = depacketizeAll(t, readFormat(t, "h265"), readPackets(t, "h265"))
		h265Fix.track, h265Fix.aus = d.Track(), out
	})
}

func rtpPacket(pt uint8, seq uint16, ts uint32, marker bool, payload []byte) *rtp.Packet {
	return &rtp.Packet{Header: rtp.Header{PayloadType: pt, SequenceNumber: seq, Timestamp: ts, Marker: marker}, Payload: payload}
}

// feed depacketizes packets and returns what comes out and the errors.
func feed(d *Depacketizer, pkts ...*rtp.Packet) (out []*av.Packet, errs []error) {
	for _, p := range pkts {
		if err := d.Depacketize(p, func(pkt *av.Packet) { out = append(out, pkt) }); err != nil {
			errs = append(errs, err)
		}
	}
	return out, errs
}

func mustDepacketizer(t *testing.T, f *Format) *Depacketizer {
	t.Helper()
	d, err := NewDepacketizer(f, DepacketizerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func parameterSets(t *testing.T, path string) [][]byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var sets [][]byte
	for nal := range h264.SplitAnnexB(b) {
		sets = append(sets, nal)
	}
	return sets
}

func avcc(nalus ...[]byte) []byte {
	var b []byte
	for _, n := range nalus {
		b = h264.AppendAVCC(b, n)
	}
	return b
}

func TestPacketizerH264(t *testing.T) {
	fixtures(t)
	f := NewFormatOf(t, h264Fix.track, 96)
	p, err := NewPacketizer(f, PacketizerConfig{MTU: 300, SSRC: 7, Sequence: 100, Timestamp: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if p.SSRC() != 7 || p.Sequence() != 100 || p.HasOrigin() {
		t.Fatal("config not taken")
	}
	key := h264Fix.aus[0]
	var got []rtp.Packet
	collect := func(rp *rtp.Packet) error {
		c := *rp
		c.Payload = bytes.Clone(rp.Payload)
		got = append(got, c)
		return nil
	}
	if err := p.Packetize(key, collect); err != nil {
		t.Fatal(err)
	}
	if !p.HasOrigin() {
		t.Fatal("no origin after the first packet")
	}
	// Parameter sets aggregated first, the IDR in FU-As.
	if got[0].Payload[0]&0x1f != h264STAPA {
		t.Fatalf("first packet type %d", got[0].Payload[0]&0x1f)
	}
	for i, rp := range got {
		if rp.SequenceNumber != uint16(100+i) || rp.Timestamp != 1000 || rp.SSRC != 7 || rp.Marker != (i == len(got)-1) {
			t.Fatalf("packet %d: %+v", i, rp.Header)
		}
	}
	if typ := got[1].Payload[0] & 0x1f; typ != h264FUA {
		t.Fatalf("second packet type %d", typ)
	}
	packets, octets := p.Counts()
	if packets != uint32(len(got)) || octets == 0 {
		t.Fatalf("counts %d %d", packets, octets)
	}
	sr := p.SenderReport(time.Unix(1, 0), key.PTS+time.Second)
	if sr.SSRC != 7 || sr.RTPTime != 1000+90000 || sr.PacketCount != packets || sr.NTPTime != rtp.NTPTime(time.Unix(1, 0)) {
		t.Fatalf("sender report %+v", sr)
	}
	// A later frame is stamped from the origin.
	got = got[:0]
	if err := p.Packetize(h264Fix.aus[3], collect); err != nil {
		t.Fatal(err)
	}
	if want := p.Timestamp(h264Fix.aus[3].PTS); got[0].Timestamp != want || want != 1000+uint32(av.ToTicks(h264Fix.aus[3].PTS-key.PTS, 90000)) {
		t.Fatalf("timestamp %d, want %d", got[0].Timestamp, want)
	}

	// A keyframe that carries its parameter sets gets no others.
	sps := parameterSets(t, "../codec/h264/testdata/h264_base_360.ps")
	au := &av.Packet{Keyframe: true, Data: avcc(sps[0], sps[1], []byte{0x09, 0xf0}, []byte{0x65, 1, 2, 3})}
	got = got[:0]
	if err := p.Packetize(au, collect); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !bytes.Contains(got[0].Payload, sps[0]) || bytes.Contains(got[0].Payload, h264Fix.track.Config[8:12]) {
		t.Fatalf("own parameter sets: %d packets", len(got))
	}
	// Errors.
	if err := p.Packetize(&av.Packet{Data: []byte{0, 0, 0, 9, 1}}, collect); err != ErrPacket {
		t.Fatalf("bad AVCC: %v", err)
	}
	stop := errors.New("stop")
	if err := p.Packetize(key, func(*rtp.Packet) error { return stop }); err != stop {
		t.Fatalf("emit error: %v", err)
	}
	if err := p.Packetize(au, func(*rtp.Packet) error { return stop }); err != stop {
		t.Fatalf("emit error: %v", err)
	}
	if err := p.SetTrack(&av.Track{Codec: av.CodecH265}); err != ErrUnsupported {
		t.Fatalf("SetTrack: %v", err)
	}
	if _, err := NewPacketizer(f, PacketizerConfig{MTU: 20}); err == nil {
		t.Fatal("tiny MTU taken")
	}
	if _, err := NewPacketizer(&Format{Track: &av.Track{Codec: av.CodecAV1}}, PacketizerConfig{}); err != ErrUnsupported {
		t.Fatalf("AV1: %v", err)
	}
	// Random initial values.
	q, _ := NewPacketizer(f, PacketizerConfig{})
	r, _ := NewPacketizer(f, PacketizerConfig{})
	if q.SSRC() == r.SSRC() && q.Sequence() == r.Sequence() {
		t.Fatal("not random")
	}
}

func TestPacketizerH264Mode0(t *testing.T) {
	fixtures(t)
	f := NewFormatOf(t, h264Fix.track, 96)
	f.PacketizationMode = 0
	p, _ := NewPacketizer(f, PacketizerConfig{})
	n := 0
	err := p.Packetize(h264Fix.aus[0], func(rp *rtp.Packet) error {
		if rp.Payload[0]&0x1f == h264STAPA {
			t.Fatal("STAP-A in mode 0")
		}
		n++
		return nil
	})
	if err != nil || n < 3 {
		t.Fatalf("%v, %d packets", err, n)
	}
}

func TestDepacketizerH264Types(t *testing.T) {
	fixtures(t)
	f := NewFormatOf(t, h264Fix.track, 96)
	var cfg h264.AVCConfig
	_ = cfg.Unmarshal(f.Track.Config)
	idr := []byte{0x65, 0x88, 1, 2, 3, 4, 5, 6}
	slice := []byte{0x41, 0x9a, 7, 8}
	sized := func(skip int, nalus ...[]byte) []byte {
		var b []byte
		for _, n := range nalus {
			b = binary.BigEndian.AppendUint16(b, uint16(len(n)))
			b = append(b, make([]byte, skip)...)
			b = append(b, n...)
		}
		return b
	}
	cases := []struct {
		name    string
		payload [][]byte
		want    []byte
		key     bool
	}{
		{"STAP-A", [][]byte{append([]byte{h264STAPA}, sized(0, cfg.SPS[0], cfg.PPS[0], idr)...)}, avcc(idr), true},
		{"STAP-B", [][]byte{append([]byte{h264STAPB, 0, 1}, sized(0, idr, slice)...)}, avcc(idr, slice), true},
		{"MTAP16", [][]byte{append([]byte{h264MTAP16, 0, 1}, sized(3, idr)...)}, avcc(idr), true},
		{"MTAP24", [][]byte{append([]byte{h264MTAP24, 0, 1}, sized(4, idr, slice)...)}, avcc(idr, slice), true},
		{"FU-A", [][]byte{{0x7c, 0x85, 0x88, 1, 2}, {0x7c, 0x05, 3, 4}, {0x7c, 0x45, 5, 6}}, avcc(idr), true},
		{"FU-B", [][]byte{{0x7d, 0x85, 0, 9, 0x88, 1, 2}, {0x7c, 0x45, 3, 4, 5, 6}}, avcc(idr), true},
		{"single", [][]byte{{0x09, 0xf0}, idr}, avcc(idr), true},
	}
	for _, tc := range cases {
		d := mustDepacketizer(t, f)
		var pkts []*rtp.Packet
		for i, pl := range tc.payload {
			pkts = append(pkts, rtpPacket(96, uint16(10+i), 9000, i == len(tc.payload)-1, pl))
		}
		out, errs := feed(d, pkts...)
		if len(errs) > 0 || len(out) != 1 || !bytes.Equal(out[0].Data, tc.want) || out[0].Keyframe != tc.key {
			t.Errorf("%s: %v %d %v", tc.name, errs, len(out), out)
		}
	}
	// Malformed payloads are reported and drop the access unit.
	for _, pl := range [][]byte{{0}, {30}, {h264STAPA, 0, 9, 1}, {h264STAPA, 0}, {h264STAPB}, {h264MTAP16, 0}, {h264MTAP16, 0, 0, 0, 0, 0}, {h264FUA}, {h264FUB, 0x80, 1}} {
		d := mustDepacketizer(t, f)
		if _, errs := feed(d, rtpPacket(96, 1, 0, true, pl)); len(errs) != 1 {
			t.Errorf("%x: %v", pl, errs)
		}
	}
	d := mustDepacketizer(t, f)
	if _, errs := feed(d, rtpPacket(97, 1, 0, true, idr)); len(errs) != 1 || errs[0] != ErrPayloadType {
		t.Errorf("payload type: %v", errs)
	}
	// An empty payload and an FU without its start are nothing.
	if out, errs := feed(d, rtpPacket(96, 2, 0, false, nil), rtpPacket(96, 3, 0, true, []byte{0x7c, 0x45, 1})); len(out)+len(errs) != 0 {
		t.Errorf("empty: %v %v", out, errs)
	}
}

func TestDepacketizerLoss(t *testing.T) {
	fixtures(t)
	f := NewFormatOf(t, h264Fix.track, 96)
	idr := []byte{0x65, 0x88, 1, 2}
	slice := []byte{0x41, 0x9a, 3, 4}
	d := mustDepacketizer(t, f)
	out, _ := feed(d,
		// A slice before any keyframe is dropped.
		rtpPacket(96, 1, 0, true, slice),
		rtpPacket(96, 2, 3000, true, idr),
		rtpPacket(96, 3, 6000, true, slice),
		// A duplicate and an old packet are dropped.
		rtpPacket(96, 3, 6000, true, slice),
		rtpPacket(96, 2, 3000, true, idr),
		// Packet 4 is lost: video waits for the next keyframe.
		rtpPacket(96, 5, 9000, true, slice),
		rtpPacket(96, 6, 12000, true, slice),
		rtpPacket(96, 7, 15000, true, idr),
		rtpPacket(96, 8, 18000, true, slice),
	)
	if len(out) != 4 || !out[0].Keyframe || !out[2].Keyframe || d.Lost() != 1 {
		t.Fatalf("%d out, lost %d", len(out), d.Lost())
	}
	if out[2].PTS != av.FromTicks(15000, 90000) {
		t.Fatalf("pts %v", out[2].PTS-out[0].PTS)
	}
	// A lost marker: the next timestamp ends the access unit.
	out, _ = feed(d, rtpPacket(96, 9, 21000, false, slice), rtpPacket(96, 10, 24000, true, slice))
	if len(out) != 2 {
		t.Fatalf("lost marker: %d out", len(out))
	}
	// A loss inside a fragmented NAL unit drops it.
	out, _ = feed(d,
		rtpPacket(96, 11, 27000, false, []byte{0x7c, 0x85, 1}),
		rtpPacket(96, 13, 27000, true, []byte{0x7c, 0x45, 2}),
		rtpPacket(96, 14, 30000, true, slice),
		rtpPacket(96, 15, 33000, true, idr),
	)
	if len(out) != 1 || !out[0].Keyframe {
		t.Fatalf("FU loss: %d out", len(out))
	}
	// An access unit over the bound is dropped.
	d, _ = NewDepacketizer(f, DepacketizerConfig{MaxAccessUnit: 10})
	out, errs := feed(d, rtpPacket(96, 1, 0, true, append([]byte{0x65}, make([]byte, 20)...)),
		rtpPacket(96, 2, 1, false, []byte{0x7c, 0x85, 1}), rtpPacket(96, 3, 1, true, []byte{0x7c, 0x45, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}),
		rtpPacket(96, 4, 2, true, idr))
	if len(out) != 1 || len(errs) != 1 {
		t.Fatalf("bound: %d out %v", len(out), errs)
	}
	// Without parameter sets nothing comes out until they come in band.
	d = mustDepacketizer(t, &Format{PayloadType: 96, ClockRate: 90000, Track: &av.Track{Codec: av.CodecH264}})
	var cfg h264.AVCConfig
	_ = cfg.Unmarshal(f.Track.Config)
	out, _ = feed(d, rtpPacket(96, 1, 0, true, idr), rtpPacket(96, 2, 1, false, cfg.SPS[0]), rtpPacket(96, 3, 1, false, cfg.PPS[0]), rtpPacket(96, 4, 1, true, idr))
	if len(out) != 1 || d.Track().Width != 320 {
		t.Fatalf("in-band: %d out, %+v", len(out), d.Track())
	}
	// Flush without anything open does nothing.
	d.Flush(func(*av.Packet) { t.Fatal("flushed nothing") })
}

func TestDepacketizerParameterSetChange(t *testing.T) {
	fixtures(t)
	f := NewFormatOf(t, h264Fix.track, 96)
	d := mustDepacketizer(t, f)
	sets := parameterSets(t, "../codec/h264/testdata/h264_base_360.ps")
	idr := []byte{0x65, 0x88, 1, 2}
	out, _ := feed(d, rtpPacket(96, 1, 0, true, idr))
	first := d.Track()
	if len(out) != 1 || first != f.Track {
		t.Fatal("track replaced without a change")
	}
	// The same parameter sets again change nothing.
	var cfg h264.AVCConfig
	_ = cfg.Unmarshal(f.Track.Config)
	feed(d, rtpPacket(96, 2, 1, false, cfg.SPS[0]), rtpPacket(96, 3, 1, true, idr))
	if d.Track() != first {
		t.Fatal("track replaced by the same sets")
	}
	feed(d, rtpPacket(96, 4, 2, false, sets[0]), rtpPacket(96, 5, 2, false, sets[1]), rtpPacket(96, 6, 2, true, idr))
	if tr := d.Track(); tr == first || tr.Width != 640 || tr.Height != 360 {
		t.Fatalf("new track %+v", tr)
	}
	// A broken SPS makes no config, and the track stays.
	before := d.Track()
	feed(d, rtpPacket(96, 7, 3, false, []byte{0x67, 0xff}), rtpPacket(96, 8, 3, true, idr))
	if d.Track() != before {
		t.Fatal("broken SPS taken")
	}

	// H.265 likewise.
	f5 := NewFormatOf(t, h265Fix.track, 96)
	d = mustDepacketizer(t, f5)
	ps := parameterSets(t, "../codec/h265/testdata/h265_main_720.ps")
	irap := []byte{0x26, 0x01, 0xaf, 1, 2}
	out, _ = feed(d, rtpPacket(96, 1, 0, false, ps[0]), rtpPacket(96, 2, 0, false, ps[1]), rtpPacket(96, 3, 0, false, ps[2]),
		rtpPacket(96, 4, 0, false, []byte{0x46, 0x01, 0x10}), rtpPacket(96, 5, 0, true, irap))
	if len(out) != 1 || !out[0].Keyframe || d.Track().Width != 1280 {
		t.Fatalf("h265: %d out %+v", len(out), d.Track())
	}
}

func TestH265Types(t *testing.T) {
	fixtures(t)
	f := NewFormatOf(t, h265Fix.track, 96)
	irap := []byte{0x26, 0x01, 0xaf, 1, 2}
	trail := []byte{0x02, 0x01, 0xd0, 3}
	ap := []byte{h265AP << 1, 1}
	for _, n := range [][]byte{irap, trail, {1}} {
		ap = binary.BigEndian.AppendUint16(ap, uint16(len(n)))
		ap = append(ap, n...)
	}
	d := mustDepacketizer(t, f)
	out, errs := feed(d,
		rtpPacket(96, 1, 0, false, []byte{h265PACI << 1, 1, 0, 0}),
		rtpPacket(96, 2, 0, true, ap),
		rtpPacket(96, 3, 3000, false, []byte{h265FU << 1, 1, 0x80 | 19, 0xaf, 1}),
		rtpPacket(96, 4, 3000, true, []byte{h265FU << 1, 1, 0x40 | 19, 2}),
		// A fragment without its start.
		rtpPacket(96, 5, 6000, true, []byte{h265FU << 1, 1, 0x40 | 1, 2}),
	)
	if len(errs) > 0 || len(out) != 2 || !bytes.Equal(out[0].Data, avcc(irap, trail)) || !bytes.Equal(out[1].Data, avcc([]byte{0x26, 0x01, 0xaf, 1, 2})) {
		t.Fatalf("%v %d", errs, len(out))
	}
	for _, pl := range [][]byte{{1}, {h265FU << 1, 1}, {h265AP << 1, 1, 0, 9}} {
		if _, errs := feed(mustDepacketizer(t, f), rtpPacket(96, 1, 0, true, pl)); len(errs) != 1 {
			t.Errorf("%x: %v", pl, errs)
		}
	}
	d, _ = NewDepacketizer(f, DepacketizerConfig{MaxAccessUnit: 4})
	if _, errs := feed(d, rtpPacket(96, 1, 0, true, []byte{h265FU << 1, 1, 0x80 | 19, 1, 2, 3, 4, 5})); len(errs) != 1 {
		t.Errorf("bound: %v", errs)
	}
	// Loss mid-fragment.
	d = mustDepacketizer(t, f)
	out, _ = feed(d, rtpPacket(96, 1, 0, false, []byte{h265FU << 1, 1, 0x80 | 19, 1}), rtpPacket(96, 3, 0, true, []byte{h265FU << 1, 1, 0x40 | 19, 2}))
	if len(out) != 0 {
		t.Fatal("damaged unit emitted")
	}
	// The packetizer aggregates and fragments, and its errors.
	p, _ := NewPacketizer(f, PacketizerConfig{MTU: 100})
	var types []h265.NALUType
	err := p.Packetize(h265Fix.aus[0], func(rp *rtp.Packet) error {
		types = append(types, h265.Type(rp.Payload))
		return nil
	})
	if err != nil || types[0] != h265AP || types[1] != h265FU {
		t.Fatalf("%v %v", err, types)
	}
	if err := p.Packetize(&av.Packet{Data: []byte{9}}, func(*rtp.Packet) error { return nil }); err != ErrPacket {
		t.Fatal(err)
	}
	stop := errors.New("stop")
	if err := p.Packetize(h265Fix.aus[0], func(*rtp.Packet) error { return stop }); err != stop {
		t.Fatal(err)
	}
	// FU emit error after its first fragment.
	n := 0
	if err := p.Packetize(&av.Packet{Data: avcc(append([]byte{0x02, 0x01}, make([]byte, 300)...))}, func(*rtp.Packet) error {
		if n++; n > 1 {
			return stop
		}
		return nil
	}); err != stop {
		t.Fatal(err)
	}
}

func TestAAC(t *testing.T) {
	cfg := aac.Config{ObjectType: 2, SampleRate: 48000, ChannelConfig: 2}
	config, _ := cfg.Append(nil)
	track := &av.Track{Codec: av.CodecAAC, Config: config, SampleRate: 48000, Channels: 2}
	f := NewFormatOf(t, track, 97)
	hdr := func(sizes ...int) []byte {
		w := bitio.NewWriter(nil)
		w.Bits(uint64(16*len(sizes)), 16)
		for _, s := range sizes {
			w.Bits(uint64(s), 13)
			w.Bits(0, 3)
		}
		return w.Bytes()
	}
	// Three access units in one packet.
	pl := append(hdr(2, 3, 1), 1, 1, 2, 2, 2, 3)
	d := mustDepacketizer(t, f)
	out, errs := feed(d, rtpPacket(97, 1, 0, true, pl))
	if len(errs) > 0 || len(out) != 3 || !bytes.Equal(out[1].Data, []byte{2, 2, 2}) || out[2].PTS != 2*cfg.FrameDuration() {
		t.Fatalf("%v %v", errs, out)
	}
	// One fragmented over three packets, and one cut short by a loss.
	out, errs = feed(d,
		rtpPacket(97, 2, 1024, false, append(hdr(5), 1, 2)),
		rtpPacket(97, 3, 1024, false, append(hdr(5), 3, 4)),
		rtpPacket(97, 4, 1024, true, append(hdr(5), 5)),
		rtpPacket(97, 5, 2048, false, append(hdr(5), 1, 2)),
		rtpPacket(97, 7, 2048, true, append(hdr(5), 5)),
		rtpPacket(97, 8, 3072, true, append(hdr(5), 1, 2)),
	)
	if len(out) != 1 || !bytes.Equal(out[0].Data, []byte{1, 2, 3, 4, 5}) || len(errs) != 2 {
		t.Fatalf("fragments: %v %v", errs, out)
	}
	for _, pl := range [][]byte{{0}, {0, 32, 0}, append(hdr(9, 9), 1), {0, 16, 0xff, 0xff}} {
		if _, errs := feed(mustDepacketizer(t, f), rtpPacket(97, 1, 0, true, pl)); len(errs) != 1 {
			t.Errorf("%x: %v", pl, errs)
		}
	}
	d, _ = NewDepacketizer(f, DepacketizerConfig{MaxAccessUnit: 3})
	if _, errs := feed(d, rtpPacket(97, 1, 0, false, append(hdr(5), 1))); len(errs) != 1 {
		t.Errorf("bound: %v", errs)
	}
	// The optional AU header fields and the auxiliary section.
	g := *f
	g.CTSDeltaLength, g.DTSDeltaLength, g.RandomAccessIndication, g.StreamStateIndication, g.AuxDataSizeLength = 4, 4, true, 2, 8
	w := bitio.NewWriter(nil)
	w.Bits(13+3+1+4+1+4+1+2+13+3+1+1+1+2, 16)
	w.Bits(1, 13)
	w.Bits(0, 3)
	w.Bits(1, 1)
	w.Bits(2, 4)
	w.Bits(1, 1)
	w.Bits(1, 4)
	w.Bits(1, 1)
	w.Bits(0, 2)
	w.Bits(2, 13)
	w.Bits(1, 3)
	w.Bits(0, 1)
	w.Bits(0, 1)
	w.Bits(0, 1)
	w.Bits(0, 2)
	pl = append(w.Bytes(), 8, 0xaa, 7, 8, 8)
	out, errs = feed(mustDepacketizer(t, &g), rtpPacket(97, 1, 0, true, pl))
	if len(errs) > 0 || len(out) != 2 || !bytes.Equal(out[1].Data, []byte{8, 8}) {
		t.Fatalf("fields: %v %v", errs, out)
	}
	if _, errs := feed(mustDepacketizer(t, &g), rtpPacket(97, 1, 0, true, append(w.Bytes(), 200))); len(errs) != 1 {
		t.Fatal("aux overrun taken")
	}
	// A packetizer refuses an access unit its size field cannot hold.
	p, _ := NewPacketizer(f, PacketizerConfig{})
	if err := p.Packetize(&av.Packet{Data: make([]byte, 1<<13)}, func(*rtp.Packet) error { return nil }); err != ErrPacket {
		t.Fatal(err)
	}
	stop := errors.New("stop")
	if err := p.Packetize(&av.Packet{Data: make([]byte, 3000)}, func(*rtp.Packet) error { return stop }); err != stop {
		t.Fatal(err)
	}
	if _, err := NewDepacketizer(&Format{Track: track}, DepacketizerConfig{}); err != ErrFormat {
		t.Fatal(err)
	}
	if d := aacFrameDuration(nil, 44100); d != av.FromTicks(1024, 44100) {
		t.Fatal(d)
	}
}

// latmElement builds an AudioMuxElement with cpresent=1: a new
// StreamMuxConfig of version, or the same one, and the access units.
func latmElement(asc []byte, version int, same bool, subframes int, aus ...[]byte) []byte {
	w := bitio.NewWriter(nil)
	w.Flag(same)
	if !same {
		r := bitio.NewReader(asc)
		n, _ := ascBits(&r)
		w.Bits(uint64(version), 1)
		if version == 1 {
			w.Bits(0, 1)    // audioMuxVersionA
			w.Bits(0, 2)    // taraBufferFullness: one byte
			w.Bits(0xff, 8) //
		}
		w.Bits(1, 1)
		w.Bits(uint64(subframes), 6)
		w.Bits(0, 7)
		if version == 1 {
			w.Bits(0, 2)
			w.Bits(uint64(n+8), 8) // ascLen, with 8 bits of padding
		}
		r = bitio.NewReader(asc)
		w.Bits(uint64(r.Bits(n)), n)
		if version == 1 {
			w.Bits(0, 8)
		}
		w.Bits(0, 3)
		w.Bits(0xff, 8)
		w.Bits(1, 1) // otherDataPresent
		if version == 1 {
			w.Bits(0, 2)
			w.Bits(0, 8)
		} else {
			w.Bits(1, 1)
			w.Bits(0, 8)
			w.Bits(0, 1)
			w.Bits(0, 8)
		}
		w.Bits(1, 1) // crcCheckPresent
		w.Bits(0, 8)
	}
	for _, au := range aus {
		n := len(au)
		for ; n >= 255; n -= 255 {
			w.Bits(255, 8)
		}
		w.Bits(uint64(n), 8)
		for _, b := range au {
			w.Bits(uint64(b), 8)
		}
	}
	return w.Bytes()
}

func TestLATMInBand(t *testing.T) {
	cfg := aac.Config{ObjectType: 2, SampleRate: 44100, ChannelConfig: 1}
	asc, _ := cfg.Append(nil)
	md := sdp.NewMedia("audio", 0, "RTP/AVP")
	md.AddCodec(sdp.Codec{PayloadType: 97, Name: "MP4A-LATM", ClockRate: 44100, Fmtp: "object=2"})
	f, err := ParseMedia(md)
	if err != nil || !f.CPresent || f.Track.Config != nil {
		t.Fatalf("%v %+v", err, f)
	}
	d := mustDepacketizer(t, f)
	big := bytes.Repeat([]byte{7}, 300)
	el := latmElement(asc, 0, false, 1, []byte{1, 2}, big)
	out, errs := feed(d,
		// Nothing without a config.
		rtpPacket(97, 1, 0, true, latmElement(nil, 0, true, 0, []byte{1})),
		rtpPacket(97, 2, 1024, false, el[:10]),
		rtpPacket(97, 3, 1024, true, el[10:]),
		rtpPacket(97, 4, 3072, true, latmElement(nil, 0, true, 0, []byte{3}, []byte{4})),
	)
	if len(errs) != 1 || len(out) != 4 || !bytes.Equal(out[1].Data, big) || !bytes.Equal(d.Track().Config, asc) {
		t.Fatalf("%v %d %x", errs, len(out), d.Track().Config)
	}
	// A config of version 1.
	cfg2 := aac.Config{ObjectType: 2, SampleRate: 48000, ChannelConfig: 2}
	asc2, _ := cfg2.Append(nil)
	out, errs = feed(d, rtpPacket(97, 5, 4096, true, latmElement(asc2, 1, false, 0, []byte{5})))
	if len(errs) > 0 || len(out) != 1 || !bytes.Equal(d.Track().Config, asc2) || d.Track().Channels != 2 {
		t.Fatalf("version 1: %v %d %+v", errs, len(out), d.Track())
	}
	// Malformed elements and an oversized one.
	for _, el := range [][]byte{{0x80, 0xff}, latmElement(asc, 0, false, 0, []byte{1})[:3], {0x40}} {
		if _, errs := feed(mustDepacketizer(t, f), rtpPacket(97, 1, 0, true, el)); len(errs) != 1 {
			t.Errorf("%x: %v", el, errs)
		}
	}
	d, _ = NewDepacketizer(f, DepacketizerConfig{MaxAccessUnit: 4})
	if _, errs := feed(d, rtpPacket(97, 1, 0, true, el)); len(errs) != 1 {
		t.Errorf("bound: %v", errs)
	}
	d.dp.loss()

	// StreamMuxConfigs the package does not take.
	for _, b := range [][]byte{
		{0xc0},                         // audioMuxVersionA
		{0x20, 0x10, 0, 0, 0, 0},       // numProgram
		{0x00, 0x00, 0x00},             // object type 0
		{0x40, 0x00, 0x20, 0x00, 0x00}, // channel config 0
		{0x40, 0x00, 0x24, 0x10, 0x40}, // frame length type 2
	} {
		if _, err := parseStreamMuxConfig(b); err == nil {
			t.Errorf("%x taken", b)
		}
	}
	// The object types with extra GASpecificConfig fields, and explicit
	// SBR signalling.
	for _, a := range [][]byte{
		bitsOf(t, 5, 23, 4, 3, 4, 2, 1, 0, 1, 0, 1, 1, 3, 0, 1, 0),       // ER AAC LD
		bitsOf(t, 5, 5, 4, 3, 4, 2, 4, 8, 5, 2, 1, 0, 1, 0, 1, 0),        // SBR on LC
		bitsOf(t, 5, 6, 4, 3, 4, 2, 1, 0, 1, 1, 14, 0, 1, 0, 3, 0),       // core coder delay, layerNr
		bitsOf(t, 5, 2, 4, 15, 24, 44100, 4, 1, 1, 0, 1, 0, 1, 0),        // explicit rate
		bitsOf(t, 5, 22, 4, 3, 4, 1, 1, 0, 1, 0, 1, 1, 16, 0, 1, 0),      // ER BSAC
		bitsOf(t, 5, 17, 4, 3, 4, 1, 1, 0, 1, 0, 1, 1, 3, 0, 1, 0),       // ER AAC LC
		bitsOf(t, 5, 20, 4, 3, 4, 1, 1, 0, 1, 0, 1, 1, 3, 0, 3, 0, 1, 0), // ER AAC scalable
	} {
		r := bitio.NewReader(a)
		if _, err := ascBits(&r); err != nil {
			t.Errorf("%x: %v", a, err)
		}
	}
	for _, a := range [][]byte{{0x20}, bitsOf(t, 5, 31, 6, 0, 4, 3, 4, 1)} {
		r := bitio.NewReader(a)
		if _, err := ascBits(&r); err == nil {
			t.Errorf("%x taken", a)
		}
	}
	if _, err := appendStreamMuxConfig(nil, []byte{0}); err == nil {
		t.Error("bad ASC taken")
	}
}

// bitsOf writes pairs of (width, value).
func bitsOf(t *testing.T, pairs ...int) []byte {
	w := bitio.NewWriter(nil)
	for i := 0; i+1 < len(pairs); i += 2 {
		w.Bits(uint64(pairs[i+1]), pairs[i])
	}
	return append(w.Bytes(), 0, 0)
}

func TestOpusG711(t *testing.T) {
	track := &av.Track{Codec: av.CodecPCMU, SampleRate: 8000, Channels: 2}
	f := NewFormatOf(t, track, 99)
	if f.PayloadType != PayloadTypePCMU {
		t.Fatal(f.PayloadType)
	}
	p, _ := NewPacketizer(f, PacketizerConfig{MTU: 112, Timestamp: 5})
	var ts []uint32
	err := p.Packetize(&av.Packet{Data: make([]byte, 250)}, func(rp *rtp.Packet) error {
		ts = append(ts, rp.Timestamp)
		if len(rp.Payload)%2 != 0 {
			t.Fatal("split mid-sample")
		}
		return nil
	})
	if err != nil || len(ts) != 3 || ts[1]-ts[0] != 50 {
		t.Fatalf("%v %v", err, ts)
	}
	stop := errors.New("stop")
	if err := p.Packetize(&av.Packet{Data: make([]byte, 10)}, func(*rtp.Packet) error { return stop }); err != stop {
		t.Fatal(err)
	}
	d := mustDepacketizer(t, f)
	if out, _ := feed(d, rtpPacket(0, 1, 0, true, nil)); len(out) != 0 {
		t.Fatal("empty payload emitted")
	}
	d.dp.loss()
}

func TestFormat(t *testing.T) {
	fixtures(t)
	if _, err := NewFormat(&av.Track{Codec: av.CodecAV1}, 96); err != ErrUnsupported {
		t.Fatal(err)
	}
	if _, err := NewFormat(&av.Track{Codec: av.CodecAAC}, 96); err != ErrUnsupported {
		t.Fatal(err)
	}
	if _, err := NewLATMFormat(&av.Track{Codec: av.CodecOpus}, 96); err != ErrUnsupported {
		t.Fatal(err)
	}
	if _, err := NewLATMFormat(&av.Track{Codec: av.CodecAAC}, 96); err != ErrUnsupported {
		t.Fatal(err)
	}
	cfg := aac.Config{ObjectType: 2, SampleRate: 44100, ChannelConfig: 1}
	asc, _ := cfg.Append(nil)
	f := NewFormatOf(t, &av.Track{Codec: av.CodecAAC, Config: asc}, 97)
	if f.ClockRate != 44100 {
		t.Fatal(f.ClockRate)
	}
	for _, tr := range []*av.Track{
		{Codec: av.CodecAV1},
		{Codec: av.CodecAAC, Config: []byte{0}},
	} {
		f := &Format{Track: tr}
		if _, err := f.Media(); err != ErrUnsupported {
			t.Errorf("%v: %v", tr.Codec, err)
		}
	}
	lf := &Format{Track: &av.Track{Codec: av.CodecAAC, Config: []byte{0x08, 0}}, LATM: true}
	if _, err := lf.Media(); err == nil {
		t.Error("LATM of an object type without GASpecificConfig")
	}
	// Formats round trip through their media sections.
	for _, tr := range []*av.Track{
		h264Fix.track, h265Fix.track,
		{Codec: av.CodecH264}, {Codec: av.CodecH265},
		{Codec: av.CodecOpus, Channels: 2}, {Codec: av.CodecOpus, Channels: 1},
		{Codec: av.CodecPCMA, Channels: 1},
	} {
		f := NewFormatOf(t, tr, 96)
		f.Control = "trackID=3"
		md, err := f.Media()
		if err != nil {
			t.Fatal(err)
		}
		g, err := ParseMedia(md)
		if err != nil || g.Control != "trackID=3" || g.Track.Codec != tr.Codec || !bytes.Equal(g.Track.Config, tr.Config) || g.Track.Width != tr.Width {
			t.Errorf("%v: %v %+v", tr.Codec, err, g)
		}
		if tr.Codec == av.CodecOpus && g.Track.Channels != max(tr.Channels, 1) && !(tr.Channels == 1 && g.Track.Channels == 2) {
			t.Errorf("opus channels %d", g.Track.Channels)
		}
	}
	parse := func(rtpmap, fmtp string) (*Format, error) {
		md := sdp.NewMedia("video", 0, "RTP/AVP", "96")
		if rtpmap != "" {
			md.AddAttribute("rtpmap", "96 "+rtpmap)
		}
		if fmtp != "" {
			md.AddAttribute("fmtp", "96 "+fmtp)
		}
		return ParseMedia(md)
	}
	sps := "Z2QAFazZQUH7ARAAAAMAEAAAAwPI8UKZYA" // base64 without padding
	for _, tc := range []struct {
		rtpmap, fmtp string
		err          error
	}{
		{"H264/90000", "packetization-mode=3", ErrFormat},
		{"H264/90000", "packetization-mode=1;sprop-parameter-sets=!!", ErrFormat},
		{"H264/90000", "sprop-parameter-sets=" + sps + ",aOvssiw", nil},
		{"H265/90000", "sprop-max-don-diff=2", ErrUnsupported},
		{"H265/90000", "sprop-vps=!!", ErrFormat},
		{"MPEG4-GENERIC/48000/2", "mode=CELP-cbr", ErrUnsupported},
		{"MPEG4-GENERIC/48000/2", "mode=AAC-hbr;streamtype=4", ErrUnsupported},
		{"MPEG4-GENERIC/48000/2", "mode=AAC-hbr;config=zz", ErrFormat},
		{"MPEG4-GENERIC/48000/2", "mode=AAC-hbr;config=00", ErrFormat},
		{"MPEG4-GENERIC/48000/2", "mode=AAC-hbr;config=1190;sizelength=x", ErrFormat},
		{"MPEG4-GENERIC/48000/2", "mode=AAC-hbr;config=1190", ErrFormat},
		{"MPEG4-GENERIC/48000/2", "mode=AAC-lbr;config=1188;sizelength=6;randomaccessindication=1;streamtype=5", nil},
		{"MP4A-LATM/44100", "cpresent=0", ErrFormat},
		{"MP4A-LATM/44100", "cpresent=0;config=zz", ErrFormat},
		{"MP4A-LATM/44100", "cpresent=0;config=00", ErrFormat},
		{"X/90000", "", ErrUnsupported},
		{"H264/0", "", ErrUnsupported},
	} {
		if _, err := parse(tc.rtpmap, tc.fmtp); err != tc.err {
			t.Errorf("%s %s: %v, want %v", tc.rtpmap, tc.fmtp, err, tc.err)
		}
	}
	// A config of AAC with channel configuration 0 takes the rtpmap's.
	if f, err := parse("MPEG4-GENERIC/48000/2", "mode=AAC-hbr;config=1180;sizelength=13"); err != nil || f.Track.Channels != 2 {
		t.Errorf("channels: %v", err)
	}
	// Static payload types need no rtpmap; the first usable format wins.
	md := sdp.NewMedia("audio", 0, "RTP/AVP", "98", "0")
	if f, err := ParseMedia(md); err != nil || f.Track.Codec != av.CodecPCMU || f.PayloadType != 0 {
		t.Fatalf("static: %v", err)
	}
	if _, err := ParseMedia(sdp.NewMedia("audio", 0, "RTP/AVP", "98")); err != ErrUnsupported {
		t.Fatal(err)
	}
	// H.264 without a usable SPS has no config.
	for _, fmtp := range []string{"sprop-parameter-sets=aOvssiw", "sprop-parameter-sets=Z2Q,aOvssiw"} {
		if f, err := parse("H264/90000", fmtp); err != nil || f.Track.Config != nil {
			t.Errorf("%s: %v", fmtp, err)
		}
	}
	if f, err := parse("H265/90000", "sprop-vps=QAE;sprop-sps=QgE;sprop-pps=RAE"); err != nil || f.Track.Config != nil {
		t.Errorf("h265 broken sets: %v", err)
	}
	if !strings.Contains(h264Fmtp(&Format{Track: &av.Track{Config: []byte{1}}}), "packetization-mode=0") {
		t.Error("h264 fmtp")
	}
	if h265Fmtp(nil) != "" {
		t.Error("h265 fmtp")
	}
}

func TestReorder(t *testing.T) {
	now := time.Unix(100, 0)
	r := NewReorder(5, 0) // rounds up to 8
	if len(r.slots) != 8 || r.delay != DefaultReorderDelay {
		t.Fatal(len(r.slots), r.delay)
	}
	var got []uint16
	emit := func(p *rtp.Packet) { got = append(got, p.SequenceNumber) }
	push := func(seqs ...uint16) {
		for _, s := range seqs {
			r.Push(now, rtpPacket(96, s, 0, false, []byte{byte(s)}), emit)
		}
	}
	push(65534, 65535, 1, 0, 2)
	if want := []uint16{65534, 65535, 0, 1, 2}; !equal(got, want) {
		t.Fatalf("%v", got)
	}
	if _, ok := r.NextTimeout(); ok {
		t.Fatal("timeout with nothing held")
	}
	got = nil
	push(4, 4, 5)
	if len(got) != 0 {
		t.Fatalf("emitted across a gap: %v", got)
	}
	at, ok := r.NextTimeout()
	if !ok || !at.Equal(now.Add(DefaultReorderDelay)) {
		t.Fatal(at, ok)
	}
	r.Expire(now, emit)
	if len(got) != 0 {
		t.Fatal("expired early")
	}
	r.Expire(at, emit)
	if !equal(got, []uint16{4, 5}) {
		t.Fatalf("%v", got)
	}
	// Late and duplicate packets are dropped.
	got = nil
	push(3, 5, 6)
	if !equal(got, []uint16{6}) {
		t.Fatalf("%v", got)
	}
	// Too many held: the gap is given up.
	got = nil
	push(8, 9, 10, 11, 12, 13, 14)
	if !equal(got, []uint16{8, 9, 10, 11, 12, 13, 14}) {
		t.Fatalf("full: %v", got)
	}
	// Too far ahead: skips everything.
	got = nil
	push(16, 100)
	if !equal(got, []uint16{16, 100}) {
		t.Fatalf("far: %v", got)
	}
	// A sender that restarts far behind is followed.
	got = nil
	push(10, 11)
	if !equal(got, []uint16{10, 11}) {
		t.Fatalf("restart: %v", got)
	}
	// The oldest held is tracked across drains.
	got = nil
	r.Push(now, rtpPacket(96, 14, 0, false, nil), emit)
	r.Push(now.Add(time.Millisecond), rtpPacket(96, 16, 0, false, nil), emit)
	r.Push(now, rtpPacket(96, 13, 0, false, nil), emit)
	push(12)
	if at, _ := r.NextTimeout(); !at.Equal(now.Add(time.Millisecond + DefaultReorderDelay)) {
		t.Fatalf("oldest %v", at)
	}
	// A packet that will not marshal is not held.
	bad := rtpPacket(200, 20, 0, false, nil)
	r.Push(now, bad, emit)
}

func equal(a, b []uint16) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestTimestampWrap(t *testing.T) {
	f := &Format{PayloadType: 0, ClockRate: 8000, Track: &av.Track{Codec: av.CodecPCMU}}
	d := mustDepacketizer(t, f)
	d.SetOrigin(0xffffff00, time.Second)
	out, _ := feed(d, rtpPacket(0, 1, 0xffffff00, true, []byte{1}), rtpPacket(0, 2, 0x100, true, []byte{1}), rtpPacket(0, 3, 0xffffff80, true, []byte{1}))
	if len(out) != 3 || out[0].PTS != time.Second || out[1].PTS != time.Second+av.FromTicks(0x200, 8000) || out[2].PTS != time.Second+av.FromTicks(0x80, 8000) {
		t.Fatalf("%v %v %v", out[0].PTS, out[1].PTS, out[2].PTS)
	}
	if _, err := NewDepacketizer(&Format{Track: &av.Track{Codec: av.CodecAV1}}, DepacketizerConfig{}); err != ErrUnsupported {
		t.Fatal(err)
	}
}

func TestDTSExtractor(t *testing.T) {
	var e dtsExtractor
	e.reorder, e.frame = 0, 10
	for i, pts := range []time.Duration{5, 10, 10, 30} {
		if dts := e.next(pts); dts != pts {
			t.Fatalf("%d: %v", i, dts)
		}
	}
	// A reorder deeper than the stream's never goes past a picture.
	e = dtsExtractor{reorder: 40, frame: 10}
	for _, pts := range []time.Duration{100, 140, 120, 110, 130} {
		if dts := e.next(pts); dts > pts {
			t.Fatalf("dts %v pts %v", dts, pts)
		}
	}
	if n, f := (h264Codec{}).reorder([]byte{0x67}); n != -1 || f != 0 {
		t.Fatal(n, f)
	}
	if n, _ := (h265Codec{}).reorder([]byte{0x42}); n != -1 {
		t.Fatal(n)
	}
	sets := parameterSets(t, "../codec/h264/testdata/h264_base_360.ps")
	if n, _ := (h264Codec{}).reorder(sets[0]); n != 0 {
		t.Fatal(n)
	}
}

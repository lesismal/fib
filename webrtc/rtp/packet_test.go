package rtp

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"
)

func unhex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.Join(strings.Fields(s), ""))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// A packet as Chrome sends video: one-byte extensions for abs-send-time
// (3), transport-wide sequence number (5) and MID (9).
const chromePacket = `90 e0 69 8f d9 c2 93 da 1c 64 27 82
	be de 00 03 32 0f 3d 77 51 01 23 90 31 00 00 00
	98 36 be 88 9e`

func TestParseChromePacket(t *testing.T) {
	b := unhex(t, chromePacket)
	var p Packet
	if err := p.Unmarshal(b); err != nil {
		t.Fatal(err)
	}
	if !p.Marker || p.PayloadType != 96 || p.SequenceNumber != 0x698f || p.Timestamp != 0xd9c293da || p.SSRC != 0x1c642782 {
		t.Fatalf("header %+v", p.Header)
	}
	if p.ExtensionProfile != ExtensionProfileOneByte || len(p.ExtensionData) != 12 {
		t.Fatalf("extension %x %x", p.ExtensionProfile, p.ExtensionData)
	}
	if v, err := ParseAbsSendTime(p.GetExtension(3)); err != nil || v != 0x0f3d77 {
		t.Errorf("abs-send-time %x %v", v, err)
	}
	if v, err := ParseTransportCCSeq(p.GetExtension(5)); err != nil || v != 0x0123 {
		t.Errorf("transport-cc %x %v", v, err)
	}
	if mid := p.GetExtension(9); string(mid) != "1" {
		t.Errorf("mid %q", mid)
	}
	if p.GetExtension(4) != nil {
		t.Error("extension 4 found")
	}
	if !bytes.Equal(p.Payload, []byte{0x98, 0x36, 0xbe, 0x88, 0x9e}) {
		t.Errorf("payload %x", p.Payload)
	}
	var ids []uint8
	for id := range p.Extensions() {
		ids = append(ids, id)
	}
	if !bytes.Equal(ids, []byte{3, 5, 9}) {
		t.Errorf("ids %v", ids)
	}
	out, err := p.Append(nil)
	if err != nil || !bytes.Equal(out, b) {
		t.Fatalf("marshal %v\n got %x\nwant %x", err, out, b)
	}
	if n, err := HeaderLen(b); err != nil || n != 28 {
		t.Errorf("HeaderLen %d %v", n, err)
	}
}

func TestPacketRoundTrip(t *testing.T) {
	p := Packet{
		Header: Header{
			Marker: true, PayloadType: 111, SequenceNumber: 65535, Timestamp: 1, SSRC: 2,
			CSRC: []uint32{3, 4, 5},
		},
		Payload:     []byte("payload"),
		PaddingSize: 5,
	}
	if err := p.SetExtension(1, []byte("mid")); err != nil {
		t.Fatal(err)
	}
	if err := p.SetExtension(10, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	b, err := p.Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != p.MarshalSize() {
		t.Fatalf("size %d, MarshalSize %d", len(b), p.MarshalSize())
	}
	var q Packet
	if err := q.Unmarshal(b); err != nil {
		t.Fatal(err)
	}
	if q.PaddingSize != 5 || !q.Padding || string(q.Payload) != "payload" || len(q.CSRC) != 3 || q.CSRC[2] != 5 {
		t.Fatalf("%+v", q)
	}
	if string(q.GetExtension(1)) != "mid" || string(q.GetExtension(10)) != "hi" {
		t.Fatal("extensions lost")
	}
	if q.ExtensionProfile != ExtensionProfileOneByte {
		t.Fatalf("profile %x", q.ExtensionProfile)
	}
}

func TestExtensionForms(t *testing.T) {
	var h Header
	h.SetExtension(1, []byte{1})
	h.SetExtension(2, []byte{2, 2})
	if h.ExtensionProfile != ExtensionProfileOneByte {
		t.Fatal("not one-byte")
	}
	// An id over 14 moves the block to the two-byte form and keeps the rest.
	h.SetExtension(200, []byte("long"))
	if h.ExtensionProfile != ExtensionProfileTwoByte {
		t.Fatalf("profile %x", h.ExtensionProfile)
	}
	h.SetExtension(3, nil)
	h.SetExtension(4, bytes.Repeat([]byte{'x'}, 200))
	h.SetExtension(1, []byte{9, 9, 9})
	want := map[uint8]string{2: "\x02\x02", 200: "long", 3: "", 4: strings.Repeat("x", 200), 1: "\x09\x09\x09"}
	got := map[uint8]string{}
	for id, p := range h.Extensions() {
		got[id] = string(p)
	}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for id, v := range want {
		if got[id] != v {
			t.Fatalf("id %d: %q", id, got[id])
		}
	}
	b, err := h.Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	var h2 Header
	if _, err := h2.Unmarshal(b); err != nil {
		t.Fatal(err)
	}
	if string(h2.GetExtension(4)) != strings.Repeat("x", 200) {
		t.Fatal("two-byte element lost")
	}
	for id := range want {
		h.DelExtension(id)
	}
	if h.Extension || h.MarshalSize() != HeaderSize {
		t.Fatalf("extensions left: %v %x", h.Extension, h.ExtensionData)
	}
	if err := h.SetExtension(0, []byte{1}); !errors.Is(err, ErrExtensionID) {
		t.Fatal(err)
	}
	h.Extension, h.ExtensionProfile, h.ExtensionData = true, 0x1234, []byte{1, 2, 3, 4}
	if err := h.SetExtension(1, []byte{1}); !errors.Is(err, ErrExtensionProfile) {
		t.Fatal(err)
	}
	if h.GetExtension(1) != nil {
		t.Fatal("found an element in a foreign profile")
	}
}

// TestSetExtensionNoAlloc checks that a sender setting the transport-wide
// sequence number on each packet does not allocate once warmed.
func TestSetExtensionNoAlloc(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates")
	}
	var p Packet
	p.Payload = make([]byte, 1000)
	buf := make([]byte, 0, 1500)
	p.SetExtension(9, []byte("0"))
	var seq uint16
	var payload [2]byte
	allocs := testing.AllocsPerRun(100, func() {
		seq++
		p.SetExtension(5, AppendTransportCCSeq(payload[:0], seq))
		p.SetExtension(3, AppendAbsSendTime(payload[:0], uint32(seq)))
		buf, _ = p.Append(buf[:0])
	})
	if allocs != 0 {
		t.Fatalf("%v allocations", allocs)
	}
}

func TestParseNoAlloc(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates")
	}
	b := unhex(t, chromePacket)
	var p Packet
	allocs := testing.AllocsPerRun(100, func() {
		if err := p.Unmarshal(b); err != nil {
			t.Fatal(err)
		}
		_ = p.GetExtension(5)
	})
	if allocs != 0 {
		t.Fatalf("%v allocations", allocs)
	}
}

func TestMalformedPackets(t *testing.T) {
	good := unhex(t, chromePacket)
	for _, tc := range []struct {
		name string
		b    []byte
		err  error
	}{
		{"empty", nil, ErrShortPacket},
		{"short", good[:11], ErrShortPacket},
		{"version", append([]byte{0x40}, good[1:]...), ErrVersion},
		{"csrc beyond end", append([]byte{0x8f}, good[1:12]...), ErrShortPacket},
		{"extension beyond end", good[:20], ErrShortPacket},
		{"extension header cut", good[:14], ErrShortPacket},
		{"zero padding", append(append([]byte{0xa0}, good[1:12]...), 0), ErrPadding},
		{"padding past payload", append(append([]byte{0xa0}, good[1:12]...), 1, 9), ErrPadding},
		{"padding with no payload", append([]byte{0xa0}, good[1:12]...), ErrPadding},
	} {
		var p Packet
		if err := p.Unmarshal(tc.b); !errors.Is(err, tc.err) {
			t.Errorf("%s: %v, want %v", tc.name, err, tc.err)
		}
	}
	// A truncated element is not found, and stops the walk.
	h := Header{Extension: true, ExtensionProfile: ExtensionProfileOneByte, ExtensionData: []byte{0x1f, 1, 2, 3}}
	if h.GetExtension(1) != nil {
		t.Error("truncated element found")
	}
	for range h.Extensions() {
		t.Error("truncated element yielded")
	}
	if _, err := (&Header{CSRC: make([]uint32, 16)}).Append(nil); !errors.Is(err, ErrTooManyCSRC) {
		t.Error(err)
	}
	if _, err := (&Header{PayloadType: 128}).Append(nil); !errors.Is(err, ErrPayloadType) {
		t.Error(err)
	}
	if _, err := (&Header{Extension: true, ExtensionData: []byte{1}}).Append(nil); !errors.Is(err, ErrExtensionLength) {
		t.Error(err)
	}
}

func TestSeqUnwrapper(t *testing.T) {
	var u SeqUnwrapper
	for _, tc := range []struct {
		seq  uint16
		want int64
	}{{65534, 65534}, {65535, 65535}, {0, 65536}, {65533, 65533}, {2, 65538}, {30000, 95536}, {60000, 125536}, {10, 131082}, {65000, 130536}} {
		if got := u.Unwrap(tc.seq); got != tc.want {
			t.Fatalf("Unwrap(%d) = %d, want %d", tc.seq, got, tc.want)
		}
	}
	u.Reset()
	if got := u.Unwrap(7); got != 7 {
		t.Fatal(got)
	}
}

func TestExtensionValues(t *testing.T) {
	ts := time.Unix(1700000000, 500_000_000)
	v := AbsSendTime(ts)
	// 6.18 fixed point: the low 6 bits of the seconds and 18 of fraction.
	wantSec := uint32((1700000000 + ntpEpochOffset) % 64)
	if v>>18 != wantSec || v&0x3ffff != 1<<17 {
		t.Fatalf("abs-send-time %x", v)
	}
	b := AppendAudioLevel(nil, true, 30)
	if voice, level, err := ParseAudioLevel(b); err != nil || !voice || level != 30 {
		t.Fatal(voice, level, err)
	}
	if _, err := ParseAbsSendTime([]byte{1}); err == nil {
		t.Fatal("short abs-send-time accepted")
	}
	if got := TimeFromNTP(NTPTime(ts)); got.Sub(ts).Abs() > time.Microsecond {
		t.Fatalf("NTP round trip %v", got)
	}
}

func BenchmarkPacketUnmarshal(b *testing.B) {
	pkt := unhex(b, chromePacket)
	pkt = append(pkt, make([]byte, 1200-len(pkt))...)
	var p Packet
	b.SetBytes(int64(len(pkt)))
	b.ReportAllocs()
	for b.Loop() {
		if err := p.Unmarshal(pkt); err != nil {
			b.Fatal(err)
		}
		_ = p.GetExtension(5)
	}
}

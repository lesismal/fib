package av1

import (
	"bytes"
	"os"
	"testing"

	"github.com/lesismal/fib/media/internal/bitio"
)

// testdata/av1.obu is what ffmpeg 7.1 wrote with "-c:v libaom-av1
// -cpu-used 8 -g 10 -f obu": 1.2 s of 160x120 testsrc at 25 fps, profile
// 0, 4:2:0, with hidden frames shown later by frame headers.
func readFixture(t testing.TB) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/av1.obu")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// temporalUnits splits a low-overhead bitstream at its temporal
// delimiters and returns the units without them.
func temporalUnits(b []byte) [][]byte {
	var tus [][]byte
	var cur []byte
	for o := range SplitOBUs(b) {
		if o.Type() == OBUTemporalDelimiter {
			if cur != nil {
				tus = append(tus, cur)
			}
			cur = []byte{}
			continue
		}
		cur = AppendOBU(cur, o, true)
	}
	return append(tus, cur)
}

func TestFixture(t *testing.T) {
	b := readFixture(t)
	if !ValidOBUs(b) {
		t.Fatal("fixture not valid")
	}
	tus := temporalUnits(b)
	if len(tus) != 30 {
		t.Fatalf("%d temporal units", len(tus))
	}
	keys := 0
	for i, tu := range tus {
		key := IsKeyframe(tu)
		if key != (i%10 == 0) {
			t.Fatalf("TU %d keyframe %v", i, key)
		}
		if key {
			keys++
			o, _, err := NextOBU(tu)
			if err != nil || o.Type() != OBUSequenceHeader {
				t.Fatalf("TU %d starts with %v", i, o.Type())
			}
			var s SequenceHeader
			if err := s.Unmarshal(tu); err != nil {
				t.Fatal(err)
			}
			if s.Width != 160 || s.Height != 120 || s.Profile != 0 || s.BitDepth != 8 || !s.SubsamplingX || !s.SubsamplingY {
				t.Fatalf("%+v", s)
			}
			c, err := NewConfig(tu)
			if err != nil {
				t.Fatal(err)
			}
			rec := c.Append(nil)
			// What ffmpeg's MP4 muxer writes as av1C for it.
			if !bytes.Equal(rec[:4], []byte{0x81, 0x00, 0x0c, 0x00}) || c.CodecString() != "av01.0.00M.08" {
				t.Fatalf("%x %s", rec, c.CodecString())
			}
			var back Config
			if err := back.Unmarshal(rec); err != nil || !bytes.Equal(back.Append(nil), rec) {
				t.Fatal("record does not round trip", err)
			}
			if !bytes.Equal(back.SequenceHeader(), AppendOBU(nil, o, true)) {
				t.Fatal("sequence header")
			}
		}
	}
	if keys != 3 {
		t.Fatal(keys)
	}
}

// TestFLVConfig reads the AV1CodecConfigurationRecord ffmpeg wrote into
// media/flv's fixture, profile 1 (4:4:4).
func TestFLVConfig(t *testing.T) {
	rec := []byte{0x81, 0x20, 0x00, 0x00, 0x0a, 0x0d, 0x20, 0x00, 0x00, 0x03, 0xb4, 0xff, 0x73, 0x6b, 0xe4, 0x80, 0x86, 0x80, 0x10}
	var c Config
	if err := c.Unmarshal(rec); err != nil {
		t.Fatal(err)
	}
	var s SequenceHeader
	if err := s.Unmarshal(c.SequenceHeader()); err != nil {
		t.Fatal(err)
	}
	if s.Profile != 1 || s.Width != 160 || s.Height != 120 || s.SubsamplingX || s.SubsamplingY {
		t.Fatalf("%+v", s)
	}
	n, err := NewConfig(c.SequenceHeader())
	if err != nil || !bytes.Equal(n.Append(nil), rec) {
		t.Fatalf("%x %v", n.Append(nil), err)
	}
}

// seqSpec builds a sequence header OBU of the fields it names.
type seqSpec struct {
	profile                         uint8
	reduced, timing, equalInterval  bool
	decoderModel, displayDelay      bool
	levels                          []uint8
	frameIDs, orderHint             bool
	chooseScreen, forceScreen       bool
	chooseIntegerMV                 bool
	highBitdepth, twelveBit, mono   bool
	colorDesc                       bool
	primaries, transfer, matrix     uint8
	subX, subY                      bool
	chromaPos                       uint8
	width, height                   int
	extension, withoutSize, cutBits bool
}

func (p seqSpec) build() []byte {
	w := bitio.NewWriter(nil)
	w.Bits(uint64(p.profile), 3)
	w.Flag(false)
	w.Flag(p.reduced)
	if p.reduced {
		w.Bits(uint64(p.levels[0]), 5)
	} else {
		w.Flag(p.timing)
		if p.timing {
			w.Bits(1001, 32)
			w.Bits(30000, 32)
			w.Flag(p.equalInterval)
			if p.equalInterval {
				w.Bits(0b00101, 5) // uvlc 4
			}
			w.Flag(p.decoderModel)
			if p.decoderModel {
				w.Bits(9, 5)
				w.Bits(1, 32)
				w.Bits(1, 5)
				w.Bits(1, 5)
			}
		}
		w.Flag(p.displayDelay)
		w.Bits(uint64(len(p.levels)-1), 5)
		for _, l := range p.levels {
			w.Bits(0, 12)
			w.Bits(uint64(l), 5)
			if l > 7 {
				w.Flag(true)
			}
			if p.decoderModel {
				w.Flag(true)
				w.Bits(0, 2*10+1)
			}
			if p.displayDelay {
				w.Flag(true)
				w.Bits(9, 4)
			}
		}
	}
	w.Bits(15, 4)
	w.Bits(15, 4)
	w.Bits(uint64(p.width-1), 16)
	w.Bits(uint64(p.height-1), 16)
	if !p.reduced {
		w.Flag(p.frameIDs)
		if p.frameIDs {
			w.Bits(0, 7)
		}
	}
	w.Bits(0, 3)
	if !p.reduced {
		w.Bits(0, 4)
		w.Flag(p.orderHint)
		if p.orderHint {
			w.Bits(0, 2)
		}
		w.Flag(p.chooseScreen)
		if !p.chooseScreen {
			w.Flag(p.forceScreen)
		}
		if p.chooseScreen || p.forceScreen {
			w.Flag(p.chooseIntegerMV)
			if !p.chooseIntegerMV {
				w.Flag(false)
			}
		}
		if p.orderHint {
			w.Bits(6, 3)
		}
	}
	w.Bits(0, 3)
	w.Flag(p.highBitdepth)
	if p.profile == 2 && p.highBitdepth {
		w.Flag(p.twelveBit)
	}
	if p.profile != 1 {
		w.Flag(p.mono)
	}
	w.Flag(p.colorDesc)
	if p.colorDesc {
		w.Bits(uint64(p.primaries), 8)
		w.Bits(uint64(p.transfer), 8)
		w.Bits(uint64(p.matrix), 8)
	}
	switch {
	case p.mono:
		w.Flag(true)
	case p.colorDesc && p.primaries == 1 && p.transfer == 13 && p.matrix == 0:
		w.Flag(false)
	default:
		w.Flag(true)
		if p.profile == 2 && p.twelveBit {
			w.Flag(p.subX)
			if p.subX {
				w.Flag(p.subY)
			}
		}
		if p.subX && p.subY {
			w.Bits(uint64(p.chromaPos), 2)
		}
		w.Flag(false)
	}
	if !p.cutBits {
		w.Flag(false)
		w.Bits(1, 1) // trailing bit
	}
	payload := w.Bytes()
	o := OBU{Header: []byte{byte(OBUSequenceHeader) << 3}, Payload: payload}
	if p.extension {
		o.Header = []byte{o.Header[0] | extensionFlag, 0x20}
	}
	return AppendOBU(nil, o, !p.withoutSize)
}

func TestSequenceHeader(t *testing.T) {
	cases := []struct {
		spec seqSpec
		want SequenceHeader
	}{
		{seqSpec{profile: 0, levels: []uint8{8, 4}, timing: true, equalInterval: true, decoderModel: true, displayDelay: true,
			frameIDs: true, orderHint: true, colorDesc: true, primaries: 1, transfer: 1, matrix: 1, subX: true, subY: true, chromaPos: 1,
			width: 1920, height: 1080, extension: true},
			SequenceHeader{Level: 8, Tier: 1, InitialDisplayDelay: 10, NumUnitsInDisplayTick: 1001, TimeScale: 30000,
				Width: 1920, Height: 1080, BitDepth: 8, ColorPrimaries: 1, TransferCharacteristics: 1, MatrixCoefficients: 1,
				ColorRange: true, SubsamplingX: true, SubsamplingY: true, ChromaSamplePosition: 1}},
		{seqSpec{profile: 0, reduced: true, levels: []uint8{5}, mono: true, highBitdepth: true, width: 64, height: 64, withoutSize: true},
			SequenceHeader{ReducedStillPictureHeader: true, Level: 5, Width: 64, Height: 64, BitDepth: 10, Monochrome: true,
				ColorPrimaries: 2, TransferCharacteristics: 2, MatrixCoefficients: 2, ColorRange: true, SubsamplingX: true, SubsamplingY: true}},
		{seqSpec{profile: 1, levels: []uint8{12}, chooseScreen: true, colorDesc: true, primaries: 1, transfer: 13, matrix: 0, width: 640, height: 480},
			SequenceHeader{Profile: 1, Level: 12, Tier: 1, Width: 640, Height: 480, BitDepth: 8, ColorPrimaries: 1, TransferCharacteristics: 13,
				ColorRange: true}},
		{seqSpec{profile: 2, levels: []uint8{9}, forceScreen: true, chooseIntegerMV: true, highBitdepth: true, twelveBit: true, subX: true, subY: true, chromaPos: 2, width: 320, height: 240},
			SequenceHeader{Profile: 2, Level: 9, Tier: 1, Width: 320, Height: 240, BitDepth: 12, ColorPrimaries: 2, TransferCharacteristics: 2,
				MatrixCoefficients: 2, ColorRange: true, SubsamplingX: true, SubsamplingY: true, ChromaSamplePosition: 2}},
		{seqSpec{profile: 2, levels: []uint8{9}, highBitdepth: true, twelveBit: true, width: 320, height: 240},
			SequenceHeader{Profile: 2, Level: 9, Tier: 1, Width: 320, Height: 240, BitDepth: 12, ColorPrimaries: 2, TransferCharacteristics: 2,
				MatrixCoefficients: 2, ColorRange: true}},
		{seqSpec{profile: 2, levels: []uint8{9}, highBitdepth: true, width: 320, height: 240},
			SequenceHeader{Profile: 2, Level: 9, Tier: 1, Width: 320, Height: 240, BitDepth: 10, ColorPrimaries: 2, TransferCharacteristics: 2,
				MatrixCoefficients: 2, ColorRange: true, SubsamplingX: true}},
	}
	for i, c := range cases {
		var s SequenceHeader
		if err := s.Unmarshal(c.spec.build()); err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		if s != c.want {
			t.Fatalf("%d: %+v\nwant %+v", i, s, c.want)
		}
		cfg, err := NewConfig(c.spec.build())
		if err != nil {
			t.Fatal(err)
		}
		var back Config
		if err := back.Unmarshal(cfg.Append(nil)); err != nil || back.Profile != s.Profile || back.Level != s.Level ||
			back.TwelveBit != (s.BitDepth == 12) || back.InitialPresentationDelay != s.InitialDisplayDelay {
			t.Fatalf("%d: %+v %v", i, back, err)
		}
		if i == 0 && cfg.CodecString() != "av01.0.08H.08" {
			t.Fatal(cfg.CodecString())
		}
		if i == 3 && cfg.CodecString() != "av01.2.09H.12" {
			t.Fatal(cfg.CodecString())
		}
		if i == 5 && cfg.CodecString() != "av01.2.09H.10" {
			t.Fatal(cfg.CodecString())
		}
	}
	// A reduced still picture is a key frame whatever its frame header.
	still := seqSpec{reduced: true, levels: []uint8{1}, width: 8, height: 8}.build()
	frame := AppendOBU(nil, OBU{Header: []byte{byte(OBUFrame) << 3}, Payload: []byte{0xff}}, true)
	if !IsKeyframe(append(still, frame...)) || IsKeyframe(frame) {
		t.Fatal("reduced still picture")
	}

	var s SequenceHeader
	if s.Unmarshal(nil) != ErrSequenceHeader || s.Unmarshal(frame) != ErrNotSequenceHeader {
		t.Fatal("not a sequence header taken")
	}
	if s.Unmarshal(seqSpec{levels: []uint8{1}, width: 8, height: 8, cutBits: true}.build()[:6]) != ErrSequenceHeader {
		t.Fatal("short sequence header taken")
	}
	if _, err := NewConfig(nil); err != ErrSequenceHeader {
		t.Fatal(err)
	}
	if _, err := NewConfig(frame); err != ErrNotSequenceHeader {
		t.Fatal(err)
	}
	if _, err := NewConfig(seqSpec{levels: []uint8{1}, width: 8, height: 8}.build()[:5]); err == nil {
		t.Fatal("short taken")
	}
	short := AppendOBU(nil, OBU{Header: []byte{byte(OBUSequenceHeader) << 3}, Payload: []byte{0}}, true)
	if _, err := NewConfig(short); err != ErrSequenceHeader {
		t.Fatal(err)
	}
}

func TestUVLC(t *testing.T) {
	for _, c := range []struct {
		b    []byte
		want uint32
	}{{[]byte{0x80}, 0}, {[]byte{0x28}, 4}, {[]byte{0, 0, 0, 0, 0x80}, 1<<32 - 1}} {
		r := bitio.NewReader(c.b)
		if v := readUVLC(&r); v != c.want {
			t.Fatalf("%x: %d", c.b, v)
		}
	}
}

func TestConfigErrors(t *testing.T) {
	var c Config
	if c.Unmarshal([]byte{0x81, 0, 0}) != ErrShortConfig {
		t.Fatal("short")
	}
	if c.Unmarshal([]byte{0x01, 0, 0, 0}) != ErrConfigVersion {
		t.Fatal("version")
	}
	if c.Unmarshal([]byte{0x81, 0, 0, 0, 0x0a, 5}) != ErrConfigOBUs {
		t.Fatal("OBUs")
	}
	// A record without configOBUs, and with an initial presentation delay
	// and a metadata OBU before the sequence header.
	if c.Unmarshal([]byte{0x81, 0, 0, 0}) != nil || c.SequenceHeader() != nil {
		t.Fatal("empty")
	}
	seq := seqSpec{levels: []uint8{1}, width: 8, height: 8}.build()
	meta := AppendOBU(nil, OBU{Header: []byte{byte(OBUMetadata) << 3}, Payload: []byte{1, 2}}, true)
	rec := append(append([]byte{0x81, 0x41, 0xff, 0x13}, meta...), seq...)
	if c.Unmarshal(rec) != nil || !bytes.Equal(c.SequenceHeader(), seq) || c.InitialPresentationDelay != 4 || !bytes.Equal(c.Append(nil), rec) {
		t.Fatalf("%+v", c)
	}
	c.OBUs = []byte{0x0a, 9}
	if c.SequenceHeader() != nil {
		t.Fatal("malformed OBUs")
	}
	if c.Unmarshal(append([]byte{0x81, 0, 0, 0}, meta...)) != nil || c.SequenceHeader() != nil {
		t.Fatal("no sequence header")
	}
}

func TestOBU(t *testing.T) {
	if OBUFrame.String() != "FRAME" || OBUType(9).String() != "OBUType(9)" || OBUType(20).String() != "OBUType(20)" {
		t.Fatal("String")
	}
	if Type(nil) != 0 {
		t.Fatal("Type")
	}
	for _, b := range [][]byte{nil, {0x80}, {0x0c}, {0x0a, 0x80}, {0x0a, 3, 1}} {
		if _, _, err := NextOBU(b); err == nil {
			t.Fatalf("%x taken", b)
		}
		if ValidOBUs(b) && len(b) > 0 {
			t.Fatalf("%x valid", b)
		}
	}
	// Without a size field an OBU takes the rest.
	o, rest, err := NextOBU([]byte{0x34, 0x20, 1, 2, 3})
	if err != nil || rest != nil || o.Type() != OBUFrame || len(o.Header) != 2 || len(o.Payload) != 3 {
		t.Fatal(o, rest, err)
	}
	if b := AppendOBU(nil, o, true); !bytes.Equal(b, []byte{0x36, 0x20, 3, 1, 2, 3}) {
		t.Fatalf("%x", b)
	}
	n := 0
	for range SplitOBUs([]byte{0x12, 0, 0x12, 0, 0xff}) {
		n++
		break
	}
	if n != 1 {
		t.Fatal("break")
	}
	n = 0
	for range SplitOBUs([]byte{0x12, 0, 0x12, 0, 0xff}) {
		n++
	}
	if n != 2 {
		t.Fatal("stop at malformed", n)
	}
	for _, v := range []uint64{0, 1, 127, 128, 300, 1<<21 + 5, 1<<32 - 1} {
		b := AppendLEB128(nil, v)
		if len(b) != LEB128Size(v) {
			t.Fatal(v)
		}
		if got, n := ReadLEB128(append(b, 0xff)); got != v || n != len(b) {
			t.Fatal(v, got, n)
		}
	}
	for _, b := range [][]byte{nil, {0x80}, {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0}, {0x80, 0x80, 0x80, 0x80, 0x10}} {
		if _, n := ReadLEB128(b); n != 0 {
			t.Fatalf("%x read", b)
		}
	}
	// Keyframe detection: an intra-only frame, a frame header too short to
	// tell, and a temporal unit with no frame are not key frames.
	if IsKeyframe([]byte{0x32, 1, 0x40}) || IsKeyframe([]byte{0x1a, 0}) || IsKeyframe([]byte{0x12, 0}) {
		t.Fatal("not keyframes")
	}
	if !IsKeyframe([]byte{0x32, 1, 0x10}) || !IsKeyframe([]byte{0x1a, 1, 0}) {
		t.Fatal("keyframes")
	}
}

package h265

import (
	"bytes"
	"os"
	"testing"

	"github.com/lesismal/fib/media/codec/h264"
	"github.com/lesismal/fib/media/internal/bitio"
)

func readFile(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// fixtures are parameter sets libx265 wrote (ffmpeg 5.1), with the hvcC
// ffmpeg's MP4 muxer built from them and what ffprobe reports.
var fixtures = []struct {
	name           string
	width, height  int
	profile, level uint8
	chroma, depth  uint8
	codec          string
}{
	{"h265_main_720", 1280, 720, 1, 93, 1, 8, "hvc1.1.6.L93.90"},
	{"h265_main10_crop", 1918, 1078, 2, 120, 1, 10, "hvc1.2.4.L120.90"},
	{"h265_444", 320, 240, 4, 60, 3, 8, "hvc1.4.10.L60.9E.8"},
	{"h265_1080", 1920, 1080, 1, 120, 1, 8, "hvc1.1.6.L120.90"},
}

func parameterSets(t testing.TB, name string) (vps, sps, pps [][]byte) {
	for nalu := range h264.SplitAnnexB(readFile(t, name+".ps")) {
		switch Type(nalu) {
		case NALVPS:
			vps = append(vps, nalu)
		case NALSPS:
			sps = append(sps, nalu)
		case NALPPS:
			pps = append(pps, nalu)
		}
	}
	if len(vps) != 1 || len(sps) != 1 || len(pps) != 1 {
		t.Fatalf("%s: %d VPS %d SPS %d PPS", name, len(vps), len(sps), len(pps))
	}
	return
}

func TestFixtures(t *testing.T) {
	for _, f := range fixtures {
		vps, sps, pps := parameterSets(t, f.name)
		var s SPS
		if err := s.Unmarshal(sps[0]); err != nil {
			t.Fatalf("%s: %v", f.name, err)
		}
		if s.Width != f.width || s.Height != f.height {
			t.Errorf("%s: %dx%d, ffprobe says %dx%d", f.name, s.Width, s.Height, f.width, f.height)
		}
		if s.ProfileIDC != f.profile || s.LevelIDC != f.level || s.ChromaFormatIDC != f.chroma ||
			s.BitDepthLuma != f.depth || s.BitDepthChroma != f.depth {
			t.Errorf("%s: %+v", f.name, s)
		}
		var v VPS
		if err := v.Unmarshal(vps[0]); err != nil || v.ProfileTierLevel != s.ProfileTierLevel {
			t.Errorf("%s: VPS %v %+v", f.name, err, v)
		}

		want := readFile(t, f.name+".hvcc")
		var c HEVCConfig
		if err := c.Unmarshal(want); err != nil {
			t.Fatalf("%s: %v", f.name, err)
		}
		if c.ProfileTierLevel != s.ProfileTierLevel || c.ChromaFormat != f.chroma || c.BitDepthLuma != f.depth ||
			c.LengthSize != 4 || len(c.Arrays) != 3 {
			t.Errorf("%s: %+v", f.name, c)
		}
		if got := c.NALUs(NALSPS); len(got) != 1 || !bytes.Equal(got[0], sps[0]) {
			t.Errorf("%s: SPS differs", f.name)
		}
		if got := c.CodecString(); got != f.codec {
			t.Errorf("%s: codec %s want %s", f.name, got, f.codec)
		}
		// What ffmpeg's MP4 muxer writes is what NewHEVCConfig builds.
		built, err := NewHEVCConfig(vps, sps, pps)
		if err != nil {
			t.Fatal(err)
		}
		got, err := built.Append(nil)
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("%s: built %v\n got %x\nwant %x", f.name, err, got, want)
		}
		if built.MarshalSize() != len(want) {
			t.Errorf("%s: MarshalSize %d", f.name, built.MarshalSize())
		}
	}
}

func TestStream(t *testing.T) {
	stream := readFile(t, "stream.h265")
	if !IsKeyframeAnnexB(stream) {
		t.Fatal("no IRAP")
	}
	types := map[NALUType]int{}
	for n := range h264.SplitAnnexB(stream) {
		h, err := ParseHeader(n)
		if err != nil || h.LayerID != 0 || h.TemporalID != 0 || h.Type != Type(n) {
			t.Fatalf("%x: %v %+v", n[:2], err, h)
		}
		types[h.Type]++
	}
	if types[NALVPS] != 1 || types[NALSPS] != 1 || types[NALPPS] != 1 || types[NALIDRNLP] != 1 || types[NALSEIPrefix] == 0 {
		t.Fatalf("types %v", types)
	}
	avcc := h264.AnnexBToAVCC(nil, stream, func(n []byte) bool { return Type(n).IsVCL() })
	if !IsKeyframe(avcc) {
		t.Fatal("avcc keyframe")
	}
	if IsKeyframe(h264.AppendAVCC(nil, []byte{2, 1})) || IsKeyframeAnnexB([]byte{0, 0, 1, 2, 1}) {
		t.Fatal("TRAIL_R is a keyframe")
	}
}

func TestTypes(t *testing.T) {
	for typ := range NALUType(64) {
		irap := typ >= 16 && typ <= 23
		if typ.IsIRAP() != irap || typ.IsVCL() != (typ < 32) {
			t.Errorf("%d", typ)
		}
	}
	if NALCRA.String() != "CRA_NUT" || NALUType(22).String() != "NALUType(22)" || NALUType(63).String() != "NALUType(63)" {
		t.Fatal("names")
	}
	if Type(nil) != 0 {
		t.Fatal("empty")
	}
	if _, err := ParseHeader([]byte{0x40}); err != ErrShortHeader {
		t.Fatal(err)
	}
	if _, err := ParseHeader([]byte{0x40, 0}); err != ErrHeader {
		t.Fatal(err)
	}
	if h, err := ParseHeader([]byte{0x41, 0x0b}); err != nil || h.LayerID != 33 || h.TemporalID != 2 || h.Type != NALVPS {
		t.Fatal(h, err)
	}
}

func TestPrependParameterSets(t *testing.T) {
	vps, sps, pps := []byte{0x40, 1}, []byte{0x42, 1}, []byte{0x44, 1}
	idr := h264.AppendAVCC(nil, []byte{0x26, 1, 2})
	got := PrependParameterSets(nil, idr, vps, sps, pps)
	want := h264.AppendAVCC(h264.AppendAVCC(h264.AppendAVCC(nil, vps), sps), pps)
	want = append(want, idr...)
	if !bytes.Equal(got, want) {
		t.Fatalf("got %x", got)
	}
	if again := PrependParameterSets(nil, got, vps); !bytes.Equal(again, got) {
		t.Fatal("doubled")
	}
}

// spsSpec describes a synthetic SPS for the paths the fixtures miss.
type spsSpec struct {
	sub        int
	subProfile []bool
	chroma     uint32
	separate   bool
	w, h       uint32
	conf       []uint32
	depth      uint32
	id         uint32
}

func (p spsSpec) build(typ NALUType) []byte {
	w := bitio.NewWriter(nil)
	w.Bits(0, 4)
	if typ == NALVPS {
		w.Bits(0xff, 8)
	}
	w.Bits(uint64(p.sub), 3)
	w.Flag(true)
	if typ == NALVPS {
		w.Bits(0xffff, 16)
	}
	w.Bits(1<<5|1, 8) // tier 1, profile 1
	w.Bits(0x60000000, 32)
	w.Bits(0x900000000000, 48)
	w.Bits(150, 8)
	for i := range p.sub {
		w.Flag(p.subProfile[i])
		w.Flag(true)
	}
	if p.sub > 0 {
		w.Bits(0, 2*(8-p.sub))
	}
	for i := range p.sub {
		if p.subProfile[i] {
			w.Bits(0, 88)
		}
		w.Bits(120, 8)
	}
	if typ == NALVPS {
		w.Bits(1, 1)
		return h264.AddEmulationPrevention([]byte{byte(typ) << 1, 1}, w.Bytes())
	}
	w.UE(p.id)
	w.UE(p.chroma)
	if p.chroma == 3 {
		w.Flag(p.separate)
	}
	w.UE(p.w)
	w.UE(p.h)
	w.Flag(p.conf != nil)
	for _, c := range p.conf {
		w.UE(c)
	}
	w.UE(p.depth)
	w.UE(p.depth)
	w.Bits(1, 1)
	return h264.AddEmulationPrevention([]byte{byte(typ) << 1, 1}, w.Bytes())
}

func TestSyntheticSPS(t *testing.T) {
	for _, tc := range []struct {
		spec          spsSpec
		width, height int
	}{
		{spsSpec{sub: 2, subProfile: []bool{true, false}, chroma: 1, w: 64, h: 64, conf: []uint32{1, 2, 3, 4}}, 58, 50},
		{spsSpec{sub: 6, subProfile: make([]bool, 6), chroma: 2, w: 64, h: 64, conf: []uint32{1, 2, 3, 4}}, 58, 57},
		{spsSpec{chroma: 3, w: 64, h: 64, conf: []uint32{1, 2, 3, 4}}, 61, 57},
		{spsSpec{chroma: 3, separate: true, w: 64, h: 64, conf: []uint32{1, 2, 3, 4}}, 61, 57},
		{spsSpec{chroma: 0, w: 64, h: 64, conf: []uint32{1, 2, 3, 4}, depth: 4}, 61, 57},
	} {
		var s SPS
		if err := s.Unmarshal(tc.spec.build(NALSPS)); err != nil {
			t.Errorf("%+v: %v", tc.spec, err)
			continue
		}
		if s.Width != tc.width || s.Height != tc.height || s.Tier != 1 || s.MaxSubLayers != uint8(tc.spec.sub+1) {
			t.Errorf("%+v: %+v", tc.spec, s)
		}
		var v VPS
		if err := v.Unmarshal(tc.spec.build(NALVPS)); err != nil || v.LevelIDC != 150 || v.MaxSubLayers != s.MaxSubLayers {
			t.Errorf("VPS %+v: %v %+v", tc.spec, err, v)
		}
	}
	ok := spsSpec{chroma: 1, w: 64, h: 64}
	for _, mod := range []func(*spsSpec){
		func(p *spsSpec) { p.sub = 7; p.subProfile = make([]bool, 7) },
		func(p *spsSpec) { p.id = 16 },
		func(p *spsSpec) { p.chroma = 4 },
		func(p *spsSpec) { p.depth = 9 },
		func(p *spsSpec) { p.w = 0 },
		func(p *spsSpec) { p.h = 1<<20 + 1 },
		func(p *spsSpec) { p.conf = []uint32{16, 16, 0, 0} },
		func(p *spsSpec) { p.conf = []uint32{0, 0, 16, 16} },
	} {
		spec := ok
		mod(&spec)
		var s SPS
		if err := s.Unmarshal(spec.build(NALSPS)); err != ErrSPS {
			t.Errorf("%+v: %v", spec, err)
		}
	}
	var s SPS
	if err := s.Unmarshal([]byte{0x40, 1}); err != ErrNotSPS {
		t.Fatal(err)
	}
	var v VPS
	if err := v.Unmarshal([]byte{0x42, 1}); err != ErrNotVPS {
		t.Fatal(err)
	}
	if err := v.Unmarshal([]byte{0x40, 1, 0}); err != ErrVPS {
		t.Fatal(err)
	}
	bad := spsSpec{sub: 7, subProfile: make([]bool, 7)}.build(NALVPS)
	if err := v.Unmarshal(bad); err != ErrVPS {
		t.Fatal(err)
	}
}

func TestConfigErrors(t *testing.T) {
	vps, sps, pps := parameterSets(t, "h265_main_720")
	c, err := NewHEVCConfig(vps, sps, pps)
	if err != nil {
		t.Fatal(err)
	}
	c.LengthSize = 2
	c.Arrays = append(c.Arrays, NALArray{Type: NALSEIPrefix, NALUs: [][]byte{{0x4e, 1, 5}}})
	c.ProfileSpace = 1
	b, err := c.Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	var d HEVCConfig
	if err := d.Unmarshal(b); err != nil || d.LengthSize != 2 || len(d.Arrays) != 4 || d.Arrays[3].Complete ||
		d.CodecString() != "hvc1.A1.6.L93.90" {
		t.Fatalf("%v %+v", err, d)
	}
	for i := range b {
		if err := d.Unmarshal(b[:i]); err == nil {
			t.Fatalf("cut at %d parses", i)
		}
	}
	bad := bytes.Clone(b)
	bad[0] = 0
	if err := d.Unmarshal(bad); err != ErrConfigVersion {
		t.Fatal(err)
	}
	bad[0], bad[21] = 1, 0x0e
	if err := d.Unmarshal(bad); err != ErrLengthSize {
		t.Fatal(err)
	}
	for _, mod := range []func(*HEVCConfig) error{
		func(c *HEVCConfig) error { c.LengthSize = 3; return ErrLengthSize },
		func(c *HEVCConfig) error { c.Arrays = make([]NALArray, 256); return ErrTooMany },
		func(c *HEVCConfig) error { c.Arrays = []NALArray{{NALUs: make([][]byte, 65536)}}; return ErrTooMany },
		func(c *HEVCConfig) error {
			c.Arrays = []NALArray{{NALUs: [][]byte{make([]byte, 65536)}}}
			return ErrSetSize
		},
	} {
		e := *c
		want := mod(&e)
		if _, err := e.Append(nil); err != want {
			t.Errorf("%v, want %v", err, want)
		}
	}
	e := *c
	e.LengthSize = 0
	e.Tier = 1
	if b, err := e.Append(nil); err != nil || b[21]&3 != 3 || !bytesHasTier(b) || e.CodecString()[:12] != "hvc1.A1.6.H9" {
		t.Fatal(err, e.CodecString())
	}
	if _, err := NewHEVCConfig(vps, nil, pps); err != ErrNoSPS {
		t.Fatal(err)
	}
	if _, err := NewHEVCConfig(vps, vps, pps); err != ErrNotSPS {
		t.Fatal(err)
	}
	if _, err := NewHEVCConfig(sps, sps, pps); err != ErrNotVPS {
		t.Fatal(err)
	}
	if c, err := NewHEVCConfig(nil, sps, nil); err != nil || len(c.Arrays) != 1 {
		t.Fatal(err)
	}
	c.ConstraintFlags = 0
	if s := c.CodecString(); s != "hvc1.A1.6.L93" {
		t.Fatal(s)
	}
}

func bytesHasTier(b []byte) bool { return b[1]&0x20 != 0 }

package h264

import (
	"bytes"
	"testing"

	"github.com/lesismal/fib/media/internal/bitio"
)

// spsSpec describes a synthetic SPS, for the syntax paths the fixtures do
// not take.
type spsSpec struct {
	profile             uint8
	id                  uint32
	chroma              uint32
	separate            bool
	depth               uint32 // bit_depth_*_minus8
	scaling             []int  // per list: 0 absent, 1 explicit, 2 use default
	log2MaxFrameNum     uint32
	poc                 uint32
	pocCycle            uint32
	wMbs, hMapUnits     uint32
	frameMbsOnly        bool
	crop                []uint32
	vui                 bool
	sarIdc              uint32
	sarW, sarH          uint32
	colour              bool
	chromaLoc           bool
	timing              bool
	nalHRD, vclHRD      bool
	cpbCnt              uint32
	restriction         bool
	reorder             uint32
	overscan, sigNoDesc bool
}

func (p spsSpec) build() []byte {
	w := bitio.NewWriter(nil)
	w.Bits(uint64(p.profile), 8)
	w.Bits(0, 8)
	w.Bits(30, 8)
	w.UE(p.id)
	if hasChromaInfo(p.profile) {
		w.UE(p.chroma)
		if p.chroma == 3 {
			w.Flag(p.separate)
		}
		w.UE(p.depth)
		w.UE(p.depth)
		w.Flag(false)
		w.Flag(p.scaling != nil)
		for i, s := range p.scaling {
			w.Flag(s != 0)
			size := 16
			if i >= 6 {
				size = 64
			}
			switch s {
			case 1:
				for i := range size {
					w.SE(int32(i%5) - 2)
				}
			case 2:
				w.SE(-8) // next scale 0: use the default list
			}
		}
	}
	w.UE(p.log2MaxFrameNum)
	w.UE(p.poc)
	switch p.poc {
	case 0:
		w.UE(2)
	case 1:
		w.Flag(false)
		w.SE(-3)
		w.SE(4)
		w.UE(p.pocCycle)
		for i := range p.pocCycle {
			w.SE(int32(i))
		}
	}
	w.UE(4)
	w.Flag(false)
	w.UE(p.wMbs - 1)
	w.UE(p.hMapUnits - 1)
	w.Flag(p.frameMbsOnly)
	if !p.frameMbsOnly {
		w.Flag(true)
	}
	w.Flag(true)
	w.Flag(p.crop != nil)
	for _, c := range p.crop {
		w.UE(c)
	}
	w.Flag(p.vui)
	if p.vui {
		w.Flag(p.sarIdc != 0)
		if p.sarIdc != 0 {
			w.Bits(uint64(p.sarIdc), 8)
			if p.sarIdc == 255 {
				w.Bits(uint64(p.sarW), 16)
				w.Bits(uint64(p.sarH), 16)
			}
		}
		w.Flag(p.overscan)
		if p.overscan {
			w.Flag(true)
		}
		w.Flag(p.colour || p.sigNoDesc)
		if p.colour || p.sigNoDesc {
			w.Bits(5, 3)
			w.Flag(true)
			w.Flag(p.colour)
			if p.colour {
				w.Bits(9, 8)
				w.Bits(16, 8)
				w.Bits(9, 8)
			}
		}
		w.Flag(p.chromaLoc)
		if p.chromaLoc {
			w.UE(1)
			w.UE(1)
		}
		w.Flag(p.timing)
		if p.timing {
			w.Bits(1001, 32)
			w.Bits(60000, 32)
			w.Flag(true)
		}
		for _, hrd := range []bool{p.nalHRD, p.vclHRD} {
			w.Flag(hrd)
			if hrd {
				w.UE(p.cpbCnt)
				w.Bits(0, 8)
				for range p.cpbCnt + 1 {
					w.UE(1000)
					w.UE(2000)
					w.Flag(false)
				}
				w.Bits(0, 20)
			}
		}
		if p.nalHRD || p.vclHRD {
			w.Flag(false)
		}
		w.Flag(false)
		w.Flag(p.restriction)
		if p.restriction {
			w.Flag(true)
			w.UE(2)
			w.UE(1)
			w.UE(16)
			w.UE(16)
			w.UE(p.reorder)
			w.UE(4)
		}
	}
	w.Bits(1, 1) // rbsp_stop_one_bit
	return AddEmulationPrevention([]byte{0x67}, w.Bytes())
}

func TestSyntheticSPS(t *testing.T) {
	scaling8 := []int{1, 0, 2, 0, 0, 1, 1, 2}
	scaling12 := []int{1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 2}
	for _, tc := range []struct {
		name          string
		spec          spsSpec
		width, height int
		check         func(*SPS) bool
	}{
		{"baseline poc2", spsSpec{profile: 66, poc: 2, wMbs: 20, hMapUnits: 15, frameMbsOnly: true}, 320, 240, nil},
		{"poc1", spsSpec{profile: 77, poc: 1, pocCycle: 3, wMbs: 20, hMapUnits: 15, frameMbsOnly: true}, 320, 240,
			func(s *SPS) bool { return s.PicOrderCntType == 1 }},
		{"high scaling", spsSpec{profile: 100, chroma: 1, scaling: scaling8, wMbs: 120, hMapUnits: 68, frameMbsOnly: true,
			crop: []uint32{0, 0, 0, 4}}, 1920, 1080, nil},
		{"444 scaling", spsSpec{profile: 244, chroma: 3, scaling: scaling12, depth: 2, wMbs: 4, hMapUnits: 4, frameMbsOnly: true,
			crop: []uint32{1, 2, 3, 0}}, 61, 61, func(s *SPS) bool { return s.BitDepthLuma == 10 }},
		{"separate planes", spsSpec{profile: 244, chroma: 3, separate: true, wMbs: 4, hMapUnits: 2, frameMbsOnly: false,
			crop: []uint32{1, 0, 1, 0}}, 63, 62, func(s *SPS) bool { return s.SeparateColourPlane }},
		{"monochrome", spsSpec{profile: 100, chroma: 0, wMbs: 4, hMapUnits: 4, frameMbsOnly: true,
			crop: []uint32{0, 1, 0, 1}}, 63, 63, nil},
		{"422 field", spsSpec{profile: 122, chroma: 2, wMbs: 45, hMapUnits: 18, frameMbsOnly: false,
			crop: []uint32{1, 1, 1, 1}}, 716, 572, nil},
		{"vui full", spsSpec{profile: 100, chroma: 1, wMbs: 20, hMapUnits: 15, frameMbsOnly: true, vui: true,
			sarIdc: 255, sarW: 4, sarH: 3, overscan: true, colour: true, chromaLoc: true, timing: true,
			nalHRD: true, vclHRD: true, cpbCnt: 2, restriction: true, reorder: 2}, 320, 240,
			func(s *SPS) bool {
				return s.SarWidth == 4 && s.SarHeight == 3 && s.ColourPrimaries == 9 && s.TransferCharacteristics == 16 &&
					s.VideoFullRange && s.FrameRate() > 29.97 && s.FrameRate() < 29.98 && s.FixedFrameRate &&
					s.MaxNumReorderFrames == 2
			}},
		{"vui sar table", spsSpec{profile: 66, poc: 2, wMbs: 20, hMapUnits: 15, frameMbsOnly: true, vui: true, sarIdc: 14,
			sigNoDesc: true, nalHRD: true}, 320, 240,
			func(s *SPS) bool {
				return s.SarWidth == 4 && s.SarHeight == 3 && s.ColourPrimaries == 2 && s.FrameRate() == 0 &&
					s.MaxNumReorderFrames == -1
			}},
		{"vui reserved sar", spsSpec{profile: 66, poc: 2, wMbs: 20, hMapUnits: 15, frameMbsOnly: true, vui: true, sarIdc: 100,
			restriction: true, reorder: 17}, 320, 240,
			func(s *SPS) bool { return s.SarWidth == 0 && s.MaxNumReorderFrames == -1 }},
	} {
		nalu := tc.spec.build()
		var s SPS
		if err := s.Unmarshal(nalu); err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if s.Width != tc.width || s.Height != tc.height {
			t.Errorf("%s: %dx%d want %dx%d", tc.name, s.Width, s.Height, tc.width, tc.height)
		}
		if tc.check != nil && !tc.check(&s) {
			t.Errorf("%s: %+v", tc.name, s)
		}
	}
}

func TestSPSErrors(t *testing.T) {
	ok := spsSpec{profile: 100, chroma: 1, wMbs: 20, hMapUnits: 15, frameMbsOnly: true}
	for _, tc := range []struct {
		name string
		spec func(*spsSpec)
	}{
		{"id", func(p *spsSpec) { p.id = 32 }},
		{"chroma", func(p *spsSpec) { p.chroma = 4 }},
		{"depth", func(p *spsSpec) { p.depth = 7 }},
		{"frame num", func(p *spsSpec) { p.log2MaxFrameNum = 13 }},
		{"poc", func(p *spsSpec) { p.poc = 3 }},
		{"poc cycle", func(p *spsSpec) { p.poc = 1; p.pocCycle = 256 }},
		{"width", func(p *spsSpec) { p.wMbs = 1<<16 + 1 }},
		{"crop", func(p *spsSpec) { p.crop = []uint32{80, 80, 0, 0} }},
	} {
		spec := ok
		tc.spec(&spec)
		var s SPS
		if err := s.Unmarshal(spec.build()); err != ErrSPS {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	var s SPS
	if err := s.Unmarshal([]byte{0x68, 1}); err != ErrNotSPS {
		t.Fatal(err)
	}
	if err := s.Unmarshal(nil); err != ErrNotSPS {
		t.Fatal(err)
	}
	// poc lsb out of range: built by hand since the spec writes 2.
	full := ok.build()
	for i := 1; i < len(full)-1; i++ {
		if err := s.Unmarshal(full[:i]); err == nil {
			t.Fatalf("truncated at %d parses", i)
		}
	}
	w := bitio.NewWriter([]byte{0x67, 66, 0, 30})
	w.UE(0)
	w.UE(0)
	w.UE(0)
	w.UE(13)
	if err := s.Unmarshal(w.Bytes()); err != ErrSPS {
		t.Fatal(err)
	}
	// An HRD with too many CPBs ends the VUI, but not the SPS.
	bad := spsSpec{profile: 66, poc: 2, wMbs: 20, hMapUnits: 15, frameMbsOnly: true, vui: true, timing: true, nalHRD: true, cpbCnt: 32}
	if err := s.Unmarshal(bad.build()); err != nil || s.Width != 320 || s.FrameRate() == 0 {
		t.Fatal(err, s)
	}
	// A VUI cut inside its timing leaves no half-read rate.
	cut := spsSpec{profile: 66, poc: 2, wMbs: 20, hMapUnits: 15, frameMbsOnly: true, vui: true, timing: true}.build()
	if err := s.Unmarshal(cut[:len(cut)-6]); err != nil || s.Width != 320 || s.TimeScale != 0 || s.NumUnitsInTick != 0 {
		t.Fatal(err, s)
	}
}

func TestSyntheticConfig(t *testing.T) {
	sps := spsSpec{profile: 122, chroma: 2, depth: 2, wMbs: 20, hMapUnits: 15, frameMbsOnly: true}.build()
	c, err := NewAVCConfig([][]byte{sps}, [][]byte{{0x68, 1}, {0x68, 2}})
	if err != nil {
		t.Fatal(err)
	}
	c.SPSExt = [][]byte{{0x6d, 1}}
	c.LengthSize = 2
	b, err := c.Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	var d AVCConfig
	if err := d.Unmarshal(b); err != nil {
		t.Fatal(err)
	}
	if !d.Ext || d.ChromaFormat != 2 || d.BitDepthLuma != 10 || d.BitDepthChroma != 10 || len(d.PPS) != 2 ||
		len(d.SPSExt) != 1 || d.LengthSize != 2 || c.MarshalSize() != len(b) {
		t.Fatalf("%+v", d)
	}
	// A cut extension is no extension.
	if err := d.Unmarshal(b[:len(b)-1]); err != nil || d.Ext || d.SPSExt != nil {
		t.Fatalf("%v %+v", err, d)
	}
	// Errors.
	for i := range len(b) - 8 { // 8: the extension
		if err := d.Unmarshal(b[:i]); err == nil {
			t.Fatalf("cut at %d parses", i)
		}
	}
	bad := bytes.Clone(b)
	bad[0] = 0
	if err := d.Unmarshal(bad); err != ErrConfigVersion {
		t.Fatal(err)
	}
	bad[0], bad[4] = 1, 0xfe
	if err := d.Unmarshal(bad); err != ErrLengthSize {
		t.Fatal(err)
	}
	if _, err := NewAVCConfig(nil, nil); err != ErrNoSPS {
		t.Fatal(err)
	}
	if _, err := NewAVCConfig([][]byte{{0x67}}, nil); err != ErrSPS {
		t.Fatal(err)
	}
	for _, mod := range []func(*AVCConfig) error{
		func(c *AVCConfig) error { c.LengthSize = 3; return ErrLengthSize },
		func(c *AVCConfig) error { c.SPS = make([][]byte, 32); return ErrTooMany },
		func(c *AVCConfig) error { c.PPS = [][]byte{make([]byte, 65536)}; return ErrSetSize },
	} {
		e := *c
		want := mod(&e)
		if _, err := e.Append(nil); err != want {
			t.Errorf("%v, want %v", err, want)
		}
	}
	e := *c
	e.LengthSize = 0
	if b, err := e.Append(nil); err != nil || b[4] != 0xff {
		t.Fatal(err)
	}
}

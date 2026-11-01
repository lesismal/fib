package h264

import (
	"bytes"
	"encoding/hex"
	"os"
	"slices"
	"strings"
	"testing"
)

func unhex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.Join(strings.Fields(s), ""))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func readFile(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// fixtures are parameter sets libx264 wrote (ffmpeg 5.1), with the
// avcC ffmpeg's MP4 muxer built from them and what ffprobe reports.
var fixtures = []struct {
	name                  string
	width, height         int
	profile, level        uint8
	chroma, depth         uint8
	sarW, sarH            uint16
	frameMbsOnly, reorder bool
}{
	{"h264_high_720", 1280, 720, 100, 31, 1, 8, 1, 1, true, true},
	{"h264_base_360", 640, 360, 66, 30, 1, 8, 1, 1, true, false},
	{"h264_crop_1918", 1918, 1078, 100, 40, 1, 8, 1, 1, true, true},
	{"h264_444", 320, 240, 244, 13, 3, 8, 1, 1, true, true},
	{"h264_422_10", 320, 240, 122, 13, 2, 10, 1, 1, true, true},
	{"h264_cqm", 352, 288, 100, 13, 1, 8, 1, 1, true, true},
	{"h264_interlaced", 720, 576, 100, 30, 1, 8, 64, 45, false, true},
}

func parameterSets(t testing.TB, name string) (sps, pps [][]byte) {
	for nalu := range SplitAnnexB(readFile(t, name+".ps")) {
		switch Type(nalu) {
		case NALSPS:
			sps = append(sps, nalu)
		case NALPPS:
			pps = append(pps, nalu)
		}
	}
	if len(sps) != 1 || len(pps) != 1 {
		t.Fatalf("%s: %d SPS %d PPS", name, len(sps), len(pps))
	}
	return sps, pps
}

func TestFixtureSPS(t *testing.T) {
	for _, f := range fixtures {
		sps, _ := parameterSets(t, f.name)
		var s SPS
		if err := s.Unmarshal(sps[0]); err != nil {
			t.Fatalf("%s: %v", f.name, err)
		}
		if s.Width != f.width || s.Height != f.height {
			t.Errorf("%s: %dx%d, ffprobe says %dx%d", f.name, s.Width, s.Height, f.width, f.height)
		}
		if s.ProfileIDC != f.profile || s.LevelIDC != f.level || s.ChromaFormatIDC != f.chroma ||
			s.BitDepthLuma != f.depth || s.BitDepthChroma != f.depth {
			t.Errorf("%s: profile %d level %d chroma %d depth %d/%d", f.name, s.ProfileIDC, s.LevelIDC,
				s.ChromaFormatIDC, s.BitDepthLuma, s.BitDepthChroma)
		}
		if !s.VUI || s.SarWidth != f.sarW || s.SarHeight != f.sarH || s.FrameMbsOnly != f.frameMbsOnly {
			t.Errorf("%s: vui %v sar %d:%d frame mbs only %v", f.name, s.VUI, s.SarWidth, s.SarHeight, s.FrameMbsOnly)
		}
		if (s.MaxNumReorderFrames > 0) != f.reorder {
			t.Errorf("%s: reorder %d", f.name, s.MaxNumReorderFrames)
		}
	}
}

func TestInterlacedVUI(t *testing.T) {
	sps, _ := parameterSets(t, "h264_interlaced")
	var s SPS
	if err := s.Unmarshal(sps[0]); err != nil {
		t.Fatal(err)
	}
	if !s.VideoFullRange || s.ColourPrimaries != 1 || s.TransferCharacteristics != 1 || s.MatrixCoefficients != 1 {
		t.Errorf("colour %+v", s)
	}
	if s.FrameRate() != 25 {
		t.Errorf("frame rate %v (%d/%d)", s.FrameRate(), s.TimeScale, s.NumUnitsInTick)
	}
	if s.PicHeightInMapUnits != 18 {
		t.Errorf("map units %d", s.PicHeightInMapUnits)
	}
}

func TestFixtureConfig(t *testing.T) {
	for _, f := range fixtures {
		want := readFile(t, f.name+".avcc")
		var c AVCConfig
		if err := c.Unmarshal(want); err != nil {
			t.Fatalf("%s: %v", f.name, err)
		}
		sps, pps := parameterSets(t, f.name)
		if len(c.SPS) != 1 || !bytes.Equal(c.SPS[0], sps[0]) || len(c.PPS) != 1 || !bytes.Equal(c.PPS[0], pps[0]) {
			t.Fatalf("%s: parameter sets differ", f.name)
		}
		if c.ProfileIndication != f.profile || c.LevelIndication != f.level || c.LengthSize != 4 {
			t.Errorf("%s: %+v", f.name, c)
		}
		if c.Ext != (f.profile != 66) || c.Ext && (c.ChromaFormat != f.chroma || c.BitDepthLuma != f.depth) {
			t.Errorf("%s: ext %v chroma %d depth %d", f.name, c.Ext, c.ChromaFormat, c.BitDepthLuma)
		}
		// What ffmpeg's MP4 muxer writes is what NewAVCConfig builds.
		built, err := NewAVCConfig(sps, pps)
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

func TestCodecString(t *testing.T) {
	c := AVCConfig{ProfileIndication: 100, ProfileCompatibility: 0, LevelIndication: 31}
	if s := c.CodecString(); s != "avc1.64001f" {
		t.Fatal(s)
	}
}

func TestSplitAnnexB(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"0102", nil},
		{"000001", nil},
		{"00000001 0910 000001 6742 00000001 6801 0000", []string{"0910", "6742", "6801"}},
		{"ff 000001 65 00 00 00 000001 41", []string{"65", "41"}},
		{"000001 000001 41 0000", []string{"41"}},
		{"000001 41 000002 00 0003 01", []string{"41000002000003 01"}},
	} {
		var got []string
		for n := range SplitAnnexB(unhex(t, tc.in)) {
			got = append(got, hex.EncodeToString(n))
		}
		var want []string
		for _, w := range tc.want {
			want = append(want, strings.ReplaceAll(w, " ", ""))
		}
		if !slices.Equal(got, want) {
			t.Errorf("%q: got %v want %v", tc.in, got, want)
		}
	}
	// Stopping early.
	n := 0
	for range SplitAnnexB(unhex(t, "000001 41 000001 41")) {
		n++
		break
	}
	if n != 1 {
		t.Fatal(n)
	}
}

func TestIsAnnexB(t *testing.T) {
	for in, want := range map[string]bool{"000001": true, "00000001": true, "000000": false, "0000": false, "00000002": false, "0001": false} {
		if IsAnnexB(unhex(t, in)) != want {
			t.Errorf("%s", in)
		}
	}
}

func TestStream(t *testing.T) {
	stream := readFile(t, "stream.h264")
	if !IsAnnexB(stream) || !IsKeyframeAnnexB(stream) {
		t.Fatal("not a keyframe stream")
	}
	types := map[NALUType]int{}
	for n := range SplitAnnexB(stream) {
		types[Type(n)]++
	}
	if types[NALSPS] != 1 || types[NALPPS] != 1 || types[NALIDR] != 1 || types[NALSlice] < 4 || types[NALSEI] != 1 {
		t.Fatalf("types %v", types)
	}
	avcc := AnnexBToAVCC(nil, stream, nil)
	if !ValidAVCC(avcc, 4) || !IsKeyframe(avcc) {
		t.Fatal("avcc")
	}
	back, err := AVCCToAnnexB(nil, avcc, 4)
	if err != nil {
		t.Fatal(err)
	}
	var a, b [][]byte
	for n := range SplitAnnexB(stream) {
		a = append(a, n)
	}
	for n := range SplitAnnexB(back) {
		b = append(b, n)
	}
	if !slices.EqualFunc(a, b, bytes.Equal) {
		t.Fatal("annex b round trip")
	}
	// Leaving out everything but slices: the access unit is no keyframe
	// once the IDR goes too.
	slicesOnly := AnnexBToAVCC(nil, stream, func(n []byte) bool { return Type(n) == NALSlice })
	if IsKeyframe(slicesOnly) || len(slicesOnly) == 0 {
		t.Fatal("filter")
	}
	if IsKeyframeAnnexB(unhex(t, "000001 41")) {
		t.Fatal("non-IDR is a keyframe")
	}
}

func TestAVCC(t *testing.T) {
	nalus := [][]byte{{0x67, 1}, {0x68}, {0x65, 1, 2, 3}}
	for _, size := range []int{1, 2, 4} {
		var b []byte
		for _, n := range nalus {
			var err error
			if b, err = AppendAVCCSize(b, n, size); err != nil {
				t.Fatal(err)
			}
		}
		if !ValidAVCC(b, size) {
			t.Fatalf("size %d invalid", size)
		}
		var got [][]byte
		for n := range SplitAVCC(b, size) {
			got = append(got, n)
		}
		if !slices.EqualFunc(got, nalus, bytes.Equal) {
			t.Fatalf("size %d: %x", size, got)
		}
		norm, err := NormalizeAVCC(nil, b, size)
		if err != nil || !IsKeyframe(norm) || len(norm) != len(b)+len(nalus)*(4-size) {
			t.Fatalf("size %d: normalize %x %v", size, norm, err)
		}
		if ValidAVCC(b[:len(b)-1], size) {
			t.Fatalf("size %d: truncated valid", size)
		}
		if _, err := NormalizeAVCC(nil, b[:len(b)-1], size); err != ErrShortNALU {
			t.Fatal(err)
		}
		if _, err := AVCCToAnnexB(nil, b[:len(b)-1], size); err != ErrShortNALU {
			t.Fatal(err)
		}
		for i := range size {
			if _, _, err := NextAVCC(b[:i], size); err != ErrShortNALU {
				t.Fatalf("size %d short %d: %v", size, i, err)
			}
		}
	}
	if _, _, err := NextAVCC([]byte{0, 0, 0}, 3); err != ErrLengthSize {
		t.Fatal(err)
	}
	if ValidAVCC(nil, 3) || !ValidAVCC(nil, 4) {
		t.Fatal("empty")
	}
	if _, err := AppendAVCCSize(nil, make([]byte, 256), 1); err != ErrNALUSize {
		t.Fatal(err)
	}
	if _, err := AppendAVCCSize(nil, make([]byte, 65536), 2); err != ErrNALUSize {
		t.Fatal(err)
	}
	if _, err := AppendAVCCSize(nil, nil, 3); err != ErrLengthSize {
		t.Fatal(err)
	}
	n := 0
	for range SplitAVCC([]byte{0, 0, 0, 1, 9, 0, 0, 0, 1, 9}, 4) {
		n++
		break
	}
	if n != 1 {
		t.Fatal(n)
	}
}

func TestPrependParameterSets(t *testing.T) {
	sps, pps := []byte{0x67, 1}, []byte{0x68, 2}
	idr := AppendAVCC(nil, []byte{0x65, 3})
	got := PrependParameterSets([]byte{0xee}, idr, sps, pps)
	want := append([]byte{0xee}, AppendAVCC(AppendAVCC(nil, sps), pps)...)
	want = append(want, idr...)
	if !bytes.Equal(got, want) {
		t.Fatalf("got %x want %x", got, want)
	}
	// An access unit that has its SPS already is left alone.
	if again := PrependParameterSets(nil, got[1:], sps, pps); !bytes.Equal(again, got[1:]) {
		t.Fatalf("doubled %x", again)
	}
}

func TestEmulationPrevention(t *testing.T) {
	for _, tc := range []struct{ rbsp, nalu string }{
		{"", ""},
		{"000000", "00000300"},
		{"000001", "00000301"},
		{"000002", "00000302"},
		{"000003", "00000303"},
		{"000004", "000004"},
		{"0000000000", "0000030000 0300"},
		{"aa0000", "aa0000"},
	} {
		rbsp, nalu := unhex(t, tc.rbsp), unhex(t, tc.nalu)
		if got := AddEmulationPrevention(nil, rbsp); !bytes.Equal(got, nalu) {
			t.Errorf("add %s: %x", tc.rbsp, got)
		}
		if got := RemoveEmulationPrevention(nil, nalu); !bytes.Equal(got, rbsp) {
			t.Errorf("remove %s: %x", tc.nalu, got)
		}
	}
}

func TestNALUType(t *testing.T) {
	if Type(nil) != 0 || Type([]byte{0x65}) != NALIDR || NALIDR.String() != "IDR" ||
		NALUType(0).String() != "NALUType(0)" || NALUType(28).String() != "NALUType(28)" {
		t.Fatal("names")
	}
}

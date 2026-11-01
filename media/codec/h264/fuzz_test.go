package h264

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func seedFiles(f *testing.F, pattern string) {
	names, _ := filepath.Glob("testdata/" + pattern)
	for _, n := range names {
		b, err := os.ReadFile(n)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
}

// FuzzSPS parses arbitrary bytes as an SPS: it either fails or yields a
// picture size within the limits it checks.
func FuzzSPS(f *testing.F) {
	for _, fx := range fixtures {
		sps, _ := parameterSets(f, fx.name)
		f.Add(sps[0])
	}
	f.Add(spsSpec{profile: 244, chroma: 3, scaling: []int{1, 2, 0, 0, 0, 0, 1, 2, 0, 0, 0, 1}, wMbs: 4, hMapUnits: 4,
		vui: true, sarIdc: 255, colour: true, timing: true, nalHRD: true, vclHRD: true, restriction: true}.build())
	f.Fuzz(func(t *testing.T, b []byte) {
		var s SPS
		if err := s.Unmarshal(b); err != nil {
			return
		}
		if s.Width <= 0 || s.Height <= 0 || s.Width > 1<<20 || s.Height > 1<<21 {
			t.Fatalf("size %dx%d", s.Width, s.Height)
		}
		if _, err := NewAVCConfig([][]byte{b}, nil); err != nil {
			t.Fatal(err)
		}
	})
}

// FuzzAVCConfig parses arbitrary bytes as an AVCDecoderConfigurationRecord:
// it either fails or yields a record that builds and parses back the same.
func FuzzAVCConfig(f *testing.F) {
	seedFiles(f, "*.avcc")
	f.Fuzz(func(t *testing.T, b []byte) {
		var c AVCConfig
		if err := c.Unmarshal(b); err != nil {
			return
		}
		out, err := c.Append(nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(out) != c.MarshalSize() {
			t.Fatalf("size %d, MarshalSize %d", len(out), c.MarshalSize())
		}
		var d AVCConfig
		if err := d.Unmarshal(out); err != nil {
			t.Fatal(err)
		}
		if d.ProfileIndication != c.ProfileIndication || d.LengthSize != c.LengthSize || d.Ext != c.Ext ||
			d.ChromaFormat != c.ChromaFormat || d.BitDepthLuma != c.BitDepthLuma ||
			!slices.EqualFunc(d.SPS, c.SPS, bytes.Equal) || !slices.EqualFunc(d.PPS, c.PPS, bytes.Equal) ||
			!slices.EqualFunc(d.SPSExt, c.SPSExt, bytes.Equal) {
			t.Fatalf("round trip\n%+v\n%+v", c, d)
		}
	})
}

// FuzzFraming splits arbitrary bytes as Annex B and as AVCC: conversions
// between the framings keep the NAL units.
func FuzzFraming(f *testing.F) {
	seedFiles(f, "*.ps")
	f.Add([]byte{0, 0, 1, 0x65, 0, 0, 0, 1, 0x41, 0, 0})
	f.Add([]byte{0, 0, 0, 2, 0x67, 1, 0, 0, 0, 1, 0x65})
	f.Fuzz(func(t *testing.T, b []byte) {
		var nalus [][]byte
		for n := range SplitAnnexB(b) {
			if len(n) == 0 || bytes.Contains(n, []byte{0, 0, 1}) || n[len(n)-1] == 0 {
				t.Fatalf("bad NAL unit %x", n)
			}
			nalus = append(nalus, n)
		}
		avcc := AnnexBToAVCC(nil, b, nil)
		if !ValidAVCC(avcc, 4) || IsKeyframe(avcc) != IsKeyframeAnnexB(b) {
			t.Fatal("avcc")
		}
		back, err := AVCCToAnnexB(nil, avcc, 4)
		if err != nil {
			t.Fatal(err)
		}
		var again [][]byte
		for n := range SplitAnnexB(back) {
			again = append(again, n)
		}
		if !slices.EqualFunc(nalus, again, bytes.Equal) {
			t.Fatal("round trip")
		}
		for _, size := range []int{1, 2, 4} {
			valid := ValidAVCC(b, size)
			norm, err := NormalizeAVCC(nil, b, size)
			if (err == nil) != valid || valid && !ValidAVCC(norm, 4) {
				t.Fatalf("size %d: valid %v err %v", size, valid, err)
			}
			for range SplitAVCC(b, size) {
			}
		}
		_ = PrependParameterSets(nil, b, []byte{0x67})
	})
}

// FuzzEmulationPrevention checks that removing what adding inserted gives
// the RBSP back, and that what adding makes holds no start code.
func FuzzEmulationPrevention(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 1, 0, 0, 3})
	f.Fuzz(func(t *testing.T, rbsp []byte) {
		nalu := AddEmulationPrevention(nil, rbsp)
		if bytes.Contains(nalu, []byte{0, 0, 1}) || bytes.Contains(nalu, []byte{0, 0, 0}) || bytes.Contains(nalu, []byte{0, 0, 2}) {
			t.Fatalf("start code in %x", nalu)
		}
		if got := RemoveEmulationPrevention(nil, nalu); !bytes.Equal(got, rbsp) {
			t.Fatalf("got %x want %x", got, rbsp)
		}
		_ = RemoveEmulationPrevention(nil, rbsp)
	})
}

package h265

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// FuzzSPS parses arbitrary bytes as an SPS and a VPS: they either fail or
// yield a picture size within the limits checked, and a record builds.
func FuzzSPS(f *testing.F) {
	for _, fx := range fixtures {
		vps, sps, _ := parameterSets(f, fx.name)
		f.Add(sps[0])
		f.Add(vps[0])
	}
	f.Add(spsSpec{sub: 3, subProfile: []bool{true, false, true}, chroma: 2, w: 100, h: 100, conf: []uint32{1, 1, 1, 1}}.build(NALSPS))
	f.Fuzz(func(t *testing.T, b []byte) {
		var v VPS
		_ = v.Unmarshal(b)
		var s SPS
		if err := s.Unmarshal(b); err != nil {
			return
		}
		if s.Width <= 0 || s.Height <= 0 || s.Width > 1<<20 || s.Height > 1<<20 {
			t.Fatalf("size %dx%d", s.Width, s.Height)
		}
		if _, err := NewHEVCConfig(nil, [][]byte{b}, nil); err != nil {
			t.Fatal(err)
		}
	})
}

// FuzzHEVCConfig parses arbitrary bytes as an HEVCDecoderConfigurationRecord:
// it either fails or yields a record that builds and parses back the same.
func FuzzHEVCConfig(f *testing.F) {
	names, _ := filepath.Glob("testdata/*.hvcc")
	for _, n := range names {
		b, err := os.ReadFile(n)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		var c HEVCConfig
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
		var d HEVCConfig
		if err := d.Unmarshal(out); err != nil {
			t.Fatal(err)
		}
		if d.ProfileTierLevel != c.ProfileTierLevel || d.LengthSize != c.LengthSize ||
			d.NumTemporalLayers != c.NumTemporalLayers || d.BitDepthLuma != c.BitDepthLuma ||
			!slices.EqualFunc(d.Arrays, c.Arrays, func(a, b NALArray) bool {
				return a.Type == b.Type && a.Complete == b.Complete && slices.EqualFunc(a.NALUs, b.NALUs, bytes.Equal)
			}) {
			t.Fatalf("round trip\n%+v\n%+v", c, d)
		}
		_ = c.CodecString()
		_ = IsKeyframe(b)
		_ = IsKeyframeAnnexB(b)
	})
}

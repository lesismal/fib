package av1

import (
	"bytes"
	"reflect"
	"testing"
)

// FuzzSequenceHeader parses arbitrary bytes as a sequence header OBU: it
// either fails or yields a size in range and a record that parses back.
func FuzzSequenceHeader(f *testing.F) {
	for _, tu := range temporalUnits(readFixture(f))[:1] {
		f.Add(tu)
	}
	f.Add(seqSpec{profile: 2, levels: []uint8{9, 12}, timing: true, decoderModel: true, displayDelay: true, highBitdepth: true,
		twelveBit: true, subX: true, subY: true, width: 64, height: 64}.build())
	f.Fuzz(func(t *testing.T, b []byte) {
		IsKeyframe(b)
		var s SequenceHeader
		if s.Unmarshal(b) != nil {
			return
		}
		if s.Width <= 0 || s.Height <= 0 || s.Width > 1<<16 || s.Height > 1<<16 {
			t.Fatalf("size %dx%d", s.Width, s.Height)
		}
		c, err := NewConfig(b)
		if err != nil {
			t.Fatal(err)
		}
		var back Config
		if err := back.Unmarshal(c.Append(nil)); err != nil || !bytes.Equal(back.Append(nil), c.Append(nil)) {
			t.Fatal("record does not round trip", err)
		}
	})
}

// FuzzConfig parses arbitrary bytes as an AV1CodecConfigurationRecord: it
// either fails or builds a record that parses back the same, reserved bits
// aside.
func FuzzConfig(f *testing.F) {
	f.Add([]byte{0x81, 0x20, 0x00, 0x00, 0x0a, 0x0d, 0x20, 0x00, 0x00, 0x03, 0xb4, 0xff, 0x73, 0x6b, 0xe4, 0x80, 0x86, 0x80, 0x10})
	f.Fuzz(func(t *testing.T, b []byte) {
		var c Config
		if c.Unmarshal(b) != nil {
			return
		}
		var back Config
		if err := back.Unmarshal(c.Append(nil)); err != nil || !reflect.DeepEqual(back, c) {
			t.Fatalf("%x builds back as %x", b, c.Append(nil))
		}
		c.SequenceHeader()
		_ = c.CodecString()
	})
}

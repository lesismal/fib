package qpack

import (
	"strings"
	"testing"
)

// FuzzDecode feeds the decoder arbitrary field sections: whatever it is
// given, it either returns fields or an error, and never panics or runs
// away with memory.
func FuzzDecode(f *testing.F) {
	f.Add([]byte{0x00, 0x00})
	f.Add([]byte{0x00, 0x00, 0xc0 | 17})
	f.Add([]byte{0x00, 0x00, 0x51, 0x0b, '/', 'i', 'n', 'd', 'e', 'x', '.', 'h', 't', 'm', 'l'})
	f.Add(encode([]HeaderField{{":status", "200"}, {"x-a", "b"}, {"x-long", strings.Repeat("v", 200)}}))
	f.Add([]byte{0x00, 0x00, 0x2f, 0x7f, 0xff, 0xff, 0xff, 0x7f})
	f.Add([]byte{0x01, 0x00, 0x80})
	f.Fuzz(func(t *testing.T, block []byte) {
		var size int
		err := Decode(block, 1<<16, func(field HeaderField) error {
			size += len(field.Name) + len(field.Value)
			return nil
		})
		if err == nil && size > 1<<16 {
			t.Fatalf("decoded %d bytes of fields past the limit", size)
		}
	})
}

// FuzzRoundTrip encodes arbitrary fields and decodes them again: what the
// encoder writes the decoder has to read back exactly.
func FuzzRoundTrip(f *testing.F) {
	f.Add(":status", "200", "x-name", "value")
	f.Add("content-type", "text/html; charset=utf-8", "", "")
	f.Add("x-å", "welt", "cookie", "a=1")
	f.Fuzz(func(t *testing.T, name1, value1, name2, value2 string) {
		want := []HeaderField{{name1, value1}, {name2, value2}}
		for _, field := range want {
			// Field names are lowercase and free of the separators the
			// encoder is not asked to escape; anything else is the
			// caller's own error, not the encoder's.
			if strings.ToLower(field.Name) != field.Name {
				t.Skip()
			}
		}
		block := encode(want)
		var got []HeaderField
		if err := Decode(block, 0, func(field HeaderField) error {
			got = append(got, field)
			return nil
		}); err != nil {
			t.Fatalf("decoding what was encoded: %v", err)
		}
		if len(got) != len(want) {
			t.Fatalf("got %d fields, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("field %d: got %q %q, want %q %q", i, got[i].Name, got[i].Value, want[i].Name, want[i].Value)
			}
		}
	})
}

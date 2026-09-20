package hpack

import (
	"strings"
	"testing"
)

// FuzzDecode feeds the decoder arbitrary header blocks: whatever it is
// given, it either returns fields or an error, and never panics.
func FuzzDecode(f *testing.F) {
	f.Add([]byte{0x82})
	f.Add([]byte{0x40, 0x0a, 'c', 'u', 's', 't', 'o', 'm', '-', 'k', 'e', 'y', 0x0d,
		'c', 'u', 's', 't', 'o', 'm', '-', 'h', 'e', 'a', 'd', 'e', 'r'})
	f.Add([]byte{0x20})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f})
	f.Add([]byte{0x00, 0x8b, 0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff})
	f.Fuzz(func(t *testing.T, block []byte) {
		decoder := NewDecoder(DefaultTableSize)
		decoder.MaxStringLength = 1 << 16
		_ = decoder.Decode(block, func(HeaderField) error { return nil })
	})
}

// FuzzRoundTrip encodes arbitrary fields and decodes them again through a
// decoder that tracks the same dynamic table.
func FuzzRoundTrip(f *testing.F) {
	f.Add(":method", "GET", "x-name", "value")
	f.Add("accept-encoding", "gzip, deflate", "", "")
	f.Add("x-å", "welt", "cookie", "a=1")
	f.Fuzz(func(t *testing.T, name1, value1, name2, value2 string) {
		want := []HeaderField{{name1, value1}, {name2, value2}}
		for _, field := range want {
			if strings.ToLower(field.Name) != field.Name {
				t.Skip()
			}
		}
		encoder := NewEncoder()
		block := encoder.Begin(nil)
		for _, field := range want {
			block = encoder.AppendField(block, field.Name, field.Value, false)
		}
		decoder := NewDecoder(DefaultTableSize)
		var got []HeaderField
		if err := decoder.Decode(block, func(field HeaderField) error {
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

// FuzzHuffman checks that what the Huffman coder writes it reads back.
func FuzzHuffman(f *testing.F) {
	f.Add("www.example.com")
	f.Add("")
	f.Add("\x00\xff hello")
	f.Fuzz(func(t *testing.T, s string) {
		coded := AppendHuffman(nil, s)
		if len(coded) != HuffmanLen(s) {
			t.Fatalf("coded %d bytes, HuffmanLen says %d", len(coded), HuffmanLen(s))
		}
		got, err := HuffmanDecode(coded, 0)
		if err != nil {
			t.Fatalf("decoding what was coded: %v", err)
		}
		if got != s {
			t.Fatalf("got %q, want %q", got, s)
		}
	})
}

package hpack

import (
	"errors"
	"testing"
)

// TestCoverDecodeErrors pins down how Decode fails on each kind of malformed
// representation: a truncated integer or string, an index past both tables,
// and a string longer than MaxStringLength, each reported as its own error.
func TestCoverDecodeErrors(t *testing.T) {
	for _, tt := range []struct {
		name  string
		block []byte
		max   int
		want  error
	}{
		{"indexedIntegerTruncated", []byte{0xff}, 0, ErrTruncated},
		{"sizeUpdateTruncated", []byte{0x3f}, 0, ErrTruncated},
		{"literalIndexTruncated", []byte{0x7f}, 0, ErrTruncated},
		{"literalNameIndexInvalid", []byte{0x7e, 0x01, 'a'}, 0, ErrInvalidIndex},
		{"valueMissing", []byte{0x41}, 0, ErrTruncated},
		{"valueLengthTruncated", []byte{0x41, 0x7f}, 0, ErrTruncated},
		{"literalNameTruncated", []byte{0x00, 0x7f}, 0, ErrTruncated},
		{"plainTooLong", []byte{0x41, 0x02, 'a', 'b'}, 1, ErrStringTooLong},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDecoder(DefaultTableSize)
			d.MaxStringLength = tt.max
			err := d.Decode(tt.block, func(HeaderField) error { return nil })
			if !errors.Is(err, tt.want) {
				t.Fatalf("Decode(% x) = %v, want %v", tt.block, err, tt.want)
			}
		})
	}
}

// TestCoverDecodeEmitError pins down that an error emit returns stops Decode
// and is what it returns, for every representation that emits a field.
func TestCoverDecodeEmitError(t *testing.T) {
	stop := errors.New("stop")
	for _, tt := range []struct {
		name  string
		block []byte
	}{
		{"indexed", []byte{0x82}},
		{"incremental", []byte{0x41, 0x01, 'a'}},
		{"withoutIndexing", []byte{0x01, 0x01, 'a'}},
		{"neverIndexed", []byte{0x11, 0x01, 'a'}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDecoder(DefaultTableSize)
			if err := d.Decode(tt.block, func(HeaderField) error { return stop }); err != stop {
				t.Fatalf("Decode = %v, want the emit error", err)
			}
		})
	}
}

// TestCoverEncoderTableSize pins down SetMaxTableSize: an allowance above the
// default changes nothing, and a shrink followed by a regrowth before the
// next block announces the smallest size and then the final one, as RFC 7541
// section 4.2 requires, in a form a decoder accepts.
func TestCoverEncoderTableSize(t *testing.T) {
	e := NewEncoder()
	e.SetMaxTableSize(2 * DefaultTableSize)
	if got := e.Begin(nil); len(got) != 0 {
		t.Fatalf("an allowance above the default announced % x", got)
	}
	e.SetMaxTableSize(100)
	e.SetMaxTableSize(200)
	block := e.Begin(nil)
	want := appendInt(appendInt(nil, 0x20, 5, 100), 0x20, 5, 200)
	if string(block) != string(want) {
		t.Fatalf("Begin = % x, want % x", block, want)
	}
	d := NewDecoder(DefaultTableSize)
	if fields := decodeAll(t, d, e.AppendField(block, "x-a", "b", false)); len(fields) != 1 {
		t.Fatalf("decoded %v", fields)
	}
	if d.table.maxSize != 200 {
		t.Fatalf("decoder table size %d, want 200", d.table.maxSize)
	}
}

// TestCoverHuffmanErrors pins down the exported Huffman decoder's failures:
// the EOS code, which runs off the tree, and output past maxLen.
func TestCoverHuffmanErrors(t *testing.T) {
	if _, err := HuffmanDecode([]byte{0xff, 0xff, 0xff, 0xff}, 0); !errors.Is(err, ErrInvalidHuffman) {
		t.Fatalf("EOS decoded: %v", err)
	}
	// '0', five bits, then EOS's thirty ones, which end in the high half
	// of the fifth byte.
	if _, err := HuffmanDecode([]byte{0x07, 0xff, 0xff, 0xff, 0xff}, 0); !errors.Is(err, ErrInvalidHuffman) {
		t.Fatalf("EOS ending in a byte's high half decoded: %v", err)
	}
	if _, err := HuffmanDecode(AppendHuffman(nil, "abc"), 2); !errors.Is(err, ErrStringTooLong) {
		t.Fatalf("3 bytes under a bound of 2: %v", err)
	}
	if s, err := HuffmanDecode(AppendHuffman(nil, "abc"), 3); err != nil || s != "abc" {
		t.Fatalf("HuffmanDecode = %q, %v", s, err)
	}
}

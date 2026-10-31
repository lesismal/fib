package turn

import (
	"bytes"
	"testing"
)

func TestChannelDataFraming(t *testing.T) {
	b := AppendChannelData(nil, 0x4001, []byte("abc"))
	if !bytes.Equal(b, []byte{0x40, 0x01, 0, 3, 'a', 'b', 'c'}) || !IsChannelData(b) {
		t.Fatalf("% x", b)
	}
	// Padding past the length is ignored.
	n, d, err := ParseChannelData(append(b, 0))
	if err != nil || n != 0x4001 || string(d) != "abc" {
		t.Fatalf("%x %q %v", n, d, err)
	}
	for _, tc := range []struct {
		b   []byte
		err error
	}{
		{[]byte{0x40, 0x00, 0}, ErrTruncated},
		{[]byte{0x40, 0x00, 0, 4, 1, 2, 3}, ErrTruncated},
		{[]byte{0x50, 0x00, 0, 0}, ErrNotChannelData},
		{[]byte{0x3F, 0xFF, 0, 0}, ErrNotChannelData},
	} {
		if _, _, err := ParseChannelData(tc.b); err != tc.err {
			t.Errorf("% x: %v, want %v", tc.b, err, tc.err)
		}
	}
	for _, first := range []byte{0x00, 0x03, 0x14, 0x3F, 0x50, 0x80} {
		if IsChannelData([]byte{first, 0, 0, 0}) {
			t.Errorf("0x%02x taken for ChannelData", first)
		}
	}
}

func FuzzChannelData(f *testing.F) {
	f.Add(AppendChannelData(nil, MinChannel, []byte("data")))
	f.Add([]byte{0x40, 0x00, 0xff, 0xff})
	f.Add([]byte{0x4f, 0xff, 0, 0, 1})
	f.Fuzz(func(t *testing.T, b []byte) {
		n, d, err := ParseChannelData(b)
		if err != nil {
			return
		}
		if !IsChannelData(b) || n < MinChannel || n > MaxChannel {
			t.Fatalf("parsed %x from % x", n, b)
		}
		if again := AppendChannelData(nil, n, d); !bytes.Equal(again, b[:len(again)]) {
			t.Fatal("round trip differs")
		}
	})
}

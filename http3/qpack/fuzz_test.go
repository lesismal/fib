package qpack

import (
	"errors"
	"strings"
	"testing"
)

// FuzzEncoderStream feeds a decoder with a small table arbitrary encoder
// stream bytes, split in two anywhere, and then a field section: it either
// takes them or fails with ErrEncoderStream, never panics, keeps its table
// within the capacity, and buffers no more of an unfinished instruction
// than one that fits could take.
func FuzzEncoderStream(f *testing.F) {
	f.Add([]byte{0x3f, 0xbd, 0x01, 0xc0, 0x01, 'a', 0x02, 0x80}, 3, []byte{0x02, 0x00, 0x80})
	f.Add([]byte{0x3f, 0x45, 0x43, 'a', 'b', 'c', 0x01, 'x', 0x00, 0x81, 0x01, 'y'}, 5, []byte{0x03, 0x81, 0x10})
	f.Add([]byte{0x2a, 0x5f, 0xff, 0xff, 0x7f}, 1, []byte{0x00, 0x00})
	f.Fuzz(func(t *testing.T, data []byte, split int, block []byte) {
		const capacity = 220
		var d Decoder
		d.EnableDynamic(capacity, 2)
		split = min(max(split, 0), len(data))
		err := d.FeedEncoder(data[:split])
		if err == nil {
			err = d.FeedEncoder(data[split:])
		}
		if err != nil && !errors.Is(err, ErrEncoderStream) {
			t.Fatalf("got %v, want ErrEncoderStream", err)
		}
		if dyn := d.dyn; dyn.size > dyn.capacity || dyn.capacity > capacity || err == nil && len(dyn.in) > 8*capacity+32 {
			t.Fatalf("table of %d bytes, capacity %d, %d bytes buffered", dyn.size, dyn.capacity, len(dyn.in))
		}
		_, err = d.AppendSection(nil, block, 1<<16, 0)
		if err != nil && !errors.Is(err, ErrDecompression) && !errors.Is(err, ErrBlocked) && !errors.Is(err, ErrTooLarge) {
			t.Fatalf("decoding: %v", err)
		}
	})
}

// FuzzDecoderStream feeds an encoder that has sent sections arbitrary
// decoder stream bytes: it either takes them or fails with
// ErrDecoderStream, and its count of unacknowledged sections stays true.
func FuzzDecoderStream(f *testing.F) {
	f.Add([]byte{0x84, 0x01, 0x44})
	f.Add([]byte{0x01, 0x01, 0x80, 0x80})
	f.Add([]byte{0xff, 0x80})
	f.Fuzz(func(t *testing.T, data []byte) {
		p := newPair(4096, 0)
		fields := []HeaderField{{"x-a", "aaaaaaaa"}, {"x-b", "bbbbbbbb"}}
		for i := 0; i < 3; i++ {
			p.decode(t, p.encode(fields, 0), 0)
			p.carry(t)
		}
		p.encode(fields, 0)
		p.encode(fields, 4)
		if err := p.enc.FeedDecoder(data); err != nil && !errors.Is(err, ErrDecoderStream) {
			t.Fatalf("got %v, want ErrDecoderStream", err)
		}
		n := 0
		for _, sections := range p.enc.unacked {
			n += len(sections)
		}
		if n != p.enc.pending || p.enc.known > p.enc.t.inserted {
			t.Fatalf("%d pending, %d counted; %d known of %d", p.enc.pending, n, p.enc.known, p.enc.t.inserted)
		}
	})
}

// FuzzDynamicRoundTrip has an Encoder and a Decoder with a small table
// carry arbitrary fields over several sections, the acknowledgments now
// and then a section behind: each decodes to what was encoded, whatever
// the table evicts on the way.
func FuzzDynamicRoundTrip(f *testing.F) {
	f.Add("x-a", "1", "cookie", "a=0123456789abcdefghij", uint8(5))
	f.Add(":authority", "example.com", "x-b", strings.Repeat("v", 90), uint8(9))
	f.Fuzz(func(t *testing.T, name1, value1, name2, value2 string, rounds uint8) {
		if strings.ToLower(name1) != name1 || strings.ToLower(name2) != name2 {
			t.Skip()
		}
		// Without blocked streams, and with two.
		for _, blocked := range []int{0, 2} {
			p := newPair(256, blocked)
			for i := 0; i < int(rounds%12); i++ {
				want := []HeaderField{{name1, value1}, {name2, value2 + string(rune('0'+i%3))}}
				id := uint64(4 * i)
				got, waited := p.decodeLate(t, p.encode(want, id), id)
				if waited && blocked == 0 {
					t.Fatal("a section waited with no blocked streams allowed")
				}
				sameFields(t, got, want)
				if i%4 != 3 {
					p.carry(t)
				}
			}
		}
	})
}

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
		var plain []HeaderField
		err := Decode(block, 1<<16, func(field HeaderField) error {
			size += len(field.Name) + len(field.Value)
			plain = append(plain, field)
			return nil
		})
		if err == nil && size > 1<<16 {
			t.Fatalf("decoded %d bytes of fields past the limit", size)
		}
		// A Decoder, remembering strings or not, decodes the same.
		var d Decoder
		for i := 0; i < 2; i++ {
			fields, derr := d.AppendFields(nil, block, 1<<16)
			if (derr == nil) != (err == nil) {
				t.Fatalf("Decoder: %v, Decode: %v", derr, err)
			}
			if err != nil {
				continue
			}
			if len(fields) != len(plain) {
				t.Fatalf("Decoder got %d fields, Decode %d", len(fields), len(plain))
			}
			for j := range plain {
				if fields[j] != plain[j] {
					t.Fatalf("field %d: Decoder got %q, Decode %q", j, fields[j], plain[j])
				}
			}
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

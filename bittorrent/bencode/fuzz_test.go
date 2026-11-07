package bencode

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

var fuzzSeeds = []string{
	"i0e", "i-1e", "i9223372036854775807e", "0:", "3:abc", "le", "de",
	"d1:ad1:bli1e2:xyeee", "l1:al1:bee", "d1:b1:x1:a1:ye", "d1:ai1e1:ai2ee",
	"d8:announce3:url4:infod6:lengthi1e4:name1:a12:piece lengthi16384eee",
	"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe",
}

// FuzzUnmarshal checks that decoding never panics, that whatever decodes
// re-encodes and decodes to the same value, and that canonical input
// re-encodes to itself.
func FuzzUnmarshal(f *testing.F) {
	for _, s := range fuzzSeeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var v any
		err := Unmarshal(data, &v)
		if Valid(data) != (err == nil || isTypeError(err)) {
			t.Fatalf("Valid and Unmarshal disagree on %q: %v", data, err)
		}
		if err != nil {
			return
		}
		b, err := Marshal(v)
		if err != nil {
			t.Fatalf("re-encode %#v: %v", v, err)
		}
		var w any
		if err := Unmarshal(b, &w); err != nil {
			t.Fatalf("decode re-encoding %q: %v", b, err)
		}
		if !reflect.DeepEqual(v, w) {
			t.Fatalf("round trip changed %#v to %#v", v, w)
		}
		d := NewBytesDecoder(data)
		d.Strict()
		if d.Decode(&w) == nil && !bytes.Equal(b, data) {
			t.Fatalf("canonical %q re-encoded as %q", data, b)
		}
		var raw RawMessage
		if err := Unmarshal(data, &raw); err != nil || !bytes.Equal(raw, data) {
			t.Fatalf("RawMessage of %q: %q %v", data, raw, err)
		}
	})
}

// FuzzStruct decodes into a struct of every kind of field, which must not
// panic, and re-encodes what decoded.
func FuzzStruct(f *testing.F) {
	for _, s := range fuzzSeeds {
		f.Add([]byte(s))
	}
	f.Add([]byte("d4:hash4:abcd6:lengthi5e3:ptrd1:xi1ee7:by hashd2:aad1:xi2eee"))
	f.Fuzz(func(t *testing.T, data []byte) {
		var s sample
		if err := Unmarshal(data, &s); err != nil {
			return
		}
		b, err := Marshal(s)
		if err != nil {
			t.Fatalf("re-encode %+v: %v", s, err)
		}
		var s2 sample
		if err := Unmarshal(b, &s2); err != nil {
			t.Fatalf("decode re-encoding %q: %v", b, err)
		}
	})
}

// FuzzDecoder checks that the io.Reader and byte-slice decoders agree on
// a sequence of values.
func FuzzDecoder(f *testing.F) {
	for _, s := range fuzzSeeds {
		f.Add([]byte(s + s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		rd := NewDecoder(bytes.NewReader(data))
		bd := NewBytesDecoder(data)
		for i := 0; i < 8; i++ {
			var a, b any
			ea := rd.Decode(&a)
			eb := bd.Decode(&b)
			if (ea == nil) != (eb == nil) {
				t.Fatalf("reader %v, bytes %v on %q", ea, eb, data)
			}
			if ea != nil {
				return
			}
			if !reflect.DeepEqual(a, b) || rd.InputOffset() != bd.InputOffset() {
				t.Fatalf("reader %#v at %d, bytes %#v at %d", a, rd.InputOffset(), b, bd.InputOffset())
			}
		}
	})
}

// FuzzLookup checks that Lookup never panics and that what it finds is a
// valid value inside the input.
func FuzzLookup(f *testing.F) {
	for _, s := range fuzzSeeds {
		f.Add([]byte(s), "info", "name")
	}
	f.Fuzz(func(t *testing.T, data []byte, k1, k2 string) {
		v, err := Lookup(data, k1, k2)
		if err == nil && !Valid(v) {
			t.Fatalf("Lookup returned invalid %q", v)
		}
	})
}

func isTypeError(err error) bool {
	var te *UnmarshalTypeError
	return errors.As(err, &te)
}

package bitio

import (
	"bytes"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	w := NewWriter([]byte{0xaa})
	w.Bits(5, 3)
	w.Flag(true)
	w.Flag(false)
	w.UE(0)
	w.UE(7)
	w.UE(0xfffffffe)
	w.SE(0)
	w.SE(-5)
	w.SE(6)
	w.SE(-2147483647)
	w.Bits(0x123456789, 36)
	b := w.Bytes()
	if b[0] != 0xaa {
		t.Fatal("dst prefix lost")
	}
	r := NewReader(b[1:])
	if v := r.Bits(3); v != 5 {
		t.Fatal(v)
	}
	if !r.Flag() || r.Flag() {
		t.Fatal("flags")
	}
	if r.UE() != 0 || r.UE() != 7 || r.UE() != 0xfffffffe {
		t.Fatal("ue")
	}
	if r.SE() != 0 || r.SE() != -5 || r.SE() != 6 || r.SE() != -2147483647 {
		t.Fatal("se")
	}
	if v := r.Bits64(36); v != 0x123456789 {
		t.Fatalf("%x", v)
	}
	if r.Err() != nil || r.Left() >= 8 {
		t.Fatal(r.Err(), r.Left())
	}
	pos := r.Pos()
	r.Skip(r.Left())
	if r.Err() != nil || r.Pos() < pos {
		t.Fatal("skip")
	}
	if r.Bit() != 0 || r.Err() != ErrShort {
		t.Fatal("read past end")
	}
}

func TestErrors(t *testing.T) {
	r := NewReader([]byte{0, 0, 0, 0, 0xff})
	if r.UE() != 0 || r.Err() != ErrGolomb {
		t.Fatal(r.Err())
	}
	r = NewReader([]byte{0})
	if r.UE() != 0 || r.Err() != ErrShort {
		t.Fatal(r.Err())
	}
	r = NewReader([]byte{0xff})
	if r.Bits(9) != 0 || r.Err() != ErrShort {
		t.Fatal(r.Err())
	}
	r = NewReader([]byte{0xff})
	r.Skip(-1)
	if r.Err() != ErrShort || r.Left() != 0 {
		t.Fatal(r.Err())
	}
	// The first error is kept.
	r.fail(ErrGolomb)
	if r.Err() != ErrShort {
		t.Fatal(r.Err())
	}
	if w := NewWriter(nil); !bytes.Equal(w.Bytes(), nil) {
		t.Fatal("empty writer")
	}
}

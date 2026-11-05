//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package websocket

import (
	"bytes"
	"crypto/rand"
	"testing"
	"testing/cryptotest"
)

// A client's masking keys come from crypto/rand, as RFC 6455 section 5.3
// asks: with crypto/rand made deterministic, the keys of two frames are the
// first eight bytes it gives.
func TestClientMaskFromCryptoRand(t *testing.T) {
	cryptotest.SetGlobalRandom(t, 1)
	var want [8]byte
	_, _ = rand.Read(want[:])
	cryptotest.SetGlobalRandom(t, 1)

	transport := &fakeTransport{}
	c := &Connection{conn: transport, client: true}
	for range 2 {
		if err := c.sendMaskedFrame(Binary, []byte("payload"), false); err != nil {
			t.Fatal(err)
		}
	}
	var got []byte
	for _, frame := range transport.frames {
		// A 7-byte payload: two header bytes, then the key.
		got = append(got, frame[2:6]...)
	}
	if !bytes.Equal(got, want[:]) {
		t.Fatalf("masking keys %x, want %x from crypto/rand", got, want)
	}
}

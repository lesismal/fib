package chacha20poly1305

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The AEAD seals the test vector of RFC 8439 section 2.8.2 to its ciphertext
// and tag, and opens it back.
func TestRFC8439Vector(t *testing.T) {
	key := unhex(t, "808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f")
	nonce := unhex(t, "070000004041424344454647")
	ad := unhex(t, "50515253c0c1c2c3c4c5c6c7")
	plain := []byte("Ladies and Gentlemen of the class of '99: If I could offer you only one tip for the future, sunscreen would be it.")
	aead, err := New(key)
	if err != nil {
		t.Fatal(err)
	}
	sealed := aead.Seal(nil, nonce, plain, ad)
	if tag := sealed[len(sealed)-TagSize:]; !bytes.Equal(tag, unhex(t, "1ae10b594f09e26a7e902ecbd0600691")) {
		t.Fatalf("tag %x", tag)
	}
	if !bytes.HasPrefix(sealed, unhex(t, "d31a8d34648e60db7b86afbc53ef7ec2")) {
		t.Fatalf("ciphertext %x", sealed[:16])
	}
	opened, err := aead.Open(nil, nonce, sealed, ad)
	if err != nil || !bytes.Equal(opened, plain) {
		t.Fatalf("opened %q, %v", opened, err)
	}
	// In place, as the record layers open.
	if opened, err = aead.Open(sealed[:0], nonce, sealed, ad); err != nil || !bytes.Equal(opened, plain) {
		t.Fatalf("opened in place %q, %v", opened, err)
	}
	if aead.NonceSize() != NonceSize || aead.Overhead() != TagSize {
		t.Fatalf("nonce size %d, overhead %d", aead.NonceSize(), aead.Overhead())
	}
}

// What was altered, or is shorter than a tag, does not open.
func TestOpenRefusals(t *testing.T) {
	aead, err := New(make([]byte, KeySize))
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, NonceSize)
	sealed := aead.Seal(nil, nonce, []byte("hello"), nil)
	sealed[0] ^= 1
	if _, err := aead.Open(nil, nonce, sealed, nil); err == nil {
		t.Fatal("an altered ciphertext opened")
	}
	if _, err := aead.Open(nil, nonce, sealed[:TagSize-1], nil); err == nil {
		t.Fatal("a ciphertext shorter than a tag opened")
	}
	if _, err := New(make([]byte, KeySize-1)); err == nil {
		t.Fatal("a short key was taken")
	}
}

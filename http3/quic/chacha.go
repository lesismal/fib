package quic

import (
	"crypto/cipher"

	"github.com/lesismal/fib/internal/chacha20poly1305"
)

// TLS 1.3 may settle on TLS_CHACHA20_POLY1305_SHA256, and QUIC then protects
// packets and headers with ChaCha20 (RFC 9001 section 5.4.4). The standard
// library keeps its implementation internal, and its exported AEADs are AES
// only, so this is the one of RFC 8439, in internal/chacha20poly1305, which the
// TLS record layer uses too: these are QUIC's names for it and the block
// function it uses for header protection.

const (
	chachaKeySize   = chacha20poly1305.KeySize
	chachaNonceSize = chacha20poly1305.NonceSize
	poly1305TagSize = chacha20poly1305.TagSize
)

func newChaCha20Poly1305(key []byte) (cipher.AEAD, error) {
	return chacha20poly1305.New(key)
}

func chachaBlock(out *[64]byte, key *[8]uint32, counter uint32, nonce *[3]uint32) {
	chacha20poly1305.Block(out, key, counter, nonce)
}

func chachaKey(key []byte) (k [8]uint32) { return chacha20poly1305.KeyOf(key) }

func chachaNonce(nonce []byte) (n [3]uint32) { return chacha20poly1305.NonceOf(nonce) }

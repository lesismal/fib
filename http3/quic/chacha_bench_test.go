package quic

import (
	"crypto/rand"
	"fmt"
	"testing"
)

// BenchmarkChaCha20Poly1305 measures the AEAD of RFC 8439 the way QUIC uses
// it: a whole packet sealed and opened, a nonce of twelve bytes and the
// header as additional data. A packet-sized plaintext is where the pure Go
// implementation matters most, and a large one shows how it streams.
func BenchmarkChaCha20Poly1305(b *testing.B) {
	key := make([]byte, chachaKeySize)
	_, _ = rand.Read(key)
	aead, err := newChaCha20Poly1305(key)
	if err != nil {
		b.Fatal(err)
	}
	nonce := make([]byte, chachaNonceSize)
	aad := make([]byte, 20)
	_, _ = rand.Read(aad)
	for _, size := range []int{1200, 16 << 10} {
		plain := make([]byte, size)
		_, _ = rand.Read(plain)
		sealed := aead.Seal(nil, nonce, plain, aad)
		b.Run(fmt.Sprintf("%dB/seal", size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			out := make([]byte, 0, size+aead.Overhead())
			for i := 0; i < b.N; i++ {
				out = aead.Seal(out[:0], nonce, plain, aad)
			}
		})
		b.Run(fmt.Sprintf("%dB/open", size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			out := make([]byte, 0, size)
			for i := 0; i < b.N; i++ {
				var err error
				out, err = aead.Open(out[:0], nonce, sealed, aad)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	// Header protection takes one ChaCha20 block of a 16-byte sample, once
	// for every packet sent and every packet received.
	b.Run("header protection", func(b *testing.B) {
		hp := chachaHeaderProtector{key: chachaKey(key)}
		sample := make([]byte, 16)
		_, _ = rand.Read(sample)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if hp.mask(sample) == [5]byte{} {
				b.Fatal("a zero mask is not one a key would give")
			}
		}
	})
}

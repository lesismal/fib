//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"strings"
	"testing"
	"time"
)

// A server certificate whose RSA key is larger than crypto/tls allows, 8192
// bits, is refused before anything is verified with it; one at the bound is
// not refused for its size. The keys stand for keys that size: nothing signs
// with them.
func TestClientRefusesOversizedServerRSAKey(t *testing.T) {
	ca := newTestCA(t, "rsa size CA")
	certFor := func(bits int) []byte {
		// An odd modulus of exactly bits bits; finding primes that large
		// takes too long, and nothing here needs them.
		n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), uint(bits)))
		if err != nil {
			t.Fatal(err)
		}
		n.SetBit(n, bits-1, 1)
		n.SetBit(n, 0, 1)
		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial.Add(1)),
			Subject:      pkix.Name{CommonName: "localhost"},
			DNSNames:     []string{"localhost"},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
		der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &rsa.PublicKey{N: n, E: 65537}, ca.key)
		if err != nil {
			t.Fatal(err)
		}
		return der
	}
	for _, tc := range []struct {
		bits    int
		refused bool
	}{{maxRSAKeySize, false}, {maxRSAKeySize + 8, true}} {
		hs := newHS(t, ca.pool, nil)
		var out []byte
		herr := hs.acceptCertificates([][]byte{certFor(tc.bits)}, &out)
		tooLarge := herr != nil && strings.Contains(herr.Error(), "larger than")
		if tooLarge != tc.refused {
			t.Errorf("%d bits: %v", tc.bits, herr)
		}
		if tc.refused && herr.alert != alertBadCertificate {
			t.Errorf("%d bits: alert %d", tc.bits, herr.alert)
		}
	}
}

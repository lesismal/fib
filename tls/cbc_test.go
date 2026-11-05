//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	stdtls "crypto/tls"
	"slices"
	"testing"
)

// The CBC suites with SHA-256 are neither offered nor served unless
// Config.CipherSuites names them, as crypto/tls's defaults leave them out.
func TestCBCSHA256OnlyWhenNamed(t *testing.T) {
	sha256Suites := []uint16{stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256, stdtls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256}
	offered := suites12(&stdtls.Config{})
	if slices.ContainsFunc(offered, cbcSHA256) {
		t.Fatalf("offered by default: %x", offered)
	}
	if !slices.Contains(offered, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA) {
		t.Fatalf("the CBC suites with SHA-1 are no longer offered: %x", offered)
	}
	if got := suites12(&stdtls.Config{CipherSuites: sha256Suites}); !slices.Equal(got, sha256Suites) {
		t.Fatalf("named, offered %x", got)
	}

	srv := newSrvHS(&stdtls.Config{}, nil)
	for _, id := range sha256Suites {
		if srv.allowsSuite12(id) {
			t.Fatalf("%x served by default", id)
		}
	}
	if !srv.allowsSuite12(stdtls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA) {
		t.Fatal("a CBC suite with SHA-1 is no longer served")
	}
	srv = newSrvHS(&stdtls.Config{CipherSuites: sha256Suites}, nil)
	if !srv.allowsSuite12(sha256Suites[0]) {
		t.Fatal("not served when named")
	}

	// A client handshake with the defaults leaves them out of its hello.
	_, pool := issueCert(t, keyOf(t, "ecdsa-p256"))
	hs := newHS12(t, pool, func(c *stdtls.Config) { c.MaxVersion = stdtls.VersionTLS12 })
	if slices.ContainsFunc(hs.suites12, cbcSHA256) {
		t.Fatalf("a default client offers %x", hs.suites12)
	}
}

// The record MAC of the CBC suites with SHA-1 sums in constant time, as
// crypto/tls's does, and agrees with HMAC-SHA1.
func TestCBCSHA1MACConstantTime(t *testing.T) {
	s, ok := lookupSuite(stdtls.VersionTLS12, stdtls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA)
	if !ok {
		t.Fatal("no suite")
	}
	if _, ok := sha1.New().(constantTimeHash); ok {
		if _, ok := s.mac().(*constantTimeSum); !ok {
			t.Fatalf("the SHA-1 record MAC is %T, not a constant-time sum", s.mac())
		}
	}
	key := bytes.Repeat([]byte{7}, 20)
	for _, n := range []int{0, 1, 55, 56, 63, 64, 65, 1000} {
		data := bytes.Repeat([]byte{byte(n)}, n)
		want := hmac.New(sha1.New, key)
		want.Write(data)
		got := hmac.New(s.mac, key)
		got.Write(data)
		if !bytes.Equal(got.Sum(nil), want.Sum(nil)) {
			t.Fatalf("%d bytes: the MACs differ", n)
		}
	}
}

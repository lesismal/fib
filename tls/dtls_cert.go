package tls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	stdtls "crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"math/big"
	"strings"
	"time"
)

// Certificates for WebRTC, whose peers know each other's by the fingerprint
// their SDP carries (RFC 8122, RFC 8827 section 6.5) rather than by a chain
// to a root.

// CertificateFingerprint is the SHA-256 fingerprint of a DER certificate as
// an SDP fingerprint attribute writes it after its hash name, "sha-256":
// upper-case hex bytes joined by colons (RFC 8122 section 5).
func CertificateFingerprint(der []byte) string {
	sum := sha256.Sum256(der)
	h := strings.ToUpper(hex.EncodeToString(sum[:]))
	var b strings.Builder
	b.Grow(len(sum)*3 - 1)
	for i := 0; i < len(h); i += 2 {
		if i > 0 {
			b.WriteByte(':')
		}
		b.WriteString(h[i : i+2])
	}
	return b.String()
}

// GenerateWebRTCCertificate makes a self-signed certificate with a new ECDSA
// P-256 key, as browsers make for their RTCPeerConnections: the key every
// WebRTC stack accepts and signs fastest with, valid from a day ago, for
// clocks that are behind, for thirty days. Its subject is a random name,
// which nothing checks.
func GenerateWebRTCCertificate() (stdtls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return stdtls.Certificate{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return stdtls.Certificate{}, err
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "WebRTC " + serial.Text(16)},
		NotBefore:    now.Add(-24 * time.Hour),
		NotAfter:     now.Add(30 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return stdtls.Certificate{}, err
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return stdtls.Certificate{}, err
	}
	return stdtls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}, nil
}

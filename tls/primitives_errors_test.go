//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	stdtls "crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// The signature checks of both versions refuse a scheme that does not go with
// the peer's key, an unknown scheme, and a signature that does not verify,
// whichever key type it is.
func TestSignatureChecksRefuseMismatches(t *testing.T) {
	ecKey, rsaKey, edKey := keyOf(t, "ecdsa-p256"), keyOf(t, "rsa"), keyOf(t, "ed25519")
	ec, rsaPub, ed := ecKey.Public(), rsaKey.Public(), edKey.Public()
	signed := []byte("what the handshake signs")
	for _, tc := range []struct {
		name  string
		alg   uint16
		pub   crypto.PublicKey
		sig   []byte
		alert uint8
	}{
		{"RSA-PSS with an ECDSA key", 0x0804, ec, nil, alertIllegalParameter},
		{"ECDSA with an RSA key", 0x0403, rsaPub, nil, alertIllegalParameter},
		{"ECDSA P-384 scheme with a P-256 key", 0x0503, ec, nil, alertIllegalParameter},
		{"ECDSA that does not verify", 0x0403, ec, []byte{1, 2, 3}, alertDecryptError},
		{"Ed25519 with an ECDSA key", 0x0807, ec, nil, alertIllegalParameter},
		{"Ed25519 that does not verify", 0x0807, ed, make([]byte, 64), alertDecryptError},
		{"RSA-PSS that does not verify", 0x0804, rsaPub, make([]byte, 256), alertDecryptError},
		{"PKCS #1, which TLS 1.3 does not sign with", 0x0401, rsaPub, nil, alertIllegalParameter},
	} {
		herr := verify13(tc.alg, tc.pub, signed, tc.sig, "server")
		if herr == nil || herr.alert != tc.alert {
			t.Errorf("TLS 1.3 %s: %v, want alert %d", tc.name, herr, tc.alert)
		}
	}

	good := func(signer crypto.Signer, alg uint16) []byte {
		sig, err := signHandshake(signer, alg, rand.Reader, signed)
		if err != nil {
			t.Fatal(err)
		}
		return sig
	}
	for _, tc := range []struct {
		name string
		alg  uint16
		pub  crypto.PublicKey
		sig  []byte
		want error
	}{
		{"an unknown scheme", 0x0999, ec, nil, errUnsupportedSignature},
		{"Ed25519 with an ECDSA key", 0x0807, ec, nil, errBadSignature},
		{"PKCS #1 with an ECDSA key", 0x0401, ec, nil, errUnsupportedSignature},
		{"RSA-PSS with an ECDSA key", 0x0804, ec, nil, errUnsupportedSignature},
		{"ECDSA with an RSA key", 0x0403, rsaPub, nil, errUnsupportedSignature},
		{"ECDSA that does not verify", 0x0403, ec, []byte{1}, errBadSignature},
		{"PKCS #1 that verifies", 0x0401, rsaPub, good(rsaKey, 0x0401), nil},
		{"Ed25519 that verifies", 0x0807, ed, good(edKey, 0x0807), nil},
	} {
		if err := verifySignature12(tc.alg, tc.pub, signed, tc.sig); !errors.Is(err, tc.want) {
			t.Errorf("TLS 1.2 %s: %v, want %v", tc.name, err, tc.want)
		}
	}
	if errBadSignature.Error() == "" || errUnsupportedSignature.Error() == "" {
		t.Fatal("a signature error has no text")
	}
}

// A P-521 key signs with its own scheme, and a key no scheme the peer accepts
// goes with is refused whichever side's order is followed.
func TestSignatureSchemeChoice(t *testing.T) {
	p521, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if got := signatureSchemes(p521.Public(), false); len(got) != 1 || got[0] != 0x0603 {
		t.Fatalf("P-521 schemes %x", got)
	}
	ec := keyOf(t, "ecdsa-p256").Public()
	for _, ours := range []bool{true, false} {
		if alg, ok := chooseSignature(ec, []uint16{0x0807, 0x0804}, false, ours); ok {
			t.Fatalf("ours %v: chose %x for an ECDSA key the peer cannot verify", ours, alg)
		}
	}
}

// A certificate a callback returns must have a chain and a key that signs.
func TestUsableCertificate(t *testing.T) {
	if herr := usableCertificate(&stdtls.Certificate{PrivateKey: keyOf(t, "ed25519")}); herr == nil {
		t.Fatal("a certificate without a chain was used")
	}
	if herr := usableCertificate(&stdtls.Certificate{Certificate: [][]byte{{1}}, PrivateKey: "not a key"}); herr == nil {
		t.Fatal("a certificate whose key cannot sign was used")
	}
	err := error(fail(alertInternalError, "boom %d", 7))
	if !strings.Contains(err.Error(), "boom 7") {
		t.Fatalf("hsError text %q", err.Error())
	}
}

// recordPair is a writer's keys and a reader's for the same direction.
func recordPair(t *testing.T, version, id uint16) (seal, open *recordKeys) {
	t.Helper()
	s, ok := lookupSuite(version, id)
	if !ok {
		t.Fatalf("no suite %x", id)
	}
	if version == stdtls.VersionTLS13 {
		secret := bytes.Repeat([]byte{9}, s.hash().Size())
		seal, _ = newKeys13(s, s.hash, secret)
		open, _ = newKeys13(s, s.hash, secret)
		return seal, open
	}
	master, cr, sr := bytes.Repeat([]byte{1}, 48), bytes.Repeat([]byte{2}, 32), bytes.Repeat([]byte{3}, 32)
	seal, _, err := newKeysFromMaster(version, s, master, cr, sr)
	if err != nil {
		t.Fatal(err)
	}
	open, _, _ = newKeysFromMaster(version, s, master, cr, sr)
	return seal, open
}

// Records that are not what their keys make are refused with the alert
// crypto/tls sends for them: a wrong version, a wrong outer type, a body too
// short or too long, a TLS 1.3 record that is all padding, a CBC body that is
// not whole blocks.
func TestRecordOpenRefusals(t *testing.T) {
	expect := func(name string, k *recordKeys, record []byte, alert uint8) {
		t.Helper()
		if _, _, got, ok := k.open(record); ok || got != alert {
			t.Errorf("%s: ok %v alert %d, want %d", name, ok, got, alert)
		}
	}
	seal13, open13 := recordPair(t, stdtls.VersionTLS13, stdtls.TLS_AES_128_GCM_SHA256)
	rec, _ := seal13.seal(nil, recordTypeApplicationData, []byte("x"), nil)
	bad := bytes.Clone(rec)
	bad[2] = 1
	expect("TLS 1.3 wrong version", open13, bad, alertProtocolVersion)
	bad = bytes.Clone(rec)
	bad[0] = recordTypeHandshake
	expect("TLS 1.3 wrong outer type", open13, bad, alertUnexpectedMessage)
	expect("TLS 1.3 short body", open13, appendRecord(nil, recordTypeApplicationData, 0x0303, make([]byte, gcmTagLen)), alertBadRecordMAC)
	// All padding, with no content type: sealed as type zero with nothing.
	seal13, open13 = recordPair(t, stdtls.VersionTLS13, stdtls.TLS_AES_128_GCM_SHA256)
	rec, _ = seal13.seal(nil, 0, nil, []byte{0, 0})
	expect("TLS 1.3 all padding", open13, rec, alertUnexpectedMessage)
	seal13, open13 = recordPair(t, stdtls.VersionTLS13, stdtls.TLS_AES_128_GCM_SHA256)
	rec, _ = seal13.seal(nil, recordTypeApplicationData, make([]byte, maxPlaintext+1), nil)
	expect("TLS 1.3 overflow", open13, rec, alertRecordOverflow)

	seal12, open12 := recordPair(t, stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256)
	expect("TLS 1.2 GCM short body", open12, appendRecord(nil, recordTypeApplicationData, 0x0303, make([]byte, 10)), alertBadRecordMAC)
	expect("TLS 1.2 GCM bad tag", open12, appendRecord(nil, recordTypeApplicationData, 0x0303, make([]byte, 40)), alertBadRecordMAC)
	rec, _ = seal12.seal(nil, recordTypeApplicationData, make([]byte, maxPlaintext+1), nil)
	expect("TLS 1.2 GCM overflow", open12, rec, alertRecordOverflow)

	sealCBC, openCBC := recordPair(t, stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA)
	expect("CBC partial block", openCBC, appendRecord(nil, recordTypeApplicationData, 0x0303, make([]byte, 50)), alertBadRecordMAC)
	rec, _ = sealCBC.seal(nil, recordTypeApplicationData, make([]byte, maxPlaintext+1), nil)
	expect("CBC overflow", openCBC, rec, alertRecordOverflow)

	if n, good := extractPadding(nil); n != 0 || good != 0 {
		t.Fatalf("empty payload: padding %d, good %d", n, good)
	}
	if seal13.authenticates([]byte{1, 2}, 0) {
		t.Fatal("a fragment of a header authenticated")
	}
}

// sealTicket seals a ticket's plaintext as it is, for the parsers to refuse.
func sealTicket(t *testing.T, keys *ticketKeys, plain []byte) []byte {
	t.Helper()
	ticket, ok := keys.sealRaw(plain, time.Now())
	if !ok {
		t.Fatal("ticket not sealed")
	}
	return ticket
}

// Tickets whose plaintext is cut short or runs on, which only a holder of the
// key could make, are refused rather than read past their end.
func TestTicketParseRefusals(t *testing.T) {
	keys := newTicketKeys(&stdtls.Config{})
	now := time.Now()
	head13 := func(pskLen int) []byte {
		p := []byte{1}
		p = binary.BigEndian.AppendUint16(p, stdtls.TLS_AES_128_GCM_SHA256)
		p = binary.BigEndian.AppendUint64(p, uint64(now.Unix()))
		p = binary.BigEndian.AppendUint32(p, 0)
		return append(p, byte(pskLen))
	}
	withName := func(p []byte) []byte { return append(p, 0, 0) }
	for _, tc := range []struct {
		name  string
		plain []byte
	}{
		{"psk cut short", append(head13(32), make([]byte, 10)...)},
		{"no server name length", append(head13(2), 7, 7)},
		{"server name cut short", append(head13(0), 0, 9, 'a')},
		{"early data fields cut short", append(withName(head13(0)), 1)},
		{"protocol cut short", append(withName(head13(0)), 1, 5, 'h')},
		{"extra count without its length", append(withName(head13(0)), 1, 0, 1)},
		{"extra cut short", append(withName(head13(0)), 1, 0, 1, 0, 9, 1)},
		{"certificates that do not parse", append(withName(head13(0)), 1, 0, 0, 1, 0, 0, 9)},
		{"no certificates", append(withName(head13(0)), 1, 0, 0, 0, 0)},
	} {
		if _, ok := keys.open(sealTicket(t, keys, tc.plain), now); ok {
			t.Errorf("TLS 1.3 ticket with %s was opened", tc.name)
		}
	}

	head12 := func(masterLen int) []byte {
		p := []byte{2}
		p = binary.BigEndian.AppendUint16(p, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256)
		p = binary.BigEndian.AppendUint64(p, uint64(now.Unix()))
		return append(p, 1, byte(masterLen))
	}
	for _, tc := range []struct {
		name  string
		plain []byte
	}{
		{"master cut short", append(head12(48), make([]byte, 10)...)},
		{"server name cut short", append(head12(0), 0, 9, 'a')},
		{"certificates that do not parse", append(head12(0), 0, 0, 2, 0)},
	} {
		if _, ok := keys.open12(sealTicket(t, keys, tc.plain), now); ok {
			t.Errorf("TLS 1.2 ticket with %s was opened", tc.name)
		}
	}
	if _, ok := keys.openRaw([]byte{1, 2, 3}); ok {
		t.Fatal("a ticket shorter than a key name was opened")
	}

	// More certificates than a ticket counts are not sealed into one.
	many := make([][]byte, 256)
	for i := range many {
		many[i] = []byte{1}
	}
	if _, ok := keys.seal(ticketState{certs: many}, now); ok {
		t.Fatal("a TLS 1.3 ticket with 256 certificates was sealed")
	}
	if _, ok := keys.seal12(ticketState12{certs: many}, now); ok {
		t.Fatal("a TLS 1.2 ticket with 256 certificates was sealed")
	}
}

// Keys that can no longer open a live ticket are dropped when a new key is
// made, and the register of tickets that carried early data, once full, makes
// room by dropping the expired ones, or else takes no more.
func TestTicketKeyRotationAndRegister(t *testing.T) {
	keys := newTicketKeys(&stdtls.Config{})
	start := time.Now()
	old := keys.current(start)
	later := start.Add(ticketLifetime + 2*ticketKeyLife + time.Hour)
	if k := keys.current(later); k == old || len(keys.keys) != 1 {
		t.Fatalf("%d keys after rotation, the old one kept: %v", len(keys.keys), k == old)
	}

	defer func(n int) { maxUsedTickets = n }(maxUsedTickets)
	maxUsedTickets = 64
	keys.used = make(map[[32]byte]time.Time, maxUsedTickets)
	expired := start.Add(-time.Hour)
	var key [32]byte
	for i := 0; i < maxUsedTickets; i++ {
		binary.BigEndian.PutUint32(key[:], uint32(i))
		keys.used[key] = expired
	}
	if !keys.firstUse([]byte("ticket"), start) {
		t.Fatal("a full register of expired tickets took no new one")
	}
	if len(keys.used) != 1 {
		t.Fatalf("%d tickets left in the register", len(keys.used))
	}
	live := start.Add(time.Hour)
	for i := 0; len(keys.used) < maxUsedTickets; i++ {
		key = sha256.Sum256(binary.BigEndian.AppendUint32(nil, uint32(i)))
		keys.used[key] = live
	}
	if keys.firstUse([]byte("another ticket"), start) {
		t.Fatal("a register full of live tickets took another")
	}
}

// The session cache keeps no more than it may, dropping the session used
// least recently, and a capacity below one means the default.
func TestQUICSessionCacheBounds(t *testing.T) {
	c := NewQUICSessionCache(2)
	for _, key := range []string{"a", "b", "c"} {
		c.Put(key, &QUICSession{})
	}
	if c.Get("a") != nil || c.Get("b") == nil || c.Get("c") == nil {
		t.Fatal("the least recently used session was not the one dropped")
	}
	c.Put("missing", nil)
	if c.Get("missing") != nil {
		t.Fatal("removing a key that is not there added it")
	}
	if l, ok := NewQUICSessionCache(0).(*lruSessions); !ok || l.cap != 64 {
		t.Fatal("a capacity of zero is not the default")
	}
}

// Small pieces with a case each of their own: a name in brackets or with a
// zone, and a name with a trailing dot, as the ClientHello carries them; a
// field read past the end of a message; a suite QUIC does not know.
func TestHandshakeHelpers(t *testing.T) {
	for in, want := range map[string]string{
		"[::1]":        "",
		"fe80::1%eth0": "",
		"example.com.": "example.com",
		"example.com":  "example.com",
	} {
		if got := hostnameInSNI(in); got != want {
			t.Errorf("hostnameInSNI(%q) = %q, want %q", in, got, want)
		}
	}
	r := newReader(nil)
	if r.u8() != 0 || r.u16() != 0 || r.u24() != 0 || r.u32() != 0 || r.ok {
		t.Fatal("reads past the end succeeded")
	}
	if _, ok := quicSuite(0x1234); ok {
		t.Fatal("an unknown suite is QUIC's")
	}
	if _, ok := lookupSuite(stdtls.VersionTLS13, 0x1234); ok {
		t.Fatal("an unknown suite was found")
	}
	if wouldBlockError.Error(wouldBlockError{}) == "" || (wouldBlockError{}).Timeout() {
		t.Fatal("errWouldBlock is not a temporary error that is not a timeout")
	}
	if err := quicError(&hsError{alert: alertInternalError}); !errors.Is(err, stdtls.AlertError(alertInternalError)) {
		t.Fatalf("an alert without a cause is %v", err)
	}
	if err := quicError(&hsError{err: io.ErrUnexpectedEOF}); err != io.ErrUnexpectedEOF {
		t.Fatalf("a failure without an alert is %v", err)
	}
	if got := cmpOr(uint32(5), 16); got != 5 {
		t.Fatalf("cmpOr kept the default over %d", got)
	}
}

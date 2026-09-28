package tls

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	stdtls "crypto/tls"
	"encoding/binary"
	"hash"

	"github.com/lesismal/fib/bufferpool"
)

// The record layer an established connection runs on once it leaves
// crypto/tls; see fast.go. It protects application data with AES-GCM, for TLS
// 1.3 (RFC 8446 section 5) and for TLS 1.2 (RFC 5288), the suites every
// peer offers first. Other suites stay with crypto/tls.

const (
	recordHeaderLen = 5
	// maxPlaintext is the most a record carries, and what Send puts in each.
	maxPlaintext = 16 << 10
	// maxCiphertext12 and maxCiphertext13 bound a record's body: TLS 1.2
	// allows 2048 bytes of expansion (RFC 5246 section 6.2.3), TLS 1.3 256
	// (RFC 8446 section 5.2).
	maxCiphertext12 = maxPlaintext + 2048
	maxCiphertext13 = maxPlaintext + 256
	// explicitNonceLen is the part of a TLS 1.2 GCM nonce each record carries.
	explicitNonceLen = 8
	gcmTagLen        = 16

	recordTypeChangeCipherSpec = 20
	recordTypeAlert            = 21
	recordTypeHandshake        = 22
	recordTypeApplicationData  = 23

	typeNewSessionTicket = 4
	typeKeyUpdate        = 24

	alertLevelWarning = 1
	alertLevelError   = 2

	alertCloseNotify       = 0
	alertUnexpectedMessage = 10
	alertBadRecordMAC      = 20
	alertRecordOverflow    = 22
	alertProtocolVersion   = 70
	alertNoRenegotiation   = 100
	alertIllegalParameter  = 47
)

// suiteParams reports the key length and hash of an AES-GCM suite of
// version, or false for any other.
func suiteParams(version, suite uint16) (keyLen int, h func() hash.Hash, ok bool) {
	switch version {
	case stdtls.VersionTLS13:
		switch suite {
		case stdtls.TLS_AES_128_GCM_SHA256:
			return 16, sha256.New, true
		case stdtls.TLS_AES_256_GCM_SHA384:
			return 32, sha512.New384, true
		}
	case stdtls.VersionTLS12:
		switch suite {
		case stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, stdtls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			stdtls.TLS_RSA_WITH_AES_128_GCM_SHA256:
			return 16, sha256.New, true
		case stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, stdtls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			stdtls.TLS_RSA_WITH_AES_256_GCM_SHA384:
			return 32, sha512.New384, true
		}
	}
	return 0, nil, false
}

// recordKeys protects one direction of a connection's records.
type recordKeys struct {
	aead cipher.AEAD
	// iv is TLS 1.3's per-record nonce base, or TLS 1.2's four-byte salt
	// followed by room for the explicit part.
	iv  [12]byte
	seq uint64
	// secret and hash derive the next generation of TLS 1.3 keys, when the
	// peer updates its own or asks for ours to be.
	secret []byte
	hash   func() hash.Hash
	keyLen int
	tls13  bool
	// nonce and ad are scratch space, which the AEAD, an interface, would
	// otherwise move to the heap on every record.
	nonce [12]byte
	ad    [13]byte
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// newKeys13 derives the keys of a TLS 1.3 traffic secret.
func newKeys13(keyLen int, h func() hash.Hash, secret []byte) (*recordKeys, error) {
	key, err := expandLabel(h, secret, "key", keyLen)
	if err != nil {
		return nil, err
	}
	iv, err := expandLabel(h, secret, "iv", 12)
	if err != nil {
		return nil, err
	}
	aead, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	k := &recordKeys{aead: aead, secret: secret, hash: h, keyLen: keyLen, tls13: true}
	copy(k.iv[:], iv)
	return k, nil
}

// next is the generation of keys a KeyUpdate moves to (RFC 8446 section
// 7.2), with its sequence number back at zero.
func (k *recordKeys) next() (*recordKeys, error) {
	secret, err := expandLabel(k.hash, k.secret, "traffic upd", k.hash().Size())
	if err != nil {
		return nil, err
	}
	return newKeys13(k.keyLen, k.hash, secret)
}

// expandLabel is HKDF-Expand-Label (RFC 8446 section 7.1) with an empty
// context.
func expandLabel(h func() hash.Hash, secret []byte, label string, length int) ([]byte, error) {
	info := make([]byte, 0, 4+6+len(label))
	info = binary.BigEndian.AppendUint16(info, uint16(length))
	info = append(info, byte(6+len(label)))
	info = append(info, "tls13 "...)
	info = append(info, label...)
	info = append(info, 0)
	return hkdf.Expand(h, secret, string(info), length)
}

// newKeys12 derives both directions' keys from a TLS 1.2 master secret
// (RFC 5246 section 6.3, RFC 5288 section 3), client's first.
func newKeys12(keyLen int, h func() hash.Hash, master, clientRandom, serverRandom []byte) (client, server *recordKeys, err error) {
	seed := make([]byte, 0, 64)
	seed = append(append(seed, serverRandom...), clientRandom...)
	block := prf12(h, master, "key expansion", seed, 2*keyLen+2*4)
	clientKey, block := block[:keyLen], block[keyLen:]
	serverKey, block := block[:keyLen], block[keyLen:]
	clientSalt, serverSalt := block[:4], block[4:8]
	if client, err = newKeys12Half(clientKey, clientSalt); err != nil {
		return nil, nil, err
	}
	if server, err = newKeys12Half(serverKey, serverSalt); err != nil {
		return nil, nil, err
	}
	return client, server, nil
}

func newKeys12Half(key, salt []byte) (*recordKeys, error) {
	aead, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	k := &recordKeys{aead: aead}
	copy(k.iv[:4], salt)
	return k, nil
}

// prf12 is the TLS 1.2 PRF, P_hash over HMAC (RFC 5246 section 5).
func prf12(h func() hash.Hash, secret []byte, label string, seed []byte, length int) []byte {
	labelSeed := append([]byte(label), seed...)
	mac := hmac.New(h, secret)
	mac.Write(labelSeed)
	a := mac.Sum(nil)
	out := make([]byte, 0, length+mac.Size())
	for len(out) < length {
		mac.Reset()
		mac.Write(a)
		mac.Write(labelSeed)
		out = mac.Sum(out)
		mac.Reset()
		mac.Write(a)
		a = mac.Sum(a[:0])
	}
	return out[:length]
}

// overhead is what sealing adds to a record's plaintext, header included.
func (k *recordKeys) overhead() int {
	if k.tls13 {
		return recordHeaderLen + 1 + gcmTagLen
	}
	return recordHeaderLen + explicitNonceLen + gcmTagLen
}

// maxBody bounds a record's body, what its header's length may say.
func (k *recordKeys) maxBody() int {
	if k.tls13 {
		return maxCiphertext13
	}
	return maxCiphertext12
}

// seal appends a record of typ carrying a followed by b, at most maxPlaintext
// bytes together, to dst, which needs room for it: the payload is copied
// into place and encrypted there.
func (k *recordKeys) seal(dst []byte, typ byte, a, b []byte) []byte {
	start := len(dst)
	n := len(a) + len(b)
	outerType, bodyLen := typ, n+gcmTagLen
	if k.tls13 {
		outerType = recordTypeApplicationData
		bodyLen++
	} else {
		bodyLen += explicitNonceLen
	}
	dst = append(dst, outerType, 3, 3, byte(bodyLen>>8), byte(bodyLen))
	header := dst[start:]
	var ad []byte
	if k.tls13 {
		k.nonce = k.iv
		seqXOR(&k.nonce, k.seq)
		ad = header
	} else {
		copy(k.nonce[:4], k.iv[:4])
		binary.BigEndian.PutUint64(k.nonce[4:], k.seq)
		dst = append(dst, k.nonce[4:]...)
		ad = k.additionalData12(typ, n)
	}
	plainStart := len(dst)
	dst = append(append(dst, a...), b...)
	if k.tls13 {
		dst = append(dst, typ)
	}
	dst = k.aead.Seal(dst[:plainStart], k.nonce[:], dst[plainStart:], ad)
	k.seq++
	return dst
}

func (k *recordKeys) additionalData12(typ byte, n int) []byte {
	binary.BigEndian.PutUint64(k.ad[:8], k.seq)
	k.ad[8], k.ad[9], k.ad[10] = typ, 3, 3
	k.ad[11], k.ad[12] = byte(n>>8), byte(n)
	return k.ad[:]
}

func seqXOR(nonce *[12]byte, seq uint64) {
	for i := 0; i < 8; i++ {
		nonce[4+i] ^= byte(seq >> (56 - 8*i))
	}
}

// open decrypts record, a whole record, header included, in place, and
// returns its type and plaintext, or the alert to answer it with.
func (k *recordKeys) open(record []byte) (typ byte, plaintext []byte, alert uint8, ok bool) {
	header, body := record[:recordHeaderLen], record[recordHeaderLen:]
	if header[1] != 3 || header[2] != 3 {
		return 0, nil, alertProtocolVersion, false
	}
	if k.tls13 {
		if header[0] != recordTypeApplicationData {
			return 0, nil, alertUnexpectedMessage, false
		}
		if len(body) < gcmTagLen+1 {
			return 0, nil, alertBadRecordMAC, false
		}
		k.nonce = k.iv
		seqXOR(&k.nonce, k.seq)
		plain, err := k.aead.Open(body[:0], k.nonce[:], body, header)
		if err != nil {
			return 0, nil, alertBadRecordMAC, false
		}
		k.seq++
		// The inner type is the last byte that is not zero padding.
		i := len(plain) - 1
		for i >= 0 && plain[i] == 0 {
			i--
		}
		if i < 0 {
			return 0, nil, alertUnexpectedMessage, false
		}
		if i > maxPlaintext {
			return 0, nil, alertRecordOverflow, false
		}
		return plain[i], plain[:i], 0, true
	}
	if len(body) < explicitNonceLen+gcmTagLen {
		return 0, nil, alertBadRecordMAC, false
	}
	copy(k.nonce[:4], k.iv[:4])
	copy(k.nonce[4:], body[:explicitNonceLen])
	ciphertext := body[explicitNonceLen:]
	ad := k.additionalData12(header[0], len(ciphertext)-gcmTagLen)
	plain, err := k.aead.Open(ciphertext[:0], k.nonce[:], ciphertext, ad)
	if err != nil {
		return 0, nil, alertBadRecordMAC, false
	}
	k.seq++
	if len(plain) > maxPlaintext {
		return 0, nil, alertRecordOverflow, false
	}
	return header[0], plain, 0, true
}

// authenticates reports whether record was sealed with k at sequence number
// seq, without touching record or k's state beyond its scratch space.
func (k *recordKeys) authenticates(record []byte, seq uint64) bool {
	if len(record) < recordHeaderLen {
		return false
	}
	scratch := bufferpool.Join(nil, record)
	defer bufferpool.Put(scratch)
	saved := k.seq
	k.seq = seq
	_, _, _, ok := k.open(scratch)
	k.seq = saved
	return ok
}

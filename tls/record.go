package tls

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	stdtls "crypto/tls"
	"encoding/binary"
	"errors"
	"hash"

	"github.com/lesismal/fib/bufferpool"
)

// The record layer an established connection runs on once it leaves
// crypto/tls; see fast.go. It protects records with AES-GCM, for TLS 1.3 (RFC
// 8446 section 5) and TLS 1.2 (RFC 5288), and with AES-CBC and HMAC, for TLS
// 1.2 and 1.1 (RFC 5246 and 4346 section 6.2.3.2), each record carrying its
// own IV. Other suites and versions stay with crypto/tls.

const (
	recordHeaderLen = 5
	// maxPlaintext is the most a record carries, and what Send puts in each.
	maxPlaintext = 16 << 10
	// maxCiphertext12 and maxCiphertext13 bound a record's body: TLS 1.2 and
	// earlier allow 2048 bytes of expansion (RFC 5246 section 6.2.3), TLS 1.3
	// 256 (RFC 8446 section 5.2).
	maxCiphertext12 = maxPlaintext + 2048
	maxCiphertext13 = maxPlaintext + 256
	// explicitNonceLen is the part of a TLS 1.2 GCM nonce each record carries.
	explicitNonceLen = 8
	gcmTagLen        = 16
	// cbcBlockLen is AES's block, and a CBC record's IV.
	cbcBlockLen = aes.BlockSize

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
)

// A record protection's shape.
type mode uint8

const (
	modeGCM13 mode = iota
	modeGCM12
	modeCBC
)

// suite is what the record layer needs to know of a cipher suite.
type suite struct {
	mode   mode
	keyLen int
	// hash is TLS 1.3's HKDF hash, and TLS 1.2's PRF hash.
	hash func() hash.Hash
	// mac and macLen are a CBC suite's record MAC.
	mac    func() hash.Hash
	macLen int
}

// lookupSuite reports the suite with id under version, or false for one the
// record layer does not protect.
func lookupSuite(version, id uint16) (suite, bool) {
	switch version {
	case stdtls.VersionTLS13:
		switch id {
		case stdtls.TLS_AES_128_GCM_SHA256:
			return suite{mode: modeGCM13, keyLen: 16, hash: sha256.New}, true
		case stdtls.TLS_AES_256_GCM_SHA384:
			return suite{mode: modeGCM13, keyLen: 32, hash: sha512.New384}, true
		}
	case stdtls.VersionTLS12:
		switch id {
		case stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, stdtls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			stdtls.TLS_RSA_WITH_AES_128_GCM_SHA256:
			return suite{mode: modeGCM12, keyLen: 16, hash: sha256.New}, true
		case stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, stdtls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			stdtls.TLS_RSA_WITH_AES_256_GCM_SHA384:
			return suite{mode: modeGCM12, keyLen: 32, hash: sha512.New384}, true
		case stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256, stdtls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
			stdtls.TLS_RSA_WITH_AES_128_CBC_SHA256:
			return suite{mode: modeCBC, keyLen: 16, hash: sha256.New, mac: sha256.New, macLen: sha256.Size}, true
		}
		fallthrough
	case stdtls.VersionTLS11:
		switch id {
		case stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA, stdtls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
			stdtls.TLS_RSA_WITH_AES_128_CBC_SHA:
			return suite{mode: modeCBC, keyLen: 16, hash: sha256.New, mac: sha1.New, macLen: sha1.Size}, true
		case stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA, stdtls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			stdtls.TLS_RSA_WITH_AES_256_CBC_SHA:
			return suite{mode: modeCBC, keyLen: 32, hash: sha256.New, mac: sha1.New, macLen: sha1.Size}, true
		}
	}
	return suite{}, false
}

// cbcMode is the CBC encrypter or decrypter crypto/cipher returns, whose IV
// changes with every record.
type cbcMode interface {
	cipher.BlockMode
	SetIV([]byte)
}

var errNoCBCIV = errors.New("fib: tls: CBC mode without SetIV")

// recordKeys protects one direction of a connection's records.
type recordKeys struct {
	mode mode
	// version is what the records' headers carry, 3.3 but for TLS 1.1's 3.2.
	version [2]byte
	seq     uint64

	// The AES-GCM suites': the AEAD, and TLS 1.3's per-record nonce base or
	// TLS 1.2's four-byte salt.
	aead cipher.AEAD
	iv   [12]byte
	// secret, hash and keyLen derive the next generation of TLS 1.3 keys,
	// when the peer updates its own or asks for ours to be.
	secret []byte
	hash   func() hash.Hash
	keyLen int

	// The CBC suites': the block cipher, the modes built on it as the
	// direction needs them, and the record MAC.
	block  cipher.Block
	enc    cbcMode
	dec    cbcMode
	mac    hash.Hash
	macLen int

	// nonce, ad, header and macSum are scratch space, which the interfaces
	// they pass through would otherwise move to the heap on every record.
	nonce  [12]byte
	ad     [13]byte
	header [recordHeaderLen]byte
	macSum [sha512.Size]byte
}

// tls13 reports whether the keys are TLS 1.3's.
func (k *recordKeys) tls13() bool { return k.mode == modeGCM13 }

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
	k := &recordKeys{mode: modeGCM13, version: [2]byte{3, 3}, aead: aead, secret: secret, hash: h, keyLen: keyLen}
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

// newKeysFromMaster derives both directions' keys from a TLS 1.2 or 1.1
// master secret (RFC 5246 section 6.3), client's first. The key block holds
// the MAC keys, which only CBC suites have, then the encryption keys, then
// the GCM salts; TLS 1.1 and 1.2 CBC records carry their IVs, so the block's
// IVs, which follow, go unused.
func newKeysFromMaster(version uint16, s suite, master, clientRandom, serverRandom []byte) (client, server *recordKeys, err error) {
	seed := make([]byte, 0, 64)
	seed = append(append(seed, serverRandom...), clientRandom...)
	saltLen := 0
	if s.mode == modeGCM12 {
		saltLen = 4
	}
	n := 2*s.macLen + 2*s.keyLen + 2*saltLen
	var block []byte
	if version == stdtls.VersionTLS11 {
		block = prf10(master, "key expansion", seed, n)
	} else {
		block = prf12(s.hash, master, "key expansion", seed, n)
	}
	clientMAC, block := block[:s.macLen], block[s.macLen:]
	serverMAC, block := block[:s.macLen], block[s.macLen:]
	clientKey, block := block[:s.keyLen], block[s.keyLen:]
	serverKey, block := block[:s.keyLen], block[s.keyLen:]
	clientSalt, serverSalt := block[:saltLen], block[saltLen:]
	recordVersion := [2]byte{3, 3}
	if version == stdtls.VersionTLS11 {
		recordVersion = [2]byte{3, 2}
	}
	half := func(macKey, key, salt []byte) (*recordKeys, error) {
		k := &recordKeys{mode: s.mode, version: recordVersion}
		if s.mode == modeGCM12 {
			aead, err := newGCM(key)
			if err != nil {
				return nil, err
			}
			k.aead = aead
			copy(k.iv[:4], salt)
			return k, nil
		}
		b, err := aes.NewCipher(key)
		if err != nil {
			return nil, err
		}
		k.block, k.mac, k.macLen = b, hmac.New(s.mac, macKey), s.macLen
		return k, nil
	}
	if client, err = half(clientMAC, clientKey, clientSalt); err != nil {
		return nil, nil, err
	}
	if server, err = half(serverMAC, serverKey, serverSalt); err != nil {
		return nil, nil, err
	}
	return client, server, nil
}

// prf12 is the TLS 1.2 PRF, P_hash over HMAC (RFC 5246 section 5).
func prf12(h func() hash.Hash, secret []byte, label string, seed []byte, length int) []byte {
	labelSeed := append([]byte(label), seed...)
	return pHash(h, secret, labelSeed, length)
}

// prf10 is the TLS 1.0 and 1.1 PRF: P_MD5 over the secret's first half,
// exclusive-ored with P_SHA1 over its second (RFC 2246 section 5).
func prf10(secret []byte, label string, seed []byte, length int) []byte {
	labelSeed := append([]byte(label), seed...)
	half := (len(secret) + 1) / 2
	out := pHash(md5.New, secret[:half], labelSeed, length)
	other := pHash(sha1.New, secret[len(secret)-half:], labelSeed, length)
	for i := range out {
		out[i] ^= other[i]
	}
	return out
}

func pHash(h func() hash.Hash, secret, seed []byte, length int) []byte {
	mac := hmac.New(h, secret)
	mac.Write(seed)
	a := mac.Sum(nil)
	out := make([]byte, 0, length+mac.Size())
	for len(out) < length {
		mac.Reset()
		mac.Write(a)
		mac.Write(seed)
		out = mac.Sum(out)
		mac.Reset()
		mac.Write(a)
		a = mac.Sum(a[:0])
	}
	return out[:length]
}

// prefixLen is what a record has before its plaintext: its header, and a TLS
// 1.2 GCM record's explicit nonce or a CBC record's IV.
func (k *recordKeys) prefixLen() int {
	switch k.mode {
	case modeGCM13:
		return recordHeaderLen
	case modeGCM12:
		return recordHeaderLen + explicitNonceLen
	}
	return recordHeaderLen + cbcBlockLen
}

// suffixLen is the most sealing adds after a record's plaintext: TLS 1.3's
// inner content type and the tag, or a CBC record's MAC and padding.
func (k *recordKeys) suffixLen() int {
	switch k.mode {
	case modeGCM13:
		return 1 + gcmTagLen
	case modeGCM12:
		return gcmTagLen
	}
	return k.macLen + cbcBlockLen
}

// overhead is the most sealing adds to a record's plaintext, header
// included.
func (k *recordKeys) overhead() int { return k.prefixLen() + k.suffixLen() }

// maxBody bounds a record's body, what its header's length may say.
func (k *recordKeys) maxBody() int {
	if k.mode == modeGCM13 {
		return maxCiphertext13
	}
	return maxCiphertext12
}

// seal appends a record of typ carrying a followed by b, at most maxPlaintext
// bytes together, to dst, which needs room for it: the payload is copied
// into place and encrypted there.
func (k *recordKeys) seal(dst []byte, typ byte, a, b []byte) ([]byte, error) {
	start := len(dst)
	dst = append(dst, make([]byte, k.prefixLen())...)
	dst = append(append(dst, a...), b...)
	return k.sealAt(dst, start, typ)
}

// sealAt seals, in place, the record of typ that starts at buf[start]: room
// for its prefix, then its plaintext, which runs to the end of buf. It
// appends the suffix, for which buf needs room.
func (k *recordKeys) sealAt(buf []byte, start int, typ byte) ([]byte, error) {
	plainStart := start + k.prefixLen()
	n := len(buf) - plainStart
	header := buf[start : start+recordHeaderLen]
	header[0], header[1], header[2] = typ, k.version[0], k.version[1]
	switch k.mode {
	case modeCBC:
		if k.enc == nil {
			enc, ok := cipher.NewCBCEncrypter(k.block, make([]byte, cbcBlockLen)).(cbcMode)
			if !ok {
				return buf[:start], errNoCBCIV
			}
			k.enc = enc
		}
		iv := buf[start+recordHeaderLen : plainStart]
		if _, err := rand.Read(iv); err != nil {
			return buf[:start], err
		}
		buf = k.appendMAC(buf, typ, buf[plainStart:], nil)
		padding := cbcBlockLen - (n+k.macLen)%cbcBlockLen
		for i := 0; i < padding; i++ {
			buf = append(buf, byte(padding-1))
		}
		k.enc.SetIV(buf[start+recordHeaderLen : plainStart])
		k.enc.CryptBlocks(buf[plainStart:], buf[plainStart:])
	case modeGCM13:
		header[0] = recordTypeApplicationData
		k.nonce = k.iv
		seqXOR(&k.nonce, k.seq)
		// The header is complete before the type is appended, which moves buf
		// to a new array if it is full, and leaves header behind in the old
		// one: the record's additional data is read from buf, after.
		bodyLen := n + 1 + gcmTagLen
		header[3], header[4] = byte(bodyLen>>8), byte(bodyLen)
		buf = append(buf, typ)
		buf = k.aead.Seal(buf[:plainStart], k.nonce[:], buf[plainStart:], buf[start:start+recordHeaderLen])
	default:
		copy(k.nonce[:4], k.iv[:4])
		binary.BigEndian.PutUint64(k.nonce[4:], k.seq)
		copy(buf[start+recordHeaderLen:plainStart], k.nonce[4:])
		ad := k.additionalData(typ, n)
		buf = k.aead.Seal(buf[:plainStart], k.nonce[:], buf[plainStart:], ad)
	}
	bodyLen := len(buf) - start - recordHeaderLen
	buf[start+3], buf[start+4] = byte(bodyLen>>8), byte(bodyLen)
	k.seq++
	return buf, nil
}

// appendMAC appends to dst the MAC of a record of typ carrying data, and then
// feeds extra to the MAC as well, after its sum; see openCBC.
func (k *recordKeys) appendMAC(dst []byte, typ byte, data, extra []byte) []byte {
	k.mac.Reset()
	k.mac.Write(k.additionalData(typ, len(data)))
	k.mac.Write(data)
	dst = k.mac.Sum(dst)
	if extra != nil {
		k.mac.Write(extra)
	}
	return dst
}

// additionalData is what a TLS 1.2 or earlier record authenticates besides its
// payload: the sequence number, and the header with the payload's length.
func (k *recordKeys) additionalData(typ byte, n int) []byte {
	binary.BigEndian.PutUint64(k.ad[:8], k.seq)
	k.ad[8], k.ad[9], k.ad[10] = typ, k.version[0], k.version[1]
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
	if header[1] != k.version[0] || header[2] != k.version[1] {
		return 0, nil, alertProtocolVersion, false
	}
	switch k.mode {
	case modeGCM13:
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
	case modeCBC:
		return k.openCBC(header, body)
	}
	if len(body) < explicitNonceLen+gcmTagLen {
		return 0, nil, alertBadRecordMAC, false
	}
	copy(k.nonce[:4], k.iv[:4])
	copy(k.nonce[4:], body[:explicitNonceLen])
	ciphertext := body[explicitNonceLen:]
	ad := k.additionalData(header[0], len(ciphertext)-gcmTagLen)
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

// openCBC decrypts a CBC record and checks its padding and MAC together, in
// time that does not depend on which of them is wrong or where, as crypto/tls
// does against padding oracles such as Lucky13: the bytes past the payload
// are fed to the MAC after its sum, so that it hashes as much whatever the
// padding claims, and a bad padding and a bad MAC fail alike.
func (k *recordKeys) openCBC(header, body []byte) (typ byte, plaintext []byte, alert uint8, ok bool) {
	if k.dec == nil {
		dec, ok := cipher.NewCBCDecrypter(k.block, make([]byte, cbcBlockLen)).(cbcMode)
		if !ok {
			return 0, nil, alertBadRecordMAC, false
		}
		k.dec = dec
	}
	minBody := cbcBlockLen + (k.macLen+1+cbcBlockLen-1)/cbcBlockLen*cbcBlockLen
	if len(body)%cbcBlockLen != 0 || len(body) < minBody {
		return 0, nil, alertBadRecordMAC, false
	}
	k.dec.SetIV(body[:cbcBlockLen])
	payload := body[cbcBlockLen:]
	k.dec.CryptBlocks(payload, payload)
	paddingLen, paddingGood := extractPadding(payload)
	n := len(payload) - k.macLen - paddingLen
	n = subtle.ConstantTimeSelect(int(uint32(n)>>31), 0, n)
	remote := payload[n : n+k.macLen]
	local := k.appendMAC(k.macSum[:0], header[0], payload[:n], payload[n+k.macLen:])
	if subtle.ConstantTimeCompare(local, remote)&int(paddingGood) != 1 {
		return 0, nil, alertBadRecordMAC, false
	}
	k.seq++
	if n > maxPlaintext {
		return 0, nil, alertRecordOverflow, false
	}
	return header[0], payload[:n], 0, true
}

// extractPadding returns, in constant time, the length of the padding to
// remove from the end of payload, and a byte that is 255 if the padding was
// valid and 0 if not (RFC 2246 section 6.2.3.2). It is crypto/tls's: a bad
// padding reports a length of one, so that the bytes it claimed are still
// fed to the MAC and a bad padding cannot be told from a bad MAC.
func extractPadding(payload []byte) (toRemove int, good byte) {
	if len(payload) < 1 {
		return 0, 0
	}
	paddingLen := payload[len(payload)-1]
	t := uint(len(payload)-1) - uint(paddingLen)
	// If len(payload) >= paddingLen+1, the top bit of t is zero.
	good = byte(int32(^t) >> 31)
	// The most a padding and its length byte can take; the payload's own
	// length is public, so it may bound the loop.
	toCheck := min(256, len(payload))
	for i := 0; i < toCheck; i++ {
		t := uint(paddingLen) - uint(i)
		// If i <= paddingLen, the top bit of t is zero.
		mask := byte(int32(^t) >> 31)
		b := payload[len(payload)-1-i]
		good &^= mask&paddingLen ^ mask&b
	}
	// All of good's bits, and them replicated across it.
	good &= good << 4
	good &= good << 2
	good &= good << 1
	good = uint8(int8(good) >> 7)
	paddingLen &= good
	return int(paddingLen) + 1, good
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

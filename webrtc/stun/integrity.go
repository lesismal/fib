package stun

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"hash/crc32"
)

const (
	integritySize       = sha1.Size
	integritySHA256Size = sha256.Size
	fingerprintSize     = 4
	// fingerprintXOR is XORed into the CRC so that FINGERPRINT differs from
	// the CRC another protocol sharing the port may carry in the same place.
	fingerprintXOR = 0x5354554e
)

// LongTermKey is the key of the long-term credential mechanism with MD5,
// the algorithm TURN servers use unless PASSWORD-ALGORITHM picks another:
// MD5(username ":" realm ":" password), RFC 8489 section 9.2.2.
func LongTermKey(username, realm, password string) []byte {
	h := md5.New()
	h.Write([]byte(username + ":" + realm + ":" + password))
	return h.Sum(nil)
}

// LongTermKeySHA256 is the long-term key when PASSWORD-ALGORITHM is SHA-256.
func LongTermKeySHA256(username, realm, password string) []byte {
	h := sha256.Sum256([]byte(username + ":" + realm + ":" + password))
	return h[:]
}

// Userhash is the value of USERHASH, SHA-256(username ":" realm).
func Userhash(username, realm string) [32]byte {
	return sha256.Sum256([]byte(username + ":" + realm))
}

// AddIntegrity adds MESSAGE-INTEGRITY, an HMAC-SHA1 with key over the message
// so far. For short-term credentials, ICE's, the key is the password; for
// long-term ones it is LongTermKey. Only MESSAGE-INTEGRITY-SHA256 and
// FINGERPRINT may be added after it.
func (m *Message) AddIntegrity(key []byte) { m.addIntegrity(AttrMessageIntegrity, sha1.New, key) }

// AddIntegritySHA256 adds MESSAGE-INTEGRITY-SHA256, an HMAC-SHA256 with key
// over the message so far.
func (m *Message) AddIntegritySHA256(key []byte) {
	m.addIntegrity(AttrMessageIntegritySHA256, sha256.New, key)
}

func (m *Message) addIntegrity(t AttrType, h func() hash.Hash, key []byte) {
	mac := hmac.New(h, key)
	size := mac.Size()
	start := len(m.Raw)
	m.Raw = m.grow(t, size)
	// The header's length already counts the new attribute, which is what
	// the HMAC is computed with (RFC 8489 section 14.5).
	mac.Write(m.Raw[:start])
	mac.Sum(m.Raw[:len(m.Raw)-size])
}

// AddFingerprint adds FINGERPRINT, the CRC-32 of the message so far XORed
// with 0x5354554e. It must be the last attribute.
func (m *Message) AddFingerprint() {
	start := len(m.Raw)
	m.Raw = m.grow(AttrFingerprint, fingerprintSize)
	crc := crc32.ChecksumIEEE(m.Raw[:start]) ^ fingerprintXOR
	binary.BigEndian.PutUint32(m.Raw[len(m.Raw)-fingerprintSize:], crc)
}

// covered returns what MESSAGE-INTEGRITY or FINGERPRINT, at the attribute
// whose value starts at off, is computed over: Raw up to the attribute, with
// the header's length set to end at the attribute's end. It sets that length
// in Raw itself, which the caller restores with the returned value once done:
// handing a copy of the header to the hash would allocate it.
func (m *Message) covered(off, n int) (b []byte, length uint16) {
	length = binary.BigEndian.Uint16(m.Raw[2:4])
	binary.BigEndian.PutUint16(m.Raw[2:4], uint16(off+pad4(n)-HeaderSize))
	return m.Raw[:off-attrHeaderSize], length
}

// CheckIntegrity verifies MESSAGE-INTEGRITY with key. It reports
// ErrIntegrityMissing for a message without one, which a server must treat
// differently from a wrong one (RFC 8489 section 9.1.3).
//
// The checks rewrite the length in Raw's header while they run and put it
// back before they return, so Raw must not be read by another goroutine
// meanwhile.
func (m *Message) CheckIntegrity(key []byte) error {
	return m.checkIntegrity(AttrMessageIntegrity, sha1.New, key, integritySize)
}

// CheckIntegritySHA256 verifies MESSAGE-INTEGRITY-SHA256 with key, which
// may be truncated to as few as 16 bytes (RFC 8489 section 14.6).
func (m *Message) CheckIntegritySHA256(key []byte) error {
	return m.checkIntegrity(AttrMessageIntegritySHA256, sha256.New, key, 0)
}

func (m *Message) checkIntegrity(t AttrType, h func() hash.Hash, key []byte, size int) error {
	i := m.index(t)
	if i < 0 {
		return ErrIntegrityMissing
	}
	a := m.attrs[i]
	if size != 0 && a.n != size || size == 0 && (a.n < 16 || a.n > integritySHA256Size || a.n%4 != 0) {
		return ErrIntegrity
	}
	mac := hmac.New(h, key)
	b, length := m.covered(a.off, a.n)
	mac.Write(b)
	binary.BigEndian.PutUint16(m.Raw[2:4], length)
	var sum [integritySHA256Size]byte
	if !hmac.Equal(mac.Sum(sum[:0])[:a.n], m.Raw[a.off:a.off+a.n]) {
		return ErrIntegrity
	}
	return nil
}

// CheckFingerprint verifies FINGERPRINT, which must be the last attribute.
// A message without one is reported with ErrAttrNotFound.
func (m *Message) CheckFingerprint() error {
	i := m.index(AttrFingerprint)
	if i < 0 {
		return ErrAttrNotFound
	}
	a := m.attrs[i]
	if a.n != fingerprintSize {
		return ErrAttrSize
	}
	if a.off+a.n != len(m.Raw) {
		return ErrFingerprintLast
	}
	b, length := m.covered(a.off, a.n)
	crc := crc32.ChecksumIEEE(b)
	binary.BigEndian.PutUint16(m.Raw[2:4], length)
	if crc^fingerprintXOR != binary.BigEndian.Uint32(m.Raw[a.off:]) {
		return ErrFingerprint
	}
	return nil
}

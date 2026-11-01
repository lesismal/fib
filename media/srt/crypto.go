package srt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/pbkdf2"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"io"
)

// The keying material message (section 3.2.2 and section 6.1): a fixed
// header, the salt and the wrapped stream encrypting keys.
const (
	kmHeaderSize = 16
	kmSaltSize   = 16
	kmSign       = 0x2029
	// kmCipherCTR is AES-CTR, the only cipher of SRT before 1.5.2's
	// AES-GCM, which this package does not offer.
	kmCipherCTR = 2
	// kmSE is the stream encapsulation of SRT itself.
	kmSE = 2
	// pbkdf2Iter is the PBKDF2 iteration count SRT fixes.
	pbkdf2Iter = 2048
)

// KM states a KMRSP carries in place of a KM message when it refuses one
// (section 3.2.2).
const (
	kmStateUnsecured = 0
	kmStateNoSecret  = 3
	kmStateBadSecret = 4
)

var (
	errBadKM       = errors.New("srt: malformed keying material")
	errBadSecret   = errors.New("srt: wrong passphrase")
	errKeyLen      = errors.New("srt: key length is not 16, 24 or 32 bytes")
	errPassphrase  = errors.New("srt: passphrase is not 10 to 79 bytes")
	errUnwrapShort = errors.New("srt: wrapped key too short")
)

// keyMaterial is a decoded KM message: the salt and the even and odd stream
// encrypting keys, either of which may be absent.
type keyMaterial struct {
	salt   [kmSaltSize]byte
	keyLen int
	even   []byte
	odd    []byte
}

// kek derives the key encrypting key of passphrase for km's salt: PBKDF2
// with HMAC-SHA1, 2048 iterations, over the low 64 bits of the salt, as
// libsrt's haicrypt does.
func (km *keyMaterial) kek(passphrase string) ([]byte, error) {
	return pbkdf2.Key(sha1.New, passphrase, km.salt[kmSaltSize-8:], pbkdf2Iter, km.keyLen)
}

// newKeyMaterial makes a random salt and even key of keyLen bytes.
func newKeyMaterial(rand io.Reader, keyLen int) (*keyMaterial, error) {
	if keyLen != 16 && keyLen != 24 && keyLen != 32 {
		return nil, errKeyLen
	}
	km := &keyMaterial{keyLen: keyLen, even: make([]byte, keyLen)}
	if _, err := io.ReadFull(rand, km.salt[:]); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(rand, km.even); err != nil {
		return nil, err
	}
	return km, nil
}

// marshal returns the KM message of km, its keys wrapped with the KEK of
// passphrase.
func (km *keyMaterial) marshal(passphrase string) ([]byte, error) {
	kek, err := km.kek(passphrase)
	if err != nil {
		return nil, err
	}
	var kk byte
	var keys []byte
	if km.even != nil {
		kk |= kkEven
		keys = append(keys, km.even...)
	}
	if km.odd != nil {
		kk |= kkOdd
		keys = append(keys, km.odd...)
	}
	wrapped, err := keyWrap(kek, keys)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 0, kmHeaderSize+kmSaltSize+len(wrapped))
	b = append(b, 0x12) // S 0, version 1, packet type 2 (KM)
	b = binary.BigEndian.AppendUint16(b, kmSign)
	b = append(b, kk)
	b = binary.BigEndian.AppendUint32(b, 0) // KEKI: the default KEK
	b = append(b, kmCipherCTR, 0, kmSE, 0, 0, 0, kmSaltSize/4, byte(km.keyLen/4))
	b = append(b, km.salt[:]...)
	return append(b, wrapped...), nil
}

// unmarshalKM decodes the KM message b with the KEK of passphrase. A key
// that does not unwrap is errBadSecret.
func unmarshalKM(b []byte, passphrase string) (*keyMaterial, error) {
	if len(b) < kmHeaderSize+kmSaltSize || b[0] != 0x12 || binary.BigEndian.Uint16(b[1:]) != kmSign {
		return nil, errBadKM
	}
	kk := b[3] & 3
	if kk == 0 || b[8] != kmCipherCTR || int(b[14])*4 != kmSaltSize {
		return nil, errBadKM
	}
	km := &keyMaterial{keyLen: int(b[15]) * 4}
	if km.keyLen != 16 && km.keyLen != 24 && km.keyLen != 32 {
		return nil, errBadKM
	}
	copy(km.salt[:], b[kmHeaderSize:])
	n := 1
	if kk == kkEven|kkOdd {
		n = 2
	}
	wrapped := b[kmHeaderSize+kmSaltSize:]
	if len(wrapped) < 8+n*km.keyLen {
		return nil, errBadKM
	}
	wrapped = wrapped[:8+n*km.keyLen]
	kek, err := km.kek(passphrase)
	if err != nil {
		return nil, err
	}
	keys, err := keyUnwrap(kek, wrapped)
	if err != nil {
		return nil, err
	}
	if kk&kkEven != 0 {
		km.even, keys = keys[:km.keyLen], keys[km.keyLen:]
	}
	if kk&kkOdd != 0 {
		km.odd = keys[:km.keyLen]
	}
	return km, nil
}

// keyWrapIV is the default initial value of RFC 3394 section 2.2.3.1.
var keyWrapIV = [8]byte{0xa6, 0xa6, 0xa6, 0xa6, 0xa6, 0xa6, 0xa6, 0xa6}

// keyWrap wraps key, a multiple of 8 bytes, with kek (RFC 3394 section
// 2.2.1).
func keyWrap(kek, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	n := len(key) / 8
	out := make([]byte, 8+len(key))
	copy(out, keyWrapIV[:])
	copy(out[8:], key)
	var buf [16]byte
	for j := range 6 {
		for i := 1; i <= n; i++ {
			copy(buf[:8], out[:8])
			copy(buf[8:], out[8*i:8*i+8])
			block.Encrypt(buf[:], buf[:])
			t := uint64(n*j + i)
			binary.BigEndian.PutUint64(out, binary.BigEndian.Uint64(buf[:8])^t)
			copy(out[8*i:], buf[8:])
		}
	}
	return out, nil
}

// keyUnwrap unwraps wrapped with kek (RFC 3394 section 2.2.2), failing with
// errBadSecret when the integrity check fails, as it does for a KEK of the
// wrong passphrase.
func keyUnwrap(kek, wrapped []byte) ([]byte, error) {
	if len(wrapped) < 24 || len(wrapped)%8 != 0 {
		return nil, errUnwrapShort
	}
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	n := len(wrapped)/8 - 1
	a := binary.BigEndian.Uint64(wrapped)
	r := append([]byte(nil), wrapped[8:]...)
	var buf [16]byte
	for j := 5; j >= 0; j-- {
		for i := n; i >= 1; i-- {
			t := uint64(n*j + i)
			binary.BigEndian.PutUint64(buf[:8], a^t)
			copy(buf[8:], r[8*(i-1):8*i])
			block.Decrypt(buf[:], buf[:])
			a = binary.BigEndian.Uint64(buf[:8])
			copy(r[8*(i-1):], buf[8:])
		}
	}
	var got [8]byte
	binary.BigEndian.PutUint64(got[:], a)
	if subtle.ConstantTimeCompare(got[:], keyWrapIV[:]) != 1 {
		return nil, errBadSecret
	}
	return r, nil
}

// cryptor encrypts and decrypts payloads with the keys of a KM.
type cryptor struct {
	salt [kmSaltSize]byte
	even cipher.Block
	odd  cipher.Block
}

func newCryptor(km *keyMaterial) (*cryptor, error) {
	c := &cryptor{salt: km.salt}
	var err error
	if km.even != nil {
		if c.even, err = aes.NewCipher(km.even); err != nil {
			return nil, err
		}
	}
	if km.odd != nil {
		if c.odd, err = aes.NewCipher(km.odd); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// xor encrypts or decrypts b in place with the key kk names, for the
// packet of sequence number seq: AES-CTR with an IV of the salt's first
// 112 bits, the packet index XORed into bits 80 to 111, and a 16-bit block
// counter (section 6.2.2, libsrt's hcrypt_SetCtrIV). It reports false when
// the key is absent.
func (c *cryptor) xor(kk int, seq uint32, b []byte) bool {
	block := c.even
	if kk == kkOdd {
		block = c.odd
	}
	if block == nil {
		return false
	}
	var iv [16]byte
	copy(iv[:14], c.salt[:14])
	var pki [4]byte
	binary.BigEndian.PutUint32(pki[:], seq)
	for i := range 4 {
		iv[10+i] ^= pki[i]
	}
	cipher.NewCTR(block, iv[:]).XORKeyStream(b, b)
	return true
}

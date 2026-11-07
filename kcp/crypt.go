package kcp

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/des"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// The crypt header of github.com/xtaci/kcp-go's block ciphers: a random
// nonce and the CRC32 (IEEE) of what follows it, encrypted together with
// the rest of the datagram.
const (
	nonceSize       = 16
	crcSize         = 4
	cryptHeaderSize = nonceSize + crcSize
	// maxPacketSize is the largest datagram kcp-go reads, and the length
	// of its XOR table.
	maxPacketSize = 1472
)

// kcp-go's CFB initial vector and the salt of its XOR table.
var (
	initialVector = [16]byte{167, 115, 79, 156, 18, 172, 27, 1, 164, 21, 242, 193, 252, 120, 230, 107}
	saltXOR       = []byte(`sH3CIVoF#rWLtJo6`)
)

type cryptKind int

const (
	cryptNone cryptKind = iota
	cryptBlock
	cryptXOR
	cryptAEAD
)

// Crypt encrypts a session's datagrams as github.com/xtaci/kcp-go's
// BlockCrypt of the same cipher and key does, so that each side reads the
// other's. A nil *Crypt sends KCP in the clear without any header, as
// kcp-go without a BlockCrypt and the reference implementation do. It is
// safe for concurrent use: a Server shares one among its sessions.
//
// Ciphers other than block ciphers in kcp-go's CFB mode, its XOR table and
// AEADs - salsa20 - have no counterpart here. A block cipher of 8 or 16
// byte blocks that the standard library does not have (blowfish, cast5,
// twofish, tea, xtea, sm4) goes to NewBlockCrypt from wherever it is
// implemented.
type Crypt struct {
	kind   cryptKind
	block  cipher.Block
	aead   cipher.AEAD
	xortbl []byte
}

// NewNoneCrypt returns kcp-go's "none" crypt: the nonce and CRC header,
// unencrypted. The CRC still turns away corrupted datagrams.
func NewNoneCrypt() *Crypt { return &Crypt{kind: cryptNone} }

// NewBlockCrypt returns kcp-go's CFB mode of block, which must have 8 or
// 16 byte blocks.
func NewBlockCrypt(block cipher.Block) (*Crypt, error) {
	if bs := block.BlockSize(); bs != 8 && bs != 16 {
		return nil, fmt.Errorf("kcp: block size %d, not 8 or 16", bs)
	}
	return &Crypt{kind: cryptBlock, block: block}, nil
}

// NewAESCrypt returns kcp-go's AES crypt (NewAESBlockCrypt): a key of 16,
// 24 or 32 bytes picks AES-128, -192 or -256.
func NewAESCrypt(key []byte) (*Crypt, error) {
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return NewBlockCrypt(b)
}

// NewTripleDESCrypt returns kcp-go's 3DES crypt, of a 24-byte key.
func NewTripleDESCrypt(key []byte) (*Crypt, error) {
	b, err := des.NewTripleDESCipher(key)
	if err != nil {
		return nil, err
	}
	return NewBlockCrypt(b)
}

// NewXORCrypt returns kcp-go's XOR crypt (NewSimpleXORBlockCrypt), which
// hides rather than protects.
func NewXORCrypt(key []byte) (*Crypt, error) {
	tbl, err := pbkdf2.Key(sha1.New, string(key), saltXOR, 32, maxPacketSize)
	if err != nil {
		return nil, err
	}
	return &Crypt{kind: cryptXOR, xortbl: tbl}, nil
}

// NewAEADCrypt returns kcp-go's AEAD crypt (NewAEADCrypt): a random nonce
// and the sealed datagram, without a CRC.
func NewAEADCrypt(aead cipher.AEAD) *Crypt { return &Crypt{kind: cryptAEAD, aead: aead} }

// NewAESGCMCrypt returns kcp-go's AES-GCM crypt (NewAESGCMCrypt).
func NewAESGCMCrypt(key []byte) (*Crypt, error) {
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	g, err := cipher.NewGCM(b)
	if err != nil {
		return nil, err
	}
	return NewAEADCrypt(g), nil
}

// NewCrypt returns the crypt kcptun names crypt: "null" (nil: no crypt),
// "none", "xor", "aes" (key[:32]), "aes-128" (key[:16]), "aes-192"
// (key[:24]), "3des" (key[:24]) or "aes-128-gcm" (key[:16]). The key is
// already derived: kcptun derives it with
// pbkdf2.Key(sha1.New, passphrase, []byte("kcp-go"), 4096, 32).
func NewCrypt(name string, key []byte) (*Crypt, error) {
	need := func(n int) ([]byte, error) {
		if len(key) < n {
			return nil, fmt.Errorf("kcp: %s needs a key of %d bytes", name, n)
		}
		return key[:n], nil
	}
	switch name {
	case "null", "":
		return nil, nil
	case "none":
		return NewNoneCrypt(), nil
	case "xor":
		return NewXORCrypt(key)
	case "aes", "aes-128", "aes-192", "3des", "aes-128-gcm":
		n := map[string]int{"aes": 32, "aes-128": 16, "aes-192": 24, "3des": 24, "aes-128-gcm": 16}[name]
		k, err := need(n)
		if err != nil {
			return nil, err
		}
		switch name {
		case "3des":
			return NewTripleDESCrypt(k)
		case "aes-128-gcm":
			return NewAESGCMCrypt(k)
		}
		return NewAESCrypt(k)
	}
	return nil, fmt.Errorf("kcp: crypt %q is not supported", name)
}

// headerSize is the room the crypt takes before the data it encrypts.
func (c *Crypt) headerSize() int {
	switch {
	case c == nil:
		return 0
	case c.kind == cryptAEAD:
		return c.aead.NonceSize()
	}
	return cryptHeaderSize
}

// trailerSize is the room the crypt takes after the data: an AEAD's tag.
func (c *Crypt) trailerSize() int {
	if c != nil && c.kind == cryptAEAD {
		return c.aead.Overhead()
	}
	return 0
}

// seal encrypts b in place, whose first headerSize bytes are the crypt's
// and whose capacity has room for trailerSize more, returning the
// datagram.
func (c *Crypt) seal(b []byte) []byte {
	if c == nil {
		return b
	}
	switch c.kind {
	case cryptAEAD:
		ns := c.aead.NonceSize()
		_, _ = rand.Read(b[:ns])
		return c.aead.Seal(b[:ns], b[:ns], b[ns:], nil)
	}
	_, _ = rand.Read(b[:nonceSize])
	binary.LittleEndian.PutUint32(b[nonceSize:], crc32.ChecksumIEEE(b[cryptHeaderSize:]))
	switch c.kind {
	case cryptBlock:
		cfbEncrypt(c.block, b)
	case cryptXOR:
		subtle.XORBytes(b, b, c.xortbl)
	}
	return b
}

var errCrypt = errors.New("kcp: datagram fails its checksum or authentication")

// open decrypts a datagram in place, returning what follows the crypt
// header.
func (c *Crypt) open(b []byte) ([]byte, error) {
	if c == nil {
		return b, nil
	}
	if c.kind == cryptAEAD {
		ns := c.aead.NonceSize()
		if len(b) < ns+c.aead.Overhead() {
			return nil, errCrypt
		}
		out, err := c.aead.Open(b[ns:ns], b[:ns], b[ns:], nil)
		if err != nil {
			return nil, errCrypt
		}
		return out, nil
	}
	if len(b) < cryptHeaderSize {
		return nil, errCrypt
	}
	switch c.kind {
	case cryptBlock:
		cfbDecrypt(c.block, b)
	case cryptXOR:
		// As kcp-go, past the table's 1472 bytes nothing is hidden.
		subtle.XORBytes(b, b, c.xortbl)
	}
	if crc32.ChecksumIEEE(b[cryptHeaderSize:]) != binary.LittleEndian.Uint32(b[nonceSize:]) {
		return nil, errCrypt
	}
	return b[cryptHeaderSize:], nil
}

// cfbEncrypt is kcp-go's CFB: the keystream starts at the encrypted
// initial vector, each whole block of ciphertext is encrypted into the
// next, and a last partial block takes what it needs of it.
func cfbEncrypt(block cipher.Block, b []byte) {
	bs := block.BlockSize()
	var tbl [16]byte
	block.Encrypt(tbl[:bs], initialVector[:bs])
	for len(b) >= bs {
		subtle.XORBytes(b[:bs], b[:bs], tbl[:bs])
		block.Encrypt(tbl[:bs], b[:bs])
		b = b[bs:]
	}
	subtle.XORBytes(b, b, tbl[:len(b)])
}

func cfbDecrypt(block cipher.Block, b []byte) {
	bs := block.BlockSize()
	var tbl, next [16]byte
	block.Encrypt(tbl[:bs], initialVector[:bs])
	for len(b) >= bs {
		block.Encrypt(next[:bs], b[:bs])
		subtle.XORBytes(b[:bs], b[:bs], tbl[:bs])
		tbl = next
		b = b[bs:]
	}
	subtle.XORBytes(b, b, tbl[:len(b)])
}

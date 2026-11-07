package mse

import (
	"crypto/rand"
	"crypto/rc4"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
	"math/big"
)

// CryptoMethod is a set of the payload methods of crypto_provide and
// crypto_select.
type CryptoMethod uint32

// The payload methods. After the handshake, the payload goes as it is with
// Plaintext and through RC4 with RC4; the handshake itself is encrypted
// either way.
const (
	Plaintext CryptoMethod = 0x01
	RC4       CryptoMethod = 0x02
	// Both provides both methods.
	Both = Plaintext | RC4
)

// Policy is how a peer treats encryption, accepting and dialing.
type Policy uint8

const (
	// Allow accepts plaintext and MSE connections, dials in plaintext, and
	// as receiver selects plaintext when the initiator provides it: the
	// cheapest that every client speaks.
	Allow Policy = iota
	// Prefer accepts both, dials with MSE providing both methods, and as
	// receiver selects RC4 when provided.
	Prefer
	// Require accepts and dials MSE with RC4 only: a plaintext handshake, or
	// an initiator that provides only plaintext, is refused.
	Require
)

// Provide is what an initiator with policy p provides: both methods, or
// RC4 alone for Require.
func (p Policy) Provide() CryptoMethod {
	if p == Require {
		return RC4
	}
	return Both
}

// Select is a receiver's choice among what the initiator provided: zero
// when nothing provided is acceptable, which fails the handshake.
func (p Policy) Select(provided CryptoMethod) CryptoMethod {
	switch p {
	case Require:
		return provided & RC4
	case Prefer:
		if provided&RC4 != 0 {
			return RC4
		}
		return provided & Plaintext
	default:
		if provided&Plaintext != 0 {
			return Plaintext
		}
		return provided & RC4
	}
}

// Sizes of the handshake.
const (
	// KeySize is the size of a public key, Ya or Yb, and of the secret S.
	KeySize = 96
	// MaxPad is the most padding any of PadA..PadD may have.
	MaxPad = 512
	// discard is the RC4 keystream both sides throw away before use.
	discard = 1024
)

var (
	// prime is P, the 768-bit prime of the specification; the generator G
	// is 2.
	prime, _ = new(big.Int).SetString("FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A63A36210000000000090563", 16)
	// primeMinus1 bounds a valid public key from above.
	primeMinus1 = new(big.Int).Sub(prime, big.NewInt(1))
	generator   = big.NewInt(2)
)

// Errors of a handshake.
var (
	// ErrPlaintextRefused is a plaintext BitTorrent handshake where the
	// receiver's policy requires encryption.
	ErrPlaintextRefused = errors.New("mse: plaintext handshake refused")
	// ErrNoSync is a peer that sent more than the padding allows without
	// the hash or verification constant that ends it: not MSE, or another
	// secret.
	ErrNoSync = errors.New("mse: no synchronization point within the padding")
	// ErrUnknownKey is an initiator whose SKEY, the info hash, the
	// receiver serves no torrent of.
	ErrUnknownKey = errors.New("mse: unknown SKEY")
	// ErrBadKey is a public key outside (1, P-1).
	ErrBadKey = errors.New("mse: invalid public key")
	// ErrBadVC is a verification constant that is not eight zeros.
	ErrBadVC = errors.New("mse: bad verification constant")
	// ErrNoMethod is a crypto_provide with no method the receiver accepts,
	// or a crypto_select that is not one of the methods provided.
	ErrNoMethod = errors.New("mse: no acceptable crypto method")
	// ErrPadding is a padding length over MaxPad.
	ErrPadding = errors.New("mse: padding too long")
)

// keyPair is one side's Diffie-Hellman key: x private, y public.
type keyPair struct {
	x *big.Int
	y [KeySize]byte
}

// newKeyPair draws a 160-bit private key, as the specification has it,
// from rnd.
func newKeyPair(rnd io.Reader) (*keyPair, error) {
	var b [20]byte
	if _, err := io.ReadFull(rnd, b[:]); err != nil {
		return nil, err
	}
	k := &keyPair{x: new(big.Int).SetBytes(b[:])}
	new(big.Int).Exp(generator, k.x, prime).FillBytes(k.y[:])
	return k, nil
}

// secret is S, from the peer's public key: an error when that key is one
// of the degenerate values that would make S predictable.
func (k *keyPair) secret(peer []byte) ([KeySize]byte, error) {
	var s [KeySize]byte
	y := new(big.Int).SetBytes(peer)
	if y.Cmp(big.NewInt(1)) <= 0 || y.Cmp(primeMinus1) >= 0 {
		return s, ErrBadKey
	}
	new(big.Int).Exp(y, k.x, prime).FillBytes(s[:])
	return s, nil
}

func hash(parts ...[]byte) (sum [20]byte) {
	h := sha1.New()
	for _, p := range parts {
		h.Write(p)
	}
	h.Sum(sum[:0])
	return sum
}

// Req2 is HASH('req2', SKEY), which the receiver recovers from the
// initiator's third message and looks the SKEY up by; a KeyTable keeps
// these.
func Req2(skey [20]byte) [20]byte {
	return hash([]byte("req2"), skey[:])
}

// newCipher is the RC4 stream keyed HASH(name, S, SKEY), its first 1024
// bytes discarded.
func newCipher(name string, s []byte, skey [20]byte) *rc4.Cipher {
	c, _ := rc4.NewCipher(func() []byte { h := hash([]byte(name), s, skey[:]); return h[:] }())
	var junk [discard]byte
	c.XORKeyStream(junk[:], junk[:])
	return c
}

// padding returns random padding of a random length up to max.
func padding(rnd io.Reader, max int) ([]byte, error) {
	var b [2]byte
	if _, err := io.ReadFull(rnd, b[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint16(b[:])) % (max + 1)
	p := make([]byte, n)
	if _, err := io.ReadFull(rnd, p); err != nil {
		return nil, err
	}
	return p, nil
}

func randReader(r io.Reader) io.Reader {
	if r == nil {
		return rand.Reader
	}
	return r
}

// Result is a finished handshake: how the payload goes from here on.
type Result struct {
	// Method is the payload method selected, Plaintext or RC4; zero when
	// PlainHandshake.
	Method CryptoMethod
	// PlainHandshake says the initiator sent a plaintext BitTorrent
	// handshake rather than MSE (a receiver's policy allowing it): Rest is
	// everything it sent, and there are no ciphers.
	PlainHandshake bool
	// SKey is the SKEY the handshake was made with: the info hash.
	SKey [20]byte
	// IA is the initiator's initial payload, decrypted, for the receiver;
	// it comes before Rest in the stream.
	IA []byte
	// Rest is what the peer sent after the handshake, already received,
	// decrypted when Method is RC4.
	Rest []byte
	// Encrypt and Decrypt are the payload's ciphers when Method is RC4,
	// each already advanced past the handshake; nil otherwise.
	Encrypt, Decrypt *rc4.Cipher
}

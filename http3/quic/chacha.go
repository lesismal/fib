package quic

import (
	"crypto/cipher"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"math/bits"
)

// TLS 1.3 may settle on TLS_CHACHA20_POLY1305_SHA256, and QUIC then protects
// packets and headers with ChaCha20 (RFC 9001 section 5.4.4). The standard
// library keeps its implementation internal, and its exported AEADs are AES
// only, so this is the one of RFC 8439: correct and constant-time. The block
// function keeps its sixteen words in variables rather than an array, which
// is what the compiler needs to leave them in registers, and the AEAD
// absorbs each part of a packet in one pass, with the poly1305 key computed
// from block zero and the keystream from block one, as the construction has
// it.

const (
	chachaKeySize   = 32
	chachaNonceSize = 12
	poly1305TagSize = 16
)

// chachaBlock computes the ChaCha20 block for key, counter and nonce: the
// quarter rounds of RFC 8439 section 2.3, ten times, plus the state they
// started from.
func chachaBlock(out *[64]byte, key *[8]uint32, counter uint32, nonce *[3]uint32) {
	x0, x1, x2, x3 := uint32(0x61707865), uint32(0x3320646e), uint32(0x79622d32), uint32(0x6b206574)
	x4, x5, x6, x7 := key[0], key[1], key[2], key[3]
	x8, x9, x10, x11 := key[4], key[5], key[6], key[7]
	x12, x13, x14, x15 := counter, nonce[0], nonce[1], nonce[2]
	for i := 0; i < 10; i++ {
		x0 += x4
		x12 ^= x0
		x12 = bits.RotateLeft32(x12, 16)
		x8 += x12
		x4 ^= x8
		x4 = bits.RotateLeft32(x4, 12)
		x0 += x4
		x12 ^= x0
		x12 = bits.RotateLeft32(x12, 8)
		x8 += x12
		x4 ^= x8
		x4 = bits.RotateLeft32(x4, 7)
		x1 += x5
		x13 ^= x1
		x13 = bits.RotateLeft32(x13, 16)
		x9 += x13
		x5 ^= x9
		x5 = bits.RotateLeft32(x5, 12)
		x1 += x5
		x13 ^= x1
		x13 = bits.RotateLeft32(x13, 8)
		x9 += x13
		x5 ^= x9
		x5 = bits.RotateLeft32(x5, 7)
		x2 += x6
		x14 ^= x2
		x14 = bits.RotateLeft32(x14, 16)
		x10 += x14
		x6 ^= x10
		x6 = bits.RotateLeft32(x6, 12)
		x2 += x6
		x14 ^= x2
		x14 = bits.RotateLeft32(x14, 8)
		x10 += x14
		x6 ^= x10
		x6 = bits.RotateLeft32(x6, 7)
		x3 += x7
		x15 ^= x3
		x15 = bits.RotateLeft32(x15, 16)
		x11 += x15
		x7 ^= x11
		x7 = bits.RotateLeft32(x7, 12)
		x3 += x7
		x15 ^= x3
		x15 = bits.RotateLeft32(x15, 8)
		x11 += x15
		x7 ^= x11
		x7 = bits.RotateLeft32(x7, 7)
		x0 += x5
		x15 ^= x0
		x15 = bits.RotateLeft32(x15, 16)
		x10 += x15
		x5 ^= x10
		x5 = bits.RotateLeft32(x5, 12)
		x0 += x5
		x15 ^= x0
		x15 = bits.RotateLeft32(x15, 8)
		x10 += x15
		x5 ^= x10
		x5 = bits.RotateLeft32(x5, 7)
		x1 += x6
		x12 ^= x1
		x12 = bits.RotateLeft32(x12, 16)
		x11 += x12
		x6 ^= x11
		x6 = bits.RotateLeft32(x6, 12)
		x1 += x6
		x12 ^= x1
		x12 = bits.RotateLeft32(x12, 8)
		x11 += x12
		x6 ^= x11
		x6 = bits.RotateLeft32(x6, 7)
		x2 += x7
		x13 ^= x2
		x13 = bits.RotateLeft32(x13, 16)
		x8 += x13
		x7 ^= x8
		x7 = bits.RotateLeft32(x7, 12)
		x2 += x7
		x13 ^= x2
		x13 = bits.RotateLeft32(x13, 8)
		x8 += x13
		x7 ^= x8
		x7 = bits.RotateLeft32(x7, 7)
		x3 += x4
		x14 ^= x3
		x14 = bits.RotateLeft32(x14, 16)
		x9 += x14
		x4 ^= x9
		x4 = bits.RotateLeft32(x4, 12)
		x3 += x4
		x14 ^= x3
		x14 = bits.RotateLeft32(x14, 8)
		x9 += x14
		x4 ^= x9
		x4 = bits.RotateLeft32(x4, 7)
	}
	binary.LittleEndian.PutUint32(out[0:], x0+0x61707865)
	binary.LittleEndian.PutUint32(out[4:], x1+0x3320646e)
	binary.LittleEndian.PutUint32(out[8:], x2+0x79622d32)
	binary.LittleEndian.PutUint32(out[12:], x3+0x6b206574)
	binary.LittleEndian.PutUint32(out[16:], x4+key[0])
	binary.LittleEndian.PutUint32(out[20:], x5+key[1])
	binary.LittleEndian.PutUint32(out[24:], x6+key[2])
	binary.LittleEndian.PutUint32(out[28:], x7+key[3])
	binary.LittleEndian.PutUint32(out[32:], x8+key[4])
	binary.LittleEndian.PutUint32(out[36:], x9+key[5])
	binary.LittleEndian.PutUint32(out[40:], x10+key[6])
	binary.LittleEndian.PutUint32(out[44:], x11+key[7])
	binary.LittleEndian.PutUint32(out[48:], x12+counter)
	binary.LittleEndian.PutUint32(out[52:], x13+nonce[0])
	binary.LittleEndian.PutUint32(out[56:], x14+nonce[1])
	binary.LittleEndian.PutUint32(out[60:], x15+nonce[2])
}

func chachaKey(key []byte) (k [8]uint32) {
	for i := range k {
		k[i] = binary.LittleEndian.Uint32(key[4*i:])
	}
	return k
}

func chachaNonce(nonce []byte) (n [3]uint32) {
	for i := range n {
		n[i] = binary.LittleEndian.Uint32(nonce[4*i:])
	}
	return n
}

// chachaXOR XORs src with the key stream starting at block counter into dst,
// which may be src itself. A whole block's key stream and data are moved
// eight bytes at a time.
func chachaXOR(dst, src []byte, key *[8]uint32, counter uint32, nonce *[3]uint32) {
	var block [64]byte
	for len(src) >= 64 {
		chachaBlock(&block, key, counter, nonce)
		counter++
		binary.LittleEndian.PutUint64(dst[0:], binary.LittleEndian.Uint64(src[0:])^binary.LittleEndian.Uint64(block[0:]))
		binary.LittleEndian.PutUint64(dst[8:], binary.LittleEndian.Uint64(src[8:])^binary.LittleEndian.Uint64(block[8:]))
		binary.LittleEndian.PutUint64(dst[16:], binary.LittleEndian.Uint64(src[16:])^binary.LittleEndian.Uint64(block[16:]))
		binary.LittleEndian.PutUint64(dst[24:], binary.LittleEndian.Uint64(src[24:])^binary.LittleEndian.Uint64(block[24:]))
		binary.LittleEndian.PutUint64(dst[32:], binary.LittleEndian.Uint64(src[32:])^binary.LittleEndian.Uint64(block[32:]))
		binary.LittleEndian.PutUint64(dst[40:], binary.LittleEndian.Uint64(src[40:])^binary.LittleEndian.Uint64(block[40:]))
		binary.LittleEndian.PutUint64(dst[48:], binary.LittleEndian.Uint64(src[48:])^binary.LittleEndian.Uint64(block[48:]))
		binary.LittleEndian.PutUint64(dst[56:], binary.LittleEndian.Uint64(src[56:])^binary.LittleEndian.Uint64(block[56:]))
		dst, src = dst[64:], src[64:]
	}
	if len(src) > 0 {
		chachaBlock(&block, key, counter, nonce)
		subtle.XORBytes(dst, src, block[:len(src)])
	}
}

// poly1305 is the one-time authenticator of RFC 8439 section 2.5, with the
// accumulator in three 64-bit limbs.
type poly1305 struct {
	r0, r1     uint64
	s0, s1     uint64
	h0, h1, h2 uint64
}

// init starts the authenticator with the one-time key of the AEAD, block
// zero's first 32 bytes (RFC 8439 section 2.8).
func (p *poly1305) init(key *[32]byte) {
	p.r0 = binary.LittleEndian.Uint64(key[0:]) & 0x0ffffffc0fffffff
	p.r1 = binary.LittleEndian.Uint64(key[8:]) & 0x0ffffffc0ffffffc
	p.s0 = binary.LittleEndian.Uint64(key[16:])
	p.s1 = binary.LittleEndian.Uint64(key[24:])
}

// step absorbs one block given as its two 64-bit words, which is how both a
// full one and a padded final one are read.
func (p *poly1305) step(m0, m1, hibit uint64) {
	var c uint64
	p.h0, c = bits.Add64(p.h0, m0, 0)
	p.h1, c = bits.Add64(p.h1, m1, c)
	p.h2 += c + hibit

	// h *= r, with h2 small enough that its products fit in 64 bits.
	h0r0hi, h0r0lo := bits.Mul64(p.h0, p.r0)
	h1r0hi, h1r0lo := bits.Mul64(p.h1, p.r0)
	h0r1hi, h0r1lo := bits.Mul64(p.h0, p.r1)
	h1r1hi, h1r1lo := bits.Mul64(p.h1, p.r1)
	h2r0 := p.h2 * p.r0
	h2r1 := p.h2 * p.r1

	m1lo, c := bits.Add64(h1r0lo, h0r1lo, 0)
	m1hi, _ := bits.Add64(h1r0hi, h0r1hi, c)
	m2lo, c := bits.Add64(h2r0, h1r1lo, 0)
	m2hi, _ := bits.Add64(0, h1r1hi, c)
	m3 := h2r1

	t0 := h0r0lo
	t1, c := bits.Add64(m1lo, h0r0hi, 0)
	t2, c := bits.Add64(m2lo, m1hi, c)
	t3, _ := bits.Add64(m3, m2hi, c)

	// Reduce modulo 2^130 - 5: what lies above bit 130 comes back times 5,
	// as 4x plus x.
	p.h0, p.h1, p.h2 = t0, t1, t2&3
	cc0, cc1 := t2&^3, t3
	p.h0, c = bits.Add64(p.h0, cc0, 0)
	p.h1, c = bits.Add64(p.h1, cc1, c)
	p.h2 += c
	cc0 = cc0>>2 | cc1<<62
	cc1 >>= 2
	p.h0, c = bits.Add64(p.h0, cc0, 0)
	p.h1, c = bits.Add64(p.h1, cc1, c)
	p.h2 += c
}

// write absorbs data, padding a final partial block with zeros, as the AEAD
// construction does for each of its parts: the additional data, the
// ciphertext, and the lengths.
func (p *poly1305) write(data []byte) {
	for len(data) >= 16 {
		p.step(binary.LittleEndian.Uint64(data[0:]), binary.LittleEndian.Uint64(data[8:]), 1)
		data = data[16:]
	}
	if len(data) > 0 {
		var buf [16]byte
		copy(buf[:], data)
		p.step(binary.LittleEndian.Uint64(buf[0:]), binary.LittleEndian.Uint64(buf[8:]), 1)
	}
}

func (p *poly1305) sum(out *[16]byte) {
	// Subtract p = 2^130 - 5 once if h >= p.
	t0, b := bits.Sub64(p.h0, 0xfffffffffffffffb, 0)
	t1, b := bits.Sub64(p.h1, 0xffffffffffffffff, b)
	_, b = bits.Sub64(p.h2, 3, b)
	mask := b - 1 // all ones when there was no borrow, so h >= p
	h0 := p.h0&^mask | t0&mask
	h1 := p.h1&^mask | t1&mask
	h0, c := bits.Add64(h0, p.s0, 0)
	h1, _ = bits.Add64(h1, p.s1, c)
	binary.LittleEndian.PutUint64(out[0:], h0)
	binary.LittleEndian.PutUint64(out[8:], h1)
}

// chacha20Poly1305 is the AEAD of RFC 8439 section 2.8.
type chacha20Poly1305 struct {
	key [8]uint32
}

func newChaCha20Poly1305(key []byte) (cipher.AEAD, error) {
	if len(key) != chachaKeySize {
		return nil, errors.New("quic: bad ChaCha20-Poly1305 key size")
	}
	return &chacha20Poly1305{key: chachaKey(key)}, nil
}

func (*chacha20Poly1305) NonceSize() int { return chachaNonceSize }
func (*chacha20Poly1305) Overhead() int  { return poly1305TagSize }

// initPoly starts the authenticator of a packet: block zero of the key
// stream is the one-time poly1305 key (RFC 8439 section 2.8).
func (a *chacha20Poly1305) initPoly(p *poly1305, nonce *[3]uint32) {
	var block [64]byte
	chachaBlock(&block, &a.key, 0, nonce)
	var key [32]byte
	copy(key[:], block[:32])
	p.init(&key)
}

// tag computes the tag over what was written, and the lengths of the two
// parts.
func (p *poly1305) tag(out *[16]byte, additionalData, ciphertext []byte) {
	var lens [16]byte
	binary.LittleEndian.PutUint64(lens[0:], uint64(len(additionalData)))
	binary.LittleEndian.PutUint64(lens[8:], uint64(len(ciphertext)))
	p.write(lens[:])
	p.sum(out)
}

func (a *chacha20Poly1305) Seal(dst, nonce, plaintext, additionalData []byte) []byte {
	n := chachaNonce(nonce)
	ret, out := sliceForAppend(dst, len(plaintext)+poly1305TagSize)
	var p poly1305
	a.initPoly(&p, &n)
	p.write(additionalData)
	chachaXOR(out, plaintext, &a.key, 1, &n)
	p.write(out[:len(plaintext)])
	var tag [16]byte
	p.tag(&tag, additionalData, out[:len(plaintext)])
	copy(out[len(plaintext):], tag[:])
	return ret
}

var errOpen = errors.New("quic: message authentication failed")

func (a *chacha20Poly1305) Open(dst, nonce, ciphertext, additionalData []byte) ([]byte, error) {
	if len(ciphertext) < poly1305TagSize {
		return nil, errOpen
	}
	n := chachaNonce(nonce)
	body := ciphertext[:len(ciphertext)-poly1305TagSize]
	var p poly1305
	a.initPoly(&p, &n)
	p.write(additionalData)
	p.write(body)
	var tag [16]byte
	p.tag(&tag, additionalData, body)
	if subtle.ConstantTimeCompare(tag[:], ciphertext[len(body):]) != 1 {
		return nil, errOpen
	}
	ret, out := sliceForAppend(dst, len(body))
	chachaXOR(out, body, &a.key, 1, &n)
	return ret, nil
}

// sliceForAppend extends in by n bytes, returning the whole and the tail.
func sliceForAppend(in []byte, n int) (head, tail []byte) {
	if total := len(in) + n; cap(in) >= total {
		head = in[:total]
	} else {
		head = make([]byte, total)
		copy(head, in)
	}
	return head, head[len(in):]
}

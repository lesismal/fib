package rtmp

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"hash"
)

// Handshake sizes: C0/S0 is the version byte, C1/S1 and C2/S2 1536 bytes.
const (
	handshakeSize = 1536
	rtmpVersion   = 3
)

// ErrHandshake is a handshake that cannot be completed: an RTMP version
// other than 3, which includes RTMPE's encrypted handshake.
var ErrHandshake = errors.New("rtmp: handshake failed")

// The Flash digest handshake ("complex handshake") keys: Adobe's server and
// player keys, of which the first 36 and 30 bytes, their text, sign S1 and
// C1, and all of which, through the peer's digest, sign S2 and C2.
var (
	fmsKey = []byte("Genuine Adobe Flash Media Server 001" +
		"\xf0\xee\xc2\x4a\x80\x68\xbe\xe8\x2e\x00\xd0\xd1\x02\x9e\x7e\x57" +
		"\x6e\xec\x5d\x2d\x29\x80\x6f\xab\x93\xb8\xe6\x36\xcf\xeb\x31\xae")
	fpKey = []byte("Genuine Adobe Flash Player 001" +
		"\xf0\xee\xc2\x4a\x80\x68\xbe\xe8\x2e\x00\xd0\xd1\x02\x9e\x7e\x57" +
		"\x6e\xec\x5d\x2d\x29\x80\x6f\xab\x93\xb8\xe6\x36\xcf\xeb\x31\xae")
	fmsKeyText = fmsKey[:36]
	fpKeyText  = fpKey[:30]
)

// The versions a peer writes in C1/S1 bytes 4 to 7 to announce the digest
// handshake: Flash Player 9.0.124.2's, and Flash Media Server 4.5's.
var (
	clientVersion = []byte{0x80, 0x00, 0x07, 0x02}
	serverVersion = []byte{0x04, 0x05, 0x00, 0x01}
)

// digestOffset returns where the 32-byte digest of a C1 or S1 sits in
// the given scheme: scheme 1 puts the digest block first, at byte 8, and
// scheme 0 after the key block, at byte 772. The four bytes at the start
// of the block, summed, place the digest within it.
func digestOffset(b []byte, scheme int) int {
	base := 8
	if scheme == 0 {
		base = 772
	}
	sum := int(b[base]) + int(b[base+1]) + int(b[base+2]) + int(b[base+3])
	return base + 4 + sum%728
}

// digest is HMAC-SHA256 under key of b without the 32 bytes at off.
func digest(mac hash.Hash, b []byte, off int) []byte {
	mac.Reset()
	mac.Write(b[:off])
	mac.Write(b[off+32:])
	return mac.Sum(nil)
}

// findDigest looks for a valid digest in a C1 or S1 signed with key, in
// either scheme, and returns the scheme and the digest.
func findDigest(b, key []byte) (int, []byte, bool) {
	mac := hmac.New(sha256.New, key)
	for _, scheme := range []int{1, 0} {
		off := digestOffset(b, scheme)
		if d := digest(mac, b, off); hmac.Equal(d, b[off:off+32]) {
			return scheme, d, true
		}
	}
	return 0, nil, false
}

// signPacket fills a C1 or S1 with random bytes after its version and
// puts its digest where scheme says.
func signPacket(b, version, key []byte, scheme int) {
	_, _ = rand.Read(b[8:])
	copy(b[4:8], version)
	off := digestOffset(b, scheme)
	copy(b[off:], digest(hmac.New(sha256.New, key), b, off))
}

// answer builds a C2 or S2 answering the peer's digest: random bytes,
// signed in their last 32 by a key derived from the peer's digest.
func answer(b, key, peerDigest []byte) {
	_, _ = rand.Read(b[:handshakeSize-32])
	tmp := hmac.New(sha256.New, key)
	tmp.Write(peerDigest)
	mac := hmac.New(sha256.New, tmp.Sum(nil))
	mac.Write(b[:handshakeSize-32])
	copy(b[handshakeSize-32:], mac.Sum(nil))
}

// serverHandshake answers C0 and C1 with S0, S1 and S2. A C1 that carries
// a valid Flash Player digest gets the digest handshake that Flash and
// Adobe-derived clients check; any other gets the simple one, S2 echoing
// C1, which is all ffmpeg, OBS and librtmp need.
func serverHandshake(c0c1 []byte) ([]byte, error) {
	if c0c1[0] != rtmpVersion {
		return nil, ErrHandshake
	}
	c1 := c0c1[1:]
	out := make([]byte, 1+2*handshakeSize)
	out[0] = rtmpVersion
	s1, s2 := out[1:1+handshakeSize], out[1+handshakeSize:]
	if !bytes.Equal(c1[4:8], []byte{0, 0, 0, 0}) {
		if scheme, d, ok := findDigest(c1, fpKeyText); ok {
			signPacket(s1, serverVersion, fmsKeyText, scheme)
			answer(s2, fmsKey, d)
			return out, nil
		}
	}
	_, _ = rand.Read(s1[8:])
	copy(s2, c1)
	return out, nil
}

// clientHello builds C0 and C1, with a Flash Player digest unless simple.
func clientHello(simple bool) []byte {
	out := make([]byte, 1+handshakeSize)
	out[0] = rtmpVersion
	if simple {
		_, _ = rand.Read(out[9:])
		return out
	}
	signPacket(out[1:], clientVersion, fpKeyText, 1)
	return out
}

// clientAnswer checks S0 and builds C2 for S1: signed when S1 carries a
// valid server digest, and S1 echoed otherwise, which is what a server
// using the simple handshake (ffmpeg's among them) compares C2 with.
func clientAnswer(s0s1 []byte) ([]byte, error) {
	if s0s1[0] != rtmpVersion {
		return nil, ErrHandshake
	}
	s1 := s0s1[1 : 1+handshakeSize]
	c2 := make([]byte, handshakeSize)
	if _, d, ok := findDigest(s1, fmsKeyText); ok {
		answer(c2, fpKey, d)
		return c2, nil
	}
	copy(c2, s1)
	return c2, nil
}

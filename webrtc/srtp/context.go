package srtp

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
)

var (
	ErrProfile      = errors.New("srtp: unsupported protection profile")
	ErrKeyLength    = errors.New("srtp: master key or salt of the wrong length")
	ErrShortPacket  = errors.New("srtp: packet too short")
	ErrMalformed    = errors.New("srtp: malformed packet")
	ErrAuthFailed   = errors.New("srtp: authentication failed")
	ErrReplayed     = errors.New("srtp: packet replayed or too old")
	ErrIndexExhaust = errors.New("srtp: SRTCP index exhausted")
)

// DefaultReplayWindow is the size of the replay windows a Context keeps
// unless Options say otherwise, in packets.
const DefaultReplayWindow = 64

// Options tune a Context.
type Options struct {
	// ReplayWindow is how many SRTP packets behind the newest one received
	// from an SSRC are still accepted, once each; 0 means
	// DefaultReplayWindow.
	ReplayWindow int
	// RTCPReplayWindow is the same for SRTCP; 0 means DefaultReplayWindow.
	RTCPReplayWindow int
}

// The labels of RFC 3711 section 4.3.1 key derivation.
const (
	labelRTPEncryption  = 0
	labelRTPAuth        = 1
	labelRTPSalt        = 2
	labelRTCPEncryption = 3
	labelRTCPAuth       = 4
	labelRTCPSalt       = 5
)

// authKeyLen is the length of HMAC-SHA1 session authentication keys.
const authKeyLen = 20

// sessionKeys are the keys of one of SRTP or SRTCP.
type sessionKeys struct {
	block cipher.Block
	gcm   cipher.AEAD
	mac   hash.Hash
	salt  [14]byte
}

// Context protects or unprotects the SRTP and SRTCP packets of one side
// of a session. One Context should do one or the other: the state it keeps
// for each SSRC is kept apart for the two, but a session uses different
// keys for each direction.
type Context struct {
	profile    Profile
	rtpTagLen  int
	rtcpTagLen int
	rtp, rtcp  sessionKeys

	replayWindow, rtcpReplayWindow int

	rtpOut  map[uint32]*rtpState
	rtpIn   map[uint32]*rtpState
	rtcpOut map[uint32]uint32
	rtcpIn  map[uint32]*replayWindow

	// Scratch space reused by every packet.
	iv        [16]byte
	sum       [sha1.Size]byte
	trailer   [12]byte
	keystream []byte
	aad       []byte
}

// rtpState is what a Context keeps for an SSRC it sends or receives SRTP
// from: the rollover counter and highest sequence number of RFC 3711
// section 3.3.1 (s_l) and, for one it receives, the replay window.
type rtpState struct {
	roc     uint32
	lastSeq uint16
	started bool
	replay  replayWindow
}

// NewContext returns a Context for the profile with the master key and
// salt. opts may be nil.
func NewContext(profile Profile, masterKey, masterSalt []byte, opts *Options) (*Context, error) {
	if !profile.Valid() {
		return nil, ErrProfile
	}
	if len(masterKey) != profile.KeyLen() || len(masterSalt) != profile.SaltLen() {
		return nil, ErrKeyLength
	}
	c := &Context{
		profile:          profile,
		rtpTagLen:        profile.RTPOverhead(),
		rtcpTagLen:       profile.RTCPOverhead() - 4,
		replayWindow:     DefaultReplayWindow,
		rtcpReplayWindow: DefaultReplayWindow,
		rtpOut:           make(map[uint32]*rtpState),
		rtpIn:            make(map[uint32]*rtpState),
		rtcpOut:          make(map[uint32]uint32),
		rtcpIn:           make(map[uint32]*replayWindow),
	}
	if opts != nil {
		if opts.ReplayWindow > 0 {
			c.replayWindow = opts.ReplayWindow
		}
		if opts.RTCPReplayWindow > 0 {
			c.rtcpReplayWindow = opts.RTCPReplayWindow
		}
	}
	// AES takes the key, whose length the profile checked, and GCM the
	// standard library's AES block, without error.
	master, _ := aes.NewCipher(masterKey)
	c.rtp.init(profile, master, masterSalt, labelRTPEncryption)
	c.rtcp.init(profile, master, masterSalt, labelRTCPEncryption)
	return c, nil
}

// NewContexts splits the keying material DTLS-SRTP exported with
// ExporterLabel, profile.KeyingMaterialLen() bytes of it, into the
// client's and the server's write keys and salts (RFC 5764 section 4.2),
// and returns a Context that protects what this side sends with its own
// and one that unprotects what it receives with the peer's. isDTLSClient
// tells which side of the DTLS handshake this one was.
func NewContexts(keyingMaterial []byte, profile Profile, isDTLSClient bool, opts *Options) (local, remote *Context, err error) {
	if !profile.Valid() {
		return nil, nil, ErrProfile
	}
	keyLen, saltLen := profile.KeyLen(), profile.SaltLen()
	if len(keyingMaterial) != profile.KeyingMaterialLen() {
		return nil, nil, fmt.Errorf("%w: keying material is %d bytes, want %d", ErrKeyLength, len(keyingMaterial), profile.KeyingMaterialLen())
	}
	clientKey := keyingMaterial[:keyLen]
	serverKey := keyingMaterial[keyLen : 2*keyLen]
	clientSalt := keyingMaterial[2*keyLen : 2*keyLen+saltLen]
	serverSalt := keyingMaterial[2*keyLen+saltLen:]
	localKey, localSalt, remoteKey, remoteSalt := clientKey, clientSalt, serverKey, serverSalt
	if !isDTLSClient {
		localKey, localSalt, remoteKey, remoteSalt = serverKey, serverSalt, clientKey, clientSalt
	}
	// The profile and the lengths are checked, so NewContext succeeds.
	local, _ = NewContext(profile, localKey, localSalt, opts)
	remote, _ = NewContext(profile, remoteKey, remoteSalt, opts)
	return local, remote, nil
}

// deriveKey returns n bytes of the AES-CM PRF of RFC 3711 section 4.3.3
// for the label, keyed with the master key: the master salt with the label
// XORed in at its eighth byte, as the high bytes of a counter run through
// AES. With a key derivation rate of zero the index plays no part.
func deriveKey(master cipher.Block, masterSalt []byte, label byte, n int) []byte {
	var in [16]byte
	copy(in[:], masterSalt)
	in[7] ^= label
	out := make([]byte, (n+15)&^15)
	for i := 0; i < len(out); i += 16 {
		binary.BigEndian.PutUint16(in[14:], uint16(i/16))
		master.Encrypt(out[i:], in[:])
	}
	return out[:n]
}

func (k *sessionKeys) init(profile Profile, master cipher.Block, masterSalt []byte, encLabel byte) {
	key := deriveKey(master, masterSalt, encLabel, profile.KeyLen())
	salt := deriveKey(master, masterSalt, encLabel+2, profile.SaltLen())
	copy(k.salt[:], salt)
	block, _ := aes.NewCipher(key)
	if profile.aead() {
		k.gcm, _ = cipher.NewGCM(block)
		return
	}
	k.block = block
	k.mac = hmac.New(sha1.New, deriveKey(master, masterSalt, encLabel+1, authKeyLen))
}

// sliceForAppend extends dst by n bytes and returns the whole and the part
// added, reusing dst's capacity when it is enough.
func sliceForAppend(dst []byte, n int) (whole, tail []byte) {
	if total := len(dst) + n; cap(dst) >= total {
		whole = dst[:total]
	} else {
		whole = make([]byte, total, total+total/8)
		copy(whole, dst)
	}
	return whole, whole[len(dst):]
}

// xorCTR runs AES in counter mode over src into dst, starting from the
// counter block c.iv whose last two bytes are zero. It builds the key
// stream block by block in a buffer the Context keeps rather than take a
// cipher.Stream, which crypto/cipher allocates anew for every IV.
func (c *Context) xorCTR(block cipher.Block, dst, src []byte) {
	n := (len(src) + 15) &^ 15
	if cap(c.keystream) < n {
		c.keystream = make([]byte, n, max(n, 1504))
	}
	ks := c.keystream[:n]
	for i := 0; i < n; i += 16 {
		b := ks[i : i+16 : i+16]
		copy(b, c.iv[:14])
		binary.BigEndian.PutUint16(b[14:], uint16(i>>4))
		block.Encrypt(b, b)
	}
	subtle.XORBytes(dst, src, ks[:len(src)])
}

// ctrIV sets c.iv to the AES-CM IV of RFC 3711 section 4.1.1 for the SSRC
// and a 48-bit packet index: (salt * 2^16) XOR (SSRC * 2^64) XOR
// (index * 2^16).
func (c *Context) ctrIV(salt *[14]byte, ssrc uint32, index uint64) {
	copy(c.iv[:14], salt[:])
	c.iv[14], c.iv[15] = 0, 0
	v := binary.BigEndian.Uint32(c.iv[4:]) ^ ssrc
	binary.BigEndian.PutUint32(c.iv[4:], v)
	hi := binary.BigEndian.Uint16(c.iv[8:]) ^ uint16(index>>32)
	lo := binary.BigEndian.Uint32(c.iv[10:]) ^ uint32(index)
	binary.BigEndian.PutUint16(c.iv[8:], hi)
	binary.BigEndian.PutUint32(c.iv[10:], lo)
}

// gcmIV sets c.iv[:12] to the 12-byte IV of RFC 7714 sections 8.1 and 9.1:
// two zero bytes, the SSRC, then the ROC and sequence number for SRTP or
// two zero bytes and the SRTCP index, all XORed with the salt.
func (c *Context) gcmIV(salt *[14]byte, ssrc, hi uint32, lo uint16) []byte {
	iv := c.iv[:12]
	iv[0], iv[1] = 0, 0
	binary.BigEndian.PutUint32(iv[2:], ssrc)
	binary.BigEndian.PutUint32(iv[6:], hi)
	binary.BigEndian.PutUint16(iv[10:], lo)
	subtle.XORBytes(iv, iv, salt[:12])
	return iv
}

// hmacTag computes the HMAC-SHA1 of the parts with the key k into c.sum.
func (c *Context) hmacTag(k *sessionKeys, a, b []byte) []byte {
	k.mac.Reset()
	k.mac.Write(a)
	if len(b) > 0 {
		k.mac.Write(b)
	}
	return k.mac.Sum(c.sum[:0])
}

// SetROC sets the rollover counter of an SSRC, for a stream joined after
// its sequence numbers have wrapped. The next packet sent or received
// from it then takes this counter and its own sequence number as the
// starting point.
func (c *Context) SetROC(ssrc, roc uint32) {
	for _, m := range []map[uint32]*rtpState{c.rtpOut, c.rtpIn} {
		s := m[ssrc]
		if s == nil {
			s = c.newRTPState()
			m[ssrc] = s
		}
		s.roc = roc
		s.started = false
	}
}

// ROC returns the rollover counter of an SSRC this Context has sent, or
// else received, packets from.
func (c *Context) ROC(ssrc uint32) (uint32, bool) {
	if s := c.rtpOut[ssrc]; s != nil {
		return s.roc, true
	}
	if s := c.rtpIn[ssrc]; s != nil {
		return s.roc, true
	}
	return 0, false
}

// RemoveSSRC forgets the state kept for an SSRC.
func (c *Context) RemoveSSRC(ssrc uint32) {
	delete(c.rtpOut, ssrc)
	delete(c.rtpIn, ssrc)
	delete(c.rtcpOut, ssrc)
	delete(c.rtcpIn, ssrc)
}

// Profile returns the Context's protection profile.
func (c *Context) Profile() Profile { return c.profile }

func (c *Context) newRTPState() *rtpState {
	s := &rtpState{}
	s.replay.init(c.replayWindow)
	return s
}

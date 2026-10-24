package quic

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"net"
	"time"
)

// retryTokenLifetime bounds how long a Retry's token is good for. The client
// sends it back with its next Initial, a round trip after the Retry.
const retryTokenLifetime = 10 * time.Second

// TokenKey makes and checks the tokens a server's Retry packets carry (RFC
// 9000 section 8.1.2). A client sends the token back with its next Initial,
// which proves that the address the Initial comes from is the client's own:
// a server that answers a client's first Initial with a Retry does no TLS
// work, and keeps no state, for an Initial whose source address is forged.
// It costs the client a round trip, so a server sends Retry only when it has
// to, as under a flood of Initials. A TokenKey is safe for concurrent use.
type TokenKey struct {
	aead cipher.AEAD
}

// NewTokenKey returns a key of its own, random: tokens are good only for
// the server that made them, and only until it restarts.
func NewTokenKey() *TokenKey {
	var k [32]byte
	_, _ = rand.Read(k[:])
	block, err := aes.NewCipher(k[:])
	if err != nil {
		panic(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		panic(err)
	}
	return &TokenKey{aead: aead}
}

// TokenStatus is what the token of a client's Initial says; see
// TokenKey.Check.
type TokenStatus int

const (
	// TokenMissing is an Initial without a token the key made: the
	// client's first, or one whose token came from elsewhere. A Retry
	// would ask it for one.
	TokenMissing TokenStatus = iota
	// TokenValid is an Initial whose token the key made for the address it
	// came from, a moment ago.
	TokenValid
	// TokenInvalid is an Initial whose token the key made, but for another
	// address or too long ago. The client has had its Retry and takes no
	// other, so Accept answers it with INVALID_TOKEN.
	TokenInvalid
)

// Retry returns the Retry packet that answers the client Initial in
// datagram, or nil if datagram is not one. The token in it names the
// connection ID the client first sent to, which the server has to give
// back in its transport parameters, and the client's address.
func (k *TokenKey) Retry(datagram []byte, remote net.Addr) []byte {
	h, err := parseHeader(datagram, cidLen)
	if err != nil || h.typ != packetInitial || len(h.dcid) < 8 {
		return nil
	}
	scid := randomCID()
	out := make([]byte, 0, 7+len(h.scid)+len(scid)+128)
	var r [1]byte
	_, _ = rand.Read(r[:])
	out = append(out, 0xc0|packetRetry<<4|r[0]&0x0f)
	out = binary.BigEndian.AppendUint32(out, Version1)
	out = append(out, byte(len(h.scid)))
	out = append(out, h.scid...)
	out = append(out, byte(len(scid)))
	out = append(out, scid...)
	out = k.appendToken(out, remote, h.dcid, scid)
	return append(out, retryTag(h.dcid, out)...)
}

// appendToken appends the token for a client at remote whose first
// destination connection ID was odcid, and whose next one is scid, which
// binds it: the time it was made, the address, and odcid, sealed.
func (k *TokenKey) appendToken(out []byte, remote net.Addr, odcid, scid []byte) []byte {
	addr := addrBytes(remote)
	plain := make([]byte, 0, 8+1+len(addr)+len(odcid))
	plain = binary.BigEndian.AppendUint64(plain, uint64(time.Now().UnixNano()))
	plain = append(plain, byte(len(addr)))
	plain = append(plain, addr...)
	plain = append(plain, odcid...)
	nonce := make([]byte, k.aead.NonceSize())
	_, _ = rand.Read(nonce)
	out = append(out, nonce...)
	return k.aead.Seal(out, nonce, plain, scid)
}

// addrBytes is what a token keeps of remote, for the Initial that brings it
// back to be checked against.
func addrBytes(remote net.Addr) []byte {
	if remote == nil {
		return nil
	}
	if u, ok := remote.(*net.UDPAddr); ok {
		b := u.AddrPort()
		out, _ := b.MarshalBinary()
		return out
	}
	s := remote.String()
	return []byte(s[:min(len(s), 255)])
}

// Check reports what the token in the client Initial in datagram, from
// remote, says.
func (k *TokenKey) Check(datagram []byte, remote net.Addr) TokenStatus {
	h, err := parseHeader(datagram, cidLen)
	if err != nil || h.typ != packetInitial {
		return TokenMissing
	}
	status, _ := k.check(h.token, remote, h.dcid)
	return status
}

// check opens token, from an Initial sent from remote to dcid, and returns
// the connection ID the client first sent to, which a valid token names.
func (k *TokenKey) check(token []byte, remote net.Addr, dcid []byte) (TokenStatus, []byte) {
	n := k.aead.NonceSize()
	if len(token) < n+k.aead.Overhead() {
		return TokenMissing, nil
	}
	plain, err := k.aead.Open(nil, token[:n], token[n:], dcid)
	if err != nil || len(plain) < 9 || len(plain) < 9+int(plain[8]) {
		return TokenMissing, nil
	}
	made := time.Unix(0, int64(binary.BigEndian.Uint64(plain)))
	addr, odcid := plain[9:9+int(plain[8])], plain[9+int(plain[8]):]
	if !bytes.Equal(addr, addrBytes(remote)) || time.Since(made) > retryTokenLifetime || len(odcid) < 8 {
		return TokenInvalid, nil
	}
	return TokenValid, odcid
}

// sendInvalidToken answers a client Initial whose Retry token is not good
// with a CONNECTION_CLOSE carrying INVALID_TOKEN, under the Initial keys of
// the connection ID the client sent it to, through pc (RFC 9000 section
// 8.1.3): the client takes no second Retry, and would otherwise wait for
// its handshake to time out. The connection that sends it is a stand-in,
// with no TLS, which nothing else sees.
func sendInvalidToken(pc PacketConn, h header) {
	c := newConn(pc, nil, Config{}, nil, false)
	c.dcid = bytes.Clone(h.scid)
	c.spaces[spaceInitial].tx, c.spaces[spaceInitial].rx = initialKeys(h.dcid, false)
	c.sendClose(transportErr(errInvalidToken, "invalid Retry token"))
}

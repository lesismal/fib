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

// newTokenLifetime bounds how long the token of a NEW_TOKEN frame is good
// for: long enough for the connections a client makes to a server soon
// after, short enough that a token an attacker finds, or one made with a key
// the server has since changed, is not good for long. A client sends it back
// with the first Initial of a later connection from the same address (RFC
// 9000 section 8.1.3).
const newTokenLifetime = 10 * time.Minute

// TokenKey makes and checks the tokens a server's Retry packets carry (RFC
// 9000 section 8.1.2), and those of its NEW_TOKEN frames (section 8.1.3). A
// client sends the token of a Retry back with its next Initial, which proves
// that the address the Initial comes from is the client's own: a server that
// answers a client's first Initial with a Retry does no TLS work, and keeps
// no state, for an Initial whose source address is forged. The token of a
// NEW_TOKEN frame proves the same for a later connection the client makes
// from the same address, without the round trip a Retry costs. A TokenKey is
// safe for concurrent use.
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

// TokenKind says what a token was made for. Its values are on the wire, as
// the first byte of the token, so they are not to be renumbered.
type TokenKind int

const (
	// TokenKindNone is not a token this key made: one from another server,
	// or no token at all.
	TokenKindNone TokenKind = iota
	// TokenKindRetry is the token of a Retry packet, which the client sends
	// back with its next Initial (RFC 9000 section 8.1.2).
	TokenKindRetry
	// TokenKindNew is the token of a NEW_TOKEN frame, which the client sends
	// back with the first Initial of a later connection from the same
	// address (RFC 9000 section 8.1.3).
	TokenKindNew
)

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
	// address or too long ago. A Retry token there takes no other, so
	// Accept answers it with INVALID_TOKEN; one of a NEW_TOKEN frame is no
	// token at all, and the client is treated as one without (see
	// TokenKey.CheckToken).
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
	out = k.appendToken(out, remote, TokenKindRetry, h.dcid, scid, time.Now())
	return append(out, retryTag(h.dcid, out)...)
}

// NewToken returns the token of a NEW_TOKEN frame for a client at remote: a
// client that sends it back in the first Initial of a later connection from
// the same address has that address taken as its own, without a Retry (RFC
// 9000 section 8.1.3). The token is good for newTokenLifetime, and the
// server keeps no state for it.
func (k *TokenKey) NewToken(remote net.Addr) []byte {
	return k.appendToken(nil, remote, TokenKindNew, nil, nil, time.Now())
}

// appendToken appends the token for a client at remote whose first
// destination connection ID was odcid, and whose next one is scid, which
// binds a Retry's token; both are nil for a NEW_TOKEN's, and aad is then nil
// too. The token keeps the time it was made, the address, and odcid, sealed,
// after a byte that says which kind it is.
func (k *TokenKey) appendToken(out []byte, remote net.Addr, kind TokenKind, odcid, aad []byte, now time.Time) []byte {
	addr := addrBytes(remote)
	plain := make([]byte, 0, 8+1+len(addr)+len(odcid))
	plain = binary.BigEndian.AppendUint64(plain, uint64(now.UnixNano()))
	plain = append(plain, byte(len(addr)))
	plain = append(plain, addr...)
	plain = append(plain, odcid...)
	nonce := make([]byte, k.aead.NonceSize())
	_, _ = rand.Read(nonce)
	out = append(out, byte(kind))
	out = append(out, nonce...)
	return k.aead.Seal(out, nonce, plain, aad)
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
// remote, says. A Retry's token and one of a NEW_TOKEN frame are both valid
// to it; CheckToken says which the token was, which a server that answers a
// bad one differently needs to know.
func (k *TokenKey) Check(datagram []byte, remote net.Addr) TokenStatus {
	status, _ := k.CheckToken(datagram, remote)
	return status
}

// CheckToken is Check, with what the token was made for. A server answers an
// Initial whose Retry token is not good with INVALID_TOKEN, since the client
// takes no second Retry (RFC 9000 section 8.1.2); one whose NEW_TOKEN token
// is not good is as if it had none, and its address is validated as any new
// client's (section 8.1.3).
func (k *TokenKey) CheckToken(datagram []byte, remote net.Addr) (TokenStatus, TokenKind) {
	h, err := parseHeader(datagram, cidLen)
	if err != nil || h.typ != packetInitial {
		return TokenMissing, TokenKindNone
	}
	status, kind, _ := k.check(h.token, remote, h.dcid)
	return status, kind
}

// check opens token, from an Initial sent from remote to dcid, and returns
// what it says, what it was made for, and the connection ID the client first
// sent to, which a valid Retry token names.
func (k *TokenKey) check(token []byte, remote net.Addr, dcid []byte) (TokenStatus, TokenKind, []byte) {
	n := k.aead.NonceSize()
	if len(token) < 1+n+k.aead.Overhead() {
		return TokenMissing, TokenKindNone, nil
	}
	kind := TokenKind(token[0])
	var aad []byte
	switch kind {
	case TokenKindRetry:
		aad = dcid
	case TokenKindNew:
	default:
		return TokenMissing, TokenKindNone, nil
	}
	token = token[1:]
	plain, err := k.aead.Open(nil, token[:n], token[n:], aad)
	if err != nil || len(plain) < 9 || len(plain) < 9+int(plain[8]) {
		return TokenMissing, TokenKindNone, nil
	}
	made := time.Unix(0, int64(binary.BigEndian.Uint64(plain)))
	addr, odcid := plain[9:9+int(plain[8])], plain[9+int(plain[8]):]
	lifetime := newTokenLifetime
	if kind == TokenKindRetry {
		lifetime = retryTokenLifetime
	}
	if !bytes.Equal(addr, addrBytes(remote)) || time.Since(made) > lifetime {
		return TokenInvalid, kind, nil
	}
	if kind == TokenKindNew {
		// Nothing else to check, and no original connection ID to give back.
		return TokenValid, kind, nil
	}
	if len(odcid) < 8 {
		return TokenInvalid, kind, nil
	}
	return TokenValid, kind, odcid
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

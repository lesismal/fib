package peer

import (
	"crypto/rand"
	"errors"
	"fmt"
)

// Protocol is the protocol string a handshake begins with, after its
// length byte.
const Protocol = "BitTorrent protocol"

// HandshakeLen is the length of a handshake: the protocol string with its
// length byte, the reserved bytes, the info hash and the peer ID.
const HandshakeLen = 1 + len(Protocol) + 8 + 20 + 20

// HandshakeHeaderLen is the part of a handshake before the peer ID: enough
// for a receiver to know which torrent the initiator wants, and to answer
// before the peer ID arrives.
const HandshakeHeaderLen = HandshakeLen - 20

// Reserved is the handshake's eight reserved bytes, each bit announcing an
// extension.
type Reserved [8]byte

// The reserved bits this package knows, as a byte and a mask: the
// extension protocol (BEP 10) is 0x10 in byte 5; the DHT's port message
// (BEP 5) 0x01, the Fast extension (BEP 6) 0x04 and BEP 52's v2 upgrade
// 0x10 in byte 7.
const (
	reservedExtensionByte, reservedExtensionMask = 5, 0x10
	reservedDHTByte, reservedDHTMask             = 7, 0x01
	reservedFastByte, reservedFastMask           = 7, 0x04
	reservedV2Byte, reservedV2Mask               = 7, 0x10
)

// Extended reports the extension protocol, BEP 10.
func (r Reserved) Extended() bool { return r[reservedExtensionByte]&reservedExtensionMask != 0 }

// DHT reports the DHT's port message, BEP 5.
func (r Reserved) DHT() bool { return r[reservedDHTByte]&reservedDHTMask != 0 }

// Fast reports the Fast extension, BEP 6.
func (r Reserved) Fast() bool { return r[reservedFastByte]&reservedFastMask != 0 }

// V2 reports BEP 52's v2 upgrade bit: the sender, handshaking with a
// hybrid torrent's v1 info hash, would take the truncated v2 one in reply.
func (r Reserved) V2() bool { return r[reservedV2Byte]&reservedV2Mask != 0 }

func (r *Reserved) set(i int, mask byte, on bool) {
	if on {
		r[i] |= mask
	} else {
		r[i] &^= mask
	}
}

// SetExtended sets or clears the extension protocol bit.
func (r *Reserved) SetExtended(on bool) { r.set(reservedExtensionByte, reservedExtensionMask, on) }

// SetDHT sets or clears the DHT bit.
func (r *Reserved) SetDHT(on bool) { r.set(reservedDHTByte, reservedDHTMask, on) }

// SetFast sets or clears the Fast extension bit.
func (r *Reserved) SetFast(on bool) { r.set(reservedFastByte, reservedFastMask, on) }

// SetV2 sets or clears the v2 upgrade bit.
func (r *Reserved) SetV2(on bool) { r.set(reservedV2Byte, reservedV2Mask, on) }

// Handshake is the first message each side sends (BEP 3).
type Handshake struct {
	Reserved Reserved
	// InfoHash is the torrent's v1 info hash, or its v2 info hash
	// truncated to 20 bytes.
	InfoHash [20]byte
	PeerID   [20]byte
}

// ErrHandshake is a handshake that does not begin with the protocol
// string.
var ErrHandshake = errors.New("peer: not a BitTorrent handshake")

// AppendHandshake appends h to dst.
func AppendHandshake(dst []byte, h *Handshake) []byte {
	dst = append(dst, byte(len(Protocol)))
	dst = append(dst, Protocol...)
	dst = append(dst, h.Reserved[:]...)
	dst = append(dst, h.InfoHash[:]...)
	return append(dst, h.PeerID[:]...)
}

// ParseHandshakeHeader parses the first HandshakeHeaderLen bytes of a
// handshake into h, all but the peer ID.
func ParseHandshakeHeader(b []byte, h *Handshake) error {
	if len(b) < HandshakeHeaderLen {
		return fmt.Errorf("peer: handshake of %d bytes", len(b))
	}
	if b[0] != byte(len(Protocol)) || string(b[1:1+len(Protocol)]) != Protocol {
		return ErrHandshake
	}
	b = b[1+len(Protocol):]
	copy(h.Reserved[:], b)
	copy(h.InfoHash[:], b[8:])
	return nil
}

// ParseHandshake parses the first HandshakeLen bytes of b.
func ParseHandshake(b []byte) (Handshake, error) {
	var h Handshake
	if len(b) < HandshakeLen {
		return h, fmt.Errorf("peer: handshake of %d bytes", len(b))
	}
	if err := ParseHandshakeHeader(b, &h); err != nil {
		return h, err
	}
	copy(h.PeerID[:], b[HandshakeHeaderLen:])
	return h, nil
}

// DefaultPeerIDPrefix is the Azureus-style prefix of the peer IDs
// NewPeerID makes by default: client "FB", fib, version 0.0.0.1.
const DefaultPeerIDPrefix = "-FB0001-"

// peerIDChars are what the random part of a peer ID is made of: printable,
// as most clients' are, so that logs and trackers show it readably.
const peerIDChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// NewPeerID returns a peer ID that begins with prefix, an Azureus-style
// "-XXvvvv-" one such as DefaultPeerIDPrefix (or DefaultPeerIDPrefix when
// empty), and goes on with random letters and digits. A prefix longer than
// 20 bytes is cut.
func NewPeerID(prefix string) [20]byte {
	if prefix == "" {
		prefix = DefaultPeerIDPrefix
	}
	var id [20]byte
	n := copy(id[:], prefix)
	var r [20]byte
	_, _ = rand.Read(r[:])
	for i := n; i < len(id); i++ {
		id[i] = peerIDChars[int(r[i])%len(peerIDChars)]
	}
	return id
}

// PeerIDClient returns the client code and version of an Azureus-style
// peer ID, "-XXvvvv-...", such as "FB" and "0001", and false for another
// style.
func PeerIDClient(id [20]byte) (client, version string, ok bool) {
	if id[0] != '-' || id[7] != '-' {
		return "", "", false
	}
	for _, c := range id[1:7] {
		if c < '0' || c > 'z' {
			return "", "", false
		}
	}
	return string(id[1:3]), string(id[3:7]), true
}

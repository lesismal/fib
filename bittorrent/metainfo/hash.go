package metainfo

import (
	"encoding/base32"
	"encoding/hex"
	"errors"
	"strings"
)

// InfoHash is a v1 info hash: the SHA-1 of a torrent's info dictionary.
// It is also the 20-byte form a v2 info hash takes in the peer handshake,
// the DHT and trackers (InfoHashV2.Truncate).
type InfoHash [20]byte

// InfoHashV2 is a v2 info hash (BEP 52): the SHA-256 of the info
// dictionary.
type InfoHashV2 [32]byte

// String returns h in lowercase hex.
func (h InfoHash) String() string { return hex.EncodeToString(h[:]) }

// IsZero reports whether h is all zeros, which stands for no hash.
func (h InfoHash) IsZero() bool { return h == InfoHash{} }

// String returns h in lowercase hex.
func (h InfoHashV2) String() string { return hex.EncodeToString(h[:]) }

// IsZero reports whether h is all zeros, which stands for no hash.
func (h InfoHashV2) IsZero() bool { return h == InfoHashV2{} }

// Truncate returns the first 20 bytes of h, which is how a v2 torrent is
// named where a v1 hash's 20 bytes are expected.
func (h InfoHashV2) Truncate() InfoHash {
	var t InfoHash
	copy(t[:], h[:])
	return t
}

// ParseInfoHash parses a v1 info hash in hex (40 characters) or base32
// (32 characters, as old magnet links have it), in either case.
func ParseInfoHash(s string) (InfoHash, error) {
	var h InfoHash
	switch len(s) {
	case 40:
		if _, err := hex.Decode(h[:], []byte(s)); err != nil {
			return h, errors.New("metainfo: invalid hex info hash")
		}
	case 32:
		n, err := base32.StdEncoding.Decode(h[:], []byte(strings.ToUpper(s)))
		if err != nil || n != len(h) {
			return h, errors.New("metainfo: invalid base32 info hash")
		}
	default:
		return h, errors.New("metainfo: info hash is neither 40 hex nor 32 base32 characters")
	}
	return h, nil
}

// ParseInfoHashV2 parses a v2 info hash in hex (64 characters).
func ParseInfoHashV2(s string) (InfoHashV2, error) {
	var h InfoHashV2
	if len(s) != 64 {
		return h, errors.New("metainfo: v2 info hash is not 64 hex characters")
	}
	if _, err := hex.Decode(h[:], []byte(s)); err != nil {
		return h, errors.New("metainfo: invalid hex v2 info hash")
	}
	return h, nil
}

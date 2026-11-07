package peer

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"math/bits"
	"net/netip"
)

// Bitfield is a set of pieces, as a bitfield message carries it: piece 0
// is the high bit of the first byte. The zero Bitfield has no pieces.
type Bitfield struct {
	b []byte
	n int
}

// NewBitfield returns an empty Bitfield of n pieces.
func NewBitfield(n int) *Bitfield {
	return &Bitfield{b: make([]byte, (n+7)/8), n: n}
}

// ErrBitfield is a bitfield message of the wrong length for the torrent,
// or with bits set past its last piece.
var ErrBitfield = errors.New("peer: bitfield does not fit the torrent")

// ParseBitfield returns the Bitfield of n pieces that b, a bitfield
// message's payload, holds, checking that b is (n+7)/8 bytes and that the
// spare bits at the end are clear, as BEP 3 requires. b is copied.
func ParseBitfield(b []byte, n int) (*Bitfield, error) {
	if n < 0 || len(b) != (n+7)/8 {
		return nil, ErrBitfield
	}
	if n%8 != 0 && b[len(b)-1]&(0xff>>(n%8)) != 0 {
		return nil, ErrBitfield
	}
	return &Bitfield{b: append([]byte(nil), b...), n: n}, nil
}

// Len is the number of pieces.
func (f *Bitfield) Len() int { return f.n }

// Bytes is the bitfield as a bitfield message carries it, not a copy.
func (f *Bitfield) Bytes() []byte { return f.b }

// Has reports piece i; false outside the range.
func (f *Bitfield) Has(i int) bool {
	if i < 0 || i >= f.n {
		return false
	}
	return f.b[i>>3]&(0x80>>(i&7)) != 0
}

// Set adds piece i; nothing outside the range.
func (f *Bitfield) Set(i int) {
	if i >= 0 && i < f.n {
		f.b[i>>3] |= 0x80 >> (i & 7)
	}
}

// Clear removes piece i.
func (f *Bitfield) Clear(i int) {
	if i >= 0 && i < f.n {
		f.b[i>>3] &^= 0x80 >> (i & 7)
	}
}

// SetAll adds every piece, as a have all message says.
func (f *Bitfield) SetAll() {
	for i := range f.b {
		f.b[i] = 0xff
	}
	if r := f.n % 8; r != 0 {
		f.b[len(f.b)-1] = 0xff << (8 - r)
	}
}

// ClearAll removes every piece.
func (f *Bitfield) ClearAll() { clear(f.b) }

// Count is the number of pieces in the set.
func (f *Bitfield) Count() int {
	c := 0
	for _, b := range f.b {
		c += bits.OnesCount8(b)
	}
	return c
}

// All reports every piece in the set; a Bitfield of none reports true.
func (f *Bitfield) All() bool { return f.Count() == f.n }

// None reports no piece in the set.
func (f *Bitfield) None() bool { return f.Count() == 0 }

// Clone returns a copy.
func (f *Bitfield) Clone() *Bitfield {
	return &Bitfield{b: append([]byte(nil), f.b...), n: f.n}
}

// Missing appends to dst the pieces of other, a Bitfield of as many pieces,
// that f lacks: what f's owner would download from other's.
func (f *Bitfield) Missing(dst []int, other *Bitfield) []int {
	for i := range min(len(f.b), len(other.b)) {
		x := other.b[i] &^ f.b[i]
		for x != 0 {
			j := bits.LeadingZeros8(x)
			dst = append(dst, i*8+j)
			x &^= 0x80 >> j
		}
	}
	return dst
}

// AllowedFastSet is the set of k pieces of a torrent of numPieces whose
// info hash is infoHash that a peer at addr may request while choked, by
// the algorithm of BEP 6: it depends only on the peer's address (its /24
// for IPv4) and the torrent, so that a peer cannot draw a new set by
// reconnecting. An IPv6 address, which BEP 6 does not cover, is masked to
// its /48, the smallest prefix an end site is usually given, and its 16
// bytes hashed as an IPv4 address's 4 are. Fewer than k pieces makes all
// of them the set.
func AllowedFastSet(addr netip.Addr, infoHash [20]byte, numPieces, k int) []uint32 {
	if numPieces <= 0 || k <= 0 {
		return nil
	}
	if k > numPieces {
		k = numPieces
	}
	var x []byte
	addr = addr.Unmap()
	if addr.Is4() {
		a := addr.As4()
		a[3] = 0
		x = append(x, a[:]...)
	} else {
		a := addr.As16()
		clear(a[6:])
		x = append(x, a[:]...)
	}
	x = append(x, infoHash[:]...)
	set := make([]uint32, 0, k)
	seen := make(map[uint32]bool, k)
	for len(set) < k {
		h := sha1.Sum(x)
		x = h[:]
		for i := 0; i < 5 && len(set) < k; i++ {
			y := binary.BigEndian.Uint32(x[i*4:])
			index := uint32(uint64(y) % uint64(numPieces))
			if !seen[index] {
				seen[index] = true
				set = append(set, index)
			}
		}
	}
	return set
}

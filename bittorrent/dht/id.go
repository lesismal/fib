package dht

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"math/bits"
	"net/netip"
	"slices"
	"strconv"
)

// ID is a node ID, or a target of the DHT's keyspace: an info hash, a BEP
// 44 item's target. Distance between two IDs is their XOR, read as a
// 160-bit unsigned integer.
type ID [20]byte

// RandomID returns an ID drawn from crypto/rand.
func RandomID() ID {
	var id ID
	_, _ = rand.Read(id[:])
	return id
}

// ParseID parses the 40 hex digits of an ID.
func ParseID(s string) (ID, error) {
	var id ID
	if len(s) != 2*len(id) {
		return id, errors.New("dht: an ID is 40 hex digits")
	}
	_, err := hex.Decode(id[:], []byte(s))
	return id, err
}

// String returns the ID in hex.
func (id ID) String() string { return hex.EncodeToString(id[:]) }

// Bytes returns the ID as a slice.
func (id ID) Bytes() []byte { return id[:] }

// IsZero reports whether the ID is all zeros, which stands for "not known"
// in a NodeInfo.
func (id ID) IsZero() bool { return id == ID{} }

// Xor returns the distance between id and o.
func (id ID) Xor(o ID) ID {
	var d ID
	for i := range d {
		d[i] = id[i] ^ o[i]
	}
	return d
}

// Closer reports whether a is closer to id than b is.
func (id ID) Closer(a, b ID) bool { return id.cmpDistance(a, b) < 0 }

// cmpDistance compares the distances of a and b from id.
func (id ID) cmpDistance(a, b ID) int {
	for i := range id {
		da, db := a[i]^id[i], b[i]^id[i]
		if da != db {
			if da < db {
				return -1
			}
			return 1
		}
	}
	return 0
}

// commonPrefixLen returns how many leading bits a and b share: 160 for
// equal IDs.
func commonPrefixLen(a, b ID) int {
	for i := range a {
		if x := a[i] ^ b[i]; x != 0 {
			return i*8 + bits.LeadingZeros8(x)
		}
	}
	return 160
}

// randomIDWithPrefix returns a random ID that shares exactly n leading bits
// with id (its bit n flipped), or, with exact false, at least n.
func randomIDWithPrefix(id ID, n int, exact bool) ID {
	r := RandomID()
	for i := 0; i < n && i < 160; i++ {
		setBit(&r, i, bit(id, i))
	}
	if exact && n < 160 {
		setBit(&r, n, !bit(id, n))
	}
	return r
}

func bit(id ID, i int) bool { return id[i/8]&(0x80>>(i%8)) != 0 }

func setBit(id *ID, i int, v bool) {
	if v {
		id[i/8] |= 0x80 >> (i % 8)
	} else {
		id[i/8] &^= 0x80 >> (i % 8)
	}
}

// NodeInfo is a node as the DHT passes it around: its ID and its UDP
// address. A zero ID is a node whose ID is not known yet, such as a
// bootstrap router.
type NodeInfo struct {
	ID   ID
	Addr netip.AddrPort
}

// String returns "id@addr".
func (n NodeInfo) String() string { return n.ID.String() + "@" + n.Addr.String() }

// Compact sizes: a node is its ID and a compact address (BEP 5, BEP 32).
const (
	compactAddr4 = 6
	compactAddr6 = 18
	compactNode4 = 20 + compactAddr4
	compactNode6 = 20 + compactAddr6
)

// appendCompactAddr appends the compact form of addr: 4 or 16 address
// bytes and the port, big-endian.
func appendCompactAddr(dst []byte, addr netip.AddrPort) []byte {
	a := addr.Addr().Unmap()
	if a.Is4() {
		b := a.As4()
		dst = append(dst, b[:]...)
	} else {
		b := a.As16()
		dst = append(dst, b[:]...)
	}
	return append(dst, byte(addr.Port()>>8), byte(addr.Port()))
}

// parseCompactAddr parses a 6- or 18-byte compact address.
func parseCompactAddr(b []byte) (netip.AddrPort, bool) {
	var a netip.Addr
	switch len(b) {
	case compactAddr4:
		a = netip.AddrFrom4([4]byte(b[:4]))
	case compactAddr6:
		a = netip.AddrFrom16([16]byte(b[:16]))
	default:
		return netip.AddrPort{}, false
	}
	return netip.AddrPortFrom(a, uint16(b[len(b)-2])<<8|uint16(b[len(b)-1])), true
}

// appendCompactNodes appends nodes of one family in compact node info form:
// "nodes" takes the IPv4 ones, "nodes6" the IPv6 ones.
func appendCompactNodes(dst []byte, nodes []NodeInfo, v6 bool) []byte {
	for _, n := range nodes {
		if n.Addr.Addr().Unmap().Is4() == v6 {
			continue
		}
		dst = append(dst, n.ID[:]...)
		dst = appendCompactAddr(dst, n.Addr)
	}
	return dst
}

// parseCompactNodes parses compact node info: 26 bytes a node for IPv4, 38
// for IPv6. Nodes at port 0 or an unspecified address are left out, since
// nobody can be reached there.
func parseCompactNodes(b []byte, v6 bool) ([]NodeInfo, error) {
	size := compactNode4
	if v6 {
		size = compactNode6
	}
	if len(b)%size != 0 {
		return nil, errors.New("dht: compact node info of length " + strconv.Itoa(len(b)))
	}
	nodes := make([]NodeInfo, 0, len(b)/size)
	for ; len(b) > 0; b = b[size:] {
		addr, _ := parseCompactAddr(b[20:size])
		if addr.Port() == 0 || addr.Addr().IsUnspecified() {
			continue
		}
		nodes = append(nodes, NodeInfo{ID: ID(b[:20]), Addr: addr})
	}
	return nodes, nil
}

// unmap normalizes an address: IPv4-mapped IPv6 addresses, which a
// dual-stack socket reports for IPv4 peers, become IPv4.
func unmap(a netip.AddrPort) netip.AddrPort {
	return netip.AddrPortFrom(a.Addr().Unmap(), a.Port())
}

// sortNodes sorts nodes by distance from target, closest first.
func sortNodes(target ID, nodes []NodeInfo) {
	slices.SortFunc(nodes, func(a, b NodeInfo) int {
		if c := target.cmpDistance(a.ID, b.ID); c != 0 {
			return c
		}
		return bytes.Compare(a.Addr.Addr().AsSlice(), b.Addr.Addr().AsSlice())
	})
}

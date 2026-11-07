package tracker

import (
	"encoding/binary"
	"net/netip"
)

// Compact peer entries (BEP 23, BEP 7): an address in network order and a
// big-endian port.
const (
	compactV4 = 4 + 2
	compactV6 = 16 + 2
)

// appendCompact appends ap in compact form, 6 bytes for an IPv4 address and
// 18 for an IPv6 one.
func appendCompact(dst []byte, ap netip.AddrPort) []byte {
	a := ap.Addr().Unmap()
	if a.Is4() {
		b := a.As4()
		dst = append(dst, b[:]...)
	} else {
		b := a.As16()
		dst = append(dst, b[:]...)
	}
	return binary.BigEndian.AppendUint16(dst, ap.Port())
}

// parseCompact appends to dst the peers of a compact list of entries of
// size bytes, compactV4 or compactV6. A trailing partial entry is ignored,
// as are entries with port 0 or an unspecified address, which nobody can
// connect to.
func parseCompact(dst []Peer, b []byte, size int) []Peer {
	for ; len(b) >= size; b = b[size:] {
		var a netip.Addr
		if size == compactV4 {
			a = netip.AddrFrom4([4]byte(b[:4]))
		} else {
			a = netip.AddrFrom16([16]byte(b[:16])).Unmap()
		}
		port := binary.BigEndian.Uint16(b[size-2:])
		if port == 0 || a.IsUnspecified() {
			continue
		}
		dst = append(dst, Peer{Addr: netip.AddrPortFrom(a, port)})
	}
	return dst
}

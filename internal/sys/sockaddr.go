package sys

import (
	"net/netip"
	"syscall"
)

// SockaddrAddrPort returns the address and port of an IPv4 or IPv6 sockaddr,
// an IPv4-mapped IPv6 address as the IPv4 address it is, or the zero
// AddrPort for any other sockaddr and for an IPv6 one with a zone.
func SockaddrAddrPort(sa syscall.Sockaddr) netip.AddrPort {
	switch sa := sa.(type) {
	case *syscall.SockaddrInet4:
		return netip.AddrPortFrom(netip.AddrFrom4(sa.Addr), uint16(sa.Port))
	case *syscall.SockaddrInet6:
		if sa.ZoneId != 0 {
			return netip.AddrPort{}
		}
		return netip.AddrPortFrom(netip.AddrFrom16(sa.Addr).Unmap(), uint16(sa.Port))
	}
	return netip.AddrPort{}
}

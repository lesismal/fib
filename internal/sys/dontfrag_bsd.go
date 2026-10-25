//go:build darwin || freebsd

package sys

import "syscall"

// SetDontFragment has the datagrams a UDP socket sends go with the Don't
// Fragment bit over IPv4, IP_DONTFRAG, and unfragmented over IPv6,
// IPV6_DONTFRAG. A socket of one family takes the option of its own.
func SetDontFragment(fd int) error {
	err4 := syscall.SetsockoptInt(fd, syscall.IPPROTO_IP, ipDontFrag, 1)
	err6 := syscall.SetsockoptInt(fd, syscall.IPPROTO_IPV6, ipv6DontFrag, 1)
	if err4 == nil || err6 == nil {
		return nil
	}
	return err4
}

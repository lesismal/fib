package sys

import "syscall"

// IP_DONTFRAGMENT and IPV6_DONTFRAG of <ws2ipdef.h>, which the syscall
// package leaves out.
const (
	ipDontFragment = 14
	ipv6DontFrag   = 14
)

// SetDontFragment has the datagrams a UDP socket sends go with the Don't
// Fragment bit over IPv4 and unfragmented over IPv6. A dual-stack IPv6
// socket takes both options, one of IPv4 only the first.
func SetDontFragment(fd syscall.Handle) error {
	err4 := syscall.SetsockoptInt(fd, syscall.IPPROTO_IP, ipDontFragment, 1)
	err6 := syscall.SetsockoptInt(fd, syscall.IPPROTO_IPV6, ipv6DontFrag, 1)
	if err4 == nil || err6 == nil {
		return nil
	}
	return err4
}

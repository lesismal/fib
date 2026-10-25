//go:build dragonfly || netbsd || openbsd

package sys

import "syscall"

// SetDontFragment has the datagrams an IPv6 UDP socket sends go
// unfragmented, IPV6_DONTFRAG. These systems have no such option for IPv4,
// so an IPv4 socket reports it cannot.
func SetDontFragment(fd int) error {
	return syscall.SetsockoptInt(fd, syscall.IPPROTO_IPV6, syscall.IPV6_DONTFRAG, 1)
}

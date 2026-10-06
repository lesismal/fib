//go:build linux && 386

package fib

import (
	"net/netip"
	"syscall"
)

// acceptSocket accepts a connection already non-blocking and close-on-exec,
// and returns the peer's address with it. 32-bit x86 has no accept4 system
// call of its own, only the socketcall multiplexer, which syscall.Accept4
// goes through.
func acceptSocket(listenFD int) (int, netip.AddrPort, error) {
	fd, sa, err := syscall.Accept4(listenFD, syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC)
	if err != nil {
		return -1, netip.AddrPort{}, err
	}
	return fd, sockaddrAddrPort(sa), nil
}

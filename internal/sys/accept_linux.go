//go:build linux && !386

package sys

import (
	"net/netip"
	"syscall"
	"unsafe"
)

// AcceptSocket accepts a connection already non-blocking and close-on-exec,
// and returns the peer's address with it. It calls accept4 directly, with the
// address read into a buffer on the stack rather than decoded into the
// Sockaddr syscall.Accept4 would allocate.
func AcceptSocket(listenFD int) (int, netip.AddrPort, error) {
	var raw syscall.RawSockaddrAny
	size := uint32(syscall.SizeofSockaddrAny)
	r0, _, errno := syscall.RawSyscall6(
		syscall.SYS_ACCEPT4,
		uintptr(listenFD),
		uintptr(unsafe.Pointer(&raw)),
		uintptr(unsafe.Pointer(&size)),
		uintptr(syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC),
		0,
		0,
	)
	if errno != 0 {
		return -1, netip.AddrPort{}, errno
	}
	return int(r0), rawAddrPort(&raw), nil
}

// rawAddrPort is sockaddrAddrPort for an address as the kernel wrote it.
func rawAddrPort(raw *syscall.RawSockaddrAny) netip.AddrPort {
	switch raw.Addr.Family {
	case syscall.AF_INET:
		sa := (*syscall.RawSockaddrInet4)(unsafe.Pointer(raw))
		port := (*[2]byte)(unsafe.Pointer(&sa.Port))
		return netip.AddrPortFrom(netip.AddrFrom4(sa.Addr), uint16(port[0])<<8|uint16(port[1]))
	case syscall.AF_INET6:
		sa := (*syscall.RawSockaddrInet6)(unsafe.Pointer(raw))
		if sa.Scope_id != 0 {
			return netip.AddrPort{}
		}
		port := (*[2]byte)(unsafe.Pointer(&sa.Port))
		return netip.AddrPortFrom(netip.AddrFrom16(sa.Addr).Unmap(), uint16(port[0])<<8|uint16(port[1]))
	}
	return netip.AddrPort{}
}

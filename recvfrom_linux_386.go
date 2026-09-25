//go:build linux && 386

package fib

import (
	"syscall"
	"unsafe"
)

// recvfrom reads one datagram into p and the sender's address into from.
// 32-bit x86 has no recvfrom system call of its own, only the socketcall
// multiplexer, which syscall.Recvfrom goes through; the address it returns is
// copied back into raw form.
func recvfrom(fd int, p []byte, from *syscall.RawSockaddrAny) (int, error) {
	n, sa, err := syscall.Recvfrom(fd, p, 0)
	if err != nil {
		return 0, err
	}
	*from = syscall.RawSockaddrAny{}
	switch a := sa.(type) {
	case *syscall.SockaddrInet4:
		pp := (*syscall.RawSockaddrInet4)(unsafe.Pointer(from))
		pp.Family = syscall.AF_INET
		port := (*[2]byte)(unsafe.Pointer(&pp.Port))
		port[0], port[1] = byte(a.Port>>8), byte(a.Port)
		pp.Addr = a.Addr
	case *syscall.SockaddrInet6:
		pp := (*syscall.RawSockaddrInet6)(unsafe.Pointer(from))
		pp.Family = syscall.AF_INET6
		port := (*[2]byte)(unsafe.Pointer(&pp.Port))
		port[0], port[1] = byte(a.Port>>8), byte(a.Port)
		pp.Scope_id = a.ZoneId
		pp.Addr = a.Addr
	}
	return n, nil
}

//go:build darwin || (linux && !386)

package fib

import (
	"syscall"
	"unsafe"
)

// recvfrom reads one datagram into p and the sender's address into from.
// syscall.Recvfrom returns the address as a syscall.Sockaddr it allocates,
// which for a listener that reads every peer's datagrams is an allocation per
// datagram; this leaves it where the kernel wrote it.
func recvfrom(fd int, p []byte, from *syscall.RawSockaddrAny) (int, error) {
	fromLen := uint32(syscall.SizeofSockaddrAny)
	n, _, errno := syscall.Syscall6(syscall.SYS_RECVFROM, uintptr(fd),
		uintptr(unsafe.Pointer(unsafe.SliceData(p))), uintptr(len(p)), 0,
		uintptr(unsafe.Pointer(from)), uintptr(unsafe.Pointer(&fromLen)))
	if errno != 0 {
		return 0, errno
	}
	return int(n), nil
}

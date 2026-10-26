//go:build linux && !386

package udpbatch

import (
	"syscall"
	"unsafe"
)

// rawSendmsg is sendmsg(2) on the descriptor, for a message already built.
// The arches that send a socket call through socketcall have their own, below.
func rawSendmsg(fd int, h unsafe.Pointer) (uintptr, uintptr, syscall.Errno) {
	return syscall.Syscall6(syscall.SYS_SENDMSG, uintptr(fd), uintptr(h), 0, 0, 0, 0)
}

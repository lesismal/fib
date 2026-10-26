//go:build linux && 386

package udpbatch

import (
	"syscall"
	"unsafe"
)

// sysSendmsg is the number of sendmsg in the socketcall table, which 386 goes
// through instead of a system call of its own.
const sysSendmsg = 16

// rawSendmsg is sendmsg(2) on the descriptor, through socketcall as 386 makes
// it: its arguments are the array the call is given a pointer to.
func rawSendmsg(fd int, h unsafe.Pointer) (uintptr, uintptr, syscall.Errno) {
	args := [3]uintptr{uintptr(fd), uintptr(h), 0}
	return syscall.Syscall(syscall.SYS_SOCKETCALL, sysSendmsg, uintptr(unsafe.Pointer(&args[0])), 0)
}

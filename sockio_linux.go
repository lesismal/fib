//go:build linux && !race && !msan && !asan

package fib

import (
	"syscall"
	"unsafe"
)

// The connection sockets are non-blocking, so a read or write on one returns
// as soon as the kernel has copied what fits; it never waits. They are made
// as raw system calls, without the scheduler's entersyscall/exitsyscall around
// them, which only pays off for a call that may block.
//
// With the scheduler told, the P of a worker in a read or write is left in the
// syscall state, and a server doing little but these calls, one read and one
// write per echoed message, kept all of its Ps and threads busy while doing no
// more work. In go-websocket-benchmark's echo (20k connections, 1KB messages,
// 8 server CPUs, the client on the other 6 and on their SMT siblings) the same
// ~705k messages a second cost 734-742% CPU through syscall.Read/Write and
// 662-664% raw, P99 fell from 18-19ms to 15.6ms, and pipelined echo cost 3-4%
// less CPU at the same 5.55M messages a second. The
// time saved was the kernel's: 608% system time fell to 564%, and the Ps,
// which through syscall.Read/Write were almost never idle, went idle and were
// woken again ten times as often, as those of frameworks that already make
// these calls raw do.
//
// A race, memory or address sanitizer build keeps the syscall package's
// wrappers instead, which tell the sanitizer what the kernel read and wrote.

// sockRead reads from a non-blocking descriptor. It never reports a negative
// count: an error comes with 0.
func sockRead(fd int, b []byte) (int, error) {
	return rawSockIO(syscall.SYS_READ, fd, unsafe.Pointer(unsafe.SliceData(b)), len(b))
}

// sockWrite writes to a non-blocking descriptor, and may write less than b.
func sockWrite(fd int, b []byte) (int, error) {
	return rawSockIO(syscall.SYS_WRITE, fd, unsafe.Pointer(unsafe.SliceData(b)), len(b))
}

// sockWritev writes iov, which must not be empty, to a non-blocking
// descriptor, and may write less than all of it.
func sockWritev(fd int, iov []syscall.Iovec) (int, error) {
	return rawSockIO(syscall.SYS_WRITEV, fd, unsafe.Pointer(&iov[0]), len(iov))
}

func rawSockIO(trap uintptr, fd int, p unsafe.Pointer, n int) (int, error) {
	r, _, errno := syscall.RawSyscall(trap, uintptr(fd), uintptr(p), uintptr(n))
	if errno != 0 {
		return 0, errno
	}
	return int(r), nil
}

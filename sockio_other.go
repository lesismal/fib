//go:build darwin || (linux && (386 || race || msan || asan))

package fib

import "syscall"

// Darwin makes system calls through libc, linux/386 reaches the socket calls
// only through socketcall, and a sanitizer build wants the syscall package's
// annotations, so these go through the syscall package. sockio_linux.go says
// why Linux otherwise makes raw socket calls.

func sockRead(fd int, b []byte) (int, error) {
	n, err := syscall.Read(fd, b)
	return max(n, 0), err
}

func sockWrite(fd int, b []byte) (int, error) {
	n, err := syscall.Write(fd, b)
	return max(n, 0), err
}

func sockWritev(fd int, iov []syscall.Iovec) (int, error) {
	n, err := writevRaw(fd, iov)
	return max(n, 0), err
}

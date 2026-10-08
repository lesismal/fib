//go:build darwin || (linux && (386 || race || msan || asan))

package sys

import "syscall"

// Darwin makes system calls through libc, linux/386 reaches the socket calls
// only through socketcall, and a sanitizer build wants the syscall package's
// annotations, so these go through the syscall package, and always make the
// VFS calls whatever Config.SocketSyscalls says. sockio_linux.go says why
// Linux otherwise makes them raw.

func Read(fd int, b []byte, _ bool) (int, error) {
	n, err := syscall.Read(fd, b)
	return max(n, 0), err
}

func Write(fd int, b []byte, _ bool) (int, error) {
	n, err := syscall.Write(fd, b)
	return max(n, 0), err
}

func Writev(fd int, iov []syscall.Iovec, _ bool) (int, error) {
	n, err := WritevRaw(fd, iov)
	return max(n, 0), err
}

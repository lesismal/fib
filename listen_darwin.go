package fib

import "syscall"

// stopListeningFD stops the listening socket fd from taking connections
// while keeping its descriptor. Shutting a listening socket down does that
// on Linux, but Darwin refuses it (ENOTCONN) and the socket goes on
// completing connections into its backlog until it is closed. So a socket
// that was never bound takes fd's place instead: the listening socket is
// closed, which stops it listening, and fd stays taken, so no loop that has
// yet to see Stop can find a descriptor opened elsewhere under it. accept on
// the replacement fails, as it does on a shut-down socket on Linux.
func stopListeningFD(fd int) {
	if syscall.Shutdown(fd, syscall.SHUT_RD) == nil {
		return
	}
	placeholder, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return
	}
	_ = syscall.Dup2(placeholder, fd)
	_ = syscall.Close(placeholder)
}

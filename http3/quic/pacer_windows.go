package quic

import (
	"errors"
	"syscall"
	"time"
)

// minPacingDelay is the shortest wait for the pacing budget. Windows' clock
// moves on with the timer interrupt, every 15.6ms by default: a budget
// filled by the time that passed sees none for that long, a timer set for
// sooner finds nothing due when it fires, and a fast path's round trip
// measures as nothing. Pacing waits a tick at least, and its burst covers
// two of them.
const minPacingDelay = 16 * time.Millisecond

// Winsock's errors for a socket with no room to send, which syscall does not
// name.
const (
	wsaEWouldBlock syscall.Errno = 10035
	wsaENoBufs     syscall.Errno = 10055
)

// socketFull reports whether err is Winsock's own for a full socket, which
// a non-blocking send answers rather than EAGAIN or ENOBUFS.
func socketFull(err error) bool {
	var errno syscall.Errno
	return errors.As(err, &errno) && (errno == wsaEWouldBlock || errno == wsaENoBufs)
}

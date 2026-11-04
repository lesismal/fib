//go:build linux

package fib

import "syscall"

// spliceSupported says the system has splice(2); see Connection.SpliceTo.
const spliceSupported = true

// The flags of splice(2) and the fcntl commands that size a pipe, which
// package syscall does not name.
const (
	spliceMove     = 0x1
	spliceNonblock = 0x2
	fSetPipeSize   = 1031
	fGetPipeSize   = 1032
)

// splicePipeSize is the room a splice's pipe is given: 32 pages, twice the
// default, so that a round of a fast upload moves more than one socket
// buffer's worth per call. A pipe the system will not grow that far keeps the
// size it has, unless that is less than splicePipeMin.
const splicePipeSize = 128 << 10

// splicePipeMin is the smallest pipe a splice takes. A user past the system's
// pipe-user-pages-soft gets pipes of two pages, which would cost a call per
// eight KB; the splice is refused for one, and its bytes go the ordinary way.
const splicePipeMin = 64 << 10

// openSplicePipe opens a non-blocking pipe for a splice.
func openSplicePipe() (splicePipe, error) {
	var fds [2]int
	if err := syscall.Pipe2(fds[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		return splicePipe{}, err
	}
	pipe := splicePipe{r: fds[0], w: fds[1]}
	_, _, _ = syscall.Syscall(syscall.SYS_FCNTL, uintptr(pipe.w), fSetPipeSize, splicePipeSize)
	size, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(pipe.w), fGetPipeSize, 0)
	if errno != 0 || int(size) < splicePipeMin {
		closeSplicePipe(pipe)
		return splicePipe{}, ErrSpliceBusy
	}
	pipe.size = int(size)
	return pipe, nil
}

func closeSplicePipe(pipe splicePipe) {
	_ = syscall.Close(pipe.r)
	_ = syscall.Close(pipe.w)
}

// spliceIn moves up to n bytes from the socket fd into the pipe's write end
// w, and spliceOut from the pipe's read end r to the socket fd. Neither ever
// waits: a socket or pipe with nothing to give, or no room, is EAGAIN. The
// calls are raw, as the socket reads and writes are (see package sys): they
// never block, and touch no memory of the process.
func spliceIn(fd, w, n int) (int, error)  { return splice(fd, w, n) }
func spliceOut(r, fd, n int) (int, error) { return splice(r, fd, n) }

func splice(from, to, n int) (int, error) {
	r, _, errno := syscall.RawSyscall6(syscall.SYS_SPLICE, uintptr(from), 0, uintptr(to), 0, uintptr(n),
		spliceMove|spliceNonblock)
	if errno != 0 {
		return 0, errno
	}
	return int(r), nil
}

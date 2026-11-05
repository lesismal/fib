//go:build netbsd || openbsd

package core

import "syscall"

// wakeBackend is the pipe other goroutines write to to wake the loop, since
// the syscall package gives these systems no EVFILT_USER.
type wakeBackend struct {
	wakeRead, wakeWrite int
}

func (e *Engine) openWake(changes []syscall.Kevent_t) ([]syscall.Kevent_t, error) {
	var p [2]int
	if err := syscall.Pipe2(p[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		return changes, err
	}
	e.wakeRead, e.wakeWrite = p[0], p[1]
	return append(changes, newKevent(e.wakeRead, syscall.EVFILT_READ, syscall.EV_ADD|syscall.EV_CLEAR)), nil
}

func (e *Engine) closeWake() {
	syscall.Close(e.wakeRead)
	syscall.Close(e.wakeWrite)
}

func (e *Engine) isWake(ev *syscall.Kevent_t) bool {
	return ev.Filter == syscall.EVFILT_READ && int(ev.Ident) == e.wakeRead
}

// wake writes a byte. A full pipe already holds a wake-up the loop has yet
// to read, so EAGAIN loses nothing.
func (e *Engine) wake() {
	b := [1]byte{1}
	for {
		if _, err := syscall.Write(e.wakeWrite, b[:]); err != syscall.EINTR {
			return
		}
	}
}

// ackWake empties the pipe, so the next write is a new edge for EV_CLEAR.
func (e *Engine) ackWake() {
	var b [64]byte
	for {
		n, err := syscall.Read(e.wakeRead, b[:])
		if err == syscall.EINTR {
			continue
		}
		if n < len(b) {
			return
		}
	}
}

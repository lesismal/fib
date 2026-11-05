//go:build darwin || dragonfly || freebsd

package core

import "syscall"

// wakeIdent names the EVFILT_USER event other goroutines trigger to wake the
// loop. User events live in their own namespace, so it cannot collide with a
// descriptor.
const wakeIdent = 0

// wakeBackend needs no state of its own: the user event lives in the kqueue.
type wakeBackend struct{}

func (e *Engine) openWake(changes []syscall.Kevent_t) ([]syscall.Kevent_t, error) {
	return append(changes, syscall.Kevent_t{Ident: wakeIdent, Filter: evfiltUser,
		Flags: syscall.EV_ADD | syscall.EV_CLEAR}), nil
}

func (e *Engine) closeWake() {}

func (e *Engine) isWake(ev *syscall.Kevent_t) bool { return ev.Filter == evfiltUser }

func (e *Engine) wake() {
	changes := [1]syscall.Kevent_t{{Ident: wakeIdent, Filter: evfiltUser, Fflags: noteTrigger}}
	for {
		if _, err := syscall.Kevent(e.kq, changes[:], nil, nil); err != syscall.EINTR {
			return
		}
	}
}

// ackWake does nothing: the user event is registered with EV_CLEAR, so
// delivering it has already reset it.
func (e *Engine) ackWake() {}

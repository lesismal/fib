package core

import "syscall"

// EVFILT_EXCEPT with NOTE_OOB reports urgent data, which the read filter does
// not.
const (
	hasExceptFilter = true
	evfiltExcept    = syscall.EVFILT_EXCEPT
	noteOOB         = syscall.NOTE_OOB
)

// The loop is woken through EVFILT_USER, which the syscall package does not
// export; the values are from <sys/event.h>.
const (
	evfiltUser  = -9
	noteTrigger = 0x01000000
)

const (
	keepAliveTunable = true
	tcpKeepIdle      = syscall.TCP_KEEPIDLE
	tcpKeepInterval  = syscall.TCP_KEEPINTVL
	tcpKeepCount     = syscall.TCP_KEEPCNT
)

// usingMultipathTCP is false: DragonFly has no Multipath TCP.
func usingMultipathTCP(int) bool { return false }

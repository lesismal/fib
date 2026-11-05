package core

import "syscall"

// EVFILT_EXCEPT with NOTE_OOB reports urgent data, which the read filter does
// not. The syscall package does not export either; the values are from
// <sys/event.h>.
const (
	hasExceptFilter = true
	evfiltExcept    = -15
	noteOOB         = 0x2
)

// The loop is woken through EVFILT_USER.
const (
	evfiltUser  = syscall.EVFILT_USER
	noteTrigger = syscall.NOTE_TRIGGER
)

// Darwin names the idle time TCP_KEEPALIVE, and the syscall package leaves
// the other two out on amd64.
const (
	keepAliveTunable = true
	tcpKeepIdle      = syscall.TCP_KEEPALIVE
	tcpKeepInterval  = 0x101
	tcpKeepCount     = 0x102
)

// usingMultipathTCP is false: Darwin offers Multipath TCP only through its
// own frameworks, never on a socket opened as this package opens them.
func usingMultipathTCP(int) bool { return false }

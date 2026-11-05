package core

import "syscall"

// FreeBSD has no EVFILT_EXCEPT, so urgent data goes unreported; the values
// only fill the slot the except filter would take.
const (
	hasExceptFilter = false
	evfiltExcept    = syscall.EVFILT_READ
	noteOOB         = 0
)

// The loop is woken through EVFILT_USER.
const (
	evfiltUser  = syscall.EVFILT_USER
	noteTrigger = syscall.NOTE_TRIGGER
)

const (
	keepAliveTunable = true
	tcpKeepIdle      = syscall.TCP_KEEPIDLE
	tcpKeepInterval  = syscall.TCP_KEEPINTVL
	tcpKeepCount     = syscall.TCP_KEEPCNT
)

// usingMultipathTCP is false: FreeBSD has no Multipath TCP.
func usingMultipathTCP(int) bool { return false }

package core

import "syscall"

// The except filter is left unused, so urgent data goes unreported; the
// values only fill the slot it would take.
const (
	hasExceptFilter = false
	evfiltExcept    = syscall.EVFILT_READ
	noteOOB         = 0
)

const (
	keepAliveTunable = true
	tcpKeepIdle      = syscall.TCP_KEEPIDLE
	tcpKeepInterval  = syscall.TCP_KEEPINTVL
	tcpKeepCount     = syscall.TCP_KEEPCNT
)

// usingMultipathTCP is false: NetBSD has no Multipath TCP.
func usingMultipathTCP(int) bool { return false }

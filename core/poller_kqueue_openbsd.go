package core

import "syscall"

// The except filter is left unused, so urgent data goes unreported; the
// values only fill the slot it would take.
const (
	hasExceptFilter = false
	evfiltExcept    = syscall.EVFILT_READ
	noteOOB         = 0
)

// OpenBSD has no per-socket keep-alive times or count, only the system-wide
// ones; see setKeepAliveTimes.
const (
	keepAliveTunable = false
	tcpKeepIdle      = 0
	tcpKeepInterval  = 0
	tcpKeepCount     = 0
)

// usingMultipathTCP is false: OpenBSD has no Multipath TCP.
func usingMultipathTCP(int) bool { return false }

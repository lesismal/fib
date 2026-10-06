package prefork

import "syscall"

// supported is whether this platform's SO_REUSEPORT spreads connections
// between processes, which Linux's does.
const supported = true

// sysProcAttr has a child sent SIGTERM if the master exits, so that it stops
// rather than outlive the process that supervises it.
func sysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
}

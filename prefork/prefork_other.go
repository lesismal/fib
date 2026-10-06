//go:build !linux

package prefork

import "syscall"

// supported is false: elsewhere SO_REUSEPORT does not spread connections
// between processes, or there is no fork, and Run serves in this process.
const supported = false

func sysProcAttr() *syscall.SysProcAttr { return nil }

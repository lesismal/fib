//go:build darwin

package fib

import "syscall"

// Darwin names the idle time TCP_KEEPALIVE, and the syscall package leaves
// the other two out on amd64.
const (
	tcpKeepIdle     = syscall.TCP_KEEPALIVE
	tcpKeepInterval = 0x101
	tcpKeepCount    = 0x102
)

// usingMultipathTCP is false: Darwin offers Multipath TCP only through its
// own frameworks, never on a socket opened as this package opens them.
func usingMultipathTCP(int) bool { return false }

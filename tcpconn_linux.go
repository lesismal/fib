//go:build linux

package fib

import "syscall"

const (
	tcpKeepIdle     = syscall.TCP_KEEPIDLE
	tcpKeepInterval = syscall.TCP_KEEPINTVL
	tcpKeepCount    = syscall.TCP_KEEPCNT
)

const (
	ipprotoMPTCP = 262
	solMPTCP     = 284
	mptcpInfo    = 1
)

// usingMultipathTCP reports a socket opened for Multipath TCP that has not
// fallen back to plain TCP, which is how the net package tells.
func usingMultipathTCP(s int) bool {
	proto, err := syscall.GetsockoptInt(s, syscall.SOL_SOCKET, syscall.SO_PROTOCOL)
	if err != nil || proto != ipprotoMPTCP {
		return false
	}
	_, err = syscall.GetsockoptInt(s, solMPTCP, mptcpInfo)
	return err != syscall.EOPNOTSUPP && err != syscall.ENOPROTOOPT
}

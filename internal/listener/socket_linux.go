//go:build linux

package listener

import (
	"syscall"

	"github.com/lesismal/fib/internal/sys"
)

// ipprotoMPTCP is IPPROTO_MPTCP, which the syscall package does not name.
const ipprotoMPTCP = 262

// newSocket opens a non-blocking stream socket of family, for Multipath TCP
// when o asks and family is an internet one. A kernel that has no Multipath
// TCP refuses the protocol with EPROTONOSUPPORT or EINVAL, and one that has
// it but is not enabled does the same, so any error falls back to plain TCP,
// which is what net.ListenConfig.SetMultipathTCP leaves: the net package, too,
// takes every error as "this kernel cannot" rather than failing to listen.
// A Unix socket is always plain, since it has no protocol to negotiate.
func newSocket(o Options, family int) (int, error) {
	if o.MultipathTCP && (family == syscall.AF_INET || family == syscall.AF_INET6) {
		if fd, err := syscall.Socket(family, syscall.SOCK_STREAM|syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC, ipprotoMPTCP); err == nil {
			return fd, nil
		}
	}
	return sys.NewSocket(family)
}

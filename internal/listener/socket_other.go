//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package listener

import "github.com/lesismal/fib/internal/sys"

// newSocket opens a plain TCP or Unix stream socket of family. Only Linux
// opens a Multipath TCP one through the protocol of the socket, so
// o.MultipathTCP is ignored here, as it is by net.ListenConfig.SetMultipathTCP
// on these systems: see Binding.MultipathTCP.
func newSocket(_ Options, family int) (int, error) {
	return sys.NewSocket(family)
}

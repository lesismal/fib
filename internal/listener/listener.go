//go:build linux || darwin

// Package listener opens the listening sockets of the readiness-based
// backends: TCP, Unix and UDP, bound where the engine's Config says.
package listener

import (
	"syscall"

	"github.com/lesismal/fib/internal/netaddr"
	"github.com/lesismal/fib/internal/sys"
)

// Options is what of Config the sockets are opened by.
type Options struct {
	// Network is the Config.Network the sockets serve.
	Network string
	// Backlog is the accept queue depth.
	Backlog int
	// ReusePort is Config.ReusePort.
	ReusePort bool
}

// Create opens a socket bound to addr, and listening on it unless
// listen is false. spread binds it with SO_REUSEPORT for the engine's pollers
// to listen beside it. Without Config.ReusePort, which would let other
// sockets share the address, the address is claimed first by a socket bound
// without SO_REUSEPORT, so that one another socket holds already is refused
// with EADDRINUSE, as it is without pollers: see claimAddress. A port left to
// the kernel is not claimed: it never chooses one a socket holds, even with
// SO_REUSEPORT, whereas the port a claim chose is free again once the claim
// gives it up, and another process claiming then would be given it too and
// share it, its connections split between the two.
func Create(o Options, addr string, listen, spread bool) (int, error) {
	family, bound, err := netaddr.ResolveListen(o.Network, addr)
	if err != nil {
		return -1, err
	}
	reusePort := o.ReusePort
	if spread && !reusePort && sockaddrPort(bound) != 0 {
		if bound, err = claimAddress(o, family, bound); err != nil {
			return -1, err
		}
		reusePort = true
	}
	return listenSocket(o, family, bound, listen, reusePort)
}

// claimAddress binds a socket to bound without SO_REUSEPORT, which fails if
// any other socket holds the address, and gives it up again, returning the
// address it was bound to: bound itself, with the port the kernel chose when
// bound left that to it. The engine's sockets then bind there with
// SO_REUSEPORT. Only a socket that sets SO_REUSEPORT itself, under the same
// user, can join them afterwards, which is what Config.ReusePort asks for and
// an engine without it does not, so a second server on the address fails as
// it would have.
func claimAddress(o Options, family int, bound syscall.Sockaddr) (syscall.Sockaddr, error) {
	fd, err := sys.NewSocket(family)
	if err != nil {
		return nil, err
	}
	defer syscall.Close(fd)
	_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	if family == syscall.AF_INET6 {
		// As listenSocket binds it, so that the claim covers what the
		// listener will.
		v6only := 0
		if o.Network == "tcp6" {
			v6only = 1
		}
		_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_IPV6, syscall.IPV6_V6ONLY, v6only)
	}
	if err := syscall.Bind(fd, bound); err != nil {
		return nil, err
	}
	return syscall.Getsockname(fd)
}

// sockaddrPort returns the port of an IPv4 or IPv6 sockaddr, and 0 for any
// other.
func sockaddrPort(sa syscall.Sockaddr) int {
	switch sa := sa.(type) {
	case *syscall.SockaddrInet4:
		return sa.Port
	case *syscall.SockaddrInet6:
		return sa.Port
	}
	return 0
}

// CreateLike opens a socket listening where like is bound, which
// SO_REUSEPORT lets it share with like; see Config.ReusePort.
func CreateLike(o Options, like int) (int, error) {
	bound, err := syscall.Getsockname(like)
	if err != nil {
		return -1, err
	}
	family := syscall.AF_INET
	if _, ok := bound.(*syscall.SockaddrInet6); ok {
		family = syscall.AF_INET6
	}
	return listenSocket(o, family, bound, true, true)
}

func listenSocket(o Options, family int, bound syscall.Sockaddr, listen, reusePort bool) (int, error) {
	fd, err := sys.NewSocket(family)
	if err != nil {
		return -1, err
	}
	if family != syscall.AF_UNIX {
		_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
		if sys.AcceptedInheritNoDelay {
			// Set once here rather than on every connection accepted, which
			// takes it over from the listener.
			_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
		}
		if reusePort {
			if err = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, sys.SoReusePort, 1); err != nil {
				syscall.Close(fd)
				return -1, err
			}
		}
	}
	if family == syscall.AF_INET6 {
		// "tcp" accepts both families on one socket; "tcp6" is IPv6 only. This
		// is the distinction net.Listen draws between the two networks.
		v6only := 0
		if o.Network == "tcp6" {
			v6only = 1
		}
		_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_IPV6, syscall.IPV6_V6ONLY, v6only)
	}
	if err = syscall.Bind(fd, bound); err == nil && listen {
		err = syscall.Listen(fd, o.Backlog)
	}
	if err != nil {
		syscall.Close(fd)
		return -1, err
	}
	return fd, nil
}

func CreateUDP(o Options, addr string) (int, error) {
	family, bound, err := netaddr.ResolveListen(o.Network, addr)
	if err != nil {
		return -1, err
	}
	return bindUDPSocket(o, family, bound)
}

// CreateUDPLike opens a UDP socket bound where like is, which
// SO_REUSEPORT lets it share with like; see Config.ReusePort.
func CreateUDPLike(o Options, like int) (int, error) {
	bound, err := syscall.Getsockname(like)
	if err != nil {
		return -1, err
	}
	family := syscall.AF_INET
	if _, ok := bound.(*syscall.SockaddrInet6); ok {
		family = syscall.AF_INET6
	}
	return bindUDPSocket(o, family, bound)
}

func bindUDPSocket(o Options, family int, bound syscall.Sockaddr) (int, error) {
	fd, err := sys.NewDatagramSocket(family)
	if err != nil {
		return -1, err
	}
	_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	if o.ReusePort {
		if err = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, sys.SoReusePort, 1); err != nil {
			syscall.Close(fd)
			return -1, err
		}
	}
	if family == syscall.AF_INET6 {
		v6only := 0
		if o.Network == "udp6" {
			v6only = 1
		}
		_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_IPV6, syscall.IPV6_V6ONLY, v6only)
	}
	if err = syscall.Bind(fd, bound); err != nil {
		syscall.Close(fd)
		return -1, err
	}
	return fd, nil
}

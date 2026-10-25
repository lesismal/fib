//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package fib

import (
	"syscall"

	"github.com/lesismal/fib/internal/sys"
)

// SetDontFragment has the datagrams of a UDP connection sent whole or not at
// all: with the Don't Fragment bit over IPv4, and never fragmented by this
// host over IPv6, so that a datagram too large for the path is dropped on the
// way rather than delivered in pieces, and one too large for the interface
// is refused with EMSGSIZE. On Linux the path MTU the kernel learns from
// ICMP is ignored (IP_PMTUDISC_PROBE), which leaves it to the application to
// find. This is what path MTU discovery above it, QUIC's, needs; the
// default, fragmenting a datagram that does not fit, is better for most
// else.
//
// A peer of a UDP listener sends through the listener's socket, so the
// option holds for every peer of it. The other BSDs than FreeBSD can do it
// for IPv6 only. It reports ErrNotUDP on a connection that is not UDP.
func (c *Connection) SetDontFragment() error {
	if c.udp == nil {
		return ErrNotUDP
	}
	fd := c.FD()
	if l := c.udp.listener; l != nil {
		fd = l.fd
	}
	if fd < 0 {
		return syscall.EBADF
	}
	return sys.SetDontFragment(fd)
}

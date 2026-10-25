//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd && !windows

package fib

import "errors"

// SetDontFragment would have the datagrams of a UDP connection sent whole or
// not at all; this platform's engine cannot, and reports
// errors.ErrUnsupported, or ErrNotUDP on a connection that is not UDP.
func (c *Connection) SetDontFragment() error {
	if !c.udp {
		return ErrNotUDP
	}
	return errors.ErrUnsupported
}

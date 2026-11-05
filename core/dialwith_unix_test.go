//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package core

import "syscall"

// probeControl undoes the SO_REUSEADDR net.Listen sets, so that a port the
// tests hold is one a dial's bind, which has no SO_REUSEADDR, can take: a TCP
// port still in TIME_WAIT fails it as it fails the dial.
func probeControl(_, _ string, c syscall.RawConn) error {
	var err error
	if ctlErr := c.Control(func(fd uintptr) {
		err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 0)
	}); ctlErr != nil {
		return ctlErr
	}
	return err
}

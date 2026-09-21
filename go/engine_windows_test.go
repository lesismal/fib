//go:build windows

package fib

import "syscall"

// newOfflineConnection builds a connection that is not attached to a socket,
// for exercising queue bookkeeping directly.
func newOfflineConnection(e *Engine) *Connection {
	c := &Connection{engine: e}
	c.handle.Store(uintptr(syscall.InvalidHandle))
	return c
}

// shrinkSendBuffer turns this socket's send buffering off, so that a reply the
// peer is not reading stays queued in the engine. Windows otherwise accepts one
// send of any size while its own backlog is below the send buffer, however
// little of it the peer has room for, and the engine is left holding nothing.
func shrinkSendBuffer(fd int) error {
	return syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, 0)
}

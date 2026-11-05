//go:build windows

package core

import "syscall"

// newOfflineConnection builds a connection that is not attached to a socket,
// for exercising queue bookkeeping directly.
func newOfflineConnection(e *Engine) *Connection {
	c := &Connection{engine: e}
	c.handle.Store(uintptr(syscall.InvalidHandle))
	return c
}

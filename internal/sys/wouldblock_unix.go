//go:build unix

package sys

import "syscall"

// IsWouldBlock reports whether err says a non-blocking call had nothing to do
// yet.
func IsWouldBlock(err error) bool { return err == syscall.EAGAIN || err == syscall.EWOULDBLOCK }

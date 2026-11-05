//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package core

// platformStateLocked is the platform part of the debug state, which only
// Windows has. Callers hold c.mu.
func (c *Connection) platformStateLocked() string { return "" }

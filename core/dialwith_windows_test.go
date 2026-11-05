package core

import "syscall"

// probeControl has nothing to undo on Windows, where net.Listen sets no
// SO_REUSEADDR.
func probeControl(_, _ string, _ syscall.RawConn) error { return nil }

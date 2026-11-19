//go:build !race

package bufferpool

import "syscall"

// mapNoReserve leaves the arena out of the commit charge until its pages are
// used, so a large arena does not fail under strict overcommit.
const mapNoReserve = syscall.MAP_NORESERVE

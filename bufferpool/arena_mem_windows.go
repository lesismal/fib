//go:build !race

package bufferpool

import (
	"syscall"
	"unsafe"
)

var procVirtualAlloc = syscall.NewLazyDLL("kernel32.dll").NewProc("VirtualAlloc")

// arenaMemory reserves and commits n bytes outside the Go heap with
// VirtualAlloc, so the arena counts towards neither GOGC's goal nor
// GOMEMLIMIT, or makes them on the heap when that fails. It says which it
// did. Committed pages are backed only once they are touched, but count
// towards the system's commit limit from the start.
func arenaMemory(n int) ([]byte, bool) {
	const memCommitReserve, pageReadWrite = 0x1000 | 0x2000, 0x04
	if procVirtualAlloc.Find() == nil {
		if p, _, _ := procVirtualAlloc.Call(0, uintptr(n), memCommitReserve, pageReadWrite); p != 0 {
			// The memory is not the Go heap's, so it never moves: reading
			// the address back as a pointer is safe.
			return unsafe.Slice((*byte)(*(*unsafe.Pointer)(unsafe.Pointer(&p))), n), true
		}
	}
	return make([]byte, n), false
}

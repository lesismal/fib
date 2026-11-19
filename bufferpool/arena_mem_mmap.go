//go:build (linux || darwin || freebsd || netbsd || openbsd || dragonfly) && !race

package bufferpool

import "syscall"

// arenaMemory maps n bytes of anonymous memory outside the Go heap, so the
// arena counts towards neither GOGC's goal nor GOMEMLIMIT, or makes them on
// the heap when the mapping fails. It says which it did. The mapping is
// never unmapped: an arena is kept for the program's life.
func arenaMemory(n int) ([]byte, bool) {
	b, err := syscall.Mmap(-1, 0, n, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE|mapNoReserve)
	if err != nil {
		return make([]byte, n), false
	}
	return b, true
}

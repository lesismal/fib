//go:build dragonfly || freebsd || netbsd || openbsd

package sys

import (
	"syscall"
)

// MemoryLimit reports the machine's physical memory, or zero when it could
// not be read. NetBSD names the 64-bit value hw.physmem64; the others have
// hw.physmem, which may be the same value or a 32-bit one.
func MemoryLimit() int64 {
	for _, name := range []string{"hw.physmem64", "hw.physmem"} {
		raw, err := syscall.Sysctl(name)
		if err != nil {
			continue
		}
		if n := sysctlUint(raw); n > 0 {
			return int64(n)
		}
	}
	return 0
}

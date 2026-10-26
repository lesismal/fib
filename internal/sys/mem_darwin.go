//go:build darwin

package sys

import (
	"syscall"
)

// MemoryLimit reports the machine's physical memory, or zero when it could
// not be read.
func MemoryLimit() int64 {
	raw, err := syscall.Sysctl("hw.memsize")
	if err != nil {
		return 0
	}
	return int64(sysctlUint(raw))
}

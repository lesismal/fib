//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package sys

import (
	"syscall"
)

// FDLimit reports the soft limit on open descriptors, which bounds how many
// connections a process can hold. Zero means it could not be read.
func FDLimit() int {
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return 0
	}
	maxInt := uint64(^uint(0) >> 1)
	if limit.Cur > maxInt {
		return int(maxInt)
	}
	return int(limit.Cur)
}

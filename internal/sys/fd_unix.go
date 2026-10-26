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
	// Cur is uint64 on Linux and the BSDs Go gives it as one, and int64 on
	// FreeBSD, where a negative value is the unlimited one: converting to
	// uint64 puts both past maxInt, and both clamp to it.
	maxInt := uint64(^uint(0) >> 1)
	cur := uint64(limit.Cur)
	if cur > maxInt {
		return int(maxInt)
	}
	return int(cur)
}

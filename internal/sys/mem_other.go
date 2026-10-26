//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd && !windows

package sys

// MemoryLimit reports zero where the platform has no way to read the
// machine's memory: the defaults then fall back to their conservative
// constants.
func MemoryLimit() int64 { return 0 }

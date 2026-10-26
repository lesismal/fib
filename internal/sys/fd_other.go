//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd && !windows

package sys

// FDLimit reports zero where the platform has no RLIMIT_NOFILE: the defaults
// then assume the reference connection count instead.
func FDLimit() int { return 0 }

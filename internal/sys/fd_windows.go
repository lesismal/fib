//go:build windows

package sys

// FDLimit reports zero: Windows has no RLIMIT_NOFILE, and its per-process
// handle limit is large enough not to size connections by.
func FDLimit() int { return 0 }

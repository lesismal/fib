//go:build windows

package benchmark

import (
	"syscall"
	"time"
)

// cpuTime is the user and kernel CPU time the process has used.
func cpuTime() time.Duration {
	process, err := syscall.GetCurrentProcess()
	if err != nil {
		return 0
	}
	var creation, exit, kernel, user syscall.Filetime
	if syscall.GetProcessTimes(process, &creation, &exit, &kernel, &user) != nil {
		return 0
	}
	// A Filetime counts 100ns intervals.
	ticks := func(t syscall.Filetime) int64 { return int64(t.HighDateTime)<<32 | int64(t.LowDateTime) }
	return time.Duration((ticks(kernel) + ticks(user)) * 100)
}

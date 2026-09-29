//go:build unix

package benchmark

import (
	"syscall"
	"time"
)

// cpuTime is the user and system CPU time the process has used.
func cpuTime() time.Duration {
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) != nil {
		return 0
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

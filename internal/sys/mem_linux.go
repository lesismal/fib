//go:build linux

package sys

import (
	"syscall"
)

// MemoryLimit reports how much memory the process may use: the machine's
// physical memory, or the smallest memory limit of the cgroups it runs in
// where that is lower, as in a container. Zero means it could not be read.
//
// The cgroup limit is the hard one, memory.max on version 2 and
// memory.limit_in_bytes on version 1, read from the process's own cgroup and
// then from each of its parents, since a limit may be set above it. A cgroup
// that sets none, or writes the "max" of version 2, falls through to its
// parent and finally to the machine.
func MemoryLimit() int64 {
	total := int64(0)
	var info syscall.Sysinfo_t
	if syscall.Sysinfo(&info) == nil {
		total = int64(info.Totalram) * int64(info.Unit)
	}
	if limit := cgroupMemoryLimit(procSelfCgroup, cgroupRootV2, cgroupRootV1); limit > 0 && (total <= 0 || limit < total) {
		total = limit
	}
	return total
}

// These are variables so that a test can point them at a tree of its own.
var (
	procSelfCgroup = "/proc/self/cgroup"
	cgroupRootV2   = "/sys/fs/cgroup"
	cgroupRootV1   = "/sys/fs/cgroup/memory"
)

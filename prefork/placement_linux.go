package prefork

import (
	"fmt"
	"math/bits"
	"os"
	"syscall"
	"unsafe"
)

// cpuWords and nodeWords are how many 64-bit words the CPU and node masks
// handed to the kernel have: room for 65536 CPUs and 4096 nodes, more than
// any kernel is built for. The kernel refuses a mask shorter than its own
// count, and ignores the zero words beyond it, but get_mempolicy and
// set_mempolicy also refuse a node mask longer than a page.
const (
	cpuWords  = 1024
	nodeWords = 64
)

// The memory policy modes of set_mempolicy(2) used here.
const mpolBind = 2

// usableCPUs reports the CPUs the calling thread may run on: the master's
// thread, locked to its goroutine, whose mask the children start from.
func usableCPUs() ([]int, error) {
	mask, err := getAffinity()
	if err != nil {
		return nil, err
	}
	return maskBits(mask), nil
}

// planPlacement places n children as p says on the NUMA nodes of this
// machine.
func planPlacement(p Placement, n int) ([]slot, error) {
	usable, err := usableCPUs()
	if err != nil {
		return nil, fmt.Errorf("sched_getaffinity: %w", err)
	}
	nodes, err := readTopology(os.DirFS("/sys/devices/system"), usable)
	if err != nil {
		return nil, err
	}
	return plan(nodes, n, p), nil
}

// placeThread moves the calling thread onto s's CPUs, and with bindMemory
// binds its memory to s's node, so that a child it starts inherits both: a
// child keeps its parent's affinity mask and memory policy through fork and
// execve. restore puts the thread back as it was. The caller must hold the
// thread, with runtime.LockOSThread, from before placeThread until restore
// returns.
func placeThread(s slot, bindMemory bool) (restore func(), err error) {
	old, err := getAffinity()
	if err != nil {
		return nil, fmt.Errorf("sched_getaffinity: %w", err)
	}
	if err := setAffinity(bitsMask(s.cpus, cpuWords)); err != nil {
		return nil, fmt.Errorf("sched_setaffinity to CPUs %v: %w", s.cpus, err)
	}
	if !bindMemory {
		return func() { _ = setAffinity(old) }, nil
	}
	mode, nodes, err := getMempolicy()
	if err == nil {
		if err = setMempolicy(mpolBind, bitsMask([]int{s.node}, nodeWords)); err != nil {
			err = fmt.Errorf("set_mempolicy to bind to node %d: %w", s.node, err)
		}
	} else {
		err = fmt.Errorf("get_mempolicy: %w", err)
	}
	if err != nil {
		_ = setAffinity(old)
		return nil, err
	}
	return func() {
		_ = setMempolicy(mode, nodes)
		_ = setAffinity(old)
	}, nil
}

func getAffinity() ([]uint64, error) {
	mask := make([]uint64, cpuWords)
	_, _, errno := syscall.RawSyscall(syscall.SYS_SCHED_GETAFFINITY, 0, uintptr(len(mask)*8), uintptr(unsafe.Pointer(&mask[0])))
	if errno != 0 {
		return nil, errno
	}
	return mask, nil
}

func setAffinity(mask []uint64) error {
	_, _, errno := syscall.RawSyscall(syscall.SYS_SCHED_SETAFFINITY, 0, uintptr(len(mask)*8), uintptr(unsafe.Pointer(&mask[0])))
	if errno != 0 {
		return errno
	}
	return nil
}

// getMempolicy reports the calling thread's memory policy: its mode, with
// the mode flags it was set with, and its nodes, ready for setMempolicy to
// put back.
func getMempolicy() (mode int, nodes []uint64, err error) {
	var m int32
	nodes = make([]uint64, nodeWords)
	_, _, errno := syscall.Syscall6(syscall.SYS_GET_MEMPOLICY, uintptr(unsafe.Pointer(&m)), uintptr(unsafe.Pointer(&nodes[0])), uintptr(len(nodes)*64), 0, 0, 0)
	if errno != 0 {
		return 0, nil, errno
	}
	return int(m), nodes, nil
}

func setMempolicy(mode int, nodes []uint64) error {
	_, _, errno := syscall.Syscall(syscall.SYS_SET_MEMPOLICY, uintptr(mode), uintptr(unsafe.Pointer(&nodes[0])), uintptr(len(nodes)*64))
	if errno != 0 {
		return errno
	}
	return nil
}

// bitsMask is the kernel mask of words words with the given bits set.
func bitsMask(set []int, words int) []uint64 {
	mask := make([]uint64, words)
	for _, b := range set {
		mask[b/64] |= 1 << (b % 64)
	}
	return mask
}

// maskBits lists the bits set in mask, ascending.
func maskBits(mask []uint64) []int {
	var set []int
	for i, w := range mask {
		for w != 0 {
			b := bits.TrailingZeros64(w)
			set = append(set, i*64+b)
			w &^= 1 << b
		}
	}
	return set
}

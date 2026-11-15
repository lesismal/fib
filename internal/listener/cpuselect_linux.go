//go:build linux && (amd64 || arm64 || arm || riscv64 || ppc64 || ppc64le || loong64)

package listener

import (
	"runtime"
	"syscall"
	"unsafe"
)

// soAttachReusePortCBPF is SO_ATTACH_REUSEPORT_CBPF, which the syscall
// package does not define: 51 on every architecture this file builds for.
const soAttachReusePortCBPF = 51

// Classic BPF opcodes (uapi/linux/filter.h, uapi/linux/bpf_common.h) and the
// ancillary offset that loads the number of the CPU the program runs on.
const (
	bpfLdWAbs = 0x20 // BPF_LD | BPF_W | BPF_ABS
	bpfModK   = 0x94 // BPF_ALU | BPF_MOD | BPF_K
	bpfRetA   = 0x16 // BPF_RET | BPF_A
	skfAdCPU  = 0xfffff000 + 36
)

// cpuSelector is the program attachCPUSelector sets: the CPU the SYN is
// being received on, modulo the group's size, is the index of the socket
// that gets the connection. The kernel runs it in the softirq that receives
// the SYN, which over loopback is the CPU of the thread that called connect.
// An index past the group's sockets, while it is still filling, falls back
// to the kernel's hash.
func cpuSelector(n int) []syscall.SockFilter {
	return []syscall.SockFilter{
		{Code: bpfLdWAbs, K: skfAdCPU},
		{Code: bpfModK, K: uint32(n)},
		{Code: bpfRetA},
	}
}

// attachCPUSelector sets cpuSelector(n) as the selector of fd's reuseport
// group. It needs no capability; a kernel without reuseport selectors for
// TCP (before Linux 4.6) refuses it.
func attachCPUSelector(fd, n int) error {
	filter := cpuSelector(n)
	prog := syscall.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
	_, _, errno := syscall.Syscall6(syscall.SYS_SETSOCKOPT, uintptr(fd), syscall.SOL_SOCKET,
		soAttachReusePortCBPF, uintptr(unsafe.Pointer(&prog)), unsafe.Sizeof(prog), 0)
	runtime.KeepAlive(filter)
	if errno != 0 {
		return errno
	}
	return nil
}

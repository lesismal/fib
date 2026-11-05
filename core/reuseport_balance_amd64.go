//go:build linux && amd64

package core

// sysBPF is the bpf(2) system call number on this architecture.
const sysBPF = 321

// soAttachReusePortEBPF is SO_ATTACH_REUSEPORT_EBPF, which the syscall
// package does not define. It is 0x34 on every architecture but the
// sparc and mips families, which the balancer does not cover.
const soAttachReusePortEBPF = 0x34

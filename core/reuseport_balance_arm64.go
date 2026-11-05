//go:build linux && arm64

package core

// sysBPF is the bpf(2) system call number on this architecture.
const sysBPF = 280

// soAttachReusePortEBPF is SO_ATTACH_REUSEPORT_EBPF; see the amd64 file.
const soAttachReusePortEBPF = 0x34

//go:build !linux

package tls

import "syscall"

// kernelTLSFits reports false: only Linux's kernel has TLS this package hands
// sealing to.
func kernelTLSFits(*recordKeys) bool { return false }

func startKernelTLS(uintptr, *recordKeys) error { return syscall.ENOTSUP }

func sendKernelRecord(uintptr, byte, []byte) (bool, error) { return false, syscall.ENOTSUP }

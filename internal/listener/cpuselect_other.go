//go:build (linux || darwin || dragonfly || freebsd || netbsd || openbsd) && !(linux && (amd64 || arm64 || arm || riscv64 || ppc64 || ppc64le || loong64))

package listener

import "errors"

// attachCPUSelector reports that this platform has no reuseport selectors,
// so the group keeps the kernel's hash, or, outside Linux, its own spreading.
func attachCPUSelector(int, int) error {
	return errors.New("fib: reuseport CPU selector is not supported on this platform")
}

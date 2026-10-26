//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package sys

import (
	"encoding/binary"
)

// sysctlUint decodes the native-endian integer a BSD sysctl returns as a
// string. The sysctl package writes the value's bytes, not its decimal
// spelling, so hw.memsize on a 16 GiB machine is eight bytes rather than
// "17179869184".
func sysctlUint(raw string) uint64 {
	b := []byte(raw)
	switch {
	case len(b) >= 8:
		return binary.NativeEndian.Uint64(b[:8])
	case len(b) >= 4:
		return uint64(binary.NativeEndian.Uint32(b[:4]))
	}
	return 0
}

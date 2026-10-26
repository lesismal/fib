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
	// A sysctl value is trimmed of trailing zero bytes by the sysctl
	// wrapper, so a 36 GiB hw.memsize comes back as seven bytes rather than
	// eight. Pad to the width and decode.
	var b [8]byte
	copy(b[:], raw)
	return binary.NativeEndian.Uint64(b[:])
}

//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package sys

import (
	"encoding/binary"
	"testing"
)

// A sysctl value is raw bytes, and the wrapper trims the trailing NUL: a
// 36 GiB hw.memsize comes back as seven bytes rather than eight.
func TestSysctlUintPadsTrimmedBytes(t *testing.T) {
	raw := func(v uint64, n int) []byte {
		var b [8]byte
		binary.NativeEndian.PutUint64(b[:], v)
		return b[:n]
	}
	for _, c := range []struct {
		raw  []byte
		want uint64
	}{
		{raw(9<<32, 7), 9 << 32},
		{raw(1<<24, 4), 1 << 24},
		{raw(2, 7), 2},
		{nil, 0},
	} {
		if got := sysctlUint(string(c.raw)); got != c.want {
			t.Errorf("sysctlUint(%v) = %d, want %d", c.raw, got, c.want)
		}
	}
}

//go:build darwin

package fib

import (
	"syscall"
	"testing"
)

// TestKqueueExceptNeedsOOB checks that the except filter, which XNU fires
// with every read, only reports priority input when the kernel marks urgent
// data, so that ordinary reads cost no MSG_OOB receive.
func TestKqueueExceptNeedsOOB(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ev     syscall.Kevent_t
		events uint32
	}{
		{"ordinary data", syscall.Kevent_t{Filter: evfiltExcept, Flags: syscall.EV_CLEAR}, 0},
		{"urgent data", syscall.Kevent_t{Filter: evfiltExcept, Flags: syscall.EV_CLEAR, Fflags: noteOOB}, evPri},
		{"end of stream", syscall.Kevent_t{Filter: evfiltExcept, Flags: syscall.EV_EOF}, 0},
		{"end of stream with an error", syscall.Kevent_t{Filter: evfiltExcept, Flags: syscall.EV_EOF,
			Fflags: uint32(syscall.ECONNRESET)}, 0},
		{"read", syscall.Kevent_t{Filter: syscall.EVFILT_READ}, evIn},
		{"read to an error", syscall.Kevent_t{Filter: syscall.EVFILT_READ, Flags: syscall.EV_EOF,
			Fflags: uint32(syscall.ECONNRESET)}, evIn | evRdHup | evErr},
	} {
		if got := kqueueEvents(&tc.ev); got != tc.events {
			t.Errorf("%s: events %#x, want %#x", tc.name, got, tc.events)
		}
	}
}

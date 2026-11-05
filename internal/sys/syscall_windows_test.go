//go:build windows

package sys

import (
	"errors"
	"syscall"
	"testing"
)

// A refused or unroutable connect matches both its Windows code and the
// error package syscall names it by everywhere; other codes are left alone.
func TestPortableErrno(t *testing.T) {
	for _, tc := range []struct {
		errno, portable syscall.Errno
	}{
		{errorConnectionRefused, syscall.ECONNREFUSED},
		{wsaeConnRefused, syscall.ECONNREFUSED},
		{errorNetworkUnreachable, syscall.ENETUNREACH},
		{wsaeNetUnreach, syscall.ENETUNREACH},
		{errorHostUnreachable, syscall.EHOSTUNREACH},
		{wsaeHostUnreach, syscall.EHOSTUNREACH},
	} {
		err := PortableErrno(tc.errno)
		if !errors.Is(err, tc.portable) || !errors.Is(err, tc.errno) || err.Error() != tc.errno.Error() {
			t.Errorf("%d: %v does not match %v and itself", tc.errno, err, tc.portable)
		}
	}
	if err := PortableErrno(WSAECONNRESET); err != WSAECONNRESET {
		t.Errorf("WSAECONNRESET became %v", err)
	}
}

// A read or a send that a reset or an abort ended matches the portable error
// as well as its own code; every other error, the codes the engine compares
// against included, is left as it is.
func TestPortableTransferErrno(t *testing.T) {
	for _, tc := range []struct {
		errno, portable syscall.Errno
	}{
		{errorNetnameDeleted, syscall.ECONNRESET},
		{WSAECONNRESET, syscall.ECONNRESET},
		{errorConnectionAborted, syscall.ECONNABORTED},
		{wsaeConnAborted, syscall.ECONNABORTED},
	} {
		err := PortableTransferErrno(tc.errno)
		if !errors.Is(err, tc.portable) || !errors.Is(err, tc.errno) || err.Error() != tc.errno.Error() {
			t.Errorf("%d: %v does not match %v and itself", tc.errno, err, tc.portable)
		}
	}
	for _, err := range []error{nil, syscall.ERROR_HANDLE_EOF, syscall.ERROR_IO_PENDING, syscall.Errno(10035), errors.New("other")} {
		if got := PortableTransferErrno(err); got != err {
			t.Errorf("%v became %v", err, got)
		}
	}
}

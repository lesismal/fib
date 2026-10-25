//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package fib

import (
	"syscall"
	"testing"
	"time"
)

// udpListenerWoken is how many times the loop found room on l's socket.
func udpListenerWoken(l *udpListener) int {
	l.writable.mu.Lock()
	defer l.writable.mu.Unlock()
	return l.writable.woken
}

// TestUDPWaitersClose checks that closing a socket lets the senders waiting
// for it to have room go on, and that none waits on it after.
func TestUDPWaitersClose(t *testing.T) {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	ran := make(chan struct{})
	w := &udpWaiters{armed: true, fns: []func(){func() { close(ran) }}}
	if err := w.closeFD(fd); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ran:
	case <-time.After(10 * time.Second):
		t.Fatal("a waiter never ran once its socket closed")
	}
	if w.wait(&Engine{}, fd, 0, false, func() {}) {
		t.Fatal("wait waits on a closed socket")
	}
}

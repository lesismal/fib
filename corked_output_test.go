//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"io"
	"runtime"
	"testing"
	"time"
)

// waitOutput's fast path reads what the connection has queued without taking
// the lock, so it has to see bytes another goroutine has queued under a cork:
// those are counted in corkedBytes, which pendingBytes does not include until
// the cork comes off. A sender that looked only at pendingBytes would be told
// the queue is short while a corked round held megabytes, and would go on
// queueing, which is what a response written beside a streaming one used to
// do.
func TestWaitOutputSeesCorkedBytes(t *testing.T) {
	if runtime.GOOS == "windows" {
		// An overlapped send hands the whole queued slice to the kernel at
		// once, so the queue drains as far as fib is concerned even with a
		// small SO_SNDBUF, which Windows keeps as a hint: there is no
		// backlog left for the wait to stand behind.
		t.Skip("Windows hands a queued send to the kernel whole")
	}
	c, client := connectOne(t, HandlerFuncs{})
	defer client.Close()
	// Keep the socket from swallowing the whole payload, so the wait really
	// has a backlog to wait for.
	if err := c.SetWriteBuffer(64 << 10); err != nil {
		t.Fatal(err)
	}
	const (
		limit = 1 << 20
		total = 3 << 20
	)
	c.Cork()
	if err := c.Send(make([]byte, total)); err != nil {
		t.Fatal(err)
	}
	if n := c.corkedBytes; n != total {
		t.Fatalf("%d bytes are corked, want %d", n, total)
	}
	if n := c.pendingBytes.Load(); n != 0 {
		t.Fatalf("%d bytes are pending before the cork comes off, want 0", n)
	}
	waited := make(chan error, 1)
	go func() { waited <- c.waitOutput(limit) }()
	select {
	case err := <-waited:
		t.Fatalf("waitOutput returned with %v while %d bytes waited under a cork", err, total)
	case <-time.After(200 * time.Millisecond):
	}
	// The peer reads it all, so the wait can end.
	drained := make(chan error, 1)
	go func() {
		_, err := io.CopyN(io.Discard, client, total)
		drained <- err
	}()
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("waitOutput: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("waitOutput did not return once the peer read what was queued")
	}
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("the peer read: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the peer never read what was queued")
	}
}

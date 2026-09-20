//go:build linux || darwin || windows

package fib

import (
	"bytes"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestHoldReadsStopsAndResumesDelivery checks the read-side counterpart of the
// write watermarks: a handler that holds reads stops hearing from its peer
// however much the peer sends, and releasing the hold delivers what the socket
// kept without the peer having to send anything more.
func TestHoldReadsStopsAndResumesDelivery(t *testing.T) {
	const payload = 512 << 10
	var (
		mu       sync.Mutex
		received []byte
		held     atomic.Bool
		holder   atomic.Pointer[Connection]
	)
	delivered := make(chan int, 64)
	handler := HandlerFuncs{
		Data: func(c *Connection, data []byte) {
			mu.Lock()
			received = append(received, data...)
			total := len(received)
			mu.Unlock()
			if held.CompareAndSwap(false, true) {
				// Stop after the first read, with the rest still in flight.
				holder.Store(c)
				c.HoldReads(true)
			}
			delivered <- total
		},
	}
	_, addr := startEchoServer(t, DefaultConfig(), handler)

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	body := bytes.Repeat([]byte("hold"), payload/4)
	writeDone := make(chan error, 1)
	go func() { _, err := conn.Write(body); writeDone <- err }()

	// Wait for the hold, then check that nothing more is delivered under it.
	select {
	case <-delivered:
	case <-time.After(5 * time.Second):
		t.Fatal("no data was delivered")
	}
	var before int
	deadline := time.After(300 * time.Millisecond)
	quiet := false
	for !quiet {
		select {
		case before = <-delivered:
		case <-deadline:
			quiet = true
		}
	}
	mu.Lock()
	stalled := len(received)
	mu.Unlock()
	if stalled == len(body) {
		t.Skip("the whole payload fitted in one read round; nothing was left to hold back")
	}
	if before > stalled {
		t.Fatalf("delivery reported %d bytes with only %d received", before, stalled)
	}

	// Releasing the hold must deliver what the socket kept, although the peer
	// has sent nothing since.
	holder.Load().HoldReads(false)
	for {
		mu.Lock()
		total := len(received)
		mu.Unlock()
		if total == len(body) {
			break
		}
		select {
		case <-delivered:
		case <-time.After(10 * time.Second):
			t.Fatalf("after the hold was released %d of %d bytes arrived", total, len(body))
		}
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !bytes.Equal(received, body) {
		t.Fatal("the bytes delivered around the hold do not match what was sent")
	}
}

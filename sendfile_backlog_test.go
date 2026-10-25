//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

// TestSendFileCountsInBacklog checks that a file SendFile queued, which
// sendfile sends straight from the file, counts in the backlog: Unsent and
// NotifyOutput see it while the peer reads nothing, and NotifyOutput's
// function runs only once the peer has read it down.
func TestSendFileCountsInBacklog(t *testing.T) {
	const size = 32 << 20
	path, data := writeTestFile(t, size)
	c, client := connectOne(t, HandlerFuncs{})
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := c.SendFile(f, 0, size); err != nil {
		t.Fatal(err)
	}
	if n := c.unsent(); n < size/2 {
		t.Fatalf("Unsent is %d with %d bytes of a file queued", n, size)
	}
	notified := make(chan error, 1)
	if !c.NotifyOutput(1<<20, func(err error) { notified <- err }) {
		t.Fatal("NotifyOutput saw room with the file queued and unread")
	}
	select {
	case err := <-notified:
		t.Fatalf("NotifyOutput's function ran with the file unread: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	got := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(io.LimitReader(client, size))
		got <- b
	}()
	select {
	case err := <-notified:
		if err != nil {
			t.Fatalf("NotifyOutput's function heard %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("NotifyOutput's function never ran as the peer read the file")
	}
	if b := <-got; !bytes.Equal(b, data) {
		t.Fatalf("the peer read %d bytes, or they differ", len(b))
	}
	if n := c.unsent(); n != 0 {
		t.Fatalf("Unsent is %d with the file sent", n)
	}
}

// TestSendFileWriteDeadline checks that a write deadline closes a connection
// whose queued file its peer does not read, as it would one whose bytes from
// Send it does not read.
func TestSendFileWriteDeadline(t *testing.T) {
	const size = 32 << 20
	path, _ := writeTestFile(t, size)
	closed := make(chan error, 1)
	c, _ := connectOne(t, HandlerFuncs{Close: func(_ *Connection, err error) { closed <- err }})
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := c.SendFile(f, 0, size); err != nil {
		t.Fatal(err)
	}
	if err := c.SetWriteDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("closed with %v, want %v", err, os.ErrDeadlineExceeded)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the write deadline left the connection open with its file unread")
	}
}

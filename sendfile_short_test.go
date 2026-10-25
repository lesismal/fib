//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package fib

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A sendfile that comes up short without an error, and with the socket still
// room to spare, as Linux's does when its read of the file comes up short, is
// called again rather than waited on: the kernel raises a write edge only for
// a socket that ran out of room, so waiting left the rest of the file queued
// for good, with the socket empty and the peer waiting.
func TestSendFileShortCountWithRoomLeft(t *testing.T) {
	const size = 4 << 20
	data := make([]byte, size)
	rand.New(rand.NewSource(1)).Read(data)
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })

	// Each call moves at most 64KB and reports no error, far below what the
	// socket takes.
	real := sendfile
	sendfile = func(out, in int, offset *int64, count int) (int, error) {
		return real(out, in, offset, min(count, 64<<10))
	}
	t.Cleanup(func() { sendfile = real })

	sent := make(chan error, 1)
	_, client := acceptOne(t, "tcp", HandlerFuncs{Open: func(c *Connection) {
		sent <- c.SendFile(f, 0, size)
	}})
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now().Add(10 * time.Second))
	got := make([]byte, size)
	if n, err := io.ReadFull(client, got); err != nil {
		t.Fatalf("the peer read %d of %d bytes: %v", n, size, err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("the peer read other bytes than the file's")
	}
}

// A file shorter than its range sends what the file has before the connection
// closes, and closes with io.ErrUnexpectedEOF: the peer is promised the whole
// range, so the short count is an error only once the bytes before it have
// gone. It is the Unix counterpart of TestTransmitFileShortFile, where the
// range TransmitFile is asked for is kept within the file and the rest read as
// it is here.
func TestSendFileShortFile(t *testing.T) {
	const size = 1 << 20
	path, data := writeTestFile(t, size)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	closed := make(chan error, 1)
	c, client := acceptOne(t, "tcp", HandlerFuncs{Close: func(_ *Connection, err error) { closed <- err }})
	if err := c.SendFile(f, 0, size+100); err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now().Add(10 * time.Second))
	got, _ := io.ReadAll(client)
	if !bytes.Equal(got, data) {
		t.Fatalf("the peer read %d bytes, or they differ", len(got))
	}
	select {
	case err := <-closed:
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("closed with %v, want %v", err, io.ErrUnexpectedEOF)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the connection stayed open")
	}
}

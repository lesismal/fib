//go:build windows

package fib

import (
	"bytes"
	"errors"
	"io"
	"os"
	"syscall"
	"testing"
	"time"
)

// A file goes by TransmitFile, from an offset, between what was sent before
// and after it, to a peer that reads it as it comes.
func TestTransmitFile(t *testing.T) {
	const size = 8<<20 + 321
	path, data := writeTestFile(t, size)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var transmits int
	real := transmitFile
	transmitFile = func(s, h syscall.Handle, n, perSend uint32, ov *syscall.Overlapped, buf *syscall.TransmitFileBuffers, flags uint32) error {
		transmits++
		return real(s, h, n, perSend, ov, buf, flags)
	}
	t.Cleanup(func() { transmitFile = real })
	c, client := connectOne(t, HandlerFuncs{})
	if err := c.Send([]byte("head")); err != nil {
		t.Fatal(err)
	}
	if err := c.SendFile(f, 10, size-20); err != nil {
		t.Fatal(err)
	}
	if err := c.Send([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now().Add(20 * time.Second))
	got := make([]byte, size-20+8)
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte("head"), data[10:size-10]...), "tail"...)
	if !bytes.Equal(got, want) {
		t.Fatal("the peer read other bytes than were sent")
	}
	c.mu.Lock()
	n := transmits
	c.mu.Unlock()
	if n == 0 {
		t.Fatal("the file did not go by TransmitFile")
	}
}

// A file shorter than its range closes the connection once TransmitFile has
// sent what it had.
func TestTransmitFileShortFile(t *testing.T) {
	const size = 1 << 20
	path, data := writeTestFile(t, size)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	closed := make(chan error, 1)
	c, client := connectOne(t, HandlerFuncs{Close: func(_ *Connection, err error) { closed <- err }})
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

// A socket TransmitFile refuses has the file read a chunk at a time instead.
func TestTransmitFileRefused(t *testing.T) {
	const size = 3<<20 + 5
	path, data := writeTestFile(t, size)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	real := transmitFile
	transmitFile = func(syscall.Handle, syscall.Handle, uint32, uint32, *syscall.Overlapped, *syscall.TransmitFileBuffers, uint32) error {
		return wsaEOPNOTSUPP
	}
	t.Cleanup(func() { transmitFile = real })
	c, client := connectOne(t, HandlerFuncs{})
	if err := c.SendFile(f, 0, size); err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now().Add(20 * time.Second))
	got := make([]byte, size)
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("the peer read other bytes than the file's")
	}
}

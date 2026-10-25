//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// connectOne serves handler on a loopback port and returns the connection the
// engine accepted, once it has opened, and the client's end of it.
func connectOne(t *testing.T, handler HandlerFuncs) (*Connection, net.Conn) {
	t.Helper()
	opened := make(chan *Connection, 1)
	open := handler.Open
	handler.Open = func(c *Connection) {
		if open != nil {
			open(c)
		}
		opened <- c
	}
	addr := startServer(t, "tcp", "127.0.0.1:0", handler)
	client, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	select {
	case c := <-opened:
		return c, client
	case <-time.After(10 * time.Second):
		t.Fatal("the connection never opened")
	}
	return nil, nil
}

// frameSealer seals each chunk into a frame: its length, then its bytes with
// every bit flipped, so that a peer can tell sealed bytes from the file's and
// where each chunk began.
type frameSealer struct{ calls int }

func (s *frameSealer) Seal(dst, plain []byte) ([]byte, error) {
	s.calls++
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(plain)))
	for _, b := range plain {
		dst = append(dst, ^b)
	}
	return dst, nil
}

func (*frameSealer) SealedLen(n int) int { return n + 4 }

// unseal reads frames from r until it has want bytes of plaintext.
func unseal(t *testing.T, r io.Reader, want int) []byte {
	t.Helper()
	out := make([]byte, 0, want)
	var head [4]byte
	for len(out) < want {
		if _, err := io.ReadFull(r, head[:]); err != nil {
			t.Fatalf("read %d of %d bytes: %v", len(out), want, err)
		}
		n := int(binary.BigEndian.Uint32(head[:]))
		if n > sendFileChunk {
			t.Fatalf("a frame of %d bytes, past a chunk", n)
		}
		start := len(out)
		out = out[:start+n]
		if _, err := io.ReadFull(r, out[start:]); err != nil {
			t.Fatal(err)
		}
		for i := start; i < len(out); i++ {
			out[i] = ^out[i]
		}
	}
	return out
}

// A file SendFileRaw seals is read a chunk at a time as the socket takes it:
// while the peer reads nothing, what is in memory stays within a chunk sealed
// and the socket's buffers, and the rest counts in the backlog as file; then
// the peer reads it all, sealed in chunks, between what was sent before and
// after it.
func TestSendFileRawSealsAsTheSocketDrains(t *testing.T) {
	const size = 24<<20 + 12345
	path, data := writeTestFile(t, size)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	c, client := connectOne(t, HandlerFuncs{})
	sealer := &frameSealer{}
	const offset = 1000
	count := int64(size - offset - 77)
	if err := c.Send([]byte("head")); err != nil {
		t.Fatal(err)
	}
	if err := c.SendFileRaw(f, offset, count, sealer); err != nil {
		t.Fatal(err)
	}
	if err := c.Send([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if n := c.unsent(); n < count/2 {
		t.Fatalf("unsent is %d with %d bytes of a file queued and unread", n, count)
	}
	if n := c.pendingBytes.Load(); n > int64(sealer.SealedLen(sendFileChunk))+4 {
		t.Fatalf("%d bytes are in memory with the peer reading nothing", n)
	}
	_ = client.SetReadDeadline(time.Now().Add(20 * time.Second))
	var head [4]byte
	if _, err := io.ReadFull(client, head[:]); err != nil || string(head[:]) != "head" {
		t.Fatalf("read %q, %v", head, err)
	}
	if got := unseal(t, client, int(count)); !bytes.Equal(got, data[offset:offset+count]) {
		t.Fatal("the peer read other bytes than the file's")
	}
	if _, err := io.ReadFull(client, head[:]); err != nil || string(head[:]) != "tail" {
		t.Fatalf("read %q, %v", head, err)
	}
	if want := int((count + sendFileChunk - 1) / sendFileChunk); sealer.calls != want {
		t.Fatalf("sealed %d chunks, want %d", sealer.calls, want)
	}
	if n := c.unsent(); n != 0 {
		t.Fatalf("unsent is %d with the file sent", n)
	}
}

// A sealed file that turns out shorter than its range closes the connection,
// after what it did have.
func TestSendFileRawSealedShortFile(t *testing.T) {
	const size = 200 << 10
	path, data := writeTestFile(t, size)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	closed := make(chan error, 1)
	c, client := connectOne(t, HandlerFuncs{Close: func(_ *Connection, err error) { closed <- err }})
	if err := c.SendFileRaw(f, 0, size+100, &frameSealer{}); err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("closed with %v, want %v", err, io.ErrUnexpectedEOF)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the connection stayed open")
	}
	_ = client.SetReadDeadline(time.Now().Add(10 * time.Second))
	all, _ := io.ReadAll(client)
	got := unseal(t, bytes.NewReader(all), size/sendFileChunk*sendFileChunk)
	if !bytes.Equal(got, data[:len(got)]) {
		t.Fatal("the peer read other bytes than the file's")
	}
}

// ControlIdle runs its function only while nothing is queued, and
// SendRawRecord sends nothing where the kernel has no TLS.
func TestControlIdle(t *testing.T) {
	c, client := connectOne(t, HandlerFuncs{})
	ran, err := c.ControlIdle(func(fd uintptr) error {
		if fd == 0 {
			return errors.New("no socket")
		}
		return nil
	})
	if !ran || err != nil {
		t.Fatalf("ControlIdle on an idle connection = %v, %v", ran, err)
	}
	// Fill the socket until a send is queued.
	chunk := make([]byte, 1<<20)
	for i := 0; c.unsent() == 0; i++ {
		if i == 256 {
			t.Fatal("the socket took 256MB with the peer reading nothing")
		}
		if err := c.Send(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if ran, _ := c.ControlIdle(func(uintptr) error { return nil }); ran {
		t.Fatal("ControlIdle ran with sends queued")
	}
	go func() { _, _ = io.Copy(io.Discard, client) }()
	deadline := time.Now().Add(10 * time.Second)
	for c.unsent() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if ran, _ := c.ControlIdle(func(uintptr) error { return nil }); !ran {
		t.Fatal("ControlIdle did not run once the queue drained")
	}
	if err := c.SendRawRecord(23, []byte("x")); runtime.GOOS != "linux" && err == nil {
		t.Fatal("SendRawRecord sent where the kernel has no TLS")
	}
	_ = c.Close()
	if ran, _ := c.ControlIdle(func(uintptr) error { return nil }); ran {
		t.Fatal("ControlIdle ran on a closed connection")
	}
	if err := c.SendRawRecord(23, []byte("x")); runtime.GOOS == "linux" && err != syscall.EPIPE {
		t.Fatalf("SendRawRecord on a closed connection = %v, want EPIPE", err)
	}
}

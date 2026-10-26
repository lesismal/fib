//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"
	"unsafe"

	"github.com/lesismal/fib/internal/outputwait"
)

// bodyTail reports whether data is the end of body, which is what a parts send
// queues when the socket has taken the part of it before: the caller's slice,
// never a copy of it.
func bodyTail(data, body []byte) bool {
	if len(data) == 0 || len(data) > len(body) {
		return false
	}
	lo := uintptr(unsafe.Pointer(unsafe.SliceData(body)))
	p0 := uintptr(unsafe.Pointer(unsafe.SliceData(data)))
	return p0 >= lo && p0+uintptr(len(data)) == lo+uintptr(len(body))
}

// SendPartsShared keeps the caller's second part as it was given: corked, so
// that nothing can reach the socket before the test looks, the whole of it has
// to be queued, and the backlog counts it by its length as a copied part was
// counted. The head beside it is copied into the queue, as it is for
// SendParts, so the buffer a response built it in may go back to its pool. Once
// the socket drains the queue the peer gets exactly what was given, in order,
// and the caller's array is untouched.
func TestSendPartsSharedQueuesTheCallersBody(t *testing.T) {
	c, peer := acceptOne(t, "tcp", HandlerFuncs{})
	// A small send buffer keeps the kernel from swallowing the whole body, so
	// that the queue the test inspects is where the rest of it waits.
	if err := c.SetWriteBuffer(1 << 20); err != nil {
		t.Fatal(err)
	}
	header := []byte("HTTP/1.1 200 OK\r\nContent-Length: 8388608\r\n\r\n")
	body := make([]byte, 8<<20)
	for i := range body {
		body[i] = byte(i * 7)
	}
	want := bytes.Clone(body)

	c.Cork()
	if err := c.SendPartsShared(header, body); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	if backlog, want := c.pendingBytes.Load()+c.corkedBytes, int64(len(header)+len(body)); backlog != want {
		c.mu.Unlock()
		t.Fatalf("backlog = %d, want %d", backlog, want)
	}
	var bodyItem *sendItem
	headCopied := false
	for i := c.sendHead; i < len(c.sends); i++ {
		item := &c.sends[i]
		if item.pooled && len(item.data) == len(header) && bytes.Equal(item.data, header) {
			// The head is the queue's own buffer, not the caller's array:
			// copying it is what lets a caller pool it after the send.
			if unsafe.SliceData(item.data) == unsafe.SliceData(header) {
				t.Error("the head is queued as the caller's array")
			}
			headCopied = true
			continue
		}
		if bodyTail(item.data[item.offset:], body) {
			bodyItem = item
		}
	}
	if !headCopied {
		t.Error("the head is not queued as a pooled buffer")
	}
	if bodyItem == nil {
		c.mu.Unlock()
		t.Fatal("the body is not queued: it was copied")
	}
	if bodyItem.pooled {
		t.Error("the body is queued as a pooled buffer")
	}
	if bodyItem.offset != 0 {
		t.Errorf("the queued body starts at offset %d, want 0", bodyItem.offset)
	}
	c.mu.Unlock()

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	// Whatever the socket could not take stays queued as the same array, with
	// the offset of what has gone, and the backlog counts what is left of it.
	c.mu.Lock()
	var bodyLeft, headLeft int64
	for i := c.sendHead; i < len(c.sends); i++ {
		item := &c.sends[i]
		left := int64(len(item.data) - item.offset)
		if bodyTail(item.data[item.offset:], body) {
			bodyLeft = left
			if item.pooled {
				t.Error("the body became a pooled item after the flush")
			}
			continue
		}
		headLeft += left
	}
	if got := c.pendingBytes.Load(); got != bodyLeft+headLeft {
		t.Errorf("pendingBytes = %d, want the %d bytes left queued", got, bodyLeft+headLeft)
	}
	c.mu.Unlock()

	if got := readAllFrom(t, peer, len(header)+len(body)); !bytes.Equal(got, append(header, want...)) {
		t.Fatal("what the peer read differs from what was sent")
	}
	if !bytes.Equal(body, want) {
		t.Fatal("the caller's body was written into")
	}
}

// A send that takes a shared body past the connection's queue limit closes the
// connection, as a copying one does: the cap counts the bytes waiting whether
// they belong to the caller or to the queue, so a peer that never reads cannot
// hold an unbounded queue of references.
func TestSendPartsSharedPastTheQueueLimitClosesTheConnection(t *testing.T) {
	const limit = 64 << 10
	opened := make(chan *Connection, 1)
	closeErr := make(chan error, 1)
	config := DefaultConfig()
	config.MaxWriteQueueBytes = limit
	_, addr := startEchoServer(t, config, HandlerFuncs{
		Open:  func(c *Connection) { opened <- c },
		Close: func(_ *Connection, err error) { closeErr <- err },
	})
	peer, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	c := <-opened
	body := bytes.Repeat([]byte{'x'}, 1<<20)
	want := bytes.Clone(body)
	c.Cork()
	if err := c.SendPartsShared([]byte("HTTP/1.1 200 OK\r\n\r\n"), body); !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("SendPartsShared past the limit = %v, want ErrSendQueueFull", err)
	}
	select {
	case err := <-closeErr:
		if !errors.Is(err, ErrSendQueueFull) {
			t.Fatalf("OnClose = %v, want ErrSendQueueFull", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the connection was not closed")
	}
	// The connection has let the body go, so the caller may do as it likes
	// with it: the queue never owned it, and dropping it wrote nothing.
	if !bytes.Equal(body, want) {
		t.Fatal("the caller's body was written into")
	}
}

// Closing a connection with a shared body queued drops the reference without
// touching the array or giving it to a pool: the response is over, so the
// caller owns it again and finds it as it left it. Under the race detector,
// a send path that wrote into it while it waited, or a close that read it
// after the test reused it, would show.
func TestCloseDropsTheCallersQueuedBody(t *testing.T) {
	closed := make(chan struct{}, 1)
	c, _ := acceptOne(t, "tcp", HandlerFuncs{
		Close: func(*Connection, error) { closed <- struct{}{} },
	})
	// Small enough that the socket cannot take the whole body at once, so
	// part of it has to be queued.
	if err := c.SetWriteBuffer(64 << 10); err != nil {
		t.Fatal(err)
	}
	body := make([]byte, 8<<20)
	for i := range body {
		body[i] = byte(i)
	}
	want := bytes.Clone(body)
	if err := c.SendPartsShared([]byte("HTTP/1.1 200 OK\r\nContent-Length: 8388608\r\n\r\n"), body); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	queued := false
	for i := c.sendHead; i < len(c.sends); i++ {
		item := &c.sends[i]
		if bodyTail(item.data[item.offset:], body) {
			queued = true
		}
	}
	c.mu.Unlock()
	if !queued {
		t.Fatal("the body is not queued: it was copied or written at once")
	}
	if pending := outputwait.Unsent(c); pending <= 0 {
		t.Fatalf("nothing is counted as queued with a %d-byte body", len(body))
	}
	if !bytes.Equal(body, want) {
		t.Fatal("the caller's body was written into while it waited")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("the connection was not closed")
	}
	// The response is over, so the caller owns the array again: the close
	// dropped the queue without writing into it or handing it to a pool, and
	// what the caller does with it now stays as it left it.
	for i := range body {
		body[i] = 0xA5
	}
	for i := range body {
		if body[i] != 0xA5 {
			t.Fatalf("the connection touched the caller's body at %d: %#x", i, body[i])
		}
	}
}

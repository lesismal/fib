//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package core

import (
	"bytes"
	"errors"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/lesismal/fib/bufferpool"
)

// putWatch is an allocator that counts the times one buffer goes back to the
// pool, and passes everything on to the built-in pool.
type putWatch struct {
	watch atomic.Pointer[byte]
	puts  atomic.Int64
}

func (p *putWatch) Get(size int) []byte { return bufferpool.Builtin.Get(size) }

func (p *putWatch) Put(buf []byte) {
	if unsafe.SliceData(buf) == p.watch.Load() {
		p.puts.Add(1)
	}
	bufferpool.Builtin.Put(buf)
}

// watchPuts installs a putWatch for the test and returns a pooled buffer of
// size bytes, patterned, that it watches.
func watchPuts(t *testing.T, size int) (*putWatch, []byte) {
	t.Helper()
	p := &putWatch{}
	bufferpool.SetAllocator(p)
	t.Cleanup(func() { bufferpool.SetAllocator(nil) })
	body := bufferpool.Get(size)
	for i := range body {
		body[i] = byte(i * 7)
	}
	p.watch.Store(unsafe.SliceData(body))
	return p, body
}

// waitPuts fails t unless the watched buffer has gone back to the pool want
// times within a few seconds.
func waitPuts(t *testing.T, p *putWatch, want int64, what string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); p.puts.Load() < want; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%s: the body went back to the pool %d times, want %d", what, p.puts.Load(), want)
		}
	}
	time.Sleep(10 * time.Millisecond)
	if got := p.puts.Load(); got != want {
		t.Fatalf("%s: the body went back to the pool %d times, want %d", what, got, want)
	}
}

// A body the socket takes whole goes back to the pool before SendPartsOwned
// returns, and the peer gets it as it was.
func TestSendPartsOwnedWhole(t *testing.T) {
	c, peer := acceptOne(t, "tcp", HandlerFuncs{})
	p, body := watchPuts(t, 8<<10)
	want := append([]byte("head "), body...)
	if err := c.SendPartsOwned([]byte("head "), body); err != nil {
		t.Fatal(err)
	}
	if got := readAllFrom(t, peer, len(want)); !bytes.Equal(got, want) {
		t.Fatal("what the peer read differs from what was sent")
	}
	waitPuts(t, p, 1, "sent whole")
}

// A body the socket cannot take waits in the queue as the buffer it is,
// whole and marked to go back to the pool, with the offset of what has gone;
// it goes back once the rest is sent, and not before.
func TestSendPartsOwnedQueuedThenSent(t *testing.T) {
	c, peer := acceptOne(t, "tcp", HandlerFuncs{})
	if err := c.SetWriteBuffer(1 << 20); err != nil {
		t.Fatal(err)
	}
	header := []byte("HTTP/1.1 200 OK\r\nContent-Length: 8388608\r\n\r\n")
	p, body := watchPuts(t, 8<<20)
	want := append(bytes.Clone(header), body...)
	c.Cork()
	if err := c.SendPartsOwned(header, body); err != nil {
		t.Fatal(err)
	}
	check := func(when string, offsetMayMove bool) {
		t.Helper()
		c.mu.Lock()
		defer c.mu.Unlock()
		for i := c.sendHead; i < len(c.sends); i++ {
			item := &c.sends[i]
			if unsafe.SliceData(item.data) != unsafe.SliceData(body) {
				continue
			}
			if !item.owned || item.pooled || len(item.data) != len(body) {
				t.Fatalf("%s: the body is queued as owned %v, pooled %v, %d bytes", when, item.owned, item.pooled, len(item.data))
			}
			if !offsetMayMove && item.offset != 0 {
				t.Fatalf("%s: offset %d", when, item.offset)
			}
			return
		}
		if !offsetMayMove {
			t.Fatalf("%s: the body is not queued as the buffer it is", when)
		}
	}
	check("corked", false)
	if p.puts.Load() != 0 {
		t.Fatal("the body went back to the pool while queued")
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	check("flushed", true)
	if got := readAllFrom(t, peer, len(want)); !bytes.Equal(got, want) {
		t.Fatal("what the peer read differs from what was sent")
	}
	waitPuts(t, p, 1, "sent from the queue")
}

// Closing a connection with an owned body queued gives the body back to the
// pool with the queue.
func TestSendPartsOwnedDroppedOnClose(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows hands a queued send to the kernel whole")
	}
	closed := make(chan struct{}, 1)
	c, _ := acceptOne(t, "tcp", HandlerFuncs{
		Close: func(*Connection, error) { closed <- struct{}{} },
	})
	if err := c.SetWriteBuffer(64 << 10); err != nil {
		t.Fatal(err)
	}
	p, body := watchPuts(t, 8<<20)
	if err := c.SendPartsOwned([]byte("head"), body); err != nil {
		t.Fatal(err)
	}
	if p.puts.Load() != 0 {
		t.Fatal("a body the socket could not take went back to the pool at once")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("the connection was not closed")
	}
	waitPuts(t, p, 1, "dropped on close")
}

// A send that fails, on a closed connection or past the queue limit, gives
// the body back at once.
func TestSendPartsOwnedFailures(t *testing.T) {
	c, _ := acceptOne(t, "tcp", HandlerFuncs{})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	p, body := watchPuts(t, 8<<10)
	if err := c.SendPartsOwned([]byte("head"), body); err == nil {
		t.Fatal("a send on a closed connection succeeded")
	}
	waitPuts(t, p, 1, "closed connection")

	const limit = 64 << 10
	opened := make(chan *Connection, 1)
	config := DefaultConfig()
	config.MaxWriteQueueBytes = limit
	_, addr := startEchoServer(t, config, HandlerFuncs{Open: func(c *Connection) { opened <- c }})
	peer, err := net.DialTimeout("tcp4", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	c = <-opened
	p, body = watchPuts(t, 1<<20)
	c.Cork()
	if err := c.SendPartsOwned([]byte("head"), body); !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("past the limit = %v, want ErrSendQueueFull", err)
	}
	waitPuts(t, p, 1, "past the queue limit")
}

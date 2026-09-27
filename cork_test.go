//go:build linux || darwin || windows

package fib

import (
	"io"
	"net"
	"testing"
	"time"
)

// What is sent on a corked connection away from its rounds waits for Flush,
// and reaches the peer in one piece once it comes.
func TestCorkHoldsSendsUntilFlush(t *testing.T) {
	opened := make(chan *Connection, 1)
	addr := startServer(t, "tcp", "127.0.0.1:0", HandlerFuncs{Open: func(c *Connection) { opened <- c }})
	peer, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	c := <-opened
	c.Cork()
	for _, part := range []string{"one ", "two ", "three"} {
		if err := c.Send([]byte(part)); err != nil {
			t.Fatal(err)
		}
	}
	buf := make([]byte, 64)
	_ = peer.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if n, err := peer.Read(buf); n > 0 || err == nil {
		t.Fatalf("read %q before Flush", buf[:n])
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(peer, buf[:len("one two three")]); err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:len("one two three")]); got != "one two three" {
		t.Fatalf("read %q", got)
	}
	// Uncorked again, a send goes out on its own.
	if err := c.Send([]byte("four")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(peer, buf[:4]); err != nil || string(buf[:4]) != "four" {
		t.Fatalf("read %q, %v", buf[:4], err)
	}
}

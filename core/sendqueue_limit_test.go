//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package core

import (
	"errors"
	"net"
	"testing"
	"time"
)

// A broadcast to a peer that never reads must be bounded: a send that takes
// the connection's queue past its limit ends the connection rather than
// growing the queue forever.
func TestSendQueueLimitClosesStalledPeer(t *testing.T) {
	const limit = 64 << 10
	sendErr := make(chan error, 1)
	closeErr := make(chan error, 1)
	config := DefaultConfig()
	config.MaxWriteQueueBytes = limit
	_, addr := startEchoServer(t, config, HandlerFuncs{
		Data: func(c *Connection, _ []byte) {
			payload := make([]byte, 32<<10)
			for i := 0; i < 64; i++ {
				if err := c.Send(payload); err != nil {
					sendErr <- err
					return
				}
			}
			sendErr <- nil
		},
		Close: func(_ *Connection, err error) { closeErr <- err },
	})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if tcp, ok := conn.(*net.TCPConn); ok {
		// A small receive buffer keeps the test from depending on how much
		// the platform's default buffers would absorb.
		_ = tcp.SetReadBuffer(4 << 10)
	}
	if _, err := conn.Write([]byte("go")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-sendErr:
		if !errors.Is(err, ErrSendQueueFull) {
			t.Fatalf("Send = %v, want ErrSendQueueFull", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the sends never reached the limit")
	}
	select {
	case err := <-closeErr:
		if !errors.Is(err, ErrSendQueueFull) {
			t.Fatalf("OnClose = %v, want ErrSendQueueFull", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the connection was not closed")
	}
}

// The engine's limit is the default, and a connection can set its own or
// remove it.
func TestWriteQueueLimit(t *testing.T) {
	var c Connection
	if got := c.WriteQueueLimit(); got != 0 {
		t.Fatalf("a connection without an engine reports %d, want 0", got)
	}
	c.SetWriteQueueLimit(1 << 20)
	if got := c.WriteQueueLimit(); got != 1<<20 {
		t.Fatalf("SetWriteQueueLimit(1MB): %d", got)
	}
	// A send that fills the queue to the limit is not over it, and one that
	// would take it a byte past is, counting what a cork holds.
	c.pendingBytes.Store(1<<20 - 8)
	if c.sendQueueOverLocked(8) {
		t.Fatal("a send that fills the queue to the limit is over it")
	}
	if !c.sendQueueOverLocked(9) {
		t.Fatal("a send past the limit is not over it")
	}
	c.corkedBytes = 8
	if !c.sendQueueOverLocked(1) {
		t.Fatal("a corked backlog does not count against the limit")
	}
	c.SetWriteQueueLimit(0)
	c.pendingBytes.Store(1 << 30)
	if c.sendQueueOverLocked(1 << 30) {
		t.Fatal("a limit of zero still applies")
	}
	if got := c.WriteQueueLimit(); got != 0 {
		t.Fatalf("WriteQueueLimit after removing the cap = %d", got)
	}
}

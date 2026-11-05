//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package core

import (
	"net"
	"testing"
	"time"
)

// TestFlushFailureCloses checks that a Flush that finds the socket failed
// ends the connection, as a send that finds it so does, rather than leaving
// it open with its output queued for a caller that does not look at the
// error. On Windows nothing else would report the failure: the connection
// then had no operation posted, and stayed open, and its writers waiting for
// room waited for ever.
func TestFlushFailureCloses(t *testing.T) {
	opened := make(chan *Connection, 1)
	server, err := New(DefaultConfig(), Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{
		Open: func(c *Connection) {
			// Nothing read can close the connection before the flush does.
			c.HoldReads(true)
			opened <- c
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	})
	client, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	c := <-opened
	// A reset rather than a FIN, so that writing to the socket fails.
	_ = client.SetLinger(0)
	_ = client.Close()
	chunk := make([]byte, 64<<10)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.Cork()
		if err := c.Send(chunk); err != nil {
			// The connection is closed already.
			return
		}
		if err := c.Flush(); err != nil {
			c.mu.Lock()
			closing := c.closing || c.closed
			c.mu.Unlock()
			if !closing {
				t.Fatalf("Flush failed with %v and left the connection open", err)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("sending to a reset connection never failed")
}

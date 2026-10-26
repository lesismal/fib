//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// acceptOne starts a server on network and returns the connection it accepts
// from a plain net.Conn dialed to it, along with that client.
func acceptOne(t *testing.T, network string, handler HandlerFuncs) (*Connection, net.Conn) {
	t.Helper()
	opened := make(chan *Connection, 1)
	open := handler.Open
	handler.Open = func(c *Connection) {
		if open != nil {
			open(c)
		}
		opened <- c
	}
	config := DefaultConfig()
	config.Network = network
	addr := "127.0.0.1:0"
	if network == "unix" {
		addr = filepath.Join(t.TempDir(), "s")
	}
	server, err := New(config, Binding{Addr: addr, Handler: handler})
	if err != nil {
		t.Fatal(err)
	}
	addrs, err := server.ListenAddrs()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		<-runDone
		server.Close()
	})
	client, err := net.Dial(network, addrs[0].String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	select {
	case c := <-opened:
		return c, client
	case <-time.After(5 * time.Second):
		t.Fatal("no connection opened")
		return nil, nil
	}
}

func readAllFrom(t *testing.T, conn net.Conn, want int) []byte {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	got := make([]byte, want)
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read %d bytes: %v", want, err)
	}
	return got
}

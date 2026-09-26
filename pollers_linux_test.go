//go:build linux

package fib

import (
	"bytes"
	"io"
	"net"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// acceptsConnections reports whether fd is a listening socket.
func acceptsConnections(t *testing.T, fd int) bool {
	t.Helper()
	listening, err := syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_ACCEPTCONN)
	if err != nil {
		t.Fatal(err)
	}
	return listening != 0
}

// With ReusePort, each poller listens on a socket of its own, bound where
// the engine's is, and accepts its connections itself; the engine's socket
// only holds the address and the port the kernel chose, and takes none.
func TestPollersAcceptWithReusePort(t *testing.T) {
	const pollers, conns = 3, 30
	config := pollerConfig("pollers-reuseport", pollers)
	config.ReusePort = true
	opened := make(chan *Connection, conns)
	server, addr := startEchoServer(t, config, HandlerFuncs{
		Open: func(c *Connection) { opened <- c },
		Data: func(c *Connection, b []byte) {
			if err := c.Send(b); err != nil {
				c.Close()
			}
		},
	})
	if !server.pollersListen || len(server.listenFDs) != 1 {
		t.Fatalf("engine has pollersListen=%v and %d listeners", server.pollersListen, len(server.listenFDs))
	}
	if acceptsConnections(t, server.listenFDs[0]) {
		t.Fatal("the engine's own socket listens beside its pollers'")
	}
	bound, err := syscall.Getsockname(server.listenFDs[0])
	if err != nil {
		t.Fatal(err)
	}
	for i, p := range server.pollers {
		if len(p.listenFDs) != 1 {
			t.Fatalf("poller %d has %d listeners, want 1", i, len(p.listenFDs))
		}
		if !acceptsConnections(t, p.listenFDs[0]) {
			t.Fatalf("poller %d's socket does not listen", i)
		}
		if got, err := syscall.Getsockname(p.listenFDs[0]); err != nil || sockaddrToAddr(got).String() != sockaddrToAddr(bound).String() {
			t.Fatalf("poller %d is bound to %v (%v), want %v", i, sockaddrToAddr(got), err, sockaddrToAddr(bound))
		}
	}

	payload := []byte("accepted by a poller")
	var wg sync.WaitGroup
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp4", addr, 5*time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
			if _, err := conn.Write(payload); err != nil {
				t.Error(err)
				return
			}
			reply := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, reply); err != nil {
				t.Error(err)
				return
			}
			if !bytes.Equal(reply, payload) {
				t.Errorf("echo mismatch: %q", reply)
			}
		}()
	}
	wg.Wait()
	used := map[*Engine]bool{}
	for i := 0; i < conns; i++ {
		c := <-opened
		if c.engine == server || c.engine.parent != server {
			t.Fatalf("connection is served by %p, want one of the engine's pollers", c.engine)
		}
		if c.Engine() != server {
			t.Fatal("Engine() does not report the engine the connection came from")
		}
		used[c.engine] = true
	}
	if len(used) < 2 {
		t.Fatalf("%d connections landed on %d poller(s)", conns, len(used))
	}
}

// A Unix socket has no SO_REUSEPORT to share, so its engine keeps accepting
// for its pollers whatever ReusePort says.
func TestReusePortLeavesUnixListenersOnEngine(t *testing.T) {
	config := pollerConfig("pollers-reuseport-unix", 2)
	config.ReusePort = true
	config.Network = "unix"
	config.Addr = filepath.Join(t.TempDir(), "fib.sock")
	server, err := Bind(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if server.pollersListen || !acceptsConnections(t, server.listenFDs[0]) {
		t.Fatal("a Unix listener was left to the pollers")
	}
	for i, p := range server.pollers {
		if len(p.listenFDs) != 0 {
			t.Fatalf("poller %d listens on %d sockets, want none", i, len(p.listenFDs))
		}
	}
}

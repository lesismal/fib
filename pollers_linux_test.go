//go:build linux

package fib

import (
	"bytes"
	"errors"
	"io"
	"net"
	"path/filepath"
	"strconv"
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

// Each poller listens on a TCP socket of its own, bound where the engine's
// is, and accepts its connections itself, with ReusePort or without it; the
// engine's socket only holds the address and the port the kernel chose, and
// takes none.
func TestPollersAcceptWithReusePort(t *testing.T) {
	for _, reusePort := range []bool{true, false} {
		t.Run("ReusePort="+strconv.FormatBool(reusePort), func(t *testing.T) {
			testPollersAccept(t, reusePort)
		})
	}
}

func testPollersAccept(t *testing.T, reusePort bool) {
	const pollers, conns = 3, 30
	config := pollerConfig("pollers-reuseport", pollers)
	config.ReusePort = reusePort
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

// Without ReusePort the pollers still listen with SO_REUSEPORT, but an
// address another socket holds is refused as it would be without pollers,
// whether a plain listener holds it or another engine.
func TestPollersRefuseAddressInUse(t *testing.T) {
	held, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	config := pollerConfig("pollers-in-use", 2)
	config.Addr = held.Addr().String()
	if e, err := Bind(config, HandlerFuncs{}); err == nil {
		e.Close()
		t.Fatalf("bound %s, which a listener holds", config.Addr)
	} else if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("bind error = %v, want EADDRINUSE", err)
	}

	config.Addr = "127.0.0.1:0"
	first, err := Bind(config, HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	addr, err := first.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	config.Addr = addr.String()
	if e, err := Bind(config, HandlerFuncs{}); err == nil {
		e.Close()
		t.Fatalf("a second engine bound %s", config.Addr)
	} else if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("second bind error = %v, want EADDRINUSE", err)
	}
	// With ReusePort both ask to share it, and do.
	config.ReusePort = true
	config.Addr = "127.0.0.1:0"
	a, err := Bind(config, HandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	addr, _ = a.LocalAddr()
	config.Addr = addr.String()
	b, err := Bind(config, HandlerFuncs{})
	if err != nil {
		t.Fatalf("ReusePort engines could not share %s: %v", config.Addr, err)
	}
	b.Close()
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

// With ReusePort on a UDP address, each poller reads a socket of its own
// bound there, the first holding the one the engine bound, and the engine
// keeps none, since a bound UDP socket takes a share of the datagrams. Every
// datagram of a peer reaches the one poller its hash picked, so a peer opens
// one connection, and the pollers sweep their own idle peers.
func TestPollersReadUDPWithReusePort(t *testing.T) {
	const pollers, peers, rounds = 4, 32, 3
	config := pollerConfig("pollers-udp-reuseport", pollers)
	config.ReusePort = true
	config.UDPIdleTimeout = 200 * time.Millisecond
	var mu sync.Mutex
	opened := map[string]*Connection{}
	reopened := 0
	closed := make(chan error, peers)
	server, addr := startUDPServer(t, config, HandlerFuncs{
		Open: func(c *Connection) {
			mu.Lock()
			if opened[c.RemoteAddr().String()] != nil {
				reopened++
			}
			opened[c.RemoteAddr().String()] = c
			mu.Unlock()
		},
		Data: func(c *Connection, b []byte) {
			if c.engine.parent != c.Engine() {
				t.Error("a UDP peer is served off the pollers")
			}
			if err := c.Send(b); err != nil {
				c.Close()
			}
		},
		Close: func(_ *Connection, err error) { closed <- err },
	})
	if !server.pollersListen || len(server.udpListeners) != 0 {
		t.Fatalf("engine has pollersListen=%v and %d UDP sockets of its own", server.pollersListen, len(server.udpListeners))
	}
	fds := map[int]bool{}
	for i, p := range server.pollers {
		if len(p.udpListeners) != 1 {
			t.Fatalf("poller %d has %d UDP sockets, want 1", i, len(p.udpListeners))
		}
		fd := p.udpListeners[0].fd
		fds[fd] = true
		got, err := syscall.Getsockname(fd)
		if err != nil || sockaddrToUDPAddr(got).String() != addr.String() {
			t.Fatalf("poller %d is bound to %v (%v), want %v", i, sockaddrToUDPAddr(got), err, addr)
		}
	}
	if len(fds) != pollers {
		t.Fatalf("%d pollers share %d sockets", pollers, len(fds))
	}

	conns := make([]*net.UDPConn, peers)
	for i := range conns {
		conn, err := net.DialUDP("udp4", nil, addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conns[i] = conn
	}
	reply := make([]byte, 64)
	for round := 0; round < rounds; round++ {
		for i, conn := range conns {
			payload := []byte("peer " + strconv.Itoa(i) + " round " + strconv.Itoa(round))
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			if _, err := conn.Write(payload); err != nil {
				t.Fatal(err)
			}
			n, err := conn.Read(reply)
			if err != nil || !bytes.Equal(reply[:n], payload) {
				t.Fatalf("peer %d read %q, %v", i, reply[:n], err)
			}
		}
	}
	mu.Lock()
	used := map[*Engine]bool{}
	for _, c := range opened {
		used[c.engine] = true
	}
	count, again := len(opened), reopened
	mu.Unlock()
	if count != peers || again != 0 {
		t.Fatalf("%d peers opened %d connections, %d of them again", peers, count, again)
	}
	if len(used) < 2 {
		t.Fatalf("%d peers landed on %d poller(s)", peers, len(used))
	}
	for i := 0; i < peers; i++ {
		select {
		case err := <-closed:
			if !errors.Is(err, ErrUDPIdleTimeout) {
				t.Fatalf("closed with %v, want ErrUDPIdleTimeout", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d idle peers were closed", i, peers)
		}
	}
}

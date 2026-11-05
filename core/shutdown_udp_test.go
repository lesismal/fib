//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package core

import (
	"context"
	"net"
	"testing"
	"time"
)

// Shutdown waits for a UDP listener's peers as for accepted connections, and
// tells their handler, which may close them to let the shutdown finish.
func TestShutdownTellsUDPPeers(t *testing.T) {
	handler := &shutdownHandler{HandlerFuncs: HandlerFuncs{Data: func(c *Connection, b []byte) { _ = c.Send(b) }}, close: true}
	server, addr := startUDPServer(t, DefaultConfig(), handler)
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Read(make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown = %v", err)
	}
	if handler.told.Load() != 1 {
		t.Fatalf("OnShutdown ran %d times for one peer", handler.told.Load())
	}
}

// An engine already closed has nothing left open, and Shutdown says so at once.
func TestShutdownAfterClose(t *testing.T) {
	server := newOfflineServer(t)
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown after Close = %v", err)
	}
}

// Shutting one UDP binding down passes over the peers of the others, which
// go on being served, and once ctx ends, closes only its own peers.
func TestShutdownBindingLeavesOtherUDPPeers(t *testing.T) {
	echo := HandlerFuncs{Data: func(c *Connection, b []byte) { _ = c.Send(b) }}
	server, addr := startUDPServer(t, DefaultConfig(), echo)
	other, err := server.AddBinding(Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: echo})
	if err != nil {
		t.Fatal(err)
	}
	otherAddrs, err := server.BindingAddrs(other)
	if err != nil {
		t.Fatal(err)
	}
	roundTrip := func(conn *net.UDPConn, msg string) {
		t.Helper()
		if _, err := conn.Write([]byte(msg)); err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		buf := make([]byte, 16)
		n, err := conn.Read(buf)
		if err != nil || string(buf[:n]) != msg {
			t.Fatalf("echo of %q: %q, %v", msg, buf[:n], err)
		}
	}
	dial := func(to *net.UDPAddr) *net.UDPConn {
		conn, err := net.DialUDP("udp", nil, to)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	kept, ended := dial(addr), dial(otherAddrs[0].(*net.UDPAddr))
	roundTrip(kept, "before")
	roundTrip(ended, "other")
	// The other binding's peer never closes, so the wait ends with ctx.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := server.ShutdownBinding(ctx, other); err != context.DeadlineExceeded {
		t.Fatalf("ShutdownBinding = %v, want the deadline", err)
	}
	roundTrip(kept, "after")
	if err := server.ShutdownBinding(context.Background(), -1); err != errNoBinding {
		t.Fatalf("ShutdownBinding(-1) = %v, want errNoBinding", err)
	}
}

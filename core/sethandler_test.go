//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package core

import (
	"context"
	"net"
	"testing"
	"time"
)

// shutdownRecorder is a handler that takes OnShutdown, and closes the
// connection when it is told.
type shutdownRecorder struct {
	HandlerFuncs
	told chan struct{}
}

func (s *shutdownRecorder) OnShutdown(c *Connection) {
	select {
	case s.told <- struct{}{}:
	default:
	}
	c.Close()
}

// A connection handed to another handler gets every callback from it from
// then on, OnShutdown too, which the engine asks the new handler for; a nil
// handler drops what arrives.
func TestSetHandler(t *testing.T) {
	next := &shutdownRecorder{told: make(chan struct{}, 1)}
	next.Data = func(c *Connection, b []byte) {
		if string(b) == "drop" {
			c.SetHandler(nil)
			return
		}
		if c.Send(append([]byte("next:"), b...)) != nil {
			c.Close()
		}
	}
	first := HandlerFuncs{Data: func(c *Connection, b []byte) {
		c.SetHandler(next)
		next.OnData(c, b)
	}}
	config := DefaultConfig()
	config.Name = "set-handler"
	server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: first})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	defer func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	}()
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}

	exchange := func(conn net.Conn, send, want string) {
		t.Helper()
		if _, err := conn.Write([]byte(send)); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(want))
		if _, err := conn.Read(got); err != nil || string(got) != want {
			t.Fatalf("sent %q, read %q (%v), want %q", send, got, err, want)
		}
	}
	conn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	exchange(conn, "one", "next:one")
	exchange(conn, "two", "next:two")

	dropped, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer dropped.Close()
	exchange(dropped, "x", "next:x")
	if _, err := dropped.Write([]byte("drop")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := dropped.Write([]byte("lost")); err != nil {
		t.Fatal(err)
	}
	_ = dropped.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if n, err := dropped.Read(make([]byte, 16)); err == nil {
		t.Fatalf("a connection handed to nil answered %d bytes", n)
	}

	_ = dropped.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = server.Shutdown(ctx) }()
	select {
	case <-next.told:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not ask the handler the connection was handed to")
	}
}

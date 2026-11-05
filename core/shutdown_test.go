package core

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// shutdownHandler echoes, and counts the times it is told of a shutdown,
// closing the connection when close says to.
type shutdownHandler struct {
	HandlerFuncs
	told  atomic.Int32
	close bool
}

func (h *shutdownHandler) OnShutdown(c *Connection) {
	h.told.Add(1)
	if h.close {
		_ = c.Close()
	}
}

func startShutdownEngine(t *testing.T, pollers int, handler Handler) (*Engine, string, chan error) {
	t.Helper()
	config := DefaultConfig()
	config.IOPollers = pollers > 0
	config.IOPollerCount = pollers
	engine, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: handler})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := engine.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	return engine, addr.String(), done
}

func echoOnce(c net.Conn, msg string) error {
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.Write([]byte(msg)); err != nil {
		return err
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(c, buf); err != nil {
		return err
	}
	if string(buf) != msg {
		return errors.New("echoed " + string(buf))
	}
	return nil
}

func shutdownEcho() HandlerFuncs {
	return HandlerFuncs{Data: func(c *Connection, b []byte) { _ = c.Send(b) }}
}

// TestShutdownWaitsForOpenConnections checks that Shutdown refuses new
// clients at once, goes on serving the connections it has, and stops the
// engine, making Run return, only once the last of them has closed.
func TestShutdownWaitsForOpenConnections(t *testing.T) {
	for _, pollers := range []int{0, 2} {
		engine, addr, done := startShutdownEngine(t, pollers, shutdownEcho())
		client, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := echoOnce(client, "before"); err != nil {
			t.Fatalf("pollers %d: before Shutdown: %v", pollers, err)
		}
		shut := make(chan error, 1)
		go func() { shut <- engine.Shutdown(context.Background()) }()
		deadline := time.Now().Add(3 * time.Second)
		for !engine.ShuttingDown() && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if !engine.ShuttingDown() {
			t.Fatalf("pollers %d: ShuttingDown is false after Shutdown began", pollers)
		}
		if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
			_ = c.Close()
			t.Errorf("pollers %d: a connection was taken after Shutdown began", pollers)
		}
		if err := echoOnce(client, "during"); err != nil {
			t.Errorf("pollers %d: an open connection was not served during Shutdown: %v", pollers, err)
		}
		select {
		case err := <-shut:
			t.Fatalf("pollers %d: Shutdown returned %v with a connection open", pollers, err)
		case err := <-done:
			t.Fatalf("pollers %d: Run returned %v with a connection open", pollers, err)
		case <-time.After(100 * time.Millisecond):
		}
		_ = client.Close()
		select {
		case err := <-shut:
			if err != nil {
				t.Errorf("pollers %d: Shutdown: %v", pollers, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("pollers %d: Shutdown did not return once the connection closed", pollers)
		}
		if err := <-done; err != nil {
			t.Errorf("pollers %d: Run: %v", pollers, err)
		}
		if err := engine.Close(); err != nil {
			t.Errorf("pollers %d: Close: %v", pollers, err)
		}
	}
}

// TestShutdownDeadline checks that Shutdown gives up on a connection that
// stays open once its context is done, reports why, and still stops the
// engine, leaving the connection to Close.
func TestShutdownDeadline(t *testing.T) {
	engine, addr, done := startShutdownEngine(t, 0, shutdownEcho())
	client, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := echoOnce(client, "x"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := engine.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown = %v, want %v", err, context.DeadlineExceeded)
	}
	if err := <-done; err != nil {
		t.Errorf("Run: %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Errorf("Read after Close = %v, want the connection closed", err)
	}
}

// TestShutdownTellsHandler checks that a ShutdownHandler hears of the
// shutdown and that Shutdown returns as soon as it closes its connections,
// while an engine with nothing open stops at once.
func TestShutdownTellsHandler(t *testing.T) {
	for _, pollers := range []int{0, 2} {
		handler := &shutdownHandler{HandlerFuncs: shutdownEcho(), close: true}
		engine, addr, done := startShutdownEngine(t, pollers, handler)
		clients := make([]net.Conn, 4)
		for i := range clients {
			c, err := net.DialTimeout("tcp", addr, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if err := echoOnce(c, "x"); err != nil {
				t.Fatal(err)
			}
			clients[i] = c
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := engine.Shutdown(ctx); err != nil {
			t.Errorf("pollers %d: Shutdown: %v", pollers, err)
		}
		cancel()
		if told := handler.told.Load(); told < int32(len(clients)) {
			t.Errorf("pollers %d: OnShutdown ran %d times for %d connections", pollers, told, len(clients))
		}
		for _, c := range clients {
			_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
			if _, err := c.Read(make([]byte, 1)); err == nil {
				t.Errorf("pollers %d: a connection OnShutdown closed is still open", pollers)
			}
		}
		if err := <-done; err != nil {
			t.Errorf("pollers %d: Run: %v", pollers, err)
		}
		if err := engine.Close(); err != nil {
			t.Errorf("pollers %d: Close: %v", pollers, err)
		}
	}
}

// TestShutdownIgnoresDialed checks that Shutdown does not wait for the
// connections the engine dialed, which are its clients' rather than ones it
// serves.
func TestShutdownIgnoresDialed(t *testing.T) {
	server, addr, serverDone := startShutdownEngine(t, 0, shutdownEcho())
	defer func() {
		server.Stop()
		<-serverDone
		_ = server.Close()
	}()
	engine, _, done := startShutdownEngine(t, 0, shutdownEcho())
	dialed := make(chan error, 1)
	if err := engine.Dial("tcp", addr, time.Second, func(_ *Connection, err error) { dialed <- err }); err != nil {
		t.Fatal(err)
	}
	if err := <-dialed; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if err := engine.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Shutdown took %v with only a dialed connection open", elapsed)
	}
	if err := <-done; err != nil {
		t.Errorf("Run: %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

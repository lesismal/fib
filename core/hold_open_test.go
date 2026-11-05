//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package core

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// TestHoldReadsFromOpenThenHangup holds a connection's reads from its OnOpen,
// as a relay does while it dials the other side, while the peer sends and
// ends its side; releasing the hold later hands over what the peer sent and
// then the end. With release, the hold is lifted from another goroutine
// after a pause, or before (none) the peer has sent anything.
func TestHoldReadsFromOpenThenHangup(t *testing.T) {
	for _, tc := range []struct {
		name    string
		release time.Duration
		closeIn time.Duration
	}{
		{"closed during the hold", 300 * time.Millisecond, 0},
		{"closed after the hold", 50 * time.Millisecond, 300 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var got []byte
			closed := make(chan struct{})
			server, err := New(DefaultConfig(), Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{
				Open: func(c *Connection) {
					c.HoldReads(true)
					time.AfterFunc(tc.release, func() { c.HoldReads(false) })
				},
				Data: func(_ *Connection, data []byte) {
					mu.Lock()
					got = append(got, data...)
					mu.Unlock()
				},
				Close: func(*Connection, error) { close(closed) },
			}})
			if err != nil {
				t.Fatal(err)
			}
			addr, err := server.LocalAddr()
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- server.Run() }()
			t.Cleanup(func() {
				server.Stop()
				<-done
				_ = server.Close()
			})
			peer, err := net.Dial("tcp", addr.String())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := peer.Write([]byte("last words")); err != nil {
				t.Fatal(err)
			}
			time.Sleep(tc.closeIn)
			peer.Close()
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("OnClose did not run once the hold was lifted")
			}
			mu.Lock()
			defer mu.Unlock()
			if string(got) != "last words" {
				t.Fatalf("OnData got %q", got)
			}
		})
	}
}

// TestStalledRoundThenHangup: a round handed the peer's end with its read,
// as the release of a hold hands it, stops reading for the write budget,
// its echo to a peer that does not read yet past the watermark, and is not
// held by the handler. What the peer sent is still in the socket, behind
// the end: the round must leave the end owed rather than close, so that
// every byte reaches OnData once the peer reads its echo.
func TestStalledRoundThenHangup(t *testing.T) {
	const size = 64 << 10
	var mu sync.Mutex
	got := 0
	closed := make(chan struct{})
	cfg := DefaultConfig()
	cfg.WriteBufferHighWatermark = 16 << 10
	cfg.ReadBufferSize = 4 << 10
	server, err := New(cfg, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{
		Open: func(c *Connection) {
			_ = c.SetWriteBuffer(4 << 10)
			c.HoldReads(true)
			time.AfterFunc(300*time.Millisecond, func() { c.HoldReads(false) })
		},
		Data: func(c *Connection, data []byte) {
			mu.Lock()
			got += len(data)
			mu.Unlock()
			_ = c.Send(append([]byte(nil), data...))
		},
		Close: func(*Connection, error) { close(closed) },
	}})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		<-done
		_ = server.Close()
	})
	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	peer := conn.(*net.TCPConn)
	_ = peer.SetReadBuffer(4 << 10)
	// All of it, and the end, wait in the socket while the hold lasts.
	if _, err := peer.Write(make([]byte, size)); err != nil {
		t.Fatal(err)
	}
	if err := peer.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	// The peer reads its echo only once the round after the release has
	// stopped for it.
	time.Sleep(600 * time.Millisecond)
	go func() { _, _ = io.Copy(io.Discard, peer) }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("OnClose did not run")
	}
	mu.Lock()
	defer mu.Unlock()
	if got != size {
		t.Fatalf("OnData got %d bytes of %d before the end", got, size)
	}
}

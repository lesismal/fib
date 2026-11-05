//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package core

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// TestMaxConns checks, with and without pollers, that a connection accepted
// while MaxConns are open is closed before its handler hears of it, and
// counted in Stats, and that one accepted once another has closed is served.
func TestMaxConns(t *testing.T) {
	for _, pollers := range []int{0, 2} {
		t.Run(fmt.Sprintf("pollers=%d", pollers), func(t *testing.T) {
			config := DefaultConfig()
			config.MaxConns = 2
			config.IOPollers = pollers > 0
			config.IOPollerCount = pollers
			opened := make(chan struct{}, 8)
			server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{
				Open: func(*Connection) { opened <- struct{}{} },
				Data: func(c *Connection, data []byte) { _ = c.Send(data) },
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
			dial := func() net.Conn {
				conn, err := net.Dial("tcp", addr.String())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { conn.Close() })
				return conn
			}
			echoes := func(conn net.Conn) bool {
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				if _, err := conn.Write([]byte("ping")); err != nil {
					return false
				}
				buf := make([]byte, 4)
				_, err := io.ReadFull(conn, buf)
				return err == nil && string(buf) == "ping"
			}
			first, second := dial(), dial()
			if !echoes(first) || !echoes(second) {
				t.Fatal("a connection under the cap was not served")
			}
			third := dial()
			if echoes(third) {
				t.Fatal("a connection over the cap was served")
			}
			if s := server.Stats(); s.OpenConns != 2 || s.ConnsRefused != 1 {
				t.Fatalf("stats %+v, want 2 open and 1 refused", s)
			}
			if n := len(opened); n != 2 {
				t.Fatalf("%d connections opened, want 2", n)
			}
			first.Close()
			deadline := time.Now().Add(5 * time.Second)
			for server.Stats().OpenConns != 1 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if !echoes(dial()) {
				t.Fatal("a connection accepted after one closed was not served")
			}
		})
	}
}

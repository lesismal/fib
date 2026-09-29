//go:build linux || darwin || windows

package fib

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// Large echoes over TCP and Unix sockets, which queue replies, write them in
// pieces and read in several calls, come back intact whichever set of system
// calls Config.SocketSyscalls picks, and with writev on or off.
func TestSocketSyscallsEcho(t *testing.T) {
	for _, network := range []string{"tcp", "unix"} {
		for _, mode := range []struct {
			name                      string
			useWritev, socketSyscalls bool
		}{{"writev", true, false}, {"write", false, false}, {"sendmsg", true, true}, {"sendto", false, true}} {
			t.Run(network+"/"+mode.name, func(t *testing.T) {
				config := DefaultConfig()
				config.Network = network
				config.Addr = "127.0.0.1:0"
				if network == "unix" {
					config.Addr = unixSocketPath(t)
				}
				config.UseWritev = mode.useWritev
				config.SocketSyscalls = mode.socketSyscalls
				server, err := Bind(config, HandlerFuncs{Data: func(c *Connection, b []byte) {
					if c.Send(b) != nil {
						c.Close()
					}
				}})
				if err != nil {
					t.Fatal(err)
				}
				runDone := make(chan error, 1)
				go func() { runDone <- server.Run() }()
				t.Cleanup(func() {
					server.Stop()
					if err := <-runDone; err != nil {
						t.Error(err)
					}
					_ = server.Close()
				})
				addrs, err := server.ListenAddrs()
				if err != nil {
					t.Fatal(err)
				}

				var wg sync.WaitGroup
				for i := 0; i < 4; i++ {
					wg.Add(1)
					go func(value byte) {
						defer wg.Done()
						conn, err := net.DialTimeout(network, addrs[0].String(), 5*time.Second)
						if err != nil {
							t.Error(err)
							return
						}
						defer conn.Close()
						_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
						payload := bytes.Repeat([]byte{value}, 1<<20)
						go func() { _, _ = conn.Write(payload) }()
						received := make([]byte, len(payload))
						if _, err := io.ReadFull(conn, received); err != nil {
							t.Error(err)
							return
						}
						if !bytes.Equal(received, payload) {
							t.Error("echo mismatch")
						}
					}(byte(i + 1))
				}
				wg.Wait()
			})
		}
	}
}

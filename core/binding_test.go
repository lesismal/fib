//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package core

import (
	"net"
	"strings"
	"testing"
	"time"
)

// prefixEcho answers every message with prefix and the message, so a client
// can tell which binding's handler served it.
func prefixEcho(prefix string) Handler {
	return HandlerFuncs{
		Data: func(c *Connection, b []byte) {
			if c.Send(append([]byte(prefix), b...)) != nil {
				c.Close()
			}
		},
	}
}

// runEngine runs e until the test ends.
func runEngine(t *testing.T, e *Engine) {
	t.Helper()
	runDone := make(chan error, 1)
	go func() { runDone <- e.Run() }()
	t.Cleanup(func() {
		e.Stop()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		_ = e.Close()
	})
}

// exchange sends message over addr and returns the reply.
func exchange(t *testing.T, addr net.Addr, message string) string {
	t.Helper()
	conn, err := net.DialTimeout(addr.Network(), addr.String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte(message)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 256)
	got := ""
	for {
		n, err := conn.Read(buf)
		got += string(buf[:n])
		if err != nil || addr.Network() == "udp" || strings.HasSuffix(got, message) {
			if err != nil {
				t.Fatalf("reading from %s: %v (have %q)", addr, err, got)
			}
			return got
		}
	}
}

// bindingConfigs are the engine shapes the bindings are tested under: one
// loop, and on the readiness backends pollers, with and without ReusePort,
// which has the pollers accept TCP connections, and read UDP datagrams,
// themselves.
func bindingConfigs() map[string]Config {
	single := DefaultConfig()
	single.IOPollers = false
	pollers := DefaultConfig()
	pollers.IOPollers, pollers.IOPollerCount = true, 3
	reusePort := pollers
	reusePort.ReusePort = true
	return map[string]Config{"single-loop": single, "pollers": pollers, "pollers-reuseport": reusePort}
}

// One engine serves every binding, each with its own handler: a lone TCP
// address, a group of TCP addresses, a Unix socket and a UDP address.
func TestBindingsServeEachWithItsOwnHandler(t *testing.T) {
	for name, config := range bindingConfigs() {
		t.Run(name, func(t *testing.T) {
			config.Name = "bindings-" + name
			unixPath := unixSocketPath(t)
			server, err := New(config,
				Binding{Addr: "127.0.0.1:0", Handler: prefixEcho("a:")},
				Binding{Network: "tcp4", Addrs: []string{"127.0.0.1:0", "127.0.0.1:0"}, Handler: prefixEcho("b:")},
				Binding{Network: "unix", Addr: unixPath, Handler: prefixEcho("x:")},
				Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: prefixEcho("u:")},
			)
			if err != nil {
				t.Fatal(err)
			}
			runEngine(t, server)

			want := []struct {
				network, prefix string
				addrs           int
			}{{"tcp", "a:", 1}, {"tcp", "b:", 2}, {"unix", "x:", 1}, {"udp", "u:", 1}}
			total := 0
			for i, w := range want {
				addrs, err := server.BindingAddrs(i)
				if err != nil {
					t.Fatalf("BindingAddrs(%d): %v", i, err)
				}
				if len(addrs) != w.addrs {
					t.Fatalf("BindingAddrs(%d) = %v, want %d addresses", i, addrs, w.addrs)
				}
				total += len(addrs)
				for _, addr := range addrs {
					if addr.Network() != w.network {
						t.Fatalf("binding %d listens on %s %s, want %s", i, addr.Network(), addr, w.network)
					}
					// Several rounds, so that with pollers the connections
					// land on more than one of them.
					for round := 0; round < 4; round++ {
						if got := exchange(t, addr, "ping"); got != w.prefix+"ping" {
							t.Fatalf("binding %d at %s answered %q, want %q", i, addr, got, w.prefix+"ping")
						}
					}
				}
			}
			if addrs, err := server.BindingAddrs(len(want)); err == nil {
				t.Fatalf("BindingAddrs past the last binding = %v", addrs)
			}
			if _, err := server.BindingAddrs(-1); err == nil {
				t.Fatal("BindingAddrs(-1) did not fail")
			}
			if addrs, err := server.ListenAddrs(); err != nil || len(addrs) != total {
				t.Fatalf("ListenAddrs = %v, %v, want %d addresses", addrs, err, total)
			}
			// The Unix socket is left out of the TCP addresses, rather than
			// failing them.
			if addrs, err := server.LocalAddrs(); err != nil || len(addrs) != 3 {
				t.Fatalf("LocalAddrs = %v, %v, want the 3 TCP addresses", addrs, err)
			}
			if addrs, err := server.LocalUDPAddrs(); err != nil || len(addrs) != 1 {
				t.Fatalf("LocalUDPAddrs = %v, %v, want 1", addrs, err)
			}
		})
	}
}

// An engine given one binding has that binding alone, and a binding that
// names no network takes Config.Network.
func TestBindingDefaults(t *testing.T) {
	config := DefaultConfig()
	config.Name = "binding-defaults"
	server, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: prefixEcho("x:")})
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, server)
	addrs, err := server.BindingAddrs(0)
	if err != nil || len(addrs) != 1 {
		t.Fatalf("BindingAddrs(0) = %v, %v", addrs, err)
	}
	if got := exchange(t, addrs[0], "hi"); got != "x:hi" {
		t.Fatalf("answered %q", got)
	}
	if _, err := server.BindingAddrs(1); err == nil {
		t.Fatal("an engine given one binding has a second")
	}

	udp := DefaultConfig()
	udp.Name = "binding-defaults-udp"
	udp.Network = "udp"
	server, err = New(udp, Binding{Addr: "127.0.0.1:0", Handler: prefixEcho("y:")})
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, server)
	addrs, err = server.BindingAddrs(0)
	if err != nil || len(addrs) != 1 || addrs[0].Network() != "udp" {
		t.Fatalf("BindingAddrs(0) = %v, %v, want one UDP address", addrs, err)
	}
	if got := exchange(t, addrs[0], "hi"); got != "y:hi" {
		t.Fatalf("answered %q", got)
	}
}

// A binding that cannot listen fails New, and gives up what the bindings
// before it had opened.
func TestBindingFailureReleasesTheRest(t *testing.T) {
	busy, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	first, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	free := first.Addr().String()
	_ = first.Close()

	for name, config := range bindingConfigs() {
		t.Run(name, func(t *testing.T) {
			config.Name = "binding-failure-" + name
			if server, err := New(config,
				Binding{Network: "tcp4", Addr: free, Handler: HandlerFuncs{}},
				Binding{Network: "udp4", Addr: "127.0.0.1:0", Handler: HandlerFuncs{}},
				Binding{Network: "tcp4", Addr: busy.Addr().String(), Handler: HandlerFuncs{}},
			); err == nil {
				_ = server.Close()
				t.Fatal("New took an address in use")
			}
			l, err := net.Listen("tcp4", free)
			if err != nil {
				t.Fatalf("the first binding's address was kept: %v", err)
			}
			_ = l.Close()
		})
	}
}

// New keeps no hold on the caller's bindings.
func TestBindingsAreCopied(t *testing.T) {
	config := DefaultConfig()
	config.Name = "bindings-copied"
	addrs := []string{"127.0.0.1:0"}
	bindings := []Binding{{Addrs: addrs, Handler: prefixEcho("c:")}}
	server, err := New(config, bindings...)
	if err != nil {
		t.Fatal(err)
	}
	runEngine(t, server)
	addrs[0] = "changed"
	bindings[0].Handler = prefixEcho("changed:")
	bound, err := server.BindingAddrs(0)
	if err != nil {
		t.Fatal(err)
	}
	if got := exchange(t, bound[0], "hi"); got != "c:hi" {
		t.Fatalf("answered %q", got)
	}
}

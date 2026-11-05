//go:build linux

package core

import (
	"testing"
)

// Under pollers each binding's addresses are served as one address on its
// own network would be: the pollers accept TCP connections each on a socket
// of its own, all wait on the one Unix socket, and read UDP datagrams each
// on a socket of its own with ReusePort and on the first poller's alone
// without it, each with the handler of the binding the socket belongs to.
func TestBindingsUnderPollers(t *testing.T) {
	for _, reusePort := range []bool{false, true} {
		config := pollerConfig("bindings-pollers", 3)
		config.ReusePort = reusePort
		server, err := New(config,
			Binding{Addr: "127.0.0.1:0", Handler: prefixEcho("t:")},
			Binding{Network: "unix", Addr: unixSocketPath(t), Handler: prefixEcho("x:")},
			Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: prefixEcho("u:")},
		)
		if err != nil {
			t.Fatal(err)
		}
		runEngine(t, server)
		if !server.pollersListen || server.udpSpread != reusePort || server.udpHanded == reusePort {
			t.Fatalf("ReusePort=%v: pollersListen=%v udpSpread=%v udpHanded=%v",
				reusePort, server.pollersListen, server.udpSpread, server.udpHanded)
		}
		if len(server.listeners) != 2 || !server.listeners[0].spread || !server.listeners[1].shared {
			t.Fatalf("ReusePort=%v: the engine's listeners are %+v", reusePort, server.listeners)
		}
		for i, p := range server.pollers {
			if len(p.listeners) != 2 || p.listeners[0].binding != 0 || p.listeners[1].fd != server.listeners[1].fd {
				t.Fatalf("poller %d listens on %+v, want a TCP socket of its own and the engine's Unix one", i, p.listeners)
			}
			wantUDP := 0
			if reusePort || i == 0 {
				wantUDP = 1
			}
			if len(p.udpListeners) != wantUDP {
				t.Fatalf("poller %d reads %d UDP sockets, want %d", i, len(p.udpListeners), wantUDP)
			}
			for _, l := range p.udpListeners {
				if l.binding != 2 {
					t.Fatalf("poller %d's UDP socket serves binding %d", i, l.binding)
				}
			}
		}
		for i, prefix := range []string{"t:", "x:", "u:"} {
			addrs, err := server.BindingAddrs(i)
			if err != nil || len(addrs) != 1 {
				t.Fatalf("BindingAddrs(%d) = %v, %v", i, addrs, err)
			}
			for round := 0; round < 6; round++ {
				if got := exchange(t, addrs[0], "ping"); got != prefix+"ping" {
					t.Fatalf("binding %d answered %q", i, got)
				}
			}
		}
	}
}

//go:build linux && (amd64 || arm64)

package core

import (
	"fmt"
	"net"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/lesismal/fib/internal/reuseportbpf"
)

// pipeBalancer is a balancer holding a pipe's descriptors and a duplicate
// in place of its maps and program.
func pipeBalancer(t *testing.T) *reusePortBalancer {
	t.Helper()
	var p [2]int
	if err := syscall.Pipe(p[:]); err != nil {
		t.Fatal(err)
	}
	third, err := syscall.Dup(p[0])
	if err != nil {
		t.Fatal(err)
	}
	b := &reusePortBalancer{Sockarray: p[0], Counter: p[1], Prog: third}
	t.Cleanup(func() { b.Close() })
	return b
}

// Closing a binding closes the selectors that balance its groups, and only
// those; closing every binding closes the rest.
func TestBindingClosesItsBalancers(t *testing.T) {
	server := newOfflineServer(t)
	mine := pipeBalancer(t)
	other := pipeBalancer(t)
	server.listenMu.Lock()
	server.balancers = append(server.balancers, bindingBalancer{binding: 0, b: mine},
		bindingBalancer{binding: 1, b: other})
	server.listenMu.Unlock()
	server.closeBalancers(0)
	if mine.Prog != -1 || other.Prog == -1 {
		t.Fatalf("closing binding 0 left %+v and %+v", *mine, *other)
	}
	server.closeBalancers(-1)
	if other.Prog != -1 || len(server.balancers) != 0 {
		t.Fatalf("closing every binding left %+v, %d balancers", *other, len(server.balancers))
	}
}

// An address AddBinding spreads over the pollers is balanced as one New was
// given is: where bpf(2) is refused its selector falls back to the kernel's
// hash, and the binding serves either way. In a prefork child the group is
// shared through the master's broker, which falls back the same way.
func TestAddBindingBalancesReusePort(t *testing.T) {
	for _, prefork := range []bool{false, true} {
		t.Run(fmt.Sprintf("prefork=%v", prefork), func(t *testing.T) {
			if prefork {
				brokerChild(t, reuseportbpf.NewBroker(), 1, 2)
			}
			config := pollerConfig("balance-add", 2)
			config.ReusePort = true
			config.BalanceReusePort = true
			server, _ := startEchoServer(t, config, echoHandler())
			addrs := []string{"127.0.0.1:0"}
			if ln, err := net.Listen("tcp6", "[::1]:0"); err == nil {
				ln.Close()
				addrs = append(addrs, "[::1]:0")
			}
			i, err := server.AddBinding(Binding{Addrs: addrs, Handler: echoHandler()})
			if err != nil {
				t.Fatal(err)
			}
			bound, err := server.BindingAddrs(i)
			if err != nil || len(bound) != len(addrs) {
				t.Fatalf("BindingAddrs = %v, %v", bound, err)
			}
			for _, a := range bound {
				c, err := net.DialTimeout("tcp", a.String(), 5*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				err = echoOnce(c, "ping")
				c.Close()
				if err != nil {
					t.Fatalf("echo from %s: %v", a, err)
				}
			}
			if err := server.RemoveBinding(i); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// The selectors New attaches leave out a Unix listener, which the pollers
// share rather than each opening one of their own.
func TestBalanceReusePortPassesOverUnixListeners(t *testing.T) {
	config := pollerConfig("balance-unix", 2)
	config.ReusePort = true
	config.BalanceReusePort = true
	path := filepath.Join(t.TempDir(), "s")
	server, err := New(config, Binding{Network: "unix", Addr: path, Handler: echoHandler()},
		Binding{Addr: "127.0.0.1:0", Handler: echoHandler()})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if addrs, err := server.ListenAddrs(); err != nil || len(addrs) != 2 {
		t.Fatalf("ListenAddrs = %v, %v", addrs, err)
	}
}

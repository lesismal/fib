//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"testing"
)

// A binding that asks for Multipath TCP listens and serves wherever it runs:
// on Linux with Multipath TCP in the kernel the connection a plain client
// makes falls back to plain TCP, as the kernel does, and on a platform
// without Multipath TCP the option is one the binding does not have, so it
// listens plain TCP. What the option buys where it applies is
// binding_mptcp_linux_test.go's.
func TestBindingMultipathTCPServesAPlainClient(t *testing.T) {
	for name, config := range bindingConfigs() {
		t.Run(name, func(t *testing.T) {
			config.Name = "mptcp-plain-" + name
			engine, err := New(config, Binding{Addr: "127.0.0.1:0", MultipathTCP: true, Handler: prefixEcho("m:")})
			if err != nil {
				t.Fatal(err)
			}
			runEngine(t, engine)
			addrs, err := engine.BindingAddrs(0)
			if err != nil || len(addrs) != 1 {
				t.Fatalf("BindingAddrs(0) = %v, %v, want 1 address", addrs, err)
			}
			if got := exchange(t, addrs[0], "ping"); got != "m:ping" {
				t.Fatalf("%s answered %q, want %q", addrs[0], got, "m:ping")
			}
		})
	}
}

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// dialClient starts an engine to dial from, with pollers or without, so that
// a dial's socket is opened off the loop or on it.
func dialClient(t *testing.T, pollers bool) *Engine {
	t.Helper()
	config := DefaultConfig()
	config.IOPollers = pollers
	client, _ := startEchoServer(t, config, HandlerFuncs{})
	return client
}

// dialWith runs a DialWith and waits for its outcome.
func dialWith(t *testing.T, e *Engine, opts DialOptions) dialResult {
	t.Helper()
	results := make(chan dialResult, 1)
	if err := e.DialWith(opts, func(c *Connection, err error) { results <- dialResult{c, err} }); err != nil {
		t.Fatalf("DialWith: %v", err)
	}
	select {
	case r := <-results:
		return r
	case <-time.After(10 * time.Second):
		t.Fatal("dial never reported")
		return dialResult{}
	}
}

// heldPorts is a run of consecutive loopback ports on network, some of them
// held by sockets of the test's own.
type heldPorts struct {
	first   int
	holders []any
}

func (h *heldPorts) release(t *testing.T, i int) {
	t.Helper()
	switch held := h.holders[i].(type) {
	case net.Listener:
		held.Close()
	case net.PacketConn:
		held.Close()
	}
	h.holders[i] = nil
}

func (h *heldPorts) close() {
	for _, held := range h.holders {
		switch held := held.(type) {
		case net.Listener:
			held.Close()
		case net.PacketConn:
			held.Close()
		}
	}
}

// holdPorts finds n consecutive free ports on 127.0.0.1 and holds them all.
// They are taken below every system's ephemeral range, which starts at 32768
// at the lowest, so that no other test's connection lingers on them, and the
// holders are bound as a dial binds, without SO_REUSEADDR, so that a port
// one of these tests left in TIME_WAIT is passed over.
func holdPorts(t *testing.T, network string, n int) *heldPorts {
	t.Helper()
	probe := net.ListenConfig{Control: probeControl}
	for attempt := 0; attempt < 200; attempt++ {
		h := &heldPorts{first: 20000 + rand.IntN(12000)}
		for i := 0; i < n; i++ {
			addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(h.first+i))
			var held any
			var err error
			if network == "udp" {
				held, err = probe.ListenPacket(context.Background(), "udp4", addr)
			} else {
				held, err = probe.Listen(context.Background(), "tcp4", addr)
			}
			if err != nil {
				break
			}
			h.holders = append(h.holders, held)
		}
		if len(h.holders) == n {
			t.Cleanup(h.close)
			return h
		}
		h.close()
	}
	t.Fatal("no run of free ports")
	return nil
}

// freePort is a loopback port nobody holds just now.
func freePort(t *testing.T, network string) uint16 {
	t.Helper()
	h := holdPorts(t, network, 1)
	h.release(t, 0)
	return uint16(h.first)
}

// The peer of a dial bound to a fixed local address and port sees it as the
// dial's source, over TCP and UDP, from a socket opened off the loop or on it.
func TestDialWithFixedLocalPort(t *testing.T) {
	for _, pollers := range []bool{true, false} {
		client := dialClient(t, pollers)
		t.Run("tcp", func(t *testing.T) {
			ln, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			port := freePort(t, "tcp")
			local := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), port)
			r := dialWith(t, client, DialOptions{Network: "tcp", Addr: ln.Addr().String(), Timeout: 5 * time.Second, LocalAddr: local})
			if r.err != nil {
				t.Fatal(r.err)
			}
			defer r.c.Close()
			peer, err := ln.Accept()
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			if got := peer.RemoteAddr().String(); got != local.String() {
				t.Fatalf("peer sees %s, want %s", got, local)
			}
			if got := r.c.LocalAddr().String(); got != local.String() {
				t.Fatalf("LocalAddr = %s, want %s", got, local)
			}
		})
		t.Run("udp", func(t *testing.T) {
			peer, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			port := freePort(t, "udp")
			// A port without an address leaves the interface to the route.
			r := dialWith(t, client, DialOptions{Network: "udp", Addr: peer.LocalAddr().String(),
				LocalAddr: netip.AddrPortFrom(netip.Addr{}, port)})
			if r.err != nil {
				t.Fatal(r.err)
			}
			defer r.c.Close()
			if err := r.c.Send([]byte("hi")); err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, 16)
			_ = peer.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, from, err := peer.ReadFrom(buf)
			if err != nil {
				t.Fatal(err)
			}
			if got := from.(*net.UDPAddr).Port; got != int(port) {
				t.Fatalf("peer sees port %d, want %d", got, port)
			}
		})
	}
}

// A range with ports in use lands on the one that is free, whichever port the
// search starts from.
func TestDialWithPortRangeSkipsTakenPorts(t *testing.T) {
	client := dialClient(t, true)
	t.Run("udp", func(t *testing.T) {
		peer, err := net.ListenPacket("udp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer peer.Close()
		held := holdPorts(t, "udp", 4)
		held.release(t, 2)
		want := held.first + 2
		for i := 0; i < 8; i++ {
			r := dialWith(t, client, DialOptions{Network: "udp4", Addr: peer.LocalAddr().String(),
				PortMin: uint16(held.first), PortMax: uint16(held.first + 3)})
			if r.err != nil {
				t.Fatal(r.err)
			}
			got := r.c.LocalAddr().(*net.UDPAddr).Port
			r.c.Close()
			if got != want {
				t.Fatalf("dial %d bound port %d, want %d", i, got, want)
			}
			// The socket is closed on the loop; wait for its port.
			waitPortFree(t, "udp", want)
		}
	})
	t.Run("tcp", func(t *testing.T) {
		ln, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		held := holdPorts(t, "tcp", 4)
		held.release(t, 1)
		want := held.first + 1
		r := dialWith(t, client, DialOptions{Addr: ln.Addr().String(), Timeout: 5 * time.Second,
			LocalAddr: netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), 0),
			PortMin:   uint16(held.first), PortMax: uint16(held.first + 3)})
		if r.err != nil {
			t.Fatal(r.err)
		}
		defer r.c.Close()
		peer, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		defer peer.Close()
		if got := peer.RemoteAddr().(*net.TCPAddr).Port; got != want {
			t.Fatalf("peer sees port %d, want %d", got, want)
		}
	})
}

func waitPortFree(t *testing.T, network string, port int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		pc, err := net.ListenPacket(network+"4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err == nil {
			pc.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("port %d never freed: %v", port, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A range with every port in use fails the dial through done, with an error
// that says so, on either kind of loop.
func TestDialWithPortRangeExhausted(t *testing.T) {
	for _, pollers := range []bool{true, false} {
		client := dialClient(t, pollers)
		for _, network := range []string{"tcp", "udp"} {
			held := holdPorts(t, network, 3)
			target := "127.0.0.1:9"
			r := dialWith(t, client, DialOptions{Network: network, Addr: target, Timeout: time.Second,
				LocalAddr: netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), 0),
				PortMin:   uint16(held.first), PortMax: uint16(held.first + 2)})
			if r.c != nil || !errors.Is(r.err, ErrNoLocalPort) || !errors.Is(r.err, syscall.EADDRINUSE) {
				t.Fatalf("%s pollers=%v: dial from a full range: %v, %v", network, pollers, r.c, r.err)
			}
			var opErr *net.OpError
			if !errors.As(r.err, &opErr) || opErr.Source == nil || !strings.HasPrefix(opErr.Source.String(), "127.0.0.1:") {
				t.Fatalf("%s: error does not name the local end: %#v", network, r.err)
			}
			held.close()
		}
	}
}

// A fixed local port in use fails the dial with EADDRINUSE from bind.
func TestDialWithFixedPortInUse(t *testing.T) {
	client := dialClient(t, true)
	held := holdPorts(t, "udp", 1)
	r := dialWith(t, client, DialOptions{Network: "udp", Addr: "127.0.0.1:9",
		LocalAddr: netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(held.first))})
	var sysErr *os.SyscallError
	if r.c != nil || !errors.Is(r.err, syscall.EADDRINUSE) || !errors.As(r.err, &sysErr) || sysErr.Syscall != "bind" {
		t.Fatalf("dial from a port in use: %v, %v", r.c, r.err)
	}
}

// Options that cannot work fail DialWith itself; done never runs.
func TestDialWithRejectsBadOptions(t *testing.T) {
	client := dialClient(t, true)
	never := func(*Connection, error) { t.Error("done called for a dial that could not start") }
	v4 := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), 0)
	v6 := netip.AddrPortFrom(netip.MustParseAddr("::1"), 0)
	for _, tc := range []struct {
		name string
		opts DialOptions
		want string
	}{
		{"v4 local, v6 literal", DialOptions{Addr: "[::1]:80", LocalAddr: v4}, "different families"},
		{"v6 local, v4 literal", DialOptions{Network: "udp", Addr: "127.0.0.1:80", LocalAddr: v6}, "different families"},
		{"v4 local on tcp6", DialOptions{Network: "tcp6", Addr: "[::1]:80", LocalAddr: v4}, "not IPv6"},
		{"v6 local on udp4", DialOptions{Network: "udp4", Addr: "127.0.0.1:80", LocalAddr: v6}, "not IPv4"},
		{"unix", DialOptions{Network: "unix", Addr: "/tmp/x.sock", LocalAddr: v4}, "unix"},
		{"empty range", DialOptions{Addr: "127.0.0.1:80", PortMin: 5000, PortMax: 4000}, "empty"},
		{"range from 0", DialOptions{Addr: "127.0.0.1:80", PortMax: 4000}, "empty"},
		{"port and range", DialOptions{Addr: "127.0.0.1:80", LocalAddr: netip.AddrPortFrom(v4.Addr(), 1),
			PortMin: 4000, PortMax: 4001}, "both set"},
		{"unknown network", DialOptions{Network: "sctp", Addr: "127.0.0.1:80"}, "unknown network"},
	} {
		err := client.DialWith(tc.opts, never)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want an error with %q", tc.name, err, tc.want)
		}
	}
}

// A name with a local address is resolved to that address's family, and a
// mapped IPv4 address counts as IPv4.
func TestDialWithResolvesToLocalFamily(t *testing.T) {
	client := dialClient(t, true)
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	r := dialWith(t, client, DialOptions{Addr: net.JoinHostPort("localhost", port), Timeout: 5 * time.Second,
		LocalAddr: netip.AddrPortFrom(netip.MustParseAddr("::ffff:127.0.0.1"), 0)})
	if r.err != nil {
		t.Fatal(r.err)
	}
	r.c.Close()
	peer, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	peer.Close()

	// A name failing resolution, or a zone that names no interface, fails
	// through done.
	if r = dialWith(t, client, DialOptions{Addr: "no-such-host.invalid:80", LocalAddr: netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), 0)}); r.err == nil {
		t.Fatal("dial to an unresolvable name succeeded")
	}
	r = dialWith(t, client, DialOptions{Network: "udp6", Addr: "[::1]:9",
		LocalAddr: netip.AddrPortFrom(netip.MustParseAddr("fe80::1%no-such-if"), 0)})
	if r.err == nil {
		t.Fatal("dial from an unknown zone succeeded")
	}
	// A zone that names an interface is bound with its index; whether the
	// kernel takes it on the loopback address is its own affair.
	ifaces, _ := net.Interfaces()
	for _, ifi := range ifaces {
		if ifi.Flags&net.FlagLoopback != 0 {
			r = dialWith(t, client, DialOptions{Network: "udp6", Addr: "[::1]:9",
				LocalAddr: netip.AddrPortFrom(netip.IPv6Loopback().WithZone(ifi.Name), 0)})
			if r.c != nil {
				r.c.Close()
			}
			break
		}
	}
}

// A dial over IPv6 binds its local end too, where the host has IPv6.
func TestDialWithIPv6Local(t *testing.T) {
	ln, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	defer ln.Close()
	client := dialClient(t, false)
	r := dialWith(t, client, DialOptions{Addr: ln.Addr().String(), Timeout: 5 * time.Second,
		LocalAddr: netip.AddrPortFrom(netip.IPv6Unspecified(), 0), PortMin: 41000, PortMax: 41999})
	if r.err != nil {
		t.Fatal(r.err)
	}
	defer r.c.Close()
	peer, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if got := peer.RemoteAddr().(*net.TCPAddr).Port; got < 41000 || got > 41999 {
		t.Fatalf("peer sees port %d, want 41000-41999", got)
	}
}

// A dial bound to a local address still times out, naming that address.
func TestDialWithTimeout(t *testing.T) {
	client := dialClient(t, true)
	port := freePort(t, "tcp")
	start := time.Now()
	r := dialWith(t, client, DialOptions{Network: "tcp4", Addr: blackholeAddr, Timeout: 200 * time.Millisecond,
		LocalAddr: netip.AddrPortFrom(netip.Addr{}, port)})
	if !errors.Is(r.err, os.ErrDeadlineExceeded) {
		t.Skipf("connect to %s did not hang: %v", blackholeAddr, r.err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("timeout took %v", elapsed)
	}
	var opErr *net.OpError
	if !errors.As(r.err, &opErr) || opErr.Source == nil || !strings.HasSuffix(opErr.Source.String(), ":"+strconv.Itoa(int(port))) {
		t.Fatalf("timeout does not name the local end: %#v", r.err)
	}
}

// A name is resolved to the local address's family only where the network
// leaves the family open.
func TestDialOptionsResolveNetwork(t *testing.T) {
	v6 := DialOptions{LocalAddr: netip.AddrPortFrom(netip.IPv6Loopback(), 0)}
	v4 := DialOptions{LocalAddr: netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), 0)}
	for _, tc := range []struct {
		opts             DialOptions
		network, resolve string
	}{
		{DialOptions{}, "tcp", "tcp"},
		{v4, "tcp", "tcp4"},
		{v6, "udp", "udp6"},
		{v6, "tcp6", "tcp6"},
		{v4, "udp4", "udp4"},
	} {
		if got := tc.opts.resolveNetwork(tc.network); got != tc.resolve {
			t.Errorf("%v on %s resolves on %s, want %s", tc.opts.LocalAddr, tc.network, got, tc.resolve)
		}
	}
}

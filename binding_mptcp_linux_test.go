//go:build linux

package fib

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// requireMultipathTCP skips the test where the kernel cannot open a Multipath
// TCP socket, either because it has no Multipath TCP or because
// net.mptcp.enabled turns it off, as net does before it listens with it.
func requireMultipathTCP(t *testing.T) {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM|syscall.SOCK_CLOEXEC, ipprotoMPTCP)
	if err != nil {
		switch {
		case errors.Is(err, syscall.EPROTONOSUPPORT), errors.Is(err, syscall.EINVAL),
			errors.Is(err, syscall.EOPNOTSUPP), errors.Is(err, syscall.ENOPROTOOPT):
			t.Skipf("kernel has no Multipath TCP: %v", err)
		}
		t.Skipf("kernel Multipath TCP socket failed: %v", err)
	}
	syscall.Close(fd)
}

// dialMultipathTCP dials addr with a Multipath TCP client, and reports
// whether the connection came up as a Multipath TCP one. The connection is
// closed when the test ends.
func dialMultipathTCP(t *testing.T, addr net.Addr) (*net.TCPConn, bool) {
	t.Helper()
	var dialer net.Dialer
	dialer.SetMultipathTCP(true)
	conn, err := dialer.Dial(addr.Network(), addr.String())
	if err != nil {
		t.Fatalf("dialing %s with Multipath TCP: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatalf("dialing %s gave a %T, want *net.TCPConn", addr, conn)
	}
	using, err := tcp.MultipathTCP()
	if err != nil {
		t.Fatalf("MultipathTCP() of the connection to %s: %v", addr, err)
	}
	return tcp, using
}

// expectMultipathTCPEcho checks that addr answers message over conn with
// prefix.
func expectMultipathTCPEcho(t *testing.T, conn *net.TCPConn, addr net.Addr, prefix, message string) {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte(message)); err != nil {
		t.Fatalf("sending %q to %s: %v", message, addr, err)
	}
	buf := make([]byte, 256)
	got := ""
	for {
		n, err := conn.Read(buf)
		got += string(buf[:n])
		if err != nil {
			t.Fatalf("reading the answer from %s: %v (have %q)", addr, err, got)
		}
		if strings.HasSuffix(got, message) {
			if got != prefix+message {
				t.Fatalf("%s answered %q, want %q", addr, got, prefix+message)
			}
			return
		}
	}
}

// A binding that asks for Multipath TCP listens on one under every engine
// shape, and a client that dials it with Multipath TCP gets an end-to-end
// Multipath TCP connection and an answer: the client's MultipathTCP reporting
// true proves the exchange went over Multipath TCP on both sides.
func TestMultipathTCPListener(t *testing.T) {
	requireMultipathTCP(t)
	for name, config := range bindingConfigs() {
		t.Run(name, func(t *testing.T) {
			config.Name = "mptcp-" + name
			engine, err := New(config, Binding{Addr: "127.0.0.1:0", MultipathTCP: true, Handler: prefixEcho("m:")})
			if err != nil {
				t.Fatal(err)
			}
			runEngine(t, engine)
			addrs, err := engine.BindingAddrs(0)
			if err != nil || len(addrs) != 1 {
				t.Fatalf("BindingAddrs(0) = %v, %v, want 1 address", addrs, err)
			}
			conn, using := dialMultipathTCP(t, addrs[0])
			if !using {
				t.Fatalf("the client connection to %s is not Multipath TCP", addrs[0])
			}
			expectMultipathTCPEcho(t, conn, addrs[0], "m:", "ping")
		})
	}
}

// A binding without MultipathTCP is a plain TCP listener: a client that asks
// for Multipath TCP falls back to plain TCP against it, and the exchange
// still works.
func TestMultipathTCPDefaultIsPlainTCP(t *testing.T) {
	requireMultipathTCP(t)
	for name, config := range bindingConfigs() {
		t.Run(name, func(t *testing.T) {
			config.Name = "plain-" + name
			engine, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: prefixEcho("p:")})
			if err != nil {
				t.Fatal(err)
			}
			runEngine(t, engine)
			addrs, err := engine.BindingAddrs(0)
			if err != nil || len(addrs) != 1 {
				t.Fatalf("BindingAddrs(0) = %v, %v, want 1 address", addrs, err)
			}
			conn, using := dialMultipathTCP(t, addrs[0])
			if using {
				t.Fatalf("a Multipath TCP client dialing the plain listener %s stayed on Multipath TCP", addrs[0])
			}
			expectMultipathTCPEcho(t, conn, addrs[0], "p:", "ping")
		})
	}
}

// AddBinding opens a Multipath TCP listener at runtime, as New does at
// startup, on every engine shape.
func TestMultipathTCPAddBinding(t *testing.T) {
	requireMultipathTCP(t)
	for name, config := range bindingConfigs() {
		t.Run(name, func(t *testing.T) {
			config.Name = "mptcp-add-" + name
			engine, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: prefixEcho("a:")})
			if err != nil {
				t.Fatal(err)
			}
			runEngine(t, engine)
			i, err := engine.AddBinding(Binding{Addr: "127.0.0.1:0", MultipathTCP: true, Handler: prefixEcho("m:")})
			if err != nil {
				t.Fatalf("AddBinding: %v", err)
			}
			addrs, err := engine.BindingAddrs(i)
			if err != nil || len(addrs) != 1 {
				t.Fatalf("BindingAddrs(%d) = %v, %v, want 1 address", i, addrs, err)
			}
			conn, using := dialMultipathTCP(t, addrs[0])
			if !using {
				t.Fatalf("the client connection to the added binding at %s is not Multipath TCP", addrs[0])
			}
			expectMultipathTCPEcho(t, conn, addrs[0], "m:", "ping")
		})
	}
}

// A spread listener on a port the kernel did not choose claims the address
// before its pollers' sockets bind it, and the claim keeps the protocol: the
// Multipath TCP listeners bind a port the claim released, and a Multipath TCP
// client reaches them. ReusePort is off, which forces the claim.
func TestMultipathTCPClaimedPort(t *testing.T) {
	requireMultipathTCP(t)
	config := DefaultConfig()
	config.Name = "mptcp-claim"
	config.IOPollers, config.IOPollerCount = true, 3
	// ReusePort off is what forces the claim below: with it on the sockets
	// bind the port themselves.
	config.ReusePort = false
	var (
		engine *Engine
		port   int
	)
	for attempt := 0; ; attempt++ {
		// A port the kernel just found free is the one to claim; another
		// process could take it in between, so a refusal is retried with a
		// new one.
		probe, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port = probe.Addr().(*net.TCPAddr).Port
		probe.Close()
		addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
		engine, err = New(config, Binding{Network: "tcp4", Addr: addr, MultipathTCP: true, Handler: prefixEcho("c:")})
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.EADDRINUSE) || attempt == 4 {
			t.Fatalf("listening on the claimed port %d: %v", port, err)
		}
	}
	runEngine(t, engine)
	addrs, err := engine.BindingAddrs(0)
	if err != nil || len(addrs) != 1 {
		t.Fatalf("BindingAddrs(0) = %v, %v, want 1 address", addrs, err)
	}
	if got := addrs[0].(*net.TCPAddr).Port; got != port {
		t.Fatalf("BindingAddrs(0) = %s, want port %d", addrs[0], port)
	}
	conn, using := dialMultipathTCP(t, addrs[0])
	if !using {
		t.Fatalf("the client connection to the claimed port %d is not Multipath TCP", port)
	}
	expectMultipathTCPEcho(t, conn, addrs[0], "c:", "ping")
}

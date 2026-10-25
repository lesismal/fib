//go:build linux || darwin

package fib

import (
	"errors"
	"net"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// dontFragmentSet reports whether a UDP socket of the family is set not to
// fragment, as SetDontFragment sets it.
func dontFragmentSet(t *testing.T, fd int, ipv6 bool) bool {
	t.Helper()
	level, opt, want := syscall.IPPROTO_IP, 28, 1 // IP_DONTFRAG
	switch {
	case runtime.GOOS == "linux" && ipv6:
		level, opt, want = syscall.IPPROTO_IPV6, 23, 3 // IPV6_MTU_DISCOVER, IPV6_PMTUDISC_PROBE
	case runtime.GOOS == "linux":
		opt, want = 10, 3 // IP_MTU_DISCOVER, IP_PMTUDISC_PROBE
	case ipv6:
		level, opt = syscall.IPPROTO_IPV6, 62 // IPV6_DONTFRAG
	}
	v, err := syscall.GetsockoptInt(fd, level, opt)
	if err != nil {
		t.Fatal(err)
	}
	return v == want
}

// SetDontFragment sets the socket a UDP peer of a listener sends through,
// and that of a dialed UDP connection; a TCP connection is not UDP.
func TestSetDontFragment(t *testing.T) {
	got := make(chan error, 1)
	var peer *Connection
	handler := HandlerFuncs{Data: func(c *Connection, b []byte) {
		peer = c
		got <- c.SetDontFragment()
	}}
	engine, addr := startUDPServer(t, DefaultConfig(), handler)
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("SetDontFragment = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no datagram")
	}
	if !dontFragmentSet(t, peer.udp.listener.fd, false) {
		t.Fatal("the listener's socket fragments")
	}

	dialed := make(chan *Connection, 1)
	if err := engine.DialWithHandler("udp", addr.String(), 5*time.Second, HandlerFuncs{}, func(c *Connection, err error) {
		if err != nil {
			t.Error(err)
		}
		dialed <- c
	}); err != nil {
		t.Fatal(err)
	}
	c := <-dialed
	if c == nil {
		t.FailNow()
	}
	if err := c.SetDontFragment(); err != nil {
		t.Fatalf("SetDontFragment on a dialed connection = %v", err)
	}
	if !dontFragmentSet(t, c.FD(), false) {
		t.Fatal("the dialed socket fragments")
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	tcp := make(chan *Connection, 1)
	if err := engine.DialWithHandler("tcp", ln.Addr().String(), 5*time.Second, HandlerFuncs{}, func(c *Connection, err error) {
		if err != nil {
			t.Error(err)
		}
		tcp <- c
	}); err != nil {
		t.Fatal(err)
	}
	if c := <-tcp; c != nil && !errors.Is(c.SetDontFragment(), ErrNotUDP) {
		t.Fatal("SetDontFragment on TCP does not report ErrNotUDP")
	}
}

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"
)

// peerRecorder reports the connections that opened and the datagrams that
// arrived, with the connection each arrived on.
type peerRecorder struct {
	opened chan *Connection
	data   chan peerDatagram
}

type peerDatagram struct {
	c    *Connection
	data string
}

func newPeerRecorder() *peerRecorder {
	return &peerRecorder{opened: make(chan *Connection, 16), data: make(chan peerDatagram, 16)}
}

func (h *peerRecorder) OnOpen(c *Connection)               { h.opened <- c }
func (h *peerRecorder) OnPriorityData(*Connection, []byte) {}
func (h *peerRecorder) OnData(c *Connection, b []byte)     { h.data <- peerDatagram{c, string(b)} }
func (h *peerRecorder) OnClose(*Connection, error)         {}

// oneSocketConfig binds each address once, on a single loop.
func oneSocketConfig() Config {
	config := DefaultConfig()
	config.IOPollers = false
	config.ReusePort = false
	return config
}

func startPeerEngine(t *testing.T, config Config, h Handler) (*Engine, *net.UDPAddr) {
	t.Helper()
	config.Network = "udp"
	config.LogSettings = false
	e, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: h})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := e.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- e.Run() }()
	t.Cleanup(func() {
		e.Stop()
		<-done
		_ = e.Close()
	})
	return e, addr
}

// TestOpenUDPPeerSpeaksFirst checks that a peer OpenUDPPeer opens sends from
// the binding's port, and that the reply arrives on it.
func TestOpenUDPPeerSpeaksFirst(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config Config
		// same says the reply must arrive on the opened connection, which
		// it does when the address has the one socket.
		same bool
	}{
		{"one socket", oneSocketConfig(), true},
		{"default", DefaultConfig(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newPeerRecorder()
			e, addr := startPeerEngine(t, tc.config, h)
			remote, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer remote.Close()
			raddr := remote.LocalAddr().(*net.UDPAddr).AddrPort()
			c, err := e.OpenUDPPeer(0, raddr)
			if err != nil {
				t.Fatal(err)
			}
			if got := <-h.opened; got != c {
				t.Fatal("OnOpen ran for another connection")
			}
			if got := c.RemoteAddrPort(); got != raddr {
				t.Fatalf("RemoteAddrPort %v, want %v", got, raddr)
			}
			again, err := e.OpenUDPPeer(0, raddr)
			if err != nil || again != c {
				t.Fatalf("second OpenUDPPeer: %v, same connection %v", err, again == c)
			}
			if err := c.Send([]byte("ping")); err != nil {
				t.Fatal(err)
			}
			_ = remote.SetReadDeadline(time.Now().Add(10 * time.Second))
			buf := make([]byte, 16)
			n, from, err := remote.ReadFromUDPAddrPort(buf)
			if err != nil {
				t.Fatal(err)
			}
			if string(buf[:n]) != "ping" || from.Port() != uint16(addr.Port) {
				t.Fatalf("read %q from %v, want ping from port %d", buf[:n], from, addr.Port)
			}
			if _, err := remote.WriteToUDPAddrPort([]byte("pong"), from); err != nil {
				t.Fatal(err)
			}
			select {
			case d := <-h.data:
				if d.data != "pong" || d.c.RemoteAddrPort() != raddr || tc.same && d.c != c {
					t.Fatalf("got %q from %v on the opened connection %v", d.data, d.c.RemoteAddrPort(), d.c == c)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("no reply")
			}
		})
	}
}

// TestOpenUDPPeerErrors checks what OpenUDPPeer refuses.
func TestOpenUDPPeerErrors(t *testing.T) {
	e, _ := startPeerEngine(t, oneSocketConfig(), newPeerRecorder())
	if _, err := e.OpenUDPPeer(0, netip.AddrPort{}); !errors.Is(err, errUDPPeerAddr) {
		t.Fatalf("zero address: %v", err)
	}
	if _, err := e.OpenUDPPeer(0, netip.MustParseAddrPort("[2001:db8::1]:9")); !errors.Is(err, errUDPPeerAddr) {
		t.Fatalf("IPv6 on an IPv4 socket: %v", err)
	}
	if _, err := e.OpenUDPPeer(1, netip.MustParseAddrPort("127.0.0.1:9")); !errors.Is(err, errNoBinding) {
		t.Fatalf("no such binding: %v", err)
	}
}

// TestOpenUDPPeerZones checks the IPv6 zones OpenUDPPeer takes, by number
// or by interface name, and that one naming no interface is an error.
func TestOpenUDPPeerZones(t *testing.T) {
	if n, err := zoneIndex(""); n != 0 || err != nil {
		t.Fatalf("no zone: %d, %v", n, err)
	}
	if n, err := zoneIndex("7"); n != 7 || err != nil {
		t.Fatalf("zone 7: %d, %v", n, err)
	}
	ifs, _ := net.Interfaces()
	for _, ifi := range ifs {
		if ifi.Flags&net.FlagLoopback != 0 {
			if n, err := zoneIndex(ifi.Name); n != uint32(ifi.Index) || err != nil {
				t.Fatalf("zone %s: %d, %v, want %d", ifi.Name, n, err, ifi.Index)
			}
		}
	}
	e, _ := startPeerEngine(t, oneSocketConfig(), newPeerRecorder())
	if _, err := e.OpenUDPPeer(0, netip.MustParseAddrPort("[fe80::1%no-such-interface]:9")); err == nil {
		t.Fatal("a zone that names no interface was taken")
	}
	if _, err := e.OpenUDPPeer(0, netip.MustParseAddrPort("[fe80::1%1]:9")); !errors.Is(err, errUDPPeerAddr) {
		t.Fatalf("a zoned IPv6 address on an IPv4 socket: %v", err)
	}
}

// TestOpenUDPPeerIPv4OnIPv6 checks that an IPv4 peer opened on an IPv6
// socket is keyed as the socket reports it, mapped into IPv6.
func TestOpenUDPPeerIPv4OnIPv6(t *testing.T) {
	config := oneSocketConfig()
	config.Network = "udp"
	config.LogSettings = false
	e, err := New(config, Binding{Addr: "[::1]:0", Handler: newPeerRecorder()})
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- e.Run() }()
	defer func() {
		e.Stop()
		<-done
		_ = e.Close()
	}()
	c, err := e.OpenUDPPeer(0, netip.MustParseAddrPort("127.0.0.1:9"))
	if err != nil {
		t.Fatal(err)
	}
	if c.udp.key != netip.MustParseAddrPort("[::ffff:127.0.0.1]:9") || c.RemoteAddrPort() != netip.MustParseAddrPort("127.0.0.1:9") {
		t.Fatalf("keyed %v, RemoteAddrPort %v", c.udp.key, c.RemoteAddrPort())
	}
}

// TestOpenUDPPeerShutdown checks that a shutting down engine opens no new
// peer but still hands back the ones it has, and that a stopped one opens
// none.
func TestOpenUDPPeerShutdown(t *testing.T) {
	e, _ := startPeerEngine(t, oneSocketConfig(), newPeerRecorder())
	known := netip.MustParseAddrPort("127.0.0.1:9")
	c, err := e.OpenUDPPeer(0, known)
	if err != nil {
		t.Fatal(err)
	}
	shut := make(chan error, 1)
	go func() { shut <- e.Shutdown(context.Background()) }()
	for !e.ShuttingDown() {
		time.Sleep(time.Millisecond)
	}
	if _, err := e.OpenUDPPeer(0, netip.MustParseAddrPort("127.0.0.1:10")); !errors.Is(err, errPeerClosed) || !errors.Is(err, net.ErrClosed) {
		t.Fatalf("a new peer while shutting down: %v", err)
	}
	if again, err := e.OpenUDPPeer(0, known); err != nil || again != c {
		t.Fatalf("a known peer while shutting down: %v", err)
	}
	c.Close()
	if err := <-shut; err != nil {
		t.Fatal(err)
	}
	if _, err := e.OpenUDPPeer(0, known); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("after Stop: %v", err)
	}
}

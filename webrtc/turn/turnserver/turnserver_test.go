//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package turnserver

import (
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/webrtc/turn"
)

const (
	realm = "fib.test"
	user  = "alice"
	pass  = "secret"
)

var loopback = netip.MustParseAddr("127.0.0.1")

func startServer(t *testing.T, config fib.Config, cfg Config) (*Server, netip.AddrPort, *fib.Engine) {
	t.Helper()
	config.LogSettings = false
	e, err := fib.New(config)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- e.Run() }()
	cfg.Realm = realm
	cfg.Auth = turn.StaticAuth(realm, map[string]string{user: pass})
	cfg.RelayIP = loopback
	// The peers of these tests are on loopback, which a TURN server must
	// not relay to unless told.
	cfg.AllowedPeers = []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}
	s, err := Listen(e, "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	addrs, err := e.BindingAddrs(s.binding)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.Close()
		e.Stop()
		<-done
		_ = e.Close()
	})
	return s, addrPort(addrs[0]), e
}

// udpClient drives a turn.Client over a real UDP socket: a goroutine reads
// it, another fires the client's timers, and a mutex serializes them.
type udpClient struct {
	mu        sync.Mutex
	c         *turn.Client
	conn      *net.UDPConn
	got       chan string
	allocated chan struct{}
	failed    chan error
	done      chan struct{}
}

func newUDPClient(t *testing.T, server netip.AddrPort, bind bool) *udpClient {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	u := &udpClient{conn: conn, got: make(chan string, 16), allocated: make(chan struct{}), failed: make(chan error, 1), done: make(chan struct{})}
	c, err := turn.NewClient(turn.ClientConfig{
		Server: server, Username: user, Password: pass, BindChannels: bind,
		Send:        func(to netip.AddrPort, b []byte) { _, _ = conn.WriteToUDPAddrPort(b, to) },
		OnAllocated: func(_, _ netip.AddrPort) { close(u.allocated) },
		OnFailed:    func(err error) { u.failed <- err },
	})
	if err != nil {
		t.Fatal(err)
	}
	u.c = c
	go func() {
		buf := make([]byte, 2048)
		for {
			n, from, err := conn.ReadFromUDPAddrPort(buf)
			if err != nil {
				return
			}
			u.mu.Lock()
			_, data, _ := c.HandlePacket(from, buf[:n], time.Now())
			if data != nil {
				u.got <- string(data)
			}
			u.mu.Unlock()
		}
	}()
	go func() {
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-u.done:
				return
			case now := <-tick.C:
				u.mu.Lock()
				if d, ok := c.NextTimeout(); ok && !d.After(now) {
					c.HandleTimeout(now)
				}
				u.mu.Unlock()
			}
		}
	}()
	t.Cleanup(func() {
		close(u.done)
		conn.Close()
	})
	return u
}

func (u *udpClient) do(f func(c *turn.Client)) {
	u.mu.Lock()
	defer u.mu.Unlock()
	f(u.c)
}

func wait[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

func TestRelayOnEngine(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config fib.Config
		bind   bool
		ports  [2]uint16
	}{
		{"default", fib.DefaultConfig(), false, [2]uint16{}},
		{"channels", fib.DefaultConfig(), true, [2]uint16{}},
		{"port range", fib.DefaultConfig(), true, [2]uint16{41000, 41009}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, addr, _ := startServer(t, tc.config, Config{PortMin: tc.ports[0], PortMax: tc.ports[1]})
			peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			peerAddr := peer.LocalAddr().(*net.UDPAddr).AddrPort()
			u := newUDPClient(t, addr, tc.bind)
			u.do(func(c *turn.Client) { _ = c.Allocate(time.Now()) })
			select {
			case <-u.allocated:
			case err := <-u.failed:
				t.Fatal(err)
			case <-time.After(10 * time.Second):
				t.Fatal("no allocation")
			}
			var relayed netip.AddrPort
			u.do(func(c *turn.Client) { relayed, _, _ = c.Relayed() })
			if relayed.Addr() != loopback || tc.ports[1] != 0 && (relayed.Port() < tc.ports[0] || relayed.Port() > tc.ports[1]) {
				t.Fatalf("relayed %v", relayed)
			}
			if s.Allocations() != 1 {
				t.Fatalf("%d allocations", s.Allocations())
			}
			buf := make([]byte, 2048)
			for i, msg := range []string{"one", "two", "three"} {
				// The first datagram may beat its permission; send until one
				// arrives.
				var from netip.AddrPort
				var n int
				for range 50 {
					u.do(func(c *turn.Client) { _ = c.SendTo(peerAddr, []byte(msg)) })
					_ = peer.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
					if n, from, err = peer.ReadFromUDPAddrPort(buf); err == nil {
						break
					}
				}
				if err != nil || string(buf[:n]) != msg || from != relayed {
					t.Fatalf("%d: peer read %q from %v: %v", i, buf[:n], from, err)
				}
				if _, err := peer.WriteToUDPAddrPort([]byte("echo:"+msg), relayed); err != nil {
					t.Fatal(err)
				}
				if got := wait(t, u.got, "the echo"); got != "echo:"+msg {
					t.Fatalf("client got %q", got)
				}
				time.Sleep(50 * time.Millisecond)
			}
			u.do(func(c *turn.Client) { c.Close(time.Now()) })
			deadline := time.Now().Add(10 * time.Second)
			for s.Allocations() != 0 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if s.Allocations() != 0 {
				t.Fatal("allocation outlived Close")
			}
		})
	}
}

func TestSSRFOnEngine(t *testing.T) {
	_, addr, _ := startServer(t, fib.DefaultConfig(), Config{})
	u := newUDPClient(t, addr, false)
	u.do(func(c *turn.Client) { _ = c.Allocate(time.Now()) })
	wait(t, u.allocated, "the allocation")
	// 10.0.0.1 is private, and not in AllowedPeers.
	private := netip.MustParseAddrPort("10.0.0.1:6379")
	u.do(func(c *turn.Client) { _ = c.CreatePermission(private.Addr(), time.Now()) })
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		u.do(func(c *turn.Client) { err = c.SendTo(private, []byte("x")) })
		if err != nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("a permission for a private peer was not refused")
}

func TestConfigRejected(t *testing.T) {
	base := turn.ServerConfig{Realm: realm, Auth: turn.StaticAuth(realm, nil)}
	if _, err := New(Config{ServerConfig: base}); err == nil {
		t.Fatal("no RelayIP")
	}
	if _, err := New(Config{ServerConfig: base, RelayIP: netip.IPv4Unspecified()}); err == nil {
		t.Fatal("wildcard RelayIP")
	}
	bad := base
	bad.Send = func(turn.FiveTuple, []byte) {}
	if _, err := New(Config{ServerConfig: bad, RelayIP: loopback}); err != errBadConfig {
		t.Fatalf("Send set: %v", err)
	}
	if _, err := New(Config{ServerConfig: base, RelayIP: loopback, PortMin: 2, PortMax: 1}); err == nil {
		t.Fatal("inverted port range")
	}
}

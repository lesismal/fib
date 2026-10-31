//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package turnserver

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/webrtc/stun"
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
	addrs, err := e.BindingAddrs(s.run.Load().binding)
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
	// quiet, guarded by mu, stops the client's timers: it sends no more
	// refreshes.
	quiet bool
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
				if d, ok := c.NextTimeout(); ok && !d.After(now) && !u.quiet {
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
	if _, err := New(Config{ServerConfig: turn.ServerConfig{Auth: base.Auth}, RelayIP: loopback}); err == nil {
		t.Fatal("no realm")
	}
	e := runEngine(t)
	if _, err := Listen(e, "127.0.0.1:0", Config{ServerConfig: base}); err == nil {
		t.Fatal("Listen with no RelayIP")
	}
	if _, err := Listen(e, "not an address", Config{ServerConfig: base, RelayIP: loopback}); err == nil {
		t.Fatal("Listen on no address")
	}
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

// An allocation whose client goes silent is freed by the server's timer,
// its relayed binding removed.
func TestExpiryOnEngine(t *testing.T) {
	cfg := Config{}
	cfg.DefaultLifetime, cfg.MaxLifetime = time.Second, time.Second
	cfg.SweepInterval = 50 * time.Millisecond
	s, addr, _ := startServer(t, fib.DefaultConfig(), cfg)
	u := newUDPClient(t, addr, false)
	u.do(func(c *turn.Client) { _ = c.Allocate(time.Now()) })
	wait(t, u.allocated, "the allocation")
	var relayed netip.AddrPort
	u.do(func(c *turn.Client) { relayed, _, _ = c.Relayed(); u.quiet = true })
	deadline := time.Now().Add(10 * time.Second)
	for s.Allocations() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if s.Allocations() != 0 || !freed(relayed) {
		t.Fatalf("%d allocations, relayed %v freed %v", s.Allocations(), relayed, freed(relayed))
	}
}

// runEngine runs an engine with no bindings until the test ends.
func runEngine(t *testing.T) *fib.Engine {
	t.Helper()
	config := fib.DefaultConfig()
	config.LogSettings = false
	e, err := fib.New(config)
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
	return e
}

// freed waits until a UDP socket can bind addr, as it can once the relayed
// binding that held it is removed.
func freed(addr netip.AddrPort) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(addr)); err == nil {
			c.Close()
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// Datagrams that reach the listening binding before Start are dropped.
func TestBeforeStart(t *testing.T) {
	e := runEngine(t)
	cfg := Config{RelayIP: loopback}
	cfg.Realm, cfg.Auth = realm, turn.StaticAuth(realm, nil)
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	i, err := e.AddBinding(fib.Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: s})
	if err != nil {
		t.Fatal(err)
	}
	addrs, _ := e.BindingAddrs(i)
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var m stun.Message
	m.Build(stun.BindingRequest, stun.NewTransactionID())
	ask := func() bool {
		_, _ = conn.WriteToUDPAddrPort(m.Raw, addrPort(addrs[0]))
		_ = conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		buf := make([]byte, 512)
		_, _, err := conn.ReadFromUDPAddrPort(buf)
		return err == nil
	}
	if ask() {
		t.Fatal("answered before Start")
	}
	s.Start(e, i)
	for range 10 {
		if ask() {
			return
		}
	}
	t.Fatal("not answered after Start")
}

func TestAllocateRelay(t *testing.T) {
	s, _, e := startServer(t, fib.DefaultConfig(), Config{})
	refused := func(ipv6 bool) error {
		ch := make(chan error, 1)
		s.allocateRelay(turn.FiveTuple{}, user, ipv6, func(r turn.Relay, err error) { ch <- err })
		return wait(t, ch, "done")
	}
	if err := refused(true); err != turn.ErrNoRelay {
		t.Fatalf("IPv6 on an IPv4 relay address: %v", err)
	}

	// A relayed address behind a 1:1 NAT is told as the public one.
	s.cfg.PublicIP = netip.MustParseAddr("::ffff:198.51.100.1")
	r, err := s.bindRelay(e)
	if err != nil {
		t.Fatal(err)
	}
	if r.Addr().Addr() != netip.MustParseAddr("198.51.100.1") || r.Addr().Port() == 0 {
		t.Fatalf("relayed %v", r.Addr())
	}
	bound := netip.AddrPortFrom(loopback, r.Addr().Port())
	r.Close()
	r.Close()
	// A datagram a closed relay still gets is dropped, not given to the
	// core.
	r.OnData(nil, []byte("late"))
	if !freed(bound) {
		t.Fatalf("relay %v still bound after Close", bound)
	}
	s.cfg.PublicIP = netip.Addr{}

	// Every port of the range taken.
	taken, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	port := uint16(taken.LocalAddr().(*net.UDPAddr).Port)
	s.cfg.PortMin, s.cfg.PortMax = port, port
	if err := refused(false); err != turn.ErrNoRelay {
		t.Fatalf("a range with no free port: %v", err)
	}
	// An address the host does not have.
	s.cfg.PortMin, s.cfg.PortMax = 0, 0
	s.cfg.RelayIP = netip.MustParseAddr("192.0.2.1")
	if _, err := s.bindRelay(e); err == nil {
		t.Fatal("bound a relay to an address the host does not have")
	}
	if a := addrPort(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}); a.IsValid() {
		t.Fatalf("a TCP address as a UDP one: %v", a)
	}
}

// The setup queue is never waited for: with it full, an allocation is
// refused and a datagram for a peer or client with no connection dropped,
// and so once the server has closed.
func TestSetupQueueFull(t *testing.T) {
	e := runEngine(t)
	cfg := Config{RelayIP: loopback, SetupQueue: 1}
	cfg.Realm, cfg.Auth = realm, turn.StaticAuth(realm, nil)
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Running but with no setup goroutines yet, so the queue stays full.
	s.run.Store(&running{engine: e})
	if !s.enqueue(func() {}) || s.enqueue(func() {}) {
		t.Fatal("a queue of one")
	}
	ch := make(chan error, 1)
	s.allocateRelay(turn.FiveTuple{}, user, false, func(_ turn.Relay, err error) { ch <- err })
	if err := <-ch; err != turn.ErrNoRelay {
		t.Fatalf("allocation with the queue full: %v", err)
	}
	r := &relay{s: s, peers: map[netip.AddrPort]*fib.Connection{}, backlog: map[netip.AddrPort][][]byte{}}
	peer := netip.MustParseAddrPort("127.0.0.1:9")
	r.SendTo(peer, []byte("x"))
	s.sendToClient(turn.FiveTuple{Client: peer}, []byte("y"))
	if s.Unsent() != 2 || len(r.backlog) != 0 {
		t.Fatalf("%d unsent, %d peers waiting", s.Unsent(), len(r.backlog))
	}
	// A peer's backlog is bounded while its connection opens.
	<-s.setup
	for range maxPeerBacklog + 3 {
		r.SendTo(peer, []byte("z"))
	}
	if len(r.backlog[peer]) != maxPeerBacklog || s.Unsent() != 5 {
		t.Fatalf("backlog %d, %d unsent", len(r.backlog[peer]), s.Unsent())
	}
	// The queued open, run on a closed relay, drops the backlog.
	r.closed.Store(true)
	(<-s.setup)()
	if s.Unsent() != 5+maxPeerBacklog {
		t.Fatalf("%d unsent", s.Unsent())
	}
	s.Close()
	if s.enqueue(func() {}) {
		t.Fatal("queued after Close")
	}
}

// A client whose connection fib forgot is reached by opening it again, on
// a setup goroutine.
func TestSendToForgottenClient(t *testing.T) {
	s, _, _ := startServer(t, fib.DefaultConfig(), Config{})
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := conn.LocalAddr().(*net.UDPAddr).AddrPort()
	s.sendToClient(turn.FiveTuple{Client: client}, []byte("hello"))
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 64)
	n, _, err := conn.ReadFromUDPAddrPort(buf)
	if err != nil || string(buf[:n]) != "hello" {
		t.Fatalf("read %q: %v", buf[:n], err)
	}
	// One the engine cannot reach is counted unsent.
	s.sendToClient(turn.FiveTuple{Client: netip.MustParseAddrPort("[::1]:9")}, []byte("x"))
	deadline := time.Now().Add(5 * time.Second)
	for s.Unsent() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if s.Unsent() != 1 {
		t.Fatal("an unreachable client was not counted")
	}
}

// Shutdown refuses new allocations while the held ones go on, and closes
// once they end, or when its context does.
func TestShutdown(t *testing.T) {
	s, addr, _ := startServer(t, fib.DefaultConfig(), Config{})
	u := newUDPClient(t, addr, false)
	u.do(func(c *turn.Client) { _ = c.Allocate(time.Now()) })
	wait(t, u.allocated, "the allocation")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := s.Shutdown(ctx); err != context.DeadlineExceeded || s.Allocations() != 0 {
		t.Fatalf("Shutdown with an allocation held: %v, %d left", err, s.Allocations())
	}

	s2, addr2, _ := startServer(t, fib.DefaultConfig(), Config{})
	v := newUDPClient(t, addr2, false)
	v.do(func(c *turn.Client) { _ = c.Allocate(time.Now()) })
	wait(t, v.allocated, "the allocation")
	done := make(chan error, 1)
	go func() { done <- s2.Shutdown(context.Background()) }()
	deadline := time.Now().Add(5 * time.Second)
	for !s2.Core().Draining() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// A new client is refused, the held one deallocates and Shutdown
	// returns.
	w := newUDPClient(t, addr2, false)
	w.do(func(c *turn.Client) { _ = c.Allocate(time.Now()) })
	if err := wait(t, w.failed, "the refusal"); err == nil {
		t.Fatal("allocated while draining")
	}
	v.do(func(c *turn.Client) { c.Close(time.Now()) })
	if err := wait(t, done, "Shutdown"); err != nil {
		t.Fatal(err)
	}
}

// Limits and credentials change in place, without a restart.
func TestUpdateInPlace(t *testing.T) {
	s, addr, _ := startServer(t, fib.DefaultConfig(), Config{})
	u := newUDPClient(t, addr, false)
	u.do(func(c *turn.Client) { _ = c.Allocate(time.Now()) })
	wait(t, u.allocated, "the allocation")
	s.Core().SetLimits(turn.Limits{MaxAllocations: 1})
	v := newUDPClient(t, addr, false)
	v.do(func(c *turn.Client) { _ = c.Allocate(time.Now()) })
	if err := wait(t, v.failed, "the refusal"); err == nil {
		t.Fatal("allocated past a lowered quota")
	}
	if s.Allocations() != 1 || s.Core().Limits().MaxAllocations != 1 || s.Core().Limits().DefaultLifetime != turn.DefaultLifetime {
		t.Fatal("the held allocation, or the limits")
	}
	s.Core().SetAuth(nil)
	s.Core().SetAuth(turn.StaticAuth(realm, map[string]string{user: "changed"}))
	s.Core().SetLimits(turn.Limits{})
	w := newUDPClient(t, addr, false)
	w.do(func(c *turn.Client) { _ = c.Allocate(time.Now()) })
	if err := wait(t, w.failed, "the refusal"); err == nil {
		t.Fatal("allocated with old credentials")
	}
}

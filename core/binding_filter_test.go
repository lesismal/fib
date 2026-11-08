package core

import (
	"errors"
	"net"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// peerPolicy is a Binding.Filter whose answer is swapped while the binding
// serves, as an application swaps its allow and deny lists on a reload: the
// filter reads it through an atomic pointer on every call.
type peerPolicy struct {
	// refused is the set of refused ports, or nil to refuse every peer.
	refused  atomic.Pointer[map[uint16]bool]
	allowAll atomic.Bool
	mu       sync.Mutex
	asked    []netip.AddrPort
}

func (p *peerPolicy) filter(peer netip.AddrPort) bool {
	p.mu.Lock()
	p.asked = append(p.asked, peer)
	p.mu.Unlock()
	if p.allowAll.Load() {
		return true
	}
	refused := p.refused.Load()
	return refused != nil && !(*refused)[peer.Port()]
}

func (p *peerPolicy) refusePorts(ports ...uint16) {
	set := make(map[uint16]bool, len(ports))
	for _, port := range ports {
		set[port] = true
	}
	p.refused.Store(&set)
	p.allowAll.Store(false)
}

// askedAbout returns how many times the filter was asked about a peer at
// port, or about any peer for port 0, and checks every address it was given
// is an IPv4 one, unmapped, since the tests' peers are all 127.0.0.1.
func (p *peerPolicy) askedAbout(t *testing.T, port uint16) int {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, peer := range p.asked {
		if !peer.Addr().Is4() || peer.Addr() != netip.MustParseAddr("127.0.0.1") {
			t.Fatalf("the filter was asked about %v, want 127.0.0.1 unmapped", peer)
		}
		if port == 0 || peer.Port() == port {
			n++
		}
	}
	return n
}

// filterBinding listens on b on a new engine of config, or adds it to one
// already running when added says to, and returns the engine and where the
// binding listens.
func filterBinding(t *testing.T, name string, config Config, added bool, b Binding) (*Engine, net.Addr) {
	t.Helper()
	if !added {
		server := startDynamic(t, name, config, b)
		return server, bindingAddrs(t, server, 0, 1)[0]
	}
	server := startDynamic(t, name, config, Binding{Addr: "127.0.0.1:0", Handler: prefixEcho("other:")})
	i, err := server.AddBinding(b)
	if err != nil {
		t.Fatal(err)
	}
	return server, bindingAddrs(t, server, i, 1)[0]
}

// expectClosedUnserved dials addr and checks the server closes the
// connection without answering what it sends.
func expectClosedUnserved(t *testing.T, addr net.Addr) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr.String(), 5*time.Second)
	if err != nil {
		// The kernel may refuse the dial itself once the closes reset it.
		return
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = conn.Write([]byte("hi"))
	buf := make([]byte, 16)
	n, err := conn.Read(buf)
	if err == nil {
		t.Fatalf("a refused peer was answered %q", buf[:n])
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal("a refused peer's connection was left open")
	}
}

// TestBindingFilterTCP checks, on every engine shape and on a binding New
// opened and one AddBinding added, that a connection the binding's Filter
// refuses is closed without OnOpen or a MaxConns slot, that one it lets
// through is served, and that swapping the filter's policy while the binding
// serves applies to the next connection and leaves those already open alone.
// The binding listens on both families where it can, so an IPv4 peer
// arrives as an IPv4-mapped IPv6 address, which the filter must see
// unmapped.
func TestBindingFilterTCP(t *testing.T) {
	for name, config := range dynamicConfigs() {
		for _, added := range []bool{false, true} {
			label := name + "/new"
			if added {
				label = name + "/added"
			}
			t.Run(label, func(t *testing.T) {
				config.MaxConns = 1
				policy := &peerPolicy{}
				log := newCloseLog()
				server, addr := filterBinding(t, "filter-tcp-"+name, config, added,
					Binding{Network: "tcp", Addr: ":0", Handler: taggedEcho("f:", log), Filter: policy.filter})
				port := addr.(*net.TCPAddr).Port
				local := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}

				// Refused: closed, not opened, and no slot taken, which a
				// MaxConns of one would show by refusing the next.
				for k := 0; k < 3; k++ {
					expectClosedUnserved(t, local)
				}
				if opened := log.opened.Load(); opened != 0 {
					t.Fatalf("%d refused connections opened", opened)
				}
				if s := server.Stats(); s.OpenConns != 0 || s.ConnsRefused != 0 {
					t.Fatalf("stats %+v after refusals, want none open or refused", s)
				}
				if policy.askedAbout(t, 0) < 3 {
					t.Fatalf("the filter was asked %d times, want 3", policy.askedAbout(t, 0))
				}

				// Allowed: served.
				policy.allowAll.Store(true)
				conn, got := dialMessage(t, local, "hi")
				defer conn.Close()
				if got != "f:hi" {
					t.Fatalf("an allowed peer was answered %q", got)
				}

				// Refused again: the next peer is refused, the open one
				// still served.
				policy.allowAll.Store(false)
				expectClosedUnserved(t, local)
				if got, err := roundTrip(conn, "again"); err != nil || got != "f:again" {
					t.Fatalf("the open connection answered %q, %v after the policy changed", got, err)
				}
				if opened := log.opened.Load(); opened != 1 {
					t.Fatalf("%d connections opened, want 1", opened)
				}
			})
		}
	}
}

// TestBindingFilterUnix checks that a Unix socket binding's connections are
// served without asking its Filter, since their peers have no address.
func TestBindingFilterUnix(t *testing.T) {
	if !unixSupported() {
		t.Skip("no Unix sockets")
	}
	var asked atomic.Int64
	server := startDynamic(t, "filter-unix", DefaultConfig(), Binding{Network: "unix", Addr: dynamicSocketPath(t),
		Handler: prefixEcho("u:"), Filter: func(netip.AddrPort) bool { asked.Add(1); return false }})
	conn, got := dialMessage(t, bindingAddrs(t, server, 0, 1)[0], "hi")
	conn.Close()
	if got != "u:hi" || asked.Load() != 0 {
		t.Fatalf("answered %q with the filter asked %d times, want served and never asked", got, asked.Load())
	}
}

// TestBindingFilterUDP checks, on every engine shape and on a binding New
// opened and one AddBinding added, that the datagrams of a UDP peer the
// binding's Filter refuses never open a connection or reach OnData, each
// asking the filter again, while another peer's are served, and that once
// the policy lets the refused peer through its next datagram is served.
func TestBindingFilterUDP(t *testing.T) {
	for name, config := range dynamicConfigs() {
		for _, added := range []bool{false, true} {
			label := name + "/new"
			if added {
				label = name + "/added"
			}
			t.Run(label, func(t *testing.T) {
				policy := &peerPolicy{}
				var opened, data sync.Map
				log := newCloseLog()
				handler := taggedEcho("u:", log)
				handler.Open = func(c *Connection) {
					opened.Store(c.RemoteAddr().String(), true)
				}
				echo := handler.Data
				handler.Data = func(c *Connection, b []byte) {
					data.Store(c.RemoteAddr().String(), true)
					echo(c, b)
				}
				_, addr := filterBinding(t, "filter-udp-"+name, config, added,
					Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: handler, Filter: policy.filter})
				dial := func() *net.UDPConn {
					conn, err := net.DialUDP("udp", nil, addr.(*net.UDPAddr))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { conn.Close() })
					return conn
				}
				refused, allowed := dial(), dial()
				refusedPort := uint16(refused.LocalAddr().(*net.UDPAddr).Port)
				policy.refusePorts(refusedPort)

				const datagrams = 5
				for k := 0; k < datagrams; k++ {
					if _, err := refused.Write([]byte("no")); err != nil {
						t.Fatal(err)
					}
				}
				deadline := time.Now().Add(5 * time.Second)
				for policy.askedAbout(t, refusedPort) < datagrams {
					if time.Now().After(deadline) {
						t.Fatalf("the filter was asked about %d of the refused peer's %d datagrams",
							policy.askedAbout(t, refusedPort), datagrams)
					}
					time.Sleep(time.Millisecond)
				}
				if got := udpExchange(t, allowed, "hi"); got != "u:hi" {
					t.Fatalf("an allowed peer was answered %q", got)
				}
				if _, ok := opened.Load(refused.LocalAddr().String()); ok {
					t.Fatal("a refused peer was opened")
				}
				if _, ok := data.Load(refused.LocalAddr().String()); ok {
					t.Fatal("a refused peer's datagram reached OnData")
				}
				_ = refused.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
				if n, err := refused.Read(make([]byte, 16)); err == nil {
					t.Fatalf("a refused peer was answered, %d bytes", n)
				}
				// The allowed peer has a connection, so its datagrams are
				// not asked about again.
				asked := policy.askedAbout(t, uint16(allowed.LocalAddr().(*net.UDPAddr).Port))
				if got := udpExchange(t, allowed, "again"); got != "u:again" {
					t.Fatalf("an allowed peer was answered %q", got)
				}
				if again := policy.askedAbout(t, uint16(allowed.LocalAddr().(*net.UDPAddr).Port)); again != asked {
					t.Fatalf("the filter was asked about a peer with a connection: %d times, then %d", asked, again)
				}

				policy.allowAll.Store(true)
				if got := udpExchange(t, refused, "now"); got != "u:now" {
					t.Fatalf("a peer let through was answered %q", got)
				}
			})
		}
	}
}

// udpExchange sends message on conn and returns the reply.
func udpExchange(t *testing.T, conn *net.UDPConn, message string) string {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write([]byte(message)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("reading the reply to %q: %v", message, err)
	}
	return string(buf[:n])
}

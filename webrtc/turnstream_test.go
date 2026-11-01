//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package webrtc

import (
	"fmt"
	"io"
	"net"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/tlstest"
	"github.com/lesismal/fib/webrtc/turn"
	"github.com/lesismal/fib/webrtc/turn/turnserver"
)

// turnURLs are one TURN server's URLs over UDP, TCP and TLS.
type turnURLs struct {
	udp, tcp, tls string
	server        *turnserver.Server
}

// startTURNStreams runs a TURN server as startTURN does, serving TCP and
// TLS (tlstest's certificate, for "localhost") besides UDP.
func startTURNStreams(t testing.TB) turnURLs {
	t.Helper()
	e := startEngine(t)
	cfg := turnserver.Config{RelayIP: netip.MustParseAddr("127.0.0.1")}
	cfg.Realm = "fib.test"
	cfg.Auth = turn.StaticAuth(cfg.Realm, map[string]string{"user": "pass"})
	cfg.AllowedPeers = []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}
	s, err := turnserver.Listen(e, "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	udp, err := e.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	bound := func(i int, err error) netip.AddrPort {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		addrs, err := e.BindingAddrs(i)
		if err != nil {
			t.Fatal(err)
		}
		return addrs[0].(*net.TCPAddr).AddrPort()
	}
	tcp := bound(s.ListenTCP("127.0.0.1:0"))
	tls := bound(s.ListenTLS("127.0.0.1:0", serverTLS))
	return turnURLs{
		udp:    fmt.Sprintf("turn:%s", udp.AddrPort()),
		tcp:    fmt.Sprintf("turn:%s?transport=tcp", tcp),
		tls:    fmt.Sprintf("turns:localhost:%d", tls.Port()),
		server: s,
	}
}

// trustedTURN is the TLS configuration that trusts tlstest's certificate,
// with no ServerName, which the URL's host then gives.
func trustedTURN(t testing.TB) Configuration {
	_, client, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	client.ServerName = ""
	return Configuration{TURNTLSConfig: client}
}

// splitProxy forwards TCP connections to target, passing what target
// sends back in pieces of a few bytes, so that the TURN client gets its
// messages split across reads. It counts the connections it has accepted
// and those the client has closed.
type splitProxy struct {
	addr             string
	accepted, closed atomic.Int32
}

func newSplitProxy(t testing.TB, target string) *splitProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	p := &splitProxy{addr: ln.Addr().String()}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			p.accepted.Add(1)
			s, err := net.Dial("tcp", target)
			if err != nil {
				c.Close()
				continue
			}
			go func() {
				_, _ = io.Copy(s, c)
				p.closed.Add(1)
				s.Close()
			}()
			go func() {
				defer c.Close()
				buf := make([]byte, 7)
				for {
					n, err := s.Read(buf)
					if n > 0 {
						if _, err := c.Write(buf[:n]); err != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	return p
}

// url is the proxy's TURN URL over TCP.
func (p *splitProxy) url() string { return "turn:" + p.addr + "?transport=tcp" }

// tcpTarget is the address of a "turn:...?transport=tcp" URL.
func tcpTarget(url string) string {
	return strings.TrimSuffix(strings.TrimPrefix(url, "turn:"), "?transport=tcp")
}

// waitFor polls cond until it holds.
func waitFor(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitAllocations(t testing.TB, s *turnserver.Server, n int) {
	t.Helper()
	waitFor(t, fmt.Sprintf("%d allocations", n), func() bool { return s.Allocations() == n })
}

// relayCandidates counts the relay candidates of pc's local description.
func relayCandidates(pc *PeerConnection) int {
	return strings.Count(pc.LocalDescription().SDP, "typ relay")
}

// TestTURNOverStream connects two PeerConnections that gather relay
// candidates only, from a TURN server they reach over TCP, through a proxy
// that splits its messages, or over TLS; the peer data between them is
// relayed over UDP. Closing them frees the allocations.
func TestTURNOverStream(t *testing.T) {
	urls := startTURNStreams(t)
	split := newSplitProxy(t, tcpTarget(urls.tcp)).url()
	for _, tc := range []struct{ name, url string }{{"tcp", split}, {"tls", urls.tls}} {
		t.Run(tc.name, func(t *testing.T) {
			e := startEngine(t)
			cfg := trustedTURN(t)
			cfg.Engine, cfg.ListenAddr = e, "127.0.0.1:0"
			cfg.ICEServers = []ICEServer{{URLs: []string{tc.url}, Username: "user", Credential: "pass"}}
			cfg.ICETransportPolicy = ICETransportPolicyRelay
			a, b := newPC(t, cfg), newPC(t, cfg)
			trickle(a, b)
			echoChannels(b)
			d := openChannel(t, a, "relayed")
			ca := connectedCh(a)
			negotiate(t, a, b, nil)
			wait(t, ca, "connected through the relay")
			waitOpen(t, d)
			roundTrip(t, d, [][]byte{[]byte("via turn over " + tc.name), randomBytes(100 << 10)})
			local, remote, ok := a.SelectedCandidatePair()
			if !ok || !strings.Contains(local, "typ relay") || !strings.Contains(remote, "typ relay") {
				t.Fatalf("selected pair %q -> %q, want relay to relay", local, remote)
			}
			waitAllocations(t, urls.server, 2)
			wait(t, a.GatheringComplete(), "gathering")
			if n := relayCandidates(a); n != 1 {
				t.Fatalf("%d relay candidates, want 1", n)
			}
			_ = a.Close()
			_ = b.Close()
			waitAllocations(t, urls.server, 0)
		})
	}
}

// TestTURNMixedServers gathers from TURN servers over UDP and TCP, which
// both allocate, and from a TLS server that is not trusted, a server that
// does not speak TURN and a port nobody listens on, which fail; gathering
// completes without them, and the PeerConnections connect.
func TestTURNMixedServers(t *testing.T) {
	urls := startTURNStreams(t)
	junk, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { junk.Close() })
	go func() {
		for {
			c, err := junk.Accept()
			if err != nil {
				return
			}
			_, _ = c.Write([]byte("SSH-2.0-OpenSSH_9.9\r\n"))
			// The client closes the connection once it fails to frame it.
			go func() {
				_, _ = io.Copy(io.Discard, c)
				c.Close()
			}()
		}
	}()
	refused, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refusedAddr := refused.Addr().String()
	refused.Close()
	e := startEngine(t)
	cfg := Configuration{
		Engine:     e,
		ListenAddr: "127.0.0.1:0",
		ICEServers: []ICEServer{
			{URLs: []string{urls.udp, urls.tcp, urls.tls}, Username: "user", Credential: "pass"},
			{URLs: []string{"turn:" + junk.Addr().String() + "?transport=tcp", "turn:" + refusedAddr + "?transport=tcp"}},
		},
		ICETransportPolicy: ICETransportPolicyRelay,
	}
	a, b := newPC(t, cfg), newPC(t, cfg)
	trickle(a, b)
	echoChannels(b)
	d := openChannel(t, a, "mixed")
	ca := connectedCh(a)
	negotiate(t, a, b, nil)
	wait(t, a.GatheringComplete(), "gathering")
	wait(t, ca, "connected")
	waitOpen(t, d)
	roundTrip(t, d, [][]byte{[]byte("udp or tcp")})
	if n := relayCandidates(a); n != 2 {
		t.Fatalf("%d relay candidates, want 2:\n%s", n, a.LocalDescription().SDP)
	}
	waitAllocations(t, urls.server, 4)
	a.mu.Lock()
	failed := 0
	for _, r := range a.relays {
		if r.failed {
			failed++
		}
	}
	a.mu.Unlock()
	if failed != 3 {
		t.Fatalf("%d relays failed, want 3", failed)
	}
}

// TestTURNStreamServerGone connects two PeerConnections through a TURN
// server over TCP and then closes the server, which closes the
// connections: the relay candidates are withdrawn, and with no pair left
// ICE fails.
func TestTURNStreamServerGone(t *testing.T) {
	urls := startTURNStreams(t)
	e := startEngine(t)
	cfg := Configuration{
		Engine:             e,
		ListenAddr:         "127.0.0.1:0",
		ICEServers:         []ICEServer{{URLs: []string{urls.tcp}, Username: "user", Credential: "pass"}},
		ICETransportPolicy: ICETransportPolicyRelay,
		ICEFailedTimeout:   3 * time.Second,
	}
	a, b := newPC(t, cfg), newPC(t, cfg)
	trickle(a, b)
	echoChannels(b)
	d := openChannel(t, a, "doomed")
	ca := connectedCh(a)
	states, failed := iceStates(a)
	negotiate(t, a, b, nil)
	wait(t, ca, "connected")
	waitOpen(t, d)
	roundTrip(t, d, [][]byte{[]byte("before")})
	urls.server.Close()
	wait(t, failed, "ICE failed")
	if s := states(); !hasState(s, ICEConnectionStateDisconnected) {
		t.Fatalf("states %v, want disconnected before failed", s)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.relays[0].failed || a.relays[0].conn != nil || len(a.localCands) != 0 || len(a.agent.LocalCandidates()) != 0 {
		t.Fatalf("relay failed %v, candidates %v", a.relays[0].failed, a.localCands)
	}
	// TURN has no priority data.
	(&turnStream{pc: a, r: a.relays[0]}).OnPriorityData(nil, nil)
}

// TestTURNStreamCloseWhileDialing closes PeerConnections while their
// connection to the TURN server may still be opening: every connection is
// closed, and no allocation outlives them.
func TestTURNStreamCloseWhileDialing(t *testing.T) {
	urls := startTURNStreams(t)
	proxy := newSplitProxy(t, tcpTarget(urls.tcp))
	e := startEngine(t)
	cfg := Configuration{Engine: e, ListenAddr: "127.0.0.1:0"}
	cfg.ICEServers = []ICEServer{{URLs: []string{proxy.url()}, Username: "user", Credential: "pass"}}
	const n = 10
	for range n {
		pc := newPC(t, cfg)
		offer, err := pc.CreateOffer(nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := pc.SetLocalDescription(offer); err != nil {
			t.Fatal(err)
		}
		_ = pc.Close()
	}
	waitFor(t, "every connection closed", func() bool { return proxy.accepted.Load() == n && proxy.closed.Load() == n })
	waitAllocations(t, urls.server, 0)
}

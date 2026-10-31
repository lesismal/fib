//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package webrtc

import (
	"fmt"
	"net/netip"
	"strings"
	"testing"

	"github.com/lesismal/fib/webrtc/turn"
	"github.com/lesismal/fib/webrtc/turn/turnserver"
)

// startTURN runs a TURN server on an engine of its own, relaying on
// loopback, and returns its URL.
func startTURN(t testing.TB) (url string, server *turnserver.Server) {
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
	addr, err := e.LocalUDPAddr()
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("turn:%s", addr.AddrPort()), s
}

// TestTURNRelay connects two PeerConnections that gather relay candidates
// only, so that everything between them goes through the TURN server.
func TestTURNRelay(t *testing.T) {
	url, server := startTURN(t)
	e := startEngine(t)
	cfg := Configuration{
		Engine:             e,
		ListenAddr:         "127.0.0.1:0",
		ICEServers:         []ICEServer{{URLs: []string{url}, Username: "user", Credential: "pass"}},
		ICETransportPolicy: ICETransportPolicyRelay,
	}
	a, b := newPC(t, cfg), newPC(t, cfg)
	trickle(a, b)
	echoChannels(b)
	d := openChannel(t, a, "relayed")
	ca := connectedCh(a)
	negotiate(t, a, b, nil)
	wait(t, ca, "connected through the relay")
	waitOpen(t, d)
	roundTrip(t, d, [][]byte{[]byte("via turn"), randomBytes(100 << 10)})
	local, remote, ok := a.SelectedCandidatePair()
	if !ok || !strings.Contains(local, "typ relay") || !strings.Contains(remote, "typ relay") {
		t.Fatalf("selected pair %q -> %q, want relay to relay", local, remote)
	}
	if n := server.Allocations(); n != 2 {
		t.Fatalf("%d allocations, want 2", n)
	}
	sd := a.LocalDescription().SDP
	if strings.Contains(sd, "typ host") || !strings.Contains(sd, "typ relay") || !strings.Contains(sd, "end-of-candidates") {
		t.Fatalf("local description should hold the relay candidate only:\n%s", sd)
	}
}

func TestTURNWithSharedMuxRefused(t *testing.T) {
	e := startEngine(t)
	_, err := NewPeerConnection(Configuration{Mux: loopbackMux(t, e), ICEServers: []ICEServer{{URLs: []string{"turn:127.0.0.1:3478"}}}})
	if err == nil {
		t.Fatal("TURN on a shared Mux accepted")
	}
}

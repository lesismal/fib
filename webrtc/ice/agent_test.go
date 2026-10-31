package ice

import (
	"errors"
	"net/netip"
	"slices"
	"testing"
	"time"

	"github.com/lesismal/fib/webrtc/stun"
)

func TestConnect(t *testing.T) {
	n := newSimNet(t, 1)
	a, b := pairUp(t, n, Config{Controlling: true}, Config{})
	if !a.gathered || !b.gathered {
		t.Fatal("gathering without STUN servers did not finish at once")
	}
	if !n.run(5*time.Second, bothConnected(a, b)) {
		t.Fatalf("not connected: %v %v", a.a.State(), b.a.State())
	}
	checkSelected(t, a, b)
	if !a.a.Controlling() || b.a.Controlling() {
		t.Fatal("roles changed with no conflict")
	}
	if !slices.Equal(a.states, []State{StateChecking, StateConnected}) || len(a.selected) != 1 || len(b.selected) != 1 {
		t.Fatalf("A states %v, selected %d and %d times", a.states, len(a.selected), len(b.selected))
	}
	// Consent keeps the pair alive well past the consent timeout.
	if n.run(2*time.Minute, func() bool { return a.a.State() != StateConnected || b.a.State() != StateConnected }) {
		t.Fatalf("lost the connection: %v %v", a.a.State(), b.a.State())
	}
}

func TestRoleConflict(t *testing.T) {
	for _, controlling := range []bool{true, false} {
		for seed := uint64(1); seed <= 4; seed++ {
			n := newSimNet(t, seed)
			a, b := pairUp(t, n, Config{Controlling: controlling}, Config{Controlling: controlling})
			// Make the tie-breakers decide either way.
			a.a.tieBreaker, b.a.tieBreaker = seed, 5-seed
			if !n.run(10*time.Second, bothConnected(a, b)) {
				t.Fatalf("controlling=%v seed %d: not connected: %v %v", controlling, seed, a.a.State(), b.a.State())
			}
			checkSelected(t, a, b)
			if a.a.Controlling() != (a.a.tieBreaker > b.a.tieBreaker) {
				t.Fatalf("controlling=%v: the agent with the larger tie-breaker is not controlling", controlling)
			}
		}
	}
}

func TestLossyLinks(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		n := newSimNet(t, seed)
		n.loss = 0.3
		n.latency = 40 * time.Millisecond
		a, b := pairUp(t, n, Config{Controlling: true}, Config{})
		if !n.run(30*time.Second, bothConnected(a, b)) {
			t.Fatalf("seed %d: not connected at 30%% loss: %v %v", seed, a.a.State(), b.a.State())
		}
		checkSelected(t, a, b)
	}
}

func TestMultipleCandidatesPicksBest(t *testing.T) {
	n := newSimNet(t, 3)
	a := n.newPeer("A", Config{Controlling: true}, 1)
	b := n.newPeer("B", Config{}, 2)
	// A has an unreachable address first, then IPv6, then IPv4.
	dead := netip.MustParseAddrPort("192.0.2.1:9")
	sa0 := a.addSock(9, dead, netip.AddrPort{})
	delete(n.socks, dead)
	sa6 := a.addSock(16, addrA6, netip.AddrPort{})
	sa4 := a.addSock(4, addrA, netip.AddrPort{})
	sb := b.addSock(2, addrB, netip.AddrPort{})
	sb6 := b.addSock(26, addrB6, netip.AddrPort{})
	if err := a.a.Gather([]Socket{sa0, sa6, sa4}, n.now); err != nil {
		t.Fatal(err)
	}
	if err := b.a.Gather([]Socket{sb6, sb}, n.now); err != nil {
		t.Fatal(err)
	}
	exchangeCredentials(t, n.now, a, b)
	signalCandidates(t, n.now, a, b)
	if !n.run(5*time.Second, bothConnected(a, b)) {
		t.Fatal("not connected")
	}
	checkSelected(t, a, b)
	p, _ := a.a.SelectedPair()
	if p.Local.LocalID != 16 {
		t.Fatalf("selected %v, want the IPv6 pair, the best that works", p.Local.AddrPort())
	}
}

func TestTrickle(t *testing.T) {
	n := newSimNet(t, 4)
	a := n.newPeer("A", Config{Controlling: true}, 1)
	b := n.newPeer("B", Config{}, 2)
	sa := a.addSock(1, addrA, netip.AddrPort{})
	sb := b.addSock(2, addrB, netip.AddrPort{})
	// Credentials first, as an offer and answer carry them; candidates
	// trickle in after, B's only after A has begun checking.
	exchangeCredentials(t, n.now, a, b)
	a.remote = b
	if err := a.a.Gather([]Socket{sa}, n.now); err != nil {
		t.Fatal(err)
	}
	b.a.EndOfRemoteCandidates()
	n.run(300*time.Millisecond, func() bool { return false })
	if b.a.State() == StateConnected {
		t.Fatal("B connected before it signaled anything")
	}
	b.remote = a
	if err := b.a.Gather([]Socket{sb}, n.now); err != nil {
		t.Fatal(err)
	}
	a.a.EndOfRemoteCandidates()
	if !n.run(5*time.Second, bothConnected(a, b)) {
		t.Fatalf("not connected: %v %v", a.a.State(), b.a.State())
	}
	checkSelected(t, a, b)
}

func TestPeerReflexive(t *testing.T) {
	n := newSimNet(t, 5)
	a := n.newPeer("A", Config{Controlling: true}, 1)
	b := n.newPeer("B", Config{}, 2)
	sa := a.addSock(1, addrA, netip.AddrPort{})
	// B is behind a NAT and has no STUN server: A only learns B's public
	// address from B's checks.
	sb := b.addSock(2, netip.MustParseAddrPort("192.168.1.2:2000"), netip.MustParseAddrPort("203.0.113.7:40000"))
	_ = a.a.Gather([]Socket{sa}, n.now)
	_ = b.a.Gather([]Socket{sb}, n.now)
	exchangeCredentials(t, n.now, a, b)
	signalCandidates(t, n.now, a, b)
	if !n.run(5*time.Second, bothConnected(a, b)) {
		t.Fatalf("not connected: %v %v", a.a.State(), b.a.State())
	}
	checkSelected(t, a, b)
	p, _ := a.a.SelectedPair()
	if p.Remote.Type != PeerReflexive || p.Remote.AddrPort() != netip.MustParseAddrPort("203.0.113.7:40000") {
		t.Fatalf("A's remote is %v", p.Remote.String())
	}
}

func TestServerReflexive(t *testing.T) {
	n := newSimNet(t, 6)
	server := netip.MustParseAddrPort("198.51.100.1:3478")
	n.stunServer(server)
	a := n.newPeer("A", Config{Controlling: true, STUNServers: []netip.AddrPort{server}}, 1)
	b := n.newPeer("B", Config{STUNServers: []netip.AddrPort{server}}, 2)
	sa := a.addSock(1, netip.MustParseAddrPort("192.168.0.10:1000"), netip.MustParseAddrPort("203.0.113.1:50001"))
	sb := b.addSock(2, netip.MustParseAddrPort("192.168.1.20:2000"), netip.MustParseAddrPort("203.0.113.2:50002"))
	_ = a.a.Gather([]Socket{sa}, n.now)
	_ = b.a.Gather([]Socket{sb}, n.now)
	if a.gathered {
		t.Fatal("gathering done before the STUN server answered")
	}
	n.run(time.Second, func() bool { return a.gathered && b.gathered })
	if !a.gathered || !b.gathered {
		t.Fatal("gathering did not finish")
	}
	var srflx *Candidate
	for i, c := range a.cands {
		if c.Type == ServerReflexive {
			srflx = &a.cands[i]
		}
	}
	if srflx == nil || srflx.AddrPort() != netip.MustParseAddrPort("203.0.113.1:50001") ||
		srflx.RelatedAddress != netip.MustParseAddrPort("192.168.0.10:1000") || srflx.LocalID != 1 {
		t.Fatalf("server-reflexive candidate: %+v", srflx)
	}
	exchangeCredentials(t, n.now, a, b)
	signalCandidates(t, n.now, a, b)
	if !n.run(5*time.Second, bothConnected(a, b)) {
		t.Fatalf("not connected: %v %v", a.a.State(), b.a.State())
	}
	checkSelected(t, a, b)
	if p, _ := a.a.SelectedPair(); p.Remote.Type != ServerReflexive {
		t.Fatalf("A's remote is %v", p.Remote.String())
	}
}

func TestGatherTimeout(t *testing.T) {
	n := newSimNet(t, 7)
	a := n.newPeer("A", Config{STUNServers: []netip.AddrPort{netip.MustParseAddrPort("198.51.100.9:3478")}}, 1)
	_ = a.a.Gather([]Socket{a.addSock(1, addrA, netip.AddrPort{})}, n.now)
	if !n.run(30*time.Second, func() bool { return a.gathered }) {
		t.Fatal("gathering did not give up on a silent STUN server")
	}
	if len(a.cands) != 1 {
		t.Fatalf("%d candidates", len(a.cands))
	}
}

func TestRestart(t *testing.T) {
	n := newSimNet(t, 8)
	a, b := pairUp(t, n, Config{Controlling: true}, Config{})
	if !n.run(5*time.Second, bothConnected(a, b)) {
		t.Fatal("not connected")
	}
	ua, _ := a.a.LocalCredentials()
	if err := a.a.Restart("", "", n.now); err != nil {
		t.Fatal(err)
	}
	if u, _ := a.a.LocalCredentials(); u == ua {
		t.Fatal("Restart kept the ufrag")
	}
	if _, ok := a.a.SelectedPair(); !ok {
		t.Fatal("restart dropped the selected pair before a new one was found")
	}
	// The offer carries A's new credentials; B restarts too in its answer.
	if err := b.a.Restart("", "", n.now); err != nil {
		t.Fatal(err)
	}
	exchangeCredentials(t, n.now, a, b)
	signalCandidates(t, n.now, a, b)
	if !n.run(5*time.Second, func() bool { return len(a.selected) == 2 && len(b.selected) == 2 }) {
		t.Fatalf("no new selection: %d %d", len(a.selected), len(b.selected))
	}
	checkSelected(t, a, b)
	if a.a.State() != StateConnected || b.a.State() != StateConnected {
		t.Fatalf("states %v %v", a.a.State(), b.a.State())
	}
	if n.run(time.Minute, func() bool { return a.a.State() != StateConnected || b.a.State() != StateConnected }) {
		t.Fatal("lost consent after the restart")
	}
}

func TestLiteRemote(t *testing.T) {
	n := newSimNet(t, 9)
	// A is full and starts controlled; B is lite. A takes control.
	a, b := pairUp(t, n, Config{}, Config{Lite: true, Controlling: true})
	a.a.SetRemoteLite(true)
	if !a.a.Controlling() || b.a.Controlling() {
		t.Fatal("roles: the full agent must control a lite one")
	}
	if !n.run(5*time.Second, bothConnected(a, b)) {
		t.Fatalf("not connected: %v %v", a.a.State(), b.a.State())
	}
	checkSelected(t, a, b)
	if n.run(time.Minute, func() bool { return a.a.State() != StateConnected }) {
		t.Fatal("full agent lost consent from the lite one")
	}
}

func TestConsentLost(t *testing.T) {
	n := newSimNet(t, 10)
	a, b := pairUp(t, n, Config{Controlling: true}, Config{})
	if !n.run(5*time.Second, bothConnected(a, b)) {
		t.Fatal("not connected")
	}
	n.cut = true
	start := n.now
	if !n.run(15*time.Second, func() bool { return a.a.State() == StateDisconnected }) {
		t.Fatalf("not disconnected: %v", a.a.State())
	}
	n.cut = false
	if !n.run(10*time.Second, func() bool { return a.a.State() == StateConnected }) {
		t.Fatal("did not come back")
	}
	n.cut = true
	start = n.now
	if !n.run(time.Minute, func() bool { return a.a.State() == StateFailed && b.a.State() == StateFailed }) {
		t.Fatalf("consent not lost: %v %v", a.a.State(), b.a.State())
	}
	if d := n.now.Sub(start); d < 25*time.Second || d > 36*time.Second {
		t.Fatalf("consent lost after %v", d)
	}
	if _, ok := a.a.SelectedPair(); ok {
		t.Fatal("a pair without consent is still selected")
	}
}

func TestFailsWhenNothingWorks(t *testing.T) {
	n := newSimNet(t, 11)
	n.cut = true
	a, b := pairUp(t, n, Config{Controlling: true}, Config{})
	a.a.EndOfRemoteCandidates()
	if !n.run(20*time.Second, func() bool { return a.a.State() == StateFailed }) {
		t.Fatalf("A: %v", a.a.State())
	}
	if n.now.Sub(time.Unix(1_700_000_000, 0)) > 15*time.Second {
		t.Fatalf("A failed after %v despite end-of-candidates", n.now.Sub(time.Unix(1_700_000_000, 0)))
	}
	// B never heard end-of-candidates: it fails at FailedTimeout.
	if !n.run(30*time.Second, func() bool { return b.a.State() == StateFailed }) {
		t.Fatalf("B: %v", b.a.State())
	}
	// A candidate arriving later makes A check again.
	n.cut = false
	if err := a.a.AddRemoteCandidate(Candidate{Type: Host, Address: addrB6.Addr(), Port: 1, Priority: 1}, n.now); err != nil {
		t.Fatal(err)
	}
	if a.a.State() != StateFailed {
		t.Fatalf("a candidate with no pair restarted checks: %v", a.a.State())
	}
	if err := a.a.AddRemoteCandidate(Candidate{Type: Host, Address: addrB.Addr(), Port: addrB.Port() + 1, Priority: 1}, n.now); err != nil {
		t.Fatal(err)
	}
	if a.a.State() != StateChecking {
		t.Fatalf("a new pair did not restart checks: %v", a.a.State())
	}
	b.addSock(3, netip.AddrPortFrom(addrB.Addr(), addrB.Port()+1), netip.AddrPort{})
	if err := b.a.AddLocalCandidate(Candidate{Type: Host, Address: addrB.Addr(), Port: addrB.Port() + 1, LocalID: 3}, n.now); err != nil {
		t.Fatal(err)
	}
	if !n.run(10*time.Second, bothConnected(a, b)) {
		t.Fatalf("did not recover: %v %v", a.a.State(), b.a.State())
	}
	checkSelected(t, a, b)
}

func TestRequestValidation(t *testing.T) {
	var sent [][]byte
	a, err := NewAgent(Config{Ufrag: "aaaa", Pwd: "aaaaaaaaaaaaaaaaaaaaaa", Send: func(_ LocalID, _ netip.AddrPort, b []byte) {
		sent = append(sent, append([]byte(nil), b...))
	}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0)
	if err := a.AddLocalCandidate(Candidate{Type: Host, Address: addrA.Addr(), Port: addrA.Port(), LocalID: 1}, now); err != nil {
		t.Fatal(err)
	}
	build := func(user, pwd string, extra func(m *stun.Message)) []byte {
		var m stun.Message
		m.Build(stun.BindingRequest, stun.NewTransactionID())
		if user != "" {
			m.AddString(stun.AttrUsername, user)
		}
		m.AddUint32(stun.AttrPriority, 1)
		m.AddUint64(stun.AttrICEControlling, 1)
		if extra != nil {
			extra(&m)
		}
		if pwd != "" {
			m.AddIntegrity([]byte(pwd))
		}
		m.AddFingerprint()
		return m.Raw
	}
	for _, tc := range []struct {
		name string
		b    []byte
		code int
	}{
		{"ok", build("aaaa:bbbb", "aaaaaaaaaaaaaaaaaaaaaa", nil), 0},
		{"no username", build("", "aaaaaaaaaaaaaaaaaaaaaa", nil), stun.CodeBadRequest},
		{"no integrity", build("aaaa:bbbb", "", nil), stun.CodeBadRequest},
		{"wrong ufrag", build("aaab:bbbb", "aaaaaaaaaaaaaaaaaaaaaa", nil), stun.CodeUnauthorized},
		{"wrong password", build("aaaa:bbbb", "bbbbbbbbbbbbbbbbbbbbbb", nil), stun.CodeUnauthorized},
		{"unknown attribute", build("aaaa:bbbb", "aaaaaaaaaaaaaaaaaaaaaa", func(m *stun.Message) { m.AddFlag(0x0031) }), stun.CodeUnknownAttribute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sent = nil
			_ = a.HandlePacket(1, addrB, tc.b, now)
			if len(sent) != 1 {
				t.Fatalf("%d responses", len(sent))
			}
			var r stun.Message
			if err := r.Decode(sent[0]); err != nil {
				t.Fatal(err)
			}
			code, _, _ := r.ErrorCode()
			if code != tc.code {
				t.Fatalf("code %d, want %d", code, tc.code)
			}
			if tc.code == 0 {
				if r.CheckIntegrity([]byte("aaaaaaaaaaaaaaaaaaaaaa")) != nil {
					t.Fatal("success response not signed with the local password")
				}
				if m, _ := r.XORAddress(stun.AttrXORMappedAddress); m != addrB {
					t.Fatalf("XOR-MAPPED-ADDRESS %v", m)
				}
			}
		})
	}
	// A request with a broken fingerprint gets no answer.
	sent = nil
	b := build("aaaa:bbbb", "aaaaaaaaaaaaaaaaaaaaaa", nil)
	b[len(b)-1] ^= 1
	if err := a.HandlePacket(1, addrB, b, now); !errors.Is(err, stun.ErrFingerprint) || len(sent) != 0 {
		t.Fatalf("broken fingerprint: %v, %d responses", err, len(sent))
	}
	if err := a.HandlePacket(1, addrB, []byte("not stun"), now); err == nil {
		t.Fatal("garbage accepted")
	}
	a.Close()
	if err := a.HandlePacket(1, addrB, b, now); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, ok := a.NextTimeout(); ok {
		t.Fatal("closed agent has a timeout")
	}
}

func TestConfigErrors(t *testing.T) {
	send := func(LocalID, netip.AddrPort, []byte) {}
	if _, err := NewAgent(Config{}); err == nil {
		t.Fatal("no Send accepted")
	}
	if _, err := NewAgent(Config{Send: send, Ufrag: "ab"}); !errors.Is(err, ErrCredentials) {
		t.Fatal(err)
	}
	a, err := NewAgent(Config{Send: send, Lite: true, Controlling: true})
	if err != nil {
		t.Fatal(err)
	}
	if a.Controlling() {
		t.Fatal("a lite agent is controlling")
	}
	now := time.Unix(0, 0)
	if err := a.AddLocalCandidate(Candidate{Type: Relay, Address: addrA.Addr(), Port: 1}, now); !errors.Is(err, ErrLiteRelaying) {
		t.Fatal(err)
	}
	for _, c := range []Candidate{
		{Component: 2, Address: addrB.Addr(), Port: 1},
		{Transport: "tcp", Address: addrB.Addr(), Port: 1},
		{Hostname: "x.local", Port: 1},
	} {
		if err := a.AddRemoteCandidate(c, now); err == nil {
			t.Fatalf("accepted %+v", c)
		}
	}
	if err := a.SetRemoteCredentials("bad ufrag", "x", now); !errors.Is(err, ErrCredentials) {
		t.Fatal(err)
	}
}

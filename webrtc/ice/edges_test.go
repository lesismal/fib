package ice

import (
	"errors"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/webrtc/stun"
)

func TestStateString(t *testing.T) {
	for s, want := range map[State]string{
		StateNew:          "new",
		StateChecking:     "checking",
		StateConnected:    "connected",
		StateDisconnected: "disconnected",
		StateFailed:       "failed",
		StateClosed:       "closed",
		State(42):         "State(42)",
	} {
		if got := s.String(); got != want {
			t.Errorf("%d: %q, want %q", uint8(s), got, want)
		}
	}
}

// failingRand gives n zero bytes and then fails, as a Config.Rand that runs
// dry would.
type failingRand struct{ n int }

func (r *failingRand) Read(b []byte) (int, error) {
	if r.n <= 0 {
		return 0, errors.New("entropy exhausted")
	}
	k := min(len(b), r.n)
	clear(b[:k])
	r.n -= k
	return k, nil
}

func mustPanic(t *testing.T, what string, f func()) {
	t.Helper()
	defer func() {
		r := recover()
		if s, _ := r.(string); !strings.Contains(s, "ice: reading random bytes") {
			t.Fatalf("%s: recovered %v", what, r)
		}
	}()
	f()
}

// The agent's randomness is not optional: a Config.Rand that fails panics
// wherever it is read, rather than producing guessable credentials,
// tie-breakers or transaction IDs.
func TestFailingRand(t *testing.T) {
	send := func(LocalID, netip.AddrPort, []byte) {}
	mustPanic(t, "tie-breaker", func() {
		_, _ = NewAgent(Config{Send: send, Ufrag: "aaaa", Pwd: "aaaaaaaaaaaaaaaaaaaaaa", Rand: &failingRand{}})
	})
	mustPanic(t, "credentials", func() {
		_, _ = NewAgent(Config{Send: send, Rand: &failingRand{}})
	})
	a, err := NewAgent(Config{Send: send, Ufrag: "aaaa", Pwd: "aaaaaaaaaaaaaaaaaaaaaa", Rand: &failingRand{8},
		STUNServers: []netip.AddrPort{netip.MustParseAddrPort("192.0.2.1:3478")}})
	if err != nil {
		t.Fatal(err)
	}
	mustPanic(t, "transaction ID", func() {
		_ = a.Gather([]Socket{{ID: 1, Addrs: []netip.AddrPort{addrA}}}, time.Unix(0, 0))
	})
}

func TestClosedAgent(t *testing.T) {
	sent := 0
	a, err := NewAgent(Config{Send: func(LocalID, netip.AddrPort, []byte) { sent++ }})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0)
	var changes []State
	a.cfg.OnStateChange = func(s State) { changes = append(changes, s) }
	a.Close()
	a.Close()
	if a.State() != StateClosed || len(changes) != 1 {
		t.Fatalf("state %v, changes %v", a.State(), changes)
	}
	host := Candidate{Type: Host, Address: addrA.Addr(), Port: addrA.Port()}
	for name, err := range map[string]error{
		"Gather":               a.Gather([]Socket{{ID: 1, Addrs: []netip.AddrPort{addrA}}}, now),
		"AddLocalCandidate":    a.AddLocalCandidate(host, now),
		"AddRemoteCandidate":   a.AddRemoteCandidate(host, now),
		"SetRemoteCredentials": a.SetRemoteCredentials("peer", testRpwd, now),
		"Restart":              a.Restart("", "", now),
	} {
		if !errors.Is(err, ErrClosed) {
			t.Errorf("%s: %v", name, err)
		}
	}
	a.HandleTimeout(now.Add(time.Minute))
	if sent != 0 {
		t.Fatalf("a closed agent sent %d packets", sent)
	}
}

func TestAddLocalCandidateRules(t *testing.T) {
	var reported []Candidate
	a, err := NewAgent(Config{
		Send:        func(LocalID, netip.AddrPort, []byte) {},
		OnCandidate: func(c Candidate) { reported = append(reported, c) },
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0)
	for _, c := range []struct {
		c   Candidate
		err error
	}{
		{Candidate{Type: Host, Hostname: "x.local", Port: 1}, ErrUnresolved},
		{Candidate{Type: Host, Port: 1}, ErrUnresolved},
		{Candidate{Type: Host, Address: addrA.Addr(), Port: 1, Transport: "tcp"}, ErrUnsupportedTransport},
	} {
		if err := a.AddLocalCandidate(c.c, now); !errors.Is(err, c.err) {
			t.Fatalf("%+v: %v, want %v", c.c, err, c.err)
		}
	}
	if err := a.Gather([]Socket{{ID: 1, Addrs: []netip.AddrPort{{}}}}, now); !errors.Is(err, ErrUnresolved) {
		t.Fatalf("Gather of no address: %v", err)
	}
	if len(reported) != 0 {
		t.Fatalf("refused candidates reported: %v", reported)
	}

	// A candidate added twice is reported once.
	host := Candidate{Type: Host, Address: addrA.Addr(), Port: addrA.Port(), LocalID: 1}
	for range 2 {
		if err := a.AddLocalCandidate(host, now); err != nil {
			t.Fatal(err)
		}
	}
	if len(reported) != 1 || reported[0].Transport != "udp" || reported[0].Component != 1 {
		t.Fatalf("reported %+v", reported)
	}

	// Server-reflexive candidates share a foundation when their base and
	// type do (RFC 8445 section 5.1.1.3), whatever their public address.
	srflx := func(public string, base netip.AddrPort) string {
		p := netip.MustParseAddrPort(public)
		c := Candidate{Type: ServerReflexive, Address: p.Addr(), Port: p.Port(), RelatedAddress: base, LocalID: 1}
		if err := a.AddLocalCandidate(c, now); err != nil {
			t.Fatal(err)
		}
		return reported[len(reported)-1].Foundation
	}
	f1 := srflx("198.51.100.1:5000", addrA)
	f2 := srflx("198.51.100.2:6000", addrA)
	f3 := srflx("198.51.100.3:7000", addrB)
	if f1 != f2 || f1 == f3 || f1 == reported[0].Foundation {
		t.Fatalf("foundations %q %q %q, host %q", f1, f2, f3, reported[0].Foundation)
	}
}

// Gather asks each STUN server from a socket of its family only, and takes
// a server's MAPPED-ADDRESS when it sends no XOR-MAPPED-ADDRESS, as an RFC
// 3489 server does.
func TestGatherServerFamilies(t *testing.T) {
	v4server := netip.MustParseAddrPort("192.0.2.1:3478")
	v6server := netip.MustParseAddrPort("[2001:db8::1]:3478")
	var sent []sentPacket
	var cands []Candidate
	done := 0
	a, err := NewAgent(Config{
		STUNServers: []netip.AddrPort{v4server, v6server},
		Software:    "fib-test",
		Send: func(_ LocalID, to netip.AddrPort, b []byte) {
			var m stun.Message
			if err := m.Decode(append([]byte(nil), b...)); err != nil {
				t.Fatal(err)
			}
			sent = append(sent, sentPacket{to, m})
		},
		OnCandidate:     func(c Candidate) { cands = append(cands, c) },
		OnGatheringDone: func() { done++ },
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0)
	if err := a.Gather([]Socket{{ID: 1, Addrs: []netip.AddrPort{addrA}}}, now); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 || sent[0].to != v4server {
		t.Fatalf("an IPv4 socket asked %d servers", len(sent))
	}
	if s, _ := sent[0].m.Get(stun.AttrSoftware); string(s) != "fib-test" {
		t.Fatalf("SOFTWARE %q", s)
	}
	if err := a.Gather([]Socket{{ID: 2, Addrs: []netip.AddrPort{addrA6}}}, now); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 2 || sent[1].to != v6server {
		t.Fatalf("an IPv6 socket asked %v", sent[len(sent)-1].to)
	}
	public := netip.MustParseAddrPort("198.51.100.7:9000")
	var r stun.Message
	r.Build(stun.BindingSuccess, sent[0].m.TransactionID)
	r.AddAddress(stun.AttrMappedAddress, public)
	if err := a.HandlePacket(1, v4server, r.Raw, now); err != nil {
		t.Fatal(err)
	}
	// The IPv6 server sees the socket's own address: no candidate.
	r.Build(stun.BindingSuccess, sent[1].m.TransactionID)
	r.AddXORAddress(stun.AttrXORMappedAddress, addrA6)
	if err := a.HandlePacket(2, v6server, r.Raw, now); err != nil {
		t.Fatal(err)
	}
	var srflx []Candidate
	for _, c := range cands {
		if c.Type == ServerReflexive {
			srflx = append(srflx, c)
		}
	}
	if len(srflx) != 1 || srflx[0].AddrPort() != public || srflx[0].RelatedAddress != addrA {
		t.Fatalf("server-reflexive candidates %v", srflx)
	}
	if done != 1 {
		t.Fatalf("gathering done %d times", done)
	}

	// Checks carry SOFTWARE too.
	_ = a.SetRemoteCredentials(testRufrag, testRpwd, now)
	_ = a.AddRemoteCandidate(Candidate{Type: Host, Address: addrB.Addr(), Port: addrB.Port(), Priority: 1, Foundation: "1"}, now)
	a.HandleTimeout(now)
	last := sent[len(sent)-1]
	if s, _ := last.m.Get(stun.AttrSoftware); last.to != addrB || string(s) != "fib-test" {
		t.Fatalf("check to %v with SOFTWARE %q", last.to, s)
	}
}

// peerCheck builds a check the peer of checkAgent's agent sends it.
func peerCheck(a *Agent, prio uint32, extra func(m *stun.Message)) []byte {
	ufrag, pwd := a.LocalCredentials()
	var m stun.Message
	m.Build(stun.BindingRequest, stun.NewTransactionID())
	m.AddString(stun.AttrUsername, ufrag+":"+testRufrag)
	if prio != 0 {
		m.AddUint32(stun.AttrPriority, prio)
	}
	m.AddUint64(stun.AttrICEControlled, 1)
	if extra != nil {
		extra(&m)
	}
	m.AddIntegrity([]byte(pwd))
	m.AddFingerprint()
	return m.Raw
}

// peerResponse builds the peer's response to the check id: a success with
// mapped as XOR-MAPPED-ADDRESS if valid, or an error with code.
func peerResponse(id [stun.TransactionIDSize]byte, mapped netip.AddrPort, code int, pwd string) []byte {
	var m stun.Message
	if code != 0 {
		m.Build(stun.BindingError, id)
		m.AddErrorCode(code, "No")
	} else {
		m.Build(stun.BindingSuccess, id)
		if mapped.IsValid() {
			m.AddXORAddress(stun.AttrXORMappedAddress, mapped)
		}
	}
	m.AddIntegrity([]byte(pwd))
	m.AddFingerprint()
	return m.Raw
}

// lastCheck runs a's timers until it sends a check, and returns it.
func lastCheck(t *testing.T, a *Agent, sent *[]sentPacket, now *time.Time) stun.Message {
	t.Helper()
	before := len(*sent)
	for range 100 {
		*now = now.Add(a.cfg.Ta)
		a.HandleTimeout(*now)
		for i := len(*sent) - 1; i >= before; i-- {
			if (*sent)[i].m.Type == stun.BindingRequest {
				return (*sent)[i].m
			}
		}
	}
	t.Fatal("no check sent")
	return stun.Message{}
}

func TestCheckResponses(t *testing.T) {
	t.Run("forged", func(t *testing.T) {
		// A response that fails its integrity check is ignored and the
		// transaction goes on; the genuine one still counts.
		now := time.Unix(1_700_000_000, 0)
		var sent []sentPacket
		a := checkAgent(t, now, &sent)
		check := lastCheck(t, a, &sent, &now)
		p := a.pairIndex[pairKey{1, addrB}]
		_ = a.HandlePacket(1, addrB, peerResponse(check.TransactionID, addrA, 0, "forgedforgedforgedforged"), now)
		if p.state != pairInProgress {
			t.Fatalf("a forged response moved the pair to %d", p.state)
		}
		_ = a.HandlePacket(1, addrB, peerResponse(check.TransactionID, netip.AddrPort{}, 0, testRpwd), now)
		if p.state != pairSucceeded || p.local.Type != Host {
			t.Fatalf("pair %d, local %v", p.state, p.local.Type)
		}
	})
	t.Run("error", func(t *testing.T) {
		now := time.Unix(1_700_000_000, 0)
		var sent []sentPacket
		a := checkAgent(t, now, &sent)
		check := lastCheck(t, a, &sent, &now)
		_ = a.HandlePacket(1, addrB, peerResponse(check.TransactionID, netip.AddrPort{}, stun.CodeBadRequest, testRpwd), now)
		if p := a.pairIndex[pairKey{1, addrB}]; p.state != pairFailed {
			t.Fatalf("pair %d after a 400", p.state)
		}
	})
	t.Run("asymmetric", func(t *testing.T) {
		// RFC 8445 section 7.2.5.2.1: a response from elsewhere than the
		// check went fails the pair.
		now := time.Unix(1_700_000_000, 0)
		var sent []sentPacket
		a := checkAgent(t, now, &sent)
		check := lastCheck(t, a, &sent, &now)
		_ = a.HandlePacket(1, netip.MustParseAddrPort("10.0.0.3:3000"), peerResponse(check.TransactionID, addrA, 0, testRpwd), now)
		if p := a.pairIndex[pairKey{1, addrB}]; p.state != pairFailed {
			t.Fatalf("pair %d after an asymmetric response", p.state)
		}
	})
	t.Run("nomination refused", func(t *testing.T) {
		// A nomination answered with an error fails its pair and leaves
		// nothing nominated, and the peer-reflexive address the first
		// response revealed was learned against the host base, which the
		// nomination's response, revealing another, keeps.
		now := time.Unix(1_700_000_000, 0)
		var sent []sentPacket
		a := checkAgent(t, now, &sent)
		check := lastCheck(t, a, &sent, &now)
		nat1 := netip.MustParseAddrPort("198.51.100.1:1000")
		_ = a.HandlePacket(1, addrB, peerResponse(check.TransactionID, nat1, 0, testRpwd), now)
		p := a.pairIndex[pairKey{1, addrB}]
		if p.state != pairSucceeded || p.local.AddrPort() != nat1 {
			t.Fatalf("pair %d local %v", p.state, p.local.AddrPort())
		}
		nom := lastCheck(t, a, &sent, &now)
		if !nom.Contains(stun.AttrUseCandidate) || a.nominating != p {
			t.Fatal("no nomination")
		}
		nat2 := netip.MustParseAddrPort("198.51.100.1:2000")
		_ = a.HandlePacket(1, addrB, peerResponse(nom.TransactionID, nat2, 0, "forgedforgedforgedforged"), now)
		_ = a.HandlePacket(1, addrB, peerResponse(nom.TransactionID, netip.AddrPort{}, stun.CodeServerError, testRpwd), now)
		if p.state != pairFailed || p.nominate || a.nominating != nil {
			t.Fatalf("pair %d nominate %v nominating %v", p.state, p.nominate, a.nominating != nil)
		}
		if _, ok := a.SelectedPair(); ok {
			t.Fatal("a refused nomination selected its pair")
		}
	})
	t.Run("prflx rebased", func(t *testing.T) {
		now := time.Unix(1_700_000_000, 0)
		var sent []sentPacket
		a := checkAgent(t, now, &sent)
		check := lastCheck(t, a, &sent, &now)
		nat1 := netip.MustParseAddrPort("198.51.100.1:1000")
		_ = a.HandlePacket(1, addrB, peerResponse(check.TransactionID, nat1, 0, testRpwd), now)
		nom := lastCheck(t, a, &sent, &now)
		// The NAT moved the mapping: the new peer-reflexive candidate's
		// base is the socket's host candidate, not the old mapping.
		nat2 := netip.MustParseAddrPort("198.51.100.1:2000")
		_ = a.HandlePacket(1, addrB, peerResponse(nom.TransactionID, nat2, 0, testRpwd), now)
		sp, ok := a.SelectedPair()
		if !ok || sp.Local.AddrPort() != nat2 || sp.Local.Type != PeerReflexive || sp.Local.RelatedAddress != addrA {
			t.Fatalf("selected %v (%v), related %v", sp.Local.AddrPort(), ok, sp.Local.RelatedAddress)
		}
	})
}

func TestRequestEdges(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	var sent []sentPacket
	a := checkAgent(t, now, &sent)

	var m stun.Message
	m.Build(stun.MessageType{Method: stun.MethodAllocate, Class: stun.ClassRequest}, stun.NewTransactionID())
	if err := a.HandlePacket(1, addrB, m.Raw, now); !errors.Is(err, ErrNotBinding) {
		t.Fatalf("Allocate: %v", err)
	}
	m.Build(stun.BindingIndication, stun.NewTransactionID())
	m.AddFingerprint()
	if err := a.HandlePacket(1, addrB, m.Raw, now); err != nil || len(sent) != 0 {
		t.Fatalf("keepalive: %v, %d answers", err, len(sent))
	}
	if err := a.HandlePacket(1, addrB, peerCheck(a, 0, nil), now); !errors.Is(err, ErrRejected) {
		t.Fatalf("no PRIORITY: %v", err)
	}
	if code, _, _ := sent[0].m.ErrorCode(); code != stun.CodeBadRequest {
		t.Fatalf("no PRIORITY answered %d", code)
	}

	// A check on a transport with no candidate of the sender's family is
	// answered but pairs with nothing.
	pairs := len(a.pairs)
	if err := a.HandlePacket(1, addrB6, peerCheck(a, 1, nil), now); err != nil {
		t.Fatal(err)
	}
	if sent[len(sent)-1].m.Type != stun.BindingSuccess || len(a.pairs) != pairs {
		t.Fatalf("answer %v, %d pairs", sent[len(sent)-1].m.Type, len(a.pairs))
	}
}

// Past MaxPairs, a peer-reflexive pair of lower priority than every pair
// held is dropped as soon as made: its check is answered, not triggered.
func TestPrflxPairPruned(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	var sent []sentPacket
	a, err := NewAgent(Config{
		MaxPairs: 1,
		Send: func(_ LocalID, to netip.AddrPort, b []byte) {
			var m stun.Message
			_ = m.Decode(append([]byte(nil), b...))
			sent = append(sent, sentPacket{to, m})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = a.Gather([]Socket{{ID: 1, Addrs: []netip.AddrPort{addrA}}}, now)
	_ = a.SetRemoteCredentials(testRufrag, testRpwd, now)
	_ = a.AddRemoteCandidate(Candidate{Type: Host, Address: addrB.Addr(), Port: addrB.Port(), Priority: Priority(Host, 65535, 1), Foundation: "1"}, now)
	other := netip.MustParseAddrPort("10.0.0.3:3000")
	if err := a.HandlePacket(1, other, peerCheck(a, 1, nil), now); err != nil {
		t.Fatal(err)
	}
	if len(a.pairs) != 1 || a.pairs[0].key.remote != addrB || len(a.triggered) != 0 {
		t.Fatalf("%d pairs, %d triggered", len(a.pairs), len(a.triggered))
	}
	if sent[len(sent)-1].m.Type != stun.BindingSuccess || sent[len(sent)-1].to != other {
		t.Fatal("the check was not answered")
	}
}

func TestLateCheckAfterSelection(t *testing.T) {
	n := newSimNet(t, 21)
	a, b := pairUp(t, n, Config{Controlling: true}, Config{})
	if !n.run(5*time.Second, bothConnected(a, b)) {
		t.Fatal("not connected")
	}
	// A check from an address B never signaled, after B selected its
	// pair: it is answered, its peer-reflexive pair made, and no check
	// of B's own follows.
	other := netip.MustParseAddrPort("10.0.0.3:3000")
	var m stun.Message
	ufrag, pwd := b.a.LocalCredentials()
	ua, _ := a.a.LocalCredentials()
	m.Build(stun.BindingRequest, stun.NewTransactionID())
	m.AddString(stun.AttrUsername, ufrag+":"+ua)
	m.AddUint32(stun.AttrPriority, 1)
	m.AddUint64(stun.AttrICEControlling, 1<<63)
	m.AddIntegrity([]byte(pwd))
	m.AddFingerprint()
	if err := b.a.HandlePacket(2, other, m.Raw, n.now); err != nil {
		t.Fatal(err)
	}
	p := b.a.pairIndex[pairKey{2, other}]
	if p == nil || p.state != pairWaiting || p.queued {
		t.Fatalf("pair %+v", p)
	}
	if sp, _ := b.a.SelectedPair(); sp.Remote.AddrPort() != addrA {
		t.Fatalf("selection moved to %v", sp.Remote.AddrPort())
	}
}

func TestRemoteCandidateIdentity(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	var sent []sentPacket
	a := checkAgent(t, now, &sent)

	// A candidate of another ICE generation is ignored.
	stale := Candidate{Type: Host, Address: addrB.Addr(), Port: 7, Extensions: []Extension{{"ufrag", "old1"}}}
	if err := a.AddRemoteCandidate(stale, now); err != nil || len(a.remotes) != 1 {
		t.Fatalf("%v, %d remotes", err, len(a.remotes))
	}
	stale.Extensions[0].Value = testRufrag
	if err := a.AddRemoteCandidate(stale, now); err != nil || len(a.remotes) != 2 {
		t.Fatalf("%v, %d remotes", err, len(a.remotes))
	}

	// A peer-reflexive candidate signaled later takes its signaled
	// identity, keeping its pair.
	other := netip.MustParseAddrPort("10.0.0.3:3000")
	if err := a.HandlePacket(1, other, peerCheck(a, 5, nil), now); err != nil {
		t.Fatal(err)
	}
	p := a.pairIndex[pairKey{1, other}]
	if p == nil || p.remote.Type != PeerReflexive {
		t.Fatal("no peer-reflexive pair")
	}
	err := a.AddRemoteCandidate(Candidate{Type: Host, Address: other.Addr(), Port: other.Port(), Priority: 9, Foundation: "sig"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if a.pairIndex[pairKey{1, other}] != p || p.remote.Type != Host || !strings.HasSuffix(p.foundation, ":sig") || len(a.remotes) != 3 {
		t.Fatalf("remote %v foundation %q, %d remotes", p.remote.Type, p.foundation, len(a.remotes))
	}
}

func TestRestartRules(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	var sent []sentPacket
	a := checkAgent(t, now, &sent)
	check := lastCheck(t, a, &sent, &now)
	u, p := a.LocalCredentials()
	if err := a.Restart("x", "", now); !errors.Is(err, ErrCredentials) {
		t.Fatalf("short ufrag: %v", err)
	}
	if err := a.Restart(u, "", now); !errors.Is(err, ErrCredentials) {
		t.Fatalf("same ufrag: %v", err)
	}
	if u2, p2 := a.LocalCredentials(); u2 != u || p2 != p {
		t.Fatal("a refused restart changed the credentials")
	}
	if err := a.Restart("", "", now); err != nil {
		t.Fatal(err)
	}
	if a.State() != StateChecking || len(a.txs) != 0 {
		t.Fatalf("state %v, %d transactions", a.State(), len(a.txs))
	}
	// The check of before the restart is not sent again.
	before := len(sent)
	for range 50 {
		now = now.Add(100 * time.Millisecond)
		a.HandleTimeout(now)
	}
	for _, s := range sent[before:] {
		if s.m.TransactionID == check.TransactionID {
			t.Fatal("a check of before the restart was retransmitted")
		}
	}
}

// The peer restarting ICE alone, its new credentials signaled to an agent
// that keeps its own, makes the agent check again while the selected pair
// carries on, until a new pair is selected.
func TestPeerRestartsAlone(t *testing.T) {
	n := newSimNet(t, 22)
	a, b := pairUp(t, n, Config{Controlling: true}, Config{})
	if !n.run(5*time.Second, bothConnected(a, b)) {
		t.Fatal("not connected")
	}
	if err := a.a.Restart("", "", n.now); err != nil {
		t.Fatal(err)
	}
	ua, pa := a.a.LocalCredentials()
	if err := b.a.SetRemoteCredentials(ua, pa, n.now); err != nil {
		t.Fatal(err)
	}
	if !b.a.consentPaused || len(b.a.pairs) != 0 {
		t.Fatalf("B kept checking the old generation: %d pairs", len(b.a.pairs))
	}
	if _, ok := b.a.SelectedPair(); !ok {
		t.Fatal("B dropped its selected pair")
	}
	ub, pb := b.a.LocalCredentials()
	if err := a.a.SetRemoteCredentials(ub, pb, n.now); err != nil {
		t.Fatal(err)
	}
	signalCandidates(t, n.now, a, b)
	if !n.run(5*time.Second, func() bool { return len(a.selected) == 2 && len(b.selected) == 2 }) {
		t.Fatalf("no new selection: %d %d", len(a.selected), len(b.selected))
	}
	checkSelected(t, a, b)
}

func TestPairing(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	newAgent := func(cfg Config) (*Agent, *[]sentPacket) {
		sent := new([]sentPacket)
		cfg.Controlling = true
		cfg.Send = func(_ LocalID, to netip.AddrPort, b []byte) {
			var m stun.Message
			_ = m.Decode(append([]byte(nil), b...))
			*sent = append(*sent, sentPacket{to, m})
		}
		a, err := NewAgent(cfg)
		if err != nil {
			t.Fatal(err)
		}
		_ = a.SetRemoteCredentials(testRufrag, testRpwd, now)
		return a, sent
	}
	remote := func(port uint16, prio uint32, foundation string) Candidate {
		return Candidate{Type: Host, Address: addrB.Addr(), Port: port, Priority: prio, Foundation: foundation}
	}

	t.Run("one pair per base", func(t *testing.T) {
		// Two addresses of one socket pair with a remote candidate once,
		// through the higher-priority local candidate.
		a, _ := newAgent(Config{})
		other := netip.AddrPortFrom(netip.MustParseAddr("10.0.0.9"), addrA.Port())
		_ = a.Gather([]Socket{{ID: 1, Addrs: []netip.AddrPort{addrA, other}}}, now)
		_ = a.AddRemoteCandidate(remote(2000, 1, "r"), now)
		if len(a.pairs) != 1 || a.pairs[0].local.AddrPort() != addrA {
			t.Fatalf("%d pairs", len(a.pairs))
		}
		best := netip.AddrPortFrom(netip.MustParseAddr("10.0.0.7"), addrA.Port())
		_ = a.AddLocalCandidate(Candidate{Type: Host, Address: best.Addr(), Port: best.Port(), LocalID: 1, Priority: Priority(Host, 65535, 1) + 1}, now)
		if len(a.pairs) != 1 || a.pairs[0].local.AddrPort() != best || !strings.HasSuffix(a.pairs[0].foundation, ":r") {
			t.Fatalf("%d pairs, local %v", len(a.pairs), a.pairs[0].local.AddrPort())
		}
	})

	t.Run("frozen", func(t *testing.T) {
		// RFC 8838 section 11: a pair whose foundation another pair is
		// checking waits for it, Frozen, and is checked once that one
		// has failed.
		a, sent := newAgent(Config{})
		_ = a.Gather([]Socket{{ID: 1, Addrs: []netip.AddrPort{addrA}}}, now)
		_ = a.AddRemoteCandidate(remote(2000, 7, "f"), now)
		_ = a.AddRemoteCandidate(remote(2001, 7, "f"), now)
		if a.pairs[0].state != pairWaiting || a.pairs[1].state != pairFrozen {
			t.Fatalf("states %d %d", a.pairs[0].state, a.pairs[1].state)
		}
		first := lastCheck(t, a, sent, &now)
		to := (*sent)[len(*sent)-1].to
		for range 5 {
			now = now.Add(a.cfg.Ta)
			a.HandleTimeout(now)
		}
		for _, s := range *sent {
			if s.m.Type == stun.BindingRequest && s.to != to {
				t.Fatal("a Frozen pair was checked while its foundation was in progress")
			}
		}
		_ = a.HandlePacket(1, to, peerResponse(first.TransactionID, netip.AddrPort{}, stun.CodeServerError, testRpwd), now)
		lastCheck(t, a, sent, &now)
		if next := (*sent)[len(*sent)-1].to; next == to {
			t.Fatalf("checked %v again rather than the Frozen pair", next)
		}
	})

	t.Run("unfrozen by success", func(t *testing.T) {
		// A pair's success sets the pairs of its foundation Waiting.
		a, sent := newAgent(Config{})
		_ = a.Gather([]Socket{{ID: 1, Addrs: []netip.AddrPort{addrA}}}, now)
		_ = a.AddRemoteCandidate(remote(2000, 9, "f"), now)
		_ = a.AddRemoteCandidate(remote(2001, 7, "f"), now)
		first := lastCheck(t, a, sent, &now)
		_ = a.HandlePacket(1, a.pairs[0].key.remote, peerResponse(first.TransactionID, addrA, 0, testRpwd), now)
		if a.pairs[0].state != pairSucceeded || a.pairs[1].state != pairWaiting {
			t.Fatalf("states %d %d", a.pairs[0].state, a.pairs[1].state)
		}
	})

	t.Run("MaxPairs", func(t *testing.T) {
		a, _ := newAgent(Config{MaxPairs: 2})
		_ = a.Gather([]Socket{{ID: 1, Addrs: []netip.AddrPort{addrA}}}, now)
		_ = a.AddRemoteCandidate(remote(2000, 50, "a"), now)
		_ = a.AddRemoteCandidate(remote(2001, 10, "b"), now)
		_ = a.AddRemoteCandidate(remote(2002, 30, "c"), now)
		if len(a.pairs) != 2 || len(a.pairIndex) != 2 || a.pairs[0].key.remote.Port() != 2000 || a.pairs[1].key.remote.Port() != 2002 {
			t.Fatalf("kept %d pairs", len(a.pairs))
		}
	})

	t.Run("MaxPairs keeps a pair in progress", func(t *testing.T) {
		// The pair being checked stays however low its priority; a
		// peer-reflexive one above it goes instead.
		a, sent := newAgent(Config{MaxPairs: 1})
		_ = a.Gather([]Socket{{ID: 1, Addrs: []netip.AddrPort{addrA}}}, now)
		_ = a.AddRemoteCandidate(remote(2000, 1, "a"), now)
		lastCheck(t, a, sent, &now)
		if err := a.HandlePacket(1, netip.MustParseAddrPort("10.0.0.3:3000"), peerCheck(a, Priority(Host, 65535, 1), nil), now); err != nil {
			t.Fatal(err)
		}
		if len(a.pairs) != 1 || a.pairs[0].key.remote.Port() != 2000 || a.pairs[0].state != pairInProgress {
			t.Fatalf("kept %d pairs", len(a.pairs))
		}
	})

	t.Run("MaxPairs prunes a triggered pair", func(t *testing.T) {
		// A pair whose check the peer's cancelled is pruned off the
		// triggered queue, and the response to that check comes too late
		// to count.
		a, sent := newAgent(Config{MaxPairs: 2})
		_ = a.Gather([]Socket{{ID: 1, Addrs: []netip.AddrPort{addrA}}}, now)
		low := remote(2000, 1, "a")
		_ = a.AddRemoteCandidate(low, now)
		first := lastCheck(t, a, sent, &now)
		if err := a.HandlePacket(1, low.AddrPort(), peerCheck(a, 1, nil), now); err != nil {
			t.Fatal(err)
		}
		for i, prio := range []uint32{Priority(Host, 65535, 1), Priority(Host, 100, 1)} {
			from := netip.AddrPortFrom(addrB.Addr(), uint16(3000+i))
			if err := a.HandlePacket(1, from, peerCheck(a, prio, nil), now); err != nil {
				t.Fatal(err)
			}
		}
		if len(a.pairs) != 2 || a.pairIndex[pairKey{1, low.AddrPort()}] != nil {
			t.Fatalf("kept %d pairs", len(a.pairs))
		}
		for _, p := range a.triggered {
			if p.key.remote == low.AddrPort() {
				t.Fatal("the pruned pair is still triggered")
			}
		}
		if err := a.HandlePacket(1, low.AddrPort(), peerResponse(first.TransactionID, addrA, 0, testRpwd), now); err != nil {
			t.Fatal(err)
		}
		if a.selected != nil || len(a.pairs) != 2 {
			t.Fatalf("selected %v, %d pairs", a.selected, len(a.pairs))
		}
	})
}

// TestLocalPreferenceFloor ranks candidates past the 4096th all at the
// lowest local preference.
func TestLocalPreferenceFloor(t *testing.T) {
	var last Candidate
	a, err := NewAgent(Config{Send: func(LocalID, netip.AddrPort, []byte) {}, OnCandidate: func(c Candidate) { last = c }})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0)
	for i := range 4097 {
		if err := a.AddLocalCandidate(Candidate{Type: Host, Address: addrA.Addr(), Port: uint16(i + 1), LocalID: 1}, now); err != nil {
			t.Fatal(err)
		}
	}
	if want := Priority(Host, 0, 1); last.Priority != want {
		t.Fatalf("priority %d, want %d", last.Priority, want)
	}
}

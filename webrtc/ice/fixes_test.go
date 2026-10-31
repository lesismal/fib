package ice

import (
	"net/netip"
	"testing"
	"time"

	"github.com/lesismal/fib/webrtc/stun"
)

// TestPeerReflexiveLocal checks that an agent behind a NAT learns its own
// peer-reflexive candidate from a check's response (RFC 8445 section
// 7.2.5.3.1) and reports it as the local candidate of its selected pair,
// without signaling it.
func TestPeerReflexiveLocal(t *testing.T) {
	n := newSimNet(t, 5)
	a := n.newPeer("A", Config{Controlling: true}, 1)
	b := n.newPeer("B", Config{}, 2)
	sa := a.addSock(1, addrA, netip.AddrPort{})
	private := netip.MustParseAddrPort("192.168.1.2:2000")
	public := netip.MustParseAddrPort("203.0.113.7:40000")
	sb := b.addSock(2, private, public)
	_ = a.a.Gather([]Socket{sa}, n.now)
	_ = b.a.Gather([]Socket{sb}, n.now)
	exchangeCredentials(t, n.now, a, b)
	signalCandidates(t, n.now, a, b)
	if !n.run(5*time.Second, bothConnected(a, b)) {
		t.Fatalf("not connected: %v %v", a.a.State(), b.a.State())
	}
	checkSelected(t, a, b)
	p, _ := b.a.SelectedPair()
	if p.Local.Type != PeerReflexive || p.Local.AddrPort() != public || p.Local.RelatedAddress != private || p.Local.LocalID != 2 {
		t.Fatalf("B's local is %v (LocalID %d)", p.Local.String(), p.Local.LocalID)
	}
	if want := Priority(PeerReflexive, 65535, 1); p.Local.Priority != want {
		t.Fatalf("prflx priority %d, want the checks' %d", p.Local.Priority, want)
	}
	found := false
	for _, c := range b.a.LocalCandidates() {
		found = found || c.Type == PeerReflexive && c.AddrPort() == public
	}
	if !found {
		t.Fatal("LocalCandidates lacks the peer-reflexive candidate")
	}
	for _, c := range b.cands {
		if c.Type == PeerReflexive {
			t.Fatal("the peer-reflexive local candidate was signaled")
		}
	}
	// A, not behind a NAT, keeps its host candidate.
	if p, _ := a.a.SelectedPair(); p.Local.Type != Host {
		t.Fatalf("A's local is %v", p.Local.String())
	}
}

// TestServerReflexiveValidPair checks that a check's mapped address that is
// a known server-reflexive candidate makes that candidate the valid pair's
// local one rather than a new peer-reflexive candidate.
func TestServerReflexiveValidPair(t *testing.T) {
	n := newSimNet(t, 6)
	server := netip.MustParseAddrPort("198.51.100.1:3478")
	n.stunServer(server)
	a := n.newPeer("A", Config{Controlling: true}, 1)
	b := n.newPeer("B", Config{STUNServers: []netip.AddrPort{server}}, 2)
	sa := a.addSock(1, addrA, netip.AddrPort{})
	public := netip.MustParseAddrPort("203.0.113.2:50002")
	sb := b.addSock(2, netip.MustParseAddrPort("192.168.1.20:2000"), public)
	_ = a.a.Gather([]Socket{sa}, n.now)
	_ = b.a.Gather([]Socket{sb}, n.now)
	n.run(time.Second, func() bool { return b.gathered })
	exchangeCredentials(t, n.now, a, b)
	signalCandidates(t, n.now, a, b)
	if !n.run(5*time.Second, bothConnected(a, b)) {
		t.Fatalf("not connected: %v %v", a.a.State(), b.a.State())
	}
	p, _ := b.a.SelectedPair()
	if p.Local.Type != ServerReflexive || p.Local.AddrPort() != public {
		t.Fatalf("B's local is %v", p.Local.String())
	}
	for _, c := range b.a.LocalCandidates() {
		if c.Type == PeerReflexive {
			t.Fatalf("a peer-reflexive candidate %v duplicates the server-reflexive one", c.String())
		}
	}
}

// sentPacket is a datagram an agent sent.
type sentPacket struct {
	to netip.AddrPort
	m  stun.Message
}

// checkAgent is a controlling agent with one socket at addrA and one remote
// host candidate at addrB, recording what it sends.
func checkAgent(t *testing.T, now time.Time, sent *[]sentPacket) *Agent {
	t.Helper()
	a, err := NewAgent(Config{
		Controlling: true,
		Send: func(_ LocalID, to netip.AddrPort, b []byte) {
			var m stun.Message
			if err := m.Decode(append([]byte(nil), b...)); err != nil {
				t.Fatal(err)
			}
			*sent = append(*sent, sentPacket{to, m})
		},
		CheckMaxSends: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = a.Gather([]Socket{{ID: 1, Addrs: []netip.AddrPort{addrA}}}, now)
	_ = a.SetRemoteCredentials(testRufrag, testRpwd, now)
	_ = a.AddRemoteCandidate(Candidate{Type: Host, Address: addrB.Addr(), Port: addrB.Port(), Priority: Priority(Host, 65535, 1), Foundation: "1"}, now)
	return a
}

const testRufrag, testRpwd = "peer", "peerpasswordpeerpassword"

// TestCheckOnInProgressPair checks RFC 8445 section 7.3.1.4: a check from the
// peer on a pair whose own check is in flight cancels that transaction, which
// is then neither retransmitted nor failed by its timeout, and triggers a new
// check of the pair.
func TestCheckOnInProgressPair(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	var sent []sentPacket
	a := checkAgent(t, now, &sent)
	a.HandleTimeout(now)
	if len(sent) != 1 || sent[0].m.Type != stun.BindingRequest {
		t.Fatalf("sent %d packets, want one check", len(sent))
	}
	first := sent[0].m.TransactionID

	// The peer checks the same pair.
	now = now.Add(10 * time.Millisecond)
	ufrag, pwd := a.LocalCredentials()
	var req stun.Message
	req.Build(stun.BindingRequest, stun.NewTransactionID())
	req.AddString(stun.AttrUsername, ufrag+":"+testRufrag)
	req.AddUint32(stun.AttrPriority, Priority(PeerReflexive, 65535, 1))
	req.AddUint64(stun.AttrICEControlled, 1)
	req.AddIntegrity([]byte(pwd))
	req.AddFingerprint()
	if err := a.HandlePacket(1, addrB, req.Raw, now); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 2 || sent[1].m.Type != stun.BindingSuccess {
		t.Fatalf("the check was not answered")
	}
	p := a.pairIndex[pairKey{1, addrB}]
	if p.state != pairWaiting || !p.queued {
		t.Fatalf("pair state %d queued %v, want a triggered Waiting pair", p.state, p.queued)
	}
	now = now.Add(a.cfg.Ta)
	a.HandleTimeout(now)
	if len(sent) != 3 || sent[2].m.Type != stun.BindingRequest || sent[2].m.TransactionID == first {
		t.Fatalf("no new triggered check: %d packets", len(sent))
	}
	second := sent[2].m.TransactionID

	// Through the old transaction's whole life only the new one is sent
	// again, and the old one's timeout does not fail the pair while the
	// new one is in flight.
	retransmitted := false
	for range 80 {
		now = now.Add(50 * time.Millisecond)
		a.HandleTimeout(now)
		for _, s := range sent[3:] {
			if s.m.Type == stun.BindingRequest && s.m.TransactionID == first {
				t.Fatal("the cancelled check was retransmitted")
			}
			retransmitted = retransmitted || s.m.TransactionID == second
		}
		if p.state == pairFailed {
			if _, ok := a.txs[second]; ok {
				t.Fatal("the cancelled check's timeout failed the pair")
			}
			break
		}
	}
	if !retransmitted {
		t.Fatal("the triggered check was never retransmitted")
	}
}

// TestCancelledCheckResponseCounts checks that a response to a cancelled
// check, arriving before the transaction would have timed out, still makes
// the pair valid.
func TestCancelledCheckResponseCounts(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	var sent []sentPacket
	a := checkAgent(t, now, &sent)
	a.HandleTimeout(now)
	first := sent[0].m.TransactionID
	p := a.pairIndex[pairKey{1, addrB}]
	a.cancelChecks(p)
	var resp stun.Message
	resp.Build(stun.BindingSuccess, first)
	resp.AddXORAddress(stun.AttrXORMappedAddress, addrA)
	resp.AddIntegrity([]byte(testRpwd))
	resp.AddFingerprint()
	if err := a.HandlePacket(1, addrB, resp.Raw, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if p.state != pairSucceeded {
		t.Fatalf("pair state %d after the cancelled check's response", p.state)
	}
}

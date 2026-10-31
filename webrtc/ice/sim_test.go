package ice

import (
	"bytes"
	mathrand "math/rand/v2"
	"net/netip"
	"slices"
	"testing"
	"time"

	"github.com/lesismal/fib/webrtc/stun"
)

// simNet is an in-memory UDP network on a fake clock: datagrams take
// latency to arrive, may be lost, and a socket may sit behind a NAT that
// shows the network another address.
type simNet struct {
	t       testing.TB
	now     time.Time
	rng     *mathrand.Rand
	loss    float64
	latency time.Duration
	queue   []simPacket
	socks   map[netip.AddrPort]*simSock
	peers   []*simPeer
	servers map[netip.AddrPort]bool
	// cut drops every datagram while set.
	cut bool
}

type simPacket struct {
	at       time.Time
	from, to netip.AddrPort
	b        []byte
}

type simSock struct {
	peer   *simPeer
	id     LocalID
	addr   netip.AddrPort
	public netip.AddrPort
}

type simPeer struct {
	name     string
	net      *simNet
	a        *Agent
	socks    map[LocalID]*simSock
	states   []State
	selected []Pair
	cands    []Candidate
	gathered bool
	// remote, when set, receives each local candidate as it is found.
	remote *simPeer
}

func newSimNet(t testing.TB, seed uint64) *simNet {
	return &simNet{
		t:       t,
		now:     time.Unix(1_700_000_000, 0),
		rng:     mathrand.New(mathrand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		latency: 10 * time.Millisecond,
		socks:   map[netip.AddrPort]*simSock{},
		servers: map[netip.AddrPort]bool{},
	}
}

// seededRand is a deterministic io.Reader for an agent's randomness.
type seededRand struct{ r *mathrand.ChaCha8 }

func (s seededRand) Read(b []byte) (int, error) { return s.r.Read(b) }

func (n *simNet) newPeer(name string, cfg Config, seed byte) *simPeer {
	p := &simPeer{name: name, net: n, socks: map[LocalID]*simSock{}}
	var key [32]byte
	key[0] = seed
	cfg.Rand = seededRand{mathrand.NewChaCha8(key)}
	cfg.Send = func(local LocalID, to netip.AddrPort, b []byte) {
		s := p.socks[local]
		if s == nil {
			n.t.Fatalf("%s sent from unknown LocalID %d", name, local)
		}
		from := s.addr
		if s.public.IsValid() {
			from = s.public
		}
		n.send(from, to, b)
	}
	cfg.OnStateChange = func(s State) { p.states = append(p.states, s) }
	cfg.OnSelectedPair = func(sp Pair) { p.selected = append(p.selected, sp) }
	cfg.OnCandidate = func(c Candidate) {
		p.cands = append(p.cands, c)
		if p.remote != nil {
			if err := p.remote.a.AddRemoteCandidate(c, n.now); err != nil {
				n.t.Fatalf("%s: AddRemoteCandidate(%v): %v", p.remote.name, c.String(), err)
			}
		}
	}
	cfg.OnGatheringDone = func() { p.gathered = true }
	a, err := NewAgent(cfg)
	if err != nil {
		n.t.Fatal(err)
	}
	p.a = a
	n.peers = append(n.peers, p)
	return p
}

// addSock gives p a socket at addr, behind a NAT showing public if valid.
func (p *simPeer) addSock(id LocalID, addr, public netip.AddrPort) Socket {
	s := &simSock{peer: p, id: id, addr: addr, public: public}
	p.socks[id] = s
	if public.IsValid() {
		p.net.socks[public] = s
	} else {
		p.net.socks[addr] = s
	}
	return Socket{ID: id, Addrs: []netip.AddrPort{addr}}
}

func (n *simNet) send(from, to netip.AddrPort, b []byte) {
	if n.cut || n.loss > 0 && n.rng.Float64() < n.loss {
		return
	}
	n.queue = append(n.queue, simPacket{n.now.Add(n.latency), from, to, bytes.Clone(b)})
}

// stunServer answers Binding requests to addr with the address they came
// from, as a STUN server does.
func (n *simNet) stunServer(addr netip.AddrPort) { n.servers[addr] = true }

func (n *simNet) deliver(pkt simPacket) {
	if n.servers[pkt.to] {
		var m stun.Message
		if m.Decode(pkt.b) != nil || m.Type != stun.BindingRequest {
			return
		}
		var r stun.Message
		r.Build(stun.BindingSuccess, m.TransactionID)
		r.AddXORAddress(stun.AttrXORMappedAddress, pkt.from)
		r.AddFingerprint()
		n.send(pkt.to, pkt.from, r.Raw)
		return
	}
	s := n.socks[pkt.to]
	if s == nil {
		return
	}
	if err := s.peer.a.HandlePacket(s.id, pkt.from, pkt.b, n.now); err != nil && err != ErrRejected {
		n.t.Logf("%s: HandlePacket: %v", s.peer.name, err)
	}
}

// run advances the clock, delivering datagrams and firing timers, until
// done reports true or limit has passed; it reports whether done did.
func (n *simNet) run(limit time.Duration, done func() bool) bool {
	end := n.now.Add(limit)
	spins := 0
	for !done() {
		var next time.Time
		for _, pkt := range n.queue {
			if next.IsZero() || pkt.at.Before(next) {
				next = pkt.at
			}
		}
		for _, p := range n.peers {
			if t, ok := p.a.NextTimeout(); ok && (next.IsZero() || t.Before(next)) {
				next = t
			}
		}
		if next.IsZero() || next.After(end) {
			n.now = end
			return done()
		}
		if next.After(n.now) {
			n.now = next
			spins = 0
		} else if spins++; spins > 10000 {
			n.t.Fatalf("no progress at %v", n.now)
		}
		slices.SortStableFunc(n.queue, func(x, y simPacket) int { return x.at.Compare(y.at) })
		for len(n.queue) > 0 && !n.queue[0].at.After(n.now) {
			pkt := n.queue[0]
			n.queue = n.queue[1:]
			n.deliver(pkt)
		}
		for _, p := range n.peers {
			if t, ok := p.a.NextTimeout(); ok && !t.After(n.now) {
				p.a.HandleTimeout(n.now)
			}
		}
	}
	return true
}

// exchangeCredentials signals each peer's ufrag and password to the other.
func exchangeCredentials(t testing.TB, now time.Time, x, y *simPeer) {
	t.Helper()
	u, p := x.a.LocalCredentials()
	if err := y.a.SetRemoteCredentials(u, p, now); err != nil {
		t.Fatal(err)
	}
	u, p = y.a.LocalCredentials()
	if err := x.a.SetRemoteCredentials(u, p, now); err != nil {
		t.Fatal(err)
	}
}

// signalCandidates gives each peer the other's candidates so far.
func signalCandidates(t testing.TB, now time.Time, x, y *simPeer) {
	t.Helper()
	for _, c := range x.a.LocalCandidates() {
		if err := y.a.AddRemoteCandidate(c, now); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range y.a.LocalCandidates() {
		if err := x.a.AddRemoteCandidate(c, now); err != nil {
			t.Fatal(err)
		}
	}
}

func bothConnected(x, y *simPeer) func() bool {
	return func() bool {
		return x.a.State() == StateConnected && y.a.State() == StateConnected
	}
}

// checkSelected checks the two peers selected matching pairs: each one's
// remote is where the other's socket is seen from.
func checkSelected(t testing.TB, x, y *simPeer) {
	t.Helper()
	px, ok1 := x.a.SelectedPair()
	py, ok2 := y.a.SelectedPair()
	if !ok1 || !ok2 {
		t.Fatalf("selected: %v %v", ok1, ok2)
	}
	seen := func(p *simPeer, id LocalID) netip.AddrPort {
		s := p.socks[id]
		if s.public.IsValid() {
			return s.public
		}
		return s.addr
	}
	if px.Remote.AddrPort() != seen(y, py.Local.LocalID) || py.Remote.AddrPort() != seen(x, px.Local.LocalID) {
		t.Fatalf("pairs do not match: %s %v->%v, %s %v->%v",
			x.name, px.Local.LocalID, px.Remote.AddrPort(), y.name, py.Local.LocalID, py.Remote.AddrPort())
	}
	if x.a.Controlling() == y.a.Controlling() {
		t.Fatalf("both agents controlling=%v", x.a.Controlling())
	}
}

var (
	addrA  = netip.MustParseAddrPort("10.0.0.1:1000")
	addrA6 = netip.MustParseAddrPort("[fd00::1]:1000")
	addrB  = netip.MustParseAddrPort("10.0.0.2:2000")
	addrB6 = netip.MustParseAddrPort("[fd00::2]:2000")
)

// pairUp makes two peers with one IPv4 socket each, gathers, and signals
// their credentials and candidates.
func pairUp(t testing.TB, n *simNet, ca, cb Config) (*simPeer, *simPeer) {
	a := n.newPeer("A", ca, 1)
	b := n.newPeer("B", cb, 2)
	sa := a.addSock(1, addrA, netip.AddrPort{})
	sb := b.addSock(2, addrB, netip.AddrPort{})
	if err := a.a.Gather([]Socket{sa}, n.now); err != nil {
		t.Fatal(err)
	}
	if err := b.a.Gather([]Socket{sb}, n.now); err != nil {
		t.Fatal(err)
	}
	exchangeCredentials(t, n.now, a, b)
	signalCandidates(t, n.now, a, b)
	return a, b
}

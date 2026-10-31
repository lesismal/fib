package turn

import (
	"bytes"
	mathrand "math/rand/v2"
	"net/netip"
	"slices"
	"testing"
	"time"
)

// timer is anything sans-IO with a clock: a Client, a Server, an ice.Agent.
type timer interface {
	NextTimeout() (time.Time, bool)
	HandleTimeout(now time.Time)
}

// simNet is an in-memory UDP network on a fake clock: datagrams take
// latency to arrive and may be lost.
type simNet struct {
	t       testing.TB
	now     time.Time
	rng     *mathrand.Rand
	loss    float64
	latency time.Duration
	queue   []simPacket
	at      map[netip.AddrPort]func(from netip.AddrPort, b []byte)
	timers  []timer
	// drop, if set, drops the datagrams it reports true for.
	drop func(from, to netip.AddrPort, b []byte) bool
}

type simPacket struct {
	at       time.Time
	from, to netip.AddrPort
	b        []byte
}

func newSimNet(t testing.TB, seed uint64) *simNet {
	return &simNet{
		t:       t,
		now:     time.Unix(1_700_000_000, 0),
		rng:     mathrand.New(mathrand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		latency: 10 * time.Millisecond,
		at:      map[netip.AddrPort]func(netip.AddrPort, []byte){},
	}
}

func (n *simNet) send(from, to netip.AddrPort, b []byte) {
	if n.loss > 0 && n.rng.Float64() < n.loss {
		return
	}
	if n.drop != nil && n.drop(from, to, b) {
		return
	}
	n.queue = append(n.queue, simPacket{n.now.Add(n.latency), from, to, bytes.Clone(b)})
}

// run advances the clock, delivering datagrams and firing timers, until done
// reports true or limit has passed; it reports whether done did.
func (n *simNet) run(limit time.Duration, done func() bool) bool {
	end := n.now.Add(limit)
	spins := 0
	for done == nil || !done() {
		var next time.Time
		for _, pkt := range n.queue {
			if next.IsZero() || pkt.at.Before(next) {
				next = pkt.at
			}
		}
		for _, tm := range n.timers {
			if t, ok := tm.NextTimeout(); ok && (next.IsZero() || t.Before(next)) {
				next = t
			}
		}
		if next.IsZero() || next.After(end) {
			n.now = end
			return done != nil && done()
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
			if f := n.at[pkt.to]; f != nil {
				f(pkt.from, pkt.b)
			}
		}
		for _, tm := range n.timers {
			if t, ok := tm.NextTimeout(); ok && !t.After(n.now) {
				tm.HandleTimeout(n.now)
			}
		}
	}
	return true
}

var (
	serverAddr = netip.MustParseAddrPort("203.0.113.1:3478")
	relayIP    = netip.MustParseAddr("203.0.113.1")
	clientAddr = netip.MustParseAddrPort("198.51.100.10:40000")
	peerAddr   = netip.MustParseAddrPort("192.0.2.7:5000")
)

const (
	testRealm = "fib.test"
	testUser  = "alice"
	testPass  = "secret"
)

// memRelay is a relayed address on the simulated network.
type memRelay struct {
	h      *simServer
	addr   netip.AddrPort
	closed bool
}

func (r *memRelay) Addr() netip.AddrPort { return r.addr }
func (r *memRelay) SendTo(peer netip.AddrPort, b []byte) {
	if r.closed {
		r.h.n.t.Fatal("send on a closed relay")
	}
	r.h.n.send(r.addr, peer, b)
}
func (r *memRelay) Close() {
	r.closed = true
	delete(r.h.n.at, r.addr)
}

// simServer runs a Server at an address of the simulated network.
type simServer struct {
	n      *simNet
	s      *Server
	addr   netip.AddrPort
	relays []*memRelay
	port   uint16
}

func newSimServer(n *simNet, addr netip.AddrPort, cfg ServerConfig) *simServer {
	h := &simServer{n: n, addr: addr, port: 50000}
	if cfg.Realm == "" {
		cfg.Realm = testRealm
	}
	if cfg.Auth == nil {
		cfg.Auth = StaticAuth(testRealm, map[string]string{testUser: testPass, "bob": "hunter2"})
	}
	cfg.Send = func(t FiveTuple, b []byte) { n.send(t.Server, t.Client, b) }
	cfg.AllocateRelay = func(t FiveTuple, user string, ipv6 bool, done func(Relay, error)) {
		if ipv6 {
			done(nil, ErrNoRelay)
			return
		}
		r := &memRelay{h: h, addr: netip.AddrPortFrom(addr.Addr(), h.port)}
		h.port++
		h.relays = append(h.relays, r)
		n.at[r.addr] = func(from netip.AddrPort, b []byte) {
			_ = h.s.HandleRelayPacket(r.addr, from, b, n.now)
		}
		done(r, nil)
	}
	s, err := NewServer(cfg)
	if err != nil {
		n.t.Fatal(err)
	}
	h.s = s
	n.at[addr] = func(from netip.AddrPort, b []byte) {
		_ = s.HandlePacket(FiveTuple{Client: from, Server: addr}, b, n.now)
	}
	n.timers = append(n.timers, s)
	return h
}

// received is a datagram a client got from a peer through the relay.
type received struct {
	peer netip.AddrPort
	data string
}

// simClient runs a Client at an address of the simulated network.
type simClient struct {
	n         *simNet
	c         *Client
	addr      netip.AddrPort
	got       []received
	allocated bool
	failed    error
	// raw counts the server's datagrams by their first byte's kind:
	// ChannelData or STUN.
	channelData int
}

func newSimClient(n *simNet, addr netip.AddrPort, cfg ClientConfig) *simClient {
	h := &simClient{n: n, addr: addr}
	if !cfg.Server.IsValid() {
		cfg.Server = serverAddr
	}
	if cfg.Username == "" {
		cfg.Username, cfg.Password = testUser, testPass
	}
	cfg.Send = func(to netip.AddrPort, b []byte) { n.send(addr, to, b) }
	cfg.OnAllocated = func(relayed, mapped netip.AddrPort) { h.allocated = true }
	cfg.OnFailed = func(err error) { h.failed = err }
	c, err := NewClient(cfg)
	if err != nil {
		n.t.Fatal(err)
	}
	h.c = c
	n.at[addr] = func(from netip.AddrPort, b []byte) {
		if IsChannelData(b) {
			h.channelData++
		}
		peer, data, ok := c.HandlePacket(from, b, n.now)
		if !ok {
			n.t.Errorf("client: a datagram from %v was not the server's", from)
		}
		if data != nil {
			h.got = append(h.got, received{peer, string(data)})
		}
	}
	n.timers = append(n.timers, c)
	return h
}

func (h *simClient) allocate(t testing.TB) {
	t.Helper()
	if err := h.c.Allocate(h.n.now); err != nil {
		t.Fatal(err)
	}
	if !h.n.run(time.Minute, func() bool { return h.allocated || h.failed != nil }) || h.failed != nil {
		t.Fatalf("allocation failed: %v", h.failed)
	}
}

// simPeer is a plain UDP endpoint that records what it gets and may echo
// it back.
type simPeer struct {
	got  []received
	echo bool
}

func newSimPeer(n *simNet, addr netip.AddrPort, echo bool) *simPeer {
	p := &simPeer{echo: echo}
	n.at[addr] = func(from netip.AddrPort, b []byte) {
		p.got = append(p.got, received{from, string(b)})
		if p.echo {
			n.send(addr, from, append([]byte("echo:"), b...))
		}
	}
	return p
}

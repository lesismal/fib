package dht

import (
	"context"
	"crypto/ed25519"
	"errors"
	"math/rand/v2"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

// simNet is an in-memory network of Nodes: Send queues a datagram, which a
// few goroutines deliver to the node at its address, dropping a share of
// them when loss is set.
type simNet struct {
	mu    sync.Mutex
	nodes map[netip.AddrPort]*Node
	rng   *rand.Rand
	loss  float64
	sent  int
	queue chan simPacket
	wg    sync.WaitGroup
}

type simPacket struct {
	from, to netip.AddrPort
	b        []byte
}

func newSimNet(t testing.TB, seed uint64) *simNet {
	s := &simNet{
		nodes: map[netip.AddrPort]*Node{},
		rng:   rand.New(rand.NewPCG(seed, seed)),
		queue: make(chan simPacket, 1<<16),
	}
	for range 4 {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for p := range s.queue {
				s.mu.Lock()
				n := s.nodes[p.to]
				s.mu.Unlock()
				if n != nil {
					n.HandleDatagram(p.from, p.b)
				}
			}
		}()
	}
	t.Cleanup(func() {
		s.mu.Lock()
		nodes := s.nodes
		s.nodes = nil
		s.mu.Unlock()
		for _, n := range nodes {
			_ = n.Close()
		}
		close(s.queue)
		s.wg.Wait()
	})
	return s
}

// simAddr is the address of the i-th node: public-looking, one node an IP,
// as on the real DHT.
func simAddr(i int) netip.AddrPort {
	return netip.AddrPortFrom(netip.AddrFrom4([4]byte{byte(11 + i/65536), byte(i >> 8), byte(i), 7}), uint16(6881+i%7))
}

// add makes a node at addr. Its ID is secure for the address (BEP 42), so
// that the votes of the "ip" key do not make it change.
func (s *simNet) add(t testing.TB, a netip.AddrPort, cfg Config) *Node {
	cfg.Send = func(to netip.AddrPort, b []byte) {
		s.mu.Lock()
		drop := s.nodes == nil || s.loss > 0 && s.rng.Float64() < s.loss
		s.sent++
		s.mu.Unlock()
		if !drop {
			s.queue <- simPacket{from: a, to: to, b: b}
		}
	}
	if cfg.Routers == nil {
		cfg.Routers = []string{}
	}
	if !cfg.PublicIP.IsValid() {
		cfg.PublicIP = a.Addr()
	}
	if cfg.QueryTimeout == 0 {
		cfg.QueryTimeout = 300 * time.Millisecond
	}
	if cfg.QueryRate == 0 {
		cfg.QueryRate = -1
	}
	n, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.nodes[a] = n
	s.mu.Unlock()
	return n
}

func (s *simNet) intn(n int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rng.IntN(n)
}

func (s *simNet) setLoss(p float64) {
	s.mu.Lock()
	s.loss = p
	s.mu.Unlock()
}

// build makes a network of size nodes, each bootstrapped from the first
// and a few that joined before it, many at a time.
func (s *simNet) build(t testing.TB, size int, cfg Config) []*Node {
	nodes := make([]*Node, size)
	for i := range nodes {
		nodes[i] = s.add(t, simAddr(i), cfg)
	}
	for i := 1; i < size; i++ {
		seeds := []NodeInfo{{Addr: simAddr(0)}}
		for range 2 {
			seeds = append(seeds, NodeInfo{Addr: simAddr(s.intn(i))})
		}
		nodes[i].AddNodes(seeds...)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const batch = 25
	for i := 1; i < size; i += batch {
		var wg sync.WaitGroup
		for j := i; j < min(i+batch, size); j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := nodes[j].Bootstrap(ctx); err != nil {
					t.Errorf("bootstrap %d: %v", j, err)
				}
			}()
		}
		wg.Wait()
	}
	return nodes
}

// trueClosest returns the k IDs of nodes closest to target, but for
// except, which a lookup of its own never returns.
func trueClosest(nodes []*Node, except *Node, target ID, k int) []ID {
	all := make([]NodeInfo, 0, len(nodes))
	for i, n := range nodes {
		if n != except {
			all = append(all, NodeInfo{ID: n.ID(), Addr: simAddr(i)})
		}
	}
	sortNodes(target, all)
	ids := make([]ID, k)
	for i := range ids {
		ids[i] = all[i].ID
	}
	return ids
}

func overlap(a []NodeInfo, b []ID) int {
	n := 0
	for _, x := range a {
		for _, y := range b {
			if x.ID == y {
				n++
			}
		}
	}
	return n
}

const simSize = 300

func TestSimLookupConverges(t *testing.T) {
	s := newSimNet(t, 1)
	nodes := s.build(t, simSize, Config{})
	tableSizes := 0
	for _, n := range nodes {
		tableSizes += n.Stats().Nodes
	}
	if avg := tableSizes / len(nodes); avg < 2*K {
		t.Fatalf("average routing table of %d nodes", avg)
	}
	ctx := context.Background()
	total := 0
	const lookups = 20
	for i := range lookups {
		target := RandomID()
		from := nodes[s.intn(len(nodes))]
		found, err := from.FindNode(ctx, target)
		if err != nil {
			t.Fatal(err)
		}
		want := trueClosest(nodes, from, target, K)
		if len(found) == 0 || found[0].ID != want[0] {
			holders := 0
			for _, n := range nodes {
				for _, ni := range n.Nodes() {
					if ni.ID == want[0] {
						holders++
					}
				}
			}
			var sent, timeouts int64
			for _, n := range nodes {
				st := n.Stats()
				sent += st.QueriesSent
				timeouts += st.QueryTimeouts
			}
			t.Errorf("lookup %d: closest %v, want %s, in %d tables; %d of %d queries timed out", i, found, want[0], holders, timeouts, sent)
		}
		total += overlap(found, want)
	}
	if total < lookups*K*9/10 {
		t.Fatalf("lookups found %d of the %d true closest", total, lookups*K)
	}
}

func TestSimAnnounceGetPeers(t *testing.T) {
	s := newSimNet(t, 2)
	nodes := s.build(t, simSize, Config{})
	ctx := context.Background()
	ih := metainfo.InfoHash(RandomID())
	if n, err := nodes[10].Announce(ctx, ih, 7777, false, nil); err != nil || n < K/2 {
		t.Fatalf("announce: %d nodes, %v", n, err)
	}
	if n, err := nodes[20].Announce(ctx, ih, 0, true, nil); err != nil || n < K/2 {
		t.Fatalf("implied-port announce: %d nodes, %v", n, err)
	}
	var got []netip.AddrPort
	if err := nodes[200].GetPeers(ctx, ih, func(p []netip.AddrPort) { got = append(got, p...) }); err != nil {
		t.Fatal(err)
	}
	want := map[netip.AddrPort]bool{
		netip.AddrPortFrom(simAddr(10).Addr(), 7777): true,
		simAddr(20): true, // implied: the DHT's own port
	}
	for _, p := range got {
		delete(want, p)
	}
	if len(want) > 0 {
		t.Fatalf("get_peers found %v, missing %v", got, want)
	}
	// BEP 51: a node that stores the torrent samples it.
	var holder *Node
	for _, n := range nodes {
		if n.Stats().Torrents > 0 {
			holder = n
			break
		}
	}
	idx := s.add(t, simAddr(simSize+1), Config{})
	for i, n := range nodes {
		if n == holder {
			sm, err := idx.SampleInfohashes(ctx, simAddr(i), RandomID())
			if err != nil {
				t.Fatal(err)
			}
			if sm.Num != 1 || len(sm.InfoHashes) != 1 || sm.InfoHashes[0] != ih || len(sm.Nodes) == 0 || sm.Interval == 0 {
				t.Fatalf("samples %+v", sm)
			}
		}
	}
}

func TestSimLoss(t *testing.T) {
	s := newSimNet(t, 3)
	nodes := s.build(t, 200, Config{})
	s.setLoss(0.2)
	ctx := context.Background()
	for round := range 3 {
		ih := metainfo.InfoHash(RandomID())
		a, b := nodes[s.intn(len(nodes))], nodes[s.intn(len(nodes))]
		if n, err := a.Announce(ctx, ih, 1000+round, false, nil); err != nil || n == 0 {
			t.Fatalf("announce under loss: %d, %v", n, err)
		}
		found := false
		if err := b.GetPeers(ctx, ih, func(p []netip.AddrPort) {
			for _, x := range p {
				found = found || x.Port() == uint16(1000+round)
			}
		}); err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("round %d: peer not found under 20%% loss", round)
		}
	}
}

func TestSimItems(t *testing.T) {
	s := newSimNet(t, 4)
	nodes := s.build(t, 150, Config{})
	ctx := context.Background()

	im, _ := NewImmutableItem(map[string]any{"hello": "world", "n": 7})
	if n, err := nodes[3].Put(ctx, im); err != nil || n < K/2 {
		t.Fatalf("put immutable: %d, %v", n, err)
	}
	got, err := nodes[100].Get(ctx, im.Target())
	if err != nil || string(got.V) != string(im.V) {
		t.Fatalf("get immutable: %v %v", got, err)
	}
	if _, err := nodes[100].Get(ctx, RandomID()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing item: %v", err)
	}

	_, priv, _ := ed25519.GenerateKey(nil)
	salt := []byte("profile")
	m1, _ := NewMutableItem("first", salt, 1, priv)
	if n, err := nodes[5].Put(ctx, m1); err != nil || n < K/2 {
		t.Fatalf("put mutable: %d, %v", n, err)
	}
	m2, _ := NewMutableItem("second", salt, 2, priv)
	if n, err := nodes[6].Put(ctx, m2); err != nil || n < K/2 {
		t.Fatalf("put mutable seq 2: %d, %v", n, err)
	}
	got, err = nodes[120].GetMutable(ctx, m1.K, salt)
	if err != nil || got.Seq != 2 || string(got.V) != "6:second" {
		t.Fatalf("get mutable: %+v %v", got, err)
	}
	// An older sequence number is refused by a node that has the newer.
	for i, n := range nodes {
		if n.Stats().Items == 0 {
			continue
		}
		n.mu.Lock()
		held := n.items.get(m1.Target(), time.Now())
		n.mu.Unlock()
		if held == nil || held.Seq != 2 {
			continue
		}
		m, err := nodes[7].call(ctx, simAddr(i), "get", &args{Target: m1.Target().Bytes()})
		if err != nil {
			t.Fatal(err)
		}
		seq := m1.Seq
		_, err = nodes[7].call(ctx, simAddr(i), "put", &args{Token: m.R.Token, V: m1.V, K: m1.K[:], Sig: m1.Sig[:], Salt: salt, Seq: &seq})
		var e *Error
		if !errors.As(err, &e) || e.Code != ErrCodeSeqTooLow {
			t.Fatalf("stale put: %v", err)
		}
		break
	}
	_, _ = nodes[7].Put(ctx, m1)
	if got, err = nodes[130].GetMutable(ctx, m1.K, salt); err != nil || got.Seq != 2 {
		t.Fatalf("after a stale put: %+v %v", got, err)
	}
}

// Nodes that enforce BEP 42 leave out nodes whose IDs do not match their
// addresses, and still find each other.
func TestSimEnforceNodeID(t *testing.T) {
	s := newSimNet(t, 5)
	nodes := s.build(t, 100, Config{EnforceNodeID: true})
	ctx := context.Background()
	rogue := s.add(t, simAddr(500), Config{ID: RandomID()})
	rogue.AddNodes(NodeInfo{Addr: simAddr(0)})
	_ = rogue.Bootstrap(ctx)
	for _, n := range nodes {
		for _, ni := range n.Nodes() {
			if ni.ID == rogue.ID() {
				t.Fatal("insecure node in a table of a node that enforces BEP 42")
			}
		}
	}
	found, err := nodes[50].FindNode(ctx, nodes[60].ID())
	if err != nil || len(found) == 0 || found[0].ID != nodes[60].ID() {
		t.Fatalf("lookup: %v %v", found, err)
	}
}

// A node that starts with a random ID takes a secure one once the
// responses' "ip" keys agree on its external address (BEP 42).
func TestSimLearnsExternalIP(t *testing.T) {
	s := newSimNet(t, 6)
	s.build(t, 30, Config{})
	a := simAddr(777)
	n := s.add(t, a, Config{PublicIP: netip.MustParseAddr("10.0.0.1")}) // wrong, and local
	n.mu.Lock()
	n.external = netip.Addr{}
	n.mu.Unlock()
	if IDSecure(n.ID(), a.Addr()) {
		t.Skip("random ID happens to be secure")
	}
	n.AddNodes(NodeInfo{Addr: simAddr(0)})
	if err := n.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !IDSecure(n.ID(), a.Addr()) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n.ExternalIP() != a.Addr() || !IDSecure(n.ID(), a.Addr()) {
		t.Fatalf("external %v, ID %s not secure", n.ExternalIP(), n.ID())
	}
}

// A read-only node (BEP 43) looks things up but answers nothing, and the
// others keep it out of their tables.
func TestSimReadOnly(t *testing.T) {
	s := newSimNet(t, 7)
	nodes := s.build(t, 60, Config{})
	ro := s.add(t, simAddr(900), Config{ReadOnly: true})
	ro.AddNodes(NodeInfo{Addr: simAddr(0)})
	if err := ro.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ro.Stats().Nodes == 0 {
		t.Fatal("read-only node learned no nodes")
	}
	for _, n := range nodes {
		for _, ni := range n.Nodes() {
			if ni.ID == ro.ID() {
				t.Fatal("read-only node in a routing table")
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := nodes[1].Ping(ctx, simAddr(900)); err == nil {
		t.Fatal("read-only node answered a ping")
	}
}

// Queries past the rate a source IP is allowed go unanswered.
func TestSimRateLimit(t *testing.T) {
	s := newSimNet(t, 8)
	srv := s.add(t, simAddr(0), Config{QueryRate: 1})
	// Read-only, so that the server does not ping it back and it does
	// not ping the server to check on it: only its pings count.
	cli := s.add(t, simAddr(1), Config{ReadOnly: true, QueryRetries: -1, QueryTimeout: 20 * time.Millisecond})
	_ = srv
	ok := 0
	for range 10 {
		if _, err := cli.Ping(context.Background(), simAddr(0)); err == nil {
			ok++
		}
	}
	if ok != 5 { // the burst: five seconds' worth at one a second
		t.Fatalf("%d pings answered", ok)
	}
}

func TestSimStateRestart(t *testing.T) {
	s := newSimNet(t, 9)
	nodes := s.build(t, 80, Config{})
	st := nodes[40].State()
	if len(st.Nodes) < K {
		t.Fatalf("state of %d nodes", len(st.Nodes))
	}
	s.mu.Lock()
	delete(s.nodes, simAddr(40))
	s.mu.Unlock()
	_ = nodes[40].Close()
	n := s.add(t, simAddr(40), Config{ID: st.ID})
	n.AddNodes(st.Nodes...)
	if err := n.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n.ID() != st.ID || n.Stats().Nodes < K {
		t.Fatalf("restarted: id %v, %d nodes", n.ID(), n.Stats().Nodes)
	}
}

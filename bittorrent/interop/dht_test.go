package interop

import (
	"context"
	"crypto/ed25519"
	"net"
	"net/netip"
	"os"
	"sync"
	"testing"
	"time"

	adht "github.com/anacrolix/dht/v2"
	"github.com/anacrolix/dht/v2/bep44"
	"github.com/anacrolix/dht/v2/exts/getput"
	"github.com/anacrolix/dht/v2/int160"
	"github.com/anacrolix/dht/v2/krpc"
	peer_store "github.com/anacrolix/dht/v2/peer-store"
	alog "github.com/anacrolix/log"
	"golang.org/x/time/rate"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bittorrent/dht"
	"github.com/lesismal/fib/bittorrent/metainfo"
)

// dhtNet is a DHT of fib nodes and anacrolix/dht servers on loopback.
type dhtNet struct {
	fib  []*dht.Node
	ana  []*adht.Server
	ctx  context.Context
	done context.CancelFunc
}

func fibAddr(n *dht.Node) netip.AddrPort {
	a := n.LocalAddr().(*net.UDPAddr).AddrPort()
	return netip.AddrPortFrom(a.Addr().Unmap(), a.Port())
}

func anaAddr(s *adht.Server) netip.AddrPort {
	a := s.Addr().(*net.UDPAddr).AddrPort()
	return netip.AddrPortFrom(a.Addr().Unmap(), a.Port())
}

func udpAddr(a netip.AddrPort) *net.UDPAddr { return net.UDPAddrFromAddrPort(a) }

func startFibEngine(t *testing.T) *fib.Engine {
	t.Helper()
	cfg := fib.DefaultConfig()
	cfg.LogSettings = false
	e, err := fib.New(cfg)
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

// peerStore is the anacrolix server's store of announced peers.
// peer_store.InMemory of anacrolix/dht v2.23.0 keys its peers by IP and
// decodes the key as a whole address, so that what it returns has a
// mangled IP and port: no peer announced to it is ever handed out.
type peerStore struct {
	mu    sync.Mutex
	peers map[peer_store.InfoHash]map[string]krpc.NodeAddr
}

func (s *peerStore) AddPeer(ih peer_store.InfoHash, na krpc.NodeAddr) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peers[ih] == nil {
		s.peers[ih] = map[string]krpc.NodeAddr{}
	}
	s.peers[ih][na.String()] = na
}

func (s *peerStore) GetPeers(ih peer_store.InfoHash) []krpc.NodeAddr {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []krpc.NodeAddr
	for _, na := range s.peers[ih] {
		out = append(out, na)
	}
	return out
}

// newAnacrolixDHT starts an anacrolix/dht server on loopback whose starting
// nodes are those start returns.
func newAnacrolixDHT(t *testing.T, start func() []netip.AddrPort) *adht.Server {
	t.Helper()
	conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := adht.NewDefaultServerConfig()
	cfg.Conn = conn
	cfg.NoSecurity = true
	cfg.PeerStore = &peerStore{peers: map[peer_store.InfoHash]map[string]krpc.NodeAddr{}}
	// The default limiter is shared by every server of the process, at
	// 25 datagrams a second: replies past it would be dropped.
	cfg.SendLimiter = rate.NewLimiter(rate.Inf, 0)
	cfg.Logger = alog.Default.FilterLevel(alog.Critical)
	cfg.QueryResendDelay = func() time.Duration { return 500 * time.Millisecond }
	cfg.StartingNodes = func() ([]adht.Addr, error) {
		var out []adht.Addr
		for _, a := range start() {
			out = append(out, adht.NewAddr(udpAddr(a)))
		}
		return out, nil
	}
	s, err := adht.NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

// newDHTNet builds a network of nf fib nodes and na anacrolix servers:
// the first fib node bootstraps from the first anacrolix server only, the
// first anacrolix server from the first fib node only, and the others
// from both, so that each implementation's bootstrap goes through the
// other's answers.
func newDHTNet(t *testing.T, nf, na int) *dhtNet {
	e := startFibEngine(t)
	d := &dhtNet{}
	d.ctx, d.done = context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(d.done)
	for range nf {
		n, err := dht.Listen(e, "127.0.0.1:0", dht.Config{Routers: []string{}, QueryRate: -1, QueryTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = n.Close() })
		d.fib = append(d.fib, n)
	}
	for i := range na {
		d.ana = append(d.ana, newAnacrolixDHT(t, func() []netip.AddrPort {
			if i == 0 {
				return []netip.AddrPort{fibAddr(d.fib[0])}
			}
			return []netip.AddrPort{fibAddr(d.fib[0]), anaAddr(d.ana[0])}
		}))
	}
	// The first of each, from the other alone.
	d.fib[0].AddNodes(dht.NodeInfo{Addr: anaAddr(d.ana[0])})
	if err := d.fib[0].Bootstrap(d.ctx); err != nil {
		t.Fatalf("fib bootstrap from anacrolix: %v", err)
	}
	if _, err := d.ana[0].BootstrapContext(d.ctx); err != nil {
		t.Fatalf("anacrolix bootstrap from fib: %v", err)
	}
	for _, s := range d.ana[1:] {
		if _, err := s.BootstrapContext(d.ctx); err != nil {
			t.Fatalf("anacrolix bootstrap: %v", err)
		}
	}
	for _, n := range d.fib[1:] {
		n.AddNodes(dht.NodeInfo{Addr: anaAddr(d.ana[0])}, dht.NodeInfo{Addr: fibAddr(d.fib[0])})
		if err := n.Bootstrap(d.ctx); err != nil {
			t.Fatalf("fib bootstrap: %v", err)
		}
	}
	// Bootstrap the first ones again, now that the others have joined.
	_ = d.fib[0].Bootstrap(d.ctx)
	_, _ = d.ana[0].BootstrapContext(d.ctx)
	return d
}

func TestDHTInterop(t *testing.T) {
	d := newDHTNet(t, 6, 6)
	ctx := d.ctx

	t.Run("tables", func(t *testing.T) {
		// Each implementation's routing table holds the other's nodes.
		anaIDs := map[dht.ID]bool{}
		for _, s := range d.ana {
			anaIDs[dht.ID(s.ID())] = true
		}
		fibIDs := map[[20]byte]bool{}
		for _, n := range d.fib {
			fibIDs[n.ID()] = true
		}
		for i, n := range d.fib {
			got := 0
			for _, ni := range n.Nodes() {
				if anaIDs[ni.ID] {
					got++
				}
			}
			if got == 0 {
				t.Errorf("fib node %d knows no anacrolix node", i)
			}
		}
		for i, s := range d.ana {
			got := 0
			for _, ni := range s.Nodes() {
				if fibIDs[ni.ID] {
					got++
				}
			}
			if got == 0 {
				t.Errorf("anacrolix server %d knows no fib node", i)
			}
		}
	})

	t.Run("ping", func(t *testing.T) {
		id, err := d.fib[1].Ping(ctx, anaAddr(d.ana[1]))
		if err != nil || id != dht.ID(d.ana[1].ID()) {
			t.Fatalf("fib pings anacrolix: %v %v", id, err)
		}
		res := d.ana[2].Ping(udpAddr(fibAddr(d.fib[2])))
		if err := res.ToError(); err != nil || res.Reply.R.ID != krpc.ID(d.fib[2].ID()) {
			t.Fatalf("anacrolix pings fib: %v", err)
		}
		// BEP 42: fib's responses tell the asker its address.
		if got := res.Reply.IP.IP.String(); got != "127.0.0.1" {
			t.Fatalf("ip of fib's response %q", got)
		}
	})

	t.Run("find_node", func(t *testing.T) {
		// fib's lookup converges on an anacrolix node, and on a fib one.
		for _, target := range []dht.ID{dht.ID(d.ana[3].ID()), d.fib[4].ID()} {
			found, err := d.fib[5].FindNode(ctx, target)
			if err != nil || len(found) == 0 || found[0].ID != target {
				t.Fatalf("fib find_node %s: %v %v", target, found, err)
			}
		}
		// anacrolix reads fib's compact nodes: asked for a node it has,
		// fib answers with that node first.
		known := d.fib[1].Nodes()[0]
		res := d.ana[4].FindNode(adht.NewAddr(udpAddr(fibAddr(d.fib[1]))), int160.FromByteArray(known.ID), adht.QueryRateLimiting{})
		if err := res.ToError(); err != nil {
			t.Fatal(err)
		}
		if n := res.Reply.R.Nodes; len(n) == 0 || n[0].ID != krpc.ID(known.ID) || n[0].Addr.Port != int(known.Addr.Port()) {
			t.Fatalf("anacrolix find_node on fib: %v, want %v first", n, known)
		}
	})

	t.Run("fib announces, anacrolix gets peers", func(t *testing.T) {
		ih := metainfo.InfoHash(dht.RandomID())
		if n, err := d.fib[1].Announce(ctx, ih, 7777, false, nil); err != nil || n == 0 {
			t.Fatalf("announce: %d %v", n, err)
		}
		a, err := d.ana[5].AnnounceTraversal(ih)
		if err != nil {
			t.Fatal(err)
		}
		defer a.Close()
		found := false
		for pv := range a.Peers {
			for _, p := range pv.Peers {
				found = found || p.Port == 7777
			}
		}
		if !found {
			t.Fatal("anacrolix did not find fib's peer")
		}
	})

	t.Run("anacrolix announces, fib gets peers", func(t *testing.T) {
		ih := metainfo.InfoHash(dht.RandomID())
		a, err := d.ana[1].Announce(ih, 8888, false)
		if err != nil {
			t.Fatal(err)
		}
		for range a.Peers {
		}
		a.Close()
		ih2 := metainfo.InfoHash(dht.RandomID())
		a2, err := d.ana[2].Announce(ih2, 0, true)
		if err != nil {
			t.Fatal(err)
		}
		for range a2.Peers {
		}
		a2.Close()
		for _, c := range []struct {
			ih   metainfo.InfoHash
			want netip.AddrPort
		}{
			{ih, netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), 8888)},
			{ih2, anaAddr(d.ana[2])}, // implied_port: the DHT's own
		} {
			var got []netip.AddrPort
			if err := d.fib[4].GetPeers(ctx, c.ih, func(p []netip.AddrPort) { got = append(got, p...) }); err != nil {
				t.Fatal(err)
			}
			if len(got) == 0 || got[0] != c.want {
				t.Fatalf("fib found %v, want %v", got, c.want)
			}
		}
	})

	t.Run("sample_infohashes", func(t *testing.T) {
		// anacrolix does not answer BEP 51 queries, but has its messages:
		// it asks a fib node that holds an announce.
		var holder *dht.Node
		for _, n := range d.fib {
			if n.Stats().Torrents > 0 {
				holder = n
				break
			}
		}
		if holder == nil {
			t.Fatal("no fib node holds a torrent")
		}
		res := d.ana[0].Query(ctx, adht.NewAddr(udpAddr(fibAddr(holder))), "sample_infohashes", adht.QueryInput{
			MsgArgs: krpc.MsgArgs{Target: krpc.ID(dht.RandomID())},
		})
		if err := res.ToError(); err != nil {
			t.Fatal(err)
		}
		r := res.Reply.R
		if r.Samples == nil || len(*r.Samples) == 0 || r.Num == nil || *r.Num != int64(holder.Stats().Torrents) || r.Interval == nil || len(r.Nodes) == 0 {
			t.Fatalf("samples %+v num %v", r.Samples, r.Num)
		}
	})

	t.Run("bep44 immutable", func(t *testing.T) {
		it, _ := dht.NewImmutableItem("fib puts, anacrolix gets")
		if n, err := d.fib[2].Put(ctx, it); err != nil || n == 0 {
			t.Fatalf("fib put: %d %v", n, err)
		}
		got, _, err := getput.Get(ctx, bep44.Target(it.Target()), d.ana[3], nil, nil)
		if err != nil || string(got.V) != string(it.V) {
			t.Fatalf("anacrolix get: %q %v", got.V, err)
		}
		put := bep44.Put{V: "anacrolix puts, fib gets"}
		target := put.Target()
		if _, err := getput.Put(ctx, krpc.ID(target), d.ana[4], nil, func(int64) bep44.Put { return put }); err != nil {
			t.Fatal(err)
		}
		fit, err := d.fib[3].Get(ctx, dht.ID(target))
		if err != nil || string(fit.V) != "24:anacrolix puts, fib gets" {
			t.Fatalf("fib get: %v %v", fit, err)
		}
	})

	t.Run("bep44 mutable", func(t *testing.T) {
		pub, priv, _ := ed25519.GenerateKey(nil)
		salt := []byte("s")
		it, _ := dht.NewMutableItem("fib v1", salt, 1, priv)
		if n, err := d.fib[1].Put(ctx, it); err != nil || n == 0 {
			t.Fatalf("fib put: %d %v", n, err)
		}
		got, _, err := getput.Get(ctx, bep44.Target(it.Target()), d.ana[5], nil, salt)
		if err != nil || !got.Mutable || got.Seq != 1 || string(got.V) != "6:fib v1" {
			t.Fatalf("anacrolix get: %+v %v", got, err)
		}
		// anacrolix puts the next version with the same key.
		var k [32]byte
		copy(k[:], pub)
		target := bep44.MakeMutableTarget(k, salt)
		if _, err := getput.Put(ctx, krpc.ID(target), d.ana[1], salt, func(seq int64) bep44.Put {
			p := bep44.Put{V: "anacrolix v2", K: &k, Salt: salt, Seq: seq + 1}
			p.Sign(priv)
			return p
		}); err != nil {
			t.Fatal(err)
		}
		fit, err := d.fib[5].GetMutable(ctx, k, salt)
		if err != nil || fit.Seq != 2 || string(fit.V) != "12:anacrolix v2" {
			t.Fatalf("fib get: %+v %v", fit, err)
		}
	})
}

// BEP 42 as both implementations compute it.
func TestDHTSecureIDInterop(t *testing.T) {
	for i := range 200 {
		ip := netip.AddrFrom4([4]byte{byte(1 + i%200), byte(i * 7), byte(i * 13), byte(i)})
		if i%2 == 1 {
			ip = netip.AddrFrom16([16]byte{0x20, 0x01, byte(i), byte(i * 3), 1, 2, 3, 4, 5})
		}
		id := dht.SecureID(ip)
		if !adht.NodeIdSecure(id, net.IP(ip.AsSlice())) {
			t.Fatalf("anacrolix rejects fib's secure ID %s for %s", id, ip)
		}
		var aid krpc.ID
		copy(aid[:], dht.RandomID().Bytes())
		adht.SecureNodeId(&aid, net.IP(ip.AsSlice()))
		if !dht.IDSecure(dht.ID(aid), ip) {
			t.Fatalf("fib rejects anacrolix's secure ID %x for %s", aid, ip)
		}
	}
}

// TestDHTPublic bootstraps from the real DHT and looks up an info hash:
// set FIB_DHT_PUBLIC=1 to run it, as it needs the internet.
func TestDHTPublic(t *testing.T) {
	if os.Getenv("FIB_DHT_PUBLIC") == "" {
		t.Skip("set FIB_DHT_PUBLIC=1 to bootstrap from the public DHT")
	}
	e := startFibEngine(t)
	n, err := dht.Listen(e, ":0", dht.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	start := time.Now()
	if err := n.Bootstrap(ctx); err != nil {
		t.Fatalf("%v: %+v", err, n.Stats())
	}
	t.Logf("bootstrapped in %v: %+v, external IP %v", time.Since(start), n.Stats(), n.ExternalIP())
	// The info hash of a Ubuntu release, widely seeded.
	ih, _ := metainfo.ParseInfoHash(os.Getenv("FIB_DHT_INFOHASH"))
	if ih.IsZero() {
		return
	}
	peers := 0
	start = time.Now()
	before := n.Stats()
	if err := n.GetPeers(ctx, ih, func(p []netip.AddrPort) { peers += len(p) }); err != nil {
		t.Fatal(err)
	}
	after := n.Stats()
	t.Logf("%d peers in %v, %d queries, %d timeouts", peers, time.Since(start),
		after.QueriesSent-before.QueriesSent, after.QueryTimeouts-before.QueryTimeouts)
}

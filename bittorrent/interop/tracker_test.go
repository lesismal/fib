package interop

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"net/netip"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bittorrent/metainfo"
	"github.com/lesismal/fib/bittorrent/tracker"
	fibhttp "github.com/lesismal/fib/http"

	"github.com/anacrolix/generics"
	atracker "github.com/anacrolix/torrent/tracker"
	httpTrackerServer "github.com/anacrolix/torrent/tracker/http/server"
	trackerServer "github.com/anacrolix/torrent/tracker/server"
	"github.com/anacrolix/torrent/tracker/udp"
	udpTrackerServer "github.com/anacrolix/torrent/tracker/udp/server"
	"github.com/anacrolix/torrent/types/infohash"
)

// trackerEngine runs a fib engine for the test.
func trackerEngine(t *testing.T) *fib.Engine {
	t.Helper()
	cfg := fib.DefaultConfig()
	cfg.LogSettings = false
	cfg.ReusePort = false
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

func trackerCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func trackerPeerID(i int) [20]byte {
	var id [20]byte
	copy(id[:], fmt.Sprintf("-FB0001-%012d", i))
	return id
}

// fibTracker serves a fib tracker over HTTP and UDP at host, and returns
// its announce URLs. Announces must carry the passkey "pk" in their path,
// which tests that anacrolix/torrent sends the URL data of BEP 41.
func fibTracker(t *testing.T, host string) (httpURL, udpURL string) {
	t.Helper()
	e := trackerEngine(t)
	srv := tracker.NewServer(tracker.ServerConfig{
		Authorize: func(a *tracker.Auth) error {
			if a.Passkey != "pk" && !(a.UDP && a.Scrape) {
				return errors.New("no passkey in " + a.URL)
			}
			return nil
		},
	})
	hi, err := e.AddBinding(fib.Binding{Addr: net.JoinHostPort(host, "0"), Handler: fibhttp.NewHandler(srv)})
	if err != nil {
		t.Skip("cannot listen on", host, err)
	}
	ui, err := e.AddBinding(fib.Binding{Network: "udp", Addr: net.JoinHostPort(host, "0"), Handler: srv.UDP()})
	if err != nil {
		t.Skip("cannot listen on", host, err)
	}
	addr := func(i int) string {
		addrs, err := e.BindingAddrs(i)
		if err != nil || len(addrs) == 0 {
			t.Fatal(addrs, err)
		}
		return addrs[0].String()
	}
	return "http://" + addr(hi) + "/pk/announce", "udp://" + addr(ui) + "/pk/announce"
}

// TestTrackerAnacrolixClientToFib has anacrolix/torrent's tracker client
// announce three peers to fib's HTTP and UDP trackers, over IPv4 and IPv6,
// complete one, and scrape.
func TestTrackerAnacrolixClientToFib(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "::1"} {
		httpURL, udpURL := fibTracker(t, host)
		for _, u := range []string{httpURL, udpURL} {
			t.Run(u, func(t *testing.T) {
				cl, err := atracker.NewClient(u, atracker.NewClientOpts{})
				if err != nil {
					t.Fatal(err)
				}
				defer cl.Close()
				var ih infohash.T
				copy(ih[:], "anacrolix to fib "+u)
				for i := 1; i <= 3; i++ {
					resp, err := cl.Announce(trackerCtx(t), atracker.AnnounceRequest{
						InfoHash: ih, PeerId: trackerPeerID(i), Port: uint16(7000 + i), Left: 10,
						Event: atracker.Started, NumWant: -1, Key: int32(i),
					}, atracker.AnnounceOpt{})
					if err != nil {
						t.Fatal(i, err)
					}
					if resp.Interval != int32(tracker.DefaultInterval/time.Second) || resp.Leechers != int32(i) || resp.Seeders != 0 {
						t.Fatalf("announce %d: %+v", i, resp)
					}
					if len(resp.Peers) != i-1 {
						t.Fatalf("announce %d: peers %v", i, resp.Peers)
					}
					for _, p := range resp.Peers {
						ap, ok := p.ToNetipAddrPort()
						if !ok || ap.Addr().Unmap() != netip.MustParseAddr(host) || ap.Port() <= 7000 || ap.Port() >= uint16(7000+i) {
							t.Fatalf("announce %d: peer %v", i, p)
						}
					}
				}
				resp, err := cl.Announce(trackerCtx(t), atracker.AnnounceRequest{
					InfoHash: ih, PeerId: trackerPeerID(1), Port: 7001, Left: 0, Event: atracker.Completed, NumWant: -1,
				}, atracker.AnnounceOpt{})
				if err != nil || resp.Seeders != 1 || resp.Leechers != 2 || len(resp.Peers) != 2 {
					t.Fatalf("completed: %+v %v", resp, err)
				}
				var other infohash.T
				copy(other[:], "unknown")
				files, err := cl.Scrape(trackerCtx(t), []infohash.T{ih, other})
				if err != nil {
					t.Fatal(err)
				}
				if len(files) != 2 || files[0] != (udp.ScrapeInfohashResult{Seeders: 1, Completed: 1, Leechers: 2}) ||
					files[1] != (udp.ScrapeInfohashResult{}) {
					t.Fatalf("scrape: %+v", files)
				}
			})
		}
	}
}

// TestTrackerFibClientToOpentracker has fib's clients announce to and
// scrape opentracker, at the address FIB_OPENTRACKER names; opentracker.sh
// runs one in a container and the test against it. It is skipped
// otherwise, as in CI.
func TestTrackerFibClientToOpentracker(t *testing.T) {
	addr := os.Getenv("FIB_OPENTRACKER")
	if addr == "" {
		t.Skip("FIB_OPENTRACKER is not set; see opentracker.sh")
	}
	e := trackerEngine(t)
	hc := fibhttp.NewClient(e, fibhttp.DefaultClientConfig())
	defer hc.Close()
	uc, err := tracker.ListenUDPClient(e, tracker.UDPClientConfig{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer uc.Close()
	c := &tracker.Client{HTTP: tracker.NewHTTPClient(hc), UDP: uc}
	for i, u := range []string{"http://" + addr + "/announce", "udp://" + addr + "/announce"} {
		t.Run(u, func(t *testing.T) {
			// The whitelist opentracker.sh gives the tracker has these.
			ih, err := metainfo.ParseInfoHash(fmt.Sprintf("f1b%037d", i+1))
			if err != nil {
				t.Fatal(err)
			}
			for i := 1; i <= 3; i++ {
				resp, err := c.Announce(trackerCtx(t), u, &tracker.AnnounceRequest{
					InfoHash: ih, PeerID: trackerPeerID(i), Port: uint16(9000 + i), Left: 10, Event: tracker.EventStarted,
				})
				if err != nil {
					t.Fatal(i, err)
				}
				t.Logf("announce %d: interval %v, %d seeders, %d leechers, peers %v", i, resp.Interval, resp.Complete, resp.Incomplete, resp.Peers)
				if resp.Interval <= 0 || resp.Incomplete != i || len(resp.Peers) < i-1 {
					t.Fatalf("announce %d: %+v", i, resp)
				}
			}
			if _, err := c.Announce(trackerCtx(t), u, &tracker.AnnounceRequest{
				InfoHash: ih, PeerID: trackerPeerID(1), Port: 9001, Event: tracker.EventCompleted,
			}); err != nil {
				t.Fatal(err)
			}
			files, err := c.Scrape(trackerCtx(t), u, []metainfo.InfoHash{ih})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("scrape: %+v", files)
			if st := files[ih]; st.Complete != 1 || st.Incomplete != 2 || st.Downloaded != 1 {
				t.Fatalf("scrape: %+v", files)
			}
		})
	}
}

// memTracker is the store of anacrolix/torrent's tracker server, which
// comes without one: the peers of each torrent, true for seeders.
type memTracker struct {
	mu    sync.Mutex
	peers map[[20]byte]map[netip.AddrPort]bool
}

func (m *memTracker) TrackAnnounce(_ context.Context, req udp.AnnounceRequest, addr netip.AddrPort) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.peers == nil {
		m.peers = map[[20]byte]map[netip.AddrPort]bool{}
	}
	s := m.peers[req.InfoHash]
	if s == nil {
		s = map[netip.AddrPort]bool{}
		m.peers[req.InfoHash] = s
	}
	if req.Event == atracker.Stopped {
		delete(s, addr)
	} else {
		s[addr] = req.Left == 0
	}
	return nil
}

func (m *memTracker) Scrape(context.Context, []trackerServer.InfoHash) ([]udp.ScrapeInfohashResult, error) {
	return nil, errors.New("not implemented")
}

func (m *memTracker) GetPeers(_ context.Context, ih trackerServer.InfoHash, opts trackerServer.GetPeersOpts,
	remote trackerServer.AnnounceAddr) (ret trackerServer.ServerAnnounceResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ret.Interval = generics.Some[int32](600)
	var seeders, leechers int32
	for addr, seeder := range m.peers[ih] {
		if seeder {
			seeders++
		} else {
			leechers++
		}
		if addr != remote && (!opts.MaxCount.Ok || uint(len(ret.Peers)) < opts.MaxCount.Value) {
			ret.Peers = append(ret.Peers, trackerServer.PeerInfo{AnnounceAddr: addr})
		}
	}
	ret.Seeders = generics.Some(seeders)
	ret.Leechers = generics.Some(leechers)
	return
}

// connTracker keeps the connection IDs anacrolix/torrent's UDP tracker
// server hands out.
type connTracker struct {
	mu  sync.Mutex
	ids map[string]udp.ConnectionId
}

func (c *connTracker) Add(_ context.Context, addr string, id udp.ConnectionId) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ids == nil {
		c.ids = map[string]udp.ConnectionId{}
	}
	c.ids[addr] = id
	return nil
}

func (c *connTracker) Check(_ context.Context, addr string, id udp.ConnectionId) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ids[addr] == id, nil
}

// TestTrackerFibClientToAnacrolix has fib's HTTP and UDP tracker clients
// announce to anacrolix/torrent's tracker servers. Those serve no scrapes,
// so there are none to check.
func TestTrackerFibClientToAnacrolix(t *testing.T) {
	handler := &trackerServer.AnnounceHandler{AnnounceTracker: &memTracker{}}
	ts := httptest.NewServer(httpTrackerServer.Handler{Announce: handler})
	defer ts.Close()

	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	us := &udpTrackerServer.Server{
		ConnTracker: &connTracker{},
		SendResponse: func(_ context.Context, b []byte, addr net.Addr) (int, error) {
			return pc.WriteTo(b, addr)
		},
		Announce: handler,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = udpTrackerServer.RunSimple(ctx, us, pc, udp.AddrFamilyIpv4) }()

	e := trackerEngine(t)
	hc := fibhttp.NewClient(e, fibhttp.DefaultClientConfig())
	defer hc.Close()
	uc, err := tracker.ListenUDPClient(e, tracker.UDPClientConfig{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer uc.Close()
	c := &tracker.Client{HTTP: tracker.NewHTTPClient(hc), UDP: uc}

	for _, u := range []string{ts.URL + "/announce", "udp://" + pc.LocalAddr().String() + "/announce"} {
		t.Run(u, func(t *testing.T) {
			var ih metainfo.InfoHash
			copy(ih[:], "fib to anacrolix "+u)
			for i := 1; i <= 3; i++ {
				resp, err := c.Announce(trackerCtx(t), u, &tracker.AnnounceRequest{
					InfoHash: ih, PeerID: trackerPeerID(i), Port: uint16(8000 + i), Left: 10, Event: tracker.EventStarted,
				})
				if err != nil {
					t.Fatal(i, err)
				}
				if resp.Interval != 600*time.Second || resp.Incomplete != i || resp.Complete != 0 || len(resp.Peers) != i-1 {
					t.Fatalf("announce %d: %+v", i, resp)
				}
				for _, p := range resp.Peers {
					if p.Addr.Addr() != netip.MustParseAddr("127.0.0.1") || p.Addr.Port() <= 8000 || p.Addr.Port() >= uint16(8000+i) {
						t.Fatalf("announce %d: peer %v", i, p)
					}
				}
			}
			resp, err := c.Announce(trackerCtx(t), u, &tracker.AnnounceRequest{
				InfoHash: ih, PeerID: trackerPeerID(1), Port: 8001, Event: tracker.EventCompleted,
			})
			if err != nil || resp.Complete != 1 || resp.Incomplete != 2 {
				t.Fatalf("completed: %+v %v", resp, err)
			}
		})
	}
}

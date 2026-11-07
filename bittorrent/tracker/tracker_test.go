//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tracker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/bittorrent/metainfo"
	fibhttp "github.com/lesismal/fib/http"
)

func startEngine(t *testing.T) *fib.Engine {
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

// env is a tracker serving HTTP and UDP on one engine, and the clients of
// both on it.
type env struct {
	engine            *fib.Engine
	srv               *Server
	httpURL, udpURL   string
	http              *HTTPClient
	udp               *UDPClient
	client            *Client
	httpAddr, udpAddr netip.AddrPort
}

func bindingAddr(t *testing.T, e *fib.Engine, i int) netip.AddrPort {
	t.Helper()
	addrs, err := e.BindingAddrs(i)
	if err != nil || len(addrs) == 0 {
		t.Fatal(addrs, err)
	}
	ap, err := netip.ParseAddrPort(addrs[0].String())
	if err != nil {
		t.Fatal(err)
	}
	return ap
}

func newEnv(t *testing.T, cfg ServerConfig, host string) *env {
	t.Helper()
	e := startEngine(t)
	srv := NewServer(cfg)
	hi, err := e.AddBinding(fib.Binding{Addr: net.JoinHostPort(host, "0"), Handler: fibhttp.NewHandler(srv)})
	if err != nil {
		t.Skip("cannot listen on", host, err)
	}
	ui, err := e.AddBinding(fib.Binding{Network: "udp", Addr: net.JoinHostPort(host, "0"), Handler: srv.UDP()})
	if err != nil {
		t.Skip("cannot listen on", host, err)
	}
	ev := &env{engine: e, srv: srv, httpAddr: bindingAddr(t, e, hi), udpAddr: bindingAddr(t, e, ui)}
	ev.httpURL = "http://" + ev.httpAddr.String() + "/announce"
	ev.udpURL = "udp://" + ev.udpAddr.String() + "/announce"
	hc := fibhttp.NewClient(e, fibhttp.DefaultClientConfig())
	t.Cleanup(hc.Close)
	ev.http = NewHTTPClient(hc)
	ev.udp, err = ListenUDPClient(e, UDPClientConfig{Timeout: 200 * time.Millisecond, Retransmits: 4})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ev.udp.Close() })
	ev.client = &Client{HTTP: ev.http, UDP: ev.udp}
	return ev
}

func hashOf(s string) metainfo.InfoHash {
	var h metainfo.InfoHash
	copy(h[:], s)
	return h
}

func peerID(i int) PeerID {
	var id PeerID
	copy(id[:], fmt.Sprintf("-FB0001-%012d", i))
	return id
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

func ports(peers []Peer) map[uint16]bool {
	m := map[uint16]bool{}
	for _, p := range peers {
		m[p.Addr.Port()] = true
	}
	return m
}

// swarmTest announces three peers, completes one, stops another and
// scrapes, through the tracker URL u.
func swarmTest(t *testing.T, ev *env, u string, wantFamily4 bool) {
	h := hashOf("swarm " + u)
	for i := 1; i <= 3; i++ {
		resp, err := ev.client.Announce(ctx(t), u, &AnnounceRequest{
			InfoHash: h, PeerID: peerID(i), Port: uint16(10000 + i), Left: 100, Event: EventStarted, Key: 7,
		})
		if err != nil {
			t.Fatal(i, err)
		}
		if resp.Interval != DefaultInterval {
			t.Fatal("interval", resp.Interval)
		}
		if resp.Incomplete != i || resp.Complete != 0 {
			t.Fatalf("announce %d: %+v", i, resp)
		}
		got := ports(resp.Peers)
		if len(resp.Peers) != i-1 || got[uint16(10000+i)] {
			t.Fatalf("announce %d: peers %v", i, resp.Peers)
		}
		for _, p := range resp.Peers {
			if p.Addr.Addr().Is4() != wantFamily4 || !p.Addr.Addr().IsLoopback() {
				t.Fatal("peer address", p.Addr)
			}
		}
	}
	resp, err := ev.client.Announce(ctx(t), u, &AnnounceRequest{
		InfoHash: h, PeerID: peerID(1), Port: 10001, Left: 0, Event: EventCompleted, Downloaded: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Complete != 1 || resp.Incomplete != 2 || len(resp.Peers) != 2 {
		t.Fatalf("completed: %+v", resp)
	}
	// A seeder gets leechers alone: peer 2 stops, peer 1 sees peer 3.
	if _, err := ev.client.Announce(ctx(t), u, &AnnounceRequest{
		InfoHash: h, PeerID: peerID(2), Port: 10002, Left: 100, Event: EventStopped, NumWant: -1,
	}); err != nil {
		t.Fatal(err)
	}
	resp, err = ev.client.Announce(ctx(t), u, &AnnounceRequest{InfoHash: h, PeerID: peerID(1), Port: 10001})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Peers) != 1 || resp.Peers[0].Addr.Port() != 10003 {
		t.Fatalf("seeder's peers: %v", resp.Peers)
	}
	files, err := ev.client.Scrape(ctx(t), u, []metainfo.InfoHash{h, hashOf("unknown")})
	if err != nil {
		t.Fatal(err)
	}
	if st := files[h]; st != (Stats{Complete: 1, Incomplete: 1, Downloaded: 1}) {
		t.Fatalf("scrape: %+v", files)
	}
	if st := files[hashOf("unknown")]; st != (Stats{}) {
		t.Fatalf("scrape of an unknown torrent: %+v", st)
	}
}

func TestHTTPSwarm(t *testing.T) {
	ev := newEnv(t, ServerConfig{}, "127.0.0.1")
	swarmTest(t, ev, ev.httpURL, true)
}

func TestUDPSwarm(t *testing.T) {
	ev := newEnv(t, ServerConfig{}, "127.0.0.1")
	swarmTest(t, ev, ev.udpURL, true)
}

func TestIPv6Swarm(t *testing.T) {
	ev := newEnv(t, ServerConfig{}, "::1")
	t.Run("http", func(t *testing.T) { swarmTest(t, ev, ev.httpURL, false) })
	t.Run("udp", func(t *testing.T) { swarmTest(t, ev, ev.udpURL, false) })
}

// TestHTTPDualStack has a peer announce over IPv4 with its IPv6 address
// (BEP 7), for a tracker that trusts it, and another find it in peers6.
func TestHTTPDualStack(t *testing.T) {
	ev := newEnv(t, ServerConfig{TrustIPParam: true}, "127.0.0.1")
	h := hashOf("dual")
	v6 := netip.MustParseAddr("2001:db8::1")
	if _, err := ev.http.Announce(ctx(t), ev.httpURL, &AnnounceRequest{
		InfoHash: h, PeerID: peerID(1), Port: 6881, Left: 1, IPv6: v6,
	}); err != nil {
		t.Fatal(err)
	}
	resp, err := ev.http.Announce(ctx(t), ev.httpURL, &AnnounceRequest{
		InfoHash: h, PeerID: peerID(2), Port: 6882, Left: 1, IPv6: netip.MustParseAddr("2001:db8::2"),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[netip.AddrPort]bool{
		netip.MustParseAddrPort("127.0.0.1:6881"): true,
		netip.AddrPortFrom(v6, 6881):              true,
	}
	if len(resp.Peers) != 2 || !want[resp.Peers[0].Addr] || !want[resp.Peers[1].Addr] {
		t.Fatalf("peers %v", resp.Peers)
	}
	if resp.ExternalIP != netip.MustParseAddr("127.0.0.1") {
		t.Fatal("external ip", resp.ExternalIP)
	}
	// Without TrustIPParam the IPv6 address is not taken.
	ev2 := newEnv(t, ServerConfig{}, "127.0.0.1")
	for i := 1; i <= 2; i++ {
		resp, err = ev2.http.Announce(ctx(t), ev2.httpURL, &AnnounceRequest{
			InfoHash: h, PeerID: peerID(i), Port: uint16(6880 + i), Left: 1, IPv6: v6,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(resp.Peers) != 1 || resp.Peers[0].Addr != netip.MustParseAddrPort("127.0.0.1:6881") {
		t.Fatalf("untrusted: peers %v", resp.Peers)
	}
}

func TestPrivateTracker(t *testing.T) {
	allowed := hashOf("allowed")
	var auths atomic.Int32
	ev := newEnv(t, ServerConfig{
		AllowInfoHash: func(h metainfo.InfoHash) bool { return h == allowed },
		Authorize: func(a *Auth) error {
			auths.Add(1)
			if a.UDP && a.Scrape {
				return nil // BEP 41 has no URL data for scrapes
			}
			if a.Passkey != "s3cret" {
				return errors.New("bad passkey " + a.Passkey)
			}
			if !strings.HasPrefix(a.URL, "/s3cret/announce") && !strings.HasPrefix(a.URL, "/announce?passkey=s3cret") &&
				!strings.HasPrefix(a.URL, "/s3cret/scrape") && !strings.HasPrefix(a.URL, "/scrape?passkey=s3cret") {
				return errors.New("url " + a.URL)
			}
			return nil
		},
	}, "127.0.0.1")
	for _, u := range []string{
		"http://" + ev.httpAddr.String() + "/s3cret/announce",
		"http://" + ev.httpAddr.String() + "/announce?passkey=s3cret",
		"udp://" + ev.udpAddr.String() + "/s3cret/announce",
		"udp://" + ev.udpAddr.String() + "/announce?passkey=s3cret",
	} {
		resp, err := ev.client.Announce(ctx(t), u, &AnnounceRequest{InfoHash: allowed, PeerID: peerID(1), Port: 1, Left: 1})
		if err != nil || resp.Incomplete != 1 {
			t.Fatal(u, resp, err)
		}
		var te *Error
		_, err = ev.client.Announce(ctx(t), u, &AnnounceRequest{InfoHash: hashOf("other"), PeerID: peerID(1), Port: 1})
		if !errors.As(err, &te) || te.Reason != "unregistered torrent" {
			t.Fatal(u, err)
		}
		files, err := ev.client.Scrape(ctx(t), u, []metainfo.InfoHash{allowed, hashOf("other")})
		if err != nil || files[allowed].Incomplete != 1 || files[hashOf("other")] != (Stats{}) {
			t.Fatal(u, files, err)
		}
		bad := strings.Replace(u, "s3cret", "wrong", 1)
		_, err = ev.client.Announce(ctx(t), bad, &AnnounceRequest{InfoHash: allowed, PeerID: peerID(1), Port: 1})
		if !errors.As(err, &te) || te.Reason != "bad passkey wrong" {
			t.Fatal(bad, err)
		}
	}
	if auths.Load() == 0 {
		t.Fatal("Authorize was not asked")
	}
}

// udpSocket is a client socket of the caller's, net's, that a UDPClient
// shares through Send and HandleDatagram, dropping the datagrams drop says
// to.
type udpSocket struct {
	conn  *net.UDPConn
	sends atomic.Int32
	drop  func(n int32) bool
}

func newUDPSocket(t *testing.T, cfg UDPClientConfig, drop func(int32) bool) (*UDPClient, *udpSocket) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	s := &udpSocket{conn: conn, drop: drop}
	cfg.Send = func(to netip.AddrPort, b []byte) error {
		if n := s.sends.Add(1); s.drop != nil && s.drop(n) {
			return nil
		}
		_, err := conn.WriteToUDPAddrPort(b, to)
		return err
	}
	c := NewUDPClient(cfg)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 2048)
		for {
			n, from, err := conn.ReadFromUDPAddrPort(buf)
			if err != nil {
				return
			}
			if IsUDPReply(buf[:n]) {
				c.HandleDatagram(from, buf[:n])
			}
		}
	}()
	t.Cleanup(func() {
		_ = c.Close()
		_ = conn.Close()
		wg.Wait()
	})
	return c, s
}

func TestUDPRetransmit(t *testing.T) {
	ev := newEnv(t, ServerConfig{}, "127.0.0.1")
	// The first connect and the first announce are lost.
	c, s := newUDPSocket(t, UDPClientConfig{Timeout: 50 * time.Millisecond, Retransmits: 3},
		func(n int32) bool { return n == 1 || n == 3 })
	resp, err := c.Announce(ctx(t), ev.udpURL, &AnnounceRequest{InfoHash: hashOf("rtx"), PeerID: peerID(1), Port: 1, Left: 1})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Incomplete != 1 || s.sends.Load() != 4 {
		t.Fatal(resp, s.sends.Load())
	}
	// The connection ID is kept: the next announce is one datagram.
	if _, err := c.Announce(ctx(t), ev.udpURL, &AnnounceRequest{InfoHash: hashOf("rtx"), PeerID: peerID(2), Port: 2}); err != nil {
		t.Fatal(err)
	}
	if s.sends.Load() != 5 {
		t.Fatal("sends", s.sends.Load())
	}

	// Everything lost: the client gives up after its last retransmission.
	c2, s2 := newUDPSocket(t, UDPClientConfig{Timeout: 20 * time.Millisecond, Retransmits: 2},
		func(int32) bool { return true })
	start := time.Now()
	_, err = c2.Announce(ctx(t), ev.udpURL, &AnnounceRequest{InfoHash: hashOf("rtx"), PeerID: peerID(1), Port: 1})
	if !errors.Is(err, ErrTimeout) || s2.sends.Load() != 3 {
		t.Fatal(err, s2.sends.Load())
	}
	// 20 + 40 + 80 ms.
	if d := time.Since(start); d < 140*time.Millisecond {
		t.Fatal("gave up after", d)
	}
}

// TestUDPSharedSocketIgnoresOthers checks that HandleDatagram takes only
// replies to its own requests, from the tracker they went to.
func TestUDPSharedSocketIgnoresOthers(t *testing.T) {
	c := NewUDPClient(UDPClientConfig{Send: func(netip.AddrPort, []byte) error { return nil }})
	defer c.Close()
	from := netip.MustParseAddrPort("127.0.0.1:1")
	for _, b := range [][]byte{
		nil, []byte("d1:ad2:id20:aaaaaaaaaaaaaaaaaaaae1:q4:ping1:t2:aa1:y1:qe"),
		{0x41, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		{0, 0, 0, 1, 1, 2, 3, 4, 0, 0, 0, 0},
	} {
		if c.HandleDatagram(from, b) {
			t.Fatalf("took %x", b)
		}
	}
	w := &udpWait{to: from, ch: make(chan []byte, 1)}
	tx, _ := c.register(w)
	reply := appendUDPReplyHeader(nil, actionConnect, tx)
	if c.HandleDatagram(netip.MustParseAddrPort("127.0.0.1:2"), reply) {
		t.Fatal("took a reply from another address")
	}
	if !c.HandleDatagram(netip.MustParseAddrPort("[::ffff:127.0.0.1]:1"), reply) {
		t.Fatal("did not take its reply")
	}
	if c.HandleDatagram(from, reply) {
		t.Fatal("took a reply twice")
	}
}

func TestPeerExpiry(t *testing.T) {
	store := NewMemoryStore(MemoryStoreConfig{PeerTTL: 200 * time.Millisecond})
	ev := newEnv(t, ServerConfig{Store: store}, "127.0.0.1")
	h := hashOf("expiry")
	for i := 1; i <= 2; i++ {
		if _, err := ev.client.Announce(ctx(t), ev.udpURL, &AnnounceRequest{InfoHash: h, PeerID: peerID(i), Port: uint16(i), Left: 1}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(120 * time.Millisecond)
	// Peer 1 announces again and stays; peer 2 expires.
	if _, err := ev.client.Announce(ctx(t), ev.httpURL, &AnnounceRequest{InfoHash: h, PeerID: peerID(1), Port: 1, Left: 1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(140 * time.Millisecond)
	files, err := ev.client.Scrape(ctx(t), ev.httpURL, []metainfo.InfoHash{h})
	if err != nil || files[h].Incomplete != 1 {
		t.Fatal(files, err)
	}
	time.Sleep(200 * time.Millisecond)
	if _, ok := store.Scrape(h); ok {
		t.Fatal("swarm kept after its peers expired")
	}
	// A torrent nobody touches is forgotten by the sweep of its shard.
	if _, err := ev.client.Announce(ctx(t), ev.httpURL, &AnnounceRequest{InfoHash: h, PeerID: peerID(1), Port: 1, Left: 1}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	other := h
	other[1]++ // the same shard
	if _, err := store.Announce(StoreAnnounce{InfoHash: other, PeerID: peerID(9)}); err != nil {
		t.Fatal(err)
	}
	if store.Torrents() != 0 || store.Peers() != 0 {
		t.Fatal("torrents", store.Torrents(), "peers", store.Peers())
	}
}

func TestTiers(t *testing.T) {
	ev := newEnv(t, ServerConfig{}, "127.0.0.1")
	dead := "udp://127.0.0.1:1/announce"
	tiers := NewTiers([][]string{{dead}, {"http://127.0.0.1:1/announce", ev.udpURL, ev.httpURL, ev.httpURL}, {}})
	tiers.Timeout = 300 * time.Millisecond
	if l := tiers.List(); len(l) != 2 || len(l[1]) != 3 {
		t.Fatal(l)
	}
	resp, u, err := tiers.Announce(ctx(t), ev.client, AnnounceRequest{InfoHash: hashOf("tiers"), PeerID: peerID(1), Port: 1, Left: 1})
	if err != nil || resp.Incomplete != 1 {
		t.Fatal(resp, err)
	}
	if u != ev.udpURL && u != ev.httpURL {
		t.Fatal(u)
	}
	if l := tiers.List(); l[1][0] != u || l[0][0] != dead {
		t.Fatal("not promoted", l)
	}
}

// TestHTTPS has the HTTP client announce over TLS, to a tracker behind
// net/http's test server.
func TestHTTPS(t *testing.T) {
	srv := NewServer(ServerConfig{})
	ts := httptest.NewTLSServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		remote, _ := netip.ParseAddrPort(r.RemoteAddr)
		_, _ = w.Write(srv.httpAnnounce(remote, r.URL.Path, r.URL.RawQuery))
	}))
	defer ts.Close()
	e := startEngine(t)
	cfg := fibhttp.DefaultClientConfig()
	cfg.TLSConfig = ts.Client().Transport.(*stdhttp.Transport).TLSClientConfig
	hc := fibhttp.NewClient(e, cfg)
	defer hc.Close()
	resp, err := NewHTTPClient(hc).Announce(ctx(t), ts.URL+"/announce", &AnnounceRequest{
		InfoHash: hashOf("tls"), PeerID: peerID(1), Port: 1, Left: 1,
	})
	if err != nil || resp.Incomplete != 1 {
		t.Fatal(resp, err)
	}
}

// TestHTTPFailureStatus reads a failure reason sent with a 4xx status, and
// fails on another status without one.
func TestHTTPFailureStatus(t *testing.T) {
	ts := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if strings.Contains(r.URL.Path, "teapot") {
			w.WriteHeader(418)
			return
		}
		w.WriteHeader(400)
		_, _ = io.WriteString(w, "d14:failure reason4:nopee")
	}))
	defer ts.Close()
	e := startEngine(t)
	hc := fibhttp.NewClient(e, fibhttp.DefaultClientConfig())
	defer hc.Close()
	c := NewHTTPClient(hc)
	var te *Error
	if _, err := c.Announce(ctx(t), ts.URL+"/announce", &AnnounceRequest{}); !errors.As(err, &te) || te.Reason != "nope" {
		t.Fatal(err)
	}
	if _, err := c.Announce(ctx(t), ts.URL+"/teapot/announce", &AnnounceRequest{}); err == nil || errors.As(err, &te) {
		t.Fatal(err)
	}
	if _, err := c.Scrape(ctx(t), ts.URL+"/tracker", nil); !errors.Is(err, ErrNoScrape) {
		t.Fatal(err)
	}
}

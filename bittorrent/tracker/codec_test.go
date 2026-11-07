package tracker

import (
	"bytes"
	"errors"
	"net/netip"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

func sampleRequest() *AnnounceRequest {
	r := &AnnounceRequest{
		Port: 6881, Uploaded: 1, Downloaded: 2, Left: 3, Event: EventStarted, NumWant: 30,
		Key: 0xDEADBEEF, TrackerID: "t i&d",
		IP:   netip.MustParseAddr("10.0.0.1"),
		IPv4: netip.MustParseAddr("192.0.2.1"),
		IPv6: netip.MustParseAddr("2001:db8::1"),
	}
	for i := range r.InfoHash {
		r.InfoHash[i] = byte(i * 13) // spaces, '+', '%', '&' and high bytes among them
		r.PeerID[i] = byte(255 - i*11)
	}
	r.InfoHash[3] = ' '
	r.InfoHash[4] = '+'
	return r
}

func TestAnnounceQueryRoundTrip(t *testing.T) {
	r := sampleRequest()
	u, err := url.Parse(announceURL("http://t.example/announce?passkey=k", r))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(u.RawQuery, "passkey=k&info_hash=") || strings.Contains(u.RawQuery, "+") {
		t.Fatal(u.RawQuery)
	}
	a, err := parseAnnounceQuery(u.RawQuery)
	if err != nil {
		t.Fatal(err)
	}
	if a.numWant != 30 || !a.compact || a.passkey != "k" {
		t.Fatalf("%+v", a)
	}
	if !reflect.DeepEqual(a.AnnounceRequest, *r) {
		t.Fatalf("got  %+v\nwant %+v", a.AnnounceRequest, *r)
	}
	r.Left, r.NumWant, r.Event = -1, -1, EventNone
	a, _ = parseAnnounceQuery(strings.TrimPrefix(announceURL("http://t/a", r), "http://t/a?"))
	if a.Left <= 0 || a.numWant != 0 || a.Event != EventNone {
		t.Fatalf("%+v", a)
	}
	// BEP 7's address and port, and anacrolix/torrent's decimal key.
	a, err = parseAnnounceQuery("info_hash=12345678901234567890&peer_id=12345678901234567890&port=1" +
		"&ipv6=%5B2001:db8::2%5D:7000&key=1234&compact=0&no_peer_id=1&event=empty")
	if err != nil || a.ipv6 != netip.MustParseAddrPort("[2001:db8::2]:7000") || a.Key != 1234 || a.compact || !a.noPeerID {
		t.Fatalf("%+v %v", a, err)
	}
	for _, q := range []string{"", "info_hash=x", "info_hash=12345678901234567890&peer_id=12345678901234567890",
		"info_hash=12345678901234567890&peer_id=12345678901234567890&port=70000",
		"info_hash=12345678901234567890&peer_id=12345678901234567890&port=1&left=x",
		"info_hash=12345678901234567890&peer_id=12345678901234567890&port=1&event=bogus",
		"info_hash=%zz"} {
		if _, err := parseAnnounceQuery(q); err == nil {
			t.Fatal("took", q)
		}
	}
}

func TestAnnounceResponseRoundTrip(t *testing.T) {
	r := &AnnounceResponse{
		Interval: 1800 * time.Second, MinInterval: 60 * time.Second, Complete: 3, Incomplete: 4,
		TrackerID: "tid", Warning: "careful", ExternalIP: netip.MustParseAddr("192.0.2.9"),
		Peers: []Peer{
			{Addr: netip.MustParseAddrPort("192.0.2.1:1"), ID: PeerID{1}},
			{Addr: netip.MustParseAddrPort("[2001:db8::1]:2"), ID: PeerID{2}},
			{Addr: netip.MustParseAddrPort("192.0.2.3:3"), ID: PeerID{3}},
		},
	}
	got, err := parseAnnounceResponse(encodeAnnounce(r, false, false))
	if err != nil || !reflect.DeepEqual(got, r) {
		t.Fatalf("dictionaries: %+v %v", got, err)
	}
	got, err = parseAnnounceResponse(encodeAnnounce(r, true, false))
	if err != nil {
		t.Fatal(err)
	}
	want := *r
	want.Peers = []Peer{{Addr: r.Peers[0].Addr}, {Addr: r.Peers[2].Addr}, {Addr: r.Peers[1].Addr}}
	if !reflect.DeepEqual(got, &want) {
		t.Fatalf("compact: %+v", got)
	}
	got, _ = parseAnnounceResponse(encodeAnnounce(r, false, true))
	if got.Peers[0].ID != (PeerID{}) {
		t.Fatal("no_peer_id")
	}
	var te *Error
	if _, err := parseAnnounceResponse(encodeFailure("go away")); !errors.As(err, &te) || te.Reason != "go away" {
		t.Fatal(err)
	}
	// A failure among keys of unexpected types, and a hostname peer.
	if _, err := parseAnnounceResponse([]byte("d14:failure reason3:bad8:intervalli1eee")); !errors.As(err, &te) || te.Reason != "bad" {
		t.Fatal(err)
	}
	got, err = parseAnnounceResponse([]byte("d8:intervali-5e5:peersld2:ip9:host.name4:porti1eed2:ip4:\xc0\x00\x02\x014:porti2eeee"))
	if err != nil || got.Interval != 0 || len(got.Peers) != 1 || got.Peers[0].Addr != netip.MustParseAddrPort("192.0.2.1:2") {
		t.Fatal(got, err)
	}
}

func TestScrapeRoundTrip(t *testing.T) {
	files := map[metainfo.InfoHash]Stats{{1}: {1, 2, 3}, {2}: {}}
	got, err := parseScrapeResponse(encodeScrape(files))
	if err != nil || !reflect.DeepEqual(got, files) {
		t.Fatal(got, err)
	}
	u := scrapeURL("http://t/scrape?k=v", []metainfo.InfoHash{{1}, {' '}})
	q, _ := url.Parse(u)
	hashes, err := parseScrapeQuery(q.RawQuery, 1)
	if err != nil || len(hashes) != 1 || hashes[0] != (metainfo.InfoHash{1}) {
		t.Fatal(u, hashes, err)
	}
}

func TestScrapeURL(t *testing.T) {
	for in, want := range map[string]string{
		"http://t.example/announce":             "http://t.example/scrape",
		"http://t.example/x/announce.php?k=1":   "http://t.example/x/scrape.php?k=1",
		"https://t.example/key/announce":        "https://t.example/key/scrape",
		"http://t.example/a":                    "",
		"http://t.example/announce/x":           "",
		"udp://t.example:80/announce?passkey=1": "udp://t.example:80/scrape?passkey=1",
	} {
		got, err := ScrapeURL(in)
		if want == "" {
			if !errors.Is(err, ErrNoScrape) {
				t.Fatal(in, got, err)
			}
		} else if got != want || err != nil {
			t.Fatal(in, got, err)
		}
	}
}

func TestUDPAnnounceRoundTrip(t *testing.T) {
	r := sampleRequest()
	r.TrackerID, r.IPv4, r.IPv6 = "", netip.Addr{}, netip.Addr{}
	urlData := "/" + strings.Repeat("p", 300) + "?passkey=x"
	b := appendUDPAnnounce(nil, 77, 99, r, urlData)
	if len(b) != udpAnnounceLen+2+255+2+(len(urlData)-255) {
		t.Fatal(len(b))
	}
	got, err := parseUDPRequest(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.conn != 77 || got.tx != 99 || got.action != actionAnnounce || got.urlData != urlData || got.numWant != 30 {
		t.Fatalf("%+v", got)
	}
	if !reflect.DeepEqual(got.announce, *r) {
		t.Fatalf("got  %+v\nwant %+v", got.announce, *r)
	}
	// NOPs, an unknown option and an end option.
	b = append(b[:udpAnnounceLen], optionNOP, optionURLData, 2, 'a', 'b', 9, 1, 'z', optionURLData, 1, 'c', optionEnd, optionURLData, 1, 'd')
	if got, _ := parseUDPRequest(b); got.urlData != "abc" {
		t.Fatal(got.urlData)
	}
	r.NumWant = -1
	if got, _ := parseUDPRequest(appendUDPAnnounce(nil, 0, 0, r, "")); got.numWant != 0 {
		t.Fatal(got.numWant)
	}
	r.NumWant = 0
	if got, _ := parseUDPRequest(appendUDPAnnounce(nil, 0, 0, r, "")); got.numWant != -1 {
		t.Fatal(got.numWant)
	}
}

func TestUDPScrapeRoundTrip(t *testing.T) {
	hashes := make([]metainfo.InfoHash, 80)
	for i := range hashes {
		hashes[i][0] = byte(i)
	}
	got, err := parseUDPRequest(appendUDPScrape(nil, 1, 2, hashes))
	if err != nil || len(got.hashes) != maxScrapeHashes || got.hashes[73] != hashes[73] {
		t.Fatal(got, err)
	}
}

// TestServeUDP drives the UDP tracker without sockets: connection IDs,
// their lifetime, and the answers of each family.
func TestServeUDP(t *testing.T) {
	s := NewServer(ServerConfig{Interval: 100 * time.Second})
	now := time.Unix(6000, 0)
	from := netip.MustParseAddrPort("192.0.2.1:5000")
	out := s.serveUDP(nil, from, appendConnect(nil, 42), now)
	if len(out) != 16 {
		t.Fatal(out)
	}
	r, _ := parseUDPReply(out)
	if r.action != actionConnect || r.tx != 42 {
		t.Fatal(r)
	}
	conn := uint64(0)
	for _, b := range r.body {
		conn = conn<<8 | uint64(b)
	}
	if out := s.serveUDP(nil, from, appendUDPHeader(nil, 1, actionConnect, 1), now); len(out) != 0 {
		t.Fatal("connect without the protocol ID answered")
	}
	req := &AnnounceRequest{InfoHash: metainfo.InfoHash{1}, PeerID: PeerID{1}, Port: 1, Left: 1}
	for _, tc := range []struct {
		from  netip.AddrPort
		after time.Duration
		ok    bool
	}{
		{from, 0, true}, {from, 119 * time.Second, true}, {from, 120 * time.Second, false},
		{netip.MustParseAddrPort("192.0.2.1:5001"), 0, false}, {netip.MustParseAddrPort("192.0.2.2:5000"), 0, false},
	} {
		out := s.serveUDP(nil, tc.from, appendUDPAnnounce(nil, conn, 7, req, ""), now.Add(tc.after))
		if (len(out) > 0) != tc.ok {
			t.Fatal(tc, out)
		}
	}
	// IPv6 peers to an IPv6 client.
	from6 := netip.MustParseAddrPort("[2001:db8::1]:5000")
	r, _ = parseUDPReply(s.serveUDP(nil, from6, appendConnect(nil, 1), now))
	conn6 := uint64(0)
	for _, b := range r.body {
		conn6 = conn6<<8 | uint64(b)
	}
	s.serveUDP(nil, netip.MustParseAddrPort("[2001:db8::2]:1"), nil, now)
	for i := byte(2); i < 4; i++ {
		req.PeerID = PeerID{i}
		req.Port = uint16(i)
		out := s.serveUDP(nil, from6, appendUDPAnnounce(nil, conn6, 7, req, ""), now)
		r, _ = parseUDPReply(out)
		resp, err := parseUDPAnnounceReply(r.body, true)
		if err != nil || resp.Interval != 100*time.Second || len(resp.Peers) != int(i)-2 {
			t.Fatal(resp, err)
		}
		if i == 3 && resp.Peers[0].Addr != netip.MustParseAddrPort("[2001:db8::1]:2") {
			t.Fatal(resp.Peers)
		}
	}
	// An error answer for an unregistered torrent of a private tracker.
	s = NewServer(ServerConfig{AllowInfoHash: func(metainfo.InfoHash) bool { return false }})
	r, _ = parseUDPReply(s.serveUDP(nil, from, appendConnect(nil, 1), now))
	conn = 0
	for _, b := range r.body {
		conn = conn<<8 | uint64(b)
	}
	r, _ = parseUDPReply(s.serveUDP(nil, from, appendUDPAnnounce(nil, conn, 9, req, ""), now))
	if r.action != actionError || r.tx != 9 || string(r.body) != "unregistered torrent" {
		t.Fatal(r)
	}
}

func TestStoreBounds(t *testing.T) {
	m := NewMemoryStore(MemoryStoreConfig{MaxTorrents: 2, MaxPeersPerTorrent: 3, MaxPeers: 5})
	addr := func(i int) netip.AddrPort { return netip.AddrPortFrom(netip.MustParseAddr("192.0.2.1"), uint16(i)) }
	ann := func(h byte, i int, seeder bool) (StoreResult, error) {
		return m.Announce(StoreAnnounce{
			InfoHash: metainfo.InfoHash{h}, PeerID: PeerID{byte(i)}, V4: addr(i), Seeder: seeder,
			NumWant: 50, WantV4: true,
		})
	}
	for i := 1; i <= 4; i++ {
		res, err := ann(1, i, i%2 == 0)
		if err != nil {
			t.Fatal(err)
		}
		if i == 4 && (res.Complete+res.Incomplete != 3 || len(res.Peers) != 2) {
			t.Fatalf("peer past the torrent's bound: %+v", res)
		}
	}
	if _, err := ann(2, 1, false); err != nil {
		t.Fatal(err)
	}
	if _, err := ann(3, 1, false); !errors.Is(err, ErrStoreFull) {
		t.Fatal(err)
	}
	for i := 2; i <= 3; i++ {
		_, _ = ann(2, i, false)
	}
	if m.Peers() != 5 || m.Torrents() != 2 {
		t.Fatal(m.Peers(), m.Torrents())
	}
	// Stopping frees room; a stopped peer of an unknown torrent makes none.
	if _, err := m.Announce(StoreAnnounce{InfoHash: metainfo.InfoHash{2}, PeerID: PeerID{1}, Event: EventStopped}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Announce(StoreAnnounce{InfoHash: metainfo.InfoHash{9}, PeerID: PeerID{1}, Event: EventStopped}); err != nil {
		t.Fatal(err)
	}
	if m.Peers() != 4 || m.Torrents() != 2 {
		t.Fatal(m.Peers(), m.Torrents())
	}
}

// TestStorePick checks that peers are handed out at random, each once,
// never the asker, and to a seeder only leechers.
func TestStorePick(t *testing.T) {
	m := NewMemoryStore(MemoryStoreConfig{})
	h := metainfo.InfoHash{7}
	for i := 0; i < 100; i++ {
		_, _ = m.Announce(StoreAnnounce{
			InfoHash: h, PeerID: PeerID{byte(i)}, Seeder: i >= 60,
			V4: netip.AddrPortFrom(netip.MustParseAddr("192.0.2.1"), uint16(1000+i)),
			V6: netip.AddrPortFrom(netip.MustParseAddr("2001:db8::1"), uint16(1000+i)),
		})
	}
	seen := map[netip.AddrPort]int{}
	for range 50 {
		res, _ := m.Announce(StoreAnnounce{InfoHash: h, PeerID: PeerID{0}, NumWant: 10, WantV4: true})
		if len(res.Peers) != 10 {
			t.Fatal(res.Peers)
		}
		once := map[netip.AddrPort]bool{}
		for _, p := range res.Peers {
			if once[p.Addr] || p.ID == (PeerID{0}) || !p.Addr.Addr().Is4() {
				t.Fatal(res.Peers)
			}
			once[p.Addr] = true
			seen[p.Addr]++
		}
	}
	if len(seen) < 80 {
		t.Fatal("peers handed out:", len(seen))
	}
	res, _ := m.Announce(StoreAnnounce{InfoHash: h, PeerID: PeerID{99}, Seeder: true, NumWant: 1000, WantV4: true, WantV6: true})
	if len(res.Peers) != 120 {
		t.Fatal(len(res.Peers))
	}
	for _, p := range res.Peers {
		if p.Addr.Port() >= 1060 {
			t.Fatal("seeder got a seeder", p)
		}
	}
}

func TestTiersPromote(t *testing.T) {
	tiers := NewTiers([][]string{{"a", "b", "c"}})
	tiers.promote("c", "id")
	tiers.promote("b", "")
	if l := tiers.List()[0]; l[0] != "b" || l[1] != "c" || tiers.ids["c"] != "id" {
		t.Fatal(l)
	}
}

func TestParseOptionsBounded(t *testing.T) {
	var b []byte
	for range 20 {
		b = append(b, optionURLData, 255)
		b = append(b, bytes.Repeat([]byte{'x'}, 255)...)
	}
	if got := parseOptions(b); len(got) > maxURLData {
		t.Fatal(len(got))
	}
}

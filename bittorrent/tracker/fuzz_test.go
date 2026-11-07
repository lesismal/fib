package tracker

import (
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/bittorrent/metainfo"
)

// FuzzServeUDP feeds the UDP tracker requests from the network: it must
// not panic, and answer nothing bigger than a datagram.
func FuzzServeUDP(f *testing.F) {
	r := sampleRequest()
	f.Add(appendConnect(nil, 1))
	f.Add(appendUDPAnnounce(nil, 5, 1, r, "/announce?passkey=x"))
	f.Add(appendUDPScrape(nil, 5, 1, []metainfo.InfoHash{{1}, {2}}))
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 2, 255, 1})
	s := NewServer(ServerConfig{Secret: []byte("fuzz")})
	from := netip.MustParseAddrPort("192.0.2.1:1")
	now := time.Unix(1e9, 0)
	valid := s.connectionID(from, now.Unix()/60)
	f.Fuzz(func(t *testing.T, b []byte) {
		if req, err := parseUDPRequest(b); err == nil && len(req.urlData) > maxURLData {
			t.Fatal("url data", len(req.urlData))
		}
		_ = s.serveUDP(nil, from, b, now)
		// The same packet with a valid connection ID reaches the actions.
		if len(b) >= 8 {
			c := append([]byte(nil), b...)
			for i := range 8 {
				c[i] = byte(valid >> (56 - 8*i))
			}
			if out := s.serveUDP(nil, from, c, now); len(out) > maxUDPReply {
				t.Fatal("answer of", len(out))
			}
		}
		_ = parseOptions(b)
	})
}

// FuzzUDPReply feeds a client replies from the network.
func FuzzUDPReply(f *testing.F) {
	f.Add(appendUDPReplyHeader(nil, actionConnect, 0), false)
	f.Add(append(appendUDPReplyHeader(nil, actionAnnounce, 0), make([]byte, 12+18)...), true)
	f.Add(append(appendUDPReplyHeader(nil, actionScrape, 0), make([]byte, 24)...), false)
	f.Add(appendUDPError(nil, 0, "nope"), false)
	hashes := []metainfo.InfoHash{{1}, {2}}
	f.Fuzz(func(t *testing.T, b []byte, v6 bool) {
		c := NewUDPClient(UDPClientConfig{Send: func(netip.AddrPort, []byte) error { return nil }})
		w := &udpWait{to: netip.MustParseAddrPort("192.0.2.1:1"), ch: make(chan []byte, 1)}
		c.mu.Lock()
		c.pending[0] = w
		c.mu.Unlock()
		took := c.HandleDatagram(w.to, b)
		r, ok := parseUDPReply(b)
		if ok != IsUDPReply(b) || took != (ok && r.tx == 0) {
			t.Fatal("IsUDPReply and HandleDatagram disagree:", ok, took)
		}
		if !ok {
			return
		}
		if resp, err := parseUDPAnnounceReply(r.body, v6); err == nil {
			for _, p := range resp.Peers {
				if !p.Addr.IsValid() || p.Addr.Port() == 0 {
					t.Fatal(p)
				}
			}
		}
		_ = parseUDPScrapeReply(r.body, hashes)
	})
}

// FuzzHTTPAnnounce feeds the HTTP tracker announce and scrape queries,
// and checks that what it parses is what an announce URL would carry.
func FuzzHTTPAnnounce(f *testing.F) {
	f.Add(strings.TrimPrefix(announceURL("http://t/a", sampleRequest()), "http://t/a?"))
	f.Add("info_hash=%00%01&peer_id=x&port=1&ipv6=[::1]:2&ip=1.2.3.4&numwant=-7&key=ffffffff")
	f.Add("info_hash=12345678901234567890&peer_id=12345678901234567890&port=1&compact=0")
	s := NewServer(ServerConfig{})
	f.Fuzz(func(t *testing.T, q string) {
		remote := netip.MustParseAddrPort("192.0.2.1:1")
		_ = s.httpAnnounce(remote, "/announce", q)
		_ = s.httpScrape(remote, "/scrape", q)
		a, err := parseAnnounceQuery(q)
		if err != nil {
			return
		}
		b, err := parseAnnounceQuery(strings.TrimPrefix(announceURL("http://t/a", &a.AnnounceRequest), "http://t/a?"))
		if err != nil {
			t.Fatal(err)
		}
		if b.InfoHash != a.InfoHash || b.PeerID != a.PeerID || b.Port != a.Port || b.Event != a.Event ||
			b.Uploaded != a.Uploaded || b.Downloaded != a.Downloaded || b.Key != a.Key && a.Key != 0 {
			t.Fatalf("%+v\n%+v", a.AnnounceRequest, b.AnnounceRequest)
		}
	})
}

// FuzzHTTPResponse feeds a client tracker answers from the network.
func FuzzHTTPResponse(f *testing.F) {
	f.Add(encodeAnnounce(&AnnounceResponse{Interval: time.Minute, Peers: []Peer{
		{Addr: netip.MustParseAddrPort("192.0.2.1:1")}, {Addr: netip.MustParseAddrPort("[::1]:2")},
	}}, true, false))
	f.Add(encodeAnnounce(&AnnounceResponse{Peers: []Peer{{Addr: netip.MustParseAddrPort("192.0.2.1:1")}}}, false, false))
	f.Add(encodeFailure("x"))
	f.Add(encodeScrape(map[metainfo.InfoHash]Stats{{1}: {1, 2, 3}}))
	f.Fuzz(func(t *testing.T, b []byte) {
		if r, err := parseAnnounceResponse(b); err == nil {
			for _, p := range r.Peers {
				if !p.Addr.IsValid() || p.Addr.Port() == 0 || p.Addr.Addr().Is4In6() {
					t.Fatal(p)
				}
			}
			if r.Interval < 0 || r.Interval > maxInterval*time.Second {
				t.Fatal(r.Interval)
			}
			// What the server writes of it, the client reads back.
			if _, err := parseAnnounceResponse(encodeAnnounce(r, true, false)); err != nil {
				t.Fatal(err)
			}
		}
		_, _ = parseScrapeResponse(b)
	})
}

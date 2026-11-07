package dht

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"net/netip"
	"testing"
	"time"

	"github.com/lesismal/fib/bittorrent/bencode"
)

// The examples of BEP 5.
func TestKRPCExamples(t *testing.T) {
	port := int64(6881)
	cases := []struct {
		m    msg
		wire string
	}{
		{msg{T: "aa", Y: "q", Q: "ping", A: &args{ID: []byte("abcdefghij0123456789")}},
			"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe"},
		{msg{T: "aa", Y: "r", R: &ret{ID: []byte("mnopqrstuvwxyz123456")}},
			"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re"},
		{msg{T: "aa", Y: "e", E: &Error{Code: 201, Msg: "A Generic Error Ocurred"}},
			"d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee"},
		{msg{T: "aa", Y: "q", Q: "find_node", A: &args{ID: []byte("abcdefghij0123456789"), Target: []byte("mnopqrstuvwxyz123456")}},
			"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe"},
		{msg{T: "aa", Y: "q", Q: "get_peers", A: &args{ID: []byte("abcdefghij0123456789"), InfoHash: []byte("mnopqrstuvwxyz123456")}},
			"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe"},
		{msg{T: "aa", Y: "r", R: &ret{ID: []byte("abcdefghij0123456789"), Token: []byte("aoeusnth"), Values: [][]byte{[]byte("axje.u"), []byte("idhtnm")}}},
			"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re"},
		{msg{T: "aa", Y: "q", Q: "announce_peer", A: &args{ID: []byte("abcdefghij0123456789"), ImpliedPort: 1, InfoHash: []byte("mnopqrstuvwxyz123456"), Port: &port, Token: []byte("aoeusnth")}},
			"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe"},
	}
	for _, c := range cases {
		if got := string(c.m.encode()); got != c.wire {
			t.Errorf("encode:\n got %s\nwant %s", got, c.wire)
		}
		m, err := parseMsg([]byte(c.wire))
		if err != nil {
			t.Fatalf("parse %s: %v", c.wire, err)
		}
		if got := string(m.encode()); got != c.wire {
			t.Errorf("round trip:\n got %s\nwant %s", got, c.wire)
		}
	}
}

func TestParseMsgRejects(t *testing.T) {
	for _, s := range []string{
		"", "le", "de", "d1:t2:aa1:y1:qe", // query without q and a
		"d1:rd2:id3:abce1:t2:aa1:y1:re", // short id
		"d1:t0:1:y1:re",                 // empty t
		"d1:t2:aa1:y1:xe",               // unknown y
		"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:q", // cut short
	} {
		if _, err := parseMsg([]byte(s)); err == nil {
			t.Errorf("parseMsg(%q) accepted", s)
		}
	}
	// A wrongly typed optional key costs only itself.
	m, err := parseMsg([]byte("d1:ad2:id20:abcdefghij01234567894:port3:abce1:q4:ping1:t2:aa1:y1:qe"))
	if err != nil || m.A.Port != nil && *m.A.Port != 0 {
		t.Fatalf("odd port: %v %+v", err, m)
	}
}

func TestCompact(t *testing.T) {
	nodes := []NodeInfo{
		{ID: RandomID(), Addr: netip.MustParseAddrPort("1.2.3.4:6881")},
		{ID: RandomID(), Addr: netip.MustParseAddrPort("[2001:db8::1]:6882")},
		{ID: RandomID(), Addr: netip.MustParseAddrPort("[::ffff:5.6.7.8]:80")},
	}
	b4 := appendCompactNodes(nil, nodes, false)
	b6 := appendCompactNodes(nil, nodes, true)
	if len(b4) != 2*26 || len(b6) != 38 {
		t.Fatalf("lengths %d %d", len(b4), len(b6))
	}
	n4, err := parseCompactNodes(b4, false)
	if err != nil || len(n4) != 2 || n4[0] != nodes[0] || n4[1].Addr != netip.MustParseAddrPort("5.6.7.8:80") {
		t.Fatalf("v4 %v %v", n4, err)
	}
	n6, err := parseCompactNodes(b6, true)
	if err != nil || len(n6) != 1 || n6[0] != nodes[1] {
		t.Fatalf("v6 %v %v", n6, err)
	}
	if _, err := parseCompactNodes(b4[:25], false); err == nil {
		t.Fatal("odd length accepted")
	}
	a, ok := parseCompactAddr(appendCompactAddr(nil, nodes[1].Addr))
	if !ok || a != nodes[1].Addr {
		t.Fatal(a)
	}
}

// The test vectors of BEP 42.
func TestSecureID(t *testing.T) {
	for _, v := range []struct {
		ip     string
		r      byte
		prefix string
	}{
		{"124.31.75.21", 1, "5fbfbf"},
		{"21.75.31.124", 86, "5a3ce9"},
		{"65.23.51.170", 22, "a5d432"},
		{"84.124.73.14", 65, "1b0321"},
		{"43.213.53.83", 90, "e56f6c"},
	} {
		ip := netip.MustParseAddr(v.ip)
		crc := secureCRC(ip, v.r)
		want, _ := hex.DecodeString(v.prefix)
		if byte(crc>>24) != want[0] || byte(crc>>16) != want[1] || byte(crc>>8)&0xf8 != want[2]&0xf8 {
			t.Errorf("%s: crc %08x, want prefix %s", v.ip, crc, v.prefix)
		}
		var id ID
		copy(id[:], want)
		id[19] = v.r
		if !IDSecure(id, ip) {
			t.Errorf("%s: vector not secure", v.ip)
		}
		for range 10 {
			if s := SecureID(ip); !IDSecure(s, ip) {
				t.Fatalf("SecureID(%s) = %s not secure", ip, s)
			}
		}
	}
	ip := netip.MustParseAddr("124.31.75.21")
	if IDSecure(ID{}, ip) {
		t.Fatal("zero ID secure")
	}
	if !IDSecure(ID{}, netip.MustParseAddr("192.168.1.1")) {
		t.Fatal("local address not exempt")
	}
	ip6 := netip.MustParseAddr("2001:db8:1:2:3:4:5:6")
	if !IDSecure(SecureID(ip6), ip6) {
		t.Fatal("v6")
	}
}

func TestIPVoter(t *testing.T) {
	var v ipVoter
	now := time.Now()
	me := netip.MustParseAddr("9.9.9.9")
	v.add(netip.MustParseAddr("1.1.1.1"), me, now)
	v.add(netip.MustParseAddr("1.1.1.1"), me, now) // one voter, one vote
	v.add(netip.MustParseAddr("1.1.1.2"), netip.MustParseAddr("8.8.8.8"), now)
	if _, ok := v.winner(); ok {
		t.Fatal("winner on too few votes")
	}
	v.add(netip.MustParseAddr("1.1.1.3"), me, now)
	v.add(netip.MustParseAddr("1.1.1.4"), me, now)
	if w, ok := v.winner(); !ok || w != me {
		t.Fatal(w, ok)
	}
}

func TestTokens(t *testing.T) {
	now := time.Now()
	tk := newTokens(now)
	a, b := netip.MustParseAddr("1.2.3.4"), netip.MustParseAddr("1.2.3.5")
	tok := tk.make(a)
	if !tk.valid(tok, a) || tk.valid(tok, b) || tk.valid(nil, a) {
		t.Fatal("token bound to the IP")
	}
	tk.rotate(now.Add(TokenRotate))
	if !tk.valid(tok, a) {
		t.Fatal("token invalid after one rotation")
	}
	tk.rotate(now.Add(2 * TokenRotate))
	if tk.valid(tok, a) {
		t.Fatal("token valid after two rotations")
	}
}

// idAt returns an ID that shares exactly prefix leading bits with self.
func idAt(self ID, prefix int) ID { return randomIDWithPrefix(self, prefix, true) }

func addr(i int) netip.AddrPort {
	return netip.AddrPortFrom(netip.AddrFrom4([4]byte{1, byte(i >> 16), byte(i >> 8), byte(i)}), 6881)
}

func TestTableSplit(t *testing.T) {
	self := RandomID()
	tb := newTable(self)
	now := time.Now()
	// Far nodes (prefix 0) fill bucket 0; near ones force splits.
	for i := range K {
		tb.replied(NodeInfo{ID: idAt(self, 0), Addr: addr(i)}, now)
	}
	if len(tb.buckets) != 1 || tb.len() != K {
		t.Fatalf("buckets %d len %d", len(tb.buckets), tb.len())
	}
	for i := range 4 {
		tb.replied(NodeInfo{ID: idAt(self, 3+i), Addr: addr(100 + i)}, now)
	}
	if len(tb.buckets) < 2 || len(tb.buckets[0].nodes) != K {
		t.Fatalf("no split: %d buckets", len(tb.buckets))
	}
	// Bucket 0 is full of good nodes and cannot split: a newcomer goes to
	// the replacement cache.
	if p := tb.replied(NodeInfo{ID: idAt(self, 0), Addr: addr(200)}, now); p != nil {
		t.Fatal("pinged a good node")
	}
	if len(tb.buckets[0].replacements) != 1 || tb.get(addr(200)) != nil {
		t.Fatal("newcomer not a replacement")
	}
	for _, b := range tb.buckets {
		for _, e := range b.nodes {
			if got := tb.bucketIndex(e.ID); tb.buckets[got] != b {
				t.Fatalf("node in bucket of the wrong range")
			}
		}
	}
	// One node per IP outside local networks.
	if tb.replied(NodeInfo{ID: idAt(self, 9), Addr: netip.AddrPortFrom(addr(100).Addr(), 1)}, now); tb.len() != K+4 {
		t.Fatal("second node at an IP admitted")
	}
	lo := netip.MustParseAddrPort("127.0.0.1:1")
	tb.replied(NodeInfo{ID: idAt(self, 9), Addr: lo}, now)
	tb.replied(NodeInfo{ID: idAt(self, 10), Addr: netip.AddrPortFrom(lo.Addr(), 2)}, now)
	if tb.get(lo) == nil || tb.get(netip.AddrPortFrom(lo.Addr(), 2)) == nil {
		t.Fatal("loopback nodes not admitted")
	}
}

func TestTableEvict(t *testing.T) {
	self := RandomID()
	tb := newTable(self)
	now := time.Now()
	tb.buckets = append(tb.buckets, &bucket{}) // bucket 0 can no longer split
	for i := range K {
		tb.replied(NodeInfo{ID: idAt(self, 0), Addr: addr(i)}, now)
	}
	later := now.Add(GoodFor + time.Minute) // all questionable now
	newcomer := NodeInfo{ID: idAt(self, 0), Addr: addr(50)}
	p := tb.replied(newcomer, later)
	if p == nil || p.Addr != addr(0) {
		t.Fatalf("ping-before-evict picked %v", p)
	}
	if tb.get(newcomer.Addr) != nil {
		t.Fatal("newcomer evicted a node before the ping")
	}
	// The ping fails twice: the node is bad, and the replacement takes its place.
	tb.failed(addr(0), later)
	if tb.get(addr(0)) == nil {
		t.Fatal("evicted after one failure")
	}
	tb.failed(addr(0), later)
	if tb.get(addr(0)) != nil || tb.get(newcomer.Addr) == nil {
		t.Fatal("bad node not replaced")
	}
	// A node that answers again is good again.
	tb.replied(NodeInfo{ID: tb.get(addr(1)).ID, Addr: addr(1)}, later)
	if !tb.get(addr(1)).good(later) {
		t.Fatal("not good after a reply")
	}
	// A bad node makes way at once when there are no replacements.
	tb.failed(addr(2), later)
	tb.failed(addr(2), later)
	if tb.get(addr(2)) == nil {
		t.Fatal("dropped without a replacement")
	}
	n2 := NodeInfo{ID: idAt(self, 0), Addr: addr(60)}
	if p := tb.replied(n2, later); p != nil || tb.get(addr(2)) != nil || tb.get(n2.Addr) == nil {
		t.Fatal("bad node did not make way")
	}
	// Five failures drop a node outright.
	for range dropFailures {
		tb.failed(addr(3), later)
	}
	if tb.get(addr(3)) != nil {
		t.Fatal("dead node kept")
	}
	if got := tb.stale(later, GoodFor); len(got) != 1 { // the empty bucket only
		t.Fatal("fresh buckets stale", len(got))
	}
	for i, target := range tb.stale(later.Add(GoodFor), GoodFor) {
		if tb.bucketIndex(target) != i {
			t.Fatalf("refresh target of bucket %d falls in %d", i, tb.bucketIndex(target))
		}
	}
}

func TestPeerStore(t *testing.T) {
	s := newPeerStore(2, 3)
	now := time.Now()
	ih1, ih2, ih3 := RandomID(), RandomID(), RandomID()
	for i := range 5 {
		s.add(ih1, addr(i), now.Add(time.Duration(i)*time.Second))
	}
	if got := s.get(ih1, 10, false, now); len(got) != 3 {
		t.Fatal(len(got))
	}
	s.add(ih2, addr(9), now)
	s.add(ih3, addr(9), now) // ih2 has the fewest peers and makes way
	if s.torrents[ih2] != nil || s.torrents[ih3] == nil || len(s.torrents) != 2 {
		t.Fatal("torrent bound")
	}
	if got := s.get(ih1, 10, true, now); len(got) != 0 {
		t.Fatal("v4 peers for v6")
	}
	s.expire(now.Add(PeerTTL + time.Hour))
	if len(s.torrents) != 0 {
		t.Fatal("not expired")
	}
}

func TestRateLimiter(t *testing.T) {
	r := rateLimiter{rate: 1, burst: 3, ips: map[netip.Addr]*ipBucket{}}
	ip := addr(1).Addr()
	now := time.Now()
	for range 3 {
		if !r.allow(ip, now) {
			t.Fatal("burst refused")
		}
	}
	if r.allow(ip, now) {
		t.Fatal("over the burst")
	}
	if !r.allow(addr(2).Addr(), now) {
		t.Fatal("another IP refused")
	}
	if !r.allow(ip, now.Add(time.Second)) {
		t.Fatal("not refilled")
	}
}

func TestState(t *testing.T) {
	st := State{ID: RandomID(), Nodes: []NodeInfo{
		{ID: RandomID(), Addr: netip.MustParseAddrPort("1.2.3.4:5")},
		{ID: RandomID(), Addr: netip.MustParseAddrPort("[2001:db8::7]:8")},
	}}
	b, err := bencode.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	var got State
	if err := bencode.Unmarshal(b, &got); err != nil || got.ID != st.ID || len(got.Nodes) != 2 || got.Nodes[1] != st.Nodes[1] {
		t.Fatal(got, err)
	}
}

func TestItems(t *testing.T) {
	it, err := NewImmutableItem("Hello World!")
	if err != nil {
		t.Fatal(err)
	}
	// BEP 44's immutable example.
	if it.Target().String() != "e5f96f6f38320f0f33959cb4d3d656452117aadb" {
		t.Fatal(it.Target())
	}
	// BEP 44's mutable example, with its key pair and signature.
	seed, _ := hex.DecodeString("77ff84905a91936367c01360803104f92432fcd904a43511876df5cdf3e7e548")
	priv := ed25519.NewKeyFromSeed(seed)
	m, err := NewMutableItem("Hello World!", nil, 1, priv)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(m.K[:]) != "77ff84905a91936367c01360803104f92432fcd904a43511876df5cdf3e7e548" &&
		!bytes.Equal(m.K[:], priv.Public().(ed25519.PublicKey)) {
		t.Fatal("key")
	}
	if got := string(m.signedBytes()); got != "3:seqi1e1:v12:Hello World!" {
		t.Fatal(got)
	}
	ms, _ := NewMutableItem("Hello World!", []byte("foobar"), 1, priv)
	if got := string(ms.signedBytes()); got != "4:salt6:foobar3:seqi1e1:v12:Hello World!" {
		t.Fatal(got)
	}
	m.Sig[0] ^= 1
	if m.Verify() != ErrItemSignature {
		t.Fatal("bad signature accepted")
	}
	s := &itemStore{items: map[ID]*storedItem{}, max: 2}
	now := time.Now()
	m2, _ := NewMutableItem("v2", nil, 2, priv)
	m3, _ := NewMutableItem("v3", nil, 3, priv)
	if err := s.put(m2, nil, now); err != nil {
		t.Fatal(err)
	}
	m1, _ := NewMutableItem("v1", nil, 1, priv)
	if err := s.put(m1, nil, now); err != ErrItemSeqTooLow {
		t.Fatal(err)
	}
	cas := int64(1)
	if err := s.put(m3, &cas, now); err != ErrItemCASMismatch {
		t.Fatal(err)
	}
	cas = 2
	if err := s.put(m3, &cas, now); err != nil {
		t.Fatal(err)
	}
	if got := s.get(m3.Target(), now); got == nil || got.Seq != 3 {
		t.Fatal(got)
	}
	if s.get(m3.Target(), now.Add(ItemTTL)) != nil {
		t.Fatal("expired item returned")
	}
}

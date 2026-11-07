package dht

import (
	"net/netip"
	"testing"
	"time"
)

var fuzzSeeds = []string{
	"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe",
	"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re",
	"d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee",
	"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe",
	"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe",
	"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe",
	"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q17:sample_infohashes1:t2:aa1:y1:qe",
	"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz1234564:wantl2:n42:n6ee1:q3:get1:t2:aa1:y1:qe",
	"d1:ad2:id20:abcdefghij01234567893:seqi1e3:sig64:" + string(make([]byte, 64)) + "1:k32:" + string(make([]byte, 32)) + "5:token8:aoeusnth1:v1:xe1:q3:put1:t2:aa1:y1:qe",
	"d2:ip6:\x01\x02\x03\x04\x1a\xe11:rd2:id20:mnopqrstuvwxyz1234565:nodes26:abcdefghij0123456789\x7f\x00\x00\x01\x1a\xe16:valuesl6:axje.ue5:token2:xxe1:t2:aa1:y1:re",
}

func FuzzParseMsg(f *testing.F) {
	for _, s := range fuzzSeeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := parseMsg(b)
		if err != nil {
			return
		}
		// What parses encodes, and parses again the same.
		m2, err := parseMsg(m.encode())
		if err != nil {
			t.Fatalf("re-parse: %v", err)
		}
		if string(m2.encode()) != string(m.encode()) {
			t.Fatal("encoding not stable")
		}
		if m.R != nil {
			_, _ = parseCompactNodes(m.R.Nodes, false)
			_, _ = parseCompactNodes(m.R.Nodes6, true)
		}
	})
}

// FuzzHandleDatagram feeds datagrams to a node, as queries and as
// responses to a query of its own.
func FuzzHandleDatagram(f *testing.F) {
	for _, s := range fuzzSeeds {
		f.Add([]byte(s))
	}
	n, err := New(Config{Routers: []string{}, QueryRate: -1, Send: func(netip.AddrPort, []byte) {}})
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { _ = n.Close() })
	from := netip.MustParseAddrPort("1.2.3.4:5678")
	f.Fuzz(func(t *testing.T, b []byte) {
		// A datagram that parses as a response answers a query waiting
		// for it.
		n.mu.Lock()
		for k, x := range n.txns {
			x.stop()
			delete(n.txns, k)
		}
		if m, err := parseMsg(b); err == nil && m.Y != "q" {
			k := txnKey{addr: from, t: m.T}
			n.txns[k] = &txn{key: k, timer: time.AfterFunc(time.Hour, func() {})}
		}
		n.mu.Unlock()
		n.HandleDatagram(from, b)
	})
}

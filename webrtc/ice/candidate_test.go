package ice

import (
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"
)

func TestParseCandidate(t *testing.T) {
	for _, tc := range []struct {
		in   string
		out  string // String() when it differs from in
		want Candidate
	}{
		{
			in: "candidate:842163049 1 udp 1677729535 203.0.113.1 54400 typ srflx raddr 0.0.0.0 rport 0 generation 0 ufrag EsAw network-id 1 network-cost 10",
			want: Candidate{Foundation: "842163049", Component: 1, Transport: "udp", Priority: 1677729535,
				Address: netip.MustParseAddr("203.0.113.1"), Port: 54400, Type: ServerReflexive,
				RelatedAddress: netip.MustParseAddrPort("0.0.0.0:0"),
				Extensions:     []Extension{{"generation", "0"}, {"ufrag", "EsAw"}, {"network-id", "1"}, {"network-cost", "10"}}},
		},
		{
			in:  "a=candidate:0 1 UDP 2122252543 0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0.local 54321 typ host",
			out: "candidate:0 1 udp 2122252543 0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0.local 54321 typ host",
			want: Candidate{Foundation: "0", Component: 1, Transport: "udp", Priority: 2122252543,
				Hostname: "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0.local", Port: 54321, Type: Host},
		},
		{
			in: "candidate:1 1 tcp 2105458943 192.168.1.2 9 typ host tcptype active",
			want: Candidate{Foundation: "1", Component: 1, Transport: "tcp", Priority: 2105458943,
				Address: netip.MustParseAddr("192.168.1.2"), Port: 9, Type: Host, Extensions: []Extension{{"tcptype", "active"}}},
		},
		{
			in: "candidate:2 2 udp 2122262782 2001:db8::1 50000 typ host generation 0",
			want: Candidate{Foundation: "2", Component: 2, Transport: "udp", Priority: 2122262782,
				Address: netip.MustParseAddr("2001:db8::1"), Port: 50000, Type: Host, Extensions: []Extension{{"generation", "0"}}},
		},
		{
			in: "candidate:a+/Z 1 udp 16777215 198.51.100.3 3478 typ relay raddr 203.0.113.1 rport 54400",
			want: Candidate{Foundation: "a+/Z", Component: 1, Transport: "udp", Priority: 16777215,
				Address: netip.MustParseAddr("198.51.100.3"), Port: 3478, Type: Relay,
				RelatedAddress: netip.MustParseAddrPort("203.0.113.1:54400")},
		},
		{
			// A related port with no address stands for 0.0.0.0.
			in:  "candidate:4 1 udp 1 192.0.2.5 6000 typ srflx rport 9",
			out: "candidate:4 1 udp 1 192.0.2.5 6000 typ srflx raddr 0.0.0.0 rport 9",
			want: Candidate{Foundation: "4", Component: 1, Transport: "udp", Priority: 1,
				Address: netip.MustParseAddr("192.0.2.5"), Port: 6000, Type: ServerReflexive,
				RelatedAddress: netip.MustParseAddrPort("0.0.0.0:9")},
		},
		{
			in: "candidate:3 1 udp 1845501695 192.0.2.5 6000 typ prflx raddr host.local rport 7",
			want: Candidate{Foundation: "3", Component: 1, Transport: "udp", Priority: 1845501695,
				Address: netip.MustParseAddr("192.0.2.5"), Port: 6000, Type: PeerReflexive,
				RelatedHostname: "host.local", RelatedAddress: netip.AddrPortFrom(netip.Addr{}, 7)},
		},
	} {
		c, err := ParseCandidate(tc.in)
		if err != nil {
			t.Fatalf("%q: %v", tc.in, err)
		}
		if c.String() != tc.want.String() || c.Hostname != tc.want.Hostname || c.Address != tc.want.Address ||
			c.RelatedAddress != tc.want.RelatedAddress || len(c.Extensions) != len(tc.want.Extensions) {
			t.Fatalf("%q:\n got %+v\nwant %+v", tc.in, c, tc.want)
		}
		out := tc.out
		if out == "" {
			out = tc.in
		}
		if c.String() != out {
			t.Fatalf("String() = %q, want %q", c.String(), out)
		}
	}
}

func TestParseCandidateErrors(t *testing.T) {
	for _, in := range []string{
		"",
		"candidate:",
		"cand:1 1 udp 1 1.2.3.4 5 typ host",
		"candidate:1 1 udp 1 1.2.3.4 5 typ",
		"candidate:1 0 udp 1 1.2.3.4 5 typ host",
		"candidate:1 1 udp x 1.2.3.4 5 typ host",
		"candidate:1 1 udp 1 1.2.3.4 65536 typ host",
		"candidate:1 1 udp 1 1.2.3.4 5 type host",
		"candidate:1 1 udp 1 1.2.3.4 5 typ nat",
		"candidate:1 1 udp 1 1.2.3.4 5 typ host raddr",
		"candidate:1 1 udp 1 1.2.3.4 5 typ host rport x",
		"candidate:f*o 1 udp 1 1.2.3.4 5 typ host",
		"candidate:1 1 udp 1 1:2:3 5 typ host",
		"candidate:012345678901234567890123456789012 1 udp 1 1.2.3.4 5 typ host",
	} {
		if _, err := ParseCandidate(in); !errors.Is(err, ErrCandidateSyntax) {
			t.Errorf("%q: %v", in, err)
		}
	}
}

// A candidate built rather than parsed may leave Transport empty: it is
// UDP, the only transport an Agent speaks.
func TestCandidateDefaultTransport(t *testing.T) {
	c := Candidate{Foundation: "1", Component: 1, Priority: 5, Address: netip.MustParseAddr("192.0.2.1"), Port: 9, Type: Host}
	if got, want := c.String(), "candidate:1 1 udp 5 192.0.2.1 9 typ host"; got != want {
		t.Fatalf("%q, want %q", got, want)
	}
	if _, ok := c.Extension("ufrag"); ok {
		t.Fatal("an extension it has not")
	}
}

func TestPriority(t *testing.T) {
	// The priorities a browser gives its host and srflx candidates.
	if p := Priority(Host, 65535, 1); p != 2130706431 {
		t.Fatalf("host %d", p)
	}
	if p := Priority(ServerReflexive, 65535, 1); p != 1694498815 {
		t.Fatalf("srflx %d", p)
	}
	if p := Priority(Relay, 0, 2); p != 254 {
		t.Fatalf("relay %d", p)
	}
	for _, typ := range []CandidateType{Host, ServerReflexive, PeerReflexive, Relay, 9} {
		if typ.String() == "" {
			t.Fatal("empty type name")
		}
	}
}

func TestPairPriority(t *testing.T) {
	a := &Agent{controlling: true}
	l, r := &Candidate{Priority: 10}, &Candidate{Priority: 20}
	// RFC 8445 section 6.1.2.3: 2^32*MIN(G,D) + 2*MAX(G,D) + (G>D?1:0).
	if p := a.pairPriority(l, r); p != 10<<32+40 {
		t.Fatalf("controlling %d", p)
	}
	a.controlling = false
	if p := a.pairPriority(l, r); p != 10<<32+40+1 {
		t.Fatalf("controlled %d", p)
	}
}

func FuzzParseCandidate(f *testing.F) {
	f.Add("candidate:842163049 1 udp 1677729535 203.0.113.1 54400 typ srflx raddr 0.0.0.0 rport 0 generation 0")
	f.Add("a=candidate:0 1 UDP 2122252543 abc.local 54321 typ host")
	f.Add("candidate:1 1 tcp 2105458943 192.168.1.2 9 typ host tcptype active")
	f.Add("candidate:2 1 udp 1 2001:db8::1 50000 typ relay raddr ::1 rport 1")
	f.Fuzz(func(t *testing.T, s string) {
		c, err := ParseCandidate(s)
		if err != nil {
			return
		}
		again, err := ParseCandidate(c.String())
		if err != nil {
			t.Fatalf("%q printed as %q, which does not parse: %v", s, c.String(), err)
		}
		if again.String() != c.String() {
			t.Fatalf("%q -> %q -> %q", s, c.String(), again.String())
		}
	})
}

func TestHostAddr(t *testing.T) {
	ipnet := func(s string) net.Addr {
		_, n, err := net.ParseCIDR(s)
		if err != nil {
			t.Fatal(err)
		}
		n.IP = net.ParseIP(strings.Split(s, "/")[0])
		return n
	}
	for _, tc := range []struct {
		addr net.Addr
		want string
	}{
		{ipnet("192.168.1.2/24"), "192.168.1.2"},
		{&net.IPNet{IP: net.ParseIP("192.168.1.2"), Mask: net.CIDRMask(24, 32)}, "192.168.1.2"}, // 16-byte, v4-mapped
		{ipnet("2001:db8::7/64"), "2001:db8::7"},
		{ipnet("127.0.0.1/8"), ""},
		{ipnet("169.254.1.1/16"), ""},
		{ipnet("fe80::1/64"), ""},
		{ipnet("224.0.0.251/32"), ""},
		{ipnet("0.0.0.0/0"), ""},
		{&net.IPNet{IP: net.IP{1, 2, 3}}, ""},
		{&net.IPAddr{IP: net.ParseIP("192.168.1.2")}, ""},
	} {
		ip, ok := hostAddr(tc.addr)
		if got := ""; ok {
			got = ip.String()
			if got != tc.want {
				t.Errorf("%v: %v, want %q", tc.addr, got, tc.want)
			}
		} else if tc.want != "" {
			t.Errorf("%v: refused, want %q", tc.addr, tc.want)
		}
	}
	addrs, err := HostAddrs()
	if err != nil {
		t.Fatal(err)
	}
	for _, ip := range addrs {
		if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.Is4In6() {
			t.Errorf("HostAddrs listed %v", ip)
		}
	}
}

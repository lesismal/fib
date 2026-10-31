package turn

import (
	"bytes"
	"net/netip"
	"testing"
	"time"

	"github.com/lesismal/fib/webrtc/stun"
)

// core drives a Server directly, building each request by hand.
type core struct {
	t      *testing.T
	s      *Server
	now    time.Time
	sent   []coreSent
	relays map[netip.AddrPort]*coreRelay
	port   uint16
}

type coreSent struct {
	t FiveTuple
	b []byte
}

type coreRelay struct {
	c      *core
	addr   netip.AddrPort
	sent   []coreSent // t.Client is the peer
	closed bool
}

func (r *coreRelay) Addr() netip.AddrPort { return r.addr }
func (r *coreRelay) SendTo(peer netip.AddrPort, b []byte) {
	r.sent = append(r.sent, coreSent{FiveTuple{peer, r.addr}, bytes.Clone(b)})
}
func (r *coreRelay) Close() { r.closed = true }

func newCore(t *testing.T, cfg ServerConfig) *core {
	c := &core{t: t, now: time.Unix(1_700_000_000, 0), relays: map[netip.AddrPort]*coreRelay{}, port: 50000}
	cfg.Realm = testRealm
	if cfg.Auth == nil {
		cfg.Auth = StaticAuth(testRealm, map[string]string{testUser: testPass, "bob": "hunter2"})
	}
	cfg.Send = func(ft FiveTuple, b []byte) { c.sent = append(c.sent, coreSent{ft, bytes.Clone(b)}) }
	cfg.AllocateRelay = func(FiveTuple, string, bool) (Relay, error) {
		r := &coreRelay{c: c, addr: netip.AddrPortFrom(relayIP, c.port)}
		c.port++
		c.relays[r.addr] = r
		return r, nil
	}
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.s = s
	return c
}

// user is one client of a core, with the credentials it has learned.
type user struct {
	c          *core
	addr       netip.AddrPort
	name, pass string
	nonce      string
	key        []byte
}

func (c *core) user(addr netip.AddrPort, name, pass string) *user {
	return &user{c: c, addr: addr, name: name, pass: pass, key: stun.LongTermKey(name, testRealm, pass)}
}

func (u *user) tuple() FiveTuple { return FiveTuple{u.addr, serverAddr} }

// do sends a request of method with the attributes add adds, with
// credentials, learning the nonce from a 401 or 438 first, and returns the
// response.
func (u *user) do(method stun.Method, add func(m *stun.Message)) *stun.Message {
	u.c.t.Helper()
	return u.doID(method, stun.NewTransactionID(), add)
}

func (u *user) doID(method stun.Method, id [12]byte, add func(m *stun.Message)) *stun.Message {
	u.c.t.Helper()
	for range 3 {
		var m stun.Message
		m.Build(stun.MessageType{Method: method, Class: stun.ClassRequest}, id)
		if add != nil {
			add(&m)
		}
		if u.nonce != "" {
			m.AddString(stun.AttrUsername, u.name)
			m.AddString(stun.AttrRealm, testRealm)
			m.AddString(stun.AttrNonce, u.nonce)
			m.AddIntegrity(u.key)
		}
		m.AddFingerprint()
		r := u.c.send(u.tuple(), m.Raw)
		if r.Type.Class == stun.ClassErrorResponse {
			code, _, _ := r.ErrorCode()
			if nonce, ok := r.Get(stun.AttrNonce); ok && (code == stun.CodeStaleNonce || code == stun.CodeUnauthorized && u.nonce == "") {
				u.nonce = string(nonce)
				continue
			}
		}
		return r
	}
	u.c.t.Fatal("no answer but challenges")
	return nil
}

// send hands the server b from t and returns its one response.
func (c *core) send(t FiveTuple, b []byte) *stun.Message {
	c.t.Helper()
	n := len(c.sent)
	if err := c.s.HandlePacket(t, b, c.now); err != nil {
		c.t.Fatalf("HandlePacket: %v", err)
	}
	if len(c.sent) != n+1 || c.sent[n].t != t {
		c.t.Fatalf("%d responses", len(c.sent)-n)
	}
	var r stun.Message
	if err := r.Decode(c.sent[n].b); err != nil {
		c.t.Fatal(err)
	}
	return &r
}

func code(r *stun.Message) int {
	if r.Type.Class == stun.ClassSuccessResponse {
		return 0
	}
	c, _, _ := r.ErrorCode()
	return c
}

func allocateReq(m *stun.Message) { m.AddRequestedTransport(stun.ProtoUDP) }

func permissionReq(peers ...netip.AddrPort) func(m *stun.Message) {
	return func(m *stun.Message) {
		for _, p := range peers {
			m.AddXORAddress(stun.AttrXORPeerAddress, p)
		}
	}
}

func channelReq(n uint16, peer netip.AddrPort) func(m *stun.Message) {
	return func(m *stun.Message) {
		m.AddChannelNumber(n)
		m.AddXORAddress(stun.AttrXORPeerAddress, peer)
	}
}

func TestServerAllocate(t *testing.T) {
	c := newCore(t, ServerConfig{Software: "fib"})
	u := c.user(clientAddr, testUser, testPass)
	id := stun.NewTransactionID()
	r := u.doID(stun.MethodAllocate, id, allocateReq)
	if code(r) != 0 || r.CheckIntegrity(u.key) != nil {
		t.Fatalf("Allocate: %d", code(r))
	}
	relayed, _ := r.XORAddress(stun.AttrXORRelayedAddress)
	mapped, _ := r.XORAddress(stun.AttrXORMappedAddress)
	life, _ := r.Lifetime()
	if relayed.Addr() != relayIP || mapped != clientAddr || life != DefaultLifetime {
		t.Fatalf("relayed %v mapped %v lifetime %v", relayed, mapped, life)
	}
	// A retransmission is answered alike; another Allocate on the same
	// 5-tuple is a mismatch.
	if r := u.doID(stun.MethodAllocate, id, allocateReq); code(r) != 0 {
		t.Fatalf("retransmitted Allocate: %d", code(r))
	}
	if r := u.do(stun.MethodAllocate, allocateReq); code(r) != stun.CodeAllocationMismatch {
		t.Fatalf("second Allocate: %d", code(r))
	}
	// Another user may not touch the allocation.
	bob := c.user(clientAddr, "bob", "hunter2")
	if r := bob.do(stun.MethodRefresh, nil); code(r) != stun.CodeWrongCredentials {
		t.Fatalf("Refresh by another user: %d", code(r))
	}
	// Requests on a 5-tuple without an allocation are mismatches.
	other := c.user(netip.MustParseAddrPort("198.51.100.10:40001"), testUser, testPass)
	for _, m := range []stun.Method{stun.MethodRefresh, stun.MethodCreatePermission, stun.MethodChannelBind} {
		if r := other.do(m, channelReq(MinChannel, peerAddr)); code(r) != stun.CodeAllocationMismatch {
			t.Fatalf("%v without an allocation: %d", m, code(r))
		}
	}
	// A lifetime is bounded by the maximum.
	if r := u.do(stun.MethodRefresh, func(m *stun.Message) { m.AddLifetime(10 * time.Hour) }); code(r) != 0 {
		t.Fatal(code(r))
	} else if l, _ := r.Lifetime(); l != time.Hour {
		t.Fatalf("lifetime %v", l)
	}
	if r := u.do(stun.MethodRefresh, func(m *stun.Message) { m.AddLifetime(0) }); code(r) != 0 || c.s.Allocations() != 0 || !c.relays[relayed].closed {
		t.Fatalf("deallocate: %d, %d allocations", code(r), c.s.Allocations())
	}
}

func TestServerRejects(t *testing.T) {
	c := newCore(t, ServerConfig{})
	u := c.user(clientAddr, testUser, testPass)
	for _, tc := range []struct {
		name string
		add  func(m *stun.Message)
		code int
	}{
		{"no transport", nil, stun.CodeBadRequest},
		{"tcp", func(m *stun.Message) { m.AddRequestedTransport(stun.ProtoTCP) }, stun.CodeUnsupportedTransportProtocol},
		{"dont-fragment", func(m *stun.Message) { allocateReq(m); m.AddFlag(stun.AttrDontFragment) }, stun.CodeUnknownAttribute},
		{"even-port", func(m *stun.Message) { allocateReq(m); m.Add(stun.AttrEvenPort, []byte{0x80}) }, stun.CodeUnknownAttribute},
		{"family", func(m *stun.Message) { allocateReq(m); m.AddUint32(stun.AttrRequestedAddressFamily, 3<<24) }, stun.CodeAddressFamilyNotSupported},
		{"ipv6 refused by the host", func(m *stun.Message) {
			allocateReq(m)
			m.AddUint32(stun.AttrRequestedAddressFamily, 2<<24)
		}, 0},
	} {
		r := u.do(stun.MethodAllocate, tc.add)
		if code(r) != tc.code {
			t.Errorf("%s: %d, want %d", tc.name, code(r), tc.code)
		}
		if tc.code == 0 {
			_ = u.do(stun.MethodRefresh, func(m *stun.Message) { m.AddLifetime(0) })
		}
	}
	// Incomplete credentials, a wrong realm and a forged nonce.
	var m stun.Message
	m.Build(stun.MessageType{Method: stun.MethodAllocate, Class: stun.ClassRequest}, stun.NewTransactionID())
	allocateReq(&m)
	m.AddString(stun.AttrUsername, testUser)
	m.AddIntegrity(u.key)
	if r := c.send(u.tuple(), m.Raw); code(r) != stun.CodeBadRequest {
		t.Errorf("no realm and nonce: %d", code(r))
	}
	m.Build(stun.MessageType{Method: stun.MethodAllocate, Class: stun.ClassRequest}, stun.NewTransactionID())
	allocateReq(&m)
	m.AddString(stun.AttrUsername, testUser)
	m.AddString(stun.AttrRealm, testRealm)
	m.AddString(stun.AttrNonce, "0000000000000000000000000000000000000000")
	m.AddIntegrity(u.key)
	if r := c.send(u.tuple(), m.Raw); code(r) != stun.CodeStaleNonce {
		t.Errorf("forged nonce: %d", code(r))
	}
	// A nonce is bound to the client's address.
	moved := c.user(netip.MustParseAddrPort("198.51.100.10:40009"), testUser, testPass)
	moved.nonce = u.nonce
	m.Build(stun.MessageType{Method: stun.MethodAllocate, Class: stun.ClassRequest}, stun.NewTransactionID())
	allocateReq(&m)
	m.AddString(stun.AttrUsername, testUser)
	m.AddString(stun.AttrRealm, testRealm)
	m.AddString(stun.AttrNonce, u.nonce)
	m.AddIntegrity(u.key)
	if r := c.send(moved.tuple(), m.Raw); code(r) != stun.CodeStaleNonce {
		t.Errorf("another address's nonce: %d", code(r))
	}
	// An unknown method.
	m.Build(stun.MessageType{Method: 0x0FF, Class: stun.ClassRequest}, stun.NewTransactionID())
	if r := c.send(u.tuple(), m.Raw); code(r) != stun.CodeBadRequest {
		t.Errorf("unknown method: %d", code(r))
	}
	// A Binding request is answered without credentials.
	m.Build(stun.BindingRequest, stun.NewTransactionID())
	if r := c.send(u.tuple(), m.Raw); code(r) != 0 {
		t.Errorf("Binding: %d", code(r))
	} else if a, _ := r.XORAddress(stun.AttrXORMappedAddress); a != clientAddr {
		t.Errorf("Binding mapped %v", a)
	}
}

func TestServerStaleNonce(t *testing.T) {
	c := newCore(t, ServerConfig{NonceLifetime: time.Minute})
	u := c.user(clientAddr, testUser, testPass)
	if r := u.do(stun.MethodAllocate, allocateReq); code(r) != 0 {
		t.Fatal(code(r))
	}
	old := u.nonce
	c.now = c.now.Add(61 * time.Second)
	if r := u.do(stun.MethodRefresh, nil); code(r) != 0 || u.nonce == old {
		t.Fatalf("Refresh with a stale nonce: %d, nonce renewed %v", code(r), u.nonce != old)
	}
}

func TestServerChannelRebinding(t *testing.T) {
	c := newCore(t, ServerConfig{})
	u := c.user(clientAddr, testUser, testPass)
	_ = u.do(stun.MethodAllocate, allocateReq)
	peer2 := netip.MustParseAddrPort("192.0.2.8:5000")
	steps := []struct {
		name string
		n    uint16
		peer netip.AddrPort
		code int
	}{
		{"bind", MinChannel, peerAddr, 0},
		{"refresh", MinChannel, peerAddr, 0},
		{"number to another peer", MinChannel, peer2, stun.CodeBadRequest},
		{"peer to another number", MinChannel + 1, peerAddr, stun.CodeBadRequest},
		{"second channel", MinChannel + 1, peer2, 0},
		{"number too low", MinChannel - 1, peerAddr, stun.CodeBadRequest},
		{"number too high", MaxChannel + 1, peerAddr, stun.CodeBadRequest},
	}
	for _, s := range steps {
		if r := u.do(stun.MethodChannelBind, channelReq(s.n, s.peer)); code(r) != s.code {
			t.Errorf("%s: %d, want %d", s.name, code(r), s.code)
		}
	}
	// Data on the channel goes to the peer, and the peer's back on it.
	cd := AppendChannelData(nil, MinChannel, []byte("hello"))
	if err := c.s.HandlePacket(u.tuple(), cd, c.now); err != nil {
		t.Fatal(err)
	}
	var relay *coreRelay
	for _, r := range c.relays {
		relay = r
	}
	if len(relay.sent) != 1 || relay.sent[0].t.Client != peerAddr || string(relay.sent[0].b) != "hello" {
		t.Fatalf("relayed %v", relay.sent)
	}
	n := len(c.sent)
	_ = c.s.HandleRelayPacket(relay.addr, peerAddr, []byte("back"), c.now)
	if ch, d, err := ParseChannelData(c.sent[n].b); err != nil || ch != MinChannel || string(d) != "back" {
		t.Fatalf("back: %v %x %q", err, ch, d)
	}
	// The binding expires with its lifetime, unless refreshed: then its
	// data is dropped, but its number and peer stay tied for the
	// quarantine, and are free after.
	_ = u.do(stun.MethodRefresh, func(m *stun.Message) { m.AddLifetime(time.Hour) })
	c.now = c.now.Add(ChannelLifetime)
	if err := c.s.HandlePacket(u.tuple(), cd, c.now); err != ErrDropped {
		t.Fatalf("data on an expired channel: %v", err)
	}
	if r := u.do(stun.MethodChannelBind, channelReq(MinChannel, peer2)); code(r) != stun.CodeBadRequest {
		t.Fatalf("rebinding in quarantine: %d", code(r))
	}
	c.now = c.now.Add(channelQuarantine)
	c.s.HandleTimeout(c.now)
	if r := u.do(stun.MethodChannelBind, channelReq(MinChannel, peer2)); code(r) != 0 {
		t.Fatalf("rebinding after the quarantine: %d", code(r))
	}
	if r := u.do(stun.MethodChannelBind, channelReq(MinChannel+1, peerAddr)); code(r) != 0 {
		t.Fatalf("binding the peer to another number after the quarantine: %d", code(r))
	}
}

func TestServerQuotas(t *testing.T) {
	c := newCore(t, ServerConfig{MaxAllocationsPerUser: 1, MaxAllocations: 2})
	a1 := c.user(netip.MustParseAddrPort("198.51.100.1:1"), testUser, testPass)
	a2 := c.user(netip.MustParseAddrPort("198.51.100.1:2"), testUser, testPass)
	b1 := c.user(netip.MustParseAddrPort("198.51.100.2:1"), "bob", "hunter2")
	b2 := c.user(netip.MustParseAddrPort("198.51.100.2:2"), "bob", "hunter2")
	c3 := c.user(netip.MustParseAddrPort("198.51.100.3:1"), "bob", "hunter2")
	if r := a1.do(stun.MethodAllocate, allocateReq); code(r) != 0 {
		t.Fatal(code(r))
	}
	if r := a2.do(stun.MethodAllocate, allocateReq); code(r) != stun.CodeAllocationQuotaReached {
		t.Fatalf("second of a user's: %d", code(r))
	}
	if r := b1.do(stun.MethodAllocate, allocateReq); code(r) != 0 {
		t.Fatal(code(r))
	}
	_ = b2
	_ = a1.do(stun.MethodRefresh, func(m *stun.Message) { m.AddLifetime(0) })
	if r := a2.do(stun.MethodAllocate, allocateReq); code(r) != 0 {
		t.Fatalf("after a deallocation: %d", code(r))
	}
	c.s.cfg.MaxAllocationsPerUser = 0
	if r := c3.do(stun.MethodAllocate, allocateReq); code(r) != stun.CodeInsufficientCapacity {
		t.Fatalf("beyond the total: %d", code(r))
	}
	// Expired allocations make room once swept, even between sweeps.
	c.now = c.now.Add(DefaultLifetime)
	if r := c3.do(stun.MethodAllocate, allocateReq); code(r) != 0 {
		t.Fatalf("after the others expired: %d", code(r))
	}
}

func TestServerSSRF(t *testing.T) {
	c := newCore(t, ServerConfig{AllowedPeers: []netip.Prefix{netip.MustParsePrefix("10.1.0.0/16")}})
	u := c.user(clientAddr, testUser, testPass)
	_ = u.do(stun.MethodAllocate, allocateReq)
	for _, p := range []string{
		"127.0.0.1:80", "10.0.0.1:22", "172.16.5.4:443", "192.168.1.1:80", "169.254.169.254:80",
		"0.0.0.0:80", "100.64.0.1:80", "224.0.0.1:5000", "255.255.255.255:9", "198.18.0.1:80",
	} {
		peer := netip.MustParseAddrPort(p)
		if r := u.do(stun.MethodCreatePermission, permissionReq(peer)); code(r) != stun.CodeForbidden {
			t.Errorf("permission for %v: %d", peer, code(r))
		}
		if r := u.do(stun.MethodChannelBind, channelReq(MinChannel, peer)); code(r) != stun.CodeForbidden {
			t.Errorf("channel to %v: %d", peer, code(r))
		}
	}
	// An IPv6 peer on an IPv4 relay is a family mismatch.
	if r := u.do(stun.MethodCreatePermission, permissionReq(netip.MustParseAddrPort("[2001:db8::1]:80"))); code(r) != stun.CodePeerAddressFamilyMismatch {
		t.Errorf("IPv6 peer: %d", code(r))
	}
	// One refused peer refuses the whole request.
	if r := u.do(stun.MethodCreatePermission, permissionReq(peerAddr, netip.MustParseAddrPort("127.0.0.1:1"))); code(r) != stun.CodeForbidden {
		t.Errorf("mixed peers: %d", code(r))
	}
	a := c.s.allocs[u.tuple()]
	if len(a.perms) != 0 {
		t.Fatalf("permissions installed: %v", a.perms)
	}
	// A Send indication to a refused peer goes nowhere.
	var m stun.Message
	m.Build(stun.MessageType{Method: stun.MethodSend, Class: stun.ClassIndication}, stun.NewTransactionID())
	m.AddXORAddress(stun.AttrXORPeerAddress, netip.MustParseAddrPort("127.0.0.1:6379"))
	m.Add(stun.AttrData, []byte("FLUSHALL\r\n"))
	if err := c.s.HandlePacket(u.tuple(), m.Raw, c.now); err != ErrDropped {
		t.Fatalf("Send to loopback: %v", err)
	}
	// The allow list opens a private prefix, and PeerAllowed overrides it all.
	if r := u.do(stun.MethodCreatePermission, permissionReq(netip.MustParseAddrPort("10.1.2.3:80"))); code(r) != 0 {
		t.Errorf("allowed private peer: %d", code(r))
	}
	c.s.cfg.PeerAllowed = func(ip netip.Addr) bool { return ip.IsLoopback() }
	if r := u.do(stun.MethodCreatePermission, permissionReq(netip.MustParseAddrPort("127.0.0.1:80"))); code(r) != 0 {
		t.Errorf("PeerAllowed: %d", code(r))
	}
	if r := u.do(stun.MethodCreatePermission, permissionReq(peerAddr)); code(r) != stun.CodeForbidden {
		t.Errorf("PeerAllowed refusing a public peer: %d", code(r))
	}
	for _, ip := range []string{"::1", "fe80::1", "fc00::1", "64:ff9b::a00:1", "2002:a00:1::", "::ffff:127.0.0.1"} {
		if !DeniedByDefault(netip.MustParseAddr(ip)) {
			t.Errorf("%s is not denied", ip)
		}
	}
	for _, ip := range []string{"8.8.8.8", "2001:4860:4860::8888", "192.0.2.7"} {
		if DeniedByDefault(netip.MustParseAddr(ip)) {
			t.Errorf("%s is denied", ip)
		}
	}
}

func TestServerBandwidthHook(t *testing.T) {
	var toPeer, fromPeer int
	budget := 10
	c := newCore(t, ServerConfig{OnRelay: func(u string, out bool, n int) bool {
		if budget -= n; budget < 0 {
			return false
		}
		if out {
			toPeer += n
		} else {
			fromPeer += n
		}
		return true
	}})
	u := c.user(clientAddr, testUser, testPass)
	_ = u.do(stun.MethodAllocate, allocateReq)
	_ = u.do(stun.MethodCreatePermission, permissionReq(peerAddr))
	send := func(data string) error {
		var m stun.Message
		m.Build(stun.MessageType{Method: stun.MethodSend, Class: stun.ClassIndication}, stun.NewTransactionID())
		m.AddXORAddress(stun.AttrXORPeerAddress, peerAddr)
		m.Add(stun.AttrData, []byte(data))
		return c.s.HandlePacket(u.tuple(), m.Raw, c.now)
	}
	if err := send("12345"); err != nil {
		t.Fatal(err)
	}
	relayed, _ := c.s.RelayFor(u.tuple())
	if err := c.s.HandleRelayPacket(relayed, peerAddr, []byte("abc"), c.now); err != nil {
		t.Fatal(err)
	}
	if err := send("toolong"); err != ErrDropped {
		t.Fatalf("over budget: %v", err)
	}
	if toPeer != 5 || fromPeer != 3 {
		t.Fatalf("counted %d and %d", toPeer, fromPeer)
	}
}

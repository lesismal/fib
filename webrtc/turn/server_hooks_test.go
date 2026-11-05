package turn

import (
	"net/netip"
	"testing"
	"time"

	"github.com/lesismal/fib/webrtc/stun"
)

// An allocation's permissions and channels are capped, refused with 508
// beyond, and expired ones make room.
func TestServerPermissionAndChannelCaps(t *testing.T) {
	c := newCore(t, ServerConfig{MaxPermissions: 2, MaxChannels: 2})
	u := c.user(clientAddr, testUser, testPass)
	_ = u.do(stun.MethodAllocate, allocateReq)
	_ = u.do(stun.MethodRefresh, func(m *stun.Message) { m.AddLifetime(time.Hour) })
	p := func(i int) netip.AddrPort {
		return netip.AddrPortFrom(netip.AddrFrom4([4]byte{192, 0, 2, byte(10 + i)}), 5000)
	}
	if r := u.do(stun.MethodCreatePermission, permissionReq(p(1), p(1), p(2))); code(r) != 0 {
		t.Fatalf("two permissions, one repeated: %d", code(r))
	}
	if r := u.do(stun.MethodCreatePermission, permissionReq(p(3))); code(r) != stun.CodeInsufficientCapacity {
		t.Fatalf("a third permission: %d", code(r))
	}
	// Refreshing a held one needs no room.
	if r := u.do(stun.MethodCreatePermission, permissionReq(p(2))); code(r) != 0 {
		t.Fatalf("refreshing a permission: %d", code(r))
	}
	// A channel to a peer without a permission needs one.
	if r := u.do(stun.MethodChannelBind, channelReq(MinChannel, p(3))); code(r) != stun.CodeInsufficientCapacity {
		t.Fatalf("a channel past the permissions: %d", code(r))
	}
	if r := u.do(stun.MethodChannelBind, channelReq(MinChannel, p(1))); code(r) != 0 {
		t.Fatal(code(r))
	}
	if r := u.do(stun.MethodChannelBind, channelReq(MinChannel+1, p(2))); code(r) != 0 {
		t.Fatal(code(r))
	}
	if r := u.do(stun.MethodChannelBind, channelReq(MinChannel+1, p(2))); code(r) != 0 {
		t.Fatalf("refreshing a channel: %d", code(r))
	}
	// Once the permissions expire, they make room; the channels are tied
	// until their quarantine is over.
	c.now = c.now.Add(ChannelLifetime)
	if r := u.do(stun.MethodCreatePermission, permissionReq(p(3), p(4))); code(r) != 0 {
		t.Fatalf("permissions after the others expired: %d", code(r))
	}
	if r := u.do(stun.MethodChannelBind, channelReq(MinChannel+2, p(3))); code(r) != stun.CodeInsufficientCapacity {
		t.Fatalf("a third channel: %d", code(r))
	}
	c.now = c.now.Add(channelQuarantine)
	_ = u.do(stun.MethodCreatePermission, permissionReq(p(3)))
	if r := u.do(stun.MethodChannelBind, channelReq(MinChannel+2, p(3))); code(r) != 0 {
		t.Fatalf("a channel after the quarantine: %d", code(r))
	}
	if st := c.s.Stats(); st.Permissions != 2 || st.Channels != 1 || st.Allocations != 1 {
		t.Fatalf("stats %+v", st)
	}
}

// PeerAllowed sees the port: a permission asks with port 0, a channel with
// its peer, and every datagram again.
func TestServerPeerAllowedPort(t *testing.T) {
	var c *core
	self := netip.AddrPortFrom(relayIP, 50001)
	var asked []netip.AddrPort
	c = newCore(t, ServerConfig{PeerAllowed: func(p netip.AddrPort) bool {
		asked = append(asked, p)
		if p.Addr() == relayIP {
			return p.Port() == 0 || c.s.HoldsRelay(p)
		}
		return !DeniedByDefault(p.Addr())
	}})
	u := c.user(clientAddr, testUser, testPass)
	_ = u.do(stun.MethodAllocate, allocateReq)
	v := c.user(netip.MustParseAddrPort("198.51.100.10:40300"), testUser, testPass)
	_ = v.do(stun.MethodAllocate, allocateReq)
	if !c.s.HoldsRelay(self) || c.s.HoldsRelay(serverAddr) {
		t.Fatal("HoldsRelay")
	}
	if r := u.do(stun.MethodCreatePermission, permissionReq(serverAddr)); code(r) != 0 || asked[len(asked)-1] != netip.AddrPortFrom(relayIP, 0) {
		t.Fatalf("a permission for the server's own address: %d, asked %v", code(r), asked)
	}
	if r := u.do(stun.MethodChannelBind, channelReq(MinChannel, serverAddr)); code(r) != stun.CodeForbidden {
		t.Fatalf("a channel to the server's listening port: %d", code(r))
	}
	if r := u.do(stun.MethodChannelBind, channelReq(MinChannel, self)); code(r) != 0 {
		t.Fatalf("a channel to another client's relay: %d", code(r))
	}
	// A datagram to the server's other port is dropped, one to the relay
	// goes; so from them.
	var m stun.Message
	m.Build(stun.MessageType{Method: stun.MethodSend, Class: stun.ClassIndication}, stun.NewTransactionID())
	m.AddXORAddress(stun.AttrXORPeerAddress, serverAddr)
	m.Add(stun.AttrData, []byte("x"))
	if err := c.s.HandlePacket(u.tuple(), m.Raw, c.now); err != ErrDropped {
		t.Fatalf("Send to the listening port: %v", err)
	}
	if err := c.s.HandlePacket(u.tuple(), AppendChannelData(nil, MinChannel, []byte("x")), c.now); err != nil {
		t.Fatal(err)
	}
	relayed, _ := c.s.RelayFor(u.tuple())
	if err := c.s.HandleRelayPacket(relayed, serverAddr, []byte("x"), c.now); err != ErrDropped {
		t.Fatalf("from the listening port: %v", err)
	}
	if err := c.s.HandleRelayPacket(relayed, self, []byte("yz"), c.now); err != nil {
		t.Fatal(err)
	}
	st := c.s.Stats()
	if st.ToPeers != (Traffic{1, 1}) || st.ToClients != (Traffic{1, 2}) || st.Dropped != 2 {
		t.Fatalf("stats %+v", st)
	}
}

// OnEvent hears of allocations made, refreshed and freed, and of requests
// refused and credentials that fail; Stats counts them.
func TestServerEvents(t *testing.T) {
	var events []Event
	c := newCore(t, ServerConfig{MaxAllocationsPerUser: 1, OnEvent: func(e Event) { events = append(events, e) }})
	kinds := func() (k []string) {
		for _, e := range events {
			s := e.Kind.String()
			if e.Kind == EventFreed {
				s += ":" + e.Reason.String()
			}
			k = append(k, s)
		}
		events = events[:0]
		return k
	}
	u := c.user(clientAddr, testUser, testPass)
	_ = u.do(stun.MethodAllocate, allocateReq)
	_ = u.do(stun.MethodRefresh, nil)
	_ = u.do(stun.MethodCreatePermission, permissionReq(peerAddr))
	_ = c.s.HandleRelayPacket(netip.AddrPortFrom(relayIP, 50000), peerAddr, []byte("abc"), c.now)
	v := c.user(netip.MustParseAddrPort("198.51.100.10:40400"), testUser, testPass)
	_ = v.do(stun.MethodAllocate, allocateReq)
	bad := c.user(netip.MustParseAddrPort("198.51.100.10:40401"), testUser, "wrong")
	_ = bad.do(stun.MethodAllocate, allocateReq)
	_ = u.do(stun.MethodRefresh, func(m *stun.Message) { m.AddLifetime(0) })
	got := kinds()
	want := []string{"allocated", "refreshed", "refused", "auth failed", "freed:deleted"}
	if len(got) != len(want) {
		t.Fatalf("events %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events %v, want %v", got, want)
		}
	}
	if e := events; len(e) != 0 {
		t.Fatal("events not drained")
	}
	st := c.s.Stats()
	if st.Refused != 1 || st.AuthFailures != 1 || st.ToClients != (Traffic{1, 3}) || st.Allocations != 0 {
		t.Fatalf("stats %+v", st)
	}

	// Expiry and Close, an unknown attribute and incomplete credentials.
	_ = u.do(stun.MethodAllocate, allocateReq)
	c.now = c.now.Add(DefaultLifetime)
	c.s.HandleTimeout(c.now)
	_ = u.do(stun.MethodAllocate, func(m *stun.Message) { allocateReq(m); m.Add(stun.AttrEvenPort, []byte{0}) })
	_ = u.do(stun.MethodAllocate, allocateReq)
	c.s.Close()
	var m stun.Message
	m.Build(stun.MessageType{Method: stun.MethodRefresh, Class: stun.ClassRequest}, stun.NewTransactionID())
	m.AddString(stun.AttrUsername, testUser)
	m.AddIntegrity(u.key)
	_ = c.send(u.tuple(), m.Raw)
	got = kinds()
	want = []string{"allocated", "freed:expired", "refused", "allocated", "freed:closed", "auth failed"}
	if len(got) != len(want) {
		t.Fatalf("events %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events %v, want %v", got, want)
		}
	}
	if EventKind(0).String() != "unknown" || FreeReason(0).String() != "unknown" {
		t.Fatal("zero values' names")
	}
}

// A draining server refuses new allocations and keeps refreshing the held
// ones; SetLimits applies to the next request.
func TestServerDrainAndLimits(t *testing.T) {
	c := newCore(t, ServerConfig{})
	u := c.user(clientAddr, testUser, testPass)
	_ = u.do(stun.MethodAllocate, allocateReq)
	c.s.SetLimits(Limits{DefaultLifetime: time.Minute, MaxLifetime: 2 * time.Minute})
	if l := c.s.Limits(); l.DefaultLifetime != time.Minute || l.MaxLifetime != 2*time.Minute {
		t.Fatalf("limits %+v", l)
	}
	if r := u.do(stun.MethodRefresh, func(m *stun.Message) { m.AddLifetime(time.Hour) }); code(r) != 0 {
		t.Fatal(code(r))
	} else if l, _ := r.Lifetime(); l != 2*time.Minute {
		t.Fatalf("lifetime %v under the new maximum", l)
	}
	if c.s.Draining() {
		t.Fatal("draining before Drain")
	}
	c.s.Drain()
	v := c.user(netip.MustParseAddrPort("198.51.100.10:40500"), testUser, testPass)
	if r := v.do(stun.MethodAllocate, allocateReq); code(r) != stun.CodeInsufficientCapacity {
		t.Fatalf("Allocate while draining: %d", code(r))
	}
	if r := u.do(stun.MethodRefresh, nil); code(r) != 0 || !c.s.Draining() {
		t.Fatalf("Refresh while draining: %d", code(r))
	}
}

// The default peer policy lets clients reach the server's own addresses on
// its relayed ports alone, checks every datagram again, and refuses an
// allocation made through a relay.
func TestServerOwnAddresses(t *testing.T) {
	other := netip.MustParseAddr("192.0.2.99")
	c := newCore(t, ServerConfig{OwnAddrs: []netip.Addr{other}})
	u := c.user(clientAddr, testUser, testPass)
	_ = u.do(stun.MethodAllocate, allocateReq)
	v := c.user(netip.MustParseAddrPort("198.51.100.10:40300"), testUser, testPass)
	_ = v.do(stun.MethodAllocate, allocateReq)
	vRelay, _ := c.s.RelayFor(v.tuple())
	if r := u.do(stun.MethodCreatePermission, permissionReq(serverAddr)); code(r) != 0 {
		t.Fatalf("a permission for the server's address: %d", code(r))
	}
	for _, p := range []netip.AddrPort{serverAddr, netip.AddrPortFrom(relayIP, 53), netip.AddrPortFrom(other, 80)} {
		if r := u.do(stun.MethodChannelBind, channelReq(MinChannel, p)); code(r) != stun.CodeForbidden {
			t.Fatalf("a channel to the server's %v: %d", p, code(r))
		}
	}
	if r := u.do(stun.MethodChannelBind, channelReq(MinChannel, vRelay)); code(r) != 0 {
		t.Fatalf("a channel to another client's relay: %d", code(r))
	}
	send := func(peer netip.AddrPort) error {
		var m stun.Message
		m.Build(stun.MessageType{Method: stun.MethodSend, Class: stun.ClassIndication}, stun.NewTransactionID())
		m.AddXORAddress(stun.AttrXORPeerAddress, peer)
		m.Add(stun.AttrData, []byte("x"))
		return c.s.HandlePacket(u.tuple(), m.Raw, c.now)
	}
	if err := send(serverAddr); err != ErrDropped {
		t.Fatalf("Send to the listening port: %v", err)
	}
	if err := send(vRelay); err != nil {
		t.Fatalf("Send to another client's relay: %v", err)
	}
	relayed, _ := c.s.RelayFor(u.tuple())
	if err := c.s.HandleRelayPacket(relayed, serverAddr, []byte("x"), c.now); err != ErrDropped {
		t.Fatalf("from the listening port: %v", err)
	}
	// A client whose source is a relay would nest allocations.
	nested := c.user(vRelay, testUser, testPass)
	if r := nested.do(stun.MethodAllocate, allocateReq); code(r) != stun.CodeForbidden {
		t.Fatalf("an allocation through a relay: %d", code(r))
	}
	// A peer denied after its permission is installed gets nothing more.
	if r := u.do(stun.MethodCreatePermission, permissionReq(peerAddr)); code(r) != 0 {
		t.Fatal(code(r))
	}
	if err := send(peerAddr); err != nil {
		t.Fatal(err)
	}
	c.s.cfg.DeniedPeers = []netip.Prefix{netip.PrefixFrom(peerAddr.Addr(), 32)}
	if err := send(peerAddr); err != ErrDropped {
		t.Fatalf("Send to a peer denied since: %v", err)
	}
	if err := c.s.HandleRelayPacket(relayed, peerAddr, []byte("x"), c.now); err != ErrDropped {
		t.Fatalf("from a peer denied since: %v", err)
	}
}

// The quotas have defaults, a negative one is no limit, and one client IP
// address holds no more than MaxAllocationsPerIP.
func TestServerQuotaDefaults(t *testing.T) {
	c := newCore(t, ServerConfig{Limits: Limits{MaxChannels: -1, MaxAllocationsPerIP: 2}})
	l := c.s.Limits()
	if l.MaxAllocations != DefaultMaxAllocations || l.MaxAllocationsPerUser != DefaultMaxAllocationsPerUser ||
		l.MaxPermissions != DefaultMaxPermissions || l.MaxChannels != -1 || l.MaxAllocationsPerIP != 2 {
		t.Fatalf("limits %+v", l)
	}
	for i := range 3 {
		u := c.user(netip.AddrPortFrom(clientAddr.Addr(), uint16(1000+i)), testUser, testPass)
		want := 0
		if i == 2 {
			want = stun.CodeAllocationQuotaReached
		}
		if r := u.do(stun.MethodAllocate, allocateReq); code(r) != want {
			t.Fatalf("allocation %d from one address: %d", i, code(r))
		}
	}
	if r := c.user(netip.MustParseAddrPort("198.51.100.11:1000"), testUser, testPass).do(stun.MethodAllocate, allocateReq); code(r) != 0 {
		t.Fatalf("from another address: %d", code(r))
	}
	c.s.SetLimits(Limits{})
	if l := c.s.Limits(); l.MaxAllocationsPerIP != DefaultMaxAllocationsPerIP || l.MaxChannels != DefaultMaxChannels {
		t.Fatalf("limits after SetLimits: %+v", l)
	}
}

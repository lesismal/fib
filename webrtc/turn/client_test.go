package turn

import (
	"bytes"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/lesismal/fib/webrtc/stun"
)

func TestAllocateAndRelay(t *testing.T) {
	for _, bind := range []bool{false, true} {
		n := newSimNet(t, 1)
		srv := newSimServer(n, serverAddr, ServerConfig{})
		c := newSimClient(n, clientAddr, ClientConfig{BindChannels: bind})
		peer := newSimPeer(n, peerAddr, true)
		c.allocate(t)
		relayed, mapped, ok := c.c.Relayed()
		if !ok || relayed.Addr() != relayIP || mapped != clientAddr {
			t.Fatalf("relayed %v mapped %v", relayed, mapped)
		}
		if srv.s.Allocations() != 1 {
			t.Fatalf("server holds %d allocations", srv.s.Allocations())
		}
		if err := c.c.SendTo(peerAddr, []byte("one")); err != nil {
			t.Fatal(err)
		}
		n.run(time.Second, nil)
		if len(peer.got) != 1 || peer.got[0] != (received{relayed, "one"}) {
			t.Fatalf("bind=%v: peer got %v", bind, peer.got)
		}
		if len(c.got) != 1 || c.got[0] != (received{peerAddr, "echo:one"}) {
			t.Fatalf("bind=%v: client got %v", bind, c.got)
		}
		before := c.channelData
		if err := c.c.SendTo(peerAddr, []byte("two")); err != nil {
			t.Fatal(err)
		}
		n.run(time.Second, nil)
		if len(c.got) != 2 || c.got[1].data != "echo:two" {
			t.Fatalf("bind=%v: client got %v", bind, c.got)
		}
		if bind != (c.channelData > before) {
			t.Fatalf("bind=%v: %d ChannelData messages from the server", bind, c.channelData-before)
		}
		// A peer without a permission is not relayed.
		stranger := netip.MustParseAddrPort("192.0.2.99:9")
		n.send(stranger, relayed, []byte("spam"))
		n.run(time.Second, nil)
		if len(c.got) != 2 {
			t.Fatalf("a datagram from a peer without a permission was relayed: %v", c.got)
		}
	}
}

func TestLossyAllocation(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		n := newSimNet(t, seed)
		n.loss = 0.3
		n.latency = 40 * time.Millisecond
		newSimServer(n, serverAddr, ServerConfig{})
		c := newSimClient(n, clientAddr, ClientConfig{BindChannels: true})
		peer := newSimPeer(n, peerAddr, true)
		c.allocate(t)
		// Keep sending until the echo of one comes back; the channel and
		// permission get bound along the way despite the losses.
		for i := 0; len(c.got) == 0 && i < 100; i++ {
			_ = c.c.SendTo(peerAddr, []byte("x"))
			n.run(200*time.Millisecond, nil)
		}
		if len(c.got) == 0 || len(peer.got) == 0 {
			t.Fatalf("seed %d: nothing relayed at 30%% loss", seed)
		}
		// A request outlives RFC 8489's 7 sends over 39.5s; one that
		// still went unanswered is sent again with the next data.
		for range 3 {
			n.run(45*time.Second, nil)
			_ = c.c.SendTo(peerAddr, []byte("y"))
		}
		ch := c.c.chans[peerAddr]
		if ch == nil || !ch.bound {
			t.Fatalf("seed %d: channel not bound: %+v", seed, ch)
		}
	}
}

func TestAuthFailure(t *testing.T) {
	n := newSimNet(t, 2)
	newSimServer(n, serverAddr, ServerConfig{})
	c := newSimClient(n, clientAddr, ClientConfig{Username: testUser, Password: "wrong"})
	_ = c.c.Allocate(n.now)
	n.run(time.Minute, func() bool { return c.failed != nil || c.allocated })
	var e *Error
	if !errors.Is(c.failed, ErrUnauthorized) || !errors.As(c.failed, &e) || e.Code != stun.CodeUnauthorized {
		t.Fatalf("failed with %v", c.failed)
	}
	if err := c.c.SendTo(peerAddr, nil); !errors.Is(err, ErrNotAllocated) {
		t.Fatalf("SendTo after a failure: %v", err)
	}
	// An unknown user fares the same.
	n2 := newSimNet(t, 2)
	newSimServer(n2, serverAddr, ServerConfig{})
	c2 := newSimClient(n2, clientAddr, ClientConfig{Username: "mallory", Password: testPass})
	_ = c2.c.Allocate(n2.now)
	n2.run(time.Minute, func() bool { return c2.failed != nil || c2.allocated })
	if !errors.Is(c2.failed, ErrUnauthorized) {
		t.Fatalf("unknown user: %v", c2.failed)
	}
}

// countErrors counts the error responses of each code the server sends.
func countErrors(n *simNet, codes map[int]int) {
	n.drop = func(from, to netip.AddrPort, b []byte) bool {
		var m stun.Message
		if from == serverAddr && m.Decode(b) == nil && m.Type.Class == stun.ClassErrorResponse {
			code, _, _ := m.ErrorCode()
			codes[code]++
		}
		return false
	}
}

func TestStaleNonce(t *testing.T) {
	n := newSimNet(t, 3)
	newSimServer(n, serverAddr, ServerConfig{NonceLifetime: time.Minute})
	codes := map[int]int{}
	countErrors(n, codes)
	c := newSimClient(n, clientAddr, ClientConfig{})
	peer := newSimPeer(n, peerAddr, false)
	c.allocate(t)
	n.run(2*time.Minute, nil)
	if err := c.c.SendTo(peerAddr, []byte("late")); err != nil {
		t.Fatal(err)
	}
	n.run(time.Second, nil)
	p := c.c.perms[peerAddr.Addr()]
	if codes[stun.CodeStaleNonce] == 0 || p == nil || p.pending || p.err != nil {
		t.Fatalf("438s %d, permission %+v (%v)", codes[stun.CodeStaleNonce], p, p.err)
	}
	_ = c.c.SendTo(peerAddr, []byte("again"))
	n.run(time.Second, nil)
	if len(peer.got) == 0 || peer.got[len(peer.got)-1].data != "again" {
		t.Fatalf("peer got %v", peer.got)
	}
	// The refreshes go on with fresh nonces for the allocation's life.
	n.run(30*time.Minute, nil)
	if c.failed != nil || c.c.state != clientAllocated {
		t.Fatalf("allocation lost: %v", c.failed)
	}
}

func TestRefreshKeepsAllocation(t *testing.T) {
	n := newSimNet(t, 4)
	srv := newSimServer(n, serverAddr, ServerConfig{DefaultLifetime: 2 * time.Minute, MaxLifetime: 2 * time.Minute})
	c := newSimClient(n, clientAddr, ClientConfig{BindChannels: true})
	peer := newSimPeer(n, peerAddr, true)
	c.allocate(t)
	_ = c.c.SendTo(peerAddr, []byte("hi"))
	n.run(time.Second, nil)
	// Well past the lifetimes of the allocation, permission and channel.
	n.run(25*time.Minute, nil)
	if srv.s.Allocations() != 1 || c.failed != nil {
		t.Fatalf("allocations %d, failed %v", srv.s.Allocations(), c.failed)
	}
	before := c.channelData
	_ = c.c.SendTo(peerAddr, []byte("still"))
	n.run(time.Second, nil)
	if len(peer.got) != 2 || c.got[len(c.got)-1].data != "echo:still" || c.channelData == before {
		t.Fatalf("peer got %v, client %v", peer.got, c.got)
	}
}

func TestLifetimeExpiry(t *testing.T) {
	n := newSimNet(t, 5)
	srv := newSimServer(n, serverAddr, ServerConfig{DefaultLifetime: 2 * time.Minute, MaxLifetime: 2 * time.Minute})
	c := newSimClient(n, clientAddr, ClientConfig{})
	newSimPeer(n, peerAddr, true)
	c.allocate(t)
	relayed, _, _ := c.c.Relayed()
	_ = c.c.SendTo(peerAddr, []byte("hi"))
	n.run(time.Second, nil)
	// The client goes silent: its refreshes are lost.
	n.drop = func(from, to netip.AddrPort, b []byte) bool { return from == clientAddr }
	n.run(2*time.Minute, nil)
	if srv.s.Allocations() != 0 || !srv.relays[0].closed {
		t.Fatalf("allocations %d after the lifetime, relay closed %v", srv.s.Allocations(), srv.relays[0].closed)
	}
	got := len(c.got)
	n.send(peerAddr, relayed, []byte("late"))
	n.run(time.Second, nil)
	if len(c.got) != got {
		t.Fatal("relayed after the allocation expired")
	}
	// The client's refresh times out and it reports the allocation lost.
	n.run(time.Minute, func() bool { return c.failed != nil })
	if c.failed == nil {
		t.Fatal("the client did not notice")
	}
}

func TestPermissionExpiry(t *testing.T) {
	n := newSimNet(t, 6)
	newSimServer(n, serverAddr, ServerConfig{})
	c := newSimClient(n, clientAddr, ClientConfig{})
	newSimPeer(n, peerAddr, false)
	c.allocate(t)
	relayed, _, _ := c.c.Relayed()
	_ = c.c.SendTo(peerAddr, []byte("hi"))
	n.run(time.Second, nil)
	n.send(peerAddr, relayed, []byte("1"))
	n.run(time.Second, nil)
	if len(c.got) != 1 {
		t.Fatalf("client got %v", c.got)
	}
	// The client forgets the permission, so it is never refreshed, while
	// the allocation is.
	clear(c.c.perms)
	n.run(PermissionLifetime, nil)
	n.send(peerAddr, relayed, []byte("2"))
	n.run(time.Second, nil)
	if len(c.got) != 1 {
		t.Fatalf("relayed after the permission expired: %v", c.got)
	}
	// Sending again installs it again.
	_ = c.c.SendTo(peerAddr, []byte("hi"))
	n.run(time.Second, nil)
	n.send(peerAddr, relayed, []byte("3"))
	n.run(time.Second, nil)
	if len(c.got) != 2 || c.got[1].data != "3" {
		t.Fatalf("client got %v", c.got)
	}
}

func TestDeallocate(t *testing.T) {
	n := newSimNet(t, 7)
	srv := newSimServer(n, serverAddr, ServerConfig{})
	c := newSimClient(n, clientAddr, ClientConfig{})
	c.allocate(t)
	c.c.Close(n.now)
	n.run(time.Minute, func() bool { return c.c.Closed() })
	if !c.c.Closed() || srv.s.Allocations() != 0 || !srv.relays[0].closed {
		t.Fatalf("closed %v, allocations %d", c.c.Closed(), srv.s.Allocations())
	}
	if err := c.c.SendTo(peerAddr, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("SendTo after Close: %v", err)
	}
	if _, ok := c.c.NextTimeout(); ok {
		t.Fatal("a closed client has timers")
	}
}

func TestTryAlternate(t *testing.T) {
	n := newSimNet(t, 8)
	alt := netip.MustParseAddrPort("203.0.113.2:3478")
	srv := newSimServer(n, alt, ServerConfig{})
	// The first server only redirects.
	n.at[serverAddr] = func(from netip.AddrPort, b []byte) {
		var m, r stun.Message
		if m.Decode(b) != nil || m.Type.Class != stun.ClassRequest {
			return
		}
		r.Build(stun.MessageType{Method: m.Type.Method, Class: stun.ClassErrorResponse}, m.TransactionID)
		r.AddErrorCode(stun.CodeTryAlternate, "Try Alternate")
		r.AddAddress(stun.AttrAlternateServer, alt)
		n.send(serverAddr, from, r.Raw)
	}
	c := newSimClient(n, clientAddr, ClientConfig{})
	c.allocate(t)
	relayed, _, _ := c.c.Relayed()
	if c.c.Server() != alt || srv.s.Allocations() != 1 || relayed.Addr() != alt.Addr() {
		t.Fatalf("server %v, relayed %v", c.c.Server(), relayed)
	}
}

func TestEphemeralCredentials(t *testing.T) {
	secret := []byte("shared secret")
	n := newSimNet(t, 9)
	now := func() time.Time { return n.now }
	newSimServer(n, serverAddr, ServerConfig{Auth: EphemeralAuth(secret, now)})
	user, pass := EphemeralCredentials(secret, "carol", n.now.Add(time.Hour))
	c := newSimClient(n, clientAddr, ClientConfig{Username: user, Password: pass})
	c.allocate(t)

	expired, pass2 := EphemeralCredentials(secret, "carol", n.now.Add(-time.Second))
	c2 := newSimClient(n, netip.MustParseAddrPort("198.51.100.11:1"), ClientConfig{Username: expired, Password: pass2})
	_ = c2.c.Allocate(n.now)
	n.run(time.Minute, func() bool { return c2.failed != nil || c2.allocated })
	if !errors.Is(c2.failed, ErrUnauthorized) {
		t.Fatalf("expired credentials: %v", c2.failed)
	}
	// The username "<expiry>:<user>" and the password are what coturn's
	// use-auth-secret expects: the base64 of HMAC-SHA1(secret, username),
	// here as `printf 1700000000:user | openssl dgst -sha1 -hmac north
	// -binary | base64` computes it.
	u, p := EphemeralCredentials([]byte("north"), "user", time.Unix(1700000000, 0))
	if u != "1700000000:user" || p != "2uyGWnO1vHFmQfE0HWQNi1ldYAk=" {
		t.Fatalf("credentials %q %q", u, p)
	}
}

func TestClientIgnoresOthers(t *testing.T) {
	c, _ := NewClient(ClientConfig{Server: serverAddr, Send: func(netip.AddrPort, []byte) {}})
	var m stun.Message
	m.Build(stun.BindingRequest, stun.NewTransactionID())
	if _, _, ok := c.HandlePacket(peerAddr, m.Raw, time.Now()); ok {
		t.Fatal("took a packet from another address")
	}
	if _, _, ok := c.HandlePacket(serverAddr, []byte{0x16, 0xfe, 0xfd, 0, 0}, time.Now()); ok {
		t.Fatal("took a DTLS record")
	}
	// A forged success response, without integrity, is ignored.
	_ = c.Allocate(time.Now())
	c.key = []byte("k")
	var id [12]byte
	for k := range c.txs {
		id = k
		c.txs[k].authed = true
	}
	m.Build(stun.MessageType{Method: stun.MethodAllocate, Class: stun.ClassSuccessResponse}, id)
	m.AddXORAddress(stun.AttrXORRelayedAddress, peerAddr)
	m.AddXORAddress(stun.AttrXORMappedAddress, peerAddr)
	m.AddLifetime(time.Minute)
	if _, _, ok := c.HandlePacket(serverAddr, m.Raw, time.Now()); !ok || c.state == clientAllocated {
		t.Fatal("a response without integrity was taken")
	}
}

func TestClientConfigAndState(t *testing.T) {
	send := func(netip.AddrPort, []byte) {}
	if _, err := NewClient(ClientConfig{Server: serverAddr}); err == nil {
		t.Fatal("no Send accepted")
	}
	if _, err := NewClient(ClientConfig{Send: send}); err == nil {
		t.Fatal("no Server accepted")
	}
	c, err := NewClient(ClientConfig{Server: serverAddr, Send: send})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0)
	if err := c.CreatePermission(peerAddr.Addr(), now); !errors.Is(err, ErrNotAllocated) {
		t.Fatalf("CreatePermission before Allocate: %v", err)
	}
	if err := c.BindChannel(peerAddr, now); !errors.Is(err, ErrNotAllocated) {
		t.Fatalf("BindChannel before Allocate: %v", err)
	}
	if err := c.Allocate(now); err != nil {
		t.Fatal(err)
	}
	if err := c.Allocate(now); !errors.Is(err, ErrAllocating) {
		t.Fatalf("second Allocate: %v", err)
	}
	// Closed while allocating, the client is done at once.
	c.Close(now)
	if !c.Closed() {
		t.Fatal("not closed")
	}
	if _, ok := c.NextTimeout(); ok {
		t.Fatal("a closed client has timers")
	}
	if err := c.Allocate(now); !errors.Is(err, ErrClosed) {
		t.Fatalf("Allocate after Close: %v", err)
	}
	c.Close(now)
	if !c.Closed() {
		t.Fatal("a second Close reopened it")
	}

	// A Rand that fails panics rather than send a guessable transaction ID.
	c, _ = NewClient(ClientConfig{Server: serverAddr, Send: send, Rand: failingReader{}})
	defer func() {
		if r, _ := recover().(string); r == "" {
			t.Fatal("no panic")
		}
	}()
	_ = c.Allocate(now)
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("entropy exhausted") }

// Requests already in flight are not sent twice.
func TestClientPendingRequests(t *testing.T) {
	n := newSimNet(t, 12)
	newSimServer(n, serverAddr, ServerConfig{})
	c := newSimClient(n, clientAddr, ClientConfig{Software: "fib-test"})
	requests := map[stun.Method]int{}
	software := 0
	n.drop = func(from, to netip.AddrPort, b []byte) bool {
		var m stun.Message
		if from == clientAddr && m.Decode(b) == nil && m.Type.Class == stun.ClassRequest {
			requests[m.Type.Method]++
			if s, _ := m.Get(stun.AttrSoftware); string(s) == "fib-test" {
				software++
			}
		}
		return false
	}
	c.allocate(t)
	for range 2 {
		if err := c.c.CreatePermission(peerAddr.Addr(), n.now); err != nil {
			t.Fatal(err)
		}
		if err := c.c.BindChannel(peerAddr, n.now); err != nil {
			t.Fatal(err)
		}
	}
	if requests[stun.MethodCreatePermission] != 1 || requests[stun.MethodChannelBind] != 1 {
		t.Fatalf("requests %v", requests)
	}
	n.run(time.Second, nil)
	if ch := c.c.chans[peerAddr]; ch == nil || !ch.bound {
		t.Fatal("channel not bound")
	}
	if total := requests[stun.MethodAllocate] + 2; software != total {
		t.Fatalf("SOFTWARE in %d of %d requests", software, total)
	}
}

// Channel numbers are handed out in turn, wrapping past MaxChannel, until
// every one is bound.
func TestClientChannelNumbers(t *testing.T) {
	n := newSimNet(t, 13)
	newSimServer(n, serverAddr, ServerConfig{})
	c := newSimClient(n, clientAddr, ClientConfig{})
	c.allocate(t)
	n.drop = func(from, to netip.AddrPort, b []byte) bool { return true }
	peer := func(i int) netip.AddrPort {
		return netip.AddrPortFrom(netip.AddrFrom4([4]byte{192, 0, 2, byte(i >> 8)}), uint16(1000+i&0xff))
	}
	c.c.nextChan = MaxChannel
	for i := range MaxChannel - MinChannel + 1 {
		if err := c.c.BindChannel(peer(i), n.now); err != nil {
			t.Fatalf("channel %d: %v", i, err)
		}
	}
	if c.c.chans[peer(0)].number != MaxChannel || c.c.chans[peer(1)].number != MinChannel {
		t.Fatalf("numbers %#x %#x", c.c.chans[peer(0)].number, c.c.chans[peer(1)].number)
	}
	if err := c.c.BindChannel(netip.MustParseAddrPort("198.51.100.99:1"), n.now); !errors.Is(err, ErrNoChannels) {
		t.Fatalf("past the last channel: %v", err)
	}
}

// fakeServer answers the client's requests at serverAddr with what reply
// builds, without credentials: the client then sends none either.
func fakeServer(n *simNet, reply func(m *stun.Message, r *stun.Message) bool) {
	n.at[serverAddr] = func(from netip.AddrPort, b []byte) {
		var m, r stun.Message
		if m.Decode(b) != nil || m.Type.Class != stun.ClassRequest {
			return
		}
		r.Build(stun.MessageType{Method: m.Type.Method, Class: stun.ClassSuccessResponse}, m.TransactionID)
		if reply(&m, &r) {
			n.send(serverAddr, from, r.Raw)
		}
	}
}

func allocated(m, r *stun.Message) {
	r.AddXORAddress(stun.AttrXORRelayedAddress, netip.AddrPortFrom(relayIP, 50000))
	r.AddXORAddress(stun.AttrXORMappedAddress, clientAddr)
	r.AddLifetime(10 * time.Minute)
}

func TestClientBadResponses(t *testing.T) {
	t.Run("allocation without addresses", func(t *testing.T) {
		n := newSimNet(t, 14)
		fakeServer(n, func(m, r *stun.Message) bool { r.AddLifetime(time.Minute); return true })
		c := newSimClient(n, clientAddr, ClientConfig{})
		_ = c.c.Allocate(n.now)
		n.run(time.Second, nil)
		if !errors.Is(c.failed, ErrBadResponse) || c.allocated {
			t.Fatalf("failed %v", c.failed)
		}
	})
	t.Run("error without a code", func(t *testing.T) {
		n := newSimNet(t, 15)
		fakeServer(n, func(m, r *stun.Message) bool {
			r.Build(stun.MessageType{Method: m.Type.Method, Class: stun.ClassErrorResponse}, m.TransactionID)
			return true
		})
		c := newSimClient(n, clientAddr, ClientConfig{})
		_ = c.c.Allocate(n.now)
		n.run(time.Second, nil)
		if !errors.Is(c.failed, ErrBadResponse) {
			t.Fatalf("failed %v", c.failed)
		}
	})
	t.Run("refresh without a lifetime", func(t *testing.T) {
		// A Refresh response without LIFETIME is taken to grant the
		// default 600s.
		n := newSimNet(t, 16)
		fakeServer(n, func(m, r *stun.Message) bool {
			if m.Type.Method == stun.MethodAllocate {
				allocated(m, r)
			}
			return true
		})
		c := newSimClient(n, clientAddr, ClientConfig{})
		_ = c.c.Allocate(n.now)
		n.run(time.Second, nil)
		start := n.now
		var refreshes []time.Duration
		n.drop = func(from, to netip.AddrPort, b []byte) bool {
			var m stun.Message
			if from == clientAddr && m.Decode(b) == nil && m.Type.Method == stun.MethodRefresh {
				refreshes = append(refreshes, n.now.Sub(start))
			}
			return false
		}
		n.run(20*time.Minute, nil)
		// The next is due 9 minutes after the response, which takes
		// a round trip.
		if d := DefaultLifetime - time.Minute; len(refreshes) != 2 || refreshes[1]-refreshes[0] < d || refreshes[1]-refreshes[0] > d+time.Second {
			t.Fatalf("refreshes at %v", refreshes)
		}
	})
	t.Run("packets that are not the server's to give", func(t *testing.T) {
		n := newSimNet(t, 17)
		newSimServer(n, serverAddr, ServerConfig{})
		c := newSimClient(n, clientAddr, ClientConfig{})
		c.allocate(t)
		var data stun.Message
		data.Build(stun.MessageType{Method: stun.MethodData, Class: stun.ClassIndication}, stun.NewTransactionID())
		data.AddXORAddress(stun.AttrXORPeerAddress, peerAddr) // no DATA
		var send stun.Message
		send.Build(stun.MessageType{Method: stun.MethodSend, Class: stun.ClassIndication}, stun.NewTransactionID())
		send.AddXORAddress(stun.AttrXORPeerAddress, peerAddr)
		send.Add(stun.AttrData, []byte("x"))
		bad := bytes.Clone(send.Raw)
		bad[3] = 3 // a length not a multiple of four
		var fp stun.Message
		fp.Build(stun.MessageType{Method: stun.MethodData, Class: stun.ClassIndication}, stun.NewTransactionID())
		fp.Add(stun.AttrFingerprint, []byte{1, 2, 3, 4})
		for name, b := range map[string][]byte{
			"ChannelData cut short": {0x40, 0x00, 0x00, 0x08, 1},
			"ChannelData unbound":   AppendChannelData(nil, MinChannel+7, []byte("x")),
			"malformed STUN":        bad,
			"broken FINGERPRINT":    fp.Raw,
			"Data without DATA":     data.Raw,
			"a Send indication":     send.Raw,
		} {
			peer, d, ok := c.c.HandlePacket(serverAddr, b, n.now)
			if !ok || d != nil || peer.IsValid() {
				t.Errorf("%s: %v %q %v", name, peer, d, ok)
			}
		}
		if c.failed != nil {
			t.Fatal(c.failed)
		}
	})
}

// A server that changes realm in a 438 has the client derive its key anew.
func TestClientStaleNonceNewRealm(t *testing.T) {
	n := newSimNet(t, 18)
	srv := newSimServer(n, serverAddr, ServerConfig{})
	c := newSimClient(n, clientAddr, ClientConfig{})
	c.allocate(t)
	// The server's realm changes; its users are the same.
	srv.s.cfg.Realm = "other.test"
	srv.s.SetAuth(StaticAuth("other.test", map[string]string{testUser: testPass}))
	for i := range srv.s.nonceKey {
		srv.s.nonceKey[i] ^= 1
	}
	if err := c.c.CreatePermission(peerAddr.Addr(), n.now); err != nil {
		t.Fatal(err)
	}
	n.run(time.Second, nil)
	if c.c.realm != "other.test" || c.c.perms[peerAddr.Addr()].pending || c.c.perms[peerAddr.Addr()].err != nil {
		t.Fatalf("realm %q, permission %+v", c.c.realm, c.c.perms[peerAddr.Addr()])
	}
}

// A lifetime under two minutes is refreshed halfway through, and one the
// client asks for is what it is granted.
func TestClientLifetimes(t *testing.T) {
	n := newSimNet(t, 19)
	newSimServer(n, serverAddr, ServerConfig{DefaultLifetime: time.Minute, MaxLifetime: time.Hour})
	c := newSimClient(n, clientAddr, ClientConfig{Lifetime: 90 * time.Second})
	c.allocate(t)
	if d := c.c.refreshAt.Sub(n.now); d < 44*time.Second || d > 45*time.Second {
		t.Fatalf("refresh in %v of a 90s lifetime", d)
	}
	n.run(10*time.Minute, nil)
	if c.failed != nil {
		t.Fatal(c.failed)
	}
}

func TestDeallocateLostResponse(t *testing.T) {
	// The response to the deallocation is lost: its retransmission finds
	// the allocation gone, 437, which completes the Close too.
	n := newSimNet(t, 20)
	srv := newSimServer(n, serverAddr, ServerConfig{})
	c := newSimClient(n, clientAddr, ClientConfig{})
	c.allocate(t)
	lost := false
	n.drop = func(from, to netip.AddrPort, b []byte) bool {
		var m stun.Message
		if !lost && from == serverAddr && m.Decode(b) == nil && m.Type.Method == stun.MethodRefresh {
			lost = true
			return true
		}
		return false
	}
	c.c.Close(n.now)
	n.run(time.Minute, func() bool { return c.c.Closed() })
	if !lost || !c.c.Closed() || srv.s.Allocations() != 0 || c.failed != nil {
		t.Fatalf("lost %v closed %v allocations %d failed %v", lost, c.c.Closed(), srv.s.Allocations(), c.failed)
	}

	// With the server gone altogether, Close gives up and is done.
	n = newSimNet(t, 21)
	newSimServer(n, serverAddr, ServerConfig{})
	c = newSimClient(n, clientAddr, ClientConfig{})
	c.allocate(t)
	n.drop = func(from, to netip.AddrPort, b []byte) bool { return true }
	c.c.Close(n.now)
	if !n.run(time.Minute, func() bool { return c.c.Closed() }) || c.failed != nil {
		t.Fatalf("closed %v, failed %v", c.c.Closed(), c.failed)
	}
}

// A permission the client keeps only by refreshing it, with no data going
// to the peer, survives a refresh whose every retransmission is lost.
func TestPermissionRefreshRetried(t *testing.T) {
	n := newSimNet(t, 22)
	newSimServer(n, serverAddr, ServerConfig{})
	c := newSimClient(n, clientAddr, ClientConfig{})
	c.allocate(t)
	relayed, _, _ := c.c.Relayed()
	if err := c.c.CreatePermission(peerAddr.Addr(), n.now); err != nil {
		t.Fatal(err)
	}
	n.run(time.Second, nil)
	start := n.now
	n.drop = func(from, to netip.AddrPort, b []byte) bool {
		var m stun.Message
		// The first refresh's transaction, from 4 minutes on, goes
		// unanswered for its whole 39.5s.
		return from == clientAddr && n.now.Sub(start) < 4*time.Minute+45*time.Second &&
			m.Decode(b) == nil && m.Type.Method == stun.MethodCreatePermission
	}
	n.run(PermissionLifetime+time.Minute, nil)
	n.send(peerAddr, relayed, []byte("still"))
	n.run(time.Second, nil)
	if len(c.got) != 1 || c.got[0] != (received{peerAddr, "still"}) {
		t.Fatalf("the permission lapsed: client got %v", c.got)
	}
}

// SendTo a peer the server refused a permission for reports the refusal
// and sends nothing.
func TestSendToRefusedPeer(t *testing.T) {
	n := newSimNet(t, 23)
	fakeServer(n, func(m, r *stun.Message) bool {
		if m.Type.Method == stun.MethodAllocate {
			allocated(m, r)
			return true
		}
		r.Build(stun.MessageType{Method: m.Type.Method, Class: stun.ClassErrorResponse}, m.TransactionID)
		r.AddErrorCode(stun.CodeForbidden, "Forbidden")
		return true
	})
	c := newSimClient(n, clientAddr, ClientConfig{})
	c.allocate(t)
	if err := c.c.SendTo(peerAddr, []byte("first")); err != nil {
		t.Fatal(err)
	}
	n.run(time.Second, nil)
	sent := 0
	n.drop = func(from, to netip.AddrPort, b []byte) bool {
		sent++
		return false
	}
	var e *Error
	if err := c.c.SendTo(peerAddr, []byte("second")); !errors.As(err, &e) || e.Code != stun.CodeForbidden {
		t.Fatalf("SendTo a refused peer: %v", err)
	}
	if sent != 0 {
		t.Fatalf("%d packets sent", sent)
	}
}

func TestICERelayIgnoresOthers(t *testing.T) {
	c, _ := NewClient(ClientConfig{Server: serverAddr, Send: func(netip.AddrPort, []byte) {}})
	r := NewICERelay(nil, 2, c)
	if _, ok := r.Candidate(); ok {
		t.Fatal("a candidate before the allocation")
	}
	if _, _, handled := r.HandlePacket(peerAddr, []byte{0x16, 0xfe, 0xfd}, time.Now()); handled {
		t.Fatal("handled a peer's packet")
	}
}

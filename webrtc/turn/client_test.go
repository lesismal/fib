package turn

import (
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

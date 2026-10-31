package turn

import (
	"bytes"
	"net/netip"
	"testing"
	"time"

	"github.com/lesismal/fib/webrtc/stun"
)

// seedRequests are well-formed requests for the server fuzzer to mutate,
// signed with the credentials it accepts and a nonce it issued.
func seedRequests(f *testing.F, s *Server, now time.Time) {
	nonce := string(s.makeNonce(clientAddr, now.Add(time.Hour).Unix()))
	key := stun.LongTermKey(testUser, testRealm, testPass)
	build := func(method stun.Method, class stun.Class, add func(m *stun.Message)) {
		var m stun.Message
		m.Build(stun.MessageType{Method: method, Class: class}, stun.NewTransactionID())
		add(&m)
		if class == stun.ClassRequest {
			m.AddString(stun.AttrUsername, testUser)
			m.AddString(stun.AttrRealm, testRealm)
			m.AddString(stun.AttrNonce, nonce)
			m.AddIntegrity(key)
		}
		m.AddFingerprint()
		f.Add(bytes.Clone(m.Raw), false)
	}
	build(stun.MethodAllocate, stun.ClassRequest, allocateReq)
	build(stun.MethodRefresh, stun.ClassRequest, func(m *stun.Message) { m.AddLifetime(time.Minute) })
	build(stun.MethodCreatePermission, stun.ClassRequest, permissionReq(peerAddr, netip.MustParseAddrPort("192.0.2.9:1")))
	build(stun.MethodChannelBind, stun.ClassRequest, channelReq(MinChannel, peerAddr))
	build(stun.MethodSend, stun.ClassIndication, func(m *stun.Message) {
		m.AddXORAddress(stun.AttrXORPeerAddress, peerAddr)
		m.Add(stun.AttrData, []byte("x"))
	})
	build(stun.MethodBinding, stun.ClassRequest, func(*stun.Message) {})
	f.Add(AppendChannelData(nil, MinChannel, []byte("x")), false)
	f.Add([]byte("peer datagram"), true)
}

// FuzzServer feeds the server an allocation's worth of state and then
// arbitrary datagrams, from the client or from a peer to the relay.
func FuzzServer(f *testing.F) {
	now := time.Unix(1_700_000_000, 0)
	mk := func() (*Server, *coreRelay) {
		var relay *coreRelay
		s, err := NewServer(ServerConfig{
			Realm:       testRealm,
			Auth:        StaticAuth(testRealm, map[string]string{testUser: testPass}),
			NonceSecret: []byte("fixed"),
			Send:        func(FiveTuple, []byte) {},
			AllocateRelay: func(_ FiveTuple, _ string, _ bool, done func(Relay, error)) {
				relay = &coreRelay{addr: netip.AddrPortFrom(relayIP, 50000)}
				done(relay, nil)
			},
		})
		if err != nil {
			f.Fatal(err)
		}
		return s, relay
	}
	s, _ := mk()
	seedRequests(f, s, now)
	f.Fuzz(func(t *testing.T, b []byte, fromPeer bool) {
		s, _ := mk()
		tuple := FiveTuple{Client: clientAddr, Server: serverAddr}
		// Allocate, permit and bind, so the fuzzed datagram meets state.
		nonce := string(s.makeNonce(clientAddr, now.Add(time.Hour).Unix()))
		key := stun.LongTermKey(testUser, testRealm, testPass)
		for _, add := range []func(*stun.Message){allocateReq, channelReq(MinChannel, peerAddr)} {
			var m stun.Message
			method := stun.MethodAllocate
			if s.Allocations() > 0 {
				method = stun.MethodChannelBind
			}
			m.Build(stun.MessageType{Method: method, Class: stun.ClassRequest}, stun.NewTransactionID())
			add(&m)
			m.AddString(stun.AttrUsername, testUser)
			m.AddString(stun.AttrRealm, testRealm)
			m.AddString(stun.AttrNonce, nonce)
			m.AddIntegrity(key)
			_ = s.HandlePacket(tuple, m.Raw, now)
		}
		if s.Allocations() != 1 {
			t.Fatal("setup failed")
		}
		in := bytes.Clone(b)
		if fromPeer {
			_ = s.HandleRelayPacket(netip.AddrPortFrom(relayIP, 50000), peerAddr, in, now)
		} else {
			_ = s.HandlePacket(tuple, in, now)
		}
		s.HandleTimeout(now.Add(2 * time.Hour))
		if s.Allocations() != 0 {
			t.Fatal("allocation outlived its lifetime")
		}
	})
}

// FuzzClient feeds an allocated client arbitrary datagrams from the server.
func FuzzClient(f *testing.F) {
	var m stun.Message
	m.Build(stun.MessageType{Method: stun.MethodData, Class: stun.ClassIndication}, stun.NewTransactionID())
	m.AddXORAddress(stun.AttrXORPeerAddress, peerAddr)
	m.Add(stun.AttrData, []byte("x"))
	f.Add(bytes.Clone(m.Raw))
	f.Add(AppendChannelData(nil, MinChannel, []byte("x")))
	m.Build(stun.MessageType{Method: stun.MethodRefresh, Class: stun.ClassErrorResponse}, stun.NewTransactionID())
	m.AddErrorCode(stun.CodeStaleNonce, "Stale Nonce")
	m.AddString(stun.AttrNonce, "n")
	f.Add(bytes.Clone(m.Raw))
	f.Fuzz(func(t *testing.T, b []byte) {
		now := time.Unix(1_700_000_000, 0)
		n := newSimNet(t, 1)
		newSimServer(n, serverAddr, ServerConfig{})
		c := newSimClient(n, clientAddr, ClientConfig{BindChannels: true})
		newSimPeer(n, peerAddr, true)
		_ = c.c.Allocate(n.now)
		n.run(time.Second, nil)
		_ = c.c.SendTo(peerAddr, []byte("x"))
		n.run(time.Second, nil)
		_, d, ok := c.c.HandlePacket(serverAddr, bytes.Clone(b), now)
		if d != nil && !ok {
			t.Fatal("data from a packet not handled")
		}
		c.c.HandleTimeout(n.now.Add(time.Hour))
		c.c.Close(n.now)
	})
}

package turn

import (
	"bytes"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/webrtc/stun"
)

// asyncCore is a core whose host opens relayed addresses later: each
// AllocateRelay's done waits in dones until the test calls it.
func asyncCore(t *testing.T, cfg ServerConfig) (*core, *[]func(Relay, error)) {
	c := newCore(t, cfg)
	var dones []func(Relay, error)
	c.s.cfg.AllocateRelay = func(_ FiveTuple, _ string, _ bool, done func(Relay, error)) {
		dones = append(dones, done)
	}
	return c, &dones
}

// request sends u's request of method with id, signed, and returns how many
// responses it drew.
func (u *user) request(method stun.Method, id [12]byte, add func(m *stun.Message)) int {
	var m stun.Message
	m.Build(stun.MessageType{Method: method, Class: stun.ClassRequest}, id)
	if add != nil {
		add(&m)
	}
	m.AddString(stun.AttrUsername, u.name)
	m.AddString(stun.AttrRealm, testRealm)
	m.AddString(stun.AttrNonce, u.nonce)
	m.AddIntegrity(u.key)
	n := len(u.c.sent)
	if err := u.c.s.HandlePacket(u.tuple(), m.Raw, u.c.now); err != nil {
		u.c.t.Fatal(err)
	}
	return len(u.c.sent) - n
}

func (c *core) last() *stun.Message {
	var r stun.Message
	if err := r.Decode(c.sent[len(c.sent)-1].b); err != nil {
		c.t.Fatal(err)
	}
	return &r
}

// An Allocate is answered when the host's done runs, and not before; its
// retransmissions meanwhile are dropped, and it counts against the quotas.
func TestServerAllocateAsync(t *testing.T) {
	c, dones := asyncCore(t, ServerConfig{MaxAllocations: 1})
	u := c.user(clientAddr, testUser, testPass)
	// Learn a nonce with a request that is refused, then ask.
	_ = u.do(stun.MethodRefresh, nil)
	id := stun.NewTransactionID()
	if n := u.request(stun.MethodAllocate, id, allocateReq); n != 0 || len(*dones) != 1 {
		t.Fatalf("%d responses before the relay, %d asked", n, len(*dones))
	}
	if n := u.request(stun.MethodAllocate, id, allocateReq); n != 0 || len(*dones) != 1 {
		t.Fatalf("a retransmission: %d responses, %d asked", n, len(*dones))
	}
	if n := u.request(stun.MethodAllocate, stun.NewTransactionID(), allocateReq); n != 1 || code(c.last()) != stun.CodeAllocationMismatch {
		t.Fatal("another Allocate on the 5-tuple in the making was not a mismatch")
	}
	v := c.user(netip.MustParseAddrPort("198.51.100.10:40100"), "bob", "hunter2")
	if r := v.do(stun.MethodAllocate, allocateReq); code(r) != stun.CodeInsufficientCapacity {
		t.Fatalf("an allocation in the making does not count: %d", code(r))
	}
	if c.s.Allocations() != 0 {
		t.Fatal("counted before its relay")
	}
	relay := &coreRelay{c: c, addr: netip.AddrPortFrom(relayIP, 51000)}
	(*dones)[0](relay, nil)
	r := c.last()
	if code(r) != 0 || r.TransactionID != id || r.CheckIntegrity(u.key) != nil {
		t.Fatalf("Allocate answered %d", code(r))
	}
	if a, _ := r.XORAddress(stun.AttrXORRelayedAddress); a != relay.addr || c.s.Allocations() != 1 {
		t.Fatalf("relayed %v, %d allocations", a, c.s.Allocations())
	}
	// A second call of done changes nothing, but closes what it brings.
	extra := &coreRelay{c: c, addr: netip.AddrPortFrom(relayIP, 51001)}
	n := len(c.sent)
	(*dones)[0](extra, nil)
	(*dones)[0](nil, ErrNoRelay)
	if !extra.closed || len(c.sent) != n || c.s.Allocations() != 1 {
		t.Fatal("a second done was not ignored")
	}

	// A relay that fails is answered 508, and frees its place.
	c2, dones2 := asyncCore(t, ServerConfig{MaxAllocationsPerUser: 1})
	w := c2.user(clientAddr, testUser, testPass)
	_ = w.do(stun.MethodRefresh, nil)
	_ = w.request(stun.MethodAllocate, stun.NewTransactionID(), allocateReq)
	(*dones2)[0](nil, ErrNoRelay)
	if code(c2.last()) != stun.CodeInsufficientCapacity {
		t.Fatalf("a failed relay: %d", code(c2.last()))
	}
	_ = w.request(stun.MethodAllocate, stun.NewTransactionID(), allocateReq)
	if len(*dones2) != 2 {
		t.Fatal("a failed relay kept its user's quota")
	}
	// A server closed meanwhile closes the relay and says nothing.
	c2.s.Close()
	late := &coreRelay{c: c2, addr: netip.AddrPortFrom(relayIP, 51002)}
	n = len(c2.sent)
	(*dones2)[1](late, nil)
	if !late.closed || len(c2.sent) != n || c2.s.Allocations() != 0 {
		t.Fatal("a relay ready after Close was kept")
	}
	if w.request(stun.MethodAllocate, stun.NewTransactionID(), allocateReq) != 0 || len(*dones2) != 2 {
		t.Fatal("a closed server allocates")
	}
	if _, ok := c2.s.NextTimeout(); ok {
		t.Fatal("a closed server wants a sweep")
	}
}

// lockedCore is a core whose host is safe for concurrent use, for tests
// that drive it from several goroutines.
type lockedCore struct {
	s      *Server
	mu     sync.Mutex
	sent   map[FiveTuple]int
	relays map[netip.AddrPort]*lockedRelay
	port   atomic.Uint32
	toPeer atomic.Int64
}

type lockedRelay struct {
	h      *lockedCore
	addr   netip.AddrPort
	closed atomic.Bool
}

func (r *lockedRelay) Addr() netip.AddrPort { return r.addr }
func (r *lockedRelay) SendTo(netip.AddrPort, []byte) {
	r.h.toPeer.Add(1)
}
func (r *lockedRelay) Close() { r.closed.Store(true) }

// TestServerConcurrent drives one server from many goroutines at once:
// clients that allocate, permit, bind, relay both ways, refresh and
// deallocate, alongside sweeps, so that the race detector sees every path
// that runs in parallel.
func TestServerConcurrent(t *testing.T) {
	h := &lockedCore{sent: map[FiveTuple]int{}, relays: map[netip.AddrPort]*lockedRelay{}}
	h.port.Store(40000)
	responses := map[FiveTuple]chan []byte{}
	var rmu sync.Mutex
	s, err := NewServer(ServerConfig{
		Realm: testRealm,
		Auth:  StaticAuth(testRealm, map[string]string{testUser: testPass}),
		Send: func(t FiveTuple, b []byte) {
			rmu.Lock()
			ch := responses[t]
			rmu.Unlock()
			if stun.IsMessage(b) {
				var m stun.Message
				if m.Decode(b) == nil && m.Type.Class != stun.ClassIndication {
					ch <- bytes.Clone(b)
				}
			}
		},
		AllocateRelay: func(_ FiveTuple, _ string, _ bool, done func(Relay, error)) {
			r := &lockedRelay{h: h, addr: netip.AddrPortFrom(relayIP, uint16(h.port.Add(1)))}
			h.mu.Lock()
			h.relays[r.addr] = r
			h.mu.Unlock()
			// Half the relays are ready later, from another goroutine.
			if r.addr.Port()%2 == 0 {
				go done(r, nil)
				return
			}
			done(r, nil)
		},
		AllowedPeers:  []netip.Prefix{netip.MustParsePrefix("192.0.2.0/24")},
		SweepInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	h.s = s
	const clients = 16
	stop := make(chan struct{})
	var sweeper sync.WaitGroup
	sweeper.Add(1)
	go func() {
		defer sweeper.Done()
		for {
			select {
			case <-stop:
				return
			default:
				s.HandleTimeout(time.Now())
				_, _ = s.NextTimeout()
				_ = s.Allocations()
			}
		}
	}()
	var wg sync.WaitGroup
	for i := range clients {
		tuple := FiveTuple{Client: netip.AddrPortFrom(netip.MustParseAddr("198.51.100.20"), uint16(1000+i)), Server: serverAddr}
		ch := make(chan []byte, 64)
		rmu.Lock()
		responses[tuple] = ch
		rmu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			key := stun.LongTermKey(testUser, testRealm, testPass)
			var nonce []byte
			ask := func(method stun.Method, add func(*stun.Message)) *stun.Message {
				for {
					var m stun.Message
					m.Build(stun.MessageType{Method: method, Class: stun.ClassRequest}, stun.NewTransactionID())
					if add != nil {
						add(&m)
					}
					if nonce != nil {
						m.AddString(stun.AttrUsername, testUser)
						m.AddString(stun.AttrRealm, testRealm)
						m.Add(stun.AttrNonce, nonce)
						m.AddIntegrity(key)
					}
					_ = s.HandlePacket(tuple, m.Raw, time.Now())
					var r stun.Message
					if err := r.Decode(<-ch); err != nil {
						t.Error(err)
						return &r
					}
					if n, ok := r.Get(stun.AttrNonce); ok {
						nonce = bytes.Clone(n)
						continue
					}
					return &r
				}
			}
			peer := netip.AddrPortFrom(netip.MustParseAddr("192.0.2.9"), uint16(2000+i))
			for round := range 5 {
				r := ask(stun.MethodAllocate, allocateReq)
				if code(r) != 0 {
					t.Errorf("Allocate: %d", code(r))
					return
				}
				relayed, _ := r.XORAddress(stun.AttrXORRelayedAddress)
				_ = ask(stun.MethodCreatePermission, permissionReq(peer))
				_ = ask(stun.MethodChannelBind, channelReq(MinChannel+uint16(round), peer))
				for range 50 {
					_ = s.HandlePacket(tuple, AppendChannelData(nil, MinChannel+uint16(round), []byte("x")), time.Now())
					_ = s.HandleRelayPacket(relayed, peer, []byte("y"), time.Now())
				}
				_ = ask(stun.MethodRefresh, nil)
				_ = ask(stun.MethodRefresh, func(m *stun.Message) { m.AddLifetime(0) })
			}
		}()
	}
	wg.Wait()
	close(stop)
	sweeper.Wait()
	if s.Allocations() != 0 {
		t.Fatalf("%d allocations left", s.Allocations())
	}
	if got := h.toPeer.Load(); got != clients*5*50 {
		t.Fatalf("%d datagrams to peers", got)
	}
	for _, r := range h.relays {
		if !r.closed.Load() {
			t.Fatalf("relay %v left open", r.addr)
		}
	}
}

// A request or datagram that finds an allocation just as another goroutine
// frees it does nothing with it.
func TestServerStaleAllocation(t *testing.T) {
	c := newCore(t, ServerConfig{})
	u := c.user(clientAddr, testUser, testPass)
	_ = u.do(stun.MethodAllocate, allocateReq)
	_ = u.do(stun.MethodCreatePermission, permissionReq(peerAddr))
	a := c.s.lookup(u.tuple())
	c.s.free(a, FreedDeleted)
	c.s.free(a, FreedDeleted)
	for _, tc := range []struct {
		method stun.Method
		add    func(*stun.Message)
		run    func(*exchange, *allocation)
	}{
		{stun.MethodRefresh, nil, c.s.refresh},
		{stun.MethodCreatePermission, permissionReq(peerAddr), c.s.createPermission},
		{stun.MethodChannelBind, channelReq(MinChannel, peerAddr), c.s.channelBind},
	} {
		x := getScratch()
		x.in.Build(stun.MessageType{Method: tc.method, Class: stun.ClassRequest}, stun.NewTransactionID())
		if tc.add != nil {
			tc.add(&x.in)
		}
		tc.run(&exchange{s: c.s, t: u.tuple(), m: &x.in, x: x, now: c.now, user: testUser, key: u.key}, a)
		if code(c.last()) != stun.CodeAllocationMismatch {
			t.Errorf("%v on a freed allocation: %d", tc.method, code(c.last()))
		}
	}
	// A Send indication that finds its allocation expired frees it.
	_ = u.do(stun.MethodAllocate, allocateReq)
	_ = u.do(stun.MethodCreatePermission, permissionReq(peerAddr))
	var m stun.Message
	m.Build(stun.MessageType{Method: stun.MethodSend, Class: stun.ClassIndication}, stun.NewTransactionID())
	m.AddXORAddress(stun.AttrXORPeerAddress, peerAddr)
	m.Add(stun.AttrData, []byte("x"))
	if err := c.s.HandlePacket(u.tuple(), m.Raw, c.now.Add(DefaultLifetime)); err != ErrDropped || c.s.Allocations() != 0 {
		t.Fatalf("Send on an expired allocation: %v, %d allocations", err, c.s.Allocations())
	}
	// Close frees what is held and cancels what is in the making.
	c2, dones := asyncCore(t, ServerConfig{})
	v := c2.user(clientAddr, testUser, testPass)
	_ = v.do(stun.MethodRefresh, nil)
	_ = v.request(stun.MethodAllocate, stun.NewTransactionID(), allocateReq)
	(*dones)[0](&coreRelay{c: c2, addr: netip.AddrPortFrom(relayIP, 52000)}, nil)
	w := c2.user(netip.MustParseAddrPort("198.51.100.10:40200"), testUser, testPass)
	_ = w.do(stun.MethodRefresh, nil)
	_ = w.request(stun.MethodAllocate, stun.NewTransactionID(), allocateReq)
	c2.s.Close()
	if c2.s.Allocations() != 0 {
		t.Fatal("Close left an allocation")
	}
}

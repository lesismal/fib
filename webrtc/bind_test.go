package webrtc

import (
	"net/netip"
	"testing"

	"github.com/lesismal/fib/webrtc/stun"
)

// TestSpoofedBindingRequest checks that a Binding request routed by its
// ufrag binds its source address only once it is answered, and never
// takes the address another PeerConnection on the Mux is connected to,
// however valid its credentials: anyone can send a request for a
// PeerConnection of their own from an address they spoof.
func TestSpoofedBindingRequest(t *testing.T) {
	a, b := pair(t, false)
	echoChannels(b)
	ca, cb := connectedCh(a), connectedCh(b)
	d := openChannel(t, a, "echo")
	negotiate(t, a, b, nil)
	wait(t, ca, "a connected")
	wait(t, cb, "b connected")
	waitOpen(t, d)
	a.mu.Lock()
	victim := a.remoteAddr
	a.mu.Unlock()
	a.mux.mu.RLock()
	owner := a.mux.peers[victim]
	a.mux.mu.RUnlock()
	if !victim.IsValid() || owner != a {
		t.Fatalf("victim %v not bound to a", victim)
	}

	// The attacker's own PeerConnection on the same Mux, with an agent.
	c := newPC(t, Configuration{Mux: a.mux})
	if _, err := c.CreateDataChannel("x", nil); err != nil {
		t.Fatal(err)
	}
	offer, err := c.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	request := func(pwd string) []byte {
		var m stun.Message
		m.Build(stun.BindingRequest, stun.NewTransactionID())
		m.Add(stun.AttrUsername, []byte(c.ufrag+":peer"))
		m.AddUint32(stun.AttrPriority, 1)
		m.AddUint64(stun.AttrICEControlled, 1)
		m.AddIntegrity([]byte(pwd))
		m.AddFingerprint()
		return m.Raw
	}
	bound := func(addr netip.AddrPort) *PeerConnection {
		a.mux.mu.RLock()
		defer a.mux.mu.RUnlock()
		return a.mux.peers[addr]
	}

	// Wrong credentials from a fresh address: answered 401, bound nowhere.
	fresh := netip.MustParseAddrPort("127.0.0.1:9")
	c.handleDatagram(fresh, request("wrong"))
	if p := bound(fresh); p != nil {
		t.Fatal("a request that failed its MESSAGE-INTEGRITY bound its source")
	}
	// Right ones: the answer binds it.
	c.handleDatagram(fresh, request(c.pwd))
	if p := bound(fresh); p != c {
		t.Fatal("an answered request did not bind its source")
	}
	// Right ones from the address a is connected to: answered, but a keeps
	// it.
	c.handleDatagram(victim, request(c.pwd))
	if p := bound(victim); p != a {
		t.Fatal("a spoofed request took a connected PeerConnection's address")
	}
	roundTrip(t, d, [][]byte{[]byte("still here")})
}

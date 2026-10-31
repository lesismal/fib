package turn

import (
	"net/netip"
	"testing"
	"time"

	"github.com/lesismal/fib/webrtc/ice"
	"github.com/lesismal/fib/webrtc/stun"
)

const (
	hostID  ice.LocalID = 1
	relayID ice.LocalID = 2
)

// icePeer is an ICE agent with one host socket, through which its TURN
// client speaks to the server, and a relay candidate on the allocation.
type icePeer struct {
	n        *simNet
	host     netip.AddrPort
	agent    *ice.Agent
	relay    *ICERelay
	other    *icePeer
	payloads []received
}

func newICEPeer(t *testing.T, n *simNet, host netip.AddrPort, controlling bool) *icePeer {
	p := &icePeer{n: n, host: host}
	client, err := NewClient(ClientConfig{
		Server:       serverAddr,
		Username:     testUser,
		Password:     testPass,
		BindChannels: true,
		Send:         func(to netip.AddrPort, b []byte) { n.send(host, to, b) },
	})
	if err != nil {
		t.Fatal(err)
	}
	agent, err := ice.NewAgent(ice.Config{
		Controlling: controlling,
		Send: func(local ice.LocalID, to netip.AddrPort, b []byte) {
			if local == relayID {
				p.relay.Send(to, b)
			} else {
				n.send(host, to, b)
			}
		},
		// Only relay candidates are signaled, so the peers can reach each
		// other only through the server.
		OnCandidate: func(c ice.Candidate) {
			if c.Type == ice.Relay && p.other != nil {
				if err := p.other.agent.AddRemoteCandidate(c, n.now); err != nil {
					t.Fatal(err)
				}
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	p.agent = agent
	p.relay = NewICERelay(agent, relayID, client)
	n.at[host] = func(from netip.AddrPort, b []byte) {
		if peer, payload, handled := p.relay.HandlePacket(from, b, n.now); handled {
			if payload != nil {
				p.payloads = append(p.payloads, received{peer, string(payload)})
			}
			return
		}
		if stun.IsMessage(b) {
			_ = agent.HandlePacket(hostID, from, b, n.now)
		}
	}
	n.timers = append(n.timers, agent, p.relay)
	return p
}

func TestICEOverRelay(t *testing.T) {
	for _, loss := range []float64{0, 0.2} {
		n := newSimNet(t, 11)
		n.loss = loss
		newSimServer(n, serverAddr, ServerConfig{})
		a := newICEPeer(t, n, netip.MustParseAddrPort("198.51.100.10:1000"), true)
		b := newICEPeer(t, n, netip.MustParseAddrPort("198.51.100.20:2000"), false)
		a.other, b.other = b, a
		for _, p := range []*icePeer{a, b} {
			if err := p.agent.Gather([]ice.Socket{{ID: hostID, Addrs: []netip.AddrPort{p.host}}}, n.now); err != nil {
				t.Fatal(err)
			}
			if err := p.relay.Client.Allocate(n.now); err != nil {
				t.Fatal(err)
			}
		}
		ua, pa := a.agent.LocalCredentials()
		ub, pb := b.agent.LocalCredentials()
		_ = a.agent.SetRemoteCredentials(ub, pb, n.now)
		_ = b.agent.SetRemoteCredentials(ua, pa, n.now)
		connected := func() bool {
			return a.agent.State() == ice.StateConnected && b.agent.State() == ice.StateConnected
		}
		if !n.run(time.Minute, connected) {
			t.Fatalf("loss %v: not connected: %v %v", loss, a.agent.State(), b.agent.State())
		}
		ra, _ := a.relay.Candidate()
		rb, _ := b.relay.Candidate()
		for _, p := range []*icePeer{a, b} {
			sp, _ := p.agent.SelectedPair()
			if sp.Local.Type != ice.Relay || sp.Local.LocalID != relayID {
				t.Fatalf("loss %v: selected local %v", loss, sp.Local.String())
			}
		}
		if sp, _ := a.agent.SelectedPair(); sp.Remote.AddrPort() != rb.AddrPort() {
			t.Fatalf("loss %v: A's remote %v, B's relay %v", loss, sp.Remote.AddrPort(), rb.AddrPort())
		}
		if ra.RelatedAddress != a.host {
			t.Fatalf("relay candidate's related address %v", ra.RelatedAddress)
		}
		// What is not STUN, DTLS here, comes back to the caller with the
		// peer's relayed address.
		record := []byte{0x16, 0xfe, 0xfd, 0, 1, 2, 3}
		ok := n.run(10*time.Second, func() bool {
			if len(b.payloads) == 0 {
				sp, _ := a.agent.SelectedPair()
				a.relay.Send(sp.Remote.AddrPort(), record)
			}
			return len(b.payloads) > 0
		})
		if !ok || b.payloads[0] != (received{ra.AddrPort(), string(record)}) {
			t.Fatalf("loss %v: B got %v", loss, b.payloads)
		}
		// Consent keeps flowing through the relay, its permissions and
		// channels refreshed, well past their lifetimes. Over four lossy
		// hops a consent check is lost now and then, which may report a
		// passing disconnection, but never a failure.
		lost := func() bool {
			if loss > 0 {
				return a.agent.State() == ice.StateFailed || b.agent.State() == ice.StateFailed
			}
			return !connected()
		}
		if n.run(12*time.Minute, lost) {
			t.Fatalf("loss %v: lost the connection: %v %v", loss, a.agent.State(), b.agent.State())
		}
	}
}

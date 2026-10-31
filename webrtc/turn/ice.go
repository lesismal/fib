package turn

import (
	"net/netip"
	"time"

	"github.com/lesismal/fib/webrtc/ice"
	"github.com/lesismal/fib/webrtc/stun"
)

// ICERelay makes a Client's allocation a relay candidate of an ice.Agent,
// on the agent's transport ID. The caller drives the client and the relay
// together:
//
//   - the agent's Config.Send passes what it sends on ID to Send, which
//     wraps it for the server;
//   - every packet the socket the client speaks through receives goes to
//     HandlePacket first: one that is not the server's comes back unhandled,
//     for the caller's usual demultiplexing, and a peer's payload relayed
//     through the server goes to the agent if it is STUN, or back to the
//     caller, a DTLS record or SRTP packet, with the peer's address;
//   - HandleTimeout and NextTimeout stand for the client's.
//
// Once the allocation is made, HandlePacket adds the relay candidate to the
// agent, which reports it through OnCandidate to be signaled.
type ICERelay struct {
	Client *Client
	Agent  *ice.Agent
	ID     ice.LocalID

	added bool
}

// NewICERelay returns the relay that connects client to agent as transport
// id. The client's allocation may be made before or after.
func NewICERelay(agent *ice.Agent, id ice.LocalID, client *Client) *ICERelay {
	return &ICERelay{Client: client, Agent: agent, ID: id}
}

// Candidate returns the relay candidate: the relayed address, with the
// mapped address as its related address (RFC 8445 section 5.1.1.2).
func (r *ICERelay) Candidate() (ice.Candidate, bool) {
	relayed, mapped, ok := r.Client.Relayed()
	if !ok {
		return ice.Candidate{}, false
	}
	return ice.Candidate{
		Type:           ice.Relay,
		Address:        relayed.Addr(),
		Port:           relayed.Port(),
		RelatedAddress: mapped,
		LocalID:        r.ID,
	}, true
}

// Send sends b to the peer at to through the relay; it is what the agent's
// Config.Send does with b when the transport is the relay's ID.
func (r *ICERelay) Send(to netip.AddrPort, b []byte) { _ = r.Client.SendTo(to, b) }

// HandlePacket passes the client a packet from the address from. handled
// is false if the packet is not the server's. A STUN message a peer sent
// through the relay goes to the agent; any other payload a peer sent is
// returned, aliasing b, with the peer's address.
func (r *ICERelay) HandlePacket(from netip.AddrPort, b []byte, now time.Time) (peer netip.AddrPort, payload []byte, handled bool) {
	peer, data, ok := r.Client.HandlePacket(from, b, now)
	if !ok {
		return netip.AddrPort{}, nil, false
	}
	r.addCandidate(now)
	if data == nil {
		return netip.AddrPort{}, nil, true
	}
	if stun.IsMessage(data) {
		_ = r.Agent.HandlePacket(r.ID, peer, data, now)
		return netip.AddrPort{}, nil, true
	}
	return peer, data, true
}

func (r *ICERelay) addCandidate(now time.Time) {
	if r.added {
		return
	}
	if c, ok := r.Candidate(); ok {
		r.added = true
		_ = r.Agent.AddLocalCandidate(c, now)
	}
}

// NextTimeout is the client's.
func (r *ICERelay) NextTimeout() (time.Time, bool) { return r.Client.NextTimeout() }

// HandleTimeout is the client's.
func (r *ICERelay) HandleTimeout(now time.Time) { r.Client.HandleTimeout(now) }

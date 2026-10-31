package turn

import (
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/lesismal/fib/webrtc/stun"
)

// EventKind says what an Event reports.
type EventKind uint8

const (
	// EventAllocated: an allocation was made, for Lifetime.
	EventAllocated EventKind = iota + 1
	// EventRefreshed: a Refresh extended an allocation by Lifetime.
	EventRefreshed
	// EventFreed: an allocation ended, for Reason, having relayed ToPeer
	// and ToClient.
	EventFreed
	// EventRefused: a request whose credentials passed was answered with
	// the error Code: a quota, a capacity, a forbidden peer, a mismatch.
	EventRefused
	// EventAuthFailed: a request came with credentials that did not pass,
	// a wrong user, password or realm (Code 401) or incomplete ones (400).
	// User is the name it claimed. A request without credentials, which
	// is how every client starts, and one with a stale nonce are not
	// failures.
	EventAuthFailed
)

func (k EventKind) String() string {
	switch k {
	case EventAllocated:
		return "allocated"
	case EventRefreshed:
		return "refreshed"
	case EventFreed:
		return "freed"
	case EventRefused:
		return "refused"
	case EventAuthFailed:
		return "auth failed"
	}
	return "unknown"
}

// FreeReason says why an allocation ended.
type FreeReason uint8

const (
	// FreedDeleted: the client sent a Refresh with a zero lifetime.
	FreedDeleted FreeReason = iota + 1
	// FreedExpired: its lifetime ran out.
	FreedExpired
	// FreedClosed: the server was closed, or the client's stream
	// connection (TCP or TLS) was.
	FreedClosed
)

func (r FreeReason) String() string {
	switch r {
	case FreedDeleted:
		return "deleted"
	case FreedExpired:
		return "expired"
	case FreedClosed:
		return "closed"
	}
	return "unknown"
}

// Event is what ServerConfig.OnEvent is told: one thing that happened to an
// allocation or a request, the fields its Kind names set.
type Event struct {
	Kind  EventKind
	Tuple FiveTuple
	User  string
	// Relay is the allocation's relayed address.
	Relay    netip.AddrPort
	Lifetime time.Duration
	// Method and Code are the refused request's and the error it was
	// answered with.
	Method stun.Method
	Code   int
	Reason FreeReason
	// ToPeer and ToClient are what a freed allocation relayed.
	ToPeer, ToClient Traffic
}

// Traffic counts datagrams relayed one way, and their payload bytes.
type Traffic struct {
	Packets, Bytes uint64
}

type trafficCounter struct {
	packets, bytes atomic.Uint64
}

func (c *trafficCounter) add(n int) {
	c.packets.Add(1)
	c.bytes.Add(uint64(n))
}

func (c *trafficCounter) addTraffic(t Traffic) {
	c.packets.Add(t.Packets)
	c.bytes.Add(t.Bytes)
}

func (c *trafficCounter) load() Traffic {
	return Traffic{Packets: c.packets.Load(), Bytes: c.bytes.Load()}
}

// Stats is a snapshot of what a server holds and has done, for metrics.
type Stats struct {
	// Allocations, Permissions and Channels are those held now, expired
	// ones the next sweep will free among them; Pending are allocations
	// waiting for their relayed address.
	Allocations, Pending, Permissions, Channels int
	// ToPeers and ToClients are what was relayed each way since the
	// server was made.
	ToPeers, ToClients Traffic
	// Dropped counts the datagrams HandlePacket and HandleRelayPacket did
	// nothing with: malformed, without an allocation, a permission or a
	// channel, or refused by PeerAllowed or OnRelay. Refused counts the
	// requests answered with an error after their credentials passed, and
	// AuthFailures those whose credentials did not (see EventAuthFailed).
	Dropped, Refused, AuthFailures uint64
}

// Stats returns a snapshot of what s holds and has done. It walks every
// allocation, so it is for a metrics scrape, not for every packet.
func (s *Server) Stats() Stats {
	st := Stats{
		ToPeers:      s.goneToPeer.load(),
		ToClients:    s.goneToClient.load(),
		Dropped:      s.dropped.Load(),
		Refused:      s.refused.Load(),
		AuthFailures: s.authFailed.Load(),
	}
	s.mu.Lock()
	st.Allocations, st.Pending = s.count, len(s.pending)
	s.mu.Unlock()
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for _, a := range sh.allocs {
			a.mu.Lock()
			st.Permissions += len(a.perms)
			st.Channels += len(a.chans)
			a.mu.Unlock()
			tp, tc := a.toPeer.load(), a.toClient.load()
			st.ToPeers.Packets += tp.Packets
			st.ToPeers.Bytes += tp.Bytes
			st.ToClients.Packets += tc.Packets
			st.ToClients.Bytes += tc.Bytes
		}
		sh.mu.RUnlock()
	}
	return st
}

// HoldsRelay reports whether addr is the relayed address of an allocation
// s holds. A PeerAllowed that lets clients of one server relay to each other
// allows the server's own address on such ports alone.
func (s *Server) HoldsRelay(addr netip.AddrPort) bool {
	return s.lookupRelay(unmap(addr)) != nil
}

func (s *Server) emit(e Event) {
	if s.cfg.OnEvent != nil {
		s.cfg.OnEvent(e)
	}
}

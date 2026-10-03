// Package peer tells a handler, or the caller of an RPC, which address the
// other side is at, like google.golang.org/grpc/peer.
package peer

import (
	"context"
	"net"
)

// Peer is the other side of an RPC.
type Peer struct {
	// Addr is the peer's address.
	Addr net.Addr
	// LocalAddr is this side's address.
	LocalAddr net.Addr
}

type peerKey struct{}

// NewContext returns a copy of ctx carrying p.
func NewContext(ctx context.Context, p *Peer) context.Context {
	return context.WithValue(ctx, peerKey{}, p)
}

// FromContext returns the Peer ctx carries.
func FromContext(ctx context.Context) (*Peer, bool) {
	p, ok := ctx.Value(peerKey{}).(*Peer)
	return p, ok
}

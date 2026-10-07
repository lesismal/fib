// Package peer tells a handler, or the caller of an RPC, which address the
// other side is at, like google.golang.org/grpc/peer.
package peer

import (
	"context"
	"net"

	"github.com/lesismal/fib/grpc/internal/ctxkeys"
)

// Peer is the other side of an RPC.
type Peer struct {
	// Addr is the peer's address.
	Addr net.Addr
	// LocalAddr is this side's address.
	LocalAddr net.Addr
}

// peerKey is shared with package grpc, which answers for it from a call's
// own context.
type peerKey = ctxkeys.Peer

// NewContext returns a copy of ctx carrying p.
func NewContext(ctx context.Context, p *Peer) context.Context {
	return context.WithValue(ctx, peerKey{}, p)
}

// FromContext returns the Peer ctx carries.
func FromContext(ctx context.Context) (*Peer, bool) {
	p, ok := ctx.Value(peerKey{}).(*Peer)
	return p, ok
}

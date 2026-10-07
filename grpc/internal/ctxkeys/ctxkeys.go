// Package ctxkeys holds the keys of the values package metadata and package
// peer put in a context, so that package grpc can answer for them from a
// call's own context without a context.WithValue for each.
package ctxkeys

// Incoming keys the metadata that came with an RPC.
type Incoming struct{}

// Peer keys the other side of an RPC.
type Peer struct{}

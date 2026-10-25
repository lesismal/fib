package fib

import "errors"

// ErrNotUDP is what a datagram-only option reports on a connection that is
// not UDP.
var ErrNotUDP = errors.New("fib: not a UDP connection")

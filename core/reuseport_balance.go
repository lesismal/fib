package core

import "errors"

// errReusePortBalanceUnsupported is what newReusePortBalancer returns where
// the eBPF reuseport balancer cannot be used: a platform without it, a
// kernel too old, or a process without CAP_BPF. The caller keeps plain
// SO_REUSEPORT, whose kernel hash spreads connections by their addresses.
var errReusePortBalanceUnsupported = errors.New("fib: reuseport balancing is not supported here")

// A reusePortBalancer attaches a round-robin selector to a group of sockets
// that share an address through SO_REUSEPORT, so that new connections are
// handed to the group's sockets in turn rather than by the kernel's hash of
// their addresses, which spreads a small number of connections unevenly. One
// selector covers a whole reuseport group, whether the group's sockets are
// the pollers of one engine or the listeners of several prefork children
// sharing a pinned selector; see newReusePortBalancer and the per-platform
// files.
//
// Its methods are no-ops where balancing is unsupported, so a caller need not
// branch on the platform.

// A bindingBalancer ties a reusePortBalancer to the binding whose listening
// group it balances, so that closing the binding closes it.
type bindingBalancer struct {
	binding int
	b       *reusePortBalancer
}

package core

import "github.com/lesismal/fib/internal/reuseportbpf"

// errReusePortBalanceUnsupported is what the selector's calls return where
// the eBPF reuseport balancer cannot be used: a platform without it, a
// kernel too old, or a process without CAP_BPF. The caller keeps plain
// SO_REUSEPORT, whose kernel hash spreads connections by their addresses.
var errReusePortBalanceUnsupported = reuseportbpf.ErrUnsupported

// A reusePortBalancer is the round-robin selector of a group of sockets that
// share an address through SO_REUSEPORT, so that new connections are handed
// to the group's sockets in turn rather than by the kernel's hash of their
// addresses, which spreads a small number of connections unevenly. One
// selector covers a whole reuseport group, whether the group's sockets are
// the pollers of one engine or the listeners of several prefork children,
// whose master makes the selector and hands it to each; see package
// reuseportbpf.
type reusePortBalancer = reuseportbpf.Group

// A bindingBalancer ties a reusePortBalancer to the binding whose listening
// group it balances, so that closing the binding closes it.
type bindingBalancer struct {
	binding int
	b       *reusePortBalancer
}

// Package reuseportbpf hands the connections a reuseport group of listening
// sockets receives to its sockets round-robin, with an eBPF selector, where
// the kernel's own default spreads them by a hash of their addresses, which
// leaves a group of a few sockets serving a few connections badly balanced.
//
// Within one process Balance makes a group's selector and attaches it. The
// children of package prefork share a group across processes: the master
// runs a Broker, which gives each child one end of a socket pair when it
// starts it and, the first time a child asks for an address's group, makes
// the group's maps and program, handing their descriptors to every child
// that asks over its socket (JoinGroup). Nothing is pinned in bpffs, so
// nothing needs it mounted, and nothing outlives the processes holding the
// descriptors.
//
// It needs CAP_BPF (CAP_SYS_ADMIN before Linux 5.8), Linux, and an
// architecture whose bpf(2) numbers it knows; elsewhere every call reports
// ErrUnsupported and the caller keeps the kernel's hash.
package reuseportbpf

import "errors"

// ErrUnsupported is what the calls return where the selector cannot be
// used: a platform without it, a kernel too old, or a process without
// CAP_BPF. The caller keeps plain SO_REUSEPORT.
var ErrUnsupported = errors.New("fib: reuseport balancing is not supported here")

// BrokerEnv names the variable a prefork child finds the descriptor of its
// end of the socket pair to the master's Broker in.
const BrokerEnv = "FIB_PREFORK_BALANCE_FD"

// Debug keeps EINVAL from bpf(2) from being taken for ErrUnsupported, so a
// test can see a real bug in the maps or the program rather than a skip. It
// is false in normal use.
var Debug bool

// Group holds the descriptors of one reuseport group's selector: the
// sockarray its sockets are registered in, the counter the program turns,
// and the program. A descriptor it does not hold is -1. The group keeps its
// own references once the program is attached, so Close only drops these.
type Group struct {
	Sockarray, Counter, Prog int
}

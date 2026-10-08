//go:build darwin

package sys

import "syscall"

const SoReusePort = syscall.SO_REUSEPORT

// ReusePortSpreads is false: Darwin lets sockets share an address through
// SO_REUSEPORT, but hands a TCP connection to one of them rather than
// spreading connections over all of them, so the pollers cannot each accept
// their own; see Config.ReusePort.
const ReusePortSpreads = false

// AcceptedInheritNoDelay is false: each accepted connection is given
// TCP_NODELAY itself.
const AcceptedInheritNoDelay = false

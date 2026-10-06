//go:build linux && (mips || mipsle || mips64 || mips64le)

package fib

// soReusePort is SO_REUSEPORT, which MIPS numbers apart from the other
// architectures.
const soReusePort = 0x200

// reusePortSpreads says sockets sharing an address through SO_REUSEPORT
// share its connections between them; see Config.ReusePort.
const reusePortSpreads = true

// acceptedInheritNoDelay says a connection accepted on a TCP listener takes
// TCP_NODELAY over from it, so setting it on the listener spares the
// setsockopt on every connection, one of the system calls the loop that
// accepts makes for each.
const acceptedInheritNoDelay = true

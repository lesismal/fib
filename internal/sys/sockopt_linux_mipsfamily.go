//go:build linux && (mips || mipsle || mips64 || mips64le)

package sys

// SoReusePort is SO_REUSEPORT, which MIPS numbers apart from the other
// architectures.
const SoReusePort = 0x200

// ReusePortSpreads says sockets sharing an address through SO_REUSEPORT
// share its connections between them; see Config.ReusePort.
const ReusePortSpreads = true

// AcceptedInheritNoDelay says a connection accepted on a TCP listener takes
// TCP_NODELAY over from it, so setting it on the listener spares the
// setsockopt on every connection, one of the system calls the loop that
// accepts makes for each.
const AcceptedInheritNoDelay = true

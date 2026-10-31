package fib

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"time"
)

// DialOptions describes a dial for DialWith: what Dial and DialWithHandler
// take, and the local end the socket is to leave from.
type DialOptions struct {
	// Network, Addr and Timeout are what Dial takes.
	Network string
	Addr    string
	Timeout time.Duration
	// Handler serves the connection, as for DialWithHandler. Nil means the
	// engine's.
	Handler Handler

	// LocalAddr is the address and port the socket is bound to before it
	// connects, as net.Dialer's LocalAddr. The zero value binds nothing and
	// leaves both to the kernel, as Dial does. An address without a port
	// leaves the port to the kernel, or to PortMin and PortMax; a port
	// without an address leaves the interface to the route, as the
	// unspecified address does for its own family. An address must be of
	// the remote's family, an IPv4-mapped one counting as IPv4: a name with
	// addresses of both is resolved to that family alone, and a literal of
	// the other fails the dial. It is not taken for "unix".
	//
	// The socket is bound as net.Dialer binds it, without SO_REUSEADDR, so a
	// port in use by any other socket, a TCP port still in TIME_WAIT among
	// them, fails the bind with EADDRINUSE. That holds for a socket on
	// another address of the family too, the wildcard against a specific
	// one: on Windows, which would let those share the port, the socket is
	// bound with SO_EXCLUSIVEADDRUSE to refuse it the same way.
	LocalAddr netip.AddrPort
	// PortMin and PortMax, when PortMax is not zero, are the inclusive range
	// the local port is taken from, LocalAddr's port being zero. Ports are
	// tried from a random one in the range upward, wrapping round, until a
	// bind succeeds; one in use moves on to the next, and a range with every
	// port in use fails the dial with an error that matches ErrNoLocalPort.
	PortMin, PortMax uint16
}

// ErrNoLocalPort is what a dial reports, wrapped in its *net.OpError, when
// every port in its DialOptions' range is in use.
var ErrNoLocalPort = errors.New("no free local port in the range")

// hasLocal reports whether o names a local end at all.
func (o *DialOptions) hasLocal() bool {
	return o.LocalAddr.Addr().IsValid() || o.LocalAddr.Port() != 0 || o.PortMax != 0 || o.PortMin != 0
}

// localIP is the address o binds, an IPv4-mapped one unmapped, or the
// invalid Addr when it names none.
func (o *DialOptions) localIP() netip.Addr {
	ip := o.LocalAddr.Addr()
	if !ip.IsValid() {
		return ip
	}
	return ip.Unmap()
}

// check reports what in o cannot start a dial on network, which is o's own
// with "" read as "tcp".
func (o *DialOptions) check(network string) error {
	if !o.hasLocal() {
		return nil
	}
	if network == "unix" {
		return errors.New("a unix dial takes no local address")
	}
	if o.PortMax != 0 || o.PortMin != 0 {
		if o.PortMin == 0 || o.PortMin > o.PortMax {
			return fmt.Errorf("local port range %d-%d is empty or starts at 0", o.PortMin, o.PortMax)
		}
		if o.LocalAddr.Port() != 0 {
			return fmt.Errorf("local port %d and range %d-%d are both set", o.LocalAddr.Port(), o.PortMin, o.PortMax)
		}
	}
	ip := o.localIP()
	if !ip.IsValid() {
		return nil
	}
	switch network {
	case "tcp4", "udp4":
		if !ip.Is4() {
			return fmt.Errorf("local address %s is not IPv4", ip)
		}
	case "tcp6", "udp6":
		if !ip.Is6() {
			return fmt.Errorf("local address %s is not IPv6", ip)
		}
	}
	return nil
}

// resolveNetwork is the network to resolve o's Addr on: a name that has
// addresses of both families is narrowed to the local address's.
func (o *DialOptions) resolveNetwork(network string) string {
	ip := o.localIP()
	if !ip.IsValid() {
		return network
	}
	suffix := "4"
	if ip.Is6() {
		suffix = "6"
	}
	switch network {
	case "tcp", "udp":
		return network + suffix
	}
	return network
}

// familyMismatch reports a remote of the other family than o's local
// address, or nil.
func (o *DialOptions) familyMismatch(remote net.IP) error {
	ip := o.localIP()
	if !ip.IsValid() || remote == nil {
		return nil
	}
	if remote4 := remote.To4() != nil; remote4 != ip.Is4() {
		return fmt.Errorf("local address %s and remote address %s are of different families", ip, remote)
	}
	return nil
}

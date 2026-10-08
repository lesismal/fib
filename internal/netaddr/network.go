// Package netaddr is documented in netaddr.go; this file holds what every
// platform has.
package netaddr

// IsUnix reports whether network names a Unix domain stream socket.
func IsUnix(network string) bool { return network == "unix" }

// IsUDP reports whether network names UDP.
func IsUDP(network string) bool {
	switch network {
	case "udp", "udp4", "udp6":
		return true
	}
	return false
}

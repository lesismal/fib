package fib

// Protocol is the transport a connection runs over.
type Protocol uint8

const (
	// ProtocolTCP is a TCP connection.
	ProtocolTCP Protocol = iota + 1
	// ProtocolUDP is a UDP connection: a dialed UDP socket, or a peer of a
	// UDP listener.
	ProtocolUDP
	// ProtocolUnix is a Unix stream socket.
	ProtocolUnix
)

// String returns the protocol's network name, as the net package spells it:
// "tcp", "udp" or "unix".
func (p Protocol) String() string {
	switch p {
	case ProtocolTCP:
		return "tcp"
	case ProtocolUDP:
		return "udp"
	case ProtocolUnix:
		return "unix"
	}
	return "unknown"
}

// IsTCP reports whether the connection is a TCP connection.
func (c *Connection) IsTCP() bool { return c.Protocol() == ProtocolTCP }

// IsUDP reports whether the connection exchanges datagrams.
func (c *Connection) IsUDP() bool { return c.Protocol() == ProtocolUDP }

// IsUnix reports whether the connection is a Unix socket.
func (c *Connection) IsUnix() bool { return c.Protocol() == ProtocolUnix }

// IsAccepted reports whether one of the engine's listeners accepted the
// connection: a TCP or Unix connection it accepted, or a peer that sent a
// UDP listener its first datagram. It is the opposite of IsDialed.
func (c *Connection) IsAccepted() bool { return !c.dialed }

// IsDialed reports whether Dial or DialWithHandler opened the connection. It
// is the opposite of IsAccepted.
func (c *Connection) IsDialed() bool { return c.dialed }

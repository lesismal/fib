package fib

// Handler callbacks run on a logical worker, except OnOpen, which runs on the
// event-loop goroutine. OnClose runs in the connection's last round, where its
// OnData ran, after every OnData before it; that is the event loop where the
// engine runs rounds there. Data is only valid for the duration of OnData.
type Handler interface {
	OnOpen(*Connection)
	OnData(*Connection, []byte)
	OnPriorityData(*Connection, []byte)
	OnClose(*Connection, error)
}

// DatagramsHandler is a Handler that takes a UDP connection's datagrams in
// bursts. When a Handler implements it, the datagrams that have arrived for
// a connection by the time its worker runs go to OnDatagrams together, in
// the order they arrived, instead of to OnData one at a time. A protocol
// that answers what it receives - with acknowledgements, say - can then
// answer the whole burst at once, in fewer datagrams than it would one
// arrival at a time. The portable backend, on platforms other than Linux,
// macOS and Windows, keeps calling OnData.
//
// Each datagram belongs to the handler, as a UDP connection's OnData data
// does; the slice holding them does not, and must not be kept once
// OnDatagrams returns. A datagram is a buffer from package bufferpool, so a
// handler that is done with one, with nothing left referring to it, may give
// it back with bufferpool.Put for the next one to be read into; one it keeps
// or drops is collected like any other slice.
type DatagramsHandler interface {
	Handler
	OnDatagrams(c *Connection, datagrams [][]byte)
}

// HandlerFuncs allows callers to implement only the callbacks they need.
type HandlerFuncs struct {
	Open         func(*Connection)
	Data         func(*Connection, []byte)
	PriorityData func(*Connection, []byte)
	Close        func(*Connection, error)
}

func (h HandlerFuncs) OnOpen(c *Connection) {
	if h.Open != nil {
		h.Open(c)
	}
}

func (h HandlerFuncs) OnData(c *Connection, b []byte) {
	if h.Data != nil {
		h.Data(c, b)
	}
}

func (h HandlerFuncs) OnPriorityData(c *Connection, b []byte) {
	if h.PriorityData != nil {
		h.PriorityData(c, b)
	}
}

func (h HandlerFuncs) OnClose(c *Connection, err error) {
	if h.Close != nil {
		h.Close(c, err)
	}
}

// Layer carries a connection's sends, transforming them on the way to the
// socket. The tls package installs one that encrypts; any other protocol that
// frames or encrypts what a connection sends, such as a DTLS implementation
// over UDP, installs its own the same way.
//
// While a layer is installed, Send, SendOwned and SendParts hand their bytes
// to its Send, and CloseAfterSend calls its CloseAfterSend. The layer reaches
// the socket itself through SendRaw and CloseAfterSendRaw. Incoming bytes are
// not the layer's concern: the handler that installed it sees them in OnData
// and decides what the handler it wraps receives.
type Layer interface {
	// Send transforms first followed by second and sends the result. Neither
	// slice may be retained after it returns.
	Send(first, second []byte) error
	// CloseAfterSend ends the connection once what was already sent has gone
	// out, after whatever closing message the layer's protocol calls for.
	CloseAfterSend()
}

// CloseWithError closes the connection, as Close does, and hands err to
// OnClose. A layer uses it to report why its protocol ended the connection,
// such as a failed handshake. Only the first close's error is reported.
func (c *Connection) CloseWithError(err error) { c.closeWithError(err) }

// Handler returns the handler the engine serves accepted connections with,
// which is also what a dial that names no handler of its own gets.
func (e *Engine) Handler() Handler { return e.handler }

// SetLayer installs l, or removes the layer with nil. Install it in OnOpen,
// before anything can send, so that no send bypasses it.
func (c *Connection) SetLayer(l Layer) { c.layer = l }

// Layer returns the installed layer, or nil.
func (c *Connection) Layer() Layer { return c.layer }

// SendRaw sends data to the socket, bypassing any layer. It copies data, as
// Send does. On a UDP connection each call is one datagram.
func (c *Connection) SendRaw(data []byte) error { return c.sendRaw(data) }

// SendRawPooled is SendRaw for data, a buffer from package bufferpool, which
// the connection takes over whatever happens: what the socket takes at once
// is not copied, and what it does not is kept rather than copied into the
// queue, and data goes back to the pool once it has been written. A layer
// that seals its output into a buffer from the pool hands it over this way,
// saving the copy SendRaw would make of all it sends while the connection is
// corked.
func (c *Connection) SendRawPooled(data []byte) error { return c.sendRawPooled(data) }

// CloseAfterSendRaw is CloseAfterSend bypassing any layer.
func (c *Connection) CloseAfterSendRaw() { c.closeAfterSendRaw() }

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

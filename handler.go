package fib

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/lesismal/fib/bufferpool"
)

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

// ErrWouldBlock is what Read returns when the socket has nothing waiting. The
// connection is event-driven, so a read that finds the socket empty reports
// this rather than waiting for the peer.
var ErrWouldBlock = errors.New("fib: operation would block")

// errNoSocket is what File and SyscallConn report for a UDP listener's peer,
// which shares its listener's socket with every other peer.
var errNoSocket = errors.New("fib: a UDP listener's peer has no socket of its own")

// Connection is a net.Conn, so it can be handed to code that asks for one to
// look at its addresses, write to it, set its deadlines or close it. Two of
// the methods do not behave as a blocking socket's would, and code that reads
// from a net.Conn expecting it to wait must not be given one of these:
//
//   - Read does not block. It reads whatever the socket has and reports
//     ErrWouldBlock when that is nothing. Incoming bytes reach a handler
//     through OnData; Read is for a handler that wants to take the rest of a
//     message off the socket itself, and it is not available at all on the
//     portable backend, whose own reader goroutine owns the socket.
//   - The deadlines do not make a call fail; they close the connection. See
//     SetDeadline.
//
// Write, Close, LocalAddr and RemoteAddr behave as they do on any net.Conn.
var _ net.Conn = (*Connection)(nil)

// Connection also has net.TCPConn's own methods, so code that sets a TCP
// option through an interface, or half-closes a connection, works on one.
// Each does nothing, and reports no error, on a connection it has no meaning
// for: TCP's own options on a Unix socket or a UDP connection, and anything
// touching the socket on a UDP listener's peer.
// ReadFrom and WriteTo are left out: Write already queues without waiting,
// and WriteTo would have to wait on reads, which reach a handler through
// OnData.
var _ interface {
	net.Conn
	CloseRead() error
	CloseWrite() error
	File() (*os.File, error)
	MultipathTCP() (bool, error)
	SetKeepAlive(bool) error
	SetKeepAliveConfig(net.KeepAliveConfig) error
	SetKeepAlivePeriod(time.Duration) error
	SetLinger(int) error
	SetNoDelay(bool) error
	SetReadBuffer(int) error
	SetWriteBuffer(int) error
	SyscallConn() (syscall.RawConn, error)
} = (*Connection)(nil)

// File is what SendFile sends from: an *os.File, or any type that wraps one
// and passes its descriptor on through SyscallConn.
type File interface {
	io.ReaderAt
	SyscallConn() (syscall.RawConn, error)
}

// ErrSendFileDatagram is what SendFile returns on a UDP connection, whose
// sends are datagrams rather than a stream a file could be copied into.
var ErrSendFileDatagram = errors.New("fib: SendFile needs a stream connection")

// sendFileChunk is how much of a file one read stages when the file cannot go
// to the socket directly: through a layer, or on a platform without sendfile.
const sendFileChunk = 64 << 10

func checkSendFileRange(offset, count int64) error {
	if offset < 0 || count < 0 {
		return fmt.Errorf("fib: invalid SendFile range %d+%d", offset, count)
	}
	return nil
}

// sendFileCopy sends count bytes of f from offset by reading them and handing
// each chunk to send, which must copy it. It is the fallback for connections
// whose bytes are transformed on the way out, as TLS does, and for the
// portable backend. A file that fails part way, such as one shorter than the
// range, leaves the peer short of bytes it was promised, so the connection is
// closed; one that fails before anything was sent leaves it as it was.
func sendFileCopy(c *Connection, f File, offset, count int64, send func([]byte) error) error {
	// send copies, so the staging buffer is the pool's again as soon as the
	// range has gone out.
	buf := bufferpool.Get(int(min(count, sendFileChunk)))
	defer bufferpool.Put(buf)
	sent := false
	for count > 0 {
		n, err := f.ReadAt(buf[:min(count, int64(len(buf)))], offset)
		if n > 0 {
			if sendErr := send(buf[:n]); sendErr != nil {
				return sendErr
			}
			sent = true
			offset += int64(n)
			count -= int64(n)
		}
		if count == 0 {
			return nil
		}
		if err == nil {
			continue
		}
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		if sent {
			c.closeWithError(err)
		}
		return err
	}
	return nil
}

// preforkChildEnv names the variable package prefork starts its children
// with; see preforkChild.
const preforkChildEnv = "FIB_PREFORK_CHILD"

// preforkChild reports whether this process is one of the children package
// prefork starts, which all serve the same addresses: every engine it binds
// listens with SO_REUSEPORT, as Config.ReusePort asks, so that the kernel
// spreads the connections, and the datagrams, over the children rather than
// refusing the address to all of them but the first.
var preforkChild = os.Getenv(preforkChildEnv) != ""

// Name reports the engine's name: Config.Name, or DefaultName when that was
// empty.
func (e *Engine) Name() string { return e.name }

// Engine reports the engine the connection belongs to: the one that
// accepted or dialed it, even when one of its pollers serves it.
func (c *Connection) Engine() *Engine { return c.engine.root() }

// logRun logs that the engine has started serving, under its name, with the
// addresses it listens on and the task pool its connections run on, when
// Config.LogStatus asks for it.
func (e *Engine) logRun() {
	if !e.logStatus {
		return
	}
	listening := "none"
	if addrs, err := e.ListenAddrs(); err != nil {
		listening = err.Error()
	} else if len(addrs) > 0 {
		parts := make([]string, len(addrs))
		for i, addr := range addrs {
			parts[i] = addr.Network() + "://" + addr.String()
		}
		listening = strings.Join(parts, ",")
	}
	pool := fmt.Sprintf("%T", e.taskPool)
	if named, ok := e.taskPool.(interface{ Name() string }); ok {
		pool = named.Name()
	}
	slog.Info("fib: engine started", "engine", e.name, "listen", listening, "taskPool", pool,
		"pollers", len(e.pollers))
}

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
// macOS, the BSDs and Windows, keeps calling OnData.
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

// HangupHandler is a Handler that is told the peer has gone while one of the
// connection's rounds is still running. A round reads the end of input, or
// the socket's error, only once its handler returns from OnData, and then
// closes the connection; a handler that waits in OnData, on a database or on
// another server, would hear nothing until it was done. OnHangup lets the
// protocol tell whatever that handler is serving that its peer has gone, so
// the work can stop early. The connection is still open when OnHangup runs.
// It closes, and OnClose runs, when the round reads the end of input as
// usual.
//
// OnHangup runs on the event loop, at most once per connection, when the
// loop sees the peer end its side of the connection, or the socket fail,
// while a round of the connection is running or waiting to run. It must
// not block, nor take a lock that a handler in OnData may hold; work that
// would do either goes to a goroutine or a pool. A hangup that arrives
// while no round is running needs no early word: the round it starts
// reads it and closes the connection.
//
// The epoll and kqueue backends call it. Windows arms a connection's read
// only between its rounds, and the portable backend reports the end of input
// as a close, so there the hangup is heard when the round ends, as it would
// be without OnHangup.
type HangupHandler interface {
	Handler
	OnHangup(*Connection)
}

// ShutdownHandler is a Handler told that its engine is shutting down; see
// Engine.Shutdown. The engine cannot tell whether a connection is in the
// middle of an exchange or idle between two, and Shutdown waits for every
// connection it accepted to close. OnShutdown is the protocol's cue to end
// the connection as soon as it can without cutting an exchange short: to
// close it now if it is idle, to close it once the exchange in flight is
// done, or to tell the peer it is going away, as HTTP/2's GOAWAY and a
// WebSocket close frame do. Engine.ShuttingDown says the same thing to code
// that asks, such as a protocol about to keep a connection open for its next
// exchange.
//
// OnShutdown runs when Shutdown begins and then again, at a falling rate,
// for as long as the connection stays open, so that a connection busy the
// first time is found idle a later one; it must be safe to call more than
// once. It is not called for a connection the engine dialed, nor for one
// already closing. It runs on the connection's event loop, like OnHangup,
// and may run while one of the connection's rounds is running OnData: it
// must not block, nor take a lock that a handler in OnData may hold. The
// portable backend calls it on the goroutine that called Shutdown, under
// the same rules.
type ShutdownHandler interface {
	Handler
	OnShutdown(*Connection)
}

// EngineCloseHandler is a Handler told that Engine.Close is about to end one
// of its connections, while it can still send: a protocol may have a last
// frame to go, HTTP/2's GOAWAY, and a datagram protocol, whose peer sees no
// end of a stream at all, has to say so itself, as QUIC does with
// CONNECTION_CLOSE. Without it the peer finds out once the connection has
// ended, or has given up waiting. OnEngineClose runs on the goroutine that
// called Close, once for each open connection whose handler takes it, its
// listeners' UDP peers and those the engine dialed among them, before
// OnClose. A round of the connection may be running OnData at the same time,
// so it must not block, nor take a lock that a handler in OnData may hold for
// long. What it sends goes only while it runs: Close cuts the connection once
// it returns, so it must write any last frame itself, without waiting.
// Engine.Stop leaves connections open, and calls it for none until Close; the
// portable backend, whose Stop closes the sockets, calls it for none.
type EngineCloseHandler interface {
	Handler
	OnEngineClose(*Connection)
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

// CorkHolder is a Layer that holds what is sent while the connection is
// corked, rather than transforming each send as it is made, and sends it all
// once the cork comes off: the tls package seals a round's replies into one
// record, where a record each would cost a seal, its overhead and an item of
// the write. The layer bounds what it holds, sending it at once past its
// bound, and asks Corked as it takes each send, under the lock that orders
// its sends.
//
// The connection calls ReleaseCorked, without its own lock, each time the
// cork comes off, after it has and before it writes what is queued; the layer
// then sends what it holds through SendRaw or SendRawPooled. A send made in
// between finds the connection uncorked, and the layer sends it behind what
// it held.
type CorkHolder interface {
	Layer
	ReleaseCorked()
}

// FileLayer is a Layer that carries a file SendFile sends as the socket takes
// it, rather than having the whole range read and handed to Send before
// SendFile returns: the tls package seals a file's records as the socket
// drains, or has the kernel seal them, where it can, and the file go by
// sendfile.
//
// SendFile calls CanSendFile first, and SendFile only if it reports true;
// otherwise the range is read and handed to Send. Context.SendFile in package
// http asks through Connection.QueuesFiles, before it begins a response.
type FileLayer interface {
	Layer
	// CanSendFile reports whether SendFile can carry a file now. Once it
	// has reported true, it reports false only for a connection that has
	// failed or is closing, where SendFile fails anyway.
	CanSendFile() bool
	// SendFile sends count bytes of f from offset, after whatever was sent
	// through the layer before it and ahead of whatever is sent after, and
	// hands the range to the connection through SendFileRaw. count is above
	// zero.
	SendFile(f File, offset, count int64) error
}

// FileSealer transforms a file's bytes as SendFileRaw reads them for the
// socket: a chunk at a time, each read once the socket has taken the one
// before, so a file costs a chunk's memory however large it is.
//
// The connection calls Seal from its flush, under its own lock, in the
// file's order, and never twice at once: Seal must not send on the
// connection, nor wait.
type FileSealer interface {
	// Seal appends to dst what plain, the next bytes of the file, become on
	// the wire, and returns the extended slice.
	Seal(dst, plain []byte) ([]byte, error)
	// SealedLen bounds what Seal makes of n bytes, which the connection
	// sizes the buffer it seals into by.
	SealedLen(n int) int
}

// CloseWithError closes the connection, as Close does, and hands err to
// OnClose. A layer uses it to report why its protocol ended the connection,
// such as a failed handshake. Only the first close's error is reported.
func (c *Connection) CloseWithError(err error) { c.closeWithError(err) }

// Handler returns the handler a dial that names no handler of its own gets,
// which does nothing: every Binding serves its connections with its own, and
// a dial names its own through DialWithHandler.
func (e *Engine) Handler() Handler { return e.handler }

// SetHandler hands the connection to h: every callback from here on goes to h
// instead of the handler that has served it so far, OnData, OnPriorityData
// and OnClose, and the ones the engine finds by asking what the handler
// implements, OnHangup, OnShutdown and OnDatagrams. It is for a handler that
// serves a connection only until it knows what the connection is, as package
// mux does with the protocol a peer speaks first, and then steps aside, so
// that the connection costs nothing more for having been looked at. A nil h
// is a handler that does nothing.
//
// h's OnOpen is not called: the caller calls it, if h expects one, and then
// hands h whatever it read while it decided. Call SetHandler only from the
// connection's own OnData or OnPriorityData. The rounds that run those follow
// one another, and the engine reads the handler in them without a lock; only
// the event loop takes one to read it, which SetHandler takes to write it.
func (c *Connection) SetHandler(h Handler) {
	if h == nil {
		h = HandlerFuncs{}
	}
	c.mu.Lock()
	c.handler = h
	c.mu.Unlock()
}

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
// It is a whole number of TLS records' plaintext, so that a file the tls
// package seals is cut into full records but for the last.
const sendFileChunk = 64 << 10

func checkSendFileRange(offset, count int64) error {
	if offset < 0 || count < 0 {
		return fmt.Errorf("fib: invalid SendFile range %d+%d", offset, count)
	}
	return nil
}

// sendFileCopy sends count bytes of f from offset by reading them and handing
// each chunk to send, which must copy it. It is the fallback for connections
// with a layer that cannot carry a file, and for the portable backend. A file that fails part way, such as one shorter than the
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
// with; see preforkChild. preforkChildrenEnv carries how many children there
// are, and preforkBPFFSEnv the bpffs directory the children pin a shared
// reuseport selector under when Config.BalanceReusePort asks; see
// balanceGroups.
const (
	preforkChildEnv    = "FIB_PREFORK_CHILD"
	preforkChildrenEnv = "FIB_PREFORK_CHILDREN"
	preforkBPFFSEnv    = "FIB_PREFORK_BPFFS"
)

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
	accept, applies := e.acceptPlacements()
	if e.acceptPlacement != AcceptFDModulo && !applies {
		// Said whatever LogStatus says: the setting asked for does nothing.
		slog.Warn("fib: Config.AcceptPlacement applies to no listener: it needs IOPollers without EngineLoop and a TCP address on Linux, where each poller listens on a socket of its own with SO_REUSEPORT",
			"engine", e.name, "acceptPlacement", e.acceptPlacement.String())
	}
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
		"pollers", len(e.pollers), "accept", accept)
}

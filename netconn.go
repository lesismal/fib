package fib

import (
	"errors"
	"net"
	"os"
	"syscall"
	"time"
)

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

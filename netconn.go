package fib

import (
	"errors"
	"net"
)

// ErrWouldBlock is what Read returns when the socket has nothing waiting. The
// connection is event-driven, so a read that finds the socket empty reports
// this rather than waiting for the peer.
var ErrWouldBlock = errors.New("fib: operation would block")

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

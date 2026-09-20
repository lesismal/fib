//go:build !linux && !darwin && !windows

package fib

import (
	"errors"
	"net"
	"time"
)

// errNoRead is what Read reports on this backend. The native backends let a
// handler take bytes off the socket itself; here the connection's own reader
// goroutine is blocked on that socket, and a read from anywhere else would
// take bytes from under it.
var errNoRead = errors.New("fib: this backend is read through OnData")

// Read is not available on this backend: incoming bytes reach the handler
// through OnData. See the native backends' Read.
func (c *Connection) Read([]byte) (int, error) { return 0, errNoRead }

// Write hands b to Send and reports it all written, since Send copies what it
// is given. It therefore does not wait for the peer.
func (c *Connection) Write(b []byte) (int, error) {
	if err := c.Send(b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// LocalAddr returns the address this side of the connection is bound to.
func (c *Connection) LocalAddr() net.Addr { return c.conn.LocalAddr() }

// SetDeadline sets both the read and the write deadline. This backend reads
// and writes through a net.Conn of its own, so they are that connection's
// deadlines: one that passes fails the read or write it belongs to, which
// closes the connection and hands OnClose the timeout error. A zero time
// removes a deadline.
func (c *Connection) SetDeadline(t time.Time) error { return c.conn.SetDeadline(t) }

// SetReadDeadline closes the connection at t if the peer has not sent
// anything more by then. See SetDeadline.
func (c *Connection) SetReadDeadline(t time.Time) error { return c.conn.SetReadDeadline(t) }

// SetWriteDeadline fails a send that has not reached the socket by t, which
// closes the connection. See SetDeadline.
func (c *Connection) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }

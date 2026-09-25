//go:build linux || darwin || windows

package fib

import (
	"io"
	"net"
	"syscall"
)

// Read takes what the socket has without waiting for the peer, and reports
// ErrWouldBlock when it has nothing. It is for a handler that wants to read
// the rest of a message itself, inside OnData, rather than wait for the next
// callback; incoming bytes otherwise reach the handler through OnData, and a
// Read racing the read loop takes bytes the loop would have delivered.
//
// It returns io.EOF once the peer has closed its side, and the reason the
// connection closed — os.ErrDeadlineExceeded for a deadline, net.ErrClosed
// otherwise — once it has. A UDP connection has no Read: its datagrams are
// read by the event loop and delivered whole to OnData.
func (c *Connection) Read(b []byte) (int, error) {
	if c.udp != nil {
		return 0, errUDPRead
	}
	if err := c.closedError(); err != nil {
		return 0, err
	}
	if len(b) == 0 {
		return 0, nil
	}
	for {
		n, err := c.sysRead(b)
		switch {
		case n > 0:
			return n, nil
		case err == nil:
			return 0, io.EOF
		case err == syscall.EINTR:
			continue
		case isWouldBlock(err):
			return 0, ErrWouldBlock
		case err == syscall.EBADF:
			// The connection closed between the check above and the read.
			return 0, c.closedErrorOr(net.ErrClosed)
		}
		return 0, err
	}
}

// Write hands b to Send and reports it all written, since Send copies what it
// is given and queues whatever the socket cannot take. It therefore does not
// wait for the peer; SetWriteDeadline bounds how long the queue may stand.
func (c *Connection) Write(b []byte) (int, error) {
	if err := c.Send(b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// LocalAddr returns the address this side of the connection is bound to: a
// *net.UDPAddr for a UDP connection, a *net.UnixAddr for a Unix socket and a
// *net.TCPAddr otherwise. It returns nil once the socket is gone.
func (c *Connection) LocalAddr() net.Addr {
	if c.udp != nil {
		if l := c.udp.listener; l != nil {
			sa, err := l.sockname()
			if err != nil {
				return nil
			}
			return sockaddrToUDPAddr(sa)
		}
	}
	sa, err := c.sockname()
	if err != nil {
		return nil
	}
	if c.udp != nil {
		return sockaddrToUDPAddr(sa)
	}
	return sockaddrToAddr(sa)
}

// closedError is why the connection is no longer usable, or nil while it is.
func (c *Connection) closedError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closing && !c.closed {
		return nil
	}
	if c.closeReason != nil {
		return c.closeReason
	}
	return net.ErrClosed
}

// closedErrorOr is closedError with a fallback for a connection that closed
// under a caller without having recorded why yet.
func (c *Connection) closedErrorOr(fallback error) error {
	if err := c.closedError(); err != nil {
		return err
	}
	return fallback
}

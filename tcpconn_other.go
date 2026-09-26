//go:build !linux && !darwin && !windows

package fib

import (
	"net"
	"os"
	"syscall"
	"time"
)

// The methods below are net.TCPConn's, beyond those net.Conn already asks
// for. This backend hands each to the net.Conn it wraps, and one that the
// wrapped connection does not have, such as a TCP option on a Unix or UDP
// connection, does nothing and reports no error.

// SetNoDelay turns Nagle's algorithm off, when noDelay is true, or back on.
// The net package starts every TCP connection with it off.
func (c *Connection) SetNoDelay(noDelay bool) error {
	if conn, ok := c.conn.(interface{ SetNoDelay(bool) error }); ok {
		return conn.SetNoDelay(noDelay)
	}
	return nil
}

// SetKeepAlive turns keep-alive probes on or off.
func (c *Connection) SetKeepAlive(keepalive bool) error {
	if conn, ok := c.conn.(interface{ SetKeepAlive(bool) error }); ok {
		return conn.SetKeepAlive(keepalive)
	}
	return nil
}

// SetKeepAlivePeriod is net.TCPConn.SetKeepAlivePeriod.
func (c *Connection) SetKeepAlivePeriod(d time.Duration) error {
	if conn, ok := c.conn.(interface{ SetKeepAlivePeriod(time.Duration) error }); ok {
		return conn.SetKeepAlivePeriod(d)
	}
	return nil
}

// SetKeepAliveConfig is net.TCPConn.SetKeepAliveConfig.
func (c *Connection) SetKeepAliveConfig(config net.KeepAliveConfig) error {
	if conn, ok := c.conn.(interface {
		SetKeepAliveConfig(net.KeepAliveConfig) error
	}); ok {
		return conn.SetKeepAliveConfig(config)
	}
	return nil
}

// SetLinger is net.TCPConn.SetLinger.
func (c *Connection) SetLinger(sec int) error {
	if conn, ok := c.conn.(interface{ SetLinger(int) error }); ok {
		return conn.SetLinger(sec)
	}
	return nil
}

// SetReadBuffer sets the size of the socket's receive buffer.
func (c *Connection) SetReadBuffer(bytes int) error {
	if conn, ok := c.conn.(interface{ SetReadBuffer(int) error }); ok {
		return conn.SetReadBuffer(bytes)
	}
	return nil
}

// SetWriteBuffer sets the size of the socket's send buffer.
func (c *Connection) SetWriteBuffer(bytes int) error {
	if conn, ok := c.conn.(interface{ SetWriteBuffer(int) error }); ok {
		return conn.SetWriteBuffer(bytes)
	}
	return nil
}

// CloseRead shuts down the reading side of the connection. OnData is not
// called again, and the end of input that follows does not close the
// connection, which goes on sending until it is closed. On a connection with
// no reading side to shut, such as a UDP one, it does nothing.
func (c *Connection) CloseRead() error {
	conn, ok := c.conn.(interface{ CloseRead() error })
	if !ok || c.udp {
		return nil
	}
	c.mu.Lock()
	closing, shut := c.closing, c.readShut
	c.readShut = true
	c.mu.Unlock()
	if closing {
		return net.ErrClosed
	}
	if shut {
		return nil
	}
	return conn.CloseRead()
}

// readEnded reports whether CloseRead has ended input.
func (c *Connection) readEnded() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readShut
}

// CloseWrite shuts down the writing side of the connection, after which sends
// are refused. Sends on this backend have reached the socket by the time they
// return, so the peer reads everything sent before it. On a connection with
// no writing side to shut, such as a UDP one, it does nothing.
func (c *Connection) CloseWrite() error {
	conn, ok := c.conn.(interface{ CloseWrite() error })
	if !ok || c.udp {
		return nil
	}
	c.mu.Lock()
	closing, shut := c.closing, c.writeShut
	c.writeShut = true
	c.mu.Unlock()
	if closing {
		return net.ErrClosed
	}
	if shut {
		return nil
	}
	// A send still writing finishes first.
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return conn.CloseWrite()
}

// MultipathTCP is net.TCPConn.MultipathTCP, and false for anything else.
func (c *Connection) MultipathTCP() (bool, error) {
	if conn, ok := c.conn.(interface{ MultipathTCP() (bool, error) }); ok {
		return conn.MultipathTCP()
	}
	return false, nil
}

// SyscallConn returns the wrapped connection's socket for raw access. A
// connection without one, a UDP listener's peer among them, returns an error.
func (c *Connection) SyscallConn() (syscall.RawConn, error) {
	if conn, ok := c.conn.(syscall.Conn); ok {
		return conn.SyscallConn()
	}
	return nil, errNoSocket
}

// File returns a duplicate of the wrapped connection's socket. A connection
// without one, a UDP listener's peer among them, returns an error.
func (c *Connection) File() (*os.File, error) {
	if conn, ok := c.conn.(interface{ File() (*os.File, error) }); ok {
		return conn.File()
	}
	return nil, errNoSocket
}

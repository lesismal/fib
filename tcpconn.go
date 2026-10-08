//go:build linux || darwin || windows

package fib

import (
	"net"
	"os"
	"syscall"
	"time"
)

// The methods below are net.TCPConn's, beyond those net.Conn already asks
// for. Each one that has no meaning for the connection's kind does nothing
// and reports no error: TCP's own options on a Unix socket or a UDP
// connection, and anything touching the socket on a UDP listener's peer,
// which shares its listener's socket with every other peer.

// SetNoDelay turns Nagle's algorithm off, when noDelay is true, or back on.
// Every TCP connection fib accepts or dials starts with it off, so that a
// small reply is not held back until the peer acknowledges the one before.
func (c *Connection) SetNoDelay(noDelay bool) error {
	return c.tcpControl(func(s rawSocket) error {
		return syscall.SetsockoptInt(s, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, boolInt(noDelay))
	})
}

// SetKeepAlive turns keep-alive probes on or off.
func (c *Connection) SetKeepAlive(keepalive bool) error {
	return c.tcpControl(func(s rawSocket) error {
		return syscall.SetsockoptInt(s, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, boolInt(keepalive))
	})
}

// SetKeepAlivePeriod sets how long the connection stays idle before the
// first keep-alive probe, rounded up to whole seconds, as
// net.TCPConn.SetKeepAlivePeriod does: zero means the net package's default
// of 15 seconds and a negative period leaves the setting alone.
func (c *Connection) SetKeepAlivePeriod(d time.Duration) error {
	return c.tcpControl(func(s rawSocket) error { return setKeepAliveTimes(s, d, -1) })
}

// SetKeepAliveConfig applies config as net.TCPConn.SetKeepAliveConfig does:
// Idle, Interval and Count take the net package's defaults when zero and are
// left alone when negative. A platform that cannot set Count leaves it alone.
func (c *Connection) SetKeepAliveConfig(config net.KeepAliveConfig) error {
	return c.tcpControl(func(s rawSocket) error {
		err := syscall.SetsockoptInt(s, syscall.SOL_SOCKET, syscall.SO_KEEPALIVE, boolInt(config.Enable))
		if err == nil {
			err = setKeepAliveTimes(s, config.Idle, config.Interval)
		}
		if err == nil {
			err = setKeepAliveCount(s, config.Count)
		}
		return err
	})
}

// SetLinger sets what closing does with output the kernel has yet to deliver,
// as net.TCPConn.SetLinger does: a negative sec sends it in the background,
// zero discards it and resets the connection, and a positive sec lets the
// close wait up to that many seconds for it.
func (c *Connection) SetLinger(sec int) error {
	return c.tcpControl(func(s rawSocket) error {
		l := syscall.Linger{Onoff: 1, Linger: int32(sec)}
		if sec < 0 {
			l = syscall.Linger{}
		}
		return syscall.SetsockoptLinger(s, syscall.SOL_SOCKET, syscall.SO_LINGER, &l)
	})
}

// SetReadBuffer sets the size of the socket's receive buffer. It applies to
// any socket of the connection's own, a Unix or UDP one included.
func (c *Connection) SetReadBuffer(bytes int) error {
	return c.control("setsockopt", func(s rawSocket) error {
		return syscall.SetsockoptInt(s, syscall.SOL_SOCKET, syscall.SO_RCVBUF, bytes)
	})
}

// SetWriteBuffer sets the size of the socket's send buffer. It applies to any
// socket of the connection's own, a Unix or UDP one included.
func (c *Connection) SetWriteBuffer(bytes int) error {
	return c.control("setsockopt", func(s rawSocket) error {
		return syscall.SetsockoptInt(s, syscall.SOL_SOCKET, syscall.SO_SNDBUF, bytes)
	})
}

// CloseRead shuts down the reading side of the connection. OnData is not
// called again, and the end of input that follows does not close the
// connection, which goes on sending until it is closed. Only a stream,
// TCP or Unix, has a side to shut; on a UDP connection it does nothing.
func (c *Connection) CloseRead() error {
	if c.udp != nil {
		return nil
	}
	return c.control("shutdown", func(s rawSocket) error {
		if c.readShut.Swap(true) {
			return nil
		}
		return c.shutdownReadLocked()
	})
}

// CloseWrite shuts down the writing side of the connection once everything
// Send has already accepted has reached the socket, so the peer reads all of
// it and then the end of the stream. Sends are refused from the call on. It
// works below any layer, whose own closing message, such as TLS's
// close_notify, it does not send. On a UDP connection it does nothing.
func (c *Connection) CloseWrite() error {
	if c.udp != nil {
		return nil
	}
	return c.control("shutdown", func(s rawSocket) error {
		if c.writeShut {
			return nil
		}
		c.writeShut = true
		if c.sendHead != len(c.sends) || c.flushing || c.writeBusyLocked() {
			// Whatever drains the queue shuts the side; see
			// finishCloseWriteLocked.
			c.shutWritePending = true
			return nil
		}
		return syscall.Shutdown(s, syscall.SHUT_WR)
	})
}

// finishCloseWriteLocked carries out a CloseWrite that was waiting for the
// queue, which its caller has just drained. Callers hold c.mu.
func (c *Connection) finishCloseWriteLocked() {
	if !c.shutWritePending {
		return
	}
	c.shutWritePending = false
	if !c.closing && !c.closed {
		_ = syscall.Shutdown(c.rawSocket(), syscall.SHUT_WR)
	}
}

// MultipathTCP reports whether the connection is using Multipath TCP. It is
// false for anything but TCP, and wherever the platform has no Multipath TCP.
func (c *Connection) MultipathTCP() (bool, error) {
	if !c.IsTCP() {
		return false, nil
	}
	var using bool
	err := c.control("getsockopt", func(s rawSocket) error {
		using = usingMultipathTCP(s)
		return nil
	})
	return using, err
}

// SyscallConn returns the connection's socket for raw access. Its Control
// holds the connection's lock while f runs, so that the event loop cannot
// close the socket, and hand its descriptor to another connection, from under
// f; f must not call the connection's methods, or it deadlocks. Its Read and
// Write call f once and report ErrWouldBlock when f reports that it is not
// done, since waiting for readiness is the event loop's job; a Read races the
// loop's own reads, as Connection.Read does. A UDP listener's peer has no
// socket of its own and returns an error.
func (c *Connection) SyscallConn() (syscall.RawConn, error) {
	if c.udp != nil && c.udp.listener != nil {
		return nil, errNoSocket
	}
	return rawConn{c}, nil
}

type rawConn struct{ c *Connection }

func (r rawConn) Control(f func(fd uintptr)) error {
	return r.c.control("raw-control", func(s rawSocket) error {
		r.c.rawExposed.Store(true)
		f(uintptr(s))
		return nil
	})
}

func (r rawConn) Read(f func(fd uintptr) bool) error {
	return r.c.control("raw-read", func(s rawSocket) error {
		r.c.rawExposed.Store(true)
		if !f(uintptr(s)) {
			return ErrWouldBlock
		}
		return nil
	})
}

func (r rawConn) Write(f func(fd uintptr) bool) error {
	return r.c.control("raw-write", func(s rawSocket) error {
		r.c.rawExposed.Store(true)
		if !f(uintptr(s)) {
			return ErrWouldBlock
		}
		return nil
	})
}

// control runs f on the connection's own socket. It holds c.mu while f runs:
// the event loop marks a connection closed under c.mu before it releases the
// socket, so the descriptor f is given cannot be closed, and taken by another
// connection, until f returns. A UDP listener's peer has no socket of its
// own, and f does not run for it. A syscall.Errno from f is wrapped as the
// net package wraps it, with op naming the call.
func (c *Connection) control(op string, f func(s rawSocket) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.closed {
		return net.ErrClosed
	}
	if c.udp != nil && c.udp.listener != nil {
		return nil
	}
	err := f(c.rawSocket())
	if errno, ok := err.(syscall.Errno); ok {
		return os.NewSyscallError(op, errno)
	}
	return err
}

// tcpControl is control for a setsockopt only TCP has.
func (c *Connection) tcpControl(f func(s rawSocket) error) error {
	if !c.IsTCP() {
		return nil
	}
	return c.control("setsockopt", f)
}

// keepAliveSeconds turns a keep-alive time into the whole seconds the kernel
// takes, rounding up, with zero standing for the net package's default.
func keepAliveSeconds(d time.Duration) int {
	if d == 0 {
		d = defaultKeepAlive
	}
	return int((d + time.Second - 1) / time.Second)
}

// defaultKeepAlive and defaultKeepAliveCount are the net package's defaults
// for a keep-alive time and probe count left at zero.
const (
	defaultKeepAlive      = 15 * time.Second
	defaultKeepAliveCount = 9
)

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// deadline is one of a connection's two deadlines. Its timer is rebuilt
// whenever the deadline moves, and gen tells a timer that fires while it is
// moving from the one that is current, so a connection given more time is not
// closed by the deadline it has outlived.
type deadline struct {
	timer *time.Timer
	gen   uint64
	at    time.Time
}

// SetDeadline sets both the read and the write deadline. A deadline here does
// not make a call fail, as it does on a blocking socket, since neither Read
// nor Write ever waits: it closes the connection when it passes, and OnClose
// is given os.ErrDeadlineExceeded. It is therefore how a server bounds a peer
// that has gone quiet or stopped reading, and a handler that wants a rolling
// timeout sets the next one each time it hears from the peer.
//
// The read deadline closes the connection when it passes, whatever the peer
// has sent meanwhile; the write deadline closes it only if output accepted by
// Send has still not reached the kernel. A zero time removes a deadline, and
// one already in the past closes the connection at once. It may be called
// from any goroutine.
func (c *Connection) SetDeadline(t time.Time) error {
	c.mu.Lock()
	closed := c.closing || c.closed
	if !closed {
		c.armDeadlineLocked(&c.readDeadline, t, false)
		c.armDeadlineLocked(&c.writeDeadline, t, true)
	}
	c.mu.Unlock()
	if closed {
		return net.ErrClosed
	}
	return nil
}

// SetReadDeadline closes the connection at t, whether or not the peer has sent
// anything. See SetDeadline.
func (c *Connection) SetReadDeadline(t time.Time) error {
	return c.setDeadline(&c.readDeadline, t, false)
}

// SetWriteDeadline closes the connection at t if output accepted by Send has
// still not reached the kernel by then; output that has all gone out leaves
// the connection alone. See SetDeadline.
func (c *Connection) SetWriteDeadline(t time.Time) error {
	return c.setDeadline(&c.writeDeadline, t, true)
}

func (c *Connection) setDeadline(d *deadline, t time.Time, write bool) error {
	c.mu.Lock()
	closed := c.closing || c.closed
	if !closed {
		c.armDeadlineLocked(d, t, write)
	}
	c.mu.Unlock()
	if closed {
		return net.ErrClosed
	}
	return nil
}

// armDeadlineLocked points d at t. A deadline set to the time it already holds
// keeps the timer it has, so a handler that re-states the same deadline on
// every read round does not rebuild a timer per round.
func (c *Connection) armDeadlineLocked(d *deadline, t time.Time, write bool) {
	if d.timer != nil && d.at.Equal(t) || d.timer == nil && t.IsZero() {
		return
	}
	d.gen++
	d.at = t
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	if t.IsZero() {
		return
	}
	gen := d.gen
	d.timer = time.AfterFunc(max(time.Until(t), 0), func() { c.expireDeadline(d, gen, write) })
}

// expireDeadline closes a connection whose deadline has passed, unless the
// deadline moved while the timer was on its way or the connection is already
// going.
func (c *Connection) expireDeadline(d *deadline, gen uint64, write bool) {
	c.mu.Lock()
	stale := d.gen != gen || c.closing || c.closed
	c.mu.Unlock()
	if stale {
		return
	}
	if write && c.pendingBytes.Load() == 0 {
		// Everything Send accepted has reached the kernel, so there is no
		// write left for the deadline to be about.
		return
	}
	c.closeWithError(os.ErrDeadlineExceeded)
}

// stopDeadlinesLocked drops both timers, so a closed connection is not kept
// reachable by a deadline it will never reach.
func (c *Connection) stopDeadlinesLocked() {
	for _, d := range [...]*deadline{&c.readDeadline, &c.writeDeadline} {
		if d.timer != nil {
			d.timer.Stop()
			d.timer = nil
		}
		d.gen++
	}
}

// SendFile sends count bytes of f, starting at offset, after whatever was
// sent before it and ahead of whatever is sent after. On Linux and macOS the
// bytes go from the file to the socket by sendfile(2), without passing
// through user space; on Windows they are read a chunk at a time as the
// socket takes them. Either way the file is read only as the peer keeps up,
// so a large file costs no more memory than a small one.
//
// The connection sends from its own duplicate of f's descriptor, so the
// caller may close f as soon as SendFile returns. The file must hold the
// whole range: a file that turns out shorter closes the connection, since the
// peer was promised count bytes.
//
// A connection with a layer, such as TLS, cannot hand the file to the kernel,
// since the layer has to transform its bytes: the range is read and sent
// through the layer before SendFile returns.
func (c *Connection) SendFile(f File, offset, count int64) error {
	if err := checkSendFileRange(offset, count); err != nil {
		return err
	}
	if c.udp != nil {
		return ErrSendFileDatagram
	}
	if count == 0 {
		return nil
	}
	if l := c.layer; l != nil {
		return sendFileCopy(c, f, offset, count, func(b []byte) error { return l.Send(b, nil) })
	}
	seg, err := newFileSegment(f, offset, count)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.closing || c.closed || c.closeAfterSend || c.writeShut {
		c.mu.Unlock()
		seg.close()
		return syscall.EPIPE
	}
	direct := c.canWriteDirectlyLocked()
	if c.sendHead == len(c.sends) {
		c.rewindQueueLocked()
	}
	c.sends = append(c.sends, sendItem{file: seg})
	c.mu.Unlock()
	if !direct {
		// Whatever is queued ahead of the file is being flushed, or will be
		// when the round that corked the connection ends, and the file goes
		// out after it.
		return nil
	}
	if err := c.flushOutput(); err != nil {
		c.closeWithError(err)
		return err
	}
	return nil
}

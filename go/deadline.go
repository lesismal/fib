//go:build linux || darwin || windows

package fib

import (
	"net"
	"os"
	"time"
)

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

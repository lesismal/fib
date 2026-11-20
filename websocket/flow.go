//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package websocket

import (
	"net"

	fib "github.com/lesismal/fib"
	epollhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/internal/outputwait"
)

// This file is what a connection offers beyond its frames: its transport's
// address, a close that skips the handshake, and the pacing of a writer that
// may not wait, in both directions, as a relay between a WebSocket and
// another connection needs.

// RemoteAddr returns the address of the connection's peer, on a server's
// connection and a dialed one alike: the TCP peer of a connection served or
// dialed directly, or upgraded over HTTP/1.1 or HTTP/2, and the address a
// QUIC connection began on for one upgraded over HTTP/3. A dial through a
// proxy, with DialerConfig.Dial, reports the proxy's. It is what the
// connection's socket reports, read each call, so it may be nil once the
// connection has closed; keep it from OnOpen if it is wanted after.
func (c *Connection) RemoteAddr() net.Addr {
	switch t := c.conn.(type) {
	case *fib.Connection:
		return t.RemoteAddr()
	case *epollhttp.Tunnel:
		if conn := t.Conn(); conn != nil {
			return conn.RemoteAddr()
		}
	}
	return nil
}

// LocalAddr returns the address of this end of the connection, as
// RemoteAddr returns the peer's: the local end of the TCP connection, or of
// the QUIC connection one upgraded over HTTP/3 runs on. It may be nil once
// the connection has closed.
func (c *Connection) LocalAddr() net.Addr {
	switch t := c.conn.(type) {
	case *fib.Connection:
		return t.LocalAddr()
	case *epollhttp.Tunnel:
		if conn := t.Conn(); conn != nil {
			return conn.LocalAddr()
		}
	}
	return nil
}

// NetConn returns the connection the WebSocket's frames go through when it
// has one of its own: one a ServerHandler serves directly, or one a Dialer
// dialed. Its own settings are then the WebSocket's to change, such as
// SetWriteQueueLimit, SetKeepAlive, SetNoDelay or a deadline; what it sends
// must still go through the WebSocket's methods, or the peer sees bytes
// that are not frames. On a connection upgraded through Upgrade it returns
// nil: see Tunnel.
func (c *Connection) NetConn() *fib.Connection {
	conn, _ := c.conn.(*fib.Connection)
	return conn
}

// Tunnel returns the tunnel of a connection upgraded through
// ServerHandler.Upgrade, and nil for any other. Its Conn is the connection
// it runs on: over HTTP/1.1 the whole of it switched to the WebSocket, as
// NetConn's connection is, while over HTTP/2 and HTTP/3 it carries other
// requests too, and its settings are the server's to keep.
func (c *Connection) Tunnel() *epollhttp.Tunnel {
	t, _ := c.conn.(*epollhttp.Tunnel)
	return t
}

// SetAttachment keeps value with the connection, for the Handler's callbacks
// and whatever else holds the connection to read back with Attachment: the
// state of the session it carries, which would otherwise have to be found in
// a map on every message. There is one slot, which nil clears; several
// values go in a struct of the caller's. It may be set and read from any
// goroutine, and stays set once the connection has closed, for OnClose to
// read. A ServerHandler's own connections are first seen in OnOpen, which
// can set it there; one upgraded through UpgradeWithAttachment has it set
// before OnOpen. Setting it allocates the 16 bytes that hold value, unless
// value is nil.
func (c *Connection) SetAttachment(value any) {
	if value == nil {
		c.attachment.Store(nil)
		return
	}
	c.attachment.Store(&value)
}

// Attachment returns what SetAttachment last set, or nil.
func (c *Connection) Attachment() any {
	if p := c.attachment.Load(); p != nil {
		return *p
	}
	return nil
}

// Abort ends the connection at once, without the close handshake: no close
// frame is sent, what is still queued for the peer is dropped, and nothing
// more that arrives is handed to the Handler. OnClose follows, with the
// code and reason of a close frame Close sent before, or 1006, and err, or
// the error the connection ended with when err is nil. It is for a peer that
// is dead, or so slow that a close frame would wait behind everything it has
// not read. Only the first Abort counts, and one after the connection has
// ended does nothing. On a connection upgraded over HTTP/2 or HTTP/3 it
// resets the stream, and the connection under it goes on.
func (c *Connection) Abort(err error) {
	c.closeSent.Store(true)
	for {
		old := c.closeState.Load()
		if old != nil && old.aborted {
			return
		}
		next := &connectionCloseState{code: 1006, aborted: true, err: err}
		if old != nil {
			next.code, next.reason = old.code, old.reason
		}
		if c.closeState.CompareAndSwap(old, next) {
			break
		}
	}
	if conn, ok := c.conn.(*fib.Connection); ok && err != nil {
		conn.CloseWithError(err)
		return
	}
	_ = c.conn.Close()
}

// pendingWriteLimit is the connection's PendingWriteLimit.
func (c *Connection) pendingWriteLimit() int64 {
	if c.settings != nil && c.settings.pendingLimit > 0 {
		return c.settings.pendingLimit
	}
	return DefaultPendingWriteLimit
}

// pending is how many bytes sent on the connection still wait to go to the
// peer: queued for the socket, or, on a stream of HTTP/2 or HTTP/3, held back
// by flow control. Over a connection of its own it is read without the
// connection's lock, as a writer pacing itself reads it, and may count what a
// write under way is handing the socket.
func (c *Connection) pending() int64 {
	switch t := c.conn.(type) {
	case *fib.Connection:
		if outputwait.Queued != nil {
			return outputwait.Queued(t)
		}
	case *epollhttp.Tunnel:
		return int64(t.Unsent())
	}
	return 0
}

// WriteMessageNoWait sends a message as WriteMessage does, never waiting for
// the peer, and reports above when more than the connection's
// PendingWriteLimit (Config.PendingWriteLimit, DialerConfig.PendingWriteLimit)
// is then waiting to go to it. A writer that gets above stops sending until a
// function it gives OnWritable runs, which is how a producer is paced by a
// slow peer without a goroutine waiting on it. Nothing is refused for being
// above: the message is queued as WriteMessage queues it, so a writer that
// goes on regardless is not held back, and only the engine's
// MaxWriteQueueBytes, if set, bounds it.
//
// What waits is counted as it goes on the wire, framed and, with
// permessage-deflate, compressed, together with every other frame the
// connection sent, pongs and close included. What a Handler sends from one
// of its connection's own callbacks waits under the read round's cork, and
// counts, until the callback returns and the round hands it to the socket;
// an OnWritable registered meanwhile runs once that has gone.
func (c *Connection) WriteMessageNoWait(opcode Opcode, payload []byte) (above bool, err error) {
	if err := c.WriteMessage(opcode, payload); err != nil {
		return false, err
	}
	return c.pending() > c.pendingWriteLimit(), nil
}

// WritePreparedMessageNoWait sends m as WritePreparedMessage does, and reports
// above as WriteMessageNoWait does. A server's connections queue the one
// frame they share, which counts in full on each of them.
func (c *Connection) WritePreparedMessageNoWait(m *PreparedMessage) (above bool, err error) {
	if err := c.WritePreparedMessage(m); err != nil {
		return false, err
	}
	return c.pending() > c.pendingWriteLimit(), nil
}

// OnWritable arranges for fn to run, once, on another goroutine, when what
// waits to go to the peer, which WriteMessageNoWait reported above the
// connection's PendingWriteLimit, has fallen to half of it, or with an error
// when the connection ends first, Abort included; and reports true. It
// reports false, and never runs fn, when no more than the limit waits
// already, so a writer resumes at once:
//
//	if above, err := c.WriteMessageNoWait(websocket.Binary, p); err == nil && above {
//		pause()
//		if !c.OnWritable(resume) {
//			resume(nil)
//		}
//	}
//
// It may be called from any goroutine, the Handler's callbacks included. Each
// call registers its own fn. On a connection upgraded through a server that
// cannot tell when its stream drains it reports false.
func (c *Connection) OnWritable(fn func(error)) bool {
	if fn == nil {
		return false
	}
	switch t := c.conn.(type) {
	case *fib.Connection:
		return t.NotifyOutput(c.pendingWriteLimit(), fn)
	case *epollhttp.Tunnel:
		return t.NotifySendable(int(c.pendingWriteLimit()), fn)
	}
	return false
}

// HoldReads stops, with hold, and resumes, without, reading what the peer
// sends, so that a Handler passing messages on to something slower paces the
// peer by TCP's flow control, or the stream's over HTTP/2 and HTTP/3, rather
// than by memory here. Holds do not nest: the last call wins. It may be
// called from any goroutine, the Handler's callbacks included.
//
// The hold is on reading, not on delivery: the frames of a read already made
// are still handed to the Handler, at most one read's worth, before the
// connection stops; a message part of whose frames have arrived waits, as
// much of it as was read, until reads resume. Nothing at all is read while
// reads are held, pings and close frames included, so a peer's ping goes
// unanswered until then: hold for less time than the peer waits for a pong.
func (c *Connection) HoldReads(hold bool) {
	switch t := c.conn.(type) {
	case *fib.Connection:
		t.HoldReads(hold)
	case *epollhttp.Tunnel:
		t.HoldReads(hold)
	}
}

// SetWriteBackpressure says whether what waits to go to the peer holds back
// reading from it, which a connection a server accepted does by default and
// a dialed one does not; see fib.Connection.SetWriteBackpressure. A relay
// that passes each of two connections on to the other, and paces each with
// HoldReads and OnWritable, turns it off, as a peer that waits to send
// before it reads more would otherwise wait forever. Over HTTP/2 and HTTP/3
// the stream's flow control paces it instead, and it does nothing.
func (c *Connection) SetWriteBackpressure(on bool) {
	switch t := c.conn.(type) {
	case *fib.Connection:
		t.SetWriteBackpressure(on)
	case *epollhttp.Tunnel:
		t.SetWriteBackpressure(on)
	}
}

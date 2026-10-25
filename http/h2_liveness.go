//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	"time"

	"github.com/lesismal/fib/internal/outputwait"
)

// DefaultSettingsTimeout is Config.SettingsTimeout's default, and
// ClientConfig.SettingsTimeout's. The server's PING timeout shares
// DefaultPingTimeout, the client's.
const DefaultSettingsTimeout = 10 * time.Second

// ErrH2PingTimeout is what the streams of an HTTP/2 connection fail with,
// and its OnClose reports, when the client answered nothing within
// Config.PingTimeout of a PING; see Config.PingInterval.
var ErrH2PingTimeout = errors.New("http2: client did not answer PING in time")

// h2PingPayload is what the server's PINGs carry. The client echoes it,
// and any frame at all is word enough that it is there, so nothing checks
// it.
var h2PingPayload = [8]byte{'f', 'i', 'b', 'p', 'i', 'n', 'g', 0}

// settingsTimeout is how long a client has to acknowledge the server's
// SETTINGS, or zero for as long as it likes.
func (h *ServerHandler) settingsTimeout() time.Duration {
	switch d := h.config.SettingsTimeout; {
	case d < 0:
		return 0
	case d == 0:
		return DefaultSettingsTimeout
	default:
		return d
	}
}

// settingsTimeout is how long a server has to acknowledge the client's
// SETTINGS, or zero for as long as it likes. It mirrors the server's, and
// shares its default.
func (c *ClientConfig) settingsTimeout() time.Duration {
	switch d := c.SettingsTimeout; {
	case d < 0:
		return 0
	case d == 0:
		return DefaultSettingsTimeout
	default:
		return d
	}
}

// pingTimeout is how long a client has to answer the server's PING.
func (h *ServerHandler) pingTimeout() time.Duration {
	if d := h.config.PingTimeout; d > 0 {
		return d
	}
	return DefaultPingTimeout
}

// armLivenessLocked starts the timers that check the client is there, as
// the server has just sent its SETTINGS: one ends the connection with
// SETTINGS_TIMEOUT unless the client acknowledges them within
// Config.SettingsTimeout, and the other, with a Config.PingInterval, looks
// in on the connection every interval; see pingTick. Callers hold sc.mu.
func (sc *h2ServerConn) armLivenessLocked() {
	if d := sc.handler.settingsTimeout(); d > 0 && !sc.settingsAcked {
		sc.settingsTimer = time.AfterFunc(d, sc.settingsExpired)
	}
	if d := sc.handler.config.PingInterval; d > 0 {
		sc.pingReads = sc.reads.Load()
		sc.pingTimer = time.AfterFunc(d, sc.pingTick)
	}
}

// settingsAckedLocked is the client's acknowledgement of the server's
// SETTINGS. Callers hold sc.mu.
func (sc *h2ServerConn) settingsAckedLocked() {
	sc.settingsAcked = true
	if sc.settingsTimer != nil {
		sc.settingsTimer.Stop()
		sc.settingsTimer = nil
	}
}

// settingsExpired ends a connection whose client has not acknowledged the
// server's SETTINGS in time (RFC 9113 section 6.5.3). An acknowledgement
// that arrived as the timer fired leaves the connection alone.
func (sc *h2ServerConn) settingsExpired() {
	sc.mu.Lock()
	expired := !sc.settingsAcked && !sc.closed
	sc.mu.Unlock()
	if expired {
		sc.fail(h2ConnErr(H2SettingsTimeout, "SETTINGS not acknowledged"))
	}
}

// pingTick looks in on the connection, every PingInterval while the client
// is there, and PingTimeout after a PING. A connection that has read
// anything since the last look is alive, and so is one whose queued output
// has shrunk: the connection stops reading while more than its high
// watermark waits for the socket, and a client downloading slowly is then
// heard from only as it takes what was sent, which, since HTTP/2's windows
// keep the server from sending more until it reads the client's updates,
// is all it can do. A connection that has done neither is sent a PING,
// unless one is out already, in which case the client has gone quiet for
// the whole of PingTimeout after it and the connection is closed. Nothing
// reaches a client that has gone, so the connection is closed at once, what
// it has queued dropped, rather than retired with GOAWAY.
func (sc *h2ServerConn) pingTick() {
	sc.mu.Lock()
	if sc.closed || sc.pingTimer == nil {
		sc.mu.Unlock()
		return
	}
	reads, pending := sc.reads.Load(), sc.pendingOutput()
	alive := reads != sc.pingReads || pending < sc.pingPending
	sc.pingReads, sc.pingPending = reads, pending
	switch {
	case alive:
		sc.pingOut = false
		sc.pingTimer.Reset(sc.handler.config.PingInterval)
	case !sc.pingOut:
		sc.pingOut = true
		out := h2AppendFrameHeader(nil, h2FramePing, 0, 0, len(h2PingPayload))
		sc.sendLocked(append(out, h2PingPayload[:]...))
		sc.pingTimer.Reset(sc.handler.pingTimeout())
	default:
		sc.closed, sc.goAway = true, true
		sc.pingTimer = nil
		sc.failBodiesLocked(ErrH2PingTimeout)
		clear(sc.streams)
		sc.mu.Unlock()
		sc.conn.CloseWithError(ErrH2PingTimeout)
		return
	}
	sc.mu.Unlock()
}

// pendingOutput is how much of what the connection sent waits for its
// socket.
func (sc *h2ServerConn) pendingOutput() int64 {
	if outputwait.Pending == nil || sc.conn == nil {
		return 0
	}
	return outputwait.Pending(sc.conn)
}

// stopLivenessLocked stops the timers armLivenessLocked started, as the
// connection closes: a timer left set would hold the connection, and its
// engine, until it fired. Callers hold sc.mu.
func (sc *h2ServerConn) stopLivenessLocked() {
	if sc.settingsTimer != nil {
		sc.settingsTimer.Stop()
		sc.settingsTimer = nil
	}
	if sc.pingTimer != nil {
		sc.pingTimer.Stop()
		sc.pingTimer = nil
	}
}

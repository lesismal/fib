package quic

import "time"

// What a peer may make a connection do for it without asking for anything,
// before the connection closes with Config.ExcessiveLoadCode. Each is a
// bucket of burst tokens, given back at rate a second, as HTTP/2's are in
// package http.
//
// Resets count the streams the peer abandons while they are still open,
// with RESET_STREAM or STOP_SENDING, and those this side resets for an
// error the peer made on them (see Stream.ResetForPeerError), each once:
// opening a stream and abandoning it at once costs this side the stream and
// whatever the application started for it, but costs the peer nothing, and
// the stream no longer counts against the limit on streams open at once (as
// HTTP/2's Rapid Reset, CVE-2023-44487, does).
//
// Glitches count the frames that ask for work and carry nothing: a packet
// that elicits an acknowledgement with nothing but PING, unless an
// acknowledgement in it acknowledged something new, as the acknowledgements
// a busy receiver bundles a PING with do; PATH_CHALLENGE, which is
// answered; MAX_DATA, MAX_STREAM_DATA and MAX_STREAMS that raise no limit;
// DATA_BLOCKED, STREAM_DATA_BLOCKED and STREAMS_BLOCKED; a STREAM frame
// with no data that ends nothing; and NEW_CONNECTION_ID for a connection ID
// already retired, which is answered with RETIRE_CONNECTION_ID. Frames that
// cost no more than their bytes to read, and those a peer may send in
// flight for a stream that has ended, such as MAX_STREAM_DATA or
// RESET_STREAM for one this side has forgotten, are not counted.
const (
	floodResetBurst  = 1000
	floodResetRate   = 100
	floodGlitchBurst = 1000
	floodGlitchRate  = 100
)

// floodBudget is one of a connection's flood buckets. It is guarded by the
// connection's mu.
type floodBudget struct {
	tokens float64
	last   time.Time
}

// take spends a token, refilling the bucket for the time since the last
// one, and reports false once there is none left.
func (b *floodBudget) take(burst, rate float64) bool {
	now := time.Now()
	if b.last.IsZero() {
		b.tokens = burst
	} else {
		b.tokens = min(burst, b.tokens+now.Sub(b.last).Seconds()*rate)
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// floodErr is what a connection closes with once its peer has spent a
// budget: the application's code for it, if it has one, and otherwise
// PROTOCOL_VIOLATION, the nearest a transport error comes.
func (c *Conn) floodErr(reason string) error {
	if code := c.config.ExcessiveLoadCode; code != 0 {
		return &ApplicationError{Code: code, Reason: reason}
	}
	return transportErr(errProtocolViolation, reason)
}

// glitch counts a frame that asked for work and carried nothing, and
// reports the error to close the connection with once there have been too
// many. Callers hold c.mu.
func (c *Conn) glitch() error {
	if c.glitches.take(floodGlitchBurst, floodGlitchRate) {
		return nil
	}
	return c.floodErr("too many frames that ask for nothing")
}

// resetCounted counts a stream abandoned while it was open, once however
// often it is, and reports the error to close the connection with once
// there have been too many. Callers hold c.mu.
func (c *Conn) resetCounted(s *Stream) error {
	if s.floodCounted {
		return nil
	}
	s.floodCounted = true
	if c.resets.take(floodResetBurst, floodResetRate) {
		return nil
	}
	return c.floodErr("too many streams reset")
}

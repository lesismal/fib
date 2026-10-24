package http

import (
	"math"
	"time"
)

// What an HTTP/2 client may make the server do for it without asking for
// anything, before the connection ends with GOAWAY and ENHANCE_YOUR_CALM,
// when Config leaves its flood limits at zero. Each is a bucket of burst
// tokens, given back at rate a second.
//
// Resets count the streams a client resets while they are still open, and
// those the server resets for a stream error the client made: opening a
// stream and resetting it at once costs the server the stream and its
// handler but costs the client nothing, and the stream no longer counts
// against MaxConcurrentStreams (Rapid Reset, CVE-2023-44487). Control
// frames count the frames that ask for work and carry no request: PING and
// SETTINGS, each of which the server acknowledges, and DATA and
// CONTINUATION frames that are empty without ending anything
// (CVE-2019-9512, CVE-2019-9515, CVE-2019-9518, CVE-2024-27316). Frames that
// cost no more than their bytes to read, such as PRIORITY, or a
// WINDOW_UPDATE or RST_STREAM for a stream that has ended, which a client
// may send in flight, are not counted.
//
// The HTTP/2 client holds a server to the same control-frame budget and
// CONTINUATION bound, with these values; a server resetting the client's
// streams costs the client nothing, so it has no reset budget.
const (
	DefaultStreamResetBurst  = 1000
	DefaultStreamResetRate   = 100
	DefaultControlFrameBurst = 1000
	DefaultControlFrameRate  = 100
	// DefaultMaxContinuationFrames bounds the CONTINUATION frames one header
	// block may arrive in, whatever their size.
	DefaultMaxContinuationFrames = 1000
)

// h2Budget is one of a connection's flood buckets. Only its reader touches
// it.
type h2Budget struct {
	tokens float64
	// burst and rate are the bucket's size and how many tokens it gets back
	// a second; a burst below zero is a bucket that never runs out.
	burst float64
	rate  float64
	last  time.Time
}

// h2NewBudget is a bucket of burst tokens given back at rate a second, with
// fallback's values for those left at zero, and none at all when either is
// below zero.
func h2NewBudget(burst, rate, fallbackBurst, fallbackRate int) h2Budget {
	if burst < 0 || rate < 0 {
		return h2Budget{burst: -1}
	}
	if burst == 0 {
		burst = fallbackBurst
	}
	if rate == 0 {
		rate = fallbackRate
	}
	return h2Budget{burst: float64(burst), rate: float64(rate)}
}

// take spends a token, refilling the bucket for the time since the last
// one, and reports false once there is none left.
func (b *h2Budget) take() bool {
	if b.burst < 0 {
		return true
	}
	now := time.Now()
	if b.last.IsZero() {
		b.tokens = b.burst
	} else {
		b.tokens = min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// h2MaxContinuations is the CONTINUATION frames a header block may arrive
// in when a limit is n: fallback for zero, and no bound below zero.
func h2MaxContinuations(n, fallback int) int {
	switch {
	case n < 0:
		return math.MaxInt
	case n == 0:
		return fallback
	}
	return n
}

// h2ControlFlood is the connection error a peer that sent too many control
// frames is told, by a server and a client alike.
func h2ControlFlood() *H2ConnError {
	return h2ConnErr(H2EnhanceYourCalm, "too many control frames")
}

// glitch counts a frame that asked for work and carried no request, and
// reports the connection error to end it with once there have been too many.
func (sc *h2ServerConn) glitch() error {
	if sc.glitches.take() {
		return nil
	}
	return h2ControlFlood()
}

// glitch counts a frame the server sent that asked for work and carried no
// response, and reports the connection error to end it with once there have
// been too many. A server is held to the same budget as a client, since a
// client acknowledges its PING and SETTINGS frames as a server does.
func (hc *h2ClientConn) glitch() error {
	if hc.glitches.take() {
		return nil
	}
	return h2ControlFlood()
}

// resetCounted counts a stream reset while it was open, and reports the
// connection error to end it with once there have been too many.
func (sc *h2ServerConn) resetCounted() error {
	if sc.resets.take() {
		return nil
	}
	return h2ConnErr(H2EnhanceYourCalm, "too many streams reset")
}

// armHeaderTimer bounds how long a header block may take to arrive in
// CONTINUATION frames by the header timeout, as an HTTP/1 header is: a
// block that has not ended by then ends the connection.
func (sc *h2ServerConn) armHeaderTimer() {
	d := sc.handler.headerTimeout()
	if d <= 0 {
		return
	}
	gen := sc.contGen.Load()
	sc.contTimer = time.AfterFunc(d, func() {
		if sc.contGen.Load() == gen {
			sc.fail(h2ConnErr(H2EnhanceYourCalm, "header block took too long"))
		}
	})
}

// stopHeaderTimer is a header block's end: the timer armHeaderTimer set for
// it no longer applies, even when it has fired already.
func (sc *h2ServerConn) stopHeaderTimer() {
	sc.contGen.Add(1)
	if sc.contTimer != nil {
		sc.contTimer.Stop()
		sc.contTimer = nil
	}
}

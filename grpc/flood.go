//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package grpc

import "time"

// What a client may make a server's transport do for it without asking for
// anything, before the connection ends with GOAWAY and ENHANCE_YOUR_CALM:
// the values package http's HTTP/2 server defaults to. Each is a bucket of
// burst tokens, given back at rate a second.
//
// Resets count the streams a client resets while they are open, those the
// server resets for a stream error the client made, and those it refuses
// for going past the stream limit: opening a stream and resetting it at
// once costs the server the stream and its handler but costs the client
// nothing (Rapid Reset, CVE-2023-44487).
//
// Control frames count the frames that ask for work and carry no call:
// PING and SETTINGS, each of which is acknowledged, and DATA, HEADERS and
// CONTINUATION frames that are empty without ending anything
// (CVE-2019-9512, CVE-2019-9515, CVE-2019-9518); a PING that follows DATA
// this side sent is not counted. A client's transport holds a server to the
// same budget, which costs it the same.
const (
	streamResetBurst  = 1000
	streamResetRate   = 100
	controlFrameBurst = 1000
	controlFrameRate  = 100
	// maxContinuationFrames bounds the CONTINUATION frames one header
	// block may arrive in, whatever their size (CVE-2024-27316).
	maxContinuationFrames = 1000
)

// budget is one of a connection's flood buckets. Only its reader touches it.
type budget struct {
	tokens float64
	burst  float64
	rate   float64
	last   time.Time
}

func newBudget(burst, rate int) budget {
	return budget{burst: float64(burst), rate: float64(rate)}
}

// take spends a token, refilling the bucket for the time since the last
// one, and reports false once there is none left.
func (b *budget) take() bool {
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

// resetCounted counts a stream reset that cost the server a stream, and
// reports the connection error to end it with once there have been too
// many. A client's transport counts nothing: a server resetting its streams
// costs it nothing.
func (t *transport) resetCounted() error {
	if t.client || t.resets.take() {
		return nil
	}
	return &connError{code: errEnhanceCalm, msg: "too many streams reset"}
}

// glitch counts a frame that asked for work and carried nothing, and
// reports the connection error to end it with once there have been too
// many.
func (t *transport) glitch() error {
	if t.glitches.take() {
		return nil
	}
	return &connError{code: errEnhanceCalm, msg: "too many control frames"}
}

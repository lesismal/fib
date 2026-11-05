package core

import (
	"context"
	"sync/atomic"
	"time"
)

const (
	// shutdownPollMin and shutdownPollMax bound how long Shutdown waits
	// between asking the loops about their connections: it starts short, so
	// that an engine with nothing left in flight stops at once, and doubles
	// up to the longer wait, as net/http.Server.Shutdown polls.
	shutdownPollMin = time.Millisecond
	shutdownPollMax = 500 * time.Millisecond
	// shutdownStopCheck is how often Shutdown, waiting for the loops to
	// answer, looks whether the engine has stopped under it, after which no
	// loop would.
	shutdownStopCheck = 10 * time.Millisecond
)

// Shutdown stops the engine gracefully, as net/http.Server.Shutdown does a
// server. Its listeners stop taking connections at once, so that a client
// connecting from then on is refused, and the connections already open are
// left to finish: Shutdown waits until every connection the engine accepted
// has closed, or until ctx is done, and then stops the engine as Stop does,
// which makes Run return. It returns ctx.Err() if ctx ended the wait, and nil
// otherwise, including when the engine stopped some other way meanwhile.
// Connections still open when it returns stay open until Close, which is
// still the caller's to make once Run has returned.
//
// The engine cannot tell an idle connection from a busy one; the protocol
// spoken over it can. Each connection whose handler is a ShutdownHandler is
// told through OnShutdown, which is how the HTTP server closes its idle
// connections, closes busy ones once their responses are written, and sends
// HTTP/2 GOAWAY. A connection whose handler is not one keeps the engine
// waiting until it closes, by its peer or its handler, or until ctx is done.
//
// Connections the engine dialed are not waited for. A UDP listener's known
// peers are, and while the engine shuts down a datagram from a peer it does
// not know is dropped rather than opening a connection.
func (e *Engine) Shutdown(ctx context.Context) error {
	e.beginShutdown()
	err := e.awaitDrained(ctx, -1)
	e.Stop()
	return err
}

// ShuttingDown reports whether Shutdown has begun. A protocol checks it to
// end a connection once its exchange in flight is done, as the HTTP server
// closes a connection behind the response it writes then, and a handler can
// check it to fail a readiness probe while the engine drains.
func (e *Engine) ShuttingDown() bool { return e.root().draining.Load() }

// awaitDrained waits until no connection the engine accepted is left open,
// or none the binding-th binding took for a binding of zero or more, asking
// the loops at a falling rate, each time telling the connections that remain
// they are to finish. It returns ctx.Err() if ctx is done first.
func (e *Engine) awaitDrained(ctx context.Context, binding int) error {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for wait := shutdownPollMin; ; wait = min(2*wait, shutdownPollMax) {
		left, err := e.shutdownRound(ctx, binding)
		if err != nil || left == 0 {
			return err
		}
		if timer == nil {
			timer = time.NewTimer(wait)
		} else {
			timer.Reset(wait)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// shutdownRound is one pass of Shutdown over the engine's loops: how many
// connections they still hold, and how many loops have yet to answer.
type shutdownRound struct {
	left    atomic.Int64
	waiting atomic.Int32
	done    chan struct{}
}

func newShutdownRound(loops int) *shutdownRound {
	r := &shutdownRound{done: make(chan struct{})}
	r.waiting.Store(int32(loops))
	return r
}

// report is one loop's answer: n connections remain on it.
func (r *shutdownRound) report(n int) {
	r.left.Add(int64(n))
	if r.waiting.Add(-1) == 0 {
		close(r.done)
	}
}

// await waits for every loop to answer, and reports the connections they
// hold between them. It reports none once the engine has stopped, since no
// loop would answer then, and ctx.Err() if ctx is done first.
func (r *shutdownRound) await(ctx context.Context, e *Engine) (int, error) {
	check := time.NewTicker(shutdownStopCheck)
	defer check.Stop()
	for {
		select {
		case <-r.done:
			return int(r.left.Load()), nil
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-check.C:
			if e.stopping.Load() {
				return 0, nil
			}
		}
	}
}

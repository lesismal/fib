package quic

import (
	"fmt"
	"runtime/debug"

	"github.com/lesismal/fib/log"
)

// errPanicked is what a connection closes with once something it ran
// panicked: whatever it was doing was left half done.
var errPanicked = &TransportError{Code: errInternal, Reason: "internal error"}

// unlockPanicked is deferred by a function that takes c.mu and holds it
// while the connection works, with *locked true for as long as it does: if
// what it ran panics with the lock held, the connection is closed with
// INTERNAL_ERROR and the lock let go, so that the panic goes on to whoever
// recovers it, the engine's pool say, with the connection ended rather than
// wedged on its lock. The writes that let the lock go meanwhile take it back
// even when they panic (see sendUnlocked), so *locked holds throughout.
func (c *Conn) unlockPanicked(locked *bool) {
	if *locked {
		c.closePanickedLocked()
	}
}

// recoverOwn is deferred by the goroutines the connection runs by itself,
// its timer's and the one that ends its closing period, where nothing else
// would recover a panic: rather than take the process down with it, the
// panic is logged and the connection closed with INTERNAL_ERROR, its
// handler told. *locked says whether the goroutine held c.mu, as for
// unlockPanicked; a handler call that panics leaves it free (see runBatch).
func (c *Conn) recoverOwn(locked *bool) {
	r := recover()
	if r == nil {
		return
	}
	log.Error("quic: connection goroutine panicked", "panic", fmt.Sprint(r), "stack", log.Lines(debug.Stack()))
	if !*locked {
		c.mu.Lock()
	}
	c.closePanickedLocked()
	defer func() {
		if r := recover(); r != nil {
			log.Error("quic: handler panicked on a connection closed for a panic", "panic", fmt.Sprint(r))
		}
	}()
	c.dispatch()
}

// closePanickedLocked closes the connection with errPanicked after a panic,
// and lets c.mu go, which the caller holds, whatever more goes wrong.
func (c *Conn) closePanickedLocked() {
	defer c.mu.Unlock()
	defer func() {
		// The CONNECTION_CLOSE could not be sent: the connection still
		// ends, without telling the peer.
		if recover() != nil && !c.closed {
			c.terminateLocked(errPanicked)
		}
	}()
	c.closeLocked(errPanicked)
}

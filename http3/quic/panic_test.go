package quic

import (
	"testing"
	"time"
)

// panicHandler panics in OnStreamsAvailable, and counts the OnClose calls.
type panicHandler struct {
	fuzzHandler
	closed *int
	err    *error
}

func (panicHandler) OnStreamsAvailable(*Conn) { panic("handler bug") }

func (h panicHandler) OnClose(_ *Conn, err error) { *h.closed, *h.err = *h.closed+1, err }

// panicPacketConn panics in Send.
type panicPacketConn struct{ fuzzPacketConn }

func (panicPacketConn) Send([]byte) error { panic("path bug") }

// checkPanicClosed fails the test unless c was closed for a panic, and its
// lock and dispatching left free.
func checkPanicClosed(t *testing.T, c *Conn) {
	t.Helper()
	if !c.mu.TryLock() {
		t.Fatal("the lock was left held")
	}
	defer c.mu.Unlock()
	if !c.closed || c.closeErr != errPanicked || c.dispatching {
		t.Fatalf("closed %v with %v, dispatching %v", c.closed, c.closeErr, c.dispatching)
	}
}

// A handler call that panics on the timer's goroutine, where nothing else
// would recover it, closes the connection with INTERNAL_ERROR, and the
// handler hears of the close, rather than the process going down.
func TestPanicOnTimerClosesConnection(t *testing.T) {
	c := frameConn(t)
	var closes int
	var closeErr error
	c.handler = panicHandler{closed: &closes, err: &closeErr}
	c.mu.Lock()
	c.events = append(c.events, event{kind: evStreamsAvailable})
	c.mu.Unlock()
	c.onTimer()
	checkPanicClosed(t, c)
	if closes != 1 || closeErr != errPanicked {
		t.Fatalf("OnClose %d times, with %v", closes, closeErr)
	}
}

// A path that panics while the connection sends, which it does with its lock
// let go, on the timer's goroutine closes the connection too, and leaves the
// lock free, though sending the CONNECTION_CLOSE panics as well.
func TestPanicOfPathOnTimerClosesConnection(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	c.pc = panicPacketConn{}
	c.pingPending = true
	c.lossDeadline = time.Now().Add(-time.Second)
	c.mu.Unlock()
	c.onTimer()
	checkPanicClosed(t, c)
}

// A panic with the lock held, on a goroutine whose caller recovers it, as
// the engine's pool does, still reaches that caller, with the connection
// closed and its lock let go.
func TestPanicUnderLockGoesOn(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	c.pc = panicPacketConn{}
	c.pingPending, c.wantFlush = true, true
	c.mu.Unlock()
	recovered := func() (r any) {
		defer func() { r = recover() }()
		c.flush()
		return nil
	}()
	if recovered != "path bug" {
		t.Fatalf("recovered %v", recovered)
	}
	checkPanicClosed(t, c)

	c = frameConn(t)
	c.handler = panicHandler{closed: new(int), err: new(error)}
	c.mu.Lock()
	c.events = append(c.events, event{kind: evStreamsAvailable})
	c.mu.Unlock()
	recovered = func() (r any) {
		defer func() { r = recover() }()
		c.dispatch()
		return nil
	}()
	if recovered != "handler bug" {
		t.Fatalf("recovered %v", recovered)
	}
	checkPanicClosed(t, c)
}

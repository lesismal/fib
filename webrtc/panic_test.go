package webrtc

import (
	"net/netip"
	"testing"

	"github.com/lesismal/fib/webrtc/ice"
	"github.com/lesismal/fib/webrtc/stun"
)

// TestDatagramPanic checks that a panic while a datagram is handled, under
// the mutex or in a callback after it, lets the mutex go, closes the
// PeerConnection and goes on to fib's worker.
func TestDatagramPanic(t *testing.T) {
	from := netip.MustParseAddrPort("192.0.2.1:5000")
	var req stun.Message
	req.Build(stun.BindingRequest, stun.NewTransactionID())
	req.Add(stun.AttrUsername, []byte("x:y"))
	req.AddIntegrity([]byte("password"))
	req.AddFingerprint()
	for _, underLock := range []bool{true, false} {
		pc := newPC(t, Configuration{Mux: loopbackMux(t, startEngine(t))})
		pc.mu.Lock()
		if underLock {
			// An agent that was never made trips on the request.
			pc.agent = &ice.Agent{}
		} else {
			pc.queue(func() { panic("callback") })
		}
		pc.mu.Unlock()
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("under the lock %v: the panic did not go on", underLock)
				}
			}()
			pc.handleDatagram(from, req.Raw)
		}()
		if !pc.mu.TryLock() {
			t.Fatalf("under the lock %v: the mutex is still held", underLock)
		}
		closed, dispatching := pc.closed, pc.dispatching
		pc.mu.Unlock()
		if !closed || dispatching {
			t.Fatalf("under the lock %v: closed %v, dispatching %v", underLock, closed, dispatching)
		}
		pc.handleDatagram(from, req.Raw)
	}
}

// TestTimerPanic checks that a panic in the timer's callback, which no one
// would recover, closes the PeerConnection instead of the process.
func TestTimerPanic(t *testing.T) {
	pc := newPC(t, Configuration{Mux: loopbackMux(t, startEngine(t))})
	pc.mu.Lock()
	pc.queue(func() { panic("callback") })
	pc.mu.Unlock()
	pc.onTimer()
	if !pc.mu.TryLock() {
		t.Fatal("the mutex is still held")
	}
	closed := pc.closed
	pc.mu.Unlock()
	if !closed {
		t.Fatal("not closed")
	}
}

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package fib

import (
	"slices"
	"sync"
	"syscall"
	"testing"
)

// corkedHoldLayer is a Layer that holds what Send takes while the connection
// is corked, as the tls package does, and records the order in which the
// connection releases the cork and closes it.
type corkedHoldLayer struct {
	c *Connection

	mu         sync.Mutex
	held       []byte
	order      []string
	releaseErr error
}

// Send holds a corked send instead of passing it to the socket, so that
// nothing of it is in the connection's send queue until ReleaseCorked.
func (l *corkedHoldLayer) Send(first, second []byte) error {
	if l.c.Corked() {
		l.mu.Lock()
		l.held = append(append(l.held, first...), second...)
		l.mu.Unlock()
		return nil
	}
	data := make([]byte, 0, len(first)+len(second))
	data = append(append(data, first...), second...)
	return l.c.SendRaw(data)
}

// ReleaseCorked sends what was held through the socket, as a layer does once
// the cork comes off.
func (l *corkedHoldLayer) ReleaseCorked() {
	l.mu.Lock()
	held := l.held
	l.held = nil
	l.order = append(l.order, "release")
	l.mu.Unlock()
	if len(held) == 0 {
		return
	}
	if err := l.c.SendRaw(held); err != nil {
		l.mu.Lock()
		l.releaseErr = err
		l.mu.Unlock()
	}
}

func (l *corkedHoldLayer) CloseAfterSend() {
	l.mu.Lock()
	l.order = append(l.order, "close")
	l.mu.Unlock()
	l.c.CloseAfterSendRaw()
}

// readPeerToEOF reads from peer until the end of the stream reaches it.
func readPeerToEOF(t *testing.T, peer int) []byte {
	t.Helper()
	var got []byte
	buf := make([]byte, 256)
	for {
		n, err := syscall.Read(peer, buf)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			t.Fatalf("reading from the peer: %v; it read %q", err, got)
		}
		if n == 0 {
			return got
		}
		got = append(got, buf[:n]...)
	}
}

// CloseAfterSend has to take the cork off before it decides to close:
// otherwise what a layer took from Send while the connection was corked is
// still in the layer when the close drops the connection, and the peer never
// receives it. The bug's only coverage was a load-dependent flake in
// TestServerEarlyDataHTTP1, so this pins the contract without timing: the
// test corks the connection and sends, the layer holds the payload, and
// CloseAfterSend must release it before its layer-level close and before the
// descriptor is closed. The peer then reads the payload followed by EOF.
func TestCloseAfterSendReleasesCorkedLayer(t *testing.T) {
	const payload = "held by the cork until CloseAfterSend"
	server := newOfflineServer(t)
	c, peer := offlineSocketConnection(t, server)
	layer := &corkedHoldLayer{c: c}
	c.SetLayer(layer)

	c.Cork()
	if err := c.Send([]byte(payload)); err != nil {
		t.Fatalf("Send while corked: %v", err)
	}
	layer.mu.Lock()
	held := len(layer.held)
	layer.mu.Unlock()
	if held != len(payload) {
		t.Fatalf("the layer held %d bytes, want %d: a corked send was not taken by the layer", held, len(payload))
	}

	c.CloseAfterSend()

	// An offline connection has no event loop to release its descriptor, so
	// the test releases it as the loop's detach would: the peer then reads
	// what the close left in the socket, and then EOF.
	if fd := int(c.fd.Swap(-1)); fd >= 0 {
		_ = syscall.Close(fd)
	}

	if got := string(readPeerToEOF(t, peer)); got != payload {
		t.Fatalf("the peer read %q before EOF, want %q: CloseAfterSend closed without releasing what the corked layer held", got, payload)
	}

	layer.mu.Lock()
	order, releaseErr := slices.Clone(layer.order), layer.releaseErr
	layer.mu.Unlock()
	if releaseErr != nil {
		t.Fatalf("the layer could not send what it held: %v", releaseErr)
	}
	if want := []string{"release", "close"}; !slices.Equal(order, want) {
		t.Fatalf("the layer was called %v, want %v: the cork has to come off before the layer's close", order, want)
	}
}

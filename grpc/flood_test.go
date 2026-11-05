//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package grpc

import (
	"bytes"
	"sync/atomic"
	"testing"
)

// floodService counts the handlers that ran and, with release set, holds
// each until release closes, whatever its context says: a handler that is
// slow to see that its call was cancelled.
type floodService struct {
	ran     atomic.Int64
	release chan struct{}
}

func (f *floodService) handle(_ any, stream ServerStream) error {
	f.ran.Add(1)
	if f.release != nil {
		<-f.release
		return nil
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

// rapidReset writes n streams, each opened and reset at once, from a
// goroutine of its own, since the server may close the connection before it
// has written them all.
func rapidReset(c *wireConn, n int) {
	var b bytes.Buffer
	for i := range n {
		id := uint32(2*i + 1)
		b.Write(headersFrame(id, c.request("/flood/x"), false))
		b.Write(rawFrame(frameRSTStream, 0, id, u32(uint32(errCancel))))
	}
	go func() { _, _ = c.conn.Write(b.Bytes()) }()
}

// TestWireServerRapidReset opens streams and resets them at once (Rapid
// Reset, CVE-2023-44487). The server used to free a reset stream's slot at
// once while its handler went on running, so every one of 20,000 streams
// ran a handler whatever MaxConcurrentStreams said. Now a handler counts
// against the limit until it returns, a call opened meanwhile waiting for
// it, and resets spend a budget whose end is a GOAWAY with
// ENHANCE_YOUR_CALM.
func TestWireServerRapidReset(t *testing.T) {
	t.Run("handlers slow to stop", func(t *testing.T) {
		f := &floodService{release: make(chan struct{})}
		_, addr := startServer(t, &echoImpl{}, MaxConcurrentStreams(10), UnknownServiceHandler(f.handle))
		t.Cleanup(func() { close(f.release) })
		c := dialWire(t, addr, true)
		rapidReset(c, 20000)
		if got := c.goAway(); got != errEnhanceCalm {
			t.Fatalf("GOAWAY %d, want ENHANCE_YOUR_CALM", got)
		}
		c.closed()
		if n := f.ran.Load(); n > 10 {
			t.Fatalf("%d handlers ran past MaxConcurrentStreams(10)", n)
		}
	})
	t.Run("handlers that stop", func(t *testing.T) {
		f := &floodService{}
		_, addr := startServer(t, &echoImpl{}, UnknownServiceHandler(f.handle))
		c := dialWire(t, addr, true)
		rapidReset(c, 20000)
		if got := c.goAway(); got != errEnhanceCalm {
			t.Fatalf("GOAWAY %d, want ENHANCE_YOUR_CALM", got)
		}
		c.closed()
		// The burst, and what the bucket got back while the test ran.
		if n := f.ran.Load(); n > streamResetBurst+streamResetRate*10 {
			t.Fatalf("%d handlers ran for streams reset at once", n)
		}
	})
	t.Run("a call waits for a reset one's handler", func(t *testing.T) {
		f := &floodService{release: make(chan struct{})}
		_, addr := startServer(t, &echoImpl{}, MaxConcurrentStreams(1), UnknownServiceHandler(f.handle))
		c := dialWire(t, addr, true)
		c.write(headersFrame(1, c.request("/flood/1"), false))
		c.sync()
		c.write(rawFrame(frameRSTStream, 0, 1, u32(uint32(errCancel))),
			headersFrame(3, c.request("/flood/3"), false),
			appendPing(nil, false, []byte("syncsync")))
		// Stream 3 is within the limit the client sees, so it is not
		// refused; its handler waits for stream 1's.
		for {
			fr, err := c.read()
			if err != nil {
				t.Fatal(err)
			}
			if fr.typ == frameRSTStream && fr.streamID == 3 {
				t.Fatal("stream 3 was refused")
			}
			if fr.typ == framePing && fr.has(flagAck) {
				break
			}
		}
		if n := f.ran.Load(); n != 1 {
			t.Fatalf("%d handlers ran with MaxConcurrentStreams(1)", n)
		}
		close(f.release)
		if st := c.trailers(3); st["grpc-status"] != "0" {
			t.Fatalf("the waiting call: %v", st)
		}
		if n := f.ran.Load(); n != 2 {
			t.Fatalf("%d handlers ran", n)
		}
	})
}

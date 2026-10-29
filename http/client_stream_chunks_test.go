//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"
)

// feed hands body to s in pieces of odd sizes, as reads of a connection
// would.
func feed(s *ResponseStream, body []byte) {
	for rest := body; len(rest) > 0; {
		n := min(len(rest), 16<<10+7)
		s.Data(rest[:n])
		rest = rest[n:]
	}
}

// A response body held from its callback while it arrives waits in chunks of
// streamChunk from the pool, and reaches the callback a chunk at a time, in
// order, rather than as one buffer grown to the whole of it.
func TestResponseStreamHeldBodyWaitsInChunks(t *testing.T) {
	var largest int
	cs := newCvCliStream(0, 1<<30, false, true, nil)
	cs.onCall = func(cr *ClientResponse) {
		cr.OnBody(func(data []byte, fin bool, err error) {
			largest = max(largest, len(data))
			cs.take(data, fin, err)
		})
		cr.HoldBody(true)
	}
	cs.s.Start()
	cr := <-cs.called
	body := chunkPattern(3*streamChunk + 1234)
	feed(cs.s, body)
	cs.s.mu.Lock()
	chunks, buffered := len(cs.s.chunks), cs.s.buffered
	for _, chunk := range cs.s.chunks {
		if cap(chunk) != streamChunk {
			t.Errorf("a chunk of cap %d, want %d", cap(chunk), streamChunk)
		}
	}
	cs.s.mu.Unlock()
	if chunks != 4 || buffered != len(body) {
		t.Fatalf("%d bytes waiting in %d chunks, want %d in 4", buffered, chunks, len(body))
	}
	cs.s.End()
	cr.HoldBody(false)
	select {
	case err := <-cs.fin:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the body never ended")
	}
	if !bytes.Equal(cs.data, body) {
		t.Fatalf("delivered %d bytes, not the %d that arrived", len(cs.data), len(body))
	}
	if largest > streamChunk {
		t.Fatalf("a piece of %d bytes, more than a chunk", largest)
	}
	cs.s.mu.Lock()
	defer cs.s.mu.Unlock()
	if len(cs.s.chunks) != 0 || cs.s.buffered != 0 {
		t.Fatalf("still holding %d chunks, %d bytes", len(cs.s.chunks), cs.s.buffered)
	}
}

// Body reads move through the chunks in order, across their edges, and give
// each back once it is read.
func TestResponseStreamReadAcrossChunks(t *testing.T) {
	cs := newCvCliStream(0, 1<<30, false, true, func(cr *ClientResponse) { _, _ = cr.Body.Read(nil) })
	cs.s.Start()
	cr := <-cs.called
	body := chunkPattern(3*streamChunk + 1234)
	feed(cs.s, body)
	cs.s.End()
	var got []byte
	p := make([]byte, 100000)
	for {
		n, err := cr.Body.Read(p)
		got = append(got, p[:n]...)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			break
		}
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("read %d bytes, not the %d that arrived", len(got), len(body))
	}
	if len(cs.s.chunks) != 0 || cs.s.buffered != 0 {
		t.Fatalf("still holding %d chunks, %d bytes", len(cs.s.chunks), cs.s.buffered)
	}
}

// A body that ends before the threshold arrives whole in a buffer of its
// size, put together from its chunks, after a read of part of it.
func TestResponseStreamWholeFromChunks(t *testing.T) {
	cs := newCvCliStream(1<<30, 0, false, true, nil)
	cs.s.Start()
	body := chunkPattern(2*streamChunk + 99)
	feed(cs.s, body)
	cs.s.End()
	if cs.whole == nil {
		t.Fatal("not delivered whole")
	}
	b := cs.whole.Body.(*bufferedBody)
	if !bytes.Equal(b.data, body) || cap(b.data) != len(body) {
		t.Fatalf("whole body of %d bytes (cap %d), want the %d that arrived", len(b.data), cap(b.data), len(body))
	}
	if len(cs.s.chunks) != 0 || cs.s.buffered != 0 {
		t.Fatalf("still holding %d chunks, %d bytes", len(cs.s.chunks), cs.s.buffered)
	}
}

// Once the pool is warm, a body that waits for its reader takes no buffer of
// its own.
func TestResponseStreamBuffersFromPool(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts differ under the race detector")
	}
	cs := newCvCliStream(0, 1<<30, false, true, func(cr *ClientResponse) { _, _ = cr.Body.Read(nil) })
	cs.s.Start()
	cr := <-cs.called
	piece := chunkPattern(16 << 10)
	p := make([]byte, 64<<10)
	round := func() {
		for range 4 {
			cs.s.Data(piece)
		}
		for n := 0; n < 4*len(piece); {
			k, err := cr.Body.Read(p)
			if err != nil {
				t.Fatal(err)
			}
			n += k
		}
	}
	round()
	if allocs := testing.AllocsPerRun(100, round); allocs != 0 {
		t.Fatalf("%.1f allocations a round, want none once the pool is warm", allocs)
	}
}

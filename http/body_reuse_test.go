package http

import (
	"bytes"
	"math"
	"testing"
)

// What arrives while the callback is busy is buffered, and the buffers are
// the stream's own again once the callback is done with them: in a steady
// stream, handing the body on takes no buffer of its own and copies each
// piece once.
func TestBodyStreamReusesBuffersWhileSinkBusy(t *testing.T) {
	s := &BodyStream{remaining: -1, limit: -1, highWater: math.MaxInt}
	piece := make([]byte, 16<<10)
	for i := range piece {
		piece[i] = byte(i)
	}
	sink := &busySink{s: s, piece: piece, got: new(bytes.Buffer)}
	s.sink = sink.call
	round := func() {
		sink.more = 4
		sink.got.Reset()
		s.push(piece)
	}
	round()
	if want := bytes.Repeat(piece, 5); !bytes.Equal(sink.got.Bytes(), want) {
		t.Fatalf("handed on %d bytes, want the 5 pieces in order (%d)", sink.got.Len(), len(want))
	}
	if allocs := testing.AllocsPerRun(100, round); allocs != 0 {
		t.Fatalf("%.1f allocations a round, want none once the buffers are warm", allocs)
	}

	// The body's end gives its buffers back to the pool.
	s.finish(nil)
	s.mu.Lock()
	buf, spare := s.buf, s.spare
	s.mu.Unlock()
	if buf != nil || spare != nil {
		t.Fatalf("after the end: buf cap %d, spare cap %d, want both returned", cap(buf), cap(spare))
	}
}

// A reader that has read the whole of an ended body leaves no buffer behind.
func TestBodyStreamReadToEndReturnsBuffer(t *testing.T) {
	s := &BodyStream{remaining: -1, limit: -1, highWater: math.MaxInt}
	s.push([]byte("hello"))
	s.finish(nil)
	p := make([]byte, 16)
	if n, err := s.Read(p); n != 5 || err != nil || string(p[:n]) != "hello" {
		t.Fatalf("Read: %d %v %q", n, err, p[:n])
	}
	if s.buf != nil {
		t.Fatalf("buffer of cap %d kept after the body was read to its end", cap(s.buf))
	}
}

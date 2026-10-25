package http

import (
	"bytes"
	"math"
	"testing"
)

// busySink is a body's callback during whose calls more of the body arrives,
// as it does on a multiplexed connection while the handler pool hands the
// body on: each call lets `more` more pieces arrive, and got, if set,
// collects what it is handed.
type busySink struct {
	s     *BodyStream
	piece []byte
	more  int
	got   *bytes.Buffer
}

func (b *busySink) call(data []byte, fin bool, err error, worker bool) {
	if b.got != nil {
		b.got.Write(data)
	}
	if b.more > 0 {
		b.more--
		b.s.push(b.piece)
	}
}

// BenchmarkBodyStreamBusySink hands a 1 MiB body on, 16 KiB at a time, to a
// callback during each of whose calls another piece arrives, as an HTTP/2
// upload's do while the handler pool hands it on.
func BenchmarkBodyStreamBusySink(b *testing.B) {
	piece := make([]byte, 16<<10)
	b.SetBytes(1 << 20)
	b.ReportAllocs()
	for b.Loop() {
		s := &BodyStream{remaining: -1, limit: -1, highWater: math.MaxInt}
		sink := &busySink{s: s, piece: piece, more: 63}
		s.sink = sink.call
		s.push(piece)
		s.finish(nil)
	}
}

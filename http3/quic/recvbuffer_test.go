package quic

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

// checkSegments fails the test unless r's segments are ascending, disjoint,
// past what was delivered, and add up to buffered.
func checkSegments(t *testing.T, r *recvBuffer) {
	t.Helper()
	at, n := r.offset, 0
	for _, s := range r.segments {
		if s.off < at || len(s.data) == 0 {
			t.Fatalf("segment at %d of %d bytes after %d", s.off, len(s.data), at)
		}
		at = s.end()
		n += len(s.data)
	}
	if n != r.buffered {
		t.Fatalf("segments hold %d bytes, buffered says %d", n, r.buffered)
	}
}

// A frame sent again and again ahead of a gap is held once: a peer once
// made a 1 MiB window buffer 5 MB with 5000 copies of one frame.
func TestRecvBufferHoldsRepeatsOnce(t *testing.T) {
	var r recvBuffer
	data := bytes.Repeat([]byte("x"), 1000)
	for range 5000 {
		if !r.push(100, data, minRecvSegments) {
			t.Fatal("refused a repeat")
		}
	}
	if len(r.segments) != 1 || r.buffered != len(data) {
		t.Fatalf("%d segments of %d bytes", len(r.segments), r.buffered)
	}
	// Ever larger frames over the same bytes, and smaller ones inside
	// them, hold no more than what they cover.
	big := bytes.Repeat(data, 2)
	for n := 1; n <= 1000; n++ {
		r.push(uint64(1100-n), big[:2*n], minRecvSegments)
		r.push(uint64(1100+n/2), data[:n/2], minRecvSegments)
	}
	checkSegments(t, &r)
	if len(r.segments) != 1 || r.buffered != len(big) {
		t.Fatalf("%d segments of %d bytes", len(r.segments), r.buffered)
	}
}

// Whatever order, overlap and repetition the pieces of a stream arrive in,
// it comes out whole and in order, and no byte is held twice.
func TestRecvBufferReassemblesAnyOrder(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for round := range 200 {
		src := make([]byte, 1+rng.IntN(4000))
		for i := range src {
			src[i] = byte(rng.Uint32())
		}
		var r recvBuffer
		var got []byte
		for len(got) < len(src) {
			off := rng.IntN(len(src))
			end := min(off+1+rng.IntN(300), len(src))
			chunk, ok := r.take(uint64(off), src[off:end], minRecvSegments)
			if !ok {
				t.Fatalf("round %d: refused", round)
			}
			got = append(got, chunk...)
			for d := r.pop(); d != nil; d = r.pop() {
				got = append(got, d...)
			}
			checkSegments(t, &r)
			if r.buffered > len(src)-len(got) {
				t.Fatalf("round %d: %d bytes held with %d to come", round, r.buffered, len(src)-len(got))
			}
		}
		if !bytes.Equal(got, src) {
			t.Fatalf("round %d: stream came out wrong", round)
		}
	}
}

// Pieces that continue one another make one segment however small they are,
// but a peer cutting its data into ever more pieces with gaps between, or
// sending them backwards, is refused at the limit.
func TestRecvBufferSegmentLimit(t *testing.T) {
	var r recvBuffer
	for i := range 4096 {
		if !r.push(uint64(1+i), []byte{'x'}, minRecvSegments) {
			t.Fatal("refused a piece that continues the last")
		}
	}
	if len(r.segments) != 1 || r.buffered != 4096 {
		t.Fatalf("%d segments of %d bytes", len(r.segments), r.buffered)
	}
	r = recvBuffer{}
	for i := range minRecvSegments {
		if !r.push(uint64(2*(minRecvSegments-i)), []byte{'x'}, minRecvSegments) {
			t.Fatalf("refused piece %d", i)
		}
	}
	if r.push(1, []byte{'x'}, minRecvSegments) || r.buffered != minRecvSegments {
		t.Fatalf("took a piece past the limit, %d bytes held", r.buffered)
	}
	// A piece that fills a gap, or covers segments, is still taken.
	if !r.push(3, []byte{'x'}, minRecvSegments) || !r.push(9, []byte("xxxxxxxxx"), minRecvSegments) {
		t.Fatal("refused a piece that makes no more segments")
	}
	checkSegments(t, &r)
	if recvSegmentLimit(1<<20) != 2048 || recvSegmentLimit(64<<10) != minRecvSegments {
		t.Fatalf("limits %d, %d", recvSegmentLimit(1<<20), recvSegmentLimit(64<<10))
	}
}

// Over a connection: a STREAM frame repeated ahead of a gap is held once,
// a stream cut into a byte here and a byte there is a flood, and CRYPTO
// data repeated ahead of a gap is held once too.
func TestStreamRepeatsHeldOnce(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	data := bytes.Repeat([]byte("y"), 1000)
	for range 5000 {
		if _, err := c.handleFrames(spaceApp, frame(frameStream|0x06, 0, 1, len(data), data), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if s := c.streamByID(0); s == nil || s.recv.buffered != len(data) || len(s.recv.segments) != 1 {
		t.Fatalf("stream holds %d bytes in %d segments", s.recv.buffered, len(s.recv.segments))
	}
	var err error
	for i := 0; err == nil && i < 4096; i++ {
		_, err = c.handleFrames(spaceApp, frame(frameStream|0x06, 4, 2*(4096-i), 1, "z"), time.Now())
	}
	var te *TransportError
	if !errors.As(err, &te) || te.Code != errProtocolViolation || !strings.Contains(te.Reason, "pieces") {
		t.Fatalf("closed with %v, want PROTOCOL_VIOLATION for the pieces", err)
	}

	for range 5000 {
		if err := c.feedCrypto(spaceApp, 1, data); err != nil {
			t.Fatal(err)
		}
	}
	if r := &c.spaces[spaceApp].cryptoRecv; r.buffered != len(data) || len(r.segments) != 1 {
		t.Fatalf("crypto holds %d bytes in %d segments", r.buffered, len(r.segments))
	}
}

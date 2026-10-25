package quic

import (
	"bytes"
	"testing"

	"github.com/lesismal/fib/bufferpool"
)

// sealPayload builds a packet in space holding payload, as nothing the
// connection writes would build it, for tests that need one.
func sealPayload(c *Conn, space, pnLen int, pn uint64, payload []byte) []byte {
	at := c.headerLen(space, pnLen, false)
	out := make([]byte, at, at+len(payload)+aeadOverhead)
	out = append(out, payload...)
	plan := packetPlan{space: space, pn: pn, pnLen: pnLen, payload: out[at:], sp: &sentPacket{}}
	return c.sealPlan(out, &plan)
}

// A message written as several parts arrives as one, in order, whether the
// parts are the pool's or the caller's, and whether the stream ends with it.
func TestStreamWriteParts(t *testing.T) {
	p := newTestPair(t, 0, nil)
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	head := bufferpool.Get(8)[:0]
	head = append(head, "head"...)
	tail := bufferpool.Get(8)[:0]
	tail = append(tail, "tail"...)
	// The body is the caller's, which the stream keeps as it is rather than
	// copying: it is still these bytes when it is sent.
	body := bytes.Repeat([]byte("body"), 1024)
	parts := []Part{
		{Data: head, Owned: true},
		{Data: body},
		{Data: tail, Owned: true},
	}
	if err := s.WriteFinalParts(parts, false); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, p.serverH, s.ID())
	want := append(append(append([]byte(nil), "head"...), body...), "tail"...)
	if got := p.serverH.received(s.ID()); !bytes.Equal(got, want) {
		t.Fatalf("received %d bytes, want %d", len(got), len(want))
	}
}

// The parts of a message are framed from the run each one is, so a loss that
// takes only part of one is sent again from where it was lost, and the whole
// message still arrives.
func TestStreamWritePartsLoss(t *testing.T) {
	p := newTestPair(t, 0.2, nil)
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	head := bufferpool.Get(8)[:0]
	head = append(head, "head"...)
	body := bytes.Repeat([]byte("body"), 4096)
	want := append(append([]byte(nil), "head"...), body...)
	if err := s.WriteFinalParts([]Part{{Data: head, Owned: true}, {Data: body}}, false); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, p.serverH, s.ID())
	if got := p.serverH.received(s.ID()); !bytes.Equal(got, want) {
		t.Fatalf("received %d bytes, want %d", len(got), len(want))
	}
}

// A send buffer made of runs hands out each run's bytes only, so that a frame
// is built from one run and never from two joined; an acknowledgement of part
// of a run cuts it, and of all of it drops the run.
func TestSendBufferParts(t *testing.T) {
	var b sendBuffer
	owned := bufferpool.Get(4)[:4]
	copy(owned, "abcd")
	b.writeParts([]Part{{Data: owned, Owned: true}, {Data: []byte("efgh")}})
	if b.end() != 8 {
		t.Fatalf("end %d", b.end())
	}
	if off, data := b.popNew(6); off != 0 || string(data) != "abcd" {
		t.Fatalf("%d %q from the first run", off, data)
	}
	if off, data := b.popNew(6); off != 4 || string(data) != "efgh" {
		t.Fatalf("%d %q from the second run", off, data)
	}
	if off, data := b.popNew(1); off != 8 || data != nil {
		t.Fatalf("%d %q past the end", off, data)
	}
	// A lost range that starts in the first run and ends in the second is
	// sent again a run at a time.
	b.onLost(2, 4)
	if off, data := b.popLost(8); off != 2 || string(data) != "cd" {
		t.Fatalf("%d %q from the first run", off, data)
	}
	if off, data := b.popLost(8); off != 4 || string(data) != "ef" {
		t.Fatalf("%d %q from the second run", off, data)
	}
	if b.hasLost() {
		t.Fatal("the lost range is still queued")
	}
	// Acknowledged in part, the first run holds what is left of it.
	b.onAck(0, 3)
	if b.end() != 8 || b.base != 3 || len(b.pieces) != 2 {
		t.Fatalf("base %d end %d, %d runs", b.base, b.end(), len(b.pieces))
	}
	if off, data := b.popLost(0); off != 0 || data != nil {
		t.Fatalf("%d %q with no room", off, data)
	}
	b.onAck(3, 5)
	if !b.allAcked() || b.end() != 8 || b.base != 8 {
		t.Fatalf("base %d end %d after the acknowledgement", b.base, b.end())
	}
}

// A stream that ends before it takes a message gives the pool's parts back
// and keeps none of the caller's.
func TestStreamWritePartsClosed(t *testing.T) {
	p := newTestPair(t, 0, nil)
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	head := bufferpool.Get(8)[:0]
	head = append(head, "head"...)
	if err := s.WriteFinalParts([]Part{{Data: head, Owned: true}, {Data: []byte("body")}}, false); err != ErrClosed {
		t.Fatalf("write to a closed stream: %v", err)
	}
	s.conn.mu.Lock()
	kept := len(s.send.pieces)
	s.conn.mu.Unlock()
	if kept != 0 {
		t.Fatalf("%d runs kept by a closed stream", kept)
	}
}

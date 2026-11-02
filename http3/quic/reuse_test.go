package quic

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/bufferpool"
)

// A server that recycles each stream once it has answered it gets streams
// back to reuse, and what it sends on a reused stream is that stream's alone:
// acknowledgements and losses of what an earlier stream in the same memory
// sent, which arrive late on a path that loses and delays datagrams, reach
// neither the stream that reuses it nor its answer. Every answer is checked
// whole by the client, under loss in both directions, with streams of
// several sizes, some of them reset by the server before they are answered,
// and some recycled before the client has finished sending.
func TestRecycledStreamsReused(t *testing.T) {
	for _, loss := range []float64{0, 0.1} {
		t.Run(fmt.Sprintf("loss %.0f%%", loss*100), func(t *testing.T) {
			testRecycledStreams(t, loss)
		})
	}
}

func testRecycledStreams(t *testing.T, loss float64) {
	p := newTestPair(t, loss, func(_, server *Config) { server.MaxIncomingStreams = 25 })
	var reused atomic.Int64
	var mu sync.Mutex
	seen := make(map[*Stream]bool)
	p.serverH.onData = func(s *Stream, data []byte, fin bool) {
		switch s.Context {
		case nil:
			s.Context = "open"
			mu.Lock()
			if seen[s] {
				reused.Add(1)
			}
			seen[s] = true
			mu.Unlock()
		case "open":
		default:
			return
		}
		id := s.ID()
		switch {
		case id%7 == 3:
			// Reset rather than answered, and recycled at once, the
			// rest of the request still on its way.
			s.Context = "reset"
			s.ResetForPeerError(0x10)
			s.Recycle()
			return
		case !fin:
			return
		}
		s.Context = "answered"
		answer := bytes.Repeat([]byte(fmt.Sprintf("%d,", id)), int(id%5)*800+1)
		_ = s.WriteFinal(bufferpool.Append(nil, answer), false)
		s.Recycle()
	}
	const streams = 600
	type pending struct {
		id   uint64
		want []byte
	}
	var inflight []pending
	finish := func(q pending) {
		if q.id%7 == 3 {
			// The client hears the reset rather than an answer.
			return
		}
		waitFinished(t, p.clientH, q.id)
		if got := p.clientH.received(q.id); !bytes.Equal(got, q.want) {
			t.Fatalf("stream %d: got %d bytes, want %d", q.id, len(got), len(q.want))
		}
	}
	for opened := 0; opened < streams; {
		s, err := p.client.OpenStream()
		if err == ErrStreamLimit {
			if len(inflight) > 0 {
				finish(inflight[0])
				inflight = inflight[1:]
			} else {
				time.Sleep(time.Millisecond)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		opened++
		id := s.ID()
		// Some requests come in two parts, so that the server sees the
		// first without its end.
		if id%3 == 0 {
			_ = s.Write([]byte("first part "), false)
		}
		_ = s.Write(bytes.Repeat([]byte("q"), int(id%4)*700+5), true)
		inflight = append(inflight, pending{id: id, want: bytes.Repeat([]byte(fmt.Sprintf("%d,", id)), int(id%5)*800+1)})
	}
	for _, q := range inflight {
		finish(q)
	}
	if reused.Load() == 0 {
		t.Fatal("no stream was reused")
	}
	t.Logf("%d of %d streams were reused ones", reused.Load(), streams)
	// What the server holds for its recycled streams, once everything sent
	// has been acknowledged, is a free list of clean ones.
	server := p.serverConn(t)
	waitFor(t, "the server's streams to finish", func() bool {
		server.mu.Lock()
		defer server.mu.Unlock()
		open := 0
		server.eachStream(func(*Stream) bool { open++; return true })
		return open == 0
	})
	server.mu.Lock()
	defer server.mu.Unlock()
	for _, s := range server.freeStreams {
		if s.conn != nil || s.sentRefs != 0 || s.recycled || s.reusable {
			t.Fatalf("a free stream is not clean: %+v", s)
		}
	}
}

// A recycled stream is taken back only once no record of a sent packet
// refers to it, and the stream queue no longer holds it: an acknowledgement
// or a loss of one of its frames, however late, finds the stream it was
// sent on rather than the one reusing its memory.
func TestStreamReuseWaitsForSentRecords(t *testing.T) {
	c := &Conn{}
	s := &Stream{conn: c, done: true, recycled: true}
	var p1, p2 sentPacket
	p1.frames, p2.frames = p1.inline[:0], p2.inline[:0]
	p1.add(sentFrame{kind: sfStream, stream: s, n: 10})
	p1.add(sentFrame{kind: sfStream, stream: s, off: 10, n: 10, fin: true})
	p2.add(sentFrame{kind: sfMaxStreamData, stream: s})
	if s.sentRefs != 3 {
		t.Fatalf("sentRefs %d, want 3", s.sentRefs)
	}
	c.queueReuseLocked(s)
	if len(c.events) != 1 || c.events[0].kind != evReuse {
		t.Fatalf("events %+v, want an evReuse", c.events)
	}
	// What dispatch does once evReuse has come up.
	s.reusable = true
	c.reuseLocked(s)
	if len(c.freeStreams) != 0 {
		t.Fatal("a stream sent records refer to was taken back")
	}
	c.releaseSent(&p1)
	if len(c.freeStreams) != 0 || s.sentRefs != 1 {
		t.Fatalf("taken back with %d refs left", s.sentRefs)
	}
	s.queued = true
	c.releaseSent(&p2)
	if len(c.freeStreams) != 0 {
		t.Fatal("a stream still in the send queue was taken back")
	}
	// The queue drops a finished stream the next time it is looked at.
	c.sendQueue = []*Stream{s}
	c.appendStreams(nil, 1200, &sentPacket{})
	if len(c.freeStreams) != 1 || c.freeStreams[0] != s {
		t.Fatal("the stream was not taken back")
	}
	if s.conn != nil || s.done || s.recycled || s.reusable || s.sentRefs != 0 {
		t.Fatalf("the stream taken back is not clean: %+v", s)
	}
	// Taken back once only.
	c.reuseLocked(s)
	if len(c.freeStreams) != 1 {
		t.Fatal("the stream was taken back twice")
	}
	c.streams = make(map[uint64]*Stream)
	if got := c.newStream(4); got != s || got.conn != c || got.id != 4 || got.recvFin != -1 {
		t.Fatal("a new stream did not reuse the free one")
	}
}

// A stream that is finished but not recycled, or recycled but not finished,
// is not queued for reuse, nor one whose connection holds early calls back.
func TestStreamReuseNeedsBoth(t *testing.T) {
	c := &Conn{}
	for _, s := range []*Stream{
		{conn: c, done: true},
		{conn: c, recycled: true},
	} {
		c.queueReuseLocked(s)
	}
	c.heldEarly = []event{{kind: evStreamData}}
	c.queueReuseLocked(&Stream{conn: c, done: true, recycled: true})
	if len(c.events) != 0 {
		t.Fatalf("events %+v", c.events)
	}
}

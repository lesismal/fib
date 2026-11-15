package quic

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// A sender held back by flow control says so with a BLOCKED frame for each
// limit it reaches: the server, which reads none of what it was sent, hears
// of the stream's limit, of the connection's, and of its limit on streams.
func TestBlockedFramesSent(t *testing.T) {
	// held opens a stream and sends its first byte alone, and returns once
	// the server holds the stream's credit back, before the rest is sent.
	// What is delivered before HoldCredit counts as consumed, and data that
	// reaches the server in one batch is all delivered before the handler
	// runs, so a busy runner handing it the bulk at once would otherwise
	// have it give the credit back.
	held := func(t *testing.T, p *testPair) *Stream {
		t.Helper()
		p.serverH.onData = func(s *Stream, data []byte, fin bool) { s.HoldCredit() }
		server := p.serverConn(t)
		s, err := p.client.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Write([]byte{1}, false); err != nil {
			t.Fatal(err)
		}
		waitFor(t, "credit held", func() bool {
			server.mu.Lock()
			defer server.mu.Unlock()
			st := server.streamByID(s.ID())
			return st != nil && st.holdCredit
		})
		return s
	}
	finish := func(t *testing.T, s *Stream, n int) {
		t.Helper()
		if err := s.Write(make([]byte, n-1), true); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("stream", func(t *testing.T) {
		p := newTestPair(t, 0, func(_, server *Config) { server.StreamReceiveWindow = 4096 })
		s := held(t, p)
		finish(t, s, 10000)
		server := p.serverConn(t)
		waitFor(t, "STREAM_DATA_BLOCKED", func() bool {
			server.mu.Lock()
			defer server.mu.Unlock()
			st := server.streamByID(s.ID())
			return st != nil && st.blockedAt == 4096
		})
	})
	t.Run("connection", func(t *testing.T) {
		p := newTestPair(t, 0, func(_, server *Config) { server.ConnReceiveWindow = 6000 })
		var ss []*Stream
		for i := 0; i < 3; i++ {
			ss = append(ss, held(t, p))
		}
		for _, s := range ss {
			finish(t, s, 3000)
		}
		server := p.serverConn(t)
		waitFor(t, "DATA_BLOCKED", func() bool {
			server.mu.Lock()
			defer server.mu.Unlock()
			return server.dataBlockedAt == 6000
		})
	})
	t.Run("connection spent before", func(t *testing.T) {
		// One stream spends the connection's credit to the byte, and all of
		// it is acknowledged, so nothing is left to send but what the next
		// stream writes: the round that finds the limit for it has the
		// frame to send and nothing else.
		p := newTestPair(t, 0, func(_, server *Config) { server.ConnReceiveWindow = 6000 })
		s := held(t, p)
		// The rest reaches the server in one batch, as on a busy runner.
		p.gather.Store(3)
		finish(t, s, 6000)
		server := p.serverConn(t)
		idle := func(c *Conn) bool {
			c.mu.Lock()
			defer c.mu.Unlock()
			return c.cc.bytesInFlight == 0 && !c.sending && len(c.sendQueue) == 0
		}
		waitFor(t, "credit spent and acknowledged", func() bool {
			p.client.mu.Lock()
			spent := p.client.sentData == 6000
			p.client.mu.Unlock()
			return spent && idle(p.client) && idle(server)
		})
		s, err := p.client.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Write([]byte{1}, true); err != nil {
			t.Fatal(err)
		}
		blocked := func() bool {
			server.mu.Lock()
			defer server.mu.Unlock()
			return server.dataBlockedAt == 6000
		}
		for deadline := time.Now().Add(5 * time.Second); !blocked(); time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				c := p.client
				c.mu.Lock()
				cs := fmt.Sprintf("client sent %d of %d, report %+v, queue %d, sending %v, in flight %d",
					c.sentData, c.peerMaxData, c.dataBlocked, len(c.sendQueue), c.sending, c.cc.bytesInFlight)
				c.mu.Unlock()
				server.mu.Lock()
				ss := fmt.Sprintf("server got %d, gives %d, consumed %d, heard %d",
					server.recvData, server.recvMaxData, server.recvConsumed, server.dataBlockedAt)
				server.mu.Unlock()
				t.Fatalf("no DATA_BLOCKED: %s; %s", cs, ss)
			}
		}
	})
	t.Run("streams", func(t *testing.T) {
		p := newTestPair(t, 0, func(_, server *Config) { server.MaxIncomingStreams = 2 })
		for i := 0; i < 2; i++ {
			if _, err := p.client.OpenStream(); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 3; i++ {
			// Asking again for the same limit reports it once.
			if _, err := p.client.OpenStream(); !errors.Is(err, ErrStreamLimit) {
				t.Fatalf("third stream: %v", err)
			}
		}
		server := p.serverConn(t)
		waitFor(t, "STREAMS_BLOCKED", func() bool {
			server.mu.Lock()
			defer server.mu.Unlock()
			return server.streamsBlockedAt[0] == 2
		})
		p.client.mu.Lock()
		r := p.client.streamsBlocked[0]
		p.client.mu.Unlock()
		if !r.reported || r.at != 2 || r.pending {
			t.Fatalf("client's report %+v", r)
		}
	})
}

// A BLOCKED frame that is lost goes again only while its limit holds.
func TestBlockedFrameLost(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dataBlocked.block(c.peerMaxData)
	sp := &sentPacket{}
	c.appendBlocked(nil, 100, sp)
	if c.dataBlocked.pending || len(sp.frames) != 1 {
		t.Fatalf("not sent: %+v", sp.frames)
	}
	c.frameLost(spaceApp, &sp.frames[0])
	if !c.dataBlocked.pending {
		t.Fatal("lost DATA_BLOCKED not sent again")
	}
	c.dataBlocked.pending = false
	c.peerMaxData += 100
	c.frameLost(spaceApp, &sp.frames[0])
	if c.dataBlocked.pending {
		t.Fatal("lost DATA_BLOCKED sent again for a limit since raised")
	}
	// The same limit is reported once.
	c.dataBlocked.block(c.peerMaxData)
	c.dataBlocked.pending = false
	c.dataBlocked.block(c.peerMaxData)
	if c.dataBlocked.pending {
		t.Fatal("a limit reported twice")
	}
}

// A BLOCKED frame does not go out for a limit the peer has since raised.
func TestBlockedFrameDroppedWhenLimitRises(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dataBlocked.block(c.peerMaxData)
	c.peerMaxData += 1000
	if b := c.appendBlocked(nil, 100, &sentPacket{}); len(b) != 0 || c.dataBlocked.pending {
		t.Fatalf("DATA_BLOCKED %x for a limit since raised", b)
	}
	c.streamsBlocked[0].block(c.peerMaxBidi)
	c.peerMaxBidi++
	if b := c.appendBlocked(nil, 100, &sentPacket{}); len(b) != 0 || c.streamsBlocked[0].pending {
		t.Fatalf("STREAMS_BLOCKED %x for a limit since raised", b)
	}
}

package quic

import (
	"errors"
	"testing"
)

// A sender held back by flow control says so with a BLOCKED frame for each
// limit it reaches: the server, which reads none of what it was sent, hears
// of the stream's limit, of the connection's, and of its limit on streams.
func TestBlockedFramesSent(t *testing.T) {
	hold := func(s *Stream, data []byte, fin bool) { s.HoldCredit() }
	t.Run("stream", func(t *testing.T) {
		p := newTestPair(t, 0, func(_, server *Config) { server.StreamReceiveWindow = 4096 })
		p.serverH.onData = hold
		s, err := p.client.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Write(make([]byte, 10000), true); err != nil {
			t.Fatal(err)
		}
		server := p.serverConn(t)
		waitFor(t, "STREAM_DATA_BLOCKED", func() bool {
			server.mu.Lock()
			defer server.mu.Unlock()
			st := server.streams[s.ID()]
			return st != nil && st.blockedAt == 4096
		})
	})
	t.Run("connection", func(t *testing.T) {
		p := newTestPair(t, 0, func(_, server *Config) { server.ConnReceiveWindow = 6000 })
		p.serverH.onData = hold
		for i := 0; i < 3; i++ {
			s, err := p.client.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Write(make([]byte, 3000), true); err != nil {
				t.Fatal(err)
			}
		}
		server := p.serverConn(t)
		waitFor(t, "DATA_BLOCKED", func() bool {
			server.mu.Lock()
			defer server.mu.Unlock()
			return server.dataBlockedAt == 6000
		})
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

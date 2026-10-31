package sctp

import (
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"
)

// rawPacket is a packet of chunks under vtag, with its checksum.
func rawPacket(vtag uint32, chunks ...[]byte) []byte {
	p := make([]byte, headerSize)
	binary.BigEndian.PutUint16(p, DefaultPort)
	binary.BigEndian.PutUint16(p[2:], DefaultPort)
	binary.BigEndian.PutUint32(p[4:], vtag)
	for _, c := range chunks {
		p = append(p, c...)
	}
	binary.LittleEndian.PutUint32(p[8:], checksum(p))
	return p
}

// rawChunk is a chunk of typ with value v, padded.
func rawChunk(typ, flags byte, v ...byte) []byte {
	c := []byte{typ, flags, 0, 0}
	binary.BigEndian.PutUint16(c[2:], uint16(4+len(v)))
	c = append(c, v...)
	for len(c)%4 != 0 {
		c = append(c, 0)
	}
	return c
}

// rawData is a DATA chunk carrying data whole.
func rawData(tsn uint32, sid, ssn uint16, ppid uint32, flags byte, data []byte) []byte {
	v := make([]byte, 12, 12+len(data))
	binary.BigEndian.PutUint32(v, tsn)
	binary.BigEndian.PutUint16(v[4:], sid)
	binary.BigEndian.PutUint16(v[6:], ssn)
	binary.BigEndian.PutUint32(v[8:], ppid)
	return rawChunk(chunkData, flags, append(v, data...)...)
}

// rawResetRequest is a RE-CONFIG chunk asking that the streams ids, or all
// of them, be reset after lastTSN.
func rawResetRequest(seq, lastTSN uint32, ids ...uint16) []byte {
	v := make([]byte, 16, 16+2*len(ids))
	binary.BigEndian.PutUint16(v, paramOutgoingReset)
	binary.BigEndian.PutUint16(v[2:], uint16(16+2*len(ids)))
	binary.BigEndian.PutUint32(v[4:], seq)
	binary.BigEndian.PutUint32(v[12:], lastTSN)
	for _, id := range ids {
		v = binary.BigEndian.AppendUint16(v, id)
	}
	return rawChunk(chunkReconfig, 0, v...)
}

func be32(n uint32) []byte { return binary.BigEndian.AppendUint32(nil, n) }

// TestConfigLimits checks that a configuration too small to work with is
// raised to what works: an MTU of 256 and a window of four packets.
func TestConfigLimits(t *testing.T) {
	a := NewAssociation(Config{MTU: 100, ReceiveWindow: 10})
	if a.mtu != 256 || a.cfg.ReceiveWindow != 4*256 {
		t.Fatalf("MTU %d, window %d", a.mtu, a.cfg.ReceiveWindow)
	}
	// Without Output it sends nothing, and goes on all the same.
	a.Connect(time.Now())
	if a.State() != StateCookieWait {
		t.Fatalf("state %v", a.State())
	}
	sent := 0
	b := NewAssociation(Config{Output: func([]byte) { sent++ }})
	b.Connect(time.Now())
	b.Connect(time.Now())
	if sent != 1 {
		t.Fatalf("Connect twice sent %d INITs", sent)
	}
}

// TestCallbacks checks OnEstablished, and that a callback may send: what it
// sends goes out as the call that ran it returns.
func TestCallbacks(t *testing.T) {
	var established [2]int
	var echoed []byte
	var l *testLink
	l = newLink(t, 1, func(i int, c *Config) {
		c.OnEstablished = func() { established[i]++ }
		c.OnMessage = func(sid uint16, ppid uint32, d []byte) {
			if i == 1 {
				if err := l.ends[1].Send(sid, ppid, append([]byte("echo:"), d...), SendOptions{}); err != nil {
					t.Error(err)
				}
				return
			}
			echoed = append([]byte(nil), d...)
		}
	})
	l.connect(true)
	if established != [2]int{1, 1} {
		t.Fatalf("OnEstablished ran %v times", established)
	}
	_ = l.ends[0].Send(0, PPIDBinary, []byte("ping"), SendOptions{})
	l.run(time.Second, func() bool { return echoed != nil })
	if string(echoed) != "echo:ping" {
		t.Fatalf("echo %q", echoed)
	}
}

// TestClosedAssociation checks that an association shut down before it was
// established is closed, and that a closed one does nothing more.
func TestClosedAssociation(t *testing.T) {
	l := newLink(t, 1, nil)
	a := l.ends[0]
	a.Connect(l.now)
	a.Shutdown()
	if a.State() != StateClosed || !l.closed[0] || l.closeEr[0] != nil {
		t.Fatalf("state %v, closed %v, %v", a.State(), l.closed[0], l.closeEr[0])
	}
	sent := l.sent[0]
	a.Shutdown()
	a.HandleTimeout(l.now.Add(time.Hour))
	if err := a.HandlePacket(rawPacket(a.localTag, rawChunk(chunkHeartbeat, 0, 0, 1, 0, 4)), l.now); !errors.Is(err, ErrClosed) {
		t.Fatalf("HandlePacket: %v", err)
	}
	if err := a.ResetStream(0); !errors.Is(err, ErrClosed) {
		t.Fatalf("ResetStream: %v", err)
	}
	if _, err := a.OpenChannel(ChannelOptions{Label: "x"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("OpenChannel: %v", err)
	}
	if l.sent[0] != sent {
		t.Fatalf("a closed association sent %d packets", l.sent[0]-sent)
	}
}

// TestStaleCookie checks that a COOKIE ECHO that comes back after the
// cookie's lifetime is refused.
func TestStaleCookie(t *testing.T) {
	l := newLink(t, 1, nil)
	var held []byte
	l.drop = func(from int, p []byte) bool {
		if from == 0 && p[headerSize] == chunkCookieEcho && held == nil {
			held = append([]byte(nil), p...)
			return true
		}
		return false
	}
	l.ends[0].Connect(l.now)
	l.run(time.Second, func() bool { return held != nil })
	err := l.ends[1].HandlePacket(held, l.now.Add(cookieLifetime+time.Second))
	if !errors.Is(err, ErrBadCookie) || !strings.Contains(err.Error(), "stale") || l.ends[1].State() != StateClosed {
		t.Fatalf("%v, state %v", err, l.ends[1].State())
	}
}

// TestInitAckChecks checks that an INIT ACK without a cookie, or with a
// malformed parameter, is refused and leaves the association waiting.
func TestInitAckChecks(t *testing.T) {
	a := NewAssociation(Config{})
	now := time.Now()
	a.Connect(now)
	fixed := append(append(be32(7), be32(1<<20)...), 0, 4, 0, 4, 0, 0, 0, 1)
	for _, tc := range []struct {
		name string
		v    []byte
	}{
		{"no cookie", fixed},
		{"bad parameter", append(append([]byte(nil), fixed...), 0, 7, 0, 99)},
		{"short parameter", append(append([]byte(nil), fixed...), 0, 7)},
	} {
		err := a.HandlePacket(rawPacket(a.localTag, rawChunk(chunkInitAck, 0, tc.v...)), now)
		if !errors.Is(err, ErrMalformed) || a.State() != StateCookieWait {
			t.Fatalf("%s: %v, state %v", tc.name, err, a.State())
		}
	}
	// An INIT with a malformed parameter is refused too.
	b := NewAssociation(Config{})
	if err := b.HandlePacket(rawPacket(0, rawChunk(chunkInit, 0, append(append([]byte(nil), fixed...), 0, 7, 0, 99)...)), now); !errors.Is(err, ErrMalformed) {
		t.Fatalf("INIT: %v", err)
	}
}

// TestInitWhileShuttingDown checks that an INIT that comes while the
// association shuts down is ignored.
func TestInitWhileShuttingDown(t *testing.T) {
	l := newLink(t, 1, nil)
	l.connect(false)
	var init []byte
	other := NewAssociation(Config{Output: func(p []byte) { init = append([]byte(nil), p...) }})
	other.Connect(l.now)
	b := l.ends[1]
	l.loss = 1
	b.Shutdown()
	if b.State() != StateShutdownSent {
		t.Fatalf("state %v", b.State())
	}
	sent := l.sent[1]
	if err := b.HandlePacket(init, l.now); err != nil || l.sent[1] != sent || b.State() != StateShutdownSent {
		t.Fatalf("%v, %d sent, state %v", err, l.sent[1]-sent, b.State())
	}
	// Its SHUTDOWN unanswered, it gives the peer up.
	l.run(time.Hour, func() bool { return l.closed[1] })
	if !errors.Is(l.closeEr[1], ErrTimeout) {
		t.Fatalf("close error %v", l.closeEr[1])
	}
}

// TestAbortReflected checks that an ABORT with the T bit, under the tag
// the peer would expect of us, aborts.
func TestAbortReflected(t *testing.T) {
	l := newLink(t, 1, nil)
	l.connect(false)
	b := l.ends[1]
	if err := b.HandlePacket(rawPacket(b.peerTag, rawChunk(chunkAbort, flagTagReflected)), l.now); err != nil {
		t.Fatal(err)
	}
	if !l.closed[1] || !errors.Is(l.closeEr[1], ErrAborted) {
		t.Fatalf("closed %v, %v", l.closed[1], l.closeEr[1])
	}
}

// TestUnknownChunkReported checks that an unrecognized chunk that asks to
// be reported is quoted in an ERROR chunk up to 64 bytes, and not before
// the association knows the peer's tag.
func TestUnknownChunkReported(t *testing.T) {
	l := newLink(t, 1, nil)
	l.connect(false)
	var reports [][]byte
	l.drop = func(from int, p []byte) bool {
		if from == 1 && p[headerSize] == chunkError {
			reports = append(reports, append([]byte(nil), p[headerSize:]...))
		}
		return false
	}
	b := l.ends[1]
	long := make([]byte, 100)
	if err := b.HandlePacket(rawPacket(b.localTag, rawChunk(0x7f, 0, long...)), l.now); err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || binary.BigEndian.Uint16(reports[0][2:]) != 4+8+64 || reports[0][8] != 0x7f {
		t.Fatalf("reports %x", reports)
	}
	a := NewAssociation(Config{})
	sent := 0
	a.cfg.Output = func([]byte) { sent++ }
	a.Connect(time.Now())
	if err := a.HandlePacket(rawPacket(a.localTag, rawChunk(0x7f, 0, 1)), time.Now()); err != nil || sent != 1 {
		t.Fatalf("%v, %d sent", err, sent)
	}
}

// TestReceiveLimits checks the DATA a receiver drops: too far ahead, past
// its window, on a stream it does not have.
func TestReceiveLimits(t *testing.T) {
	var got []gotMsg
	l := newLink(t, 1, func(i int, c *Config) {
		collect(&got)(i, c)
		c.Streams = 4
		c.ReceiveWindow = 1
	})
	l.connect(false)
	b := l.ends[1]
	window := b.cfg.ReceiveWindow
	deliver := func(chunks ...[]byte) {
		t.Helper()
		if err := b.HandlePacket(rawPacket(b.localTag, chunks...), l.now); err != nil {
			t.Fatal(err)
		}
	}
	cum := b.peerCum
	deliver(rawData(cum+maxGapSpan+1, 0, 0, PPIDBinary, flagBegin|flagEnd, []byte{1}))
	deliver(rawData(cum+2, 0, 1, PPIDBinary, flagBegin|flagEnd, make([]byte, window+1)))
	if b.peerCum != cum || len(b.gaps) != 0 || b.held != 0 {
		t.Fatalf("cum %d (was %d), gaps %v, held %d", b.peerCum, cum, b.gaps, b.held)
	}
	// The next TSN is taken whatever its stream, and a stream that is not
	// there gets nothing.
	deliver(rawData(cum+1, 9, 0, PPIDBinary, flagBegin|flagEnd, []byte{1}))
	if b.peerCum != cum+1 || len(got) != 0 {
		t.Fatalf("cum %d, %d delivered", b.peerCum-cum, len(got))
	}
	// FORWARD TSN skips streams it does not have and SSNs already past.
	deliver(rawChunk(chunkForwardTSN, 0, append(be32(cum+2), 0, 9, 0, 0, 0, 0, 0xff, 0xff)...))
	if b.peerCum != cum+2 || b.stream(0).nextSSN != 0 {
		t.Fatalf("cum %d, next SSN %d", b.peerCum-cum, b.stream(0).nextSSN)
	}
	err := b.HandlePacket(rawPacket(b.localTag, rawChunk(chunkForwardTSN, 0, be32(cum+2+1<<30+1)...)), l.now)
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("FORWARD TSN far ahead: %v", err)
	}
}

// TestMaxMessageSizeEnforced checks that a peer sending a message larger
// than MaxMessageSize, to be reassembled or held for its turn, is aborted.
func TestMaxMessageSizeEnforced(t *testing.T) {
	small := func(i int, c *Config) {
		if i == 1 {
			c.MaxMessageSize = 1000
		}
	}
	t.Run("held", func(t *testing.T) {
		// A whole message in one chunk goes up as it comes, but one that
		// has to wait for an earlier one is held, and too large to be.
		l := newLink(t, 1, small)
		l.connect(false)
		b := l.ends[1]
		_ = b.HandlePacket(rawPacket(b.localTag, rawData(b.peerCum+1, 0, 1, PPIDBinary, flagBegin|flagEnd, make([]byte, 1100))), l.now)
		if !l.closed[1] || !errors.Is(l.closeEr[1], ErrMessageTooLarge) {
			t.Fatalf("receiver: %v", l.closeEr[1])
		}
	})
	for _, tc := range []struct {
		name string
		size int
		opts SendOptions
	}{
		{"ordered", 3000, SendOptions{}},
		{"unordered", 3000, SendOptions{Unordered: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := newLink(t, 1, small)
			l.connect(false)
			a := l.ends[0]
			a.SetRemoteMaxMessageSize(0)
			if err := a.Send(0, PPIDBinary, make([]byte, tc.size), tc.opts); err != nil {
				t.Fatal(err)
			}
			l.run(time.Minute, func() bool { return l.closed[1] })
			if !errors.Is(l.closeEr[1], ErrMessageTooLarge) {
				t.Fatalf("receiver: %v", l.closeEr[1])
			}
			l.run(time.Second, func() bool { return l.closed[0] })
			if !errors.Is(l.closeEr[0], ErrAborted) {
				t.Fatalf("sender: %v", l.closeEr[0])
			}
		})
	}
}

// TestSendLimits checks Send and BufferedAmount on streams the
// association does not have.
func TestSendLimits(t *testing.T) {
	l := newLink(t, 1, func(i int, c *Config) { c.Streams = 4 })
	l.connect(false)
	a := l.ends[0]
	if err := a.Send(4, PPIDBinary, []byte{1}, SendOptions{}); !errors.Is(err, ErrInvalidStream) {
		t.Fatalf("Send on stream 4 of 4: %v", err)
	}
	if n := a.BufferedAmount(60000); n != 0 {
		t.Fatalf("BufferedAmount %d", n)
	}
}

// TestResetRequests checks a reset asked twice, a request that resets every
// stream and drops what was held for them, and the answers to requests this
// side does not perform.
func TestResetRequests(t *testing.T) {
	var got []gotMsg
	l := newLink(t, 1, collect(&got))
	l.connect(false)
	a, b := l.ends[0], l.ends[1]
	var reconfigs int
	l.drop = func(from int, p []byte) bool {
		if from == 0 && p[headerSize] == chunkReconfig {
			reconfigs++
		}
		return false
	}
	if err := a.ResetStream(3); err != nil {
		t.Fatal(err)
	}
	if err := a.ResetStream(3); err != nil {
		t.Fatal(err)
	}
	l.run(time.Second, func() bool { return !a.stream(3).resetting })
	if reconfigs != 1 || a.stream(3).resetting {
		t.Fatalf("%d requests, resetting %v", reconfigs, a.stream(3).resetting)
	}

	// Stream 2 holds a message that waits for SSN 0, which never comes; a
	// request naming no stream resets them all, and drops it.
	deliver := func(chunks ...[]byte) {
		t.Helper()
		if err := b.HandlePacket(rawPacket(b.localTag, chunks...), l.now); err != nil {
			t.Fatal(err)
		}
	}
	cum := b.peerCum
	deliver(rawData(cum+1, 2, 1, PPIDBinary, flagBegin|flagEnd, []byte("held")))
	if b.held == 0 || b.stream(2).nextSSN != 0 {
		t.Fatalf("held %d", b.held)
	}
	seq := b.peerReqSeq
	deliver(rawResetRequest(seq, cum+1))
	if b.held != 0 || len(b.stream(2).ordered) != 0 || b.peerReqSeq != seq+1 || len(got) != 0 {
		t.Fatalf("held %d, request sequence %d, %d delivered", b.held, b.peerReqSeq-seq, len(got))
	}

	// An Incoming SSN Reset Request is denied, and asked again it is
	// answered the same, the sequence moving on once.
	incoming := func(seq uint32) []byte {
		v := append([]byte{0, paramIncomingReset, 0, 8}, be32(seq)...)
		return rawChunk(chunkReconfig, 0, v...)
	}
	seq = b.peerReqSeq
	deliver(incoming(seq))
	deliver(incoming(seq))
	if b.peerReqSeq != seq+1 || b.lastResult != reconfigDenied {
		t.Fatalf("request sequence %d, result %d", b.peerReqSeq-seq, b.lastResult)
	}
}

// TestMalformedParameters checks the packets whose chunks or parameters
// are cut short.
func TestMalformedParameters(t *testing.T) {
	l := newLink(t, 1, nil)
	l.connect(false)
	b := l.ends[1]
	for _, tc := range []struct {
		name string
		p    []byte
	}{
		{"chunk header cut", rawPacket(b.localTag, []byte{chunkSack, 0})},
		{"parameter header cut", rawPacket(b.localTag, rawChunk(chunkReconfig, 0, 0, 13))},
		{"short response", rawPacket(b.localTag, rawChunk(chunkReconfig, 0, 0, 16, 0, 8, 0, 0, 0, 0))},
		{"short incoming reset", rawPacket(b.localTag, rawChunk(chunkReconfig, 0, 0, 14, 0, 6, 0, 0))},
		{"short shutdown", rawPacket(b.localTag, rawChunk(chunkShutdown, 0, 1, 2))},
	} {
		if err := b.HandlePacket(tc.p, l.now); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	if !l.established() {
		t.Fatal("malformed packets broke the association")
	}
}

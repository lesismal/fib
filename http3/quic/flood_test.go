package quic

import (
	"errors"
	"testing"
	"time"
)

// floodCode is the code the tests' servers close a flooding peer with.
const floodCode = 0x107

// sendRaw sends payload from the client in a 1-RTT packet of its own, as
// nothing the client writes would build it.
func sendRaw(p *testPair, payload []byte) {
	c := p.client
	c.mu.Lock()
	plan := packetPlan{space: spaceApp, pn: c.spaces[spaceApp].nextPN, pnLen: 4, payload: payload, sp: &sentPacket{}}
	c.spaces[spaceApp].nextPN++
	d := c.sealPlan(nil, &plan)
	c.mu.Unlock()
	_ = p.clientEnd.Send(d)
}

// openedRaw has the client count n bidirectional streams as its own, for
// the raw frames that open them: it would otherwise take the server's
// answers on them for answers on streams it never opened.
func openedRaw(p *testPair, n int) {
	p.client.mu.Lock()
	p.client.openedBidi = uint64(n)
	p.client.mu.Unlock()
}

// sendRawFrames sends frames from the client, as many to a packet as fit.
func sendRawFrames(p *testPair, frames [][]byte) {
	var payload []byte
	for _, f := range frames {
		if len(payload)+len(f) > 1000 {
			sendRaw(p, payload)
			payload = nil
		}
		payload = append(payload, f...)
	}
	if len(payload) > 0 {
		sendRaw(p, payload)
	}
}

// floodPair is a test pair whose server closes a flooding client with
// floodCode, and takes more streams at once than the tests open.
func floodPair(t *testing.T) *testPair {
	return newTestPair(t, 0, func(_, server *Config) {
		server.ExcessiveLoadCode = floodCode
		server.MaxIncomingStreams = 4 * floodResetBurst
	})
}

// TestFloodGlitchesCounted checks which frames count against the glitches a
// peer may cause: each of the counted ones, repeated past the burst, closes
// the connection, and none of the others does.
func TestFloodGlitchesCounted(t *testing.T) {
	cases := []struct {
		name    string
		setup   func(c *Conn)
		payload func(i int) []byte
		counted bool
	}{
		{name: "PING", counted: true,
			payload: func(int) []byte { return frame(framePing) }},
		{name: "PING padded", counted: true,
			payload: func(int) []byte { return append(frame(framePing), 0, 0, 0) }},
		{name: "PATH_CHALLENGE", counted: true,
			payload: func(i int) []byte { return frame(framePathChallenge, "12345678") }},
		{name: "MAX_DATA not raised", counted: true,
			payload: func(int) []byte { return frame(frameMaxData, 0) }},
		{name: "MAX_STREAMS not raised", counted: true,
			payload: func(int) []byte { return frame(frameMaxStreamsBidi, 0) }},
		{name: "DATA_BLOCKED", counted: true,
			payload: func(int) []byte { return frame(frameDataBlocked, 0) }},
		{name: "STREAM_DATA_BLOCKED", counted: true,
			payload: func(int) []byte { return frame(frameStreamDataBlocked, 0, 0) }},
		{name: "STREAMS_BLOCKED", counted: true,
			payload: func(int) []byte { return frame(frameStreamsBlockedUni, 0) }},
		{name: "empty STREAM", counted: true,
			payload: func(int) []byte { return frame(frameStream|0x02, 0, 0) }},
		{name: "NEW_CONNECTION_ID already retired", counted: true,
			setup: func(c *Conn) {
				if _, err := c.handleFrames(spaceApp, newCIDFrame(2, 2, []byte("cid-two!")), time.Now()); err != nil {
					panic(err)
				}
			},
			payload: func(int) []byte { return newCIDFrame(1, 0, []byte("cid-one!")) }},
		{name: "DATA_BLOCKED above the limit given", counted: true,
			payload: func(int) []byte { return frame(frameDataBlocked, 1<<40) }},
		{name: "DATA_BLOCKED at new limits",
			payload: func(i int) []byte { return frame(frameDataBlocked, i+1) }},
		{name: "STREAM_DATA_BLOCKED at new limits",
			payload: func(i int) []byte { return frame(frameStreamDataBlocked, 0, i+1) }},
		{name: "STREAMS_BLOCKED at new limits",
			payload: func(i int) []byte { return frame(frameStreamsBlockedBidi, i+1) }},
		{name: "PING with a STREAM frame",
			payload: func(i int) []byte {
				return append(frame(framePing), frame(frameStream|0x06, 0, i, 1, "x")...)
			}},
		{name: "MAX_DATA raised",
			payload: func(i int) []byte { return frame(frameMaxData, 1<<20+i) }},
		{name: "MAX_STREAM_DATA for a stream forgotten",
			setup: func(c *Conn) {
				c.peerOpenedBidi = 1
			},
			payload: func(int) []byte { return frame(frameMaxStreamData, 0, 0) }},
		{name: "STREAM that ends",
			payload: func(i int) []byte { return frame(frameStream|0x01, 4*i) }},
		{name: "PADDING",
			payload: func(int) []byte { return []byte{framePadding, framePadding} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := frameConn(t)
			c.config.MaxIncomingStreams = 4 * floodGlitchBurst
			c.maxBidi = 4 * floodGlitchBurst
			c.mu.Lock()
			defer c.mu.Unlock()
			if tc.setup != nil {
				tc.setup(c)
			}
			var err error
			for i := 0; i < floodGlitchBurst+10 && err == nil; i++ {
				_, err = c.handleFrames(spaceApp, tc.payload(i), time.Now())
			}
			var te *TransportError
			switch {
			case !tc.counted && err != nil:
				t.Fatalf("closed with %v", err)
			case tc.counted && (!errors.As(err, &te) || te.Code != errProtocolViolation):
				t.Fatalf("closed with %v, want PROTOCOL_VIOLATION", err)
			}
		})
	}
}

// TestFloodPingWithNewAck checks that a PING bundled with an acknowledgement
// of something new, as a receiver that only acknowledges sends one now and
// then to have its acknowledgements acknowledged, is not counted.
func TestFloodPingWithNewAck(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	s := &c.spaces[spaceApp]
	now := time.Now()
	for i := 0; i < floodGlitchBurst+10; i++ {
		s.sent.push(&sentPacket{pn: s.nextPN, time: now})
		ack := frame(frameAck, s.nextPN, 0, 0, 0)
		s.nextPN++
		if _, err := c.handleFrames(spaceApp, append(ack, framePing), now); err != nil {
			t.Fatalf("PING %d: %v", i, err)
		}
	}
	// The same acknowledgement again acknowledges nothing new.
	var err error
	for i := 0; i < floodGlitchBurst+10 && err == nil; i++ {
		_, err = c.handleFrames(spaceApp, append(frame(frameAck, s.nextPN-1, 0, 0, 0), framePing), now)
	}
	if err == nil {
		t.Fatal("PINGs with an old acknowledgement were not counted")
	}
}

// TestFloodPingsClose checks that a client sending nothing but PING, past
// the burst, has its connection closed with the code the server was given.
func TestFloodPingsClose(t *testing.T) {
	p := floodPair(t)
	for i := 0; i < floodGlitchBurst+100; i++ {
		sendRaw(p, frame(framePing))
	}
	waitPeerClose(t, p.clientH, floodCode)
}

// resetFrames are n streams opened from first, each with a byte of a
// request, and abandoned at once with STOP_SENDING and RESET_STREAM.
func resetFrames(first, n int) [][]byte {
	var frames [][]byte
	for i := first; i < first+n; i++ {
		id := 4 * i
		frames = append(frames, append(append(frame(frameStream|0x02, id, 1, "x"),
			frame(frameStopSending, id, 0)...), frame(frameResetStream, id, 0, 1)...))
	}
	return frames
}

// TestFloodRapidReset checks that a client opening streams and abandoning
// them at once, past the burst, has its connection closed, though each
// stream is counted once for its STOP_SENDING and RESET_STREAM together.
func TestFloodRapidReset(t *testing.T) {
	p := floodPair(t)
	openedRaw(p, floodResetBurst+100)
	sendRawFrames(p, resetFrames(0, floodResetBurst+100))
	waitPeerClose(t, p.clientH, floodCode)
}

// TestFloodResetsUnderTheBurst checks that resets within the burst leave
// the connection serving.
func TestFloodResetsUnderTheBurst(t *testing.T) {
	p := floodPair(t)
	n := floodResetBurst - 10
	p.serverH.onData = func(s *Stream, data []byte, fin bool) {
		if s.ID() >= uint64(4*n) {
			_ = s.Write(data, fin)
		}
	}
	openedRaw(p, n)
	sendRawFrames(p, resetFrames(0, n))
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write([]byte("still there"), true)
	waitFinished(t, p.clientH, s.ID())
}

// TestFloodResetsForPeerErrors checks that streams the server resets for
// the client's errors count as the client's own resets do.
func TestFloodResetsForPeerErrors(t *testing.T) {
	p := floodPair(t)
	p.serverH.onData = func(s *Stream, _ []byte, _ bool) { s.ResetForPeerError(1) }
	openedRaw(p, floodResetBurst+100)
	var frames [][]byte
	for i := 0; i < floodResetBurst+100; i++ {
		frames = append(frames, frame(frameStream|0x02, 4*i, 1, "x"))
	}
	sendRawFrames(p, frames)
	waitPeerClose(t, p.clientH, floodCode)
}

// TestFloodClientTakesResets checks that a client is not held to the reset
// budget: a server that resets every stream the client opens, past the
// burst, as one answering each upload before its body ends may, leaves the
// client's connection open. The client's RESET_STREAM in answer to each
// STOP_SENDING does not count against the server's budget either.
func TestFloodClientTakesResets(t *testing.T) {
	p := floodPair(t)
	n := floodResetBurst + 10
	p.serverH.onData = func(s *Stream, data []byte, fin bool) {
		if s.ID() >= uint64(4*n) {
			_ = s.Write(data, fin)
			return
		}
		s.StopSending(1)
		s.Reset(1)
	}
	for i := 0; i < n; i++ {
		s, err := p.client.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		_ = s.Write([]byte("x"), false)
	}
	for i := 0; i < n; i++ {
		select {
		case <-p.clientH.reset:
		case err := <-p.clientH.closed:
			t.Fatalf("closed after %d resets: %v", i, err)
		case <-time.After(20 * time.Second):
			t.Fatalf("%d of %d streams reset", i, n)
		}
	}
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write([]byte("still there"), true)
	waitFinished(t, p.clientH, s.ID())
	if got := string(p.clientH.received(s.ID())); got != "still there" {
		t.Fatalf("echoed %q", got)
	}
}

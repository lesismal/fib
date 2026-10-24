package quic

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

// frameConn is a server connection past its handshake, as fuzzConn makes
// one, that also has the client's connection ID and 1-RTT keys: what a test
// hands it frame by frame can be answered in packets of its own.
func frameConn(t *testing.T) *Conn {
	t.Helper()
	c := fuzzConn(t)
	// Ending it closes its TLS too.
	t.Cleanup(func() { c.Abort(errTestOver) })
	c.dcid = []byte{1, 2, 3, 4, 5, 6, 7, 8}
	c.peerCIDs[0] = peerCID{cid: c.dcid}
	c.spaces[spaceApp].tx, c.spaces[spaceApp].rx = initialKeys(c.scid, false)
	return c
}

// frame builds a frame of type typ from varints and byte strings.
func frame(typ uint64, fields ...any) []byte {
	b := AppendVarint(nil, typ)
	for _, f := range fields {
		switch v := f.(type) {
		case int:
			b = AppendVarint(b, uint64(v))
		case uint64:
			b = AppendVarint(b, v)
		case []byte:
			b = append(b, v...)
		case string:
			b = append(b, v...)
		default:
			panic("frame: unsupported field")
		}
	}
	return b
}

// newCIDFrame is a NEW_CONNECTION_ID frame for cid.
func newCIDFrame(seq, retirePrior uint64, cid []byte) []byte {
	b := frame(frameNewConnectionID, seq, retirePrior)
	b = append(b, byte(len(cid)))
	b = append(b, cid...)
	return append(b, bytes.Repeat([]byte{0xee}, statelessResetTokenLen)...)
}

// The frames of a 1-RTT packet are the peer's to get right, and each way of
// getting one wrong is a connection error with the code RFC 9000 gives it,
// never a panic or a frame quietly half taken. Each case hands a connection
// past its handshake the frames in pre, which are fine, and then payload,
// whose error code must be want (0 for none).
func TestMalformedFrames(t *testing.T) {
	const peerBidi, peerUni, localBidi, localUni = 0, 2, 1, 3
	maxVarintBytes := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	tooManyStreams := []byte{0xd0, 0, 0, 0, 0, 0, 0, 1} // 2^60 + 1
	cases := []struct {
		name    string
		setup   func(c *Conn)
		space   int
		early   bool
		pre     [][]byte
		payload []byte
		want    uint64
		check   func(t *testing.T, c *Conn)
	}{
		{name: "no frames", payload: nil, want: errProtocolViolation},
		{name: "truncated frame type", payload: []byte{0x40}, want: errFrameEncoding},
		{name: "unknown frame type", payload: []byte{0x21}, want: errFrameEncoding},
		{name: "1-RTT frame in a handshake packet", space: spaceHandshake,
			payload: frame(frameMaxData, 1), want: errProtocolViolation},
		{name: "ACK in a 0-RTT packet", early: true,
			payload: frame(frameAck, 0, 0, 0, 0), want: errProtocolViolation},

		{name: "truncated RESET_STREAM", payload: frame(frameResetStream, peerBidi), want: errFrameEncoding},
		{name: "RESET_STREAM for a stream this side only sends on",
			payload: frame(frameResetStream, localUni, 0, 0), want: errStreamState},
		{name: "RESET_STREAM changing the final size",
			pre:     [][]byte{frame(frameStream|0x03, peerBidi, 2, "ab")},
			payload: frame(frameResetStream, peerBidi, 7, 1), want: errFinalSize},
		{name: "RESET_STREAM beyond the stream window",
			setup:   func(c *Conn) { c.config.StreamReceiveWindow = 4 },
			payload: frame(frameResetStream, peerBidi, 7, 5), want: errFlowControl},
		{name: "RESET_STREAM beyond the connection window",
			setup:   func(c *Conn) { c.recvMaxData = 4 },
			payload: frame(frameResetStream, peerBidi, 7, 5), want: errFlowControl},
		{name: "RESET_STREAM repeated",
			pre:     [][]byte{frame(frameResetStream, peerBidi, 7, 3)},
			payload: frame(frameResetStream, peerBidi, 7, 3),
			check: func(t *testing.T, c *Conn) {
				if n := countEvents(c, evStreamReset); n != 1 {
					t.Fatalf("%d resets reported, want 1", n)
				}
			}},

		{name: "truncated STOP_SENDING", payload: frame(frameStopSending, peerBidi), want: errFrameEncoding},
		{name: "STOP_SENDING for a stream this side only receives on",
			payload: frame(frameStopSending, peerUni, 0), want: errStreamState},
		{name: "STOP_SENDING for a stream not yet opened",
			payload: frame(frameStopSending, localBidi, 0), want: errStreamState},
		{name: "STOP_SENDING after the stream was reset",
			setup: func(c *Conn) {
				s := c.newStream(localBidi)
				c.openedBidi = 1
				s.resetLocked(9)
			},
			payload: frame(frameStopSending, localBidi, 4),
			check: func(t *testing.T, c *Conn) {
				if n := countEvents(c, evStopSending); n != 0 {
					t.Fatalf("%d stop sendings reported for a reset stream", n)
				}
			}},

		{name: "truncated CRYPTO", payload: frame(frameCrypto, 0, 5, "ab"), want: errFrameEncoding},
		{name: "CRYPTO far beyond what has arrived",
			payload: frame(frameCrypto, maxCryptoBuffer, 1, "a"), want: errCryptoBufferExceeds},

		{name: "NEW_TOKEN from a client", payload: frame(frameNewToken, 1, "t"), want: errProtocolViolation},
		{name: "empty NEW_TOKEN", setup: func(c *Conn) { c.isClient = true },
			payload: frame(frameNewToken, 0), want: errFrameEncoding},
		{name: "NEW_TOKEN", setup: func(c *Conn) { c.isClient = true },
			payload: frame(frameNewToken, 2, "tk")},

		{name: "truncated STREAM", payload: frame(frameStream|0x02, peerBidi, 5, "a"), want: errFrameEncoding},
		{name: "STREAM for a stream this side only sends on",
			payload: frame(frameStream, localUni, "a"), want: errStreamState},
		{name: "STREAM beyond the stream limit",
			payload: frame(frameStream, 100<<2, "a"), want: errStreamLimit},
		{name: "STREAM for a stream long finished",
			setup:   func(c *Conn) { c.peerOpenedBidi = 1 },
			payload: frame(frameStream, peerBidi, "late"),
			check: func(t *testing.T, c *Conn) {
				if c.streams[peerBidi] != nil {
					t.Fatal("a finished stream came back")
				}
			}},
		{name: "STREAM offset beyond 2^62",
			payload: append(frame(frameStream|0x06, peerBidi), append(maxVarintBytes, 1, 'a')...), want: errFrameEncoding},
		{name: "STREAM beyond the final size",
			pre:     [][]byte{frame(frameStream|0x03, peerBidi, 2, "ab")},
			payload: frame(frameStream|0x06, peerBidi, 2, 1, "c"), want: errFinalSize},
		{name: "STREAM final size below what arrived",
			pre:     [][]byte{frame(frameStream|0x02, peerBidi, 4, "abcd")},
			payload: frame(frameStream|0x03, peerBidi, 2, "ab"), want: errFinalSize},
		{name: "STREAM beyond the stream window",
			setup:   func(c *Conn) { c.config.StreamReceiveWindow = 4 },
			payload: frame(frameStream|0x02, peerBidi, 5, "abcde"), want: errFlowControl},
		{name: "STREAM beyond the connection window",
			setup:   func(c *Conn) { c.recvMaxData = 4 },
			payload: frame(frameStream|0x02, peerBidi, 5, "abcde"), want: errFlowControl},
		{name: "STREAM fin on a stream this side stopped",
			setup: func(c *Conn) {
				s, _ := c.peerStream(peerBidi, true)
				s.stopSendingLocked(5)
				// Its sending side is over too, so the stream can go.
				s.resetLocked(5)
				s.resetAcked = true
			},
			payload: frame(frameStream|0x03, peerBidi, 3, "end"),
			check: func(t *testing.T, c *Conn) {
				if c.streams[peerBidi] != nil {
					t.Fatal("the stopped stream was not finished by its fin")
				}
			}},

		{name: "MAX_DATA lets a blocked stream send",
			setup: func(c *Conn) {
				c.peerMaxData = 0
				s := c.newStream(localBidi)
				c.openedBidi = 1
				s.send.write([]byte("blocked"))
			},
			payload: frame(frameMaxData, 1000),
			check: func(t *testing.T, c *Conn) {
				if c.peerMaxData != 1000 || len(c.sendQueue) != 1 {
					t.Fatalf("peer max data %d, %d streams queued", c.peerMaxData, len(c.sendQueue))
				}
			}},
		{name: "truncated MAX_STREAM_DATA", payload: frame(frameMaxStreamData, peerBidi), want: errFrameEncoding},
		{name: "MAX_STREAM_DATA for a stream this side only receives on",
			payload: frame(frameMaxStreamData, peerUni, 10), want: errStreamState},
		{name: "MAX_STREAM_DATA for a stream of this side's long finished",
			setup:   func(c *Conn) { c.openedBidi = 1 },
			payload: frame(frameMaxStreamData, localBidi, 10)},
		{name: "MAX_STREAMS above 2^60",
			payload: append([]byte{frameMaxStreamsBidi}, tooManyStreams...), want: errFrameEncoding},
		{name: "MAX_STREAMS for unidirectional streams",
			payload: frame(frameMaxStreamsUni, 40),
			check: func(t *testing.T, c *Conn) {
				if c.peerMaxUni != 40 || countEvents(c, evStreamsAvailable) != 1 {
					t.Fatalf("peer max uni %d", c.peerMaxUni)
				}
			}},
		{name: "blocked frames and RETIRE_CONNECTION_ID",
			payload: slices.Concat(frame(frameDataBlocked, 10), frame(frameStreamDataBlocked, peerBidi, 10),
				frame(frameStreamsBlockedBidi, 3), frame(frameStreamsBlockedUni, 3), frame(frameRetireConnectionID, 0))},

		{name: "truncated CONNECTION_CLOSE",
			payload: frame(frameConnectionClose, errNoError, 0, 5, "a"), want: errFrameEncoding},
		{name: "HANDSHAKE_DONE from a client", payload: frame(frameHandshakeDone), want: errProtocolViolation},

		{name: "ACK with a first range below zero", payload: frame(frameAck, 1, 0, 0, 5), want: errFrameEncoding},
		{name: "ACK with a gap below zero",
			setup:   func(c *Conn) { c.spaces[spaceApp].nextPN = 10 },
			payload: frame(frameAck, 5, 0, 1, 0, 10, 0), want: errFrameEncoding},
		{name: "ACK_ECN",
			setup:   func(c *Conn) { c.spaces[spaceApp].nextPN = 10 },
			payload: frame(frameAckECN, 5, 0, 0, 0, 1, 2, 3)},
		{name: "truncated ACK_ECN",
			setup:   func(c *Conn) { c.spaces[spaceApp].nextPN = 10 },
			payload: frame(frameAckECN, 5, 0, 0, 0, 1), want: errFrameEncoding},

		{name: "truncated NEW_CONNECTION_ID", payload: frame(frameNewConnectionID, 1, 0), want: errFrameEncoding},
		{name: "NEW_CONNECTION_ID with an empty connection ID",
			payload: newCIDFrame(1, 0, nil), want: errFrameEncoding},
		{name: "NEW_CONNECTION_ID with a sequence number reused",
			pre:     [][]byte{newCIDFrame(1, 0, []byte("cid-one!"))},
			payload: newCIDFrame(1, 0, []byte("cid-two!")), want: errProtocolViolation},
		{name: "NEW_CONNECTION_ID repeated",
			pre:     [][]byte{newCIDFrame(1, 0, []byte("cid-one!"))},
			payload: newCIDFrame(1, 0, []byte("cid-one!"))},
		{name: "NEW_CONNECTION_ID beyond the limit",
			pre:     [][]byte{newCIDFrame(1, 0, []byte("cid-one!"))},
			payload: newCIDFrame(2, 0, []byte("cid-two!")), want: errConnectionIDLimit},
		{name: "NEW_CONNECTION_ID retiring the one in use",
			payload: newCIDFrame(1, 1, []byte("cid-one!")),
			check: func(t *testing.T, c *Conn) {
				if string(c.dcid) != "cid-one!" || c.dcidSeq != 1 || !slices.Equal(c.retireCIDs, []uint64{0}) {
					t.Fatalf("dcid %q seq %d, retiring %v", c.dcid, c.dcidSeq, c.retireCIDs)
				}
			}},
		{name: "NEW_CONNECTION_ID already retired",
			pre:     [][]byte{newCIDFrame(2, 2, []byte("cid-two!"))},
			payload: newCIDFrame(1, 0, []byte("cid-one!")),
			check: func(t *testing.T, c *Conn) {
				if !slices.Equal(c.retireCIDs, []uint64{0, 1}) || len(c.peerCIDs) != 1 {
					t.Fatalf("retiring %v, keeping %d", c.retireCIDs, len(c.peerCIDs))
				}
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := frameConn(t)
			c.mu.Lock()
			defer c.mu.Unlock()
			if tc.setup != nil {
				tc.setup(c)
			}
			now := time.Now()
			for _, f := range tc.pre {
				if _, err := c.handleFrames(spaceApp, f, now); err != nil {
					t.Fatalf("frames before the case: %v", err)
				}
			}
			c.curEarly = tc.early
			space := spaceApp
			if tc.space != 0 {
				space = tc.space
			}
			_, err := c.handleFrames(space, tc.payload, now)
			c.curEarly = false
			if tc.want == 0 {
				if err != nil {
					t.Fatalf("failed with %v", err)
				}
			} else {
				var te *TransportError
				if !errors.As(err, &te) || te.Code != tc.want {
					t.Fatalf("failed with %v, want code 0x%x", err, tc.want)
				}
			}
			if tc.check != nil {
				tc.check(t, c)
			}
		})
	}
}

func countEvents(c *Conn, kind eventKind) int {
	n := 0
	for _, e := range c.events {
		if e.kind == kind {
			n++
		}
	}
	return n
}

// A CONNECTION_CLOSE is read back as the error it was built from, from the
// side that sent it. An application's close outside a 1-RTT packet is a
// transport close with APPLICATION_ERROR, so as not to tell an observer
// what the application said, and an error of no QUIC kind is INTERNAL_ERROR.
func TestConnectionCloseRoundTrip(t *testing.T) {
	long := strings.Repeat("r", 300)
	cases := []struct {
		err    error
		oneRTT bool
		want   error
	}{
		{&ApplicationError{Code: 7, Reason: "bye"}, true, &ApplicationError{Code: 7, Reason: "bye", Remote: true}},
		{&ApplicationError{Code: 7, Reason: "bye"}, false, &TransportError{Code: errApplication, Remote: true}},
		{&TransportError{Code: errFlowControl, Reason: long}, true,
			&TransportError{Code: errFlowControl, Reason: long[:256], Remote: true}},
		{errors.New("something else"), true, &TransportError{Code: errInternal, Remote: true}},
	}
	for _, tc := range cases {
		c := frameConn(t)
		c.mu.Lock()
		_, err := c.handleFrames(spaceApp, appendConnectionClose(nil, tc.err, tc.oneRTT), time.Now())
		got := c.closeErr
		c.mu.Unlock()
		if err != nil {
			t.Fatalf("%v: %v", tc.err, err)
		}
		if got == nil || got.Error() != tc.want.Error() {
			t.Fatalf("%v (1-RTT %v) read back as %v, want %v", tc.err, tc.oneRTT, got, tc.want)
		}
	}
}

// The errors a connection ends with say which side ended it, and a TLS
// alert is told as one.
func TestErrorStrings(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{&TransportError{Code: errFlowControl, Reason: "r"}, "quic: local transport error 0x3: r"},
		{&TransportError{Code: errCryptoBase + 40, Reason: "r", Remote: true}, "quic: remote TLS alert 40: r"},
		{&ApplicationError{Code: 0x10c, Reason: "r"}, "quic: local application error 0x10c: r"},
		{&ApplicationError{Code: 1, Reason: "r", Remote: true}, "quic: remote application error 0x1: r"},
	}
	for _, tc := range cases {
		if got := tc.err.Error(); got != tc.want {
			t.Fatalf("%q, want %q", got, tc.want)
		}
	}
}

// RETIRE_CONNECTION_ID for a connection ID the peer asked to retire goes out
// in the next packet, and again if that packet is lost.
func TestRetireConnectionIDSentAndResent(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	_, err := c.handleFrames(spaceApp, newCIDFrame(1, 1, []byte("cid-one!")), time.Now())
	c.wantFlush = true
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	c.dispatch()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.retireCIDs) != 0 {
		t.Fatalf("still retiring %v", c.retireCIDs)
	}
	var retire *sentFrame
	app := &c.spaces[spaceApp]
	app.sent.each(app.sent.end(), func(p *sentPacket) bool {
		for i := range p.frames {
			if p.frames[i].kind == sfRetireCID {
				retire = &p.frames[i]
			}
		}
		return true
	})
	if retire == nil {
		t.Fatal("no RETIRE_CONNECTION_ID sent")
	}
	c.frameLost(spaceApp, retire)
	if !slices.Equal(c.retireCIDs, []uint64{0}) {
		t.Fatalf("retiring %v after the loss", c.retireCIDs)
	}
}

// Every kind of frame whose loss matters is queued again when it is lost,
// unless what it said no longer needs saying.
func TestControlFramesLost(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	peer, _ := c.peerStream(0, true)
	for _, kind := range []uint8{sfMaxStreamsBidi, sfMaxStreamsUni, sfStopSending} {
		c.frameLost(spaceApp, &sentFrame{kind: kind, stream: peer})
	}
	if !c.maxStreamsBidiPending || !c.maxStreamsUniPending || !peer.stopPending {
		t.Fatalf("bidi %v uni %v stop %v", c.maxStreamsBidiPending, c.maxStreamsUniPending, peer.stopPending)
	}
	// The MAX_STREAMS frames go in the next packet.
	sp := &sentPacket{}
	b := c.appendControlFrames(nil, 100, sp)
	if c.maxStreamsBidiPending || c.maxStreamsUniPending || len(sp.frames) != 2 || b[len(b)-2] != frameMaxStreamsUni {
		t.Fatalf("control frames %x", b)
	}
	// A stream whose receiving side is over needs no STOP_SENDING.
	peer.stopPending = false
	peer.recvDone = true
	c.frameLost(spaceApp, &sentFrame{kind: sfStopSending, stream: peer})
	if peer.stopPending {
		t.Fatal("STOP_SENDING queued for a finished receiving side")
	}
}

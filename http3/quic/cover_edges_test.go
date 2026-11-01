package quic

import (
	"bytes"
	"crypto/tls"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/lesismal/fib/bufferpool"
)

// A send buffer's lost range past what it keeps is dropped rather than sent,
// and an acknowledgement past its end moves the base only to the end.
func TestCqSendBufferEdges(t *testing.T) {
	var b sendBuffer
	b.write([]byte("0123"))
	b.popNew(4)
	b.onLost(4, 4)
	if off, data := b.popLost(8); off != 0 || data != nil {
		t.Fatalf("%d %q from a range past the data", off, data)
	}
	if b.hasLost() {
		t.Fatal("a range past the data is still queued")
	}
	b.onAck(0, 100)
	if b.base != 4 || !b.allAcked() {
		t.Fatalf("base %d after an acknowledgement past the end", b.base)
	}
}

// The connection IDs of a connection that has published none, retiring
// one among several, one retired already, one the glitch budget no longer
// allows, and NEW_CONNECTION_ID frames that do not all fit.
func TestCqConnectionIDEdges(t *testing.T) {
	scid := []byte("handshak")
	if cids := (&Conn{scid: scid}).LocalCIDs(); len(cids) != 1 || !bytes.Equal(cids[0], scid) {
		t.Fatalf("LocalCIDs of a fresh connection: %x", cids)
	}

	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	// Not confirmed, so nothing is issued.
	confirmed, issuing := c.handshakeConfirmed, c.cidsIssuing
	c.handshakeConfirmed = false
	c.issueCIDsLocked()
	c.handshakeConfirmed = confirmed
	if c.cidsIssuing != issuing {
		t.Fatal("connection IDs issued before the handshake was confirmed")
	}
	c.localCIDs = append(c.localCIDs, localCID{seq: 1, cid: randomCID(), token: make([]byte, 16)},
		localCID{seq: 2, cid: randomCID(), token: make([]byte, 16)})
	c.nextCIDSeq = 3
	for i := range c.localCIDs {
		c.localCIDs[i].pending = true
	}
	c.cidsPending = true
	// Room for one frame: the rest wait.
	sp := &sentPacket{}
	if b := c.appendNewCIDs(nil, 1+1+1+1+cidLen+16, sp); len(b) == 0 || !c.cidsPending {
		t.Fatalf("NEW_CONNECTION_ID %x, still pending %v", b, c.cidsPending)
	}
	if err := c.handleRetireCID(2, nil); err != nil {
		t.Fatal(err)
	}
	if err := c.handleRetireCID(2, nil); err != nil {
		t.Fatalf("retired again: %v", err)
	}
	c.glitches = newFloodBudget(1, 1, 1, 1)
	c.glitches.take()
	var te *TransportError
	if err := c.handleRetireCID(1, nil); !errors.As(err, &te) {
		t.Fatalf("retiring past the glitch budget: %v", err)
	}
}

// The names of the congestion controllers, and HyStart++ leaving slow start
// under NewReno, which has no curve to start.
func TestCqCongestionEdges(t *testing.T) {
	for cc, want := range map[CongestionControl]string{CUBIC: "CUBIC", NewReno: "NewReno", 7: "CongestionControl(7)"} {
		if got := cc.String(); got != want {
			t.Errorf("%d: %q, want %q", int(cc), got, want)
		}
	}
	cc := &congestion{algo: NewReno, cwnd: 12000}
	cc.hy.inCSS = true
	cc.hystartExit()
	if cc.hy.inCSS || cc.ssthresh != 12000 || cc.wMax != 0 {
		t.Fatalf("after HyStart++: %+v", cc)
	}
}

// A packet key with a key AES will not take is refused.
func TestCqBadAEADKey(t *testing.T) {
	if _, err := newAEAD(tls.TLS_AES_128_GCM_SHA256, make([]byte, 5)); err == nil {
		t.Fatal("an AES key of 5 bytes was taken")
	}
}

// Frames the peer may send that change nothing, or are cut short: a
// MAX_STREAM_DATA that does not raise the limit, an IMMEDIATE_ACK, a
// STREAM_DATA_BLOCKED past the streams the peer may open, and
// STREAM_DATA_BLOCKED, STREAMS_BLOCKED and ACK_FREQUENCY with fields missing.
func TestCqFrameEdges(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if _, err := c.handleFrames(spaceApp, frame(frameMaxStreamData, 0, 0), now); err != nil {
		t.Fatalf("MAX_STREAM_DATA not raised: %v", err)
	}
	c.spaces[spaceApp].ackNow = false
	if _, err := c.handleFrames(spaceApp, frame(frameImmediateAck), now); err != nil || !c.spaces[spaceApp].ackNow {
		t.Fatalf("IMMEDIATE_ACK: %v, ack now %v", err, c.spaces[spaceApp].ackNow)
	}
	var te *TransportError
	if _, err := c.handleFrames(spaceApp, frame(frameStreamDataBlocked, 1<<20, 0), now); !errors.As(err, &te) || te.Code != errStreamLimit {
		t.Fatalf("STREAM_DATA_BLOCKED past the limit: %v", err)
	}
	for name, payload := range map[string][]byte{
		"STREAM_DATA_BLOCKED": frame(frameStreamDataBlocked, 0),
		"STREAMS_BLOCKED":     frame(frameStreamsBlockedBidi),
		"ACK_FREQUENCY":       frame(frameAckFrequency, 1),
	} {
		if _, err := c.handleFrames(spaceApp, payload, now); !errors.As(err, &te) || te.Code != errFrameEncoding {
			t.Errorf("%s cut short: %v", name, err)
		}
	}
}

// Transport parameters: an active_connection_id_limit above the default goes
// out and comes back, and a min_ack_delay past 2^14 milliseconds is refused.
func TestCqParamsEdges(t *testing.T) {
	p := defaultParams()
	p.activeCIDLimit = 7
	got, err := parseParams(p.encode(), true)
	if err != nil || got.activeCIDLimit != 7 {
		t.Fatalf("active_connection_id_limit came back %d, %v", got.activeCIDLimit, err)
	}
	b := appendParamVarint(nil, paramMinAckDelay, uint64((1<<14)*time.Millisecond/time.Microsecond)+1)
	if _, err := parseParams(b, true); err == nil {
		t.Fatal("a min_ack_delay past 2^14 milliseconds was taken")
	}
}

// What the sending side does at its edges: a full socket already waited on
// is waited on still; a datagram held for a full socket goes back to the
// pool once the connection has closed; a lost STREAMS_BLOCKED is sent again;
// a lost probe that is not the one in flight changes nothing; and an
// ACK_FREQUENCY asks for a threshold of one at least.
func TestCqSendEdges(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.awaitingRoom = true
	if !c.awaitRoomLocked(nil, syscall.EAGAIN) {
		t.Fatal("a socket waited on for room was not waited on")
	}
	c.awaitingRoom = false

	c.blocked, c.closed = bufferpool.Get(64), true
	if c.sendBlockedLocked() || c.blocked != nil {
		t.Fatal("a closed connection sent what it held")
	}
	c.closed = false

	c.peerMaxBidi, c.peerMaxUni = 4, 4
	c.streamsBlocked[0] = blockedReport{at: 4, reported: true}
	c.streamsBlocked[1] = blockedReport{at: 4, reported: true}
	c.frameLost(spaceApp, &sentFrame{kind: sfStreamsBlockedBidi, off: 4})
	c.frameLost(spaceApp, &sentFrame{kind: sfStreamsBlockedUni, off: 4})
	if !c.streamsBlocked[0].pending || !c.streamsBlocked[1].pending {
		t.Fatalf("lost STREAMS_BLOCKED not sent again: %+v", c.streamsBlocked)
	}

	c.mtu.inFlight = false
	c.mtuLostLocked(&sentPacket{mtuProbe: true}, time.Now())
	if c.mtu.fails != 0 {
		t.Fatal("a probe not in flight counted as a failure")
	}

	params := *c.peerParams
	params.hasMinAckDelay = true
	c.peerParams = &params
	c.ackFreqPending, c.ackFreqAsking = true, 0
	b := c.appendControlFrames(nil, 100, &sentPacket{})
	want := AppendVarint(nil, frameAckFrequency)
	want = AppendVarint(want, c.ackFreqSeq)
	want = AppendVarint(want, 1)
	if !bytes.Contains(b, want) || c.ackFreqPending {
		t.Fatalf("control frames %x, want ACK_FREQUENCY with a threshold of 1", b)
	}

	// The peer asked for no delay: an ack-eliciting packet is acknowledged
	// at once.
	c.ackFreq = ackFrequency{valid: true, threshold: 10}
	s := &c.spaces[spaceApp]
	s.ackNow, s.unackedEliciting = false, 0
	c.ackElicitingPacket(spaceApp, 5, 4, time.Now())
	if !s.ackNow {
		t.Fatal("no acknowledgement due with a delay of zero")
	}
	if d := (&Conn{}).ackFrequencyDelay(); d != 0 {
		t.Fatalf("delay %v without the peer's parameters", d)
	}

	// A STREAM_DATA_BLOCKED waiting to report a limit the peer has since
	// raised is not sent.
	st, err := c.peerStream(0, false)
	if err != nil || st == nil {
		t.Fatalf("stream 0: %v", err)
	}
	st.blocked = blockedReport{at: 1, reported: true, pending: true}
	st.sendMax = 100
	if b := st.appendFrames(nil, 1200, &sentPacket{}); len(b) != 0 || st.blocked.pending {
		t.Fatalf("frames %x for a limit raised since, pending %v", b, st.blocked.pending)
	}
}

// A client whose first Initial carried a cached token, and is answered with
// a Retry all the same, forgets the token.
func TestCqRetryForgetsCachedToken(t *testing.T) {
	k := NewTokenKey()
	cache := NewTokenCache()
	cache.remember(testAddr, k.NewToken(testAddr), time.Now())
	c, sink, _ := dialSink(t, func(config *Config) { config.TokenCache = cache })
	sink.mu.Lock()
	first := sink.sent[0]
	sink.mu.Unlock()
	c.HandleDatagram(k.Retry(first, testAddr))
	if cache.token(testAddr, time.Now()) != nil {
		t.Fatal("the token a Retry turned down was kept")
	}
}

// A message given as parts arrives whole.
func TestCqStreamWriteParts(t *testing.T) {
	p := newTestPair(t, 0, nil)
	p.serverH.onData = func(s *Stream, data []byte, fin bool) { _ = s.Write(data, fin) }
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	owned := bufferpool.Get(5)[:5]
	copy(owned, "owned")
	if err := s.WriteParts([]Part{{Data: []byte("caller's ")}, {Data: owned, Owned: true}}, true); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, p.clientH, s.ID())
	if got := string(p.clientH.received(s.ID())); got != "caller's owned" {
		t.Fatalf("echoed %q", got)
	}
}

// A probe of path MTU discovery whose time has come wakes the timer, which
// sends none on a path not yet validated; and a datagram held for a full
// socket goes back to the pool as the connection closes.
func TestCqTimerMTUProbeAndClose(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	c.pathValidated = false
	c.mtu.on, c.mtu.probe, c.mtu.inFlight, c.mtu.at = true, 1300, false, time.Now().Add(-time.Second)
	c.mu.Unlock()
	c.onTimer()
	c.mu.Lock()
	inFlight := c.mtu.inFlight
	c.mtu.on = false
	c.blocked = bufferpool.Get(64)
	c.mu.Unlock()
	if inFlight {
		t.Fatal("a probe went out on a path not validated")
	}
	c.Abort(errTestOver)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.blocked != nil {
		t.Fatal("a closed connection kept the datagram it held")
	}
}

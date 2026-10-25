package quic

import (
	"crypto/tls"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/bufferpool"
)

// cqShort is a 1-RTT packet to c as its peer would send it: sealed with k,
// numbered pn, with first as its first byte before protection (the key phase
// and reserved bits are the test's to set), and a PING padded to give header
// protection its sample.
func cqShort(c *Conn, k *keys, first byte, pn uint64, payload []byte) []byte {
	if payload == nil {
		payload = append([]byte{framePing}, make([]byte, 24)...)
	}
	pkt := []byte{first | 0x40 | 3}
	pkt = append(pkt, c.scid...)
	pnOffset := len(pkt)
	pkt = append(pkt, byte(pn>>24), byte(pn>>16), byte(pn>>8), byte(pn))
	pkt = append(pkt, payload...)
	pkt = append(pkt, make([]byte, aeadOverhead)...)[:len(pkt)]
	return k.seal(pkt, pnOffset, 4, pn)
}

// cqPeerKeys is the packet protection frameConn's peer sends with: the
// client keys of frameConn's connection ID, derived afresh so that sealing
// shares no state with the connection's own copy.
func cqPeerKeys(c *Conn) *keys {
	_, rx := initialKeys(c.scid, false)
	return rx
}

// cqHas reports whether packet number pn is among those received.
func cqHas(s rangeSet, pn uint64) bool {
	for _, r := range s {
		if r.lo <= pn && pn <= r.hi {
			return true
		}
	}
	return false
}

// cqPC is a path that counts how often it was closed.
type cqPC struct{ closes atomic.Int32 }

func (*cqPC) Send([]byte) error { return nil }
func (p *cqPC) Close() error    { p.closes.Add(1); return nil }

// A packet in the key phase the connection left is opened with the keys it
// left, when it is older than the update: reordering across a key update
// loses nothing (RFC 9001 section 6.5).
func TestCqReorderedAcrossKeyUpdate(t *testing.T) {
	c := frameConn(t)
	k := cqPeerKeys(c)
	c.HandleDatagram(cqShort(c, k.next(), 0x04, 10, nil))
	c.HandleDatagram(cqShort(c, k, 0, 5, nil))
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		t.Fatalf("closed with %v", c.closeErr)
	}
	if c.rxGen != 1 || c.prevRx == nil {
		t.Fatalf("receive generation %d, previous keys %v", c.rxGen, c.prevRx != nil)
	}
	if !cqHas(c.spaces[spaceApp].recv, 5) || !cqHas(c.spaces[spaceApp].recv, 10) {
		t.Fatal("a packet on either side of the update was not taken")
	}
}

// A key update before the handshake is confirmed is a KEY_UPDATE_ERROR (RFC
// 9001 section 6.1), and a short header with its reserved bits set a
// PROTOCOL_VIOLATION (RFC 9000 section 17.3.1), each found once the packet
// is authentic. Of datagrams handled together, those after the one that
// ended the connection are not looked at.
func TestCqPacketsThatEndTheConnection(t *testing.T) {
	t.Run("key update before confirmation", func(t *testing.T) {
		c := frameConn(t)
		c.handshakeConfirmed = false
		c.HandleDatagram(cqShort(c, cqPeerKeys(c).next(), 0x04, 1, nil))
		c.mu.Lock()
		defer c.mu.Unlock()
		wantCode(t, c.closeErr, errKeyUpdate)
	})
	t.Run("reserved bits", func(t *testing.T) {
		c := frameConn(t)
		k := cqPeerKeys(c)
		c.HandlePathDatagrams(c.pc, c.remote, [][]byte{cqShort(c, k, 0x18, 1, nil), cqShort(c, k, 0, 2, nil)})
		c.mu.Lock()
		defer c.mu.Unlock()
		wantCode(t, c.closeErr, errProtocolViolation)
		if cqHas(c.spaces[spaceApp].recv, 2) {
			t.Fatal("a packet after the close was taken")
		}
	})
}

// Packets held for keys they may yet get are dropped once they cannot: one
// that no longer parses, 0-RTT once the ClientHello is answered without
// early keys; a Handshake packet whose space is gone is held on but makes no
// progress, and a packet of a discarded space is not opened at all.
func TestCqBufferedPackets(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	body := func(b []byte) []byte {
		b = AppendVarint(b, 30)
		return append(b, make([]byte, 30)...)
	}
	zeroRTT := body(longHeader(0xc0|packet0RTT<<4, Version1, c.scid, c.dcid))
	handshake := body(longHeader(0xc0|packetHandshake<<4, Version1, c.scid, c.dcid))
	short := cqShort(c, cqPeerKeys(c), 0, 1, nil)
	c.spaces[spaceHandshake].discarded = true
	// Held packets are taken as from the path the connection is on.
	c.setRxPathLocked(nil, nil)
	c.buffered = [][]byte{{0x80}, zeroRTT, handshake, short}
	c.retryBuffered(time.Now())
	if len(c.buffered) != 1 || c.buffered[0][0] != handshake[0] {
		t.Fatalf("%d packets still held", len(c.buffered))
	}
	if !cqHas(c.spaces[spaceApp].recv, 1) {
		t.Fatal("the 1-RTT packet held was not taken")
	}
	if c.handlePacket(handshake, header{typ: packetHandshake}, time.Now()) {
		t.Fatal("a packet of a discarded space was taken as authentic")
	}
}

// A client drops a 0-RTT packet, which only clients send (RFC 9000 section
// 17.2.3), and a long header is never taken for a stateless reset.
func TestCqClientDropsZeroRTT(t *testing.T) {
	c, _, _ := dialSink(t, nil)
	c.mu.Lock()
	c.peerResetToken = make([]byte, statelessResetTokenLen)
	pkt := AppendVarint(longHeader(0xc0|packet0RTT<<4, Version1, c.scid, c.dcid), 30)
	pkt = append(pkt, make([]byte, 30)...)
	c.mu.Unlock()
	c.HandleDatagram(pkt)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		t.Fatalf("closed with %v", c.closeErr)
	}
	if len(c.buffered) != 0 {
		t.Fatal("a 0-RTT packet held for keys a client never gets")
	}
}

// A packet too short to sample for header protection is dropped without
// harm.
func TestCqPacketTooShortToUnprotect(t *testing.T) {
	c := frameConn(t)
	c.HandleDatagram(append(append([]byte{0x40}, c.scid...), 1, 2))
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || len(c.spaces[spaceApp].recv) != 0 {
		t.Fatalf("closed %v, %d ranges received", c.closed, len(c.spaces[spaceApp].recv))
	}
}

// A server that closes before its handshake completes has 1-RTT keys to
// write with, but the client may not yet have them to read: the close goes
// only in the spaces it can read (RFC 9000 section 10.2.3).
func TestCqCloseBeforeHandshakeComplete(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	c.handshakeComplete = false
	next := c.spaces[spaceApp].nextPN
	c.closeLocked(transportErr(errInternal, "test"))
	defer c.mu.Unlock()
	if c.spaces[spaceApp].nextPN != next {
		t.Fatal("a 1-RTT CONNECTION_CLOSE sent before the handshake completed")
	}
}

// A client that closes during its handshake sends its CONNECTION_CLOSE in an
// Initial packet, which is padded to the size every client Initial has (RFC
// 9000 section 14.1).
func TestCqClientCloseInInitialPadded(t *testing.T) {
	c, sink, _ := dialSink(t, nil)
	before := sink.count()
	c.Close(0, "")
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.sent) != before+1 {
		t.Fatalf("%d datagrams sent on close", len(sink.sent)-before)
	}
	if n := len(sink.sent[before]); n < minInitialDatagram {
		t.Fatalf("closing Initial of %d bytes", n)
	}
}

// What a server is told of the session a client resumes: early data against
// limits it never gave is turned away, and the early keys TLS would have it
// write with are none of a server's business.
func TestCqServerResumption(t *testing.T) {
	serverTLS, _ := testTLSConfigs(t)
	c := newConn(fuzzPacketConn{}, testAddr, Config{TLSConfig: serverTLS}, fuzzHandler{}, false)
	session := &tls.SessionState{EarlyData: true}
	c.tls = &scriptTLS{events: []tls.QUICEvent{
		{Kind: tls.QUICResumeSession, SessionState: session},
		{Kind: tls.QUICSetWriteSecret, Level: tls.QUICEncryptionLevelEarly, Suite: tls.TLS_AES_128_GCM_SHA256, Data: make([]byte, 32)},
	}}
	c.mu.Lock()
	err := c.processTLSEvents()
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if session.EarlyData {
		t.Fatal("early data accepted with no limits remembered")
	}
	if c.earlyRx != nil || c.earlyTx != nil {
		t.Fatal("early keys kept")
	}
}

// The peer's unidirectional streams, as they finish, earn it more: a
// MAX_STREAMS goes once half of what it may open is used up.
func TestCqUniStreamsReplenished(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config.MaxIncomingUniStreams, c.maxUni = 2, 2
	if _, err := c.handleFrames(spaceApp, frame(frameStream|0x03, 2, 2, "ab"), time.Now()); err != nil {
		t.Fatal(err)
	}
	if !c.maxStreamsUniPending || c.maxUni != 3 {
		t.Fatalf("MAX_STREAMS pending %v, limit %d", c.maxStreamsUniPending, c.maxUni)
	}
}

// Streams the peer has not let it open are refused, unidirectional as
// bidirectional, and an owned buffer written to a stream of a closed
// connection goes back to the pool.
func TestCqStreamsRefused(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	c.peerMaxUni = 0
	c.mu.Unlock()
	if _, err := c.OpenUniStream(); err != ErrStreamLimit {
		t.Fatalf("opened a unidirectional stream past the limit: %v", err)
	}
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	c.Abort(errTestOver)
	if err := s.WriteOwned(bufferpool.Get(8), false); err != ErrClosed {
		t.Fatalf("write to a closed connection: %v", err)
	}
}

// Lost flow control updates are sent again: MAX_DATA for the connection,
// and MAX_STREAM_DATA for a stream still receiving (RFC 9000 section
// 13.3), which makes the stream have something to send.
func TestCqFlowControlUpdatesLost(t *testing.T) {
	c := frameConn(t)
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.frameLost(spaceApp, &sentFrame{kind: sfMaxData})
	c.frameLost(spaceApp, &sentFrame{kind: sfMaxStreamData, stream: s})
	if !c.maxDataPending || !s.maxDataPending || !s.sendable() {
		t.Fatalf("MAX_DATA %v, MAX_STREAM_DATA %v, sendable %v", c.maxDataPending, s.maxDataPending, s.sendable())
	}
}

// A server sends nothing to an address it has not validated once it has
// sent three times what it received (RFC 9000 section 8.1).
func TestCqAmplificationLimit(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addressValidated, c.bytesRecv = false, 10
	if out := c.appendDatagram(nil, time.Now()); len(out) != 0 {
		t.Fatalf("sent %d bytes", len(out))
	}
}

// The timer, the keep-alive and the sender each have nothing to do in some
// states: a connection already closed sets no timer and ignores one that
// fires, a send while another goroutine sends is left to that one, and a
// PING is due a period after the last packet sent when that came after the
// last one heard.
func TestCqTimerEdges(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	c.config.KeepAlivePeriod = time.Second
	now := time.Now()
	c.lastActivity = now
	c.spaces[spaceApp].lastAckElicitingSent = now.Add(time.Second)
	if got := c.keepAliveAt(); !got.Equal(now.Add(2 * time.Second)) {
		t.Errorf("keep-alive due %v after now", got.Sub(now))
	}
	c.sending, c.wantFlush = true, true
	c.sendLocked()
	if !c.wantFlush {
		t.Error("a second sender took the flush")
	}
	c.sending = false
	c.mu.Unlock()
	c.Abort(errTestOver)
	c.mu.Lock()
	c.timerAt = time.Time{}
	c.armTimerLocked(time.Now())
	if !c.timerAt.IsZero() {
		t.Error("a timer set on a closed connection")
	}
	c.mu.Unlock()
	c.onTimer()
}

// Paths a server's connection moves between: going back to the path it was
// validating away from leaves the one it tried, and so does moving on from
// one still being validated, with no unvalidated path to go back to; a new host starts its round
// trip and congestion window afresh (RFC 9000 section 9.4); a path that
// fails validation with none to go back to ends the connection; and a
// connection that ends mid-validation closes the path it would go back to.
func TestCqPathEdges(t *testing.T) {
	t.Run("back to the validated path", func(t *testing.T) {
		c := frameConn(t)
		old, tried := &cqPC{}, &cqPC{}
		c.mu.Lock()
		c.pc, c.prevPC, c.prevRemote = tried, old, testAddr
		c.rxPC, c.rxRemote = old, testAddr
		c.migrateLocked(time.Now(), 0)
		givenUp := c.takeGivenUpLocked()
		pc, validated := c.pc, c.pathValidated
		c.mu.Unlock()
		if pc != old || !validated || len(givenUp) != 1 || givenUp[0] != tried {
			t.Fatalf("on %v, validated %v, gave up %v", pc, validated, givenUp)
		}
	})
	t.Run("away from a path still being validated", func(t *testing.T) {
		c := frameConn(t)
		tried, next := &cqPC{}, &cqPC{}
		c.mu.Lock()
		c.pc, c.pathValidated = tried, false
		c.rxPC, c.rxRemote = next, testAddr
		c.migrateLocked(time.Now(), 0)
		givenUp := c.takeGivenUpLocked()
		pc, prev := c.pc, c.prevPC
		c.mu.Unlock()
		if pc != next || prev != nil || len(givenUp) != 1 || givenUp[0] != tried {
			t.Fatalf("on %v, back to %v, gave up %v", pc, prev, givenUp)
		}
	})
	t.Run("new host", func(t *testing.T) {
		c := frameConn(t)
		next := &cqPC{}
		c.mu.Lock()
		c.rtt.smoothed = time.Second
		c.cc.cwnd = 1
		c.rxPC, c.rxRemote = next, &net.UDPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 1}
		c.migrateLocked(time.Now(), 0)
		defer c.mu.Unlock()
		if c.cc.cwnd != initialWindow(c.maxDatagram) || c.rtt.smoothed == time.Second {
			t.Fatalf("window %d, rtt %v kept from the old host", c.cc.cwnd, c.rtt.smoothed)
		}
		if c.pc != next || c.pathValidated || c.prevPC == nil {
			t.Fatal("not validating the new path")
		}
	})
	t.Run("failed with nowhere to go back to", func(t *testing.T) {
		c := frameConn(t)
		c.mu.Lock()
		c.prevPC = nil
		c.pathFailedLocked()
		err := c.closeErr
		c.mu.Unlock()
		if err != ErrPathValidation {
			t.Fatalf("ended with %v", err)
		}
	})
	t.Run("closed while validating", func(t *testing.T) {
		c := frameConn(t)
		prev := &cqPC{}
		c.mu.Lock()
		c.prevPC = prev
		c.mu.Unlock()
		c.Abort(errTestOver)
		if prev.closes.Load() != 1 {
			t.Fatalf("path to go back to closed %d times", prev.closes.Load())
		}
	})
}

// The receive buffer drops a stored segment that data delivered since has
// covered, and stores only the part past what was delivered of one that
// overlaps it.
func TestCqRecvBufferCoveredSegment(t *testing.T) {
	var r recvBuffer
	r.push(1, []byte("12"))
	r.push(0, []byte("0123"))
	if got := r.pop(); string(got) != "0123" {
		t.Fatalf("popped %q", got)
	}
	if got := r.pop(); got != nil || len(r.segments) != 0 {
		t.Fatalf("popped %q, %d segments left", got, len(r.segments))
	}
	// Data that overlaps what was delivered is stored less the overlap.
	r.push(2, []byte("2345"))
	if got := r.pop(); string(got) != "45" || r.buffered != 0 {
		t.Fatalf("popped %q, %d bytes left", got, r.buffered)
	}
}

// Once the most ranges are kept, a packet that starts a new range drops the
// oldest, which acknowledgements no longer need to report.
func TestCqRangeSetDropsOldest(t *testing.T) {
	var s rangeSet
	for i := 0; i <= maxAckRanges; i++ {
		if !s.add(uint64(10 + 2*i)) {
			t.Fatalf("packet %d not taken", 10+2*i)
		}
	}
	if len(s) != maxAckRanges || s[0].lo != 12 {
		t.Fatalf("%d ranges from %d", len(s), s[0].lo)
	}
}

// A 1-RTT packet that reaches a client before its 1-RTT keys is held for
// them, as packets reordered ahead of the handshake's last flight are.
func TestCqPacketHeldForKeys(t *testing.T) {
	c, _, _ := dialSink(t, nil)
	c.mu.Lock()
	pkt := append(append([]byte{0x40}, c.scid...), make([]byte, 30)...)
	c.mu.Unlock()
	c.HandleDatagram(pkt)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.buffered) != 1 {
		t.Fatalf("%d packets held", len(c.buffered))
	}
}

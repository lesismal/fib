package quic

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

// appendAckFrequencyFrame builds an ACK_FREQUENCY frame as the draft has it.
func appendAckFrequencyFrame(b []byte, seq, threshold uint64, delay time.Duration, reordering uint64) []byte {
	b = AppendVarint(b, frameAckFrequency)
	b = AppendVarint(b, seq)
	b = AppendVarint(b, threshold)
	b = AppendVarint(b, uint64(delay/time.Microsecond))
	return AppendVarint(b, reordering)
}

// A request updates how often this side acknowledges, and an older one that
// arrives after it is ignored (draft-ietf-quic-ack-frequency section 4).
func TestAckFrequencyFrame(t *testing.T) {
	c := &Conn{local: defaultParams()}
	frame := appendAckFrequencyFrame(nil, 5, 10, 25*time.Millisecond, 2)
	r := reader{b: frame[VarintLen(uint64(frameAckFrequency)):]}
	if err := c.handleAckFrequency(&r); err != nil {
		t.Fatal(err)
	}
	if !c.ackFreq.valid || c.ackFreq.seq != 5 || c.ackFreq.threshold != 10 ||
		c.ackFreq.maxDelay != 25*time.Millisecond || c.ackFreq.reordering != 2 {
		t.Fatalf("the request taken is %+v", c.ackFreq)
	}
	// An older sequence number, or the same one again, says nothing new.
	r = reader{b: appendAckFrequencyFrame(nil, 4, 1, time.Millisecond, 0)[VarintLen(uint64(frameAckFrequency)):]}
	if err := c.handleAckFrequency(&r); err != nil {
		t.Fatal(err)
	}
	if c.ackFreq.threshold != 10 || c.ackFreq.reordering != 2 {
		t.Fatalf("an older request was taken: %+v", c.ackFreq)
	}
	r = reader{b: appendAckFrequencyFrame(nil, 6, 1, 2*time.Millisecond, 0)[VarintLen(uint64(frameAckFrequency)):]}
	if err := c.handleAckFrequency(&r); err != nil {
		t.Fatal(err)
	}
	if c.ackFreq.seq != 6 || c.ackFreq.threshold != 1 || c.ackFreq.reordering != 0 {
		t.Fatalf("the newer request was not taken: %+v", c.ackFreq)
	}
}

// A requested delay that max_ack_delay cannot take, or that is below the
// min_ack_delay this side advertised, is a protocol error
// (draft-ietf-quic-ack-frequency section 4).
func TestAckFrequencyInvalidDelay(t *testing.T) {
	c := &Conn{local: defaultParams()}
	for _, delay := range []time.Duration{(1 << 14) * time.Millisecond, 20 * time.Second} {
		r := reader{b: appendAckFrequencyFrame(nil, 1, 10, delay, 1)[VarintLen(uint64(frameAckFrequency)):]}
		err := c.handleAckFrequency(&r)
		var te *TransportError
		if err == nil || !errors.As(err, &te) || te.Code != errProtocolViolation {
			t.Fatalf("a requested delay of %v gave %v, want PROTOCOL_VIOLATION", delay, err)
		}
	}
	// A delay under the advertised minimum is as invalid as one too large.
	c = &Conn{local: defaultParams()}
	c.local.minAckDelay = 5 * time.Millisecond
	r := reader{b: appendAckFrequencyFrame(nil, 1, 10, 2*time.Millisecond, 1)[VarintLen(uint64(frameAckFrequency)):]}
	if err := c.handleAckFrequency(&r); err == nil {
		t.Fatal("a delay below min_ack_delay was taken")
	}
}

// The peer's request decides when an acknowledgement is due: after its
// threshold of ack-eliciting packets, or its delay, whichever is first, and
// an out-of-order packet only when it tolerates no reordering. Without a
// request the defaults of RFC 9000 section 13.2.1 stand.
func TestAckScheduling(t *testing.T) {
	now := time.Now()
	c := &Conn{local: defaultParams()}
	c.ackFreq = ackFrequency{valid: true, threshold: 3, maxDelay: 7 * time.Millisecond, reordering: 0}
	for i := 0; i < 3; i++ {
		c.ackElicitingPacket(spaceApp, uint64(i), int64(i)-1, now)
	}
	s := &c.spaces[spaceApp]
	if s.ackNow || s.unackedEliciting != 3 || s.ackDeadline != now.Add(7*time.Millisecond) {
		t.Fatalf("after three in order: ackNow %v, unacked %d, deadline %v",
			s.ackNow, s.unackedEliciting, s.ackDeadline)
	}
	// The fourth packet since the acknowledgement is one more than asked.
	c.ackElicitingPacket(spaceApp, 3, 2, now)
	if !s.ackNow {
		t.Fatal("the threshold did not call for an acknowledgement")
	}

	// A gap is tolerated when the peer asked for no immediate
	// acknowledgement of one, and not otherwise.
	c = &Conn{local: defaultParams()}
	c.ackFreq = ackFrequency{valid: true, threshold: 10, maxDelay: 7 * time.Millisecond, reordering: 0}
	c.ackElicitingPacket(spaceApp, 0, -1, now)
	c.ackElicitingPacket(spaceApp, 5, 0, now)
	if s := &c.spaces[spaceApp]; s.ackNow {
		t.Fatal("a gap called for an acknowledgement the peer tolerates")
	}
	c = &Conn{local: defaultParams()}
	c.ackFreq = ackFrequency{valid: true, threshold: 10, maxDelay: 7 * time.Millisecond, reordering: 1}
	c.ackElicitingPacket(spaceApp, 0, -1, now)
	c.ackElicitingPacket(spaceApp, 5, 0, now)
	if s := &c.spaces[spaceApp]; !s.ackNow {
		t.Fatal("a gap did not call for an acknowledgement the peer cannot wait for")
	}

	// The default is an acknowledgement every other packet, on a gap at
	// once, after max_ack_delay if nothing else.
	c = &Conn{local: defaultParams()}
	c.ackElicitingPacket(spaceApp, 0, -1, now)
	if s := &c.spaces[spaceApp]; s.ackNow || s.ackDeadline != now.Add(c.local.maxAckDelay) {
		t.Fatalf("the first packet: ackNow %v, deadline %v", s.ackNow, s.ackDeadline)
	}
	c.ackElicitingPacket(spaceApp, 1, 0, now)
	if s := &c.spaces[spaceApp]; !s.ackNow {
		t.Fatal("the second packet did not call for an acknowledgement")
	}
	c = &Conn{local: defaultParams()}
	c.ackElicitingPacket(spaceApp, 5, 3, now)
	if s := &c.spaces[spaceApp]; !s.ackNow {
		t.Fatal("a gap did not call for an acknowledgement by default")
	}

	// Handshake packets are acknowledged at once, whatever was asked.
	c = &Conn{local: defaultParams()}
	c.ackFreq = ackFrequency{valid: true, threshold: 100, maxDelay: time.Second, reordering: 0}
	c.ackElicitingPacket(spaceInitial, 0, -1, now)
	if s := &c.spaces[spaceInitial]; !s.ackNow {
		t.Fatal("an Initial packet waited for an acknowledgement")
	}
}

// A bulk transfer asks the peer to acknowledge less often, and a request
// that fits in a little more than the bulk threshold does not
// (draft-ietf-quic-ack-frequency section 8).
func TestAckFrequencyRequestedForBulk(t *testing.T) {
	for _, tc := range []struct {
		size    int
		request bool
	}{
		{8 << 10, false},
		{256 << 10, true},
	} {
		p := newTestPair(t, 0, nil)
		p.serverH.onData = func(s *Stream, data []byte, fin bool) {
			_ = s.Write(data, fin)
		}
		payload := bytes.Repeat([]byte("x"), tc.size)
		s, err := p.client.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Write(payload, true); err != nil {
			t.Fatal(err)
		}
		waitFinished(t, p.clientH, s.ID())
		if got := p.clientH.received(s.ID()); len(got) != len(payload) {
			t.Fatalf("%d bytes of a %d-byte payload", len(got), len(payload))
		}
		p.server.mu.Lock()
		requested := p.server.ackFreqRequested
		p.server.mu.Unlock()
		if requested != tc.request {
			t.Fatalf("%d bytes: request sent %v, want %v", tc.size, requested, tc.request)
		}
		if !tc.request {
			continue
		}
		// The client took the request: an acknowledgement every ten
		// ack-eliciting packets, with a gap of two tolerated, and the ack
		// delay it already had.
		p.client.mu.Lock()
		af := p.client.ackFreq
		p.client.mu.Unlock()
		if !af.valid || af.threshold != ackFrequencyThreshold || af.reordering != ackFrequencyReordering ||
			af.maxDelay <= 0 || af.maxDelay > 25*time.Millisecond {
			t.Fatalf("the request taken by the client is %+v", af)
		}
	}
}

// A peer that did not advertise min_ack_delay is not sent the frame, and the
// transfer goes on with the acknowledgements RFC 9000 asks for: the
// extension is only ever an optimisation.
func TestAckFrequencyNotSentWithoutSupport(t *testing.T) {
	p := newTestPair(t, 0, nil)
	p.serverH.onData = func(s *Stream, data []byte, fin bool) {
		_ = s.Write(data, fin)
	}
	// The client is as one that does not know the extension.
	p.server.mu.Lock()
	p.server.peerParams.hasMinAckDelay = false
	p.server.mu.Unlock()
	payload := bytes.Repeat([]byte("x"), 256<<10)
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(payload, true); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, p.clientH, s.ID())
	if got := p.clientH.received(s.ID()); len(got) != len(payload) {
		t.Fatalf("%d bytes of a %d-byte payload", len(got), len(payload))
	}
	p.server.mu.Lock()
	requested := p.server.ackFreqRequested
	p.server.mu.Unlock()
	if requested {
		t.Fatal("a peer without min_ack_delay was sent ACK_FREQUENCY")
	}
	p.client.mu.Lock()
	af := p.client.ackFreq
	p.client.mu.Unlock()
	if af.valid {
		t.Fatal("the peer of a connection without min_ack_delay took a request")
	}
}

// min_ack_delay is encoded in microseconds, and one above max_ack_delay is
// an invalid transport parameter (draft-ietf-quic-ack-frequency section 3).
func TestMinAckDelayParameter(t *testing.T) {
	p := defaultParams()
	p.hasMinAckDelay = true
	b := p.encode()
	if len(b) == 0 {
		t.Fatal("no transport parameters")
	}
	got, err := parseParams(b, false)
	if err != nil {
		t.Fatal(err)
	}
	if !got.hasMinAckDelay || got.minAckDelay != 0 {
		t.Fatalf("min_ack_delay read as %v (advertised %v)", got.minAckDelay, got.hasMinAckDelay)
	}
	// One above max_ack_delay: 30ms against the default 25ms.
	bad := defaultParams()
	bad.hasMinAckDelay, bad.minAckDelay = true, 30*time.Millisecond
	if _, err := parseParams(bad.encode(), false); err == nil {
		t.Fatal("a min_ack_delay above max_ack_delay was taken")
	}
}

// A loss after a bulk transfer asks the peer to acknowledge every other
// packet again, so that a window the loss shrank still gets the feedback loss
// recovery needs, and that at most once a round trip
// (draft-ietf-quic-ack-frequency section 8.1).
func TestAckFrequencyAfterLoss(t *testing.T) {
	c := &Conn{local: defaultParams()}
	c.ackFreqRequested, c.ackFreqAsking = true, ackFrequencyThreshold
	base := time.Now()
	c.ackFreqAt = base
	c.rtt.smoothed = 100 * time.Millisecond
	c.askForAcks(base.Add(50 * time.Millisecond))
	if c.ackFreqPending || c.ackFreqAsking != ackFrequencyThreshold {
		t.Fatalf("a loss asked for acknowledgements within a round trip: pending %v, asking %d",
			c.ackFreqPending, c.ackFreqAsking)
	}
	c.askForAcks(base.Add(100 * time.Millisecond))
	if !c.ackFreqPending || c.ackFreqAsking != ackFrequencyLossThreshold || c.ackFreqSeq != 1 {
		t.Fatalf("the loss did not ask for more acknowledgements: pending %v, asking %d, seq %d",
			c.ackFreqPending, c.ackFreqAsking, c.ackFreqSeq)
	}
	// A second loss within the next round trip asks for nothing more.
	c.ackFreqPending = false
	c.ackFreqAsking = ackFrequencyThreshold
	c.askForAcks(base.Add(150 * time.Millisecond))
	if c.ackFreqPending {
		t.Fatal("a second request followed within the round trip")
	}
	c.askForAcks(base.Add(200 * time.Millisecond))
	if !c.ackFreqPending || c.ackFreqSeq != 2 {
		t.Fatalf("the next round trip did not ask again: pending %v, seq %d", c.ackFreqPending, c.ackFreqSeq)
	}
}

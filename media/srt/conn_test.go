package srt

import (
	"encoding/binary"
	"errors"
	"net/netip"
	"testing"
	"time"
)

// TestHandshake connects a caller to a listener and checks what they
// agreed on.
func TestHandshake(t *testing.T) {
	s := newSim(t, Config{Latency: 200 * time.Millisecond, StreamID: "#!::r=live/s,m=publish"},
		Config{Latency: 80 * time.Millisecond, FlowWindow: 100})
	s.connect()
	c, l := s.caller.conn, s.server.conn
	if c.State() != StateConnected || l.State() != StateConnected || c.State().String() != "connected" {
		t.Fatal("states")
	}
	if c.Latency() != 200*time.Millisecond || l.Latency() != 200*time.Millisecond {
		t.Fatalf("latencies %v %v", c.Latency(), l.Latency())
	}
	if l.StreamID() != "#!::r=live/s,m=publish" || c.StreamID() != l.StreamID() {
		t.Fatalf("stream ID %q", l.StreamID())
	}
	if c.PeerSocketID() != l.SocketID() || l.PeerSocketID() != c.SocketID() || c.SocketID() == 0 {
		t.Fatal("socket IDs")
	}
	if c.peerFlowWindow != 100 {
		t.Fatalf("flow window %d", c.peerFlowWindow)
	}
	if len(s.requests) != 1 || s.requests[0].Latency != 200*time.Millisecond || s.requests[0].Encrypted {
		t.Fatalf("request %+v", s.requests[0])
	}
	if c.Encrypted() || c.Err() != nil {
		t.Fatal("encrypted")
	}
}

// TestData sends both ways without loss: everything arrives in order, one
// latency after it was sent.
func TestData(t *testing.T) {
	s := newSim(t, Config{}, Config{})
	s.connect()
	start := s.now
	for i := range 100 {
		if err := s.caller.conn.Write(payload(i, 1316), s.now); err != nil {
			t.Fatal(err)
		}
		if err := s.server.conn.Write(payload(i, 188), s.now); err != nil {
			t.Fatal(err)
		}
		s.run(10 * time.Millisecond)
	}
	s.run(time.Second)
	checkData(t, s.server.data, 0, 100, nil)
	checkData(t, s.caller.data, 0, 100, nil)
	// The first payload, sent at start, is delivered one latency later
	// (give or take the delay the time base absorbed).
	if d := s.server.dataAt[0].Sub(start); d < DefaultLatency || d > DefaultLatency+2*s.delay {
		t.Fatalf("delivered after %v", d)
	}
	st := s.caller.conn.Stats()
	if st.PacketsSent != 100 || st.PacketsRetrans != 0 || st.SendBufferPackets != 0 || st.BytesSent != 131600 {
		t.Fatalf("stats %+v", st)
	}
	if s.server.conn.Stats().PacketsReceived != 100 {
		t.Fatal("received")
	}
	// ACKACKs measured the round trip.
	if rtt := s.server.conn.Stats().RTT; rtt < 2*s.delay-time.Millisecond || rtt > 100*time.Millisecond {
		t.Fatalf("RTT %v", rtt)
	}
}

// TestLossRecovery drops every 7th data packet once: NAKs bring all of
// them back in time.
func TestLossRecovery(t *testing.T) {
	s := newSim(t, Config{}, Config{})
	s.connect()
	dropped := map[uint32]bool{}
	n := 0
	s.drop = func(toServer bool, b []byte) bool {
		seq, ok := isData(b)
		if !ok || !toServer || dropped[seq] {
			return false
		}
		if n++; n%7 == 0 {
			dropped[seq] = true
			return true
		}
		return false
	}
	for i := range 200 {
		_ = s.caller.conn.Write(payload(i, 100), s.now)
		s.run(2 * time.Millisecond)
	}
	s.run(time.Second)
	checkData(t, s.server.data, 0, 200, nil)
	st := s.caller.conn.Stats()
	if st.PacketsRetrans < uint64(len(dropped)) || s.server.conn.Stats().PacketsLost != uint64(len(dropped)) {
		t.Fatalf("retrans %d lost %d dropped %d", st.PacketsRetrans, s.server.conn.Stats().PacketsLost, len(dropped))
	}
}

// TestPeriodicNAK drops the first NAK and the first retransmission: the
// periodic NAK asks again.
func TestPeriodicNAK(t *testing.T) {
	s := newSim(t, Config{}, Config{Latency: 500 * time.Millisecond})
	s.connect()
	var naks, rexmits int
	s.drop = func(toServer bool, b []byte) bool {
		if seq, ok := isData(b); ok && toServer {
			if seq == seqAdd(s.caller.conn.isn, 3) && binary.BigEndian.Uint32(b[4:])&flagRexmitPkt == 0 {
				return true
			}
			if binary.BigEndian.Uint32(b[4:])&flagRexmitPkt != 0 {
				rexmits++
				return rexmits == 1
			}
		}
		if ctrlType(b) == ctrlNAK && !toServer {
			naks++
			return naks == 1
		}
		return false
	}
	for i := range 10 {
		_ = s.caller.conn.Write(payload(i, 100), s.now)
		s.run(5 * time.Millisecond)
	}
	s.run(time.Second)
	checkData(t, s.server.data, 0, 10, nil)
	if naks < 3 || rexmits < 2 {
		t.Fatalf("NAKs %d retransmissions %d", naks, rexmits)
	}
}

// TestTooLateDrop loses a packet for good: the receiver skips it when the
// next one is due, and the sender gives up on it.
func TestTooLateDrop(t *testing.T) {
	s := newSim(t, Config{}, Config{})
	s.connect()
	lost := seqAdd(s.caller.conn.isn, 2)
	s.drop = func(toServer bool, b []byte) bool {
		seq, ok := isData(b)
		return ok && toServer && seq == lost
	}
	for i := range 6 {
		_ = s.caller.conn.Write(payload(i, 100), s.now)
		s.run(20 * time.Millisecond)
	}
	s.run(2 * time.Second)
	checkData(t, s.server.data, 0, 6, map[int]bool{2: true})
	if d := s.server.conn.Stats().PacketsRecvDrop; d != 1 {
		t.Fatalf("receiver dropped %d", d)
	}
	if len(s.server.conn.loss) != 0 {
		t.Fatal("loss list not cleared")
	}
	// The receiver's ACK past the hole emptied the sender's buffer.
	st := s.caller.conn.Stats()
	if st.SendBufferPackets != 0 {
		t.Fatalf("sender %+v", st)
	}
}

// TestSenderDrop: with ACKs lost, the sender drops what is too old and
// tells the receiver with a DROPREQ, which closes its loss list.
func TestSenderDrop(t *testing.T) {
	s := newSim(t, Config{}, Config{})
	s.connect()
	var dropreqs int
	block := true
	s.drop = func(toServer bool, b []byte) bool {
		if ctrlType(b) == ctrlDropReq {
			dropreqs++
		}
		if seq, ok := isData(b); ok && toServer && block && seq != s.caller.conn.isn {
			return true
		}
		return !toServer && block
	}
	for i := range 5 {
		_ = s.caller.conn.Write(payload(i, 100), s.now)
	}
	s.run(1500 * time.Millisecond)
	if dropreqs == 0 || s.caller.conn.Stats().PacketsSendDrop != 5 {
		t.Fatalf("DROPREQs %d, dropped %d", dropreqs, s.caller.conn.Stats().PacketsSendDrop)
	}
	block = false
	_ = s.caller.conn.Write(payload(5, 100), s.now)
	s.run(time.Second)
	checkData(t, s.server.data, 0, 6, map[int]bool{1: true, 2: true, 3: true, 4: true})
}

// TestDropReq checks the receiver's handling of DROPREQ ranges.
func TestDropReq(t *testing.T) {
	c := &Conn{rcvMax: 100}
	set := func(ranges ...lossRange) {
		c.loss = nil
		for _, r := range ranges {
			c.loss = append(c.loss, lossEntry{lossRange: r})
		}
	}
	got := func() []lossRange {
		var out []lossRange
		for _, e := range c.loss {
			out = append(out, e.lossRange)
		}
		return out
	}
	set(lossRange{10, 20}, lossRange{30, 30}, lossRange{40, 50})
	c.handleDropReq(15, 45)
	if g := got(); len(g) != 2 || g[0] != (lossRange{10, 14}) || g[1] != (lossRange{46, 50}) {
		t.Fatalf("%v", g)
	}
	c.handleDropReq(5, 3)   // backwards
	c.handleDropReq(5, 200) // past what arrived
	if len(c.loss) != 2 {
		t.Fatal("bad DROPREQs changed the list")
	}
	set(lossRange{10, 20})
	c.removeLoss(10)
	c.removeLoss(20)
	c.removeLoss(15)
	c.removeLoss(99)
	if g := got(); len(g) != 2 || g[0] != (lossRange{11, 14}) || g[1] != (lossRange{16, 19}) {
		t.Fatalf("%v", g)
	}
	set(lossRange{7, 7})
	c.removeLoss(7)
	if len(c.loss) != 0 {
		t.Fatal("single")
	}
	set(lossRange{10, 20}, lossRange{30, 40})
	c.removeLossBefore(35)
	if g := got(); len(g) != 1 || g[0] != (lossRange{35, 40}) {
		t.Fatalf("%v", g)
	}
	c.removeLossBefore(50)
	if c.loss != nil {
		t.Fatal("removeLossBefore")
	}
}

// TestEncryption connects with each key length and sends both ways.
func TestEncryption(t *testing.T) {
	for _, kl := range []int{0, 16, 24, 32} {
		s := newSim(t, Config{Passphrase: "0123456789abc", PBKeyLen: kl}, Config{Passphrase: "0123456789abc"})
		s.connect()
		if !s.caller.conn.Encrypted() || !s.server.conn.Encrypted() || !s.requests[0].Encrypted {
			t.Fatal("not encrypted")
		}
		want := max(kl, 16)
		if s.server.conn.cfg.PBKeyLen != want {
			t.Fatalf("key length %d", s.server.conn.cfg.PBKeyLen)
		}
		var plain bool
		s.drop = func(toServer bool, b []byte) bool {
			if _, ok := isData(b); ok && binary.BigEndian.Uint32(b[headerSize:]) == 7 {
				plain = true
			}
			return false
		}
		for i := range 10 {
			_ = s.caller.conn.Write(payload(i, 500), s.now)
			_ = s.server.conn.Write(payload(i, 500), s.now)
		}
		s.run(time.Second)
		checkData(t, s.server.data, 0, 10, nil)
		checkData(t, s.caller.data, 0, 10, nil)
		if plain {
			t.Fatal("plaintext on the wire")
		}
	}
}

// TestRejections covers the ways a listener refuses a caller.
func TestRejections(t *testing.T) {
	cases := []struct {
		name      string
		caller    Config
		server    Config
		acceptErr error
		want      RejectReason
	}{
		{"bad secret", Config{Passphrase: "0123456789abc"}, Config{Passphrase: "another passphrase"}, nil, RejectBadSecret},
		{"caller unencrypted", Config{}, Config{Passphrase: "0123456789abc"}, nil, RejectUnsecure},
		{"listener unencrypted", Config{Passphrase: "0123456789abc"}, Config{}, nil, RejectUnsecure},
		{"accept reason", Config{StreamID: "x"}, Config{}, RejectNotFound, RejectNotFound},
		{"accept error", Config{}, Config{}, errors.New("no"), RejectPeer},
		{"bad server config", Config{}, Config{Passphrase: "short"}, nil, RejectSystem},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSim(t, tc.caller, tc.server)
			s.acceptErr = tc.acceptErr
			s.run(time.Second)
			if s.caller.closed != 1 || s.caller.err != tc.want || s.server.conn != nil {
				t.Fatalf("closed %d with %v, want %v", s.caller.closed, s.caller.err, tc.want)
			}
			if tc.want.Error() == "" {
				t.Fatal("Error")
			}
		})
	}
}

// TestHandshakeLoss drops the first induction, the first conclusion and
// the first conclusion response: retries get through, and the listener
// answers the repeated conclusion from its Conn.
func TestHandshakeLoss(t *testing.T) {
	s := newSim(t, Config{}, Config{})
	seen := map[string]int{}
	s.drop = func(toServer bool, b []byte) bool {
		if ctrlType(b) != ctrlHandshake {
			return false
		}
		typ := binary.BigEndian.Uint32(b[headerSize+20:])
		k := map[bool]string{true: "req", false: "resp"}[toServer] + map[uint32]string{hsInduction: "ind", hsConclusion: "con"}[typ]
		seen[k]++
		return seen[k] == 1 && k != "respind"
	}
	s.run(2 * time.Second)
	if s.caller.connected != 1 || s.server.connected != 1 {
		t.Fatalf("connected %d %d: %v", s.caller.connected, s.server.connected, seen)
	}
	if seen["reqcon"] < 3 || seen["respcon"] < 2 {
		t.Fatalf("handshakes %v", seen)
	}
}

// TestHandshakeTimeout: a listener that never answers.
func TestHandshakeTimeout(t *testing.T) {
	s := newSim(t, Config{HandshakeTimeout: time.Second}, Config{})
	s.drop = func(bool, []byte) bool { return true }
	s.run(2 * time.Second)
	if s.caller.err != ErrHandshakeTimeout || s.caller.conn.State() != StateClosed {
		t.Fatal(s.caller.err)
	}
	if _, ok := s.caller.conn.NextTimeout(); ok {
		t.Fatal("timeout after close")
	}
	if err := s.caller.conn.Write([]byte{1}, s.now); err != ErrClosed {
		t.Fatal(err)
	}
}

// TestIdleAndKeepalive: keepalives hold an idle connection open; a peer
// that goes silent is timed out.
func TestIdleAndKeepalive(t *testing.T) {
	s := newSim(t, Config{PeerIdleTimeout: 2 * time.Second}, Config{PeerIdleTimeout: 2 * time.Second})
	s.connect()
	var keepalives int
	s.drop = func(toServer bool, b []byte) bool {
		if ctrlType(b) == ctrlKeepalive {
			keepalives++
		}
		return false
	}
	s.run(5 * time.Second)
	if s.caller.closed != 0 || s.server.closed != 0 || keepalives < 6 {
		t.Fatalf("closed %d %d, %d keepalives", s.caller.closed, s.server.closed, keepalives)
	}
	s.drop = func(toServer bool, b []byte) bool { return toServer }
	s.run(3 * time.Second)
	if s.server.err != ErrPeerIdle {
		t.Fatal(s.server.err)
	}
}

// TestShutdown: Close tells the peer.
func TestShutdown(t *testing.T) {
	s := newSim(t, Config{}, Config{})
	s.connect()
	s.caller.conn.Close(s.now)
	s.caller.conn.Close(s.now)
	s.run(100 * time.Millisecond)
	if s.caller.closed != 1 || s.caller.err != nil || s.server.err != ErrPeerClosed {
		t.Fatalf("%d %v %v", s.caller.closed, s.caller.err, s.server.err)
	}
	if s.caller.conn.State().String() != "closed" {
		t.Fatal("state")
	}
	// A closed Conn ignores what comes.
	s.caller.conn.Handle(make([]byte, 20), s.now)
	s.caller.conn.HandleTimeout(s.now)
}

// TestWriteErrors covers Write's refusals and the flow window.
func TestWriteErrors(t *testing.T) {
	s := newSim(t, Config{PayloadSize: 100, SendBufferPackets: 5}, Config{FlowWindow: 3})
	if err := s.caller.conn.Write([]byte{1}, s.now); err != ErrNotConnected {
		t.Fatal(err)
	}
	if s.caller.conn.State().String() != "handshake" {
		t.Fatal("state")
	}
	s.connect()
	if err := s.caller.conn.Write(make([]byte, 101), s.now); err != ErrTooLarge {
		t.Fatal(err)
	}
	// Hold every ACK back: 3 go out (the window), 2 wait, then the buffer
	// is full.
	s.drop = func(toServer bool, b []byte) bool { return !toServer }
	for i := range 5 {
		if err := s.caller.conn.Write(payload(i, 50), s.now); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.caller.conn.Write(payload(5, 50), s.now); err != ErrBufferFull {
		t.Fatal(err)
	}
	if s.caller.conn.sndSent != 3 || s.caller.conn.SendBacklog() != 250 {
		t.Fatalf("sent %d backlog %d", s.caller.conn.sndSent, s.caller.conn.SendBacklog())
	}
	s.drop = nil
	s.run(time.Second)
	checkData(t, s.server.data, 0, 5, nil)
}

// TestReceiverEdges feeds the receiver duplicates, stale and out-of-window
// packets, garbage and packets for another socket.
func TestReceiverEdges(t *testing.T) {
	s := newSim(t, Config{}, Config{FlowWindow: 16})
	s.connect()
	l, c := s.server.conn, s.caller.conn
	data := func(seq uint32, dst uint32, kk int) []byte {
		h := header{seq: seq, info: ppSolo | uint32(kk)<<kkShift | 1, ts: c.timestamp(s.now), dst: dst}
		return append(h.append(nil), payload(int(seq), 20)...)
	}
	isn := c.isn
	l.Handle(data(isn, l.SocketID(), 0), s.now)
	l.Handle(data(isn, l.SocketID(), 0), s.now)                 // duplicate
	l.Handle(data(seqAdd(isn, 100), l.SocketID(), 0), s.now)    // past the window
	l.Handle(data(seqAdd(isn, 1), l.SocketID()+1, 0), s.now)    // another socket
	l.Handle(data(seqAdd(isn, 1), l.SocketID(), kkEven), s.now) // encrypted, no key
	l.Handle([]byte{1, 2, 3}, s.now)                            // short
	s.run(time.Second)
	st := l.Stats()
	if st.PacketsDuplicate != 1 || st.PacketsRecvDrop != 1 || st.PacketsUndecrypt != 1 {
		t.Fatalf("%+v", st)
	}
	l.Handle(data(isn, l.SocketID(), 0), s.now) // delivered already
	if l.Stats().PacketsDuplicate != 2 {
		t.Fatal("stale")
	}
	if len(s.server.data) != 1 {
		t.Fatalf("%d payloads", len(s.server.data))
	}
	// Control packets that are malformed or of no interest.
	ctrl := func(typ, sub uint16, info uint32, cif []byte) []byte {
		h := header{ctrl: true, typ: typ, subtype: sub, info: info, dst: l.SocketID()}
		return append(h.append(nil), cif...)
	}
	l.Handle(ctrl(ctrlACK, 0, 1, nil), s.now)
	l.Handle(ctrl(ctrlACK, 0, 1, make([]byte, 28)), s.now) // acknowledges the past
	l.Handle(ctrl(ctrlDropReq, 0, 0, []byte{1}), s.now)
	l.Handle(ctrl(ctrlCongestion, 0, 0, nil), s.now)
	l.Handle(ctrl(ctrlPeerError, 0, 0, nil), s.now)
	l.Handle(ctrl(ctrlUser, 99, 0, nil), s.now)
	l.Handle(ctrl(ctrlACKACK, 0, 12345, nil), s.now)
	l.Handle(ctrl(ctrlHandshake, 0, 0, []byte{1}), s.now)
	if l.State() != StateConnected {
		t.Fatal("closed by garbage")
	}
}

// TestKMRefresh sends a new key mid-stream, as a rekeying sender does.
func TestKMRefresh(t *testing.T) {
	pass := "0123456789abc"
	s := newSim(t, Config{Passphrase: pass}, Config{Passphrase: pass})
	s.connect()
	c, l := s.caller.conn, s.server.conn
	// The caller switches to an odd key of the same salt and announces
	// both keys.
	km := *c.km
	km.odd = make([]byte, km.keyLen)
	km.odd[0] = 9
	msg, err := km.marshal(pass)
	if err != nil {
		t.Fatal(err)
	}
	var rsp []byte
	s.drop = func(toServer bool, b []byte) bool {
		if ctrlType(b) == ctrlUser && !toServer {
			rsp = b
		}
		return false
	}
	c.sendControl(s.now, ctrlUser, extKMReq, 0, msg)
	s.run(50 * time.Millisecond)
	if rsp == nil || binary.BigEndian.Uint16(rsp[2:]) != extKMRsp || l.crypt.odd == nil {
		t.Fatal("no KMRSP")
	}
	c.crypt, _ = newCryptor(&km)
	c.kk = kkOdd
	_ = c.Write(payload(1, 300), s.now)
	s.run(time.Second)
	checkData(t, s.server.data, 1, 2, nil)
	// A refresh with an odd key alone and another salt replaces both.
	km2, _ := newKeyMaterial(newRand(9), 16)
	km2.odd, km2.even = km2.even, nil
	msg, _ = km2.marshal(pass)
	c.sendControl(s.now, ctrlUser, extKMReq, 0, msg)
	s.run(50 * time.Millisecond)
	if l.crypt.even != nil {
		t.Fatal("even key kept across salts")
	}
	// A refresh with another passphrase is refused.
	msg, _ = km2.marshal("another passphrase")
	rsp = nil
	c.sendControl(s.now, ctrlUser, extKMReq, 0, msg)
	s.run(50 * time.Millisecond)
	if rsp == nil || len(rsp) != headerSize+4 {
		t.Fatal("no refusal")
	}
	// Without a passphrase, a refresh is ignored.
	l.cfg.Passphrase = ""
	l.handleKMRefresh(msg, s.now)
}

// TestKeyDerivationLimit checks that a listener derives KEKs at no more
// than KeyDerivations a second, dropping a conclusion past it for the
// caller to send again, and that the same salt costs one derivation.
func TestKeyDerivationLimit(t *testing.T) {
	pass := "0123456789abc"
	s := newSim(t, Config{Passphrase: pass}, Config{Passphrase: pass})
	var err error
	s.lis, err = NewListener(ListenerConfig{Accept: s.accept, Rand: newRand(1), KeyDerivations: 1})
	if err != nil {
		t.Fatal(err)
	}
	// Another caller's salt takes the second's one token.
	other, _ := newKeyMaterial(newRand(7), 16)
	if _, err := s.lis.kek.kek(other, pass, s.now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.lis.kek.kek(s.caller.conn.km, pass, s.now); err != errKEKBusy {
		t.Fatalf("a second derivation in the second: %v", err)
	}
	if _, err := s.lis.kek.kek(other, pass, s.now); err != nil {
		t.Fatalf("the same salt again: %v", err)
	}
	s.run(500 * time.Millisecond)
	if s.server.conn != nil || len(s.requests) == 0 {
		t.Fatalf("accepted past the limit: %d requests", len(s.requests))
	}
	s.run(2 * time.Second)
	if s.caller.connected != 1 || s.server.connected != 1 || !s.server.conn.Encrypted() {
		t.Fatalf("connected caller %d server %d", s.caller.connected, s.server.connected)
	}
	// A refresh under the connection's salt needs no derivation; one under
	// a new salt takes the connection's one a second.
	l := s.server.conn
	km := *s.caller.conn.km
	km.odd = make([]byte, km.keyLen)
	msg, _ := km.marshal(pass)
	l.handleKMRefresh(msg, s.now)
	if l.kmKEK != nil || l.crypt.odd == nil {
		t.Fatal("a refresh under the same salt derived its KEK")
	}
	for i := range 2 {
		km2, _ := newKeyMaterial(newRand(byte(20+i)), 16)
		msg, _ = km2.marshal(pass)
		l.handleKMRefresh(msg, s.now)
		if (l.km.salt == km2.salt) != (i == 0) {
			t.Fatalf("refresh %d under a new salt in one second: taken %v", i, l.km.salt == km2.salt)
		}
	}
}

// TestTimestampWrap runs a connection across the 32-bit timestamp wrap.
func TestTimestampWrap(t *testing.T) {
	s := newSim(t, Config{}, Config{})
	s.connect()
	// Move the caller's origin so its timestamps wrap in 200 ms.
	shift := time.Duration(1<<32)*time.Microsecond - 200*time.Millisecond - s.now.Sub(s.caller.conn.start)
	s.caller.conn.start = s.caller.conn.start.Add(-shift)
	s.server.conn.base = s.server.conn.base.Add(-shift)
	for i := range 40 {
		_ = s.caller.conn.Write(payload(i, 100), s.now)
		s.run(10 * time.Millisecond)
	}
	s.run(time.Second)
	checkData(t, s.server.data, 0, 40, nil)
	for i := 1; i < len(s.server.dataAt); i++ {
		if d := s.server.dataAt[i].Sub(s.server.dataAt[i-1]); d < 5*time.Millisecond || d > 15*time.Millisecond {
			t.Fatalf("payload %d %v after the one before", i, d)
		}
	}
	if c := s.server.conn; c.unwrapTS(0xffffff00) >= c.tsFull {
		t.Fatal("a timestamp before the wrap unwrapped after it")
	}
}

// TestDrift follows a sender whose clock runs slow: the time base moves
// with it, so packets keep their latency.
func TestDrift(t *testing.T) {
	s := newSim(t, Config{}, Config{})
	s.connect()
	l := s.server.conn
	base := l.base
	for i := range 3 * driftWindow {
		// Each packet arrives 10 µs later than its timestamp says.
		l.trackDrift(int64(i)*1000, base.Add(time.Duration(i)*time.Millisecond+time.Duration(i)*10*time.Microsecond+time.Millisecond))
	}
	if d := l.base.Sub(base); d < 2*time.Millisecond || d > 3*driftStep {
		t.Fatalf("base moved %v", d)
	}
}

// TestNAKCodec round-trips loss lists, and parseLossList stops at
// malformed ones.
func TestNAKCodec(t *testing.T) {
	ranges := []lossRange{{5, 5}, {10, 20}, {maxSeq - 2, 1}}
	var got []lossRange
	parseLossList(appendLossList(nil, ranges), func(a, b uint32) { got = append(got, lossRange{a, b}) })
	if len(got) != 3 || got[0] != ranges[0] || got[1] != ranges[1] || got[2] != ranges[2] {
		t.Fatalf("%v", got)
	}
	got = nil
	parseLossList([]byte{0x80, 0, 0, 1}, func(a, b uint32) { got = append(got, lossRange{a, b}) })
	parseLossList([]byte{0x80, 0, 0, 9, 0, 0, 0, 1}, func(a, b uint32) { got = append(got, lossRange{a, b}) })
	if got != nil {
		t.Fatalf("%v", got)
	}
}

// TestNAKOfDropped: a NAK for packets the sender no longer has gets a
// DROPREQ, and the rest of the NAK is still served.
func TestNAKOfDropped(t *testing.T) {
	s := newSim(t, Config{}, Config{})
	s.connect()
	c := s.caller.conn
	for i := range 4 {
		_ = c.Write(payload(i, 10), s.now)
	}
	isn := c.isn
	c.popSent(2) // as if acknowledged
	var dropreq []byte
	s.drop = func(toServer bool, b []byte) bool {
		if ctrlType(b) == ctrlDropReq {
			dropreq = b
		}
		return false
	}
	before := c.Stats().PacketsRetrans
	c.handleNAK(appendLossList(nil, []lossRange{{isn, seqAdd(isn, 3)}, {seqAdd(isn, 0), seqAdd(isn, 1)}}), s.now)
	if dropreq == nil || binary.BigEndian.Uint32(dropreq[headerSize:]) != isn || binary.BigEndian.Uint32(dropreq[headerSize+4:]) != seqAdd(isn, 1) {
		t.Fatal("DROPREQ")
	}
	if c.Stats().PacketsRetrans != before+2 {
		t.Fatalf("retransmitted %d", c.Stats().PacketsRetrans-before)
	}
}

// TestCallerHandshakeEdges feeds a caller handshakes it must ignore or
// refuse.
func TestCallerHandshakeEdges(t *testing.T) {
	newCaller := func() (*Conn, *error) {
		var err error
		c, e := NewCaller(Config{Rand: newRand(4), OnClose: func(e error) { err = e }}, time.Unix(0, 0))
		if e != nil {
			t.Fatal(e)
		}
		return c, &err
	}
	hsPacket := func(dst uint32, hs handshake) []byte {
		h := header{ctrl: true, typ: ctrlHandshake, dst: dst}
		return hs.append(h.append(nil))
	}
	now := time.Unix(0, 0)
	// An HSv4 induction response.
	c, err := newCaller()
	c.Handle(hsPacket(c.socketID, handshake{version: 4, typ: hsInduction}), now)
	if *err != RejectVersion {
		t.Fatal(*err)
	}
	// A data packet and a garbage handshake during the handshake.
	c, err = newCaller()
	c.Handle(make([]byte, 20), now)
	c.Handle(append((&header{ctrl: true, typ: ctrlHandshake}).append(nil), 1), now)
	// A cookie of zero, then a conclusion response without HSRSP.
	c.Handle(hsPacket(c.socketID, handshake{version: 5, extField: srtMagic, typ: hsInduction}), now)
	if c.cookie != 1 {
		t.Fatal("cookie")
	}
	c.Handle(hsPacket(c.socketID, handshake{version: 5, typ: hsConclusion}), now)
	if *err != RejectVersion {
		t.Fatal(*err)
	}
	// An encrypting caller and responses without KMRSP, or with a refusal.
	for _, km := range [][]byte{nil, {0, 0, 0, kmStateBadSecret}} {
		c, err := NewCaller(Config{Rand: newRand(5), Passphrase: "0123456789abc", OnClose: func(e error) {}}, now)
		if err != nil {
			t.Fatal(err)
		}
		var got error
		c.cfg.OnClose = func(e error) { got = e }
		c.Handle(hsPacket(c.socketID, handshake{version: 5, extField: srtMagic, typ: hsInduction, cookie: 7}), now)
		c.Handle(hsPacket(c.socketID, handshake{version: 5, typ: hsConclusion, hsReq: &hsExt{}, hsRsp: true, km: km, kmRsp: true}), now)
		if got != RejectUnsecure && got != RejectBadSecret {
			t.Fatal(got)
		}
	}
}

// TestConfigErrors checks what NewCaller refuses.
func TestConfigErrors(t *testing.T) {
	for _, cfg := range []Config{
		{Passphrase: "short"},
		{Passphrase: "0123456789abc", PBKeyLen: 20},
		{StreamID: string(make([]byte, 600))},
		{Rand: newRandFail()},
		{Rand: newRandFailAfter(8), Passphrase: "0123456789abc"},
	} {
		if _, err := NewCaller(cfg, time.Now()); err == nil {
			t.Fatalf("%+v accepted", cfg)
		}
	}
	if _, err := NewListener(ListenerConfig{}); err == nil {
		t.Fatal("listener without Accept")
	}
	if _, err := NewListener(ListenerConfig{Accept: func(*Request) (Config, error) { return Config{}, nil }, Rand: newRandFail()}); err == nil {
		t.Fatal("listener without randomness")
	}
}

type failRand struct{ left int }

func newRandFail() *failRand           { return &failRand{} }
func newRandFailAfter(n int) *failRand { return &failRand{left: n} }
func (r *failRand) Read(b []byte) (int, error) {
	if r.left >= len(b) {
		r.left -= len(b)
		return len(b), nil
	}
	return 0, errors.New("no randomness")
}

// TestListenerEdges: stale cookies, other packets, and a listener whose
// randomness fails while accepting.
func TestListenerEdges(t *testing.T) {
	cfg := Config{}
	l, _ := NewListener(ListenerConfig{Accept: func(*Request) (Config, error) { return cfg, nil }, Rand: newRand(1)})
	addr := s0addr()
	now := time.Unix(10_000, 0)
	var out [][]byte
	output := func(b []byte) { out = append(out, append([]byte(nil), b...)) }
	conclusion := func(cookie uint32, version uint32) []byte {
		hs := handshake{version: version, typ: hsConclusion, socketID: 5, cookie: cookie, hsReq: &hsExt{}}
		return hs.append((&header{ctrl: true, typ: ctrlHandshake}).append(nil))
	}
	if l.Handle(conclusion(1234, 5), addr, now, output) != nil || len(out) != 0 {
		t.Fatal("bad cookie accepted")
	}
	// The cookie of the previous minute is still good; two minutes ago is
	// not.
	if l.Handle(conclusion(l.cookie(addr, now.Add(-cookieBucket)), 5), addr, now, output) == nil {
		t.Fatal("last minute's cookie refused")
	}
	if l.Handle(conclusion(l.cookie(addr, now.Add(-2*cookieBucket)), 5), addr, now, output) != nil {
		t.Fatal("stale cookie accepted")
	}
	// HSv4 conclusions are refused.
	out = nil
	l.Handle(conclusion(l.cookie(addr, now), 4), addr, now, output)
	if len(out) != 1 || binary.BigEndian.Uint32(out[0][headerSize+20:]) != hsRejectBase+uint32(RejectVersion) {
		t.Fatal("HSv4 not refused")
	}
	// Data and non-handshake control packets are ignored.
	if l.Handle(make([]byte, 20), addr, now, output) != nil {
		t.Fatal("data")
	}
	// Randomness failing for the socket ID.
	cfg.Rand = newRandFail()
	out = nil
	if l.Handle(conclusion(l.cookie(addr, now), 5), addr, now, output) != nil || len(out) != 1 {
		t.Fatal("accepted without a socket ID")
	}
	if _, _, ok := IsHandshake([]byte{1}); ok {
		t.Fatal("IsHandshake")
	}
	// IPv6 peers get their address back in the handshake.
	if ip := peerIP(netipMust("[2001:db8::1]:9")); ip[3] != 0x20 {
		t.Fatalf("%x", ip)
	}
}

func s0addr() netip.AddrPort { return netip.MustParseAddrPort("192.0.2.9:1000") }

func netipMust(s string) netip.AddrPort { return netip.MustParseAddrPort(s) }

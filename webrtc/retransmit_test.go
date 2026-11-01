//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package webrtc

import (
	"bytes"
	"encoding/binary"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/webrtc/rtp"
	"github.com/lesismal/fib/webrtc/sdp"
)

func TestRetransmitConfigDefaults(t *testing.T) {
	for _, c := range []struct{ in, want int }{{0, 0}, {-3, 0}, {1, 1}, {300, 512}, {512, 512}, {100000, 4096}} {
		rc := RetransmitConfig{Packets: c.in}
		rc.defaults()
		if rc.Packets != c.want {
			t.Fatalf("Packets %d: %d, want %d", c.in, rc.Packets, c.want)
		}
		if c.want == 0 && (rc.MaxAge != 0 || rc.MaxBytesPerSecond != 0) {
			t.Fatalf("defaults of a disabled config: %+v", rc)
		}
		if c.want > 0 && (rc.MaxAge != defaultRetransmitAge || rc.MaxBytesPerSecond != defaultRetransmitRate) {
			t.Fatalf("defaults: %+v", rc)
		}
	}
	rc := RetransmitConfig{Packets: 8, MaxAge: time.Minute, MaxBytesPerSecond: -1}
	rc.defaults()
	if rc.MaxAge != time.Minute || rc.MaxBytesPerSecond != -1 {
		t.Fatalf("explicit values changed: %+v", rc)
	}
}

func TestRTXPayloadTypes(t *testing.T) {
	m := rtxPayloadTypes(DefaultCodecs())
	want := map[uint8]uint8{96: 97, 98: 99, 102: 103, 108: 109, 112: 113, 127: 100, 45: 46}
	if len(m) != len(want) {
		t.Fatalf("%v, want %v", m, want)
	}
	for k, v := range want {
		if m[k] != v {
			t.Fatalf("%v, want %v", m, want)
		}
	}
	// Every dynamic payload type taken: no RTX for the last codec.
	var codecs []Codec
	for pt := 35; pt <= 127; pt++ {
		if pt < 64 || pt >= 96 {
			codecs = append(codecs, Codec{MimeType: "video/X", ClockRate: 90000, PayloadType: uint8(pt)})
		}
	}
	codecs = append(codecs,
		Codec{MimeType: "video/VP8", ClockRate: 90000, PayloadType: 96, Feedback: []string{"nack"}},
		Codec{MimeType: "video/rtx", ClockRate: 90000, PayloadType: 97, Feedback: []string{"nack"}})
	if m := rtxPayloadTypes(codecs); len(m) != 0 {
		t.Fatalf("%v with no payload type free", m)
	}
	if hasNACK([]string{"nack pli"}) || !hasNACK([]string{"goog-remb", " NACK "}) {
		t.Fatal("hasNACK")
	}
	rtx := remoteRTX([]sdp.Codec{
		{PayloadType: 97, Name: "rtx", Fmtp: "apt=96"},
		{PayloadType: 99, Name: "RTX"},
		{PayloadType: 101, Name: "rtx", Fmtp: "apt=300"},
		{PayloadType: 96, Name: "VP8"},
	})
	if len(rtx) != 1 || rtx[96] != 97 {
		t.Fatalf("remote RTX %v", rtx)
	}
}

// rtxBench is a sender with a history, unconnected: what it sends goes to
// sent, decrypted by in.
type rtxBench struct {
	pc    *PeerConnection
	s     *RTPSender
	track *TrackLocal
	sent  [][]byte
}

func newRTXBench(t *testing.T, rc RetransmitConfig) *rtxBench {
	t.Helper()
	out, _ := srtpPair(t)
	b := &rtxBench{}
	pc := &PeerConnection{cfg: Configuration{Retransmit: rc}, srtpOut: out, senders: map[uint32]*RTPSender{}}
	pc.cfg.Retransmit.defaults()
	tr := &Transceiver{pc: pc, kind: MediaKindVideo, extIDs: map[string]uint8{rtp.ExtensionURIAbsSendTime: 3, rtp.ExtensionURIMid: 1}, midBytes: []byte("0")}
	s := &RTPSender{pc: pc, t: tr, ssrc: 1, pt: 96, hasPT: true}
	tr.sender = s
	pc.senders[1] = s
	b.track = NewTrackLocal(Codec{MimeType: "video/VP8"}, "", "")
	s.track = b.track
	b.track.addSender(s)
	s.setHistory(true)
	pc.sendHook = func(p []byte) bool {
		b.sent = append(b.sent, append([]byte(nil), p...))
		return false
	}
	b.pc, b.s = pc, s
	return b
}

func (b *rtxBench) write(t *testing.T, from, to uint16, size int) {
	t.Helper()
	for seq := from; seq != to+1; seq++ {
		p := rtp.Packet{Header: rtp.Header{PayloadType: 100, SequenceNumber: seq, Timestamp: uint32(seq)}, Payload: bytes.Repeat([]byte{byte(seq)}, size)}
		if err := b.track.WritePacket(&p); err != nil {
			t.Fatal(err)
		}
	}
}

// nack sends the sender a NACK of lost, and returns what it sent again.
func (b *rtxBench) nack(t *testing.T, lost ...uint16) [][]byte {
	t.Helper()
	pkt, err := (&rtp.NACK{SenderSSRC: 9, MediaSSRC: b.s.ssrc, Pairs: rtp.AppendNACKPairs(nil, lost)}).Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	b.sent = nil
	h, p, _, err := rtp.NextRTCP(pkt)
	if err != nil {
		t.Fatal(err)
	}
	b.pc.nacked(h, p, time.Now())
	return b.sent
}

func seqOf(p []byte) uint16 { return binary.BigEndian.Uint16(p[2:]) }

// TestRetransmitHistory resends what the history holds, the latest
// Packets of them, once per round trip, within the age and rate bounds.
func TestRetransmitHistory(t *testing.T) {
	b := newRTXBench(t, RetransmitConfig{Packets: 3, MaxBytesPerSecond: -1})
	b.write(t, 1, 10, 100)
	originals := b.sent
	if len(originals) != 10 || len(b.track.hist.slots) != 4 {
		t.Fatalf("%d sent, %d slots", len(originals), len(b.track.hist.slots))
	}
	got := b.nack(t, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	if len(got) != 4 {
		t.Fatalf("%d resent, want the 4 the history keeps", len(got))
	}
	for i, p := range got {
		// A plain retransmission is the very packet sent before.
		if seqOf(p) != uint16(7+i) || !bytes.Equal(p, originals[6+i]) {
			t.Fatalf("resent %d is not the original", seqOf(p))
		}
	}
	if n, by := b.s.Retransmitted(); n != 4 || by == 0 {
		t.Fatalf("Retransmitted %d, %d", n, by)
	}
	// Not again within a round trip, but after it.
	if got := b.nack(t, 10); len(got) != 0 {
		t.Fatal("resent twice within a round trip")
	}
	b.s.sent[10&b.s.smask].resent -= int64(time.Second)
	if got := b.nack(t, 10); len(got) != 1 {
		t.Fatal("not resent a round trip later")
	}
	// Too old.
	b.s.sent[9&b.s.smask].resent = 0
	b.s.sent[8&b.s.smask].resent = 0
	b.track.hist.slots[9&b.track.hist.mask].at -= int64(2 * time.Second)
	if got := b.nack(t, 9); len(got) != 0 {
		t.Fatal("resent a packet past MaxAge")
	}
	// A sequence number the track wrote after the sender's record: not
	// what the sender sent.
	b.s.sent[8&b.s.smask].id = 1
	if got := b.nack(t, 8); len(got) != 0 {
		t.Fatal("resent another packet than the one sent")
	}

	// A NACK with RTCP padding, of a stream the PeerConnection does not
	// send, or of one that is stopped, resends nothing.
	pkt, _ := (&rtp.NACK{MediaSSRC: 1, Pairs: []rtp.NACKPair{{PacketID: 7}}}).Append(nil)
	pkt = append(pkt, 0, 0, 0, 4)
	pkt[0] |= 0x20
	binary.BigEndian.PutUint16(pkt[2:], uint16(len(pkt)/4-1))
	h, p, _, _ := rtp.NextRTCP(pkt)
	b.sent = nil
	b.s.sent[7&b.s.smask].resent = 0
	b.pc.nacked(h, p, time.Now())
	if len(b.sent) != 1 {
		t.Fatalf("%d resent for a padded NACK", len(b.sent))
	}
	binary.BigEndian.PutUint32(p[8:], 2)
	b.pc.nacked(h, p, time.Now())
	b.s.clearPayloadType()
	binary.BigEndian.PutUint32(p[8:], 1)
	b.pc.nacked(h, p, time.Now())
	if len(b.sent) != 1 {
		t.Fatal("resent for another stream or a stopped one")
	}
}

// TestRetransmitUnmarshalable sends nothing for a packet that cannot be
// written, the first time or again.
func TestRetransmitUnmarshalable(t *testing.T) {
	b := newRTXBench(t, RetransmitConfig{Packets: 4})
	p := rtp.Packet{Header: rtp.Header{SequenceNumber: 1, CSRC: make([]uint32, 16)}}
	_ = b.track.WritePacket(&p)
	if len(b.sent) != 0 {
		t.Fatal("sent a packet with 16 CSRCs")
	}
	b.write(t, 2, 2, 10)
	b.s.pt = 200
	b.pc.retransmit(b.s, 2, time.Now())
	if n, _ := b.s.Retransmitted(); n != 0 {
		t.Fatal("resent under a payload type that cannot be written")
	}
}

// TestRetransmitRate bounds the bytes resent per second.
func TestRetransmitRate(t *testing.T) {
	b := newRTXBench(t, RetransmitConfig{Packets: 16, MaxBytesPerSecond: 1})
	b.write(t, 1, 4, 1200)
	if got := b.nack(t, 1, 2, 3, 4); len(got) != 2 {
		t.Fatalf("%d resent, want the 2 a burst of %d bytes holds", len(got), minRetransmitBurst)
	}
	// A second later the bucket has a byte more.
	b.s.tokenAt -= int64(time.Second)
	if got := b.nack(t, 3); len(got) != 0 {
		t.Fatal("resent past the rate")
	}
	b.pc.cfg.Retransmit.MaxBytesPerSecond = 10 << 20
	b.s.tokenAt -= int64(time.Second)
	if got := b.nack(t, 3, 4); len(got) != 2 {
		t.Fatalf("%d resent once the bucket refilled", len(got))
	}
}

// TestRetransmitRTX resends as RTX: on the RTX SSRC and payload type, with
// sequence numbers of its own and the original one ahead of the payload.
func TestRetransmitRTX(t *testing.T) {
	b := newRTXBench(t, RetransmitConfig{Packets: 8})
	b.s.rtxSSRC, b.s.rtxPT, b.s.rtxSeq = 2, 97, 0xffff
	p := rtp.Packet{Header: rtp.Header{SequenceNumber: 5, Timestamp: 9, Marker: true, CSRC: []uint32{7}},
		Payload: []byte{1, 2, 3}, PaddingSize: 4}
	if err := b.track.WritePacket(&p); err != nil {
		t.Fatal(err)
	}
	p.SequenceNumber = 6
	_ = b.track.WritePacket(&p)
	got := b.nack(t, 5, 6)
	if len(got) != 2 {
		t.Fatalf("%d resent", len(got))
	}
	_, in := srtpPair(t)
	for i, want := range []uint16{0xffff, 0} {
		dec, err := in.UnprotectRTP(nil, got[i])
		if err != nil {
			t.Fatal(err)
		}
		var r rtp.Packet
		if err := r.Unmarshal(dec); err != nil {
			t.Fatal(err)
		}
		if r.SSRC != 2 || r.PayloadType != 97 || r.SequenceNumber != want || !r.Marker || r.Timestamp != 9 ||
			len(r.CSRC) != 1 || r.PaddingSize != 0 || !bytes.Equal(r.Payload, []byte{0, byte(5 + i), 1, 2, 3}) ||
			string(r.GetExtension(1)) != "0" || r.GetExtension(3) == nil {
			t.Fatalf("RTX packet %+v", r)
		}
	}
}

// TestRetransmitHistoryShared keeps one history per track, for every
// sender of it, until the last one stops.
func TestRetransmitHistoryShared(t *testing.T) {
	b := newRTXBench(t, RetransmitConfig{Packets: 4})
	other := &RTPSender{pc: b.pc, t: b.s.t, ssrc: 3, track: b.track}
	other.setHistory(true)
	if b.track.hist.users != 2 {
		t.Fatalf("%d users", b.track.hist.users)
	}
	// A sender that asks for more grows it.
	big := &PeerConnection{cfg: Configuration{Retransmit: RetransmitConfig{Packets: 16}}}
	third := &RTPSender{pc: big, t: b.s.t, ssrc: 4, track: b.track}
	third.setHistory(true)
	b.write(t, 1, 2, 10)
	if len(b.track.hist.slots) != 16 || len(third.sent) != 16 {
		t.Fatalf("%d slots, %d records", len(b.track.hist.slots), len(third.sent))
	}
	third.setHistory(true)
	if b.track.hist.users != 3 {
		t.Fatal("setting the history again retains it again")
	}
	third.setHistory(false)
	other.setHistory(false)
	if b.track.hist == nil || !b.track.histOn.Load() {
		t.Fatal("history gone with a user left")
	}
	b.s.detach()
	if b.track.hist != nil || b.track.histOn.Load() || b.s.sent != nil {
		t.Fatal("history kept with no user")
	}
	b.track.release()
	if id := b.track.remember(&rtp.Packet{}, nil); id != 0 {
		t.Fatal("remembered without a history")
	}
	if _, _, _, ok := b.track.lookup(1, 1, nil); ok {
		t.Fatal("looked up without a history")
	}
	// A packet the history cannot marshal is not kept.
	b.track.retain(4)
	if id := b.track.remember(&rtp.Packet{Header: rtp.Header{PayloadType: 200}}, nil); id != 0 {
		t.Fatal("remembered a packet that cannot be marshaled")
	}
}

// TestRoundTripFromReports measures the round trip time from the report
// blocks that echo a sender report, and resends no sooner than it.
func TestRoundTripFromReports(t *testing.T) {
	b := newRTXBench(t, RetransmitConfig{Packets: 4})
	out, in := srtpPair(t)
	b.pc.srtpIn = in
	now := time.Now()
	lsr := uint32(rtp.NTPTime(now.Add(-50*time.Millisecond)) >> 16)
	report := func(p rtp.RTCP, want time.Duration) {
		t.Helper()
		pkt, _ := rtp.AppendRTCP(nil, p)
		pkt, err := out.ProtectRTCP(nil, pkt)
		if err != nil {
			t.Fatal(err)
		}
		b.pc.handleRTCP(pkt, now)
		if b.s.rtt < want-time.Millisecond || b.s.rtt > want+time.Millisecond {
			t.Fatalf("round trip %v, want %v", b.s.rtt, want)
		}
	}
	report(&rtp.SenderReport{SSRC: 9, Reports: []rtp.ReceptionReport{{SSRC: 1, LastSenderReport: lsr, Delay: 65536 / 50}}}, 30*time.Millisecond)
	report(&rtp.ReceiverReport{SSRC: 9, Reports: []rtp.ReceptionReport{
		{SSRC: 1, LastSenderReport: lsr, Delay: 65536 / 100},
		{SSRC: 1},
		{SSRC: 77, LastSenderReport: lsr},
	}}, 40*time.Millisecond)
	b.write(t, 1, 1, 10)
	b.nack(t, 1)
	b.s.sent[1].resent -= int64(30 * time.Millisecond)
	if len(b.nack(t, 1)) != 0 {
		t.Fatal("resent within the round trip")
	}
	b.s.sent[1].resent -= int64(20 * time.Millisecond)
	if len(b.nack(t, 1)) != 1 {
		t.Fatal("not resent after the round trip")
	}
}

// TestRTXReceive turns RTX packets back into the packets they repair, for
// the stream the FID group, the repaired RID or the transceiver's only
// stream says.
func TestRTXReceive(t *testing.T) {
	tr := &Transceiver{codecs: []negotiatedCodec{{recvPT: 96, rtxRecv: 97}}}
	if _, ok := tr.aptOf(96); ok {
		t.Fatal("96 taken for RTX")
	}
	a := &TrackRemote{t: tr, ssrc: 10, rid: "a"}
	bb := &TrackRemote{t: tr, ssrc: 11, rid: "b"}
	pc := &PeerConnection{tracks: map[uint32]*TrackRemote{10: a, 11: bb}, rtxPrimary: map[uint32]uint32{20: 10, 21: 99},
		extIDs: map[string]uint8{rtp.ExtensionURIRepairedRid: 5}}
	tr.tracks = []*TrackRemote{a, bb}
	h := &rtp.Header{SSRC: 20}
	if pc.rtxTrack(tr, h) != a {
		t.Fatal("by FID")
	}
	h.SSRC = 21
	if pc.rtxTrack(tr, h) != nil {
		t.Fatal("FID of a stream not there")
	}
	h.SSRC = 22
	_ = h.SetExtension(5, []byte("b"))
	if pc.rtxTrack(tr, h) != bb {
		t.Fatal("by repaired RID")
	}
	_ = h.SetExtension(5, []byte("c"))
	if pc.rtxTrack(tr, h) != nil {
		t.Fatal("unknown repaired RID")
	}
	h2 := &rtp.Header{SSRC: 22}
	if pc.rtxTrack(tr, h2) != nil {
		t.Fatal("two streams and nothing to choose by")
	}
	tr.tracks = tr.tracks[:1]
	if pc.rtxTrack(tr, h2) != a {
		t.Fatal("the only stream")
	}

	// The RTX packet of 0x1234, with padding, and one of padding only.
	src := rtp.Packet{Header: rtp.Header{PayloadType: 97, SequenceNumber: 1, Timestamp: 5, SSRC: 22, Marker: true},
		Payload: []byte{0x12, 0x34, 9, 9}, PaddingSize: 3}
	_ = src.SetExtension(1, []byte("x"))
	raw, _ := src.Append(nil)
	var rh rtp.Header
	n, _ := rh.Unmarshal(raw)
	out := unwrapRTX(raw, &rh, n, 96, a)
	var got rtp.Packet
	if err := got.Unmarshal(out); err != nil {
		t.Fatal(err)
	}
	if got.SSRC != 10 || got.PayloadType != 96 || got.SequenceNumber != 0x1234 || !got.Marker ||
		got.Timestamp != 5 || !bytes.Equal(got.Payload, []byte{9, 9}) || got.PaddingSize != 3 || string(got.GetExtension(1)) != "x" {
		t.Fatalf("unwrapped %+v", got)
	}
	src.Payload = []byte{1}
	raw, _ = src.Append(nil)
	n, _ = rh.Unmarshal(raw)
	if unwrapRTX(raw, &rh, n, 96, a) != nil {
		t.Fatal("unwrapped padding")
	}

	// Received: an RTX packet goes to the stream it repairs, one of
	// another payload type or of padding only nowhere.
	sout, sin := srtpPair(t)
	a.ready.Store(true)
	a.OnRTP(func([]byte) {})
	pc.srtpIn, pc.rtxIn, pc.cfg.MaxRemoteSSRCs = sin, map[uint32]*TrackRemote{20: a}, 8
	for i, c := range []struct {
		pt      uint8
		payload []byte
	}{{97, []byte{0, 7, 1}}, {98, []byte{0, 8, 1}}, {97, []byte{1}}} {
		p := rtp.Packet{Header: rtp.Header{PayloadType: c.pt, SequenceNumber: uint16(i), SSRC: 20}, Payload: c.payload}
		raw, _ := p.Append(nil)
		b, err := sout.ProtectRTP(nil, raw)
		if err != nil {
			t.Fatal(err)
		}
		pc.handleRTP(b, time.Now())
	}
	if len(pc.events) != 1 {
		t.Fatalf("%d packets delivered", len(pc.events))
	}
	var got2 rtp.Packet
	if err := got2.Unmarshal(*pc.events[0].buf); err != nil || got2.SSRC != 10 || got2.SequenceNumber != 7 || got2.PayloadType != 96 {
		t.Fatalf("delivered %+v", got2.Header)
	}
}

// TestRTXNegotiation offers RTX with an SSRC group when retransmission is
// on, accepts it only then, and answers NACKs without it when the peer
// takes none.
func TestRTXNegotiation(t *testing.T) {
	e := startEngine(t)
	m := loopbackMux(t, e)
	rc := RetransmitConfig{Packets: 64}
	cases := []struct {
		name     string
		a, b     RetransmitConfig
		offerRTX bool
		rtx      bool
		nack     bool
	}{
		{"both", rc, rc, true, true, true},
		{"answerer without", rc, RetransmitConfig{}, true, false, true},
		{"offerer without", RetransmitConfig{}, rc, false, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			a := newPC(t, Configuration{Mux: m, Retransmit: c.a})
			b := newPC(t, Configuration{Mux: m, Retransmit: c.b})
			sa, err := a.AddTrack(NewTrackLocal(Codec{MimeType: "video/VP8"}, "v", "s"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := a.AddTrack(NewTrackLocal(Codec{MimeType: "audio/opus"}, "a", "s")); err != nil {
				t.Fatal(err)
			}
			sb, err := b.AddTrack(NewTrackLocal(Codec{MimeType: "video/VP8"}, "bv", "bs"))
			if err != nil {
				t.Fatal(err)
			}
			negotiate(t, a, b, nil)
			offer, answer := a.LocalDescription().SDP, b.LocalDescription().SDP
			if got := strings.Contains(offer, "a=rtpmap:97 rtx/90000") && strings.Contains(offer, "a=fmtp:97 apt=96") &&
				strings.Contains(offer, "a=ssrc-group:FID "); got != c.offerRTX {
				t.Fatalf("offer has RTX: %v\n%s", got, offer)
			}
			if got := strings.Contains(answer, "rtx/90000") && strings.Contains(answer, "a=ssrc-group:FID "); got != c.rtx {
				t.Fatalf("answer has RTX: %v\n%s", got, answer)
			}
			if got := sa.rtxPT == 97; got != c.rtx {
				t.Fatalf("offerer's RTX payload type %d", sa.rtxPT)
			}
			if got := sa.sent != nil; got != c.nack {
				t.Fatalf("offerer answers NACKs: %v", got)
			}
			if c.rtx && (sb.rtxPT != 97 || sb.sent == nil) {
				t.Fatalf("answerer's RTX payload type %d", sb.rtxPT)
			}
			// Again, from what was negotiated.
			negotiate(t, a, b, nil)
			if got := strings.Contains(a.LocalDescription().SDP, "apt=96"); got != c.rtx {
				t.Fatalf("second offer has RTX: %v", got)
			}
			if got := sa.rtxPT == 97; got != c.rtx {
				t.Fatalf("offerer's RTX payload type %d after renegotiating", sa.rtxPT)
			}
		})
	}
}

// TestRetransmitOverLoss sends a stream between two PeerConnections with
// some of its packets lost on the way, which the receiver asks for again
// until it has every one, as RTX or as they were.
func TestRetransmitOverLoss(t *testing.T) {
	t.Run("rtx", func(t *testing.T) { retransmitOverLoss(t, true) })
	t.Run("plain", func(t *testing.T) { retransmitOverLoss(t, false) })
}

func retransmitOverLoss(t *testing.T, withRTX bool) {
	const n = 300
	e := startEngine(t)
	rc := RetransmitConfig{Packets: 512}
	acfg := Configuration{Mux: loopbackMux(t, e), Retransmit: rc, RTCPReportInterval: 20 * time.Millisecond}
	bcfg := Configuration{Engine: e, ListenAddr: "127.0.0.1:0", RTCPReportInterval: 20 * time.Millisecond}
	if withRTX {
		bcfg.Retransmit = rc
	}
	a, b := newPC(t, acfg), newPC(t, bcfg)
	trickle(a, b)
	track := NewTrackLocal(Codec{MimeType: "video/VP8"}, "v", "s")
	sa, err := a.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}

	// Every seventh packet is lost the first time it is sent.
	var mu sync.Mutex
	dropped := map[uint16][]byte{}
	resent := map[uint16][]byte{}
	rtxSeen := 0
	a.mu.Lock()
	a.sendHook = func(p []byte) bool {
		if len(p) < rtp.HeaderSize || p[0]>>6 != 2 || rtp.IsRTCP(p) {
			return true
		}
		mu.Lock()
		defer mu.Unlock()
		seq := seqOf(p)
		switch binary.BigEndian.Uint32(p[8:]) {
		case sa.ssrc:
			if dropped[seq] != nil {
				resent[seq] = append([]byte(nil), p...)
			} else if seq%7 == 3 {
				dropped[seq] = append([]byte(nil), p...)
				return false
			}
		case sa.rtxSSRC:
			rtxSeen++
		}
		return true
	}
	a.mu.Unlock()

	got := map[uint16]bool{}
	var last uint16
	tracks := make(chan *TrackRemote, 1)
	b.OnTrack(func(tr *TrackRemote) {
		tr.OnRTP(func(pkt []byte) {
			var p rtp.Packet
			if p.Unmarshal(pkt) != nil || p.SSRC != tr.SSRC() || len(p.Payload) < 2 ||
				binary.BigEndian.Uint16(p.Payload) != p.SequenceNumber || p.PayloadType != 96 {
				t.Errorf("received %+v", p.Header)
				return
			}
			mu.Lock()
			got[p.SequenceNumber] = true
			last = max(last, p.SequenceNumber)
			mu.Unlock()
		})
		tracks <- tr
	})
	ca := connectedCh(a)
	negotiate(t, a, b, nil)
	wait(t, ca, "connected")
	if strings.Contains(b.LocalDescription().SDP, "rtx/") != withRTX {
		t.Fatalf("answer:\n%s", b.LocalDescription().SDP)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	defer func() {
		close(stop)
		wg.Wait()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for seq := uint16(1); seq <= n; seq++ {
			payload := binary.BigEndian.AppendUint16(nil, seq)
			p := rtp.Packet{Header: rtp.Header{SequenceNumber: seq, Timestamp: uint32(seq) * 3000}, Payload: append(payload, make([]byte, 500)...)}
			_ = track.WritePacket(&p)
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	var tr *TrackRemote
	select {
	case tr = <-tracks:
	case <-time.After(waitTimeout):
		t.Fatal("no track")
	}

	// The receiver asks for what it misses until it has everything.
	deadline := time.Now().Add(waitTimeout)
	for {
		mu.Lock()
		var lost []uint16
		for seq := uint16(1); seq < last; seq++ {
			if !got[seq] {
				lost = append(lost, seq)
			}
		}
		complete := len(got) == n
		mu.Unlock()
		if complete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("still missing %v", lost)
		}
		if len(lost) > 0 {
			pkt, _ := (&rtp.NACK{SenderSSRC: 1, MediaSSRC: tr.SSRC(), Pairs: rtp.AppendNACKPairs(nil, lost)}).Append(nil)
			_ = b.WriteRTCP(pkt)
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	packets, _ := sa.Retransmitted()
	if len(dropped) != (n-3)/7+1 || packets < uint64(len(dropped)) {
		t.Fatalf("%d dropped, %d retransmitted", len(dropped), packets)
	}
	if withRTX {
		if rtxSeen == 0 || len(resent) != 0 {
			t.Fatalf("%d RTX packets, %d plain", rtxSeen, len(resent))
		}
		return
	}
	if rtxSeen != 0 || len(resent) != len(dropped) {
		t.Fatalf("%d RTX packets, %d plain", rtxSeen, len(resent))
	}
	for seq, p := range resent {
		if !bytes.Equal(p, dropped[seq]) {
			t.Fatalf("packet %d resent other than it was sent", seq)
		}
	}
}

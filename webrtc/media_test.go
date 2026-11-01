//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package webrtc

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/webrtc/rtp"
)

// TestStaticPayloadTypeZero sends G.711 µ-law, whose static payload type
// is 0: the sender writes it like any other.
func TestStaticPayloadTypeZero(t *testing.T) {
	pcmu := Codec{MimeType: "audio/PCMU", ClockRate: 8000, PayloadType: 0}
	e := startEngine(t)
	a := newPC(t, Configuration{Engine: e, ListenAddr: "127.0.0.1:0", Codecs: []Codec{pcmu}})
	b := newPC(t, Configuration{Engine: e, ListenAddr: "127.0.0.1:0", Codecs: []Codec{pcmu}})
	trickle(a, b)
	track := NewTrackLocal(pcmu, "a", "s")
	s, err := a.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	rb := collect(b)
	ca := connectedCh(a)
	negotiate(t, a, b, nil)
	wait(t, ca, "a connected")
	stop := make(chan struct{})
	defer close(stop)
	go fakeRTP(track, 0, stop)
	got := nextTrack(t, rb)
	if got.ssrc != s.SSRC() || got.pt != 0 || got.track.Codec().Name() != "PCMU" {
		t.Fatalf("ssrc %x pt %d codec %+v", got.ssrc, got.pt, got.track.Codec())
	}
}

// forwarded writes packets to track as a forwarder would: with header
// extensions of a source's ids, padding, and every other sequence number
// missing, as if lost on the way in. The payload's first byte is mark.
func forwarded(track *TrackLocal, mark byte, stop <-chan struct{}) {
	var seq uint16
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
		}
		seq += 2
		p := rtp.Packet{Header: rtp.Header{PayloadType: 100, SequenceNumber: seq, Timestamp: uint32(seq) * 3000, SSRC: 0x5678, Marker: true},
			Payload: append([]byte{mark}, bytes.Repeat([]byte{1}, 200)...), PaddingSize: 4}
		_ = p.SetExtension(9, []byte{0, 0, 7})
		_ = p.SetExtension(1, []byte("source mid"))
		_ = track.WritePacket(&p)
	}
}

// packets collects the RTP packets of each track pc receives.
type packets struct {
	tracks chan *TrackRemote
	mu     sync.Mutex
	got    map[uint32][][]byte
}

func receivePackets(pc *PeerConnection) *packets {
	r := &packets{tracks: make(chan *TrackRemote, 4), got: make(map[uint32][][]byte)}
	pc.OnTrack(func(tr *TrackRemote) {
		tr.OnRTP(func(b []byte) {
			r.mu.Lock()
			if len(r.got[tr.SSRC()]) < 1000 {
				r.got[tr.SSRC()] = append(r.got[tr.SSRC()], append([]byte(nil), b...))
			}
			r.mu.Unlock()
		})
		r.tracks <- tr
	})
	return r
}

// next waits for a packet of ssrc that match accepts.
func (r *packets) next(t *testing.T, ssrc uint32, match func(*rtp.Packet) bool) *rtp.Packet {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		got := r.got[ssrc]
		r.got[ssrc] = nil
		r.mu.Unlock()
		for _, b := range got {
			var p rtp.Packet
			if p.Unmarshal(b) == nil && match(&p) {
				return &p
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no matching packet of SSRC %x", ssrc)
	return nil
}

// TestMediaForwarding sends a forwarded track, with header extensions the
// session negotiated under other ids, and checks what the receiver gets,
// what it reports back, and how the sender responds to a FIR, a replaced
// track and a stopped transceiver.
func TestMediaForwarding(t *testing.T) {
	e := startEngine(t)
	exts := append(DefaultHeaderExtensions(), HeaderExtension{URI: rtp.ExtensionURITransmissionTime, Kind: MediaKindVideo})
	cfg := Configuration{HeaderExtensions: exts, RTCPReportInterval: 20 * time.Millisecond}
	acfg, bcfg := cfg, cfg
	acfg.Mux = loopbackMux(t, e)
	bcfg.Engine, bcfg.ListenAddr = e, "127.0.0.1:0"
	a, b := newPC(t, acfg), newPC(t, bcfg)
	trickle(a, b)

	// b's track, added before the offer, goes on the transceiver the
	// offer's video section makes.
	tb := NewTrackLocal(Codec{MimeType: "video/VP8"}, "tb", "sb")
	if _, err := b.AddTrack(tb); err != nil {
		t.Fatal(err)
	}
	ta := NewTrackLocal(Codec{MimeType: "video/VP8"}, "ta", "sa")
	ta.SetHeaderExtensions(map[string]uint8{rtp.ExtensionURITransmissionTime: 9, rtp.ExtensionURIMid: 1})
	sa, err := a.AddTrack(ta)
	if err != nil {
		t.Fatal(err)
	}
	// A data channel keeps the transport when the only media section is
	// stopped.
	if _, err := a.CreateDataChannel("x", nil); err != nil {
		t.Fatal(err)
	}
	rb := receivePackets(b)
	reports := make(chan rtp.ReceptionReport, 64)
	a.OnRTCP(func(pkt []byte) {
		for rest := pkt; len(rest) > 0; {
			h, p, r, err := rtp.NextRTCP(rest)
			if err != nil {
				return
			}
			rest = r
			var rr rtp.ReceiverReport
			if h.Type != rtp.TypeRR || rr.Unmarshal(p) != nil {
				continue
			}
			for _, x := range rr.Reports {
				select {
				case reports <- x:
				default:
				}
			}
		}
	})
	ca := connectedCh(a)
	negotiate(t, a, b, nil)
	wait(t, ca, "connected")
	if n := len(b.Transceivers()); n != 1 {
		t.Fatalf("b has %d transceivers, want its own one", n)
	}
	stop := make(chan struct{})
	defer close(stop)
	go forwarded(ta, 0xaa, stop)
	go fakeRTP(tb, 96, stop)

	var tr *TrackRemote
	select {
	case tr = <-rb.tracks:
	case <-time.After(waitTimeout):
		t.Fatal("no track")
	}
	id := tr.HeaderExtensions()[rtp.ExtensionURITransmissionTime]
	if tr.SSRC() != sa.SSRC() || tr.RID() != "" || tr.Codec().MimeType != "video/VP8" || tr.Codec().PayloadType != 96 ||
		tr.Transceiver().Mid() == "" || tr.Transceiver().CurrentDirection() != DirectionSendRecv || id == 0 {
		t.Fatalf("track %x %q %+v mid %q, extension id %d", tr.SSRC(), tr.RID(), tr.Codec(), tr.Transceiver().Mid(), id)
	}
	p := rb.next(t, sa.SSRC(), func(p *rtp.Packet) bool { return len(p.Payload) > 0 && p.Payload[0] == 0xaa })
	mid := p.GetExtension(tr.HeaderExtensions()[rtp.ExtensionURIMid])
	if !bytes.Equal(p.GetExtension(id), []byte{0, 0, 7}) || string(mid) != tr.Transceiver().Mid() || p.PaddingSize != 4 {
		t.Fatalf("toffset %x, mid %q, padding %d", p.GetExtension(id), mid, p.PaddingSize)
	}

	// b reports on a's stream the half of it that never came, and the
	// sender reports it got.
	deadline := time.After(waitTimeout)
	for done := false; !done; {
		select {
		case r := <-reports:
			done = r.SSRC == sa.SSRC() && r.FractionLost > 0 && r.LastSenderReport != 0 && r.Delay != 0
		case <-deadline:
			t.Fatal("no receiver report with losses and a sender report's timing")
		}
	}

	kf := make(chan struct{}, 1)
	sa.OnKeyFrameRequest(func() {
		select {
		case kf <- struct{}{}:
		default:
		}
	})
	if err := tr.RequestFIR(); err != nil {
		t.Fatal(err)
	}
	wait(t, kf, "the FIR at the sender")

	// Another track replaces the first without negotiation.
	ta2 := NewTrackLocal(Codec{MimeType: "video/VP8", ClockRate: 90000}, "", "")
	if err := sa.ReplaceTrack(ta2); err != nil {
		t.Fatal(err)
	}
	go forwarded(ta2, 0xbb, stop)
	rb.next(t, sa.SSRC(), func(p *rtp.Packet) bool { return len(p.Payload) > 0 && p.Payload[0] == 0xbb })

	// A stopped transceiver's section is rejected, by the offer and the
	// answer both.
	sa.Transceiver().Stop()
	negotiate(t, a, b, nil)
	if !strings.Contains(b.LocalDescription().SDP, "m=video 0 ") {
		t.Fatalf("answer to a stopped section:\n%s", b.LocalDescription().SDP)
	}
	if d := tr.Transceiver().CurrentDirection(); d != DirectionInactive {
		t.Fatalf("b's transceiver is %v", d)
	}
}

// insertAfter puts lines after the first line of sdp that starts with
// prefix.
func insertAfter(t *testing.T, sdp, prefix string, lines ...string) string {
	t.Helper()
	i := strings.Index(sdp, "\r\n"+prefix)
	if i < 0 {
		t.Fatalf("no %q in\n%s", prefix, sdp)
	}
	end := i + 2 + strings.Index(sdp[i+2:], "\r\n") + 2
	return sdp[:end] + strings.Join(lines, "\r\n") + "\r\n" + sdp[end:]
}

// TestRejectedSections answers an offer with sections this side cannot
// take: video in a codec it lacks, a data channel section rejected, and a
// kind of media it does not know. Each is rejected in the answer, which
// the offerer then applies.
func TestRejectedSections(t *testing.T) {
	h265 := Codec{MimeType: "video/H265", ClockRate: 90000, PayloadType: 49}
	opus := DefaultCodecs()[0]
	a := sharedPC(t, Configuration{Codecs: []Codec{opus, h265}})
	b := sharedPC(t, Configuration{})
	as, err := a.AddTrack(NewTrackLocal(opus, "", ""))
	if err != nil {
		t.Fatal(err)
	}
	audio := as.Transceiver()
	video, err := a.AddTransceiver(MediaKindVideo, DirectionSendRecv)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.CreateDataChannel("x", nil); err != nil {
		t.Fatal(err)
	}
	offer, err := a.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	offer.SDP = strings.Replace(offer.SDP, "m=application 9", "m=application 0", 1)
	offer.SDP += "m=text 0 RTP/AVP 0\r\nc=IN IP4 0.0.0.0\r\na=mid:9\r\n"
	// A candidate at the session level counts as the first section's, and
	// an extension id out of range is ignored.
	offer.SDP = strings.Replace(offer.SDP, "t=0 0\r\n", "t=0 0\r\na=candidate:1 1 udp 2130706431 127.0.0.1 9 typ host\r\n", 1)
	offer.SDP = insertAfter(t, offer.SDP, "a=mid:", "a=extmap:0 urn:x")
	if err := b.SetRemoteDescription(offer); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.RemoteDescription().SDP, "a=candidate:1 1 udp") {
		t.Fatal("the session's candidate was dropped")
	}
	if err := b.AddICECandidate(ICECandidateInit{}); err != nil {
		t.Fatalf("end of candidates: %v", err)
	}
	answer, err := b.CreateAnswer()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"m=audio 9 ", "m=video 0 ", "m=application 0 ", "m=text 0 "} {
		if !strings.Contains(answer.SDP, want) {
			t.Fatalf("answer lacks %q:\n%s", want, answer.SDP)
		}
	}
	if err := b.SetLocalDescription(answer); err != nil {
		t.Fatal(err)
	}
	if err := a.SetRemoteDescription(answer); err != nil {
		t.Fatal(err)
	}
	if audio.CurrentDirection() != DirectionSendOnly || video.CurrentDirection() != DirectionInactive {
		t.Fatalf("audio %v, video %v", audio.CurrentDirection(), video.CurrentDirection())
	}

	// A kind no codec is configured for is offered rejected.
	c := sharedPC(t, Configuration{Codecs: []Codec{opus}})
	if _, err := c.AddTransceiver(MediaKindVideo, DirectionRecvOnly); err != nil {
		t.Fatal(err)
	}
	if offer, err = c.CreateOffer(nil); err != nil || !strings.Contains(offer.SDP, "m=video 0 ") {
		t.Fatalf("offer for video with no video codec: %v\n%s", err, offer.SDP)
	}
}

// TestSimulcastOffer answers an offer of simulcast layers by RID with the
// layers to receive.
func TestSimulcastOffer(t *testing.T) {
	a := sharedPC(t, Configuration{})
	b := sharedPC(t, Configuration{})
	if _, err := a.AddTrack(NewTrackLocal(Codec{MimeType: "video/VP8"}, "", "")); err != nil {
		t.Fatal(err)
	}
	offer, err := a.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	offer.SDP = insertAfter(t, offer.SDP, "a=mid:", "a=rid:h send", "a=rid:l send", "a=rid:x recv", "a=simulcast:send h;l")
	if err := b.SetRemoteDescription(offer); err != nil {
		t.Fatal(err)
	}
	answer, err := b.CreateAnswer()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"a=rid:h recv", "a=rid:l recv", "a=simulcast:recv h;l", rtp.ExtensionURIRid} {
		if !strings.Contains(answer.SDP, want) {
			t.Fatalf("answer lacks %q:\n%s", want, answer.SDP)
		}
	}
	if strings.Contains(answer.SDP, "rid:x") {
		t.Fatalf("answer takes a layer the offer receives:\n%s", answer.SDP)
	}
}

// TestDataSectionRejectedLater re-offers with the data section, accepted
// before, now rejected: the answer rejects it too.
func TestDataSectionRejectedLater(t *testing.T) {
	a, b := sharedPC(t, Configuration{}), sharedPC(t, Configuration{})
	if _, err := a.AddTrack(NewTrackLocal(Codec{MimeType: "audio/opus"}, "", "")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.CreateDataChannel("x", nil); err != nil {
		t.Fatal(err)
	}
	negotiate(t, a, b, nil)
	if !strings.Contains(b.LocalDescription().SDP, "m=application 9 ") {
		t.Fatalf("first answer:\n%s", b.LocalDescription().SDP)
	}
	offer, err := a.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	offer.SDP = strings.Replace(offer.SDP, "m=application 9", "m=application 0", 1)
	if err := b.SetRemoteDescription(offer); err != nil {
		t.Fatal(err)
	}
	answer, err := b.CreateAnswer()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(answer.SDP, "m=application 0 ") || !strings.Contains(answer.SDP, "m=audio 9 ") {
		t.Fatalf("answer to the data section rejected:\n%s", answer.SDP)
	}
}

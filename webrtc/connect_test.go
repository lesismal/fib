//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package webrtc

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/webrtc/rtp"
)

// stateCh reports when pc's PeerConnectionState is s.
func stateCh(pc *PeerConnection, s PeerConnectionState) <-chan struct{} {
	ch := make(chan struct{})
	var once sync.Once
	pc.OnConnectionStateChange(func(got PeerConnectionState) {
		if got == s {
			once.Do(func() { close(ch) })
		}
	})
	return ch
}

// iceStates records pc's ICEConnectionStates and reports when it fails.
func iceStates(pc *PeerConnection) (states func() []ICEConnectionState, failed <-chan struct{}) {
	var mu sync.Mutex
	var seen []ICEConnectionState
	ch := make(chan struct{})
	var once sync.Once
	pc.OnICEConnectionStateChange(func(s ICEConnectionState) {
		mu.Lock()
		seen = append(seen, s)
		mu.Unlock()
		if s == ICEConnectionStateFailed {
			once.Do(func() { close(ch) })
		}
	})
	return func() []ICEConnectionState {
		mu.Lock()
		defer mu.Unlock()
		return append([]ICEConnectionState(nil), seen...)
	}, ch
}

func hasState(states []ICEConnectionState, s ICEConnectionState) bool {
	for _, x := range states {
		if x == s {
			return true
		}
	}
	return false
}

// TestPeerGoesSilent connects a client to a lite server, then closes the
// server's Mux: the client's consent checks go unanswered and the server
// hears nothing more, so each side is disconnected and then fails, the
// lite one by its own timers.
func TestPeerGoesSilent(t *testing.T) {
	serverEngine, clientEngine := startEngine(t), startEngine(t)
	mux := loopbackMux(t, serverEngine)
	// Reports are due often, with no media to report on.
	timeouts := Configuration{ICEDisconnectedTimeout: 300 * time.Millisecond, ICEFailedTimeout: 800 * time.Millisecond, ICEKeepaliveInterval: 100 * time.Millisecond,
		RTCPReportInterval: 20 * time.Millisecond}
	scfg, ccfg := timeouts, timeouts
	scfg.Mux, scfg.ICELite = mux, true
	ccfg.Engine, ccfg.ListenAddr = clientEngine, "127.0.0.1:0"
	s, c := newPC(t, scfg), newPC(t, ccfg)
	trickle(c, s)
	echoChannels(s)
	d := openChannel(t, c, "x")
	cs, cc := connectedCh(s), connectedCh(c)
	negotiate(t, c, s, nil)
	wait(t, cs, "server connected")
	wait(t, cc, "client connected")
	waitOpen(t, d)
	roundTrip(t, d, [][]byte{[]byte("before")})

	sStates, sFailed := iceStates(s)
	cStates, cFailed := iceStates(c)
	sDown, cDown := stateCh(s, PeerConnectionStateFailed), stateCh(c, PeerConnectionStateFailed)
	if err := mux.Close(); err != nil {
		t.Fatal(err)
	}
	wait(t, sFailed, "the lite server to fail")
	wait(t, cFailed, "the client to fail")
	wait(t, sDown, "the server's PeerConnection to fail")
	wait(t, cDown, "the client's PeerConnection to fail")
	for name, states := range map[string][]ICEConnectionState{"server": sStates(), "client": cStates()} {
		if !hasState(states, ICEConnectionStateDisconnected) {
			t.Errorf("%s went %v, never disconnected", name, states)
		}
	}
	if s.ConnectionState() != PeerConnectionStateFailed {
		t.Fatalf("server state %v", s.ConnectionState())
	}
}

// TestFingerprintMismatch hands the offerer an answer whose fingerprint is
// not the answerer's certificate's: DTLS fails, and so does the
// PeerConnection, though ICE connects.
func TestFingerprintMismatch(t *testing.T) {
	a, b := pair(t, false)
	failed := stateCh(a, PeerConnectionStateFailed)
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
	if err := b.SetRemoteDescription(offer); err != nil {
		t.Fatal(err)
	}
	answer, err := b.CreateAnswer()
	if err != nil {
		t.Fatal(err)
	}
	if err := b.SetLocalDescription(answer); err != nil {
		t.Fatal(err)
	}
	wrong, _ := fingerprintOf("sha-256", []byte("someone else"))
	answer.SDP = strings.ReplaceAll(answer.SDP, b.fingerprint, wrong)
	if err := a.SetRemoteDescription(answer); err != nil {
		t.Fatal(err)
	}
	wait(t, failed, "DTLS to fail")
	if a.ICEConnectionState() != ICEConnectionStateConnected {
		t.Fatalf("ICE %v", a.ICEConnectionState())
	}
}

// TestDTLSBeforeAnswer has a lite offerer get the answerer's DTLS
// handshake before the answer itself: the answerer, controlling against a
// lite peer, connects and starts DTLS on its own checks. The offerer keeps
// the early records until the answer says who the DTLS client is.
func TestDTLSBeforeAnswer(t *testing.T) {
	e := startEngine(t)
	a := newPC(t, Configuration{Mux: loopbackMux(t, e), ICELite: true})
	b := newPC(t, Configuration{Engine: e, ListenAddr: "127.0.0.1:0"})
	trickle(a, b)
	echoChannels(b)
	d := openChannel(t, a, "x")
	ca, cb := connectedCh(a), connectedCh(b)
	offer, err := a.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	if err := b.SetRemoteDescription(offer); err != nil {
		t.Fatal(err)
	}
	answer, err := b.CreateAnswer()
	if err != nil {
		t.Fatal(err)
	}
	if err := b.SetLocalDescription(answer); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(waitTimeout)
	for {
		a.mu.Lock()
		early := len(a.earlyDTLS)
		a.mu.Unlock()
		if early > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no DTLS record reached the offerer before its answer")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := a.SetRemoteDescription(answer); err != nil {
		t.Fatal(err)
	}
	wait(t, ca, "offerer connected")
	wait(t, cb, "answerer connected")
	waitOpen(t, d)
	roundTrip(t, d, [][]byte{[]byte("after early DTLS")})
}

// TestDTLSRolesFromSetup has the offer say a=setup:active, as an offerer
// that insists on being the DTLS client does: the answerer takes the
// server's role and says so, and the offerer, which offered actpass,
// becomes the client.
func TestDTLSRolesFromSetup(t *testing.T) {
	a, b := pair(t, false)
	echoChannels(b)
	d := openChannel(t, a, "x")
	ca := connectedCh(a)
	offer, err := a.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	offer.SDP = strings.ReplaceAll(offer.SDP, "a=setup:actpass", "a=setup:active")
	answer, err := Answer(t.Context(), b, offer)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(answer.SDP, "a=setup:passive") {
		t.Fatalf("answer to an active offerer:\n%s", answer.SDP)
	}
	if err := a.SetRemoteDescription(answer); err != nil {
		t.Fatal(err)
	}
	wait(t, ca, "connected")
	waitOpen(t, d)
	roundTrip(t, d, [][]byte{[]byte("roles swapped")})
	if a.dtlsRole != "active" || b.dtlsRole != "passive" {
		t.Fatalf("DTLS roles %q and %q", a.dtlsRole, b.dtlsRole)
	}
}

// TestDataChannelAfterMedia connects with media only, then adds a data
// channel, which a second negotiation brings up over the DTLS connection
// already made; then exercises an open channel's API.
func TestDataChannelAfterMedia(t *testing.T) {
	a, b := pair(t, false)
	if _, err := a.AddTrack(NewTrackLocal(Codec{MimeType: "audio/opus"}, "", "")); err != nil {
		t.Fatal(err)
	}
	ca := connectedCh(a)
	negotiate(t, a, b, nil)
	wait(t, ca, "connected")
	remote := make(chan *DataChannel, 4)
	b.OnDataChannel(func(d *DataChannel) { remote <- d })
	d := openChannel(t, a, "late")
	negotiate(t, a, b, nil)
	waitOpen(t, d)
	var rd *DataChannel
	select {
	case rd = <-remote:
	case <-time.After(waitTimeout):
		t.Fatal("the peer saw no channel")
	}
	id, ok := d.ID()
	if rid, rok := rd.ID(); !ok || !rok || id != rid || d.ReadyState() != DataChannelStateOpen {
		t.Fatalf("ids %d %v and %d %v, state %v", id, ok, rid, rok, d.ReadyState())
	}

	// What is queued past the threshold is reported once it drains.
	low := make(chan struct{}, 1)
	d.OnBufferedAmountLow(func() {
		select {
		case low <- struct{}{}:
		default:
		}
	})
	d.SetBufferedAmountLowThreshold(1024)
	big := randomBytes(200 << 10)
	if err := d.Send(big); err != nil {
		t.Fatal(err)
	}
	if n := d.BufferedAmount(); n == 0 {
		t.Fatal("nothing buffered after a 200 KiB send")
	}
	wait(t, low, "the buffered amount to fall")

	// A packet that cannot be written, with more CSRCs than a header
	// holds, is dropped; the next one goes.
	sender := a.Transceivers()[0].Sender()
	sent := func() uint32 {
		a.mu.Lock()
		defer a.mu.Unlock()
		return sender.packets
	}
	bad := rtp.Packet{Header: rtp.Header{SequenceNumber: 1, CSRC: make([]uint32, 16)}, Payload: []byte{1}}
	if err := sender.Track().WritePacket(&bad); err != nil || sent() != 0 {
		t.Fatalf("a packet of 16 CSRCs: %v, %d sent", err, sent())
	}
	bad.CSRC = nil
	if err := sender.Track().WritePacket(&bad); err != nil || sent() != 1 {
		t.Fatalf("a good packet: %v, %d sent", err, sent())
	}

	// Two negotiated channels cannot share a stream id: the second closes
	// at once.
	n1, err := a.CreateDataChannel("n1", &DataChannelInit{Negotiated: true, ID: 40})
	if err != nil {
		t.Fatal(err)
	}
	n2, err := a.CreateDataChannel("n2", &DataChannelInit{Negotiated: true, ID: 40})
	if err != nil {
		t.Fatal(err)
	}
	if n1.ReadyState() == DataChannelStateClosed || n2.ReadyState() != DataChannelStateClosed {
		t.Fatalf("negotiated channels %v and %v", n1.ReadyState(), n2.ReadyState())
	}

	// Closing an open channel closes it at the peer too.
	closed, rclosed := make(chan struct{}), make(chan struct{})
	d.OnClose(func() { close(closed) })
	rd.OnClose(func() { close(rclosed) })
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	wait(t, closed, "the channel closed")
	wait(t, rclosed, "the peer's channel closed")
}

// TestTracksWithoutMid receives streams with no MID header extension
// negotiated: one by the SSRC its section lists, one whose section lists
// none, by its payload type. A receiver that takes one stream drops the
// other.
func TestTracksWithoutMid(t *testing.T) {
	for _, maxSSRCs := range []int{0, 1} {
		e := startEngine(t)
		cfg := Configuration{HeaderExtensions: []HeaderExtension{}, MaxRemoteSSRCs: maxSSRCs}
		acfg, bcfg := cfg, cfg
		acfg.Mux = loopbackMux(t, e)
		bcfg.Engine, bcfg.ListenAddr = e, "127.0.0.1:0"
		a, b := newPC(t, acfg), newPC(t, bcfg)
		trickle(a, b)
		video := NewTrackLocal(Codec{MimeType: "video/VP8"}, "", "")
		audio := NewTrackLocal(Codec{MimeType: "audio/opus", ClockRate: 48000, Channels: 2}, "", "")
		if _, err := a.AddTrack(video); err != nil {
			t.Fatal(err)
		}
		sa, err := a.AddTrack(audio)
		if err != nil {
			t.Fatal(err)
		}
		rb := collect(b)
		ca := connectedCh(a)
		offer, err := a.CreateOffer(nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := a.SetLocalDescription(offer); err != nil {
			t.Fatal(err)
		}
		var kept []string
		for _, l := range strings.SplitAfter(offer.SDP, "\r\n") {
			if !strings.HasPrefix(l, fmt.Sprintf("a=ssrc:%d ", sa.SSRC())) {
				kept = append(kept, l)
			}
		}
		offer.SDP = strings.Join(kept, "")
		answer, err := Answer(t.Context(), b, offer)
		if err != nil {
			t.Fatal(err)
		}
		if err := a.SetRemoteDescription(answer); err != nil {
			t.Fatal(err)
		}
		wait(t, ca, "connected")
		stop := make(chan struct{})
		go fakeRTP(video, 96, stop)
		go fakeRTP(audio, 111, stop)
		first := nextTrack(t, rb)
		if maxSSRCs == 1 {
			time.Sleep(300 * time.Millisecond)
			select {
			case r := <-rb:
				t.Errorf("a second track %x past MaxRemoteSSRCs", r.ssrc)
			default:
			}
		} else {
			second := nextTrack(t, rb)
			kinds := map[MediaKind]bool{first.track.Kind(): true, second.track.Kind(): true}
			if !kinds[MediaKindVideo] || !kinds[MediaKindAudio] {
				t.Errorf("tracks of kinds %v", kinds)
			}
		}
		close(stop)
	}
}

// TestTURNRefused gathers with a TURN server that refuses the credentials:
// gathering completes without a relay candidate.
func TestTURNRefused(t *testing.T) {
	url, _ := startTURN(t)
	pc := newPC(t, Configuration{Engine: startEngine(t), ListenAddr: "127.0.0.1:0",
		ICEServers: []ICEServer{{URLs: []string{url}, Username: "user", Credential: "wrong"}}})
	if _, err := pc.CreateDataChannel("x", nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()
	offer, err := Offer(ctx, pc, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-pc.GatheringComplete():
	default:
		t.Fatal("gathering did not complete")
	}
	if strings.Contains(offer.SDP, "typ relay") || !strings.Contains(offer.SDP, "typ host") || !strings.Contains(offer.SDP, "end-of-candidates") {
		t.Fatalf("offer:\n%s", offer.SDP)
	}
}

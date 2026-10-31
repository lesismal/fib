//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package webrtc

import (
	"context"
	stdtls "crypto/tls"
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/webrtc/rtp"
	"github.com/lesismal/fib/webrtc/sctp"
)

// sharedPC makes a PeerConnection on a Mux of a fresh engine, for tests
// that never connect it.
func sharedPC(t *testing.T, config Configuration) *PeerConnection {
	t.Helper()
	config.Mux = loopbackMux(t, startEngine(t))
	return newPC(t, config)
}

func TestConfigurationErrors(t *testing.T) {
	e := startEngine(t)
	mux := loopbackMux(t, e)
	cert, err := tls.GenerateWebRTCCertificate()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		config Configuration
	}{
		{"certificate without DER", Configuration{Mux: mux, Certificates: []stdtls.Certificate{{}}}},
		{"bad ICE server URL", Configuration{Mux: mux, ICEServers: []ICEServer{{URLs: []string{"nonsense"}}}}},
		{"bad ICE server port", Configuration{Mux: mux, ICEServers: []ICEServer{{URLs: []string{"stun:127.0.0.1:99999"}}}}},
		{"relay policy without TURN", Configuration{Mux: mux, ICETransportPolicy: ICETransportPolicyRelay}},
		{"bad listen address", Configuration{Engine: e, ListenAddr: "no-such-address"}},
	} {
		if _, err := NewPeerConnection(tc.config); err == nil {
			t.Errorf("%s: accepted", tc.name)
		}
	}

	// Two PeerConnections may share a certificate, and a TURN server over
	// TLS, which is not supported, is left out rather than refused.
	a := newPC(t, Configuration{Mux: mux, Certificates: []stdtls.Certificate{cert},
		ICEServers: []ICEServer{{URLs: []string{"turns:127.0.0.1", "stun:127.0.0.1:3478"}}}})
	b := newPC(t, Configuration{Mux: mux, Certificates: []stdtls.Certificate{cert}})
	if a.fingerprint != b.fingerprint || a.fingerprint != tls.CertificateFingerprint(cert.Certificate[0]) {
		t.Fatal("a shared certificate gave two fingerprints")
	}
	if len(a.turnServers) != 0 || len(a.stunServers) != 1 {
		t.Fatalf("turn servers %v, stun servers %v", a.turnServers, a.stunServers)
	}

	// A closed Mux takes no more PeerConnections.
	closed := loopbackMux(t, e)
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closed.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := NewPeerConnection(Configuration{Mux: closed}); !errors.Is(err, errMuxClosed) {
		t.Fatalf("PeerConnection on a closed Mux: %v", err)
	}
}

func TestMuxAddresses(t *testing.T) {
	e := startEngine(t)
	public := netip.MustParseAddr("203.0.113.9")
	m, err := NewMux(e, MuxConfig{Addr: "127.0.0.1:0", PublicAddrs: []netip.Addr{public, public}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	if addrs := m.Addrs(); len(addrs) != 1 || addrs[0].Addr() != public || addrs[0].Port() == 0 {
		t.Fatalf("public addresses %v", addrs)
	}
	// A wildcard address advertises the interfaces', IPv4 ones only for an
	// IPv4 socket.
	w, err := NewMux(e, MuxConfig{Network: "udp4"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	for _, a := range w.Addrs() {
		if !a.Addr().Is4() || a.Addr().IsUnspecified() || a.Port() == 0 {
			t.Fatalf("wildcard mux advertises %v", a)
		}
	}
	if _, err := NewMux(e, MuxConfig{Network: "udp", Addr: "no-such-address"}); err == nil {
		t.Fatal("a bad address accepted")
	}
}

func TestSignalingStateErrors(t *testing.T) {
	pc := sharedPC(t, Configuration{})
	if pc.LocalDescription() != nil || pc.RemoteDescription() != nil {
		t.Fatal("descriptions before any were set")
	}
	if _, err := pc.CreateAnswer(); err != ErrSignalingState {
		t.Fatalf("CreateAnswer with no offer: %v", err)
	}
	if err := pc.SetLocalDescription(SessionDescription{Type: SDPTypeOffer, SDP: "not sdp"}); !errors.Is(err, ErrDescription) {
		t.Fatalf("SetLocalDescription of garbage: %v", err)
	}
	if err := pc.SetRemoteDescription(SessionDescription{Type: SDPTypeOffer, SDP: "not sdp"}); !errors.Is(err, ErrDescription) {
		t.Fatalf("SetRemoteDescription of garbage: %v", err)
	}
	if _, err := pc.CreateDataChannel("x", nil); err != nil {
		t.Fatal(err)
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := pc.SetLocalDescription(SessionDescription{Type: "pranswer", SDP: offer.SDP}); !errors.Is(err, ErrDescription) {
		t.Fatalf("a pranswer: %v", err)
	}
	if err := pc.SetLocalDescription(SessionDescription{Type: SDPTypeAnswer, SDP: offer.SDP}); err != ErrSignalingState {
		t.Fatalf("an answer in the stable state: %v", err)
	}
	if err := pc.SetRemoteDescription(SessionDescription{Type: SDPTypeAnswer, SDP: offer.SDP}); err != ErrSignalingState {
		t.Fatalf("a remote answer in the stable state: %v", err)
	}
	var changes []SignalingState
	pc.OnSignalingStateChange(func(s SignalingState) { changes = append(changes, s) })
	// An offer may be set again before its answer, which changes nothing.
	for range 2 {
		if err := pc.SetLocalDescription(offer); err != nil {
			t.Fatal(err)
		}
	}
	if len(changes) != 1 || pc.SignalingState() != SignalingStateHaveLocalOffer {
		t.Fatalf("signaling state %v after the changes %v", pc.SignalingState(), changes)
	}
	if err := pc.SetRemoteDescription(offer); err != ErrSignalingState {
		t.Fatalf("a remote offer while one of ours is out: %v", err)
	}

	// An offer received makes another offer, or the local one, wrong.
	other := sharedPC(t, Configuration{})
	if err := other.SetRemoteDescription(offer); err != nil {
		t.Fatal(err)
	}
	if _, err := other.CreateOffer(nil); err != ErrSignalingState {
		t.Fatalf("CreateOffer with a remote offer: %v", err)
	}
	if err := other.SetLocalDescription(offer); err != ErrSignalingState {
		t.Fatalf("a local offer with a remote one: %v", err)
	}
	if other.RemoteDescription() == nil || other.RemoteDescription().Type != SDPTypeOffer {
		t.Fatal("RemoteDescription")
	}

	// Closed, it refuses everything.
	_ = other.Close()
	if _, err := other.CreateAnswer(); err != ErrClosed {
		t.Fatalf("CreateAnswer when closed: %v", err)
	}
	if err := other.SetLocalDescription(offer); err != ErrClosed {
		t.Fatalf("SetLocalDescription when closed: %v", err)
	}
	if err := other.AddICECandidate(ICECandidateInit{}); err != ErrClosed {
		t.Fatalf("AddICECandidate when closed: %v", err)
	}
	if other.ConnectionState() != PeerConnectionStateClosed || other.SignalingState() != SignalingStateClosed || other.ICEConnectionState() != ICEConnectionStateClosed {
		t.Fatalf("closed states %v %v %v", other.ConnectionState(), other.SignalingState(), other.ICEConnectionState())
	}
	if err := other.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestRemoteDescriptionErrors sets offers each broken in one way.
func TestRemoteDescriptionErrors(t *testing.T) {
	src := sharedPC(t, Configuration{})
	if _, err := src.CreateDataChannel("x", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := src.AddTransceiver(MediaKindAudio, DirectionRecvOnly); err != nil {
		t.Fatal(err)
	}
	offer, err := src.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(offer.SDP), "\r\n")
	without := func(prefixes ...string) string {
		var out []string
	next:
		for _, l := range lines {
			for _, p := range prefixes {
				if strings.HasPrefix(l, p) {
					continue next
				}
			}
			out = append(out, l)
		}
		return strings.Join(out, "\r\n") + "\r\n"
	}
	rejectAll := strings.NewReplacer("m=audio 9", "m=audio 0", "m=application 9", "m=application 0").Replace(offer.SDP)
	for _, tc := range []struct {
		name, sdp string
	}{
		{"no ICE credentials", without("a=ice-ufrag", "a=ice-pwd")},
		{"no mid", without("a=mid")},
		{"not bundled", without("a=group:BUNDLE")},
		{"no fingerprint", without("a=fingerprint")},
		{"every section rejected", rejectAll},
		{"every section rejected, credentials in the session", strings.Replace(rejectAll, "t=0 0\r\n", "t=0 0\r\na=ice-ufrag:abcd\r\na=ice-pwd:abcdefghijklmnopqrstuvwx\r\n", 1)},
		{"no section", without("m=", "a=", "c=")},
	} {
		pc := sharedPC(t, Configuration{})
		if err := pc.SetRemoteDescription(SessionDescription{Type: SDPTypeOffer, SDP: tc.sdp}); !errors.Is(err, ErrDescription) {
			t.Errorf("%s: %v\n%s", tc.name, err, tc.sdp)
		}
	}
	pc := sharedPC(t, Configuration{})
	if err := pc.SetRemoteDescription(SessionDescription{Type: "rollback", SDP: offer.SDP}); !errors.Is(err, ErrDescription) {
		t.Fatalf("a rollback: %v", err)
	}
}

func TestAddICECandidate(t *testing.T) {
	pc := sharedPC(t, Configuration{MaxRemoteCandidates: 1})
	c := ICECandidateInit{Candidate: "candidate:1 1 udp 2130706431 127.0.0.1 5000 typ host"}
	if err := pc.AddICECandidate(ICECandidateInit{Candidate: "candidate:garbage"}); err == nil {
		t.Fatal("a malformed candidate accepted")
	}
	// The end of candidates before a remote description is a no-op.
	if err := pc.AddICECandidate(ICECandidateInit{}); err != nil {
		t.Fatal(err)
	}
	if err := pc.AddICECandidate(c); err != nil {
		t.Fatal(err)
	}
	if err := pc.AddICECandidate(c); err != ErrTooManyCandidates {
		t.Fatalf("a second pending candidate over the limit: %v", err)
	}
}

func TestDataChannelOptions(t *testing.T) {
	pc := sharedPC(t, Configuration{})
	n := uint16(3)
	if _, err := pc.CreateDataChannel("x", &DataChannelInit{MaxRetransmits: &n, MaxPacketLifeTime: &n}); err != ErrConfiguration {
		t.Fatalf("both partial reliability options: %v", err)
	}
	negotiated := false
	pc.OnNegotiationNeeded(func() { negotiated = true })
	d, err := pc.CreateDataChannel("unordered", &DataChannelInit{Protocol: "chat", Unordered: true, MaxRetransmits: &n, Negotiated: true, ID: 7})
	if err != nil {
		t.Fatal(err)
	}
	if !negotiated {
		t.Fatal("the first data channel did not need negotiation")
	}
	if d.Protocol() != "chat" || d.Ordered() || !d.Negotiated() || d.opts.Type != sctp.ChannelPartialReliableRexmitUnordered || d.opts.ReliabilityParameter != 3 {
		t.Fatalf("options %+v", d.opts)
	}
	timed, err := pc.CreateDataChannel("timed", &DataChannelInit{MaxPacketLifeTime: &n})
	if err != nil {
		t.Fatal(err)
	}
	if !timed.Ordered() || timed.opts.Type != sctp.ChannelPartialReliableTimed {
		t.Fatalf("timed options %+v", timed.opts)
	}
	// Before the association is up the channel is connecting, with no
	// stream id and nothing buffered, and cannot send.
	if _, ok := d.ID(); ok {
		t.Fatal("an id before the association")
	}
	if d.ReadyState() != DataChannelStateConnecting || d.BufferedAmount() != 0 {
		t.Fatalf("state %v, buffered %d", d.ReadyState(), d.BufferedAmount())
	}
	d.SetBufferedAmountLowThreshold(100)
	d.OnBufferedAmountLow(func() {})
	if err := d.Send([]byte("x")); err != sctp.ErrChannelNotOpen {
		t.Fatalf("Send before open: %v", err)
	}
	if err := d.SendText("x"); err != sctp.ErrChannelNotOpen {
		t.Fatalf("SendText before open: %v", err)
	}
	closed := make(chan struct{})
	d.OnClose(func() { close(closed) })
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	wait(t, closed, "OnClose")
	if err := d.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if d.ReadyState() != DataChannelStateClosed {
		t.Fatalf("state %v after Close", d.ReadyState())
	}
	if err := d.Send([]byte("x")); err != sctp.ErrChannelClosed {
		t.Fatalf("Send after Close: %v", err)
	}
	// Closing the PeerConnection closes the channels it has not opened,
	// and its ICE, which had not started.
	timedClosed := make(chan struct{})
	timed.OnClose(func() { close(timedClosed) })
	iceClosed := make(chan struct{})
	pc.OnICEConnectionStateChange(func(s ICEConnectionState) {
		if s == ICEConnectionStateClosed {
			close(iceClosed)
		}
	})
	_ = pc.Close()
	wait(t, timedClosed, "OnClose of a channel never opened")
	wait(t, iceClosed, "ICE closed")
	if _, err := pc.CreateDataChannel("late", nil); err != ErrClosed {
		t.Fatalf("CreateDataChannel when closed: %v", err)
	}
}

func TestTransceiverAPI(t *testing.T) {
	pc := sharedPC(t, Configuration{})
	needed := 0
	pc.OnNegotiationNeeded(func() { needed++ })
	if _, err := pc.AddTransceiver("text", DirectionRecvOnly); err != ErrConfiguration {
		t.Fatalf("a text transceiver: %v", err)
	}
	if _, err := pc.AddTrack(NewTrackLocal(Codec{MimeType: "text/plain"}, "", "")); err != ErrConfiguration {
		t.Fatalf("a text track: %v", err)
	}
	tr, err := pc.AddTransceiver(MediaKindVideo, DirectionRecvOnly)
	if err != nil {
		t.Fatal(err)
	}
	if tr.Kind() != MediaKindVideo || tr.Direction() != DirectionRecvOnly || tr.CurrentDirection() != DirectionInactive || tr.Mid() != "" || len(tr.Tracks()) != 0 {
		t.Fatalf("new transceiver %v %v %v %q", tr.Kind(), tr.Direction(), tr.CurrentDirection(), tr.Mid())
	}
	// AddTrack takes the receiving transceiver of its kind.
	track := NewTrackLocal(Codec{MimeType: "video/VP8"}, "id", "stream")
	s, err := pc.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	if s != tr.Sender() || s.Transceiver() != tr || s.Track() != track || tr.Direction() != DirectionSendRecv {
		t.Fatal("AddTrack did not take the recvonly transceiver")
	}
	if track.ID() != "id" || track.StreamID() != "stream" || track.Codec().MimeType != "video/VP8" {
		t.Fatal("track accessors")
	}
	if got := pc.Transceivers(); len(got) != 1 || got[0] != tr {
		t.Fatalf("Transceivers %v", got)
	}
	if err := s.ReplaceTrack(NewTrackLocal(Codec{MimeType: "audio/opus"}, "", "")); err != ErrConfiguration {
		t.Fatalf("ReplaceTrack of another kind: %v", err)
	}
	other := NewTrackLocal(Codec{MimeType: "video/VP8"}, "", "")
	if err := s.ReplaceTrack(other); err != nil || s.Track() != other || len(*track.senders.Load()) != 0 || len(*other.senders.Load()) != 1 {
		t.Fatalf("ReplaceTrack: %v", err)
	}
	if err := s.ReplaceTrack(nil); err != nil || s.Track() != nil {
		t.Fatalf("ReplaceTrack(nil): %v", err)
	}
	// A sender of another PeerConnection, or with no track, is nothing to
	// remove.
	foreign := sharedPC(t, Configuration{})
	fs, err := foreign.AddTrack(NewTrackLocal(Codec{MimeType: "audio/opus"}, "", ""))
	if err != nil {
		t.Fatal(err)
	}
	if err := pc.RemoveTrack(fs); err != nil || fs.Track() == nil {
		t.Fatalf("RemoveTrack of a foreign sender: %v", err)
	}
	if err := pc.RemoveTrack(s); err != nil {
		t.Fatal(err)
	}
	if _, err := pc.AddTrack(track); err != nil {
		t.Fatal(err)
	}
	if err := pc.RemoveTrack(s); err != nil || s.Track() != nil || tr.Direction() != DirectionRecvOnly {
		t.Fatalf("RemoveTrack: %v, direction %v", err, tr.Direction())
	}
	// A track on two PeerConnections loses one sender to RemoveTrack and
	// keeps the other, which drops what is written until it connects.
	fs2, err := foreign.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	s3, err := pc.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	if err := pc.RemoveTrack(s3); err != nil {
		t.Fatal(err)
	}
	if got := *track.senders.Load(); len(got) != 1 || got[0] != fs2 {
		t.Fatalf("senders after RemoveTrack: %v", got)
	}
	pkt, _ := (&rtp.Packet{Header: rtp.Header{PayloadType: 96, SSRC: 1}, Payload: []byte{1}}).Append(nil)
	if err := track.WriteRTP(pkt); err != nil || fs2.packets != 0 {
		t.Fatalf("WriteRTP before connecting: %v, %d packets sent", err, fs2.packets)
	}
	tr.SetDirection(DirectionInactive)
	tr.SetDirection(DirectionInactive)
	if tr.Direction() != DirectionInactive {
		t.Fatal("SetDirection")
	}
	tr.Stop()
	tr.Stop()
	before := needed
	if before < 5 {
		t.Fatalf("%d negotiations needed", before)
	}
	// A stopped transceiver's section is offered rejected.
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(offer.SDP, "m=video 0 ") || !strings.Contains(offer.SDP, "a=inactive") {
		t.Fatalf("a stopped transceiver's section:\n%s", offer.SDP)
	}
	if err := (&TrackLocal{}).WriteRTP([]byte{1, 2}); err == nil {
		t.Fatal("WriteRTP of a malformed packet")
	}
	if err := pc.WriteRTCP([]byte{0x80, 201, 0, 1, 0, 0, 0, 1}); err != ErrNotConnected {
		t.Fatalf("WriteRTCP before DTLS: %v", err)
	}
	_ = pc.Close()
	if _, err := pc.AddTrack(track); err != ErrClosed {
		t.Fatalf("AddTrack when closed: %v", err)
	}
	if _, err := pc.AddTransceiver(MediaKindAudio, DirectionRecvOnly); err != ErrClosed {
		t.Fatalf("AddTransceiver when closed: %v", err)
	}
	if err := pc.RemoveTrack(s); err != ErrClosed {
		t.Fatalf("RemoveTrack when closed: %v", err)
	}
	if err := s.ReplaceTrack(other); err != ErrClosed {
		t.Fatalf("ReplaceTrack when closed: %v", err)
	}
	if err := pc.WriteRTCP(nil); err != ErrClosed {
		t.Fatalf("WriteRTCP when closed: %v", err)
	}
	if needed != before {
		t.Fatal("a closed PeerConnection asked for negotiation")
	}
}

// TestStateCallbacks follows the signaling and gathering of an offer with
// no section, whose candidates name mid "0".
func TestStateCallbacks(t *testing.T) {
	pc := sharedPC(t, Configuration{})
	var signaling []SignalingState
	gathered := make(chan struct{})
	cands := make(chan *ICECandidateInit, 16)
	pc.OnSignalingStateChange(func(s SignalingState) { signaling = append(signaling, s) })
	pc.OnICEGatheringStateChange(func(s ICEGatheringState) {
		if s == ICEGatheringStateComplete {
			close(gathered)
		}
	})
	pc.OnICEConnectionStateChange(func(ICEConnectionState) {})
	pc.OnICECandidate(func(c *ICECandidateInit) { cands <- c })
	if pc.ICEGatheringState() != ICEGatheringStateNew {
		t.Fatal("gathering before an offer")
	}
	if _, _, ok := pc.SelectedCandidatePair(); ok {
		t.Fatal("a selected pair with no ICE agent")
	}
	offer, err := Offer(t.Context(), pc, nil)
	if err != nil {
		t.Fatal(err)
	}
	wait(t, gathered, "gathering")
	if pc.ICEGatheringState() != ICEGatheringStateComplete || !strings.Contains(offer.SDP, "v=0") {
		t.Fatalf("gathering %v", pc.ICEGatheringState())
	}
	if _, _, ok := pc.SelectedCandidatePair(); ok {
		t.Fatal("a selected pair with no remote peer")
	}
	c := <-cands
	if c == nil || c.SDPMid != "0" || c.UsernameFragment == "" {
		t.Fatalf("first candidate %+v", c)
	}
	_ = pc.Close()
	if len(signaling) != 2 || signaling[0] != SignalingStateHaveLocalOffer || signaling[1] != SignalingStateClosed {
		t.Fatalf("signaling states %v", signaling)
	}
	if _, err := Offer(t.Context(), pc, nil); err != ErrClosed {
		t.Fatalf("Offer when closed: %v", err)
	}
	if _, err := Answer(t.Context(), pc, offer); err != ErrClosed {
		t.Fatalf("Answer when closed: %v", err)
	}
}

// TestGatheringCut returns an offer when the context ends before a STUN
// server that never answers lets gathering complete.
func TestGatheringCut(t *testing.T) {
	pc := sharedPC(t, Configuration{ICEServers: []ICEServer{{URLs: []string{"stun:127.0.0.1:9"}}}})
	if _, err := pc.CreateDataChannel("x", nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	offer, err := Offer(ctx, pc, nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(offer.SDP, "end-of-candidates") || !strings.Contains(offer.SDP, "typ host") {
		t.Fatalf("offer cut short:\n%s", offer.SDP)
	}
	select {
	case <-pc.GatheringComplete():
		t.Fatal("gathering complete with the STUN server unanswered")
	default:
	}
}

func TestUnknownStateStrings(t *testing.T) {
	if ICEConnectionState(9).String() != "ICEConnectionState(9)" || ICEGatheringState(9).String() != "ICEGatheringState(9)" ||
		SignalingState(9).String() != "SignalingState(9)" || directionOf(false, false) != DirectionInactive {
		t.Fatal("names of unknown states")
	}
}

// TestRestartOnClosedMux sets an offer with new ICE credentials once the
// Mux has closed: the new ufrag cannot be routed, and the offer is refused.
func TestRestartOnClosedMux(t *testing.T) {
	m := loopbackMux(t, startEngine(t))
	pc := newPC(t, Configuration{Mux: m})
	if _, err := pc.CreateDataChannel("x", nil); err != nil {
		t.Fatal(err)
	}
	offer, err := pc.CreateOffer(&OfferOptions{ICERestart: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pc.SetLocalDescription(offer); !errors.Is(err, errMuxClosed) {
		t.Fatalf("restart on a closed Mux: %v", err)
	}
}

// TestMuxSendAndIdle sends to an address the Mux has no connection for:
// what is sent while the connection opens is queued and goes out once it
// is open, and the connection is closed and forgotten once idle.
func TestMuxSendAndIdle(t *testing.T) {
	// The engine looks for idle connections as often as its own idle
	// timeout asks.
	e := startEngineWith(t, func(c *fib.Config) { c.UDPIdleTimeout = 100 * time.Millisecond })
	m, err := NewMux(e, MuxConfig{Addr: "127.0.0.1:0", IdleTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	to := peer.LocalAddr().(*net.UDPAddr).AddrPort()
	for i := range 3 {
		m.send(to, []byte{byte(i)})
	}
	_ = peer.SetReadDeadline(time.Now().Add(waitTimeout))
	buf := make([]byte, 16)
	for i := range 3 {
		n, err := peer.Read(buf)
		if err != nil || n != 1 || buf[0] != byte(i) {
			t.Fatalf("datagram %d: %x, %v", i, buf[:n], err)
		}
	}
	conns := func() int {
		m.mu.RLock()
		defer m.mu.RUnlock()
		return len(m.conns)
	}
	if conns() != 1 {
		t.Fatalf("%d connections after sending", conns())
	}
	deadline := time.Now().Add(waitTimeout)
	for conns() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the idle connection was kept")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Once closed, the Mux sends nothing.
	_ = m.Close()
	m.send(to, []byte{9})
	_ = peer.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if n, err := peer.Read(buf); err == nil {
		t.Fatalf("a closed Mux sent %x", buf[:n])
	}
}

// TestMuxRebind has a second PeerConnection bind a remote address the
// first had: the address is routed to the second, and stays so when the
// first closes.
func TestMuxRebind(t *testing.T) {
	m := loopbackMux(t, startEngine(t))
	first, second := newPC(t, Configuration{Mux: m}), newPC(t, Configuration{Mux: m})
	addr := netip.MustParseAddrPort("127.0.0.1:9")
	for _, pc := range []*PeerConnection{first, second} {
		pc.mu.Lock()
		pc.bind(addr)
		pc.mu.Unlock()
	}
	_ = first.Close()
	n := m.Len()
	m.mu.RLock()
	got := m.peers[addr]
	m.mu.RUnlock()
	if got != second || n != 1 {
		t.Fatalf("address routed to %p, want %p; %d PeerConnections", got, second, n)
	}
}

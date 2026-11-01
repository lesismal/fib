package webrtc

import (
	stdtls "crypto/tls"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/webrtc/ice"
	"github.com/lesismal/fib/webrtc/rtp"
	"github.com/lesismal/fib/webrtc/sdp"
	"github.com/lesismal/fib/webrtc/srtp"
	"github.com/lesismal/fib/webrtc/stun"
)

func TestRequestUfrag(t *testing.T) {
	var m stun.Message
	m.Build(stun.BindingRequest, stun.NewTransactionID())
	m.AddString(stun.AttrSoftware, "x")
	m.AddString(stun.AttrUsername, "srvU:cliU")
	m.AddIntegrity([]byte("pwd"))
	m.AddFingerprint()
	if got := requestUfrag(m.Raw); got != "srvU" {
		t.Fatalf("got %q", got)
	}
	var r stun.Message
	r.Build(stun.MessageType{Method: stun.MethodBinding, Class: stun.ClassSuccessResponse}, stun.NewTransactionID())
	r.AddString(stun.AttrUsername, "a:b")
	// A request with no USERNAME, or one with no colon, names no ufrag;
	// neither does one whose attribute runs past the message.
	var noUser, noColon stun.Message
	noUser.Build(stun.BindingRequest, stun.NewTransactionID())
	noUser.AddString(stun.AttrSoftware, "x")
	noColon.Build(stun.BindingRequest, stun.NewTransactionID())
	noColon.AddString(stun.AttrUsername, "nocolon")
	overrun := append([]byte(nil), noUser.Raw...)
	overrun[22], overrun[23] = 0, 200
	for _, b := range [][]byte{r.Raw, m.Raw[:10], {0, 1, 0, 4, 0x21, 0x12, 0xa4, 0x42}, append(m.Raw[:20:20], 0, 6, 0, 99), noUser.Raw, noColon.Raw, overrun} {
		if got := requestUfrag(b); got != "" {
			t.Fatalf("%x: got %q", b, got)
		}
	}
}

func FuzzRequestUfrag(f *testing.F) {
	var m stun.Message
	m.Build(stun.BindingRequest, stun.NewTransactionID())
	m.AddString(stun.AttrUsername, "u:v")
	f.Add(m.Raw)
	f.Fuzz(func(t *testing.T, b []byte) { requestUfrag(b) })
}

func TestCodecMatches(t *testing.T) {
	h := func(mode, profile string) Codec {
		return Codec{MimeType: "video/H264", ClockRate: 90000, Fmtp: "packetization-mode=" + mode + ";profile-level-id=" + profile}
	}
	for _, tc := range []struct {
		l, r Codec
		want bool
	}{
		{Codec{MimeType: "video/VP8", ClockRate: 90000}, Codec{MimeType: "video/vp8", ClockRate: 90000}, true},
		{Codec{MimeType: "video/VP8", ClockRate: 90000}, Codec{MimeType: "video/VP9", ClockRate: 90000}, false},
		{Codec{MimeType: "audio/opus", ClockRate: 48000, Channels: 2}, Codec{MimeType: "audio/opus", ClockRate: 48000, Channels: 1}, false},
		{h("1", "42e01f"), h("1", "42e034"), true},
		{h("1", "42e01f"), h("1", "42001f"), false},
		{h("1", "42e01f"), h("0", "42e01f"), false},
		{h("1", "64001f"), h("1", "640032"), true},
		{Codec{MimeType: "video/VP9", ClockRate: 90000, Fmtp: "profile-id=0"}, Codec{MimeType: "video/VP9", ClockRate: 90000}, true},
		{Codec{MimeType: "video/VP9", ClockRate: 90000, Fmtp: "profile-id=0"}, Codec{MimeType: "video/VP9", ClockRate: 90000, Fmtp: "profile-id=2"}, false},
	} {
		if got := tc.l.matches(tc.r); got != tc.want {
			t.Errorf("%v matches %v = %v", tc.l, tc.r, got)
		}
	}
	got := intersectCodecs("video", []sdp.Codec{{PayloadType: 120, Name: "VP9", ClockRate: 90000, Fmtp: "profile-id=2"}, {PayloadType: 97, Name: "VP8", ClockRate: 90000}, {PayloadType: 98, Name: "rtx", ClockRate: 90000}, {PayloadType: 99}}, DefaultCodecs())
	if len(got) != 1 || got[0].sendPT != 97 || got[0].Name() != "VP8" {
		t.Fatalf("intersection %+v", got)
	}
}

func TestICEServerURL(t *testing.T) {
	for _, tc := range []struct {
		url   string
		addr  string
		turn  bool
		error bool
	}{
		{"stun:127.0.0.1", "127.0.0.1:3478", false, false},
		{"stun:127.0.0.1:19302", "127.0.0.1:19302", false, false},
		{"turn:[::1]:3479?transport=udp", "[::1]:3479", true, false},
		{"turn:127.0.0.1?transport=tcp", "invalid AddrPort", true, false},
		{"turns:127.0.0.1", "invalid AddrPort", true, false},
		{"stuns:127.0.0.1", "invalid AddrPort", false, false},
		{"http://x", "", false, true},
		{"nonsense", "", false, true},
	} {
		addr, turn, err := iceServer(tc.url)
		if (err != nil) != tc.error || err == nil && (addr.String() != tc.addr || turn != tc.turn) {
			t.Errorf("%s: %v %v %v", tc.url, addr, turn, err)
		}
	}
}

func TestFingerprint(t *testing.T) {
	cert, err := tls.GenerateWebRTCCertificate()
	if err != nil {
		t.Fatal(err)
	}
	der := cert.Certificate[0]
	if v, ok := fingerprintOf("SHA-256", der); !ok || v != tls.CertificateFingerprint(der) {
		t.Fatalf("sha-256 fingerprint %q", v)
	}
	for _, alg := range []string{"sha-1", "sha-224", "sha-384", "sha-512"} {
		if v, ok := fingerprintOf(alg, der); !ok || strings.Count(v, ":") < 19 {
			t.Fatalf("%s fingerprint %q", alg, v)
		}
	}
	if _, ok := fingerprintOf("md5", der); ok {
		t.Fatal("md5 accepted")
	}
}

func TestRandomString(t *testing.T) {
	for range 100 {
		s := randomString(24)
		if len(s) != 24 || strings.ContainsAny(s, "-_=") {
			t.Fatalf("%q", s)
		}
	}
}

func TestStateStrings(t *testing.T) {
	if PeerConnectionStateConnected.String() != "connected" || ICEConnectionStateChecking.String() != "checking" ||
		ICEGatheringStateComplete.String() != "complete" || SignalingStateHaveRemoteOffer.String() != "have-remote-offer" ||
		PeerConnectionState(99).String() != "PeerConnectionState(99)" {
		t.Fatal("state names")
	}
}

func TestSignalingErrors(t *testing.T) {
	if _, err := NewPeerConnection(Configuration{}); err == nil {
		t.Fatal("no Mux and no Engine accepted")
	}
	if _, err := NewPeerConnection(Configuration{BundlePolicy: 1}); err == nil {
		t.Fatal("bundle policy accepted")
	}
}

func TestOrderSection(t *testing.T) {
	md := sdp.NewMedia("audio", 9, sdp.ProtoRTP)
	md.SetMid("0")
	md.SetDirection(sdp.SendRecv)
	md.SetSetup(sdp.SetupActive)
	md.SetICECredentials("ufrag", "pwd")
	(&PeerConnection{}).orderSection(md)
	var names []string
	for _, l := range md.Lines {
		if l.Type == 'a' {
			name, _, _ := l.Attribute()
			names = append(names, name)
		}
	}
	if got := strings.Join(names, " "); got != "ice-ufrag ice-pwd setup mid sendrecv" {
		t.Fatalf("attributes in the order %s", got)
	}
}

func TestExtensionIDs(t *testing.T) {
	pc := &PeerConnection{extIDs: map[string]uint8{}}
	for i := range 14 {
		if id, ok := pc.extensionID(fmt.Sprint("urn:x:", i)); !ok || id != uint8(i+1) {
			t.Fatalf("extension %d got id %d", i, id)
		}
	}
	if id, ok := pc.extensionID("urn:x:3"); !ok || id != 4 {
		t.Fatalf("a URI's id changed to %d", id)
	}
	if _, ok := pc.extensionID("urn:x:14"); ok {
		t.Fatal("a fifteenth one-byte extension id")
	}
}

func TestVerifyPeerWithoutCertificate(t *testing.T) {
	if err := (&PeerConnection{}).verifyPeer(nil, nil); err != ErrFingerprint {
		t.Fatalf("no certificate: %v", err)
	}
}

// TestAgentStates maps each ice.State to the ICE and PeerConnection states
// of a PeerConnection that has no DTLS connection yet.
func TestAgentStates(t *testing.T) {
	pc := &PeerConnection{iceState: ICEConnectionStateChecking}
	for _, tc := range []struct {
		agent ice.State
		ice   ICEConnectionState
		conn  PeerConnectionState
	}{
		{ice.StateNew, ICEConnectionStateNew, PeerConnectionStateNew},
		{ice.StateNew, ICEConnectionStateNew, PeerConnectionStateNew},
		{ice.StateChecking, ICEConnectionStateChecking, PeerConnectionStateConnecting},
		{ice.StateConnected, ICEConnectionStateConnected, PeerConnectionStateConnecting},
		{ice.StateDisconnected, ICEConnectionStateDisconnected, PeerConnectionStateDisconnected},
		{ice.StateFailed, ICEConnectionStateFailed, PeerConnectionStateFailed},
	} {
		pc.onAgentState(tc.agent)
		if pc.iceState != tc.ice || pc.connState != tc.conn {
			t.Fatalf("agent %v: %v and %v, want %v and %v", tc.agent, pc.iceState, pc.connState, tc.ice, tc.conn)
		}
	}
}

// TestLiteDeadline checks the defaults a lite PeerConnection times a
// silent peer by: disconnected after 8s, failed 30s after that.
func TestLiteDeadline(t *testing.T) {
	at := time.Unix(1_700_000_000, 0)
	pc := &PeerConnection{lastRecv: at, iceState: ICEConnectionStateConnected}
	if d := pc.liteDeadline().Sub(at); d != 8*time.Second {
		t.Fatalf("connected: %v", d)
	}
	pc.iceState = ICEConnectionStateDisconnected
	if d := pc.liteDeadline().Sub(at); d != 30*time.Second {
		t.Fatalf("disconnected: %v", d)
	}
}

// TestRejectedSectionWithoutRemote has nothing to reject a mid with when
// there is no remote description, no section of that mid in it, or one
// whose m= line does not parse; such a mid is left out of a description.
func TestRejectedSectionWithoutRemote(t *testing.T) {
	pc := &PeerConnection{midOrder: []string{"0"}}
	if pc.rejectedSection("0") != nil {
		t.Fatal("a section rejected with no remote description")
	}
	pc.remoteDesc = sdp.New(1, 1)
	if pc.rejectedSection("0") != nil {
		t.Fatal("a section rejected for a mid the remote lacks")
	}
	md := &sdp.MediaDescription{Lines: []sdp.Line{{Type: 'm', Value: "audio x"}}}
	md.SetMid("0")
	pc.remoteDesc.AddMedia(md)
	if pc.rejectedSection("0") != nil {
		t.Fatal("a section rejected for a malformed m= line")
	}
	if sd := pc.buildDescription(SDPTypeAnswer, "ufrag", "pwd"); len(sd.Media) != 0 {
		t.Fatalf("a description with %d sections", len(sd.Media))
	}
}

// TestSCTPCloseClosesChannels closes each data channel, once, when the
// association closes.
func TestSCTPCloseClosesChannels(t *testing.T) {
	pc := &PeerConnection{}
	d := &DataChannel{pc: pc, onClose: func() {}}
	pc.channels = []*DataChannel{d}
	pc.onSCTPClose(nil)
	d.closedLocked()
	if !d.closed || len(pc.channels) != 0 || len(pc.events) != 1 {
		t.Fatalf("closed %v, %d channels, %d events", d.closed, len(pc.channels), len(pc.events))
	}
}

// TestStateNoops covers the calls that change nothing: a timer firing
// after Close, gathering not yet done, and a gathering state set again.
func TestStateNoops(t *testing.T) {
	(&PeerConnection{closed: true}).onTimer()
	pc := &PeerConnection{}
	pc.checkGathering()
	pc.setGathering(pc.gathering)
	if pc.gathering != ICEGatheringStateNew || len(pc.events) != 0 {
		t.Fatalf("gathering %v, %d events", pc.gathering, len(pc.events))
	}
}

// TestLiteHeardAgain has a lite PeerConnection that thought its peer gone
// connected again once it hears from it.
func TestLiteHeardAgain(t *testing.T) {
	from := netip.MustParseAddrPort("127.0.0.1:9")
	pc := &PeerConnection{
		cfg:      Configuration{ICELite: true},
		iceState: ICEConnectionStateDisconnected,
		bound:    map[netip.AddrPort]struct{}{from: {}},
	}
	pc.heard(from, time.Now())
	if pc.iceState != ICEConnectionStateConnected {
		t.Fatalf("ICE state %v", pc.iceState)
	}
}

// dtlsClient is a DTLS client that counts the datagrams it sends.
func dtlsClient(t *testing.T, sends *int) *tls.DTLSConn {
	t.Helper()
	cert, err := tls.GenerateWebRTCCertificate()
	if err != nil {
		t.Fatal(err)
	}
	c, err := tls.NewDTLSClient(&stdtls.Config{Certificates: []stdtls.Certificate{cert}, InsecureSkipVerify: true},
		tls.DTLSOptions{Output: func([]byte) { *sends++ }})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestDTLSFailedBeforeStart has the client's DTLS connection fail, on a
// fatal alert from the peer, before ICE selects a pair: starting it then
// fails the PeerConnection, and later datagrams are dropped.
func TestDTLSFailedBeforeStart(t *testing.T) {
	var sends int
	pc := &PeerConnection{
		dtls:       dtlsClient(t, &sends),
		dtlsRole:   sdp.SetupActive,
		iceState:   ICEConnectionStateConnected,
		remoteAddr: netip.MustParseAddrPort("127.0.0.1:9"),
	}
	alert := []byte{21, 0xfe, 0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 40}
	if err := pc.dtls.HandleDatagram(alert, time.Now()); err == nil {
		t.Fatal("a fatal alert did not fail the connection")
	}
	pc.startDTLS(time.Now())
	if !pc.dtlsFailed || pc.connState != PeerConnectionStateFailed || sends != 0 {
		t.Fatalf("failed %v, state %v, %d sends", pc.dtlsFailed, pc.connState, sends)
	}
	pc.handleDTLS(alert, time.Now())
}

// TestDTLSBeforeAnyPair takes the peer's first DTLS datagram before ICE
// has selected a pair: its address is where DTLS replies go, and it waits
// for the DTLS connection.
func TestDTLSBeforeAnyPair(t *testing.T) {
	from := netip.MustParseAddrPort("127.0.0.1:9")
	pc := &PeerConnection{}
	pc.demux(localID, from, []byte{22, 0xfe, 0xfd}, time.Now())
	if pc.remoteAddr != from || pc.remoteLocal != localID || len(pc.earlyDTLS) != 1 {
		t.Fatalf("remote %v by %d, %d early datagrams", pc.remoteAddr, pc.remoteLocal, len(pc.earlyDTLS))
	}
}

// TestTimerRetransmitsDTLS has the timer resend a ClientHello that went
// unanswered past its timeout.
func TestTimerRetransmitsDTLS(t *testing.T) {
	var sends int
	pc := &PeerConnection{dtls: dtlsClient(t, &sends), timer: time.AfterFunc(time.Hour, func() {})}
	t.Cleanup(func() { pc.timer.Stop() })
	if err := pc.dtls.Start(time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	pc.onTimer()
	if sends != 2 {
		t.Fatalf("%d ClientHellos, want 2", sends)
	}
}

// srtpPair returns the contexts of the two ends of an SRTP session: the
// sender's outgoing one and the receiver's incoming one.
func srtpPair(t *testing.T) (out, in *srtp.Context) {
	t.Helper()
	p := srtp.ProfileAES128CMHMACSHA1_80
	km := make([]byte, p.KeyingMaterialLen())
	out, _, err := srtp.NewContexts(km, p, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, in, err = srtp.NewContexts(km, p, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	return out, in
}

// TestRTPOfNoTransceiver drops packets that are too short, of an SSRC no
// transceiver takes, or of one whose transceiver lacks their payload type.
func TestRTPOfNoTransceiver(t *testing.T) {
	out, in := srtpPair(t)
	tr := &Transceiver{mid: "0", current: DirectionRecvOnly}
	pc := &PeerConnection{
		srtpIn:       in,
		tracks:       map[uint32]*TrackRemote{},
		remoteSSRCs:  map[uint32]string{2: "0"},
		transceivers: []*Transceiver{tr},
		cfg:          Configuration{MaxRemoteSSRCs: 8},
	}
	pc.handleRTP(make([]byte, 4), time.Now())
	for _, ssrc := range []byte{1, 2} {
		b, err := out.ProtectRTP(nil, []byte{0x80, 96, 0, 1, 0, 0, 0, 1, 0, 0, 0, ssrc, 0xaa})
		if err != nil {
			t.Fatal(err)
		}
		pc.handleRTP(b, time.Now())
	}
	if len(pc.tracks) != 0 || len(tr.tracks) != 0 || len(pc.events) != 0 {
		t.Fatalf("%d tracks, %d events", len(pc.tracks), len(pc.events))
	}
}

// TestMalformedRTCP drops SRTCP that does not authenticate, and stops at
// a compound packet whose RTCP header runs past it.
func TestMalformedRTCP(t *testing.T) {
	out, in := srtpPair(t)
	pc := &PeerConnection{srtpIn: in, onRTCP: func([]byte) {}}
	pc.handleRTCP(make([]byte, 40), time.Now())
	b, err := out.ProtectRTCP(nil, []byte{0x80, 201, 0, 5, 0, 0, 0, 1})
	if err != nil {
		t.Fatal(err)
	}
	pc.handleRTCP(b, time.Now())
	if len(pc.events) != 1 {
		t.Fatalf("%d events, want the one OnRTCP", len(pc.events))
	}
	// WriteRTCP refuses a packet SRTCP cannot protect.
	pc = &PeerConnection{srtpOut: out}
	if err := pc.WriteRTCP([]byte{0x80}); !errors.Is(err, srtp.ErrShortPacket) {
		t.Fatalf("a short RTCP packet: %v", err)
	}
}

// TestReceiverReportLimit reports on 31 streams at most, the most one
// receiver report holds.
func TestReceiverReportLimit(t *testing.T) {
	out, _ := srtpPair(t)
	pc := &PeerConnection{srtpOut: out, tracks: map[uint32]*TrackRemote{}}
	for i := range 32 {
		pc.tracks[uint32(i+1)] = &TrackRemote{ssrc: uint32(i + 1)}
	}
	pc.sendReports(time.Now())
	reported := 0
	for _, tr := range pc.tracks {
		if tr.expectedPrior != 0 {
			reported++
		}
	}
	if reported != 31 {
		t.Fatalf("%d streams reported", reported)
	}
}

// TestReportClock maps a sender report's wall time to RTP time by the
// track's report clock when it has one, and from the last packet sent
// when it has none.
func TestReportClock(t *testing.T) {
	out, _ := srtpPair(t)
	wall := time.Unix(1700000000, 0)
	track := NewTrackLocal(Codec{MimeType: "audio/opus", ClockRate: 48000}, "a", "s")
	s := &RTPSender{ssrc: 7, pt: 111, clockRate: 48000, packets: 1, lastTS: 5, lastAt: wall, track: track}
	pc := &PeerConnection{srtpOut: out, transceivers: []*Transceiver{{sender: s}}}
	report := func(now time.Time) uint32 {
		t.Helper()
		pc.sendReports(now)
		_, p, _, err := rtp.NextRTCP(pc.rtcpBuf[:cap(pc.rtcpBuf)])
		var sr rtp.SenderReport
		if err != nil || sr.Unmarshal(p) != nil || sr.SSRC != 7 {
			t.Fatalf("no sender report: %v", err)
		}
		return sr.RTPTime
	}
	if ts := report(wall.Add(time.Second)); ts != 5+48000 {
		t.Fatalf("from the last packet: %d", ts)
	}
	track.SetReportClock(wall.Add(-2*time.Second), 1<<32-48000)
	if ts := report(wall.Add(1500 * time.Millisecond)); ts != 48000*5/2 {
		t.Fatalf("by the clock: %d", ts)
	}
	if ts := report(wall.Add(-3 * time.Second)); ts != 1<<32-48000*2 {
		t.Fatalf("before the clock: %d", ts)
	}
	s.track = nil
	if s.reportClock() != nil {
		t.Fatal("a clock with no track")
	}
}

package webrtc

import (
	"strings"
	"testing"

	"github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/webrtc/sdp"
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
	for _, b := range [][]byte{r.Raw, m.Raw[:10], {0, 1, 0, 4, 0x21, 0x12, 0xa4, 0x42}, append(m.Raw[:20:20], 0, 6, 0, 99)} {
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
	got := intersectCodecs("video", []sdp.Codec{{PayloadType: 120, Name: "VP9", ClockRate: 90000, Fmtp: "profile-id=2"}, {PayloadType: 97, Name: "VP8", ClockRate: 90000}, {PayloadType: 98, Name: "rtx", ClockRate: 90000}}, DefaultCodecs())
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
		{"turns:127.0.0.1", "invalid AddrPort", false, false},
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

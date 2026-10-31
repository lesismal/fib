package sdp

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func readTestdata(t testing.TB) map[string][]byte {
	t.Helper()
	paths, err := filepath.Glob("testdata/*.sdp")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no testdata: %v", err)
	}
	files := make(map[string][]byte, len(paths))
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		files[filepath.Base(p)] = b
	}
	return files
}

func mustParse(t testing.TB, b []byte) *SessionDescription {
	t.Helper()
	sd, err := Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	return sd
}

// TestRoundTrip marshals each browser's description back to the bytes it
// was parsed from, and reads the same lines from LF endings and from a
// description that lacks its final line ending.
func TestRoundTrip(t *testing.T) {
	for name, b := range readTestdata(t) {
		t.Run(name, func(t *testing.T) {
			if !bytes.HasSuffix(b, []byte("\r\n")) || bytes.Count(b, []byte("\n")) != bytes.Count(b, []byte("\r\n")) {
				t.Fatal("testdata does not use CRLF line endings")
			}
			sd := mustParse(t, b)
			if got := sd.Marshal(); !bytes.Equal(got, b) {
				t.Fatalf("marshal differs:\n%s", got)
			}
			if got := sd.String(); got != string(b) {
				t.Fatal("String differs from Marshal")
			}
			lf := bytes.ReplaceAll(b, []byte("\r\n"), []byte("\n"))
			for _, in := range [][]byte{lf, lf[:len(lf)-1], b[:len(b)-2], append(append([]byte(nil), b...), "\r\n\n"...)} {
				if got := mustParse(t, in).Marshal(); !bytes.Equal(got, b) {
					t.Fatalf("marshal of %q... differs", in[:20])
				}
			}
			if got := sd.Clone().Marshal(); !bytes.Equal(got, b) {
				t.Fatal("clone marshals differently")
			}
		})
	}
}

type mediaWant struct {
	kind      string
	mid       string
	direction Direction
	codecs    []string // "pt name/rate[/channels]"
	rids      []string
	ssrcs     int
}

type browserWant struct {
	origin      Origin
	bundle      []string
	setup       Setup
	ufrag, pwd  string
	fingerprint string
	options     []string
	candidates  int
	eoc         bool
	maxMessage  uint64
	media       []mediaWant
}

var browserWants = map[string]browserWant{
	"chrome_offer.sdp": {
		origin: Origin{"-", 4215775240449105457, 2, "IN", "IP4", "127.0.0.1"},
		bundle: []string{"0", "1", "2"}, setup: SetupActpass,
		ufrag: "Xk9c", pwd: "4WqzLr0N8sZpVb2Hj7TfYd1u",
		fingerprint: "sha-256 7B:8B:F0:65:5F:78:E2:51:3B:AC:6F:F3:3F:46:1B:35:DC:B8:5F:64:1A:24:C2:43:F0:A1:58:D0:A1:2C:19:08",
		options:     []string{"trickle"}, maxMessage: 262144,
		media: []mediaWant{
			{"audio", "0", SendRecv, []string{"111 opus/48000/2", "63 red/48000/2", "9 G722/8000", "0 PCMU/8000", "8 PCMA/8000", "13 CN/8000", "110 telephone-event/48000", "126 telephone-event/8000"}, nil, 2},
			{"video", "1", SendRecv, []string{"96 VP8/90000", "97 rtx/90000", "102 H264/90000", "103 rtx/90000", "98 VP9/90000", "99 rtx/90000", "45 AV1/90000", "46 rtx/90000", "116 red/90000", "117 rtx/90000", "118 ulpfec/90000"}, nil, 4},
			{"application", "2", SendRecv, nil, nil, 0},
		},
	},
	"chrome_answer.sdp": {
		origin: Origin{"-", 6729150283746195012, 2, "IN", "IP4", "127.0.0.1"},
		bundle: []string{"0", "1", "2"}, setup: SetupActive,
		ufrag: "vR2s", pwd: "Qm8Hc3LkZt5Ny0Wb7Jr2Pv9x",
		fingerprint: "sha-256 A1:5C:0E:94:7F:22:B8:6D:33:E1:C9:48:0B:F7:6A:2D:91:5E:C4:08:7B:3F:A2:D6:1E:89:C0:54:2B:F3:6E:17",
		options:     []string{"trickle"}, maxMessage: 262144,
		media: []mediaWant{
			{"audio", "0", RecvOnly, []string{"109 opus/48000/2", "9 G722/8000", "0 PCMU/8000", "8 PCMA/8000", "101 telephone-event/8000"}, nil, 0},
			{"video", "1", SendRecv, []string{"120 VP8/90000", "124 rtx/90000", "126 H264/90000", "127 rtx/90000"}, nil, 2},
			{"application", "2", SendRecv, nil, nil, 0},
		},
	},
	"chrome_simulcast_offer.sdp": {
		origin: Origin{"-", 8473920165038471926, 2, "IN", "IP4", "127.0.0.1"},
		bundle: []string{"0"}, setup: SetupActpass,
		ufrag: "H7fQ", pwd: "b3Zr9Kp1Wx6Nc2Ly8Tv0Qs5M",
		fingerprint: "sha-256 3E:A8:71:0C:D5:9F:24:B6:6E:13:C7:F0:58:2A:9D:E4:B1:07:6C:F3:48:D2:95:1A:EC:3B:80:67:F9:24:5D:C0",
		options:     []string{"trickle"},
		media: []mediaWant{
			{"video", "0", SendOnly, []string{"96 VP8/90000", "97 rtx/90000", "102 H264/90000", "103 rtx/90000"}, []string{"q", "h", "f"}, 0},
		},
	},
	"chrome_datachannel_offer.sdp": {
		origin: Origin{"-", 1287640985126340127, 2, "IN", "IP4", "127.0.0.1"},
		bundle: []string{"0"}, setup: SetupActpass,
		ufrag: "pL4w", pwd: "Zx7Kb2Nq9Rc4Vm1Hs8Ty3Gf6",
		fingerprint: "sha-256 C4:19:7E:A2:5B:F8:30:D6:9C:41:E7:2F:86:0B:D3:5A:1F:C9:64:B0:E8:27:93:4D:A5:6C:12:F0:3B:8E:D7:59",
		options:     []string{"trickle"}, candidates: 2, maxMessage: 262144,
		media: []mediaWant{{"application", "0", SendRecv, nil, nil, 0}},
	},
	"firefox_offer.sdp": {
		origin: Origin{"mozilla...THIS_IS_SDPARTA-131.0", 1932515542476378424, 0, "IN", "IP4", "0.0.0.0"},
		bundle: []string{"0", "1", "2"}, setup: SetupActpass,
		ufrag: "4e7a9c1d", pwd: "8c5b2e91f04d7a63b9e1c2d58f07a4b6",
		fingerprint: "sha-256 5D:E2:8A:41:0F:B7:96:3C:D1:68:2E:F9:A4:07:5B:C3:E8:1D:72:96:4F:B0:3A:E5:C7:18:6D:92:F4:0E:A1:3B",
		options:     []string{"trickle"}, maxMessage: 1073741823,
		media: []mediaWant{
			{"audio", "0", SendRecv, []string{"109 opus/48000/2", "9 G722/8000/1", "0 PCMU/8000", "8 PCMA/8000", "101 telephone-event/8000"}, nil, 1},
			{"video", "1", SendRecv, []string{"120 VP8/90000", "124 rtx/90000", "121 VP9/90000", "125 rtx/90000", "126 H264/90000", "127 rtx/90000", "97 H264/90000", "98 rtx/90000"}, nil, 2},
			{"application", "2", SendRecv, nil, nil, 0},
		},
	},
	"firefox_answer.sdp": {
		origin: Origin{"mozilla...THIS_IS_SDPARTA-131.0", 7006279766542240474, 0, "IN", "IP4", "0.0.0.0"},
		bundle: []string{"0", "1", "2"}, setup: SetupActive,
		ufrag: "b7d2e04f", pwd: "e13a7c90b25f48d6a1c3e7f02b9d4a85",
		fingerprint: "sha-256 92:1F:6C:B4:08:E3:7A:D5:41:9C:2B:F0:65:A8:D3:17:4E:B9:C2:06:7F:35:E8:A1:5D:94:0C:B7:3E:62:F8:2A",
		options:     []string{"trickle"}, candidates: 3, eoc: true, maxMessage: 1073741823,
		media: []mediaWant{
			{"audio", "0", SendRecv, []string{"111 opus/48000/2", "0 PCMU/8000", "8 PCMA/8000"}, nil, 1},
			{"video", "1", RecvOnly, []string{"96 VP8/90000", "97 rtx/90000"}, nil, 0},
			{"application", "2", SendRecv, nil, nil, 0},
		},
	},
	"firefox_simulcast_offer.sdp": {
		origin: Origin{"mozilla...THIS_IS_SDPARTA-131.0", 3871046295180437562, 0, "IN", "IP4", "0.0.0.0"},
		bundle: []string{"0"}, setup: SetupActpass,
		ufrag: "a1c5e93b", pwd: "3f9a0d6c1e7b4825a9d0c3f6e1b84a72",
		fingerprint: "sha-256 0B:7E:D4:63:A9:12:F5:8C:3D:E6:21:B8:94:5F:CA:07:E3:4B:96:D1:28:7C:F0:A5:3E:89:16:CB:52:D7:0F:A4",
		options:     []string{"trickle"},
		media: []mediaWant{
			{"video", "0", SendOnly, []string{"120 VP8/90000", "124 rtx/90000", "126 H264/90000", "127 rtx/90000"}, []string{"h", "m", "l"}, 6},
		},
	},
	"firefox_datachannel_offer.sdp": {
		origin: Origin{"mozilla...THIS_IS_SDPARTA-131.0", 5248179036514829047, 0, "IN", "IP4", "0.0.0.0"},
		bundle: []string{"0"}, setup: SetupActpass,
		ufrag: "9d4b1f7e", pwd: "6a2e9f0c4b7d1385e2a9c0f47b6d3e18",
		fingerprint: "sha-256 E7:30:9B:4C:D2:85:1A:F6:63:B9:0E:72:C8:5D:A1:3F:94:26:EB:07:D5:48:BC:61:2F:A9:E3:76:10:C4:8D:5B",
		options:     []string{"trickle"}, candidates: 3, eoc: true, maxMessage: 1073741823,
		media: []mediaWant{{"application", "0", SendRecv, nil, nil, 0}},
	},
	"safari_offer.sdp": {
		origin: Origin{"-", 2902417638270418541, 2, "IN", "IP4", "127.0.0.1"},
		bundle: []string{"0", "1", "2"}, setup: SetupActpass,
		ufrag: "t5Jm", pwd: "Wd3Nk8Bq1Xz6Lc0Rv9Hp2Sy7",
		fingerprint: "sha-256 6F:D3:28:A0:7C:E9:15:B4:4A:82:F1:5E:C6:39:0D:A7:B2:64:1F:E8:93:5C:D0:2B:76:A4:E1:08:3F:C5:9A:14",
		options:     []string{"trickle"}, maxMessage: 262144,
		media: []mediaWant{
			{"audio", "0", SendRecv, []string{"111 opus/48000/2", "63 red/48000/2", "103 ISAC/16000", "9 G722/8000", "102 ILBC/8000", "0 PCMU/8000", "8 PCMA/8000", "105 CN/16000", "13 CN/8000", "110 telephone-event/48000", "113 telephone-event/16000", "126 telephone-event/8000"}, nil, 2},
			{"video", "1", SendRecv, []string{"96 H264/90000", "97 rtx/90000", "98 H264/90000", "99 rtx/90000", "100 H265/90000", "101 rtx/90000", "127 VP8/90000", "125 rtx/90000", "104 red/90000"}, nil, 4},
			{"application", "2", SendRecv, nil, nil, 0},
		},
	},
	"safari_datachannel_answer.sdp": {
		origin: Origin{"-", 7381920465017283946, 2, "IN", "IP4", "127.0.0.1"},
		bundle: []string{"0"}, setup: SetupActive,
		ufrag: "Rb8v", pwd: "Gq5Tn2Wk9Hc7Ld1Zx4Mp6Vs3",
		fingerprint: "sha-256 19:B5:E0:7A:C3:6D:F2:48:A1:9E:05:D7:3C:B6:82:4F:E9:1A:70:C8:5B:D3:26:9F:4E:A0:B7:13:6C:F5:D8:02",
		options:     []string{"trickle"}, maxMessage: 262144,
		media: []mediaWant{{"application", "0", SendRecv, nil, nil, 0}},
	},
}

// TestBrowserDescriptions reads the typed values of each browser's
// description, looking an attribute up in the session where a browser
// writes it there, as Firefox does the fingerprint.
func TestBrowserDescriptions(t *testing.T) {
	files := readTestdata(t)
	if len(files) != len(browserWants) {
		t.Fatalf("%d testdata files, %d expectations", len(files), len(browserWants))
	}
	for name, want := range browserWants {
		t.Run(name, func(t *testing.T) {
			sd := mustParse(t, files[name])
			if o, ok := sd.Origin(); !ok || o != want.origin {
				t.Errorf("origin %+v", o)
			}
			if b, ok := sd.Bundle(); !ok || !reflect.DeepEqual(b, want.bundle) {
				t.Errorf("bundle %v", b)
			}
			if sd.ICELite() {
				t.Error("ice-lite")
			}
			if len(sd.Media) != len(want.media) {
				t.Fatalf("%d media sections", len(sd.Media))
			}
			candidates := 0
			for i, mw := range want.media {
				md := sd.Media[i]
				if md.Kind() != mw.kind {
					t.Errorf("media %d kind %q", i, md.Kind())
				}
				if mid, _ := md.Mid(); mid != mw.mid || sd.MediaByMid(mw.mid) != md {
					t.Errorf("media %d mid %q", i, mid)
				}
				if d := sd.MediaDirection(md); d != mw.direction {
					t.Errorf("media %d direction %q", i, d)
				}
				if v, _ := sd.Lookup(md, "setup"); Setup(v) != want.setup {
					t.Errorf("media %d setup %q", i, v)
				}
				if v, _ := sd.Lookup(md, "ice-ufrag"); v != want.ufrag {
					t.Errorf("media %d ufrag %q", i, v)
				}
				if v, _ := sd.Lookup(md, "ice-pwd"); v != want.pwd {
					t.Errorf("media %d pwd %q", i, v)
				}
				fps := md.Fingerprints()
				if len(fps) == 0 {
					fps = sd.Fingerprints()
				}
				if len(fps) != 1 || fps[0].String() != want.fingerprint {
					t.Errorf("media %d fingerprints %v", i, fps)
				}
				options := md.ICEOptions()
				if len(options) == 0 {
					options = sd.ICEOptions()
				}
				if !reflect.DeepEqual(options, want.options) {
					t.Errorf("media %d ice-options %v", i, options)
				}
				for _, c := range md.Candidates() {
					if !strings.HasPrefix(c, "candidate:") {
						t.Errorf("candidate %q", c)
					}
					candidates++
				}
				// Firefox writes candidates only in the first section of
				// a bundle.
				if len(md.Candidates()) > 0 && md.EndOfCandidates() != want.eoc {
					t.Errorf("media %d end-of-candidates %v", i, md.EndOfCandidates())
				}
				var codecs []string
				for _, c := range md.Codecs() {
					codecs = append(codecs, RTPMap{c.PayloadType, c.Name, c.ClockRate, c.Channels}.String())
				}
				if !reflect.DeepEqual(codecs, mw.codecs) {
					t.Errorf("media %d codecs %v", i, codecs)
				}
				var rids []string
				for _, r := range md.RIDs() {
					if r.Direction != RIDSend {
						t.Errorf("rid %+v", r)
					}
					rids = append(rids, r.ID)
				}
				if !reflect.DeepEqual(rids, mw.rids) {
					t.Errorf("media %d rids %v", i, rids)
				}
				if sc, ok := md.Simulcast(); ok != (mw.rids != nil) || (ok && (len(sc.Send) != len(mw.rids) || sc.Recv != nil)) {
					t.Errorf("media %d simulcast %+v %v", i, sc, ok)
				}
				if len(md.SSRCs()) != mw.ssrcs {
					t.Errorf("media %d ssrcs %v", i, md.SSRCs())
				}
				if md.IsDataChannel() != (mw.kind == "application") {
					t.Errorf("media %d IsDataChannel", i)
				}
				if md.IsDataChannel() {
					if p, ok := md.SCTPPort(); !ok || p != 5000 {
						t.Errorf("sctp-port %d", p)
					}
					if n, ok := md.MaxMessageSize(); !ok || n != want.maxMessage {
						t.Errorf("max-message-size %d", n)
					}
				} else {
					if !md.RTCPMux() {
						t.Errorf("media %d lacks rtcp-mux", i)
					}
					if len(md.ExtMaps()) == 0 || len(md.MSIDs()) != 1 && mw.direction != RecvOnly {
						t.Errorf("media %d extmaps %v msids %v", i, md.ExtMaps(), md.MSIDs())
					}
				}
			}
			if candidates != want.candidates {
				t.Errorf("%d candidates", candidates)
			}
		})
	}
}

// TestChromeOfferDetails reads the values a PeerConnection negotiates
// codecs and streams with from Chrome's offer.
func TestChromeOfferDetails(t *testing.T) {
	sd := mustParse(t, readTestdata(t)["chrome_offer.sdp"])
	audio, video := sd.Media[0], sd.Media[1]
	opus, ok := audio.Codec(111)
	if !ok || opus.Fmtp != "minptime=10;useinbandfec=1" || !reflect.DeepEqual(opus.Feedback, []string{"transport-cc"}) {
		t.Fatalf("opus %+v", opus)
	}
	if v, ok := opus.Param("useinbandfec"); !ok || v != "1" {
		t.Fatalf("useinbandfec %q", v)
	}
	h264, _ := video.Codec(102)
	if v, _ := h264.Param("profile-level-id"); v != "42e01f" {
		t.Fatalf("h264 %+v", h264)
	}
	if want := []string{"goog-remb", "transport-cc", "ccm fir", "nack", "nack pli"}; !reflect.DeepEqual(h264.Feedback, want) {
		t.Fatalf("h264 feedback %v", h264.Feedback)
	}
	if rtx, _ := video.Codec(103); rtx.Fmtp != "apt=102" {
		t.Fatalf("rtx %+v", rtx)
	}
	if !video.RTCPRsize() || !audio.RTCPRsize() {
		t.Fatal("rtcp-rsize")
	}
	if got := video.SSRCGroups(); len(got) != 1 || got[0].Semantics != "FID" || !reflect.DeepEqual(got[0].SSRCs, []uint32{1734906122, 3127542081}) {
		t.Fatalf("ssrc-group %v", got)
	}
	if got := video.SSRCs()[1]; got != (SSRC{1734906122, "msid", "3f1c2a9e-5b7d-4e8a-9c0f-6d2b1a4e8f73 0c6e2b8f-9d1a-4f3c-b5e7-2a8d4c6f1b90"}) {
		t.Fatalf("ssrc %+v", got)
	}
	if got := video.MSIDs(); len(got) != 1 || got[0] != (MSID{"3f1c2a9e-5b7d-4e8a-9c0f-6d2b1a4e8f73", "0c6e2b8f-9d1a-4f3c-b5e7-2a8d4c6f1b90"}) {
		t.Fatalf("msid %v", got)
	}
	if got := video.ExtMaps()[9]; got != (ExtMap{ID: 10, URI: "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id"}) {
		t.Fatalf("extmap %+v", got)
	}
	if c, ok := video.Connection(); !ok || c != (Connection{"IN", "IP4", "0.0.0.0"}) {
		t.Fatalf("c= %+v", c)
	}
	if m, _ := video.MediaName(); m.Port != 9 || m.Proto != ProtoRTP || len(m.Formats) != 11 {
		t.Fatalf("m= %+v", m)
	}
	if !sd.HasAttribute("extmap-allow-mixed") {
		t.Fatal("extmap-allow-mixed")
	}
	if v, _ := sd.Attribute("msid-semantic"); v != " WMS 3f1c2a9e-5b7d-4e8a-9c0f-6d2b1a4e8f73" {
		t.Fatalf("msid-semantic %q", v)
	}
	if v, ok := audio.Attribute("rtcp"); !ok || v != "9 IN IP4 0.0.0.0" {
		t.Fatalf("rtcp %q", v)
	}

	ff := mustParse(t, readTestdata(t)["firefox_offer.sdp"])
	if got := ff.Media[0].ExtMaps()[1]; got != (ExtMap{2, RecvOnly, "urn:ietf:params:rtp-hdrext:csrc-audio-level", ""}) {
		t.Fatalf("firefox extmap %+v", got)
	}
	if d, ok := ff.Direction(); !ok || d != SendRecv {
		t.Fatal("firefox session direction")
	}
}

func TestParseErrors(t *testing.T) {
	const head = "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n"
	for _, tc := range []struct {
		in  string
		err error
	}{
		{"", ErrEmpty},
		{"\r\n\n", ErrEmpty},
		{"o=- 1 2 IN IP4 127.0.0.1\r\n", ErrMissingVersion},
		{"v=1\r\n", ErrUnsupportedVersion},
		{"v=0\r\n", ErrMissingOrigin},
		{"v=0\r\n\r\n", ErrMissingOrigin},
		{"v=0\r\ns=-\r\n", ErrMissingOrigin},
		{"v=0\r\no=- 1 2 IN IP4\r\ns=-\r\nt=0 0\r\n", ErrInvalidOrigin},
		{"v=0\r\no=- x 2 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n", ErrInvalidOrigin},
		{"v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\n", ErrMissingSessionName},
		{"v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\nt=0 0\r\n", ErrMissingSessionName},
		{"v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\n", ErrMissingTiming},
		{"v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=-\r\nm=audio 9 RTP/AVP 0\r\nt=0 0\r\n", ErrMisplacedLine},
		{head + "v=0\r\n", ErrMisplacedLine},
		{head + "s=x\r\n", ErrMisplacedLine},
		{head + "\r\na=x\r\n", ErrInvalidLine},
		{head + "a\r\n", ErrInvalidLine},
		{head + "A=x\r\n", ErrInvalidLine},
		{head + " a=x\r\n", ErrInvalidLine},
		{head + "a=x\ry\r\n", ErrInvalidLine},
		{head + "a=x\r\r\n", ErrInvalidLine},
		{head + "a=x\x00\r\n", ErrInvalidLine},
		{head + "m=audio 9 RTP/AVP\r\n", ErrInvalidMedia},
		{head + "m=audio x RTP/AVP 0\r\n", ErrInvalidMedia},
		{head + "m=audio 70000 RTP/AVP 0\r\n", ErrInvalidMedia},
		{head + "m=audio -1 RTP/AVP 0\r\n", ErrInvalidMedia},
		{head + "m=audio 9/0 RTP/AVP 0\r\n", ErrInvalidMedia},
	} {
		if _, err := ParseString(tc.in); !errors.Is(err, tc.err) {
			t.Errorf("%q: %v, want %v", tc.in, err, tc.err)
		}
	}

	// Unknown line types and attributes, an empty s= and port counts are
	// accepted and kept.
	in := "v=0\r\no=- 1 2 IN IP4 127.0.0.1\r\ns=\r\nt=0 0\r\ny=unknown\r\na=x-custom:1 2\r\nm=audio 9/2 RTP/AVP 0\r\nq=what\r\na=\r\n"
	sd, err := ParseString(in)
	if err != nil {
		t.Fatal(err)
	}
	if sd.String() != in {
		t.Fatalf("got %q", sd.String())
	}
	if m, _ := sd.Media[0].MediaName(); m.NumPorts != 2 || m.String() != "audio 9/2 RTP/AVP 0" {
		t.Fatalf("m= %+v", m)
	}
}

func TestValues(t *testing.T) {
	for _, tc := range []struct {
		in    string
		parse func(string) (any, error)
	}{
		{"- 1 2 IN IP4 127.0.0.1", func(s string) (any, error) { return ParseOrigin(s) }},
		{"IN IP6 ::1", func(s string) (any, error) { return ParseConnection(s) }},
		{"video 9 UDP/TLS/RTP/SAVPF 96 97", func(s string) (any, error) { return ParseMediaName(s) }},
		{"BUNDLE 0 1", func(s string) (any, error) { return ParseGroup(s) }},
		{"BUNDLE", func(s string) (any, error) { return ParseGroup(s) }},
		{"sha-256 AB:CD", func(s string) (any, error) { return ParseFingerprint(s) }},
		{"111 opus/48000/2", func(s string) (any, error) { return ParseRTPMap(s) }},
		{"0 PCMU/8000", func(s string) (any, error) { return ParseRTPMap(s) }},
		{"111 minptime=10;useinbandfec=1", func(s string) (any, error) { return ParseFmtp(s) }},
		{"* nack", func(s string) (any, error) { return ParseRTCPFeedback(s) }},
		{"96 nack pli", func(s string) (any, error) { return ParseRTCPFeedback(s) }},
		{"2/recvonly urn:x attr", func(s string) (any, error) { return ParseExtMap(s) }},
		{"4294967295 cname:x", func(s string) (any, error) { return ParseSSRC(s) }},
		{"FID 1 2", func(s string) (any, error) { return ParseSSRCGroup(s) }},
		{"- track", func(s string) (any, error) { return ParseMSID(s) }},
		{"stream", func(s string) (any, error) { return ParseMSID(s) }},
		{"f send pt=96;max-width=1280", func(s string) (any, error) { return ParseRID(s) }},
		{"1 recv", func(s string) (any, error) { return ParseRID(s) }},
		{"send a,~b;c recv d", func(s string) (any, error) { return ParseSimulcast(s) }},
	} {
		v, err := tc.parse(tc.in)
		if err != nil {
			t.Errorf("%q: %v", tc.in, err)
			continue
		}
		if s := v.(interface{ String() string }).String(); s != tc.in {
			t.Errorf("%q written as %q", tc.in, s)
		}
	}

	if s, err := ParseSimulcast("recv 1;~2 send 3"); err != nil || !reflect.DeepEqual(s, Simulcast{
		Send: [][]SimulcastRID{{{"3", false}}},
		Recv: [][]SimulcastRID{{{"1", false}}, {{"2", true}}},
	}) {
		t.Errorf("simulcast %+v %v", s, err)
	}
	if s, err := ParseSimulcast("send rid=1;2"); err != nil || len(s.Send) != 2 {
		t.Errorf("draft simulcast %+v %v", s, err)
	}

	for _, tc := range []struct {
		in    string
		parse func(string) error
	}{
		{"", func(s string) error { _, err := ParseGroup(s); return err }},
		{"sha-256", func(s string) error { _, err := ParseFingerprint(s); return err }},
		{"sha-256 AB CD", func(s string) error { _, err := ParseFingerprint(s); return err }},
		{"opus/48000", func(s string) error { _, err := ParseRTPMap(s); return err }},
		{"128 opus/48000", func(s string) error { _, err := ParseRTPMap(s); return err }},
		{"111 opus", func(s string) error { _, err := ParseRTPMap(s); return err }},
		{"111 opus/x", func(s string) error { _, err := ParseRTPMap(s); return err }},
		{"111 opus/48000/2/1", func(s string) error { _, err := ParseRTPMap(s); return err }},
		{"x a=1", func(s string) error { _, err := ParseFmtp(s); return err }},
		{"111", func(s string) error { _, err := ParseFmtp(s); return err }},
		{"96", func(s string) error { _, err := ParseRTCPFeedback(s); return err }},
		{"x nack", func(s string) error { _, err := ParseRTCPFeedback(s); return err }},
		{"0 urn:x", func(s string) error { _, err := ParseExtMap(s); return err }},
		{"1/sideways urn:x", func(s string) error { _, err := ParseExtMap(s); return err }},
		{"1", func(s string) error { _, err := ParseExtMap(s); return err }},
		{"4294967296 cname:x", func(s string) error { _, err := ParseSSRC(s); return err }},
		{"1", func(s string) error { _, err := ParseSSRC(s); return err }},
		{"FID 1 x", func(s string) error { _, err := ParseSSRCGroup(s); return err }},
		{"a b c", func(s string) error { _, err := ParseMSID(s); return err }},
		{"f both", func(s string) error { _, err := ParseRID(s); return err }},
		{"f=1 send", func(s string) error { _, err := ParseRID(s); return err }},
		{"send", func(s string) error { _, err := ParseSimulcast(s); return err }},
		{"send a send b", func(s string) error { _, err := ParseSimulcast(s); return err }},
		{"both a", func(s string) error { _, err := ParseSimulcast(s); return err }},
		{"send a;;b", func(s string) error { _, err := ParseSimulcast(s); return err }},
		{"send ~", func(s string) error { _, err := ParseSimulcast(s); return err }},
		{"IN IP4", func(s string) error { _, err := ParseConnection(s); return err }},
	} {
		if err := tc.parse(tc.in); !errors.Is(err, ErrInvalidAttribute) {
			t.Errorf("%q: %v", tc.in, err)
		}
	}

	if v, ok := FmtpParam("a=1; profile-level-id=42e01f ;x", "PROFILE-level-id"); !ok || v != "42e01f" {
		t.Errorf("FmtpParam %q", v)
	}
	if v, ok := FmtpParam("a=1;x", "x"); !ok || v != "" {
		t.Errorf("FmtpParam flag %q %v", v, ok)
	}
	if _, ok := FmtpParam("a=1", "b"); ok {
		t.Error("FmtpParam found a missing key")
	}
	if SendOnly.Reverse() != RecvOnly || RecvOnly.Reverse() != SendOnly || Inactive.Reverse() != Inactive {
		t.Error("Reverse")
	}
}

// TestEdit checks that setters replace a line where it stands, that added
// lines go where RFC 8866 orders them, and that a candidate goes before
// end-of-candidates.
func TestEdit(t *testing.T) {
	md := NewMedia("audio", 9, ProtoRTP)
	md.AddAttribute("mid", "0")
	md.AddFlag("sendrecv")
	md.AddAttribute("x", "1")
	md.AddAttribute("x", "2")
	md.AddLine('b', "AS:30")
	md.AddLine('i', "info")
	md.SetDirection(RecvOnly)
	md.SetAttribute("x", "3")
	md.SetEndOfCandidates(true)
	md.AddCandidate("a=candidate:1 1 udp 1 1.2.3.4 5 typ host")
	md.AddCandidate("2 1 udp 1 1.2.3.4 6 typ host")
	md.SetFlag("rtcp-mux", true)
	md.SetFlag("rtcp-mux", true)
	md.SetConnection(Connection{"IN", "IP6", "::"})
	want := []string{
		"m=audio 9 UDP/TLS/RTP/SAVPF",
		"i=info",
		"c=IN IP6 ::",
		"b=AS:30",
		"a=mid:0",
		"a=recvonly",
		"a=x:3",
		"a=candidate:1 1 udp 1 1.2.3.4 5 typ host",
		"a=candidate:2 1 udp 1 1.2.3.4 6 typ host",
		"a=end-of-candidates",
		"a=rtcp-mux",
	}
	var got []string
	for _, l := range md.Lines {
		got = append(got, l.String())
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("lines:\n%s", strings.Join(got, "\n"))
	}
	if n := md.DeleteAttribute("candidate"); n != 2 {
		t.Fatalf("deleted %d", n)
	}
	md.SetFlag("rtcp-mux", false)
	if md.RTCPMux() || len(md.Candidates()) != 0 {
		t.Fatal("not deleted")
	}
	md.SetPort(0)
	if !md.Rejected() {
		t.Fatal("not rejected")
	}

	sd := New(1, 2)
	sd.AddLine('c', "IN IP4 0.0.0.0")
	sd.AddLine('r', "7d 1h 0 25h")
	sd.AddLine('u', "http://example.com/")
	sd.SetGroup(Group{"LS", []string{"a"}})
	sd.SetBundle("0")
	sd.SetBundle("0", "1")
	sd.SetLine('s', "name")
	o, _ := sd.Origin()
	o.SessionVersion++
	sd.SetOrigin(o)
	const wantSession = "v=0\r\no=- 1 3 IN IP4 127.0.0.1\r\ns=name\r\nu=http://example.com/\r\nc=IN IP4 0.0.0.0\r\n" +
		"t=0 0\r\nr=7d 1h 0 25h\r\na=group:LS a\r\na=group:BUNDLE 0 1\r\n"
	if got := sd.String(); got != wantSession {
		t.Fatalf("session:\n%s", got)
	}
	if v, ok := sd.Line('u'); !ok || v != "http://example.com/" {
		t.Fatal("Line")
	}
}

// TestBuildAnswer builds the answer a PeerConnection would give Chrome's
// offer with the builders, then reads it back.
func TestBuildAnswer(t *testing.T) {
	offer := mustParse(t, readTestdata(t)["chrome_offer.sdp"])
	answer := New(42, 2)
	mids, _ := offer.Bundle()
	answer.SetBundle(mids...)
	answer.AddFlag("extmap-allow-mixed")
	fp := Fingerprint{"sha-256", "01:02:03"}
	for _, om := range offer.Media {
		mid, _ := om.Mid()
		var md *MediaDescription
		if om.IsDataChannel() {
			md = NewDataChannelMedia()
		} else {
			md = NewMedia(om.Kind(), 9, ProtoRTP)
		}
		md.SetICECredentials("ufrag", "pwd")
		md.SetICEOptions("trickle")
		md.AddFingerprint(fp)
		md.SetSetup(SetupActive)
		md.SetMid(mid)
		if om.IsDataChannel() {
			// sctp-port came before the attributes above; put it last
			// as Chrome does.
			md.DeleteAttribute("sctp-port")
			md.SetSCTPPort(DefaultSCTPPort)
			md.SetMaxMessageSize(262144)
		} else {
			for _, e := range om.ExtMaps() {
				md.AddExtMap(e)
			}
			md.SetDirection(offer.MediaDirection(om).Reverse())
			md.AddMSID(MSID{"-", "track-" + mid})
			md.SetRTCPMux(true)
			md.SetRTCPRsize(true)
			c := om.Codecs()[0]
			md.AddCodec(c)
			md.AddSSRC(SSRC{1000, "cname", "x"})
		}
		answer.AddMedia(md)
	}
	answer.SetICELite(true)
	b := answer.Marshal()
	got := mustParse(t, b)
	if !bytes.Equal(got.Marshal(), b) {
		t.Fatal("answer does not round-trip")
	}
	if !got.ICELite() {
		t.Fatal("ice-lite")
	}
	video := got.MediaByMid("1")
	if m, _ := video.MediaName(); !reflect.DeepEqual(m.Formats, []string{"96"}) {
		t.Fatalf("formats %v", m.Formats)
	}
	if c, _ := video.Codec(96); c.Name != "VP8" || len(c.Feedback) != 5 {
		t.Fatalf("codec %+v", c)
	}
	if s, _ := video.Setup(); s != SetupActive {
		t.Fatal("setup")
	}
	if u, _ := video.ICEUfrag(); u != "ufrag" {
		t.Fatal("ufrag")
	}
	if p, _ := video.ICEPwd(); p != "pwd" {
		t.Fatal("pwd")
	}
	dc := got.Media[2]
	if p, _ := dc.SCTPPort(); p != 5000 || dc.Lines[len(dc.Lines)-1].String() != "a=max-message-size:262144" {
		t.Fatalf("data channel %v", dc.Lines)
	}
	if m, _ := dc.MediaName(); m.String() != "application 9 UDP/DTLS/SCTP webrtc-datachannel" {
		t.Fatalf("m= %v", m)
	}
	if NewSessionID() >= 1<<63 {
		t.Fatal("session id")
	}

	video.SetSimulcast(Simulcast{Recv: [][]SimulcastRID{{{ID: "f"}}, {{ID: "h", Paused: true}}}})
	video.AddRID(RID{"f", RIDRecv, ""})
	video.AddSSRCGroup(SSRCGroup{"FID", []uint32{1, 2}})
	if v, _ := video.Attribute("simulcast"); v != "recv f;~h" {
		t.Fatalf("simulcast %q", v)
	}
	if g := video.SSRCGroups(); len(g) != 1 || g[0].String() != "FID 1 2" {
		t.Fatal("ssrc-group")
	}
}

// TestDTLSSCTPForm reads the sctp port of the m= line of the form before
// RFC 8841, "DTLS/SCTP 5000".
func TestDTLSSCTPForm(t *testing.T) {
	sd, err := ParseString("v=0\no=- 1 1 IN IP4 0.0.0.0\ns=-\nt=0 0\nm=application 9 DTLS/SCTP 5000\na=sctpmap:5000 webrtc-datachannel 1024\n")
	if err != nil {
		t.Fatal(err)
	}
	md := sd.Media[0]
	if p, ok := md.SCTPPort(); !ok || p != 5000 || !md.IsDataChannel() {
		t.Fatalf("port %d", p)
	}
	if _, ok := md.MaxMessageSize(); ok {
		t.Fatal("max-message-size")
	}
}

func BenchmarkParse(b *testing.B) {
	in := readTestdata(b)["chrome_offer.sdp"]
	b.SetBytes(int64(len(in)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Parse(in); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshal(b *testing.B) {
	sd := mustParse(b, readTestdata(b)["chrome_offer.sdp"])
	buf := make([]byte, 0, 8192)
	b.ReportAllocs()
	for b.Loop() {
		buf = sd.AppendTo(buf[:0])
	}
}

// TestMissingAndWildcard covers the readers of what a description may
// lack, the attribute values they refuse, and the codec details read from
// a=rtpmap and a=rtcp-fb lines that name a payload type the m= line does
// not, or every payload type with "*".
func TestMissingAndWildcard(t *testing.T) {
	var empty SessionDescription
	if _, ok := empty.Origin(); ok {
		t.Error("origin of an empty description")
	}
	if empty.MediaByMid("0") != nil {
		t.Error("media of an empty description")
	}
	var bare MediaDescription
	if _, ok := bare.MediaName(); ok || bare.Codecs() != nil {
		t.Error("codecs of a section with no m= line")
	}

	md := NewMedia("video", 9, ProtoRTP, "96", "97")
	md.AddAttribute("rtpmap", "96 VP8/90000")
	md.AddAttribute("rtpmap", "100 H264/90000")
	md.AddAttribute("fmtp", "100 x=1")
	md.AddAttribute("rtcp-fb", "* nack")
	md.AddAttribute("rtcp-fb", "x")
	md.AddAttribute("rtcp-fb", "96 goog-remb")
	md.AddAttribute("rtcp-fb", "100 ccm fir")
	codecs := md.Codecs()
	want := []Codec{
		{PayloadType: 96, Name: "VP8", ClockRate: 90000, Feedback: []string{"nack", "goog-remb"}},
		{PayloadType: 97, Feedback: []string{"nack"}},
	}
	if !reflect.DeepEqual(codecs, want) {
		t.Fatalf("codecs %+v", codecs)
	}
	if _, ok := md.Codec(100); ok {
		t.Error("codec of a payload type the m= line does not list")
	}

	sd := New(1, 1)
	sd.AddMedia(md)
	if sd.MediaDirection(md) != SendRecv {
		t.Error("default direction")
	}
	sd.SetDirection(SendOnly)
	if sd.MediaDirection(md) != SendOnly {
		t.Error("session direction not inherited")
	}

	md.SetICEOptions("trickle")
	md.SetICEOptions()
	if _, ok := md.Attribute("ice-options"); ok {
		t.Error("ice-options kept")
	}

	for _, tc := range []struct {
		in    string
		parse func(string) error
	}{
		{"111 opus/48000/x", func(s string) error { _, err := ParseRTPMap(s); return err }},
		{"1 :x", func(s string) error { _, err := ParseSSRC(s); return err }},
		{" ", func(s string) error { _, err := ParseSSRCGroup(s); return err }},
	} {
		if err := tc.parse(tc.in); !errors.Is(err, ErrInvalidAttribute) {
			t.Errorf("%q: %v", tc.in, err)
		}
	}
	// A long value is cut short in the error.
	long := "111 opus/" + strings.Repeat("9", 60)
	_, err := ParseRTPMap(long)
	if !errors.Is(err, ErrInvalidAttribute) || !strings.Contains(err.Error(), strings.Repeat("9", 40)+"...") || strings.Contains(err.Error(), strings.Repeat("9", 41)) {
		t.Errorf("%v", err)
	}
}

// TestAddLineOrder checks where SetLine adds a line a section lacks, and
// that a line of a type RFC 8866 does not name goes before the attributes.
func TestAddLineOrder(t *testing.T) {
	sd := New(1, 1)
	sd.AddAttribute("group", "BUNDLE 0")
	sd.SetLine('i', "info")
	sd.AddLine('y', "1")
	const want = "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\ni=info\r\nt=0 0\r\ny=1\r\na=group:BUNDLE 0\r\n"
	if got := sd.String(); got != want {
		t.Fatalf("session:\n%s", got)
	}
	md := NewMedia("audio", 9, ProtoRTP, "0")
	md.AddAttribute("mid", "0")
	md.AddLine('y', "1")
	var got []string
	for _, l := range md.Lines {
		got = append(got, l.String())
	}
	if !reflect.DeepEqual(got, []string{"m=audio 9 UDP/TLS/RTP/SAVPF 0", "c=IN IP4 0.0.0.0", "y=1", "a=mid:0"}) {
		t.Fatalf("media: %q", got)
	}
}

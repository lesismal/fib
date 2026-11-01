//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package rtc

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/webrtc"
)

// offerer makes offers as a WHIP or WHEP client would.
func (ev *env) offerer(t testing.TB, codecs []webrtc.Codec, setup func(pc *webrtc.PeerConnection)) (*webrtc.PeerConnection, string) {
	t.Helper()
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{Engine: ev.engine, ListenAddr: "127.0.0.1:0", Codecs: codecs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	setup(pc)
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	offer, err := webrtc.Offer(ctx, pc, nil)
	if err != nil {
		t.Fatal(err)
	}
	return pc, offer.SDP
}

// playOffer is the offer of a player of video and audio.
func (ev *env) playOffer(t testing.TB) (*webrtc.PeerConnection, string) {
	return ev.offerer(t, DefaultReceiveCodecs(), func(pc *webrtc.PeerConnection) {
		_, _ = pc.AddTransceiver(webrtc.MediaKindVideo, webrtc.DirectionRecvOnly)
		_, _ = pc.AddTransceiver(webrtc.MediaKindAudio, webrtc.DirectionRecvOnly)
	})
}

// publishOffer is the offer of a publisher of the given codecs.
func (ev *env) publishOffer(t testing.TB, codecs ...webrtc.Codec) (*webrtc.PeerConnection, string) {
	return ev.offerer(t, codecs, func(pc *webrtc.PeerConnection) {
		for i, c := range codecs {
			s, err := pc.AddTrack(webrtc.NewTrackLocal(c, "t"+string(rune('0'+i)), "s"))
			if err != nil {
				t.Fatal(err)
			}
			s.Transceiver().SetDirection(webrtc.DirectionSendOnly)
		}
	})
}

var (
	opusCodec = webrtc.Codec{MimeType: "audio/opus", ClockRate: 48000, Channels: 2, PayloadType: 111}
	vp8Codec  = webrtc.Codec{MimeType: "video/VP8", ClockRate: 90000, PayloadType: 96}
	h264Codec = webrtc.Codec{MimeType: "video/H264", ClockRate: 90000, PayloadType: 102, Fmtp: h264Fmtp("42e01f")}
)

func TestNewServerErrors(t *testing.T) {
	if _, err := NewServer(Config{}); err == nil {
		t.Fatal("no hub accepted")
	}
	if _, err := NewServer(Config{Hub: hub.New(hub.Config{})}); err == nil {
		t.Fatal("no mux accepted")
	}
	if KindWHIP.String() != "whip" || KindWHEP.String() != "whep" || Kind(0).String() != "unknown" {
		t.Fatal("kind names")
	}
}

func TestEndpointErrors(t *testing.T) {
	refused := errors.New("refused")
	ev := newEnv(t, func(c *Config) {
		c.AllowOrigin = "https://example.com"
		c.Authorize = func(r *http.Request, kind Kind, path, token string) error {
			if strings.HasPrefix(path, "secret") && token != "good" {
				return ErrUnauthorized
			}
			return nil
		}
	})
	ev.hub = hubWithHooks(ev, refused)
	_, play := ev.playOffer(t)
	_, pub := ev.publishOffer(t, opusCodec)
	_, vp8 := ev.publishOffer(t, vp8Codec)
	cases := []struct {
		name, method, path, ctype, body string
		header                          map[string]string
		status                          int
		check                           func(*http.Response, string) bool
	}{
		{"unknown", "GET", "/nothing", "", "", nil, 404, nil},
		{"no path", "POST", "/whip/", "application/sdp", pub, nil, 404, nil},
		{"method", "GET", "/whip/x", "", "", nil, 405, func(r *http.Response, _ string) bool { return r.Header.Get("Allow") == allowedEndpointMethods }},
		{"type", "POST", "/whip/x", "text/plain", pub, nil, 415, nil},
		{"empty", "POST", "/whip/x", "application/sdp", "", nil, 400, nil},
		{"garbage", "POST", "/whip/x", "application/sdp", "hello", nil, 400, nil},
		{"bad offer", "POST", "/whip/x", "application/sdp", "v=0\r\no=- 1 1 IN IP4 0.0.0.0\r\ns=-\r\nt=0 0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 0\r\na=sendonly\r\na=rtpmap:0 PCMU/8000\r\n", nil, 400, nil},
		{"not found", "POST", "/whep/none", "application/sdp", play, nil, 404, nil},
		{"no token", "POST", "/whip/secret", "application/sdp", pub, nil, 401, func(r *http.Response, _ string) bool { return r.Header.Get("WWW-Authenticate") != "" }},
		{"bad token", "POST", "/whip/secret", "application/sdp", pub, map[string]string{"Authorization": "Bearer bad"}, 403, nil},
		{"hook refuses", "POST", "/whip/refuse", "application/sdp", pub, nil, 403, nil},
		{"hook refuses play", "POST", "/whep/refuse-play", "application/sdp", play, nil, 403, nil},
		{"no media", "POST", "/whip/vp8", "application/sdp", vp8, nil, 406, nil},
		{"preflight", "OPTIONS", "/whip/x", "", "", map[string]string{"Access-Control-Request-Method": "POST"}, 204, func(r *http.Response, _ string) bool {
			return r.Header.Get("Access-Control-Allow-Methods") == allowedEndpointMethods && r.Header.Get("Accept-Post") == contentTypeSDP &&
				r.Header.Get("Access-Control-Allow-Origin") == "https://example.com" && r.Header.Get("Vary") == "Origin"
		}},
		{"options", "OPTIONS", "/whep/x", "", "", nil, 204, func(r *http.Response, _ string) bool { return r.Header.Get("Access-Control-Allow-Methods") == "" }},
		{"no session", "DELETE", "/rtc/session/nope", "", "", nil, 404, nil},
	}
	for _, c := range cases {
		resp, body := ev.request(t, c.method, c.path, c.ctype, c.body, c.header)
		if resp.StatusCode != c.status || (c.check != nil && !c.check(resp, body)) {
			t.Errorf("%s: %d %v %q", c.name, resp.StatusCode, resp.Header, body)
		}
	}
	// A refused publish leaves no session behind.
	if ev.srv.Len() != 0 {
		t.Fatalf("%d sessions left", ev.srv.Len())
	}
}

// hubWithHooks replaces the env's hub with one whose hooks refuse the
// paths "refuse" and "refuse-play", with a stream at "refuse-play".
func hubWithHooks(ev *env, refused error) *hub.Hub {
	h := hub.New(hub.Config{
		OnPublish: func(path string, _ hub.PublishInfo) error {
			if path == "refuse" {
				return refused
			}
			return nil
		},
		OnSubscribe: func(path string, _ hub.SubscribeInfo) error {
			if path == "refuse-play" {
				return refused
			}
			return nil
		},
	})
	_, _ = h.Publish("refuse-play", []*av.Track{{Codec: av.CodecOpus, SampleRate: 48000, Channels: 2}}, hub.PublishInfo{})
	ev.srv.config.Hub = h
	return h
}

// TestSessionLimits covers 409 for a busy path, 503 past MaxSessions and
// after Close, and 406 for a stream WebRTC cannot carry.
func TestSessionLimits(t *testing.T) {
	ev := newEnv(t, func(c *Config) { c.MaxSessions = 2 })
	if _, err := ev.hub.Publish("aac", []*av.Track{{Codec: av.CodecAAC, Config: []byte{0x12, 0x10}}}, hub.PublishInfo{}); err != nil {
		t.Fatal(err)
	}
	_, play := ev.playOffer(t)
	if resp, body := ev.request(t, "POST", "/whep/aac", "application/sdp", play, nil); resp.StatusCode != 406 {
		t.Fatalf("aac play: %d %s", resp.StatusCode, body)
	}
	_, pub1 := ev.publishOffer(t, opusCodec)
	_, pub2 := ev.publishOffer(t, opusCodec)
	_, pub3 := ev.publishOffer(t, opusCodec)
	resp, body := ev.request(t, "POST", "/whip/a", "application/sdp", pub1, nil)
	if resp.StatusCode != 201 || resp.Header.Get("ETag") == "" || !strings.HasPrefix(resp.Header.Get("Location"), DefaultResourcePrefix) {
		t.Fatalf("publish: %d %v %s", resp.StatusCode, resp.Header, body)
	}
	if resp, _ := ev.request(t, "POST", "/whip/a", "application/sdp", pub2, nil); resp.StatusCode != 409 {
		t.Fatalf("busy: %d", resp.StatusCode)
	}
	if resp, _ := ev.request(t, "POST", "/whip/b", "application/sdp", pub2, nil); resp.StatusCode != 201 {
		t.Fatalf("second: %d", resp.StatusCode)
	}
	if resp, _ := ev.request(t, "POST", "/whip/c", "application/sdp", pub3, nil); resp.StatusCode != 503 {
		t.Fatalf("over the limit: %d", resp.StatusCode)
	}
	ev.hub.Close()
	if resp, _ := ev.request(t, "POST", "/whip/c", "application/sdp", pub3, nil); resp.StatusCode != 503 {
		t.Fatalf("hub closed: %d", resp.StatusCode)
	}
	ev.srv.Close()
	if ev.srv.Len() != 0 {
		t.Fatal("Close left sessions")
	}
	if resp, _ := ev.request(t, "POST", "/whip/c", "application/sdp", pub3, nil); resp.StatusCode != 503 {
		t.Fatalf("closed: %d", resp.StatusCode)
	}
}

// TestResource covers PATCH and DELETE on a session, and the Link headers
// of the answer.
func TestResource(t *testing.T) {
	ev := newEnv(t, func(c *Config) {
		c.LinkICEServers = []webrtc.ICEServer{{URLs: []string{"stun:stun.example.net"}},
			{URLs: []string{"turn:turn.example.net?transport=udp"}, Username: "u", Credential: "p"}}
		c.ConnectTimeout = time.Hour
	})
	pc, pub := ev.publishOffer(t, opusCodec)
	resp, body := ev.request(t, "POST", "/whip/cam", "application/sdp", pub, nil)
	if resp.StatusCode != 201 {
		t.Fatalf("publish: %d %s", resp.StatusCode, body)
	}
	links := resp.Header.Values("Link")
	if len(links) != 2 || links[0] != `<stun:stun.example.net>; rel="ice-server"` ||
		links[1] != `<turn:turn.example.net?transport=udp>; rel="ice-server"; username="u"; credential="p"; credential-type="password"` {
		t.Fatalf("links %q", links)
	}
	loc, etag := resp.Header.Get("Location"), resp.Header.Get("ETag")
	ufrag := between(pc.LocalDescription().SDP, "a=ice-ufrag:", "\r\n")
	frag := "a=ice-ufrag:" + ufrag + "\r\na=ice-pwd:x\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:0\r\n" +
		"a=candidate:1 1 udp 2130706431 127.0.0.1 9999 typ host\r\na=end-of-candidates\r\n"
	const trickle = "application/trickle-ice-sdpfrag"
	cases := []struct {
		name, method, ctype, body string
		header                    map[string]string
		status                    int
	}{
		{"options", "OPTIONS", "", "", map[string]string{"Access-Control-Request-Method": "PATCH"}, 204},
		{"get", "GET", "", "", nil, 405},
		{"type", "PATCH", "text/plain", frag, nil, 415},
		{"restart", "PATCH", trickle, frag, map[string]string{"If-Match": "*"}, 422},
		{"stale", "PATCH", trickle, frag, map[string]string{"If-Match": `"other"`}, 412},
		{"other ufrag", "PATCH", trickle, strings.Replace(frag, ufrag, "zzzz", 1), nil, 422},
		{"malformed", "PATCH", trickle, "garbage", nil, 400},
		{"bad candidate", "PATCH", trickle, "a=mid:0\r\na=candidate:garbage\r\n", nil, 400},
		{"oversized", "PATCH", trickle, strings.Repeat("a=x\r\n", 20000), nil, 400},
		{"trickle", "PATCH", trickle, frag, map[string]string{"If-Match": etag}, 204},
		{"delete", "DELETE", "", "", nil, 200},
		{"gone", "DELETE", "", "", nil, 404},
	}
	for _, c := range cases {
		resp, body := ev.request(t, c.method, loc, c.ctype, c.body, c.header)
		if resp.StatusCode != c.status {
			t.Errorf("%s: %d %q", c.name, resp.StatusCode, body)
		}
	}
	if len(ev.hub.Streams()) != 0 {
		t.Fatal("DELETE left the stream")
	}
	ev.hub.Close()
	if resp, _ := ev.request(t, "POST", "/whip/cam", "application/sdp", pub, nil); resp.StatusCode != 503 {
		t.Fatalf("hub closed: %d", resp.StatusCode)
	}
}

func between(s, from, to string) string {
	_, rest, _ := strings.Cut(s, from)
	v, _, _ := strings.Cut(rest, to)
	return v
}

// TestConnectTimeout closes a session whose peer never connects.
func TestConnectTimeout(t *testing.T) {
	ev := newEnv(t, func(c *Config) { c.ConnectTimeout = 100 * time.Millisecond; c.GatherTimeout = time.Nanosecond })
	_, pub := ev.publishOffer(t, opusCodec)
	if resp, _ := ev.request(t, "POST", "/whip/cam", "application/sdp", pub, nil); resp.StatusCode != 201 {
		t.Fatalf("publish: %d", resp.StatusCode)
	}
	waitFor(t, "the session to time out", func() bool { return ev.srv.Len() == 0 && len(ev.hub.Streams()) == 0 })
}

func waitFor(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestKeyFrameRequests follows a viewer's keyframe request to a WHIP
// publisher of the same server, and to OnKeyFrameRequest for a stream
// published otherwise; and the periodic requests of KeyFrameInterval.
func TestKeyFrameRequests(t *testing.T) {
	var hooked atomic.Int32
	ev := newEnv(t, func(c *Config) {
		c.OnKeyFrameRequest = func(path string) {
			if path == "plain" {
				hooked.Add(1)
			}
		}
		c.PLIInterval = 20 * time.Millisecond
		c.KeyFrameInterval = 50 * time.Millisecond
	})
	m := loadTS(t, "testdata/h264_opus.ts")
	var requests atomic.Int32
	cfg := ev.clientConfig()
	cfg.OnKeyFrameRequest = func() { requests.Add(1) }
	push, err := Push(cfg, ev.base+"/whip/cam", m.tracks)
	if err != nil {
		t.Fatal(err)
	}
	defer push.Close()
	stop := make(chan struct{})
	defer close(stop)
	go feed(m, func(p *av.Packet) { _ = push.WritePacket(p) }, stop)
	// The periodic requests.
	waitFor(t, "periodic keyframe requests", func() bool { return requests.Load() >= 4 })

	col := newCollector()
	pcfg := col.config(ev.clientConfig())
	pcfg.PLIInterval = 10 * time.Millisecond
	pull, err := Pull(pcfg, ev.base+"/whep/cam")
	if err != nil {
		t.Fatal(err)
	}
	defer pull.Close()
	col.wait(t, "video", func(tracks []*av.Track, packets []*av.Packet) bool {
		n, _ := counts(tracks, packets)
		return n[av.CodecH264] > 5
	})
	before := requests.Load()
	waitFor(t, "a forwarded keyframe request", func() bool {
		pull.RequestKeyFrame()
		time.Sleep(20 * time.Millisecond)
		return requests.Load() > before+3
	})

	ev.publish(t, "plain", m)
	col2 := newCollector()
	pull2, err := Pull(col2.config(ev.clientConfig()), ev.base+"/whep/plain")
	if err != nil {
		t.Fatal(err)
	}
	defer pull2.Close()
	col2.wait(t, "video", func(tracks []*av.Track, packets []*av.Packet) bool {
		n, _ := counts(tracks, packets)
		return n[av.CodecH264] > 5
	})
	waitFor(t, "OnKeyFrameRequest", func() bool {
		pull2.RequestKeyFrame()
		time.Sleep(20 * time.Millisecond)
		return hooked.Load() > 0
	})
}

// TestRepublish keeps a viewer through a republish whose times start over,
// with its RTP times continuing.
func TestRepublish(t *testing.T) {
	ev := newEnv(t, func(c *Config) { c.RepublishWait = 5 * time.Second; c.NoGOPCache = true })
	m := loadTS(t, "testdata/h264_opus.ts")
	pub, err := ev.hub.Publish("live", m.tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { feed(m, pub.WritePacket, stop); close(done) }()
	col := newCollector()
	pull, err := Pull(col.config(ev.clientConfig()), ev.base+"/whep/live")
	if err != nil {
		t.Fatal(err)
	}
	defer pull.Close()
	col.wait(t, "video", func(tracks []*av.Track, packets []*av.Packet) bool {
		n, _ := counts(tracks, packets)
		return n[av.CodecH264] > 20
	})
	close(stop)
	<-done
	pub.Close()
	col.mu.Lock()
	n0 := len(col.packets)
	last := col.packets[n0-1].DTS
	col.mu.Unlock()
	pub, err = ev.hub.Publish("live", m.tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	stop = make(chan struct{})
	done = make(chan struct{})
	go func() { feed(m, pub.WritePacket, stop); close(done) }()
	defer func() { close(stop); <-done; pub.Close() }()
	col.wait(t, "video after the republish", func(tracks []*av.Track, packets []*av.Packet) bool {
		return len(packets) > n0+40
	})
	col.mu.Lock()
	defer col.mu.Unlock()
	for _, p := range col.packets[n0:] {
		if p.DTS < last-time.Second {
			t.Fatalf("time went back from %v to %v", last, p.DTS)
		}
	}
	select {
	case err := <-col.closed:
		t.Fatalf("closed: %v", err)
	default:
	}
}

// TestUnpublishEndsViewer ends a WHEP session when its stream goes, and a
// pushing client when its server ends the session.
func TestUnpublishEndsViewer(t *testing.T) {
	ev := newEnv(t, func(c *Config) { c.RepublishWait = -1 })
	m := loadTS(t, "testdata/h264_opus.ts")
	pub, err := ev.hub.Publish("live", m.tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { feed(m, pub.WritePacket, stop); close(done) }()
	col := newCollector()
	pull, err := Pull(col.config(ev.clientConfig()), ev.base+"/whep/live")
	if err != nil {
		t.Fatal(err)
	}
	defer pull.Close()
	col.wait(t, "video", func(tracks []*av.Track, packets []*av.Packet) bool { return len(packets) > 10 })
	close(stop)
	<-done
	pub.Close()
	waitFor(t, "the session to end", func() bool { return ev.srv.Len() == 0 })
	select {
	case <-col.closed:
	case <-time.After(waitTimeout):
		t.Fatal("client not closed")
	}
}

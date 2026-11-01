//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package rtsp

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/aac"
	"github.com/lesismal/fib/media/codec/h264"
	"github.com/lesismal/fib/media/hub"
)

const testTimeout = 10 * time.Second

// run makes an engine with bindings, or only dials when there are none,
// and runs it until the test ends.
func run(t *testing.T, bindings ...fib.Binding) *fib.Engine {
	t.Helper()
	config := fib.DefaultConfig()
	config.Name = "rtsp-" + t.Name()
	config.LogSettings = false
	engine, err := fib.New(config, bindings...)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		<-done
		_ = engine.Close()
	})
	return engine
}

// serve runs a Server with config on 127.0.0.1 with UDP, and returns its
// URL prefix.
func serve(t *testing.T, config ServerConfig) (string, *Server, *fib.Engine) {
	t.Helper()
	if config.UDPRTPAddr == "" {
		config.UDPRTPAddr, config.UDPRTCPAddr = "127.0.0.1:0", "127.0.0.1:0"
	}
	srv := NewServer(config)
	e := run(t, fib.Binding{Addr: "127.0.0.1:0", Handler: srv})
	if err := srv.ListenUDP(e); err != nil {
		t.Fatal(err)
	}
	addrs, err := e.BindingAddrs(0)
	if err != nil {
		t.Fatal(err)
	}
	return "rtsp://" + addrs[0].String() + "/", srv, e
}

func readTestdata(t *testing.T, path string) [][]byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var nalus [][]byte
	for n := range h264.SplitAnnexB(b) {
		nalus = append(nalus, n)
	}
	return nalus
}

// testTracks returns an H.264 Baseline track, which has no B-frames, and
// an AAC track.
func testTracks(t *testing.T) []*av.Track {
	ps := readTestdata(t, "../codec/h264/testdata/h264_base_360.ps")
	cfg, err := h264.NewAVCConfig(ps[:1], ps[1:2])
	if err != nil {
		t.Fatal(err)
	}
	vc, _ := cfg.Append(nil)
	ac := aac.Config{ObjectType: 2, SampleRate: 48000, ChannelConfig: 2}
	asc, _ := ac.Append(nil)
	return []*av.Track{
		{Codec: av.CodecH264, Config: vc, Width: 640, Height: 360},
		{Codec: av.CodecAAC, Config: asc, SampleRate: 48000, Channels: 2},
	}
}

// testPackets returns n video frames at 25 fps, a keyframe every 10, with
// the audio frames between them.
func testPackets(n int) []*av.Packet {
	var pkts []*av.Packet
	audio := time.Duration(0)
	frame := 21333333 * time.Nanosecond
	for i := 0; i < n; i++ {
		t := time.Second + time.Duration(i)*40*time.Millisecond
		key := i%10 == 0
		nal := append([]byte{0x41}, bytes.Repeat([]byte{byte(i)}, 300+i)...)
		if key {
			nal = append([]byte{0x65}, bytes.Repeat([]byte{byte(i)}, 4000)...)
		}
		pkts = append(pkts, &av.Packet{Track: 0, DTS: t, PTS: t, Keyframe: key, Data: h264.AppendAVCC(nil, nal)})
		for ; time.Second+audio < t+40*time.Millisecond; audio += frame {
			at := time.Second + audio
			pkts = append(pkts, &av.Packet{Track: 1, DTS: at, PTS: at, Keyframe: true, Data: append([]byte{byte(len(pkts) >> 8), byte(len(pkts))}, make([]byte, 98)...)})
		}
	}
	return pkts
}

// sink records what a subscription or a client gets.
type sink struct {
	mu     sync.Mutex
	tracks [][]*av.Track
	pkts   []*av.Packet
	closed []error
	cond   chan struct{}
}

func newSink() *sink { return &sink{cond: make(chan struct{}, 1)} }

func (s *sink) poke() {
	select {
	case s.cond <- struct{}{}:
	default:
	}
}

func (s *sink) OnTracks(tracks []*av.Track) {
	s.mu.Lock()
	s.tracks = append(s.tracks, tracks)
	s.mu.Unlock()
	s.poke()
}

func (s *sink) OnPacket(pkt *av.Packet) {
	s.mu.Lock()
	s.pkts = append(s.pkts, pkt)
	s.mu.Unlock()
	s.poke()
}

func (s *sink) OnClose(err error) {
	s.mu.Lock()
	s.closed = append(s.closed, err)
	s.mu.Unlock()
	s.poke()
}

// wait waits until cond holds, or fails the test.
func (s *sink) wait(t *testing.T, what string, cond func(s *sink) bool) {
	t.Helper()
	deadline := time.After(testTimeout)
	for {
		s.mu.Lock()
		ok := cond(s)
		s.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-s.cond:
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			s.mu.Lock()
			defer s.mu.Unlock()
			t.Fatalf("timed out waiting for %s: %d tracks, %d packets, closed %v", what, len(s.tracks), len(s.pkts), s.closed)
		}
	}
}

func (s *sink) config() ClientConfig {
	return ClientConfig{OnTracks: s.OnTracks, OnPacket: s.OnPacket, OnClose: s.OnClose}
}

// publishHub publishes the test stream into h at path, paced by the
// caller.
func publishHub(t *testing.T, h *hub.Hub, path string) *hub.Publisher {
	t.Helper()
	pub, err := h.Publish(path, testTracks(t), hub.PublishInfo{Protocol: "test"})
	if err != nil {
		t.Fatal(err)
	}
	return pub
}

// checkSame checks that got carries packets of want in order from some
// point on, by data, with times shifted by one offset, and that video
// starts with a keyframe.
func checkSame(t *testing.T, want, got []*av.Packet) {
	t.Helper()
	byTrack := func(pkts []*av.Packet, track int) []*av.Packet {
		var out []*av.Packet
		for _, p := range pkts {
			if p.Track == track {
				out = append(out, p)
			}
		}
		return out
	}
	for track := range 2 {
		w, g := byTrack(want, track), byTrack(got, track)
		if len(g) == 0 {
			t.Fatalf("track %d: nothing", track)
		}
		if track == 0 && !g[0].Keyframe {
			t.Fatal("video does not start with a keyframe")
		}
		i := slices.IndexFunc(w, func(p *av.Packet) bool { return bytes.Equal(p.Data, g[0].Data) })
		if i < 0 {
			t.Fatalf("track %d: first packet not sent", track)
		}
		w = w[i:]
		offset := g[0].PTS - w[0].PTS
		for i := range min(len(g), len(w)) {
			if !bytes.Equal(g[i].Data, w[i].Data) {
				t.Fatalf("track %d packet %d differs", track, i)
			}
			if d := g[i].PTS - w[i].PTS - offset; d < -time.Millisecond || d > time.Millisecond {
				t.Fatalf("track %d packet %d pts %v, want %v", track, i, g[i].PTS-offset, w[i].PTS)
			}
		}
	}
}

// TestPublishAndPlay publishes with a Client into a Server, and plays the
// stream back with Clients over TCP and UDP.
func TestPublishAndPlay(t *testing.T) {
	for _, tc := range []struct {
		name          string
		publish, play ClientTransport
	}{
		{"tcp", TransportTCP, TransportTCP},
		{"udp", TransportUDP, TransportUDP},
		{"mixed", TransportTCP, TransportUDP},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := hub.New(hub.Config{})
			base, _, _ := serve(t, ServerConfig{Hub: h})
			ce := run(t)
			ready := make(chan struct{})
			pc := ClientConfig{Engine: ce, Transport: tc.publish, OnReady: func() { close(ready) }}
			pub, err := Publish(pc, base+"live/s", testTracks(t))
			if err != nil {
				t.Fatal(err)
			}
			defer pub.Close()
			select {
			case <-ready:
			case <-time.After(testTimeout):
				t.Fatal("publish not ready")
			}
			if err := pub.WritePacket(&av.Packet{Track: 9}); err != nil {
				t.Fatal(err)
			}
			pkts := testPackets(60)
			// Half before the player comes, from the GOP cache, half after.
			for _, p := range pkts[:len(pkts)/2] {
				if err := pub.WritePacket(p); err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Millisecond)
			}
			s := newSink()
			cfg := s.config()
			cfg.Engine, cfg.Transport = ce, tc.play
			player, err := Play(cfg, base+"live/s")
			if err != nil {
				t.Fatal(err)
			}
			defer player.Close()
			s.wait(t, "tracks", func(s *sink) bool { return len(s.tracks) > 0 })
			for _, p := range pkts[len(pkts)/2:] {
				_ = pub.WritePacket(p)
				time.Sleep(2 * time.Millisecond)
			}
			last := pkts[len(pkts)-1]
			s.wait(t, "packets", func(s *sink) bool {
				return slices.ContainsFunc(s.pkts, func(p *av.Packet) bool { return bytes.Equal(p.Data, last.Data) && p.Track == last.Track })
			})
			s.mu.Lock()
			got := slices.Clone(s.pkts)
			tracks := s.tracks[0]
			s.mu.Unlock()
			if len(tracks) != 2 || tracks[0].Width != 640 || tracks[1].SampleRate != 48000 {
				t.Fatalf("tracks %+v %+v", tracks[0], tracks[1])
			}
			checkSame(t, pkts, got)
			// Over TCP the player starts with the cached GOP; over UDP,
			// which cannot take its burst, at the next keyframe.
			first := got[0]
			cached := slices.ContainsFunc(pkts[:len(pkts)/2], func(p *av.Packet) bool {
				return p.Track == first.Track && bytes.Equal(p.Data, first.Data)
			})
			if cached != (tc.play == TransportTCP) {
				t.Fatalf("player over %v started with a cached packet: %v", tc.play, cached)
			}
			if len(player.Tracks()) != 2 {
				t.Fatal("Tracks")
			}
			player.Close()
			s.wait(t, "close", func(s *sink) bool { return len(s.closed) == 1 })
			if s.closed[0] != ErrClientClosed {
				t.Fatal(s.closed[0])
			}
		})
	}
}

// TestRelay plays a stream into a second hub with PlayToHub, and pushes it
// back with PublishFromHub.
func TestRelay(t *testing.T) {
	h := hub.New(hub.Config{})
	base, _, _ := serve(t, ServerConfig{Hub: h})
	pub := publishHub(t, h, "src")
	defer pub.Close()
	pkts := testPackets(40)
	for _, p := range pkts[:20] {
		pub.WritePacket(p)
	}
	ce := run(t)
	local := hub.New(hub.Config{})
	user := newSink()
	cfg := user.config()
	cfg.Engine = ce
	relay, err := PlayToHub(cfg, base+"src", local, "copy")
	if err != nil {
		t.Fatal(err)
	}
	user.wait(t, "relay tracks", func(s *sink) bool { return len(s.tracks) > 0 })
	s := newSink()
	if _, err := local.Subscribe("copy", s, hub.SubscribeOptions{}); err != nil {
		t.Fatal(err)
	}
	// Push the copy back to the server under another path.
	ready := make(chan struct{})
	pushed := make(chan error, 1)
	pusher, err := PublishFromHub(ClientConfig{Engine: ce, OnReady: func() { close(ready) }, OnClose: func(err error) { pushed <- err }},
		base+"back", local, "copy", hub.SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-ready:
	case <-time.After(testTimeout):
		t.Fatal("push not ready")
	}
	if pusher.Client() == nil {
		t.Fatal("no client")
	}
	back := newSink()
	waitPublished(t, h, "back")
	if _, err := h.Subscribe("back", back, hub.SubscribeOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, p := range pkts[20:] {
		pub.WritePacket(p)
		time.Sleep(2 * time.Millisecond)
	}
	s.wait(t, "relayed packets", func(s *sink) bool { return len(s.pkts) >= 40 })
	back.wait(t, "pushed packets", func(s *sink) bool { return len(s.pkts) >= 20 })
	checkSame(t, pkts, s.pkts)
	// The source ends: the relay ends, and so does the push.
	pub.Close()
	user.wait(t, "relay close", func(s *sink) bool { return len(s.closed) > 0 })
	select {
	case <-pushed:
	case <-time.After(testTimeout):
		t.Fatal("push not closed")
	}
	relay.Close()
	pusher.Close()
}

func waitPublished(t *testing.T, h *hub.Hub, path string) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		for _, st := range h.Streams() {
			if st.Path == path && st.Publishing {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s not published", path)
}

func TestAuthClient(t *testing.T) {
	for _, methods := range []AuthMethod{AuthDigest, AuthBasic} {
		h := hub.New(hub.Config{})
		base, _, _ := serve(t, ServerConfig{Hub: h, AuthMethods: methods,
			Password: func(path, user string) (string, bool) { return "pw", user == "u" && path == "live/s" }})
		pub := publishHub(t, h, "live/s")
		for _, p := range testPackets(12) {
			pub.WritePacket(p)
		}
		ce := run(t)
		s := newSink()
		cfg := s.config()
		cfg.Engine = ce
		c, err := Play(cfg, strings.Replace(base, "rtsp://", "rtsp://u:pw@", 1)+"live/s")
		if err != nil {
			t.Fatal(err)
		}
		s.wait(t, "packets", func(s *sink) bool { return len(s.pkts) > 5 })
		c.Close()
		// Wrong credentials and none fail with 401.
		for _, u := range []string{"rtsp://u:bad@", "rtsp://"} {
			s := newSink()
			cfg := s.config()
			cfg.Engine = ce
			if _, err := Play(cfg, strings.Replace(base, "rtsp://", u, 1)+"live/s"); err != nil {
				t.Fatal(err)
			}
			s.wait(t, "refusal", func(s *sink) bool { return len(s.closed) > 0 })
			var se *StatusError
			if !errors.As(s.closed[0], &se) || se.Code != StatusUnauthorized || !strings.Contains(se.Error(), "401") {
				t.Fatalf("%s: %v", u, s.closed[0])
			}
		}
		pub.Close()
	}
}

func TestClientErrors(t *testing.T) {
	ce := run(t)
	if _, err := Play(ClientConfig{}, "rtsp://h/s"); err == nil {
		t.Fatal("no engine")
	}
	for _, u := range []string{"http://h/s", "rtsp:///s", "rtsp://h/%zz"} {
		if _, err := Play(ClientConfig{Engine: ce}, u); err == nil {
			t.Errorf("%s taken", u)
		}
	}
	if _, err := Publish(ClientConfig{Engine: ce}, "rtsp://127.0.0.1:1/s", []*av.Track{nil, {Codec: av.CodecAV1}}); err != ErrNoTracks {
		t.Fatal(err)
	}
	if _, err := Play(ClientConfig{Engine: ce}, "rtsp://[::1:1/s"); err == nil {
		t.Fatal("bad host taken")
	}

	// A server that never answers.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var conns []net.Conn
	var connsMu sync.Mutex
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			connsMu.Lock()
			conns = append(conns, c)
			connsMu.Unlock()
		}
	}()
	defer func() {
		connsMu.Lock()
		for _, c := range conns {
			c.Close()
		}
		connsMu.Unlock()
	}()
	s := newSink()
	cfg := s.config()
	cfg.Engine, cfg.RequestTimeout = ce, 100*time.Millisecond
	if _, err := Play(cfg, "rtsp://"+ln.Addr().String()+"/s"); err != nil {
		t.Fatal(err)
	}
	s.wait(t, "timeout", func(s *sink) bool { return len(s.closed) > 0 })
	if s.closed[0] != ErrTimeout {
		t.Fatal(s.closed[0])
	}
	// A refused dial, and a close before the dial ends.
	s = newSink()
	cfg = s.config()
	cfg.Engine = ce
	c, err := Play(cfg, "rtsp://127.0.0.1:1/s")
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	c.Close()
	s.wait(t, "refused", func(s *sink) bool { return len(s.closed) > 0 })
	if c.WritePacket(&av.Packet{}) != ErrClientClosed {
		t.Fatal("write after close")
	}

	// A stream that is not there, and one with nothing to play.
	h := hub.New(hub.Config{})
	base, _, _ := serve(t, ServerConfig{Hub: h})
	s = newSink()
	cfg = s.config()
	cfg.Engine = ce
	if _, err := Play(cfg, base+"missing"); err != nil {
		t.Fatal(err)
	}
	s.wait(t, "404", func(s *sink) bool { return len(s.closed) > 0 })
	var se *StatusError
	if !errors.As(s.closed[0], &se) || se.Code != StatusNotFound {
		t.Fatal(s.closed[0])
	}
	pub, _ := h.Publish("av1", []*av.Track{{Codec: av.CodecAV1}}, hub.PublishInfo{})
	defer pub.Close()
	s = newSink()
	cfg = s.config()
	cfg.Engine = ce
	if _, err := Play(cfg, base+"av1"); err != nil {
		t.Fatal(err)
	}
	s.wait(t, "415", func(s *sink) bool { return len(s.closed) > 0 })
	if !errors.As(s.closed[0], &se) || se.Code != StatusUnsupportedMediaType {
		t.Fatal(s.closed[0])
	}
	// A played stream that sends nothing times out.
	quiet := publishHub(t, h, "quiet")
	defer quiet.Close()
	s = newSink()
	cfg = s.config()
	cfg.Engine, cfg.ReadTimeout, cfg.KeepAlive = ce, 300*time.Millisecond, 100*time.Millisecond
	if _, err := Play(cfg, base+"quiet"); err != nil {
		t.Fatal(err)
	}
	s.wait(t, "read timeout", func(s *sink) bool { return len(s.closed) > 0 })
	if s.closed[0] != ErrTimeout {
		t.Fatal(s.closed[0])
	}
	// WritePacket before the publish is ready.
	p, err := Publish(ClientConfig{Engine: ce}, base+"late", testTracks(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.WritePacket(&av.Packet{}); err != ErrNotReady {
		t.Fatal(err)
	}
	if p.Backlog() != 0 {
		t.Fatal("backlog")
	}
	p.Close()
}

// rawClient speaks RTSP over a plain socket, for what the Client never
// sends.
type rawClient struct {
	t       *testing.T
	c       net.Conn
	p       Parser
	msgs    []any
	cseq    int
	session string
}

func dialRaw(t *testing.T, base string) *rawClient {
	t.Helper()
	host := strings.TrimSuffix(strings.TrimPrefix(base, "rtsp://"), "/")
	c, err := net.Dial("tcp", host)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &rawClient{t: t, c: c}
}

func (r *rawClient) send(raw string) { _, _ = r.c.Write([]byte(raw)) }

// next returns the next message, skipping interleaved frames unless
// frames is set.
func (r *rawClient) next(frames bool) any {
	r.t.Helper()
	buf := make([]byte, 64<<10)
	for {
		for len(r.msgs) > 0 {
			m := r.msgs[0]
			r.msgs = r.msgs[1:]
			if _, ok := m.(*Interleaved); ok && !frames {
				continue
			}
			return m
		}
		_ = r.c.SetReadDeadline(time.Now().Add(testTimeout))
		n, err := r.c.Read(buf)
		if err != nil {
			return err
		}
		if err := r.p.Feed(buf[:n], func(m any) error {
			switch m := m.(type) {
			case *Response:
				c := *m
				c.Body = bytes.Clone(m.Body)
				r.msgs = append(r.msgs, &c)
			case *Interleaved:
				r.msgs = append(r.msgs, &Interleaved{m.Channel, bytes.Clone(m.Data)})
			case *Request:
				r.msgs = append(r.msgs, m)
			}
			return nil
		}); err != nil {
			return err
		}
	}
}

func (r *rawClient) do(method, url string, header ...string) *Response {
	r.t.Helper()
	r.cseq++
	req := &Request{Method: method, URL: url, Header: Header{{"CSeq", strconv.Itoa(r.cseq)}}}
	if r.session != "" {
		req.Header.Add("Session", r.session)
	}
	var body []byte
	for _, h := range header {
		k, v, _ := strings.Cut(h, ": ")
		if k == "Body" {
			body = []byte(v)
			continue
		}
		req.Header.Set(k, v)
	}
	req.Body = body
	_, _ = r.c.Write(req.Append(nil))
	for {
		m := r.next(false)
		switch m := m.(type) {
		case *Response:
			if m.CSeq() != r.cseq {
				continue
			}
			if v := m.Header.Get("Session"); v != "" {
				r.session = ParseSession(v).ID
			}
			return m
		case error:
			r.t.Fatalf("%s: %v", method, m)
		}
	}
}

func expect(t *testing.T, resp *Response, code int) {
	t.Helper()
	if resp.StatusCode != code {
		t.Fatalf("status %d %s, want %d: %s", resp.StatusCode, resp.Reason, code, resp.Header)
	}
}

const testSDP = "v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=x\r\nt=0 0\r\n" +
	"m=video 0 RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\na=fmtp:96 packetization-mode=1;sprop-parameter-sets=Z0LAHtkAoC/5cBEAAAMAAQAAAwAyDxYuSA==,aMuDyyA=\r\na=control:streamid=0\r\n" +
	"m=audio 0 RTP/AVP 0\r\na=control:streamid=1\r\n"

func TestServerRequests(t *testing.T) {
	h := hub.New(hub.Config{OnSubscribe: func(path string, info hub.SubscribeInfo) error {
		if path == "forbidden" {
			return errors.New("no")
		}
		return nil
	}})
	var hooked []string
	base, _, _ := serve(t, ServerConfig{Hub: h, OnRequest: func(c *ServerConn, req *Request) *Response {
		hooked = append(hooked, req.Method)
		if c.Conn() == nil || c.User() != "" {
			t.Error("conn")
		}
		if strings.HasSuffix(req.URL, "/hooked") {
			return &Response{StatusCode: StatusMovedPermanently}
		}
		return nil
	}})
	pub := publishHub(t, h, "live/s")
	defer pub.Close()
	for _, p := range testPackets(12) {
		pub.WritePacket(p)
	}
	single, _ := h.Publish("single", testTracks(t)[1:], hub.PublishInfo{})
	defer single.Close()
	forbidden, _ := h.Publish("forbidden", testTracks(t), hub.PublishInfo{})
	defer forbidden.Close()

	r := dialRaw(t, base)
	resp := r.do(MethodOptions, "*")
	expect(t, resp, StatusOK)
	if !strings.Contains(resp.Header.Get("Public"), "RECORD") || resp.Header.Get("Server") != "fib" {
		t.Fatal(resp.Header)
	}
	expect(t, r.do(MethodDescribe, base+"hooked"), StatusMovedPermanently)
	expect(t, r.do(MethodDescribe, base+"missing"), StatusNotFound)
	expect(t, r.do(MethodDescribe, base+"forbidden"), StatusForbidden)
	expect(t, r.do(MethodDescribe, "http://x/y"), StatusBadRequest)
	expect(t, r.do("FOO", base+"live/s"), StatusNotImplemented)
	expect(t, r.do(MethodPlay, base+"live/s"), StatusSessionNotFound)
	expect(t, r.do(MethodGetParameter, base+"live/s"), StatusOK)
	resp = r.do(MethodDescribe, base+"live/s")
	expect(t, resp, StatusOK)
	if resp.Header.Get("Content-Base") != base+"live/s/" || !bytes.Contains(resp.Body, []byte("trackID=1")) {
		t.Fatalf("%s %s", resp.Header, resp.Body)
	}
	// Transports the server cannot do, and an aggregate SETUP.
	expect(t, r.do(MethodSetup, base+"live/s/trackID=0", "Transport: RTP/AVP;multicast"), StatusUnsupportedTransport)
	expect(t, r.do(MethodSetup, base+"live/s/trackID=0", "Transport: RTP/AVP;unicast"), StatusUnsupportedTransport)
	expect(t, r.do(MethodSetup, base+"live/s", "Transport: RTP/AVP/TCP;unicast"), StatusAggregateNotAllowed)
	expect(t, r.do(MethodSetup, base+"live/s/trackID=7", "Transport: RTP/AVP/TCP;unicast"), StatusNotFound)
	resp = r.do(MethodSetup, base+"live/s/trackID=0", "Transport: RTP/AVP/TCP;unicast;interleaved=0-1")
	expect(t, resp, StatusOK)
	ts := ParseTransports(resp.Header.Get("Transport"))
	if len(ts) != 1 || ts[0].Interleaved != [2]int{0, 1} || !ts[0].HasSSRC {
		t.Fatal(resp.Header)
	}
	expect(t, r.do(MethodSetup, base+"live/s/trackID=0", "Transport: RTP/AVP/TCP;unicast"), StatusMethodNotValidInState)
	// A channel taken is moved.
	resp = r.do(MethodSetup, base+"live/s/trackID=1", "Transport: RTP/AVP/TCP;unicast;interleaved=1-2")
	expect(t, resp, StatusOK)
	if ts := ParseTransports(resp.Header.Get("Transport")); ts[0].Interleaved != [2]int{2, 3} {
		t.Fatal(resp.Header)
	}
	expect(t, r.do(MethodSetup, base+"single", "Transport: RTP/AVP/TCP;unicast"), StatusAggregateNotAllowed)
	expect(t, r.do(MethodRecord, base+"live/s"), StatusMethodNotValidInState)
	expect(t, r.do(MethodPause, base+"live/s"), StatusMethodNotValidInState)
	resp = r.do(MethodPlay, base+"live/s", "Range: npt=0-")
	expect(t, resp, StatusOK)
	infos := ParseRTPInfo(resp.Header.Get("RTP-Info"))
	if len(infos) != 2 || !infos[0].HasSeq || !strings.HasSuffix(infos[1].URL, "trackID=1") {
		t.Fatal(resp.Header)
	}
	// The GOP cache comes first, with a sender report for each stream.
	sawSR := map[uint8]bool{}
	for len(sawSR) < 2 {
		switch m := r.next(true).(type) {
		case *Interleaved:
			if m.Channel%2 == 1 {
				sawSR[m.Channel] = true
			}
		case error:
			t.Fatal(m)
		}
	}
	expect(t, r.do(MethodPlay, base+"live/s"), StatusOK)
	expect(t, r.do(MethodSetParameter, base+"live/s"), StatusOK)
	r.session = "other"
	expect(t, r.do(MethodPause, base+"live/s"), StatusSessionNotFound)
	r.session = ""
	expect(t, r.do(MethodPause, base+"live/s"), StatusOK)
	expect(t, r.do(MethodPause, base+"live/s"), StatusOK)
	resp = r.do(MethodPlay, base+"live/s")
	expect(t, resp, StatusOK)
	expect(t, r.do(MethodSetup, base+"other/trackID=0", "Transport: RTP/AVP/TCP;unicast"), StatusAggregateNotAllowed)
	expect(t, r.do(MethodTeardown, base+"live/s"), StatusOK)
	expect(t, r.do(MethodPlay, base+"live/s"), StatusSessionNotFound)
	// SETUP without DESCRIBE, of a stream with one track by its own URL.
	r2 := dialRaw(t, base)
	expect(t, r2.do(MethodSetup, base+"single", "Transport: RTP/AVP/TCP;unicast"), StatusOK)
	expect(t, r2.do(MethodSetup, base+"missing/trackID=0", "Transport: RTP/AVP/TCP;unicast"), StatusAggregateNotAllowed)
	r3 := dialRaw(t, base)
	expect(t, r3.do(MethodSetup, base+"missing/trackID=0", "Transport: RTP/AVP/TCP;unicast"), StatusNotFound)
	expect(t, r3.do(MethodSetup, base+"live/s/trackID=0", "Transport: RTP/AVP/TCP;unicast"), StatusOK)
	// The stream is gone before PLAY: the connection closes.
	gone, _ := h.Publish("gone", testTracks(t), hub.PublishInfo{})
	r4 := dialRaw(t, base)
	expect(t, r4.do(MethodSetup, base+"gone/trackID=0", "Transport: RTP/AVP/TCP;unicast"), StatusOK)
	gone.Close()
	r4.cseq++
	r4.send("PLAY " + base + "gone RTSP/1.0\r\nCSeq: " + strconv.Itoa(r4.cseq) + "\r\n\r\n")
	for {
		if _, ok := r4.next(false).(error); ok {
			break
		}
	}
	if len(hooked) == 0 {
		t.Fatal("hook not called")
	}
}

func TestServerRecordRaw(t *testing.T) {
	h := hub.New(hub.Config{})
	base, srv, _ := serve(t, ServerConfig{Hub: h})
	if rtpPort, rtcpPort := srv.UDPPorts(); rtpPort == 0 || rtcpPort == 0 {
		t.Fatal("no UDP ports")
	}
	r := dialRaw(t, base)
	expect(t, r.do(MethodAnnounce, base+"rec", "Content-Type: text/plain", "Body: x"), StatusUnsupportedMediaType)
	expect(t, r.do(MethodAnnounce, base+"rec", "Content-Type: application/sdp", "Body: garbage"), StatusBadRequest)
	expect(t, r.do(MethodAnnounce, base+"rec", "Content-Type: application/sdp", "Body: v=0\r\no=- 0 0 IN IP4 0.0.0.0\r\ns=x\r\nt=0 0\r\nm=video 0 RTP/AVP 96\r\na=rtpmap:96 VP8/90000\r\n"), StatusUnsupportedMediaType)
	expect(t, r.do(MethodAnnounce, base+"rec", "Content-Type: application/sdp", "Body: "+testSDP), StatusOK)
	expect(t, r.do(MethodRecord, base+"rec"), StatusMethodNotValidInState)
	expect(t, r.do(MethodSetup, base+"rec/streamid=1", "Transport: RTP/AVP/TCP;unicast;mode=play"), StatusUnsupportedTransport)
	expect(t, r.do(MethodSetup, base+"rec/streamid=1", "Transport: RTP/AVP/TCP;unicast;interleaved=2-3;mode=record"), StatusOK)
	// No control matches: the next stream is taken.
	expect(t, r.do(MethodSetup, base+"rec/whatever", "Transport: RTP/AVP/TCP;unicast;interleaved=0-1;mode=record"), StatusOK)
	expect(t, r.do(MethodSetup, base+"rec/more", "Transport: RTP/AVP/TCP;unicast;mode=record"), StatusNotFound)
	expect(t, r.do(MethodRecord, base+"rec"), StatusOK)
	expect(t, r.do(MethodAnnounce, base+"rec", "Content-Type: application/sdp", "Body: "+testSDP), StatusMethodNotValidInState)
	expect(t, r.do(MethodSetup, base+"rec/streamid=0", "Transport: RTP/AVP/TCP;unicast;mode=record"), StatusMethodNotValidInState)
	s := newSink()
	if _, err := h.Subscribe("rec", s, hub.SubscribeOptions{}); err != nil {
		t.Fatal(err)
	}
	// A G.711 frame on channel 2, RTCP on 3, nothing on 9.
	var frames []byte
	frames = AppendInterleaved(frames, 2, []byte{0x80, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 7, 7, 7})
	frames = AppendInterleaved(frames, 3, []byte{0x80, 200, 0, 0})
	frames = AppendInterleaved(frames, 9, []byte{1})
	r.send(string(frames))
	s.wait(t, "recorded", func(s *sink) bool { return len(s.pkts) == 1 })
	if s.pkts[0].Track != 1 || !bytes.Equal(s.pkts[0].Data, []byte{7, 7, 7}) {
		t.Fatalf("%+v", s.pkts[0])
	}
	// A second publisher of the path is refused.
	r2 := dialRaw(t, base)
	expect(t, r2.do(MethodAnnounce, base+"rec", "Content-Type: application/sdp", "Body: "+testSDP), StatusOK)
	expect(t, r2.do(MethodSetup, base+"rec/streamid=0", "Transport: RTP/AVP/TCP;unicast;mode=record"), StatusOK)
	expect(t, r2.do(MethodRecord, base+"rec"), StatusForbidden)
	// TEARDOWN unpublishes.
	expect(t, r.do(MethodTeardown, base+"rec"), StatusOK)
	s.wait(t, "unpublished", func(s *sink) bool { return len(s.closed) == 1 })
	// A malformed request closes the connection with a 400.
	r.send("NOT RTSP\r\n\r\n")
	if resp, ok := r.next(false).(*Response); !ok || resp.StatusCode != StatusBadRequest {
		t.Fatal(resp)
	}
}

func TestServerRecordUDP(t *testing.T) {
	h := hub.New(hub.Config{})
	base, srv, _ := serve(t, ServerConfig{Hub: h, ReorderDelay: 20 * time.Millisecond})
	rtpPort, _ := srv.UDPPorts()
	uc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer uc.Close()
	nat, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer nat.Close()
	port := uc.LocalAddr().(*net.UDPAddr).Port
	r := dialRaw(t, base)
	expect(t, r.do(MethodAnnounce, base+"udp", "Content-Type: application/sdp", "Body: "+testSDP), StatusOK)
	expect(t, r.do(MethodSetup, base+"udp/streamid=0", "Transport: RTP/AVP;unicast;client_port=1-2;mode=record"), StatusOK)
	resp := r.do(MethodSetup, base+"udp/streamid=1", "Transport: RTP/AVP;unicast;client_port="+strconv.Itoa(port)+";ssrc=000000AA;mode=record")
	expect(t, resp, StatusOK)
	ts := ParseTransports(resp.Header.Get("Transport"))
	if ts[0].ServerPort[0] != rtpPort || ts[0].ClientPort != [2]int{port, port + 1} {
		t.Fatal(resp.Header)
	}
	expect(t, r.do(MethodRecord, base+"udp"), StatusOK)
	s := newSink()
	if _, err := h.Subscribe("udp", s, hub.SubscribeOptions{}); err != nil {
		t.Fatal(err)
	}
	server := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: rtpPort}
	pkt := func(seq uint16, payload byte) []byte {
		return []byte{0x80, 0, byte(seq >> 8), byte(seq), 0, 0, byte(seq >> 8), byte(seq), 0, 0, 0, 0xaa, payload}
	}
	// Out of order, a duplicate, and a gap the reorder buffer gives up on.
	for _, seq := range []uint16{1, 3, 2, 2, 5} {
		_, _ = uc.WriteToUDP(pkt(seq, byte(seq)), server)
	}
	// From another port of the same IP: found by SSRC. Datagrams of two
	// sockets may be read in either order, so this one waits for the rest:
	// were it first, the others would be late.
	s.wait(t, "udp packets", func(s *sink) bool { return len(s.pkts) == 4 })
	_, _ = nat.WriteToUDP(pkt(6, 6), server)
	// Unknown sources are dropped.
	other, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	_, _ = other.WriteToUDP([]byte{0x80, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 0xbb, 1}, server)
	_, _ = other.WriteToUDP([]byte{1}, server)
	s.wait(t, "udp packets", func(s *sink) bool { return len(s.pkts) == 5 })
	for i, want := range []byte{1, 2, 3, 5, 6} {
		if s.pkts[i].Data[0] != want {
			t.Fatalf("packet %d: %v", i, s.pkts[i].Data)
		}
	}
	// RTCP from the client's RTCP port keeps the session.
	_, _ = uc.WriteToUDP([]byte{0x80, 201, 0, 1, 0, 0, 0, 1}, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: rtpPort + 0})
	if err := srv.ListenUDP(nil); err == nil {
		t.Fatal("ListenUDP twice")
	}
}

func TestServerTimeouts(t *testing.T) {
	h := hub.New(hub.Config{})
	base, _, _ := serve(t, ServerConfig{Hub: h, IdleTimeout: 100 * time.Millisecond, SessionTimeout: 300 * time.Millisecond,
		SenderReportInterval: 50 * time.Millisecond})
	pub := publishHub(t, h, "s")
	defer pub.Close()
	// Idle without a session.
	r := dialRaw(t, base)
	start := time.Now()
	if _, ok := r.next(false).(error); !ok || time.Since(start) > 5*time.Second {
		t.Fatal("idle connection not closed")
	}
	// A playing session that keeps up with GET_PARAMETER lives on, gets
	// sender reports, and ends once it stops.
	r = dialRaw(t, base)
	expect(t, r.do(MethodSetup, base+"s/trackID=1", "Transport: RTP/AVP/TCP;unicast"), StatusOK)
	expect(t, r.do(MethodPlay, base+"s"), StatusOK)
	go func() {
		for i := range 20 {
			pub.WritePacket(&av.Packet{Track: 1, DTS: time.Duration(i) * 20 * time.Millisecond, Keyframe: true, Data: []byte{1, 2}})
			time.Sleep(20 * time.Millisecond)
		}
	}()
	srs := 0
	for until := time.Now().Add(500 * time.Millisecond); time.Now().Before(until); {
		r.cseq++
		r.send("GET_PARAMETER " + base + "s RTSP/1.0\r\nCSeq: " + strconv.Itoa(r.cseq) + "\r\nSession: " + r.session + "\r\n\r\n")
		for deadline := time.Now().Add(100 * time.Millisecond); time.Now().Before(deadline); {
			_ = r.c.SetReadDeadline(deadline)
			if m, ok := r.next(true).(*Interleaved); ok && m.Channel == 1 {
				srs++
			}
		}
	}
	if srs < 2 {
		t.Fatalf("%d sender reports", srs)
	}
	for {
		if _, ok := r.next(true).(error); ok {
			break
		}
	}
}

func TestSlowPlayer(t *testing.T) {
	h := hub.New(hub.Config{})
	base, _, _ := serve(t, ServerConfig{Hub: h, MaxQueuedBytes: 64 << 10})
	pub := publishHub(t, h, "s")
	defer pub.Close()
	r := dialRaw(t, base)
	if tc, ok := r.c.(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(4096)
	}
	expect(t, r.do(MethodSetup, base+"s/trackID=0", "Transport: RTP/AVP/TCP;unicast"), StatusOK)
	expect(t, r.do(MethodPlay, base+"s"), StatusOK)
	// The player reads nothing while a few megabytes go by: they are
	// dropped, not queued.
	big := testPackets(200)
	for i, p := range big {
		if p.Track == 0 {
			p.Data = h264.AppendAVCC(nil, append([]byte{byte(0x41 + 0x24*b2i(p.Keyframe))}, make([]byte, 30000)...))
		}
		pub.WritePacket(p)
		if i%50 == 0 {
			time.Sleep(time.Millisecond)
		}
	}
	n := 0
	buf := make([]byte, 1<<20)
	_ = r.c.SetReadDeadline(time.Now().Add(time.Second))
	for {
		k, err := r.c.Read(buf)
		n += k
		if err != nil {
			break
		}
	}
	if n > 8<<20 {
		t.Fatalf("player got %d bytes, more than the bound", n)
	}
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func TestServerAuthRaw(t *testing.T) {
	h := hub.New(hub.Config{})
	base, _, _ := serve(t, ServerConfig{Hub: h, Password: func(path, user string) (string, bool) { return "pw", user == "u" }})
	r := dialRaw(t, base)
	expect(t, r.do(MethodOptions, base+"s"), StatusOK)
	resp := r.do(MethodDescribe, base+"s")
	expect(t, resp, StatusUnauthorized)
	a, ok := newClientAuth("u", "pw", resp.Header.Values("WWW-Authenticate"))
	if !ok {
		t.Fatal(resp.Header)
	}
	expect(t, r.do(MethodDescribe, base+"s", "Authorization: "+a.authorization(MethodDescribe, base+"s")), StatusNotFound)
	expect(t, r.do(MethodSetup, base+"s/trackID=0", "Authorization: "+a.authorization(MethodSetup, base+"s/trackID=0")), StatusNotFound)
}

func TestServerShutdown(t *testing.T) {
	srv := NewServer(ServerConfig{})
	e := run(t, fib.Binding{Addr: "127.0.0.1:0", Handler: srv})
	addrs, _ := e.BindingAddrs(0)
	base := "rtsp://" + addrs[0].String() + "/"
	r := dialRaw(t, base)
	expect(t, r.do(MethodDescribe, base+"s"), StatusServiceUnavailable)
	expect(t, r.do(MethodAnnounce, base+"s", "Body: "+testSDP), StatusServiceUnavailable)
	expect(t, r.do(MethodSetup, base+"s/trackID=0", "Transport: RTP/AVP/TCP"), StatusServiceUnavailable)
	if p, q := srv.UDPPorts(); p != 0 || q != 0 {
		t.Fatal("UDP ports without ListenUDP")
	}
	// Shutdown closes the connection.
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	go func() { _ = e.Shutdown(ctx) }()
	for {
		if _, ok := r.next(true).(error); ok {
			break
		}
	}
}

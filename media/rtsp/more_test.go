//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package rtsp

import (
	"bytes"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/codec/h264"
	"github.com/lesismal/fib/media/codec/h265"
	"github.com/lesismal/fib/media/hub"
)

// fakeServer answers a client's requests with what answer returns for
// each, raw bytes written as they are, or closes the connection for nil.
func fakeServer(t *testing.T, answer func(req *Request) []byte) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				var p Parser
				buf := make([]byte, 64<<10)
				for {
					n, err := c.Read(buf)
					if err != nil {
						return
					}
					stop := false
					_ = p.Feed(buf[:n], func(m any) error {
						if req, ok := m.(*Request); ok {
							b := answer(req)
							if b == nil {
								stop = true
								return errors.New("stop")
							}
							_, _ = c.Write(b)
						}
						return nil
					})
					if stop {
						return
					}
				}
			}()
		}
	}()
	return "rtsp://" + ln.Addr().String() + "/s"
}

func reply(req *Request, code int, h Header, body string) []byte {
	resp := &Response{StatusCode: code, Header: append(Header{{"CSeq", req.Header.Get("CSeq")}}, h...), Body: []byte(body)}
	return resp.Append(nil)
}

const fakeSDP = "v=0\r\no=- 0 0 IN IP4 127.0.0.1\r\ns=x\r\nt=0 0\r\na=control:rtsp://127.0.0.1/abs/\r\n" +
	"m=video 0 RTP/AVP 96\r\na=rtpmap:96 VP8/90000\r\n" +
	"m=audio 0 RTP/AVP 0\r\na=control:trackID=1\r\n"

func TestClientAgainstFakeServers(t *testing.T) {
	ce := run(t)
	play := func(t *testing.T, url string, transport ClientTransport) error {
		t.Helper()
		s := newSink()
		cfg := s.config()
		cfg.Engine, cfg.Transport = ce, transport
		if _, err := Play(cfg, url); err != nil {
			t.Fatal(err)
		}
		s.wait(t, "close", func(s *sink) bool { return len(s.closed) > 0 })
		return s.closed[0]
	}
	describeWith := func(sdp string, setup func(req *Request) []byte) func(req *Request) []byte {
		return func(req *Request) []byte {
			switch req.Method {
			case MethodOptions:
				return reply(req, 200, Header{{"Public", "DESCRIBE, SETUP, PLAY"}}, "")
			case MethodDescribe:
				return append(
					// A request of the server's comes first; the client
					// answers it.
					(&Request{Method: MethodSetParameter, URL: "*", Header: Header{{"CSeq", "1"}}}).Append(nil),
					reply(req, 200, Header{{"Content-Location", "rtsp://127.0.0.1/loc/"}}, sdp)...)
			case MethodSetup:
				return setup(req)
			}
			return reply(req, 200, nil, "")
		}
	}
	for _, tc := range []struct {
		name      string
		transport ClientTransport
		answer    func(req *Request) []byte
		want      error
	}{
		{"bad sdp", TransportTCP, describeWith("garbage", nil), nil},
		{"no transport", TransportTCP, describeWith(fakeSDP, func(req *Request) []byte {
			if !strings.HasPrefix(req.URL, "rtsp://127.0.0.1/abs/trackID=1") {
				return reply(req, 400, nil, "")
			}
			return reply(req, 200, nil, "")
		}), ErrUnexpected},
		{"udp for tcp", TransportTCP, describeWith(fakeSDP, func(req *Request) []byte {
			return reply(req, 200, Header{{"Transport", "RTP/AVP;unicast;client_port=1-2;server_port=3-4"}}, "")
		}), ErrUnexpected},
		{"tcp for udp", TransportUDP, describeWith(fakeSDP, func(req *Request) []byte {
			return reply(req, 200, Header{{"Transport", "RTP/AVP/TCP;interleaved=0-1"}}, "")
		}), ErrUnexpected},
		{"no tracks", TransportTCP, describeWith(strings.Split(fakeSDP, "m=audio")[0], nil), ErrNoTracks},
		{"garbage response", TransportTCP, func(req *Request) []byte { return []byte("HTTP/1.1 200 OK\r\n\r\n") }, ErrMalformed},
		{"closed", TransportTCP, func(req *Request) []byte { return nil }, nil},
		{"always stale", TransportTCP, func(req *Request) []byte {
			return reply(req, 401, Header{{"WWW-Authenticate", `Digest realm="r", nonce="n", stale=true`}}, "")
		}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url := fakeServer(t, tc.answer)
			if tc.name == "always stale" {
				url = strings.Replace(url, "rtsp://", "rtsp://u:p@", 1)
			}
			err := play(t, url, tc.transport)
			if tc.want != nil && err != tc.want {
				t.Fatalf("%v, want %v", err, tc.want)
			}
			if err == nil {
				t.Fatal("no error")
			}
		})
	}
	// A server that plays G.711 over TCP and keeps alive with OPTIONS.
	var methods []string
	url := fakeServer(t, describeWith(fakeSDP, func(req *Request) []byte {
		return reply(req, 200, Header{{"Transport", "RTP/AVP/TCP;interleaved=6-7"}, {"Session", "S;timeout=1"}}, "")
	}))
	_ = url
	s := newSink()
	cfg := s.config()
	cfg.Engine, cfg.ReadTimeout = ce, 2*time.Second
	ka := fakeServer(t, func(req *Request) []byte {
		methods = append(methods, req.Method)
		switch req.Method {
		case MethodOptions:
			return reply(req, 200, Header{{"Public", "DESCRIBE"}}, "")
		case MethodDescribe:
			return reply(req, 200, nil, fakeSDP)
		case MethodSetup:
			return reply(req, 200, Header{{"Transport", "RTP/AVP/TCP;interleaved=6-7"}, {"Session", "S;timeout=1"}}, "")
		case MethodPlay:
			b := reply(req, 200, Header{{"RTP-Info", "url=rtsp://127.0.0.1/abs/trackID=1;seq=1;rtptime=100"}}, "")
			b = AppendInterleaved(b, 6, []byte{0x80, 0, 0, 1, 0, 0, 0, 100, 0, 0, 0, 1, 5, 5})
			b = AppendInterleaved(b, 7, []byte{0x80, 200, 0, 0})
			return b
		}
		return reply(req, 200, nil, "")
	})
	c, err := Play(cfg, ka)
	if err != nil {
		t.Fatal(err)
	}
	s.wait(t, "packet", func(s *sink) bool { return len(s.pkts) == 1 })
	if s.pkts[0].PTS != recordEpoch || s.tracks[0][0].Codec != av.CodecPCMU {
		t.Fatalf("%+v", s.pkts[0])
	}
	// Keep-alives go out as OPTIONS, every half of the 1 s timeout.
	time.Sleep(1200 * time.Millisecond)
	c.Close()
	n := 0
	for _, m := range methods {
		if m == MethodOptions {
			n++
		}
	}
	if n < 2 {
		t.Fatalf("keep-alives: %v", methods)
	}
}

// TestTrackChanges has the publisher change its tracks: the same codec
// with other parameter sets goes on, in band, to players, relays and the
// pushers that cannot follow; another codec ends the players.
func TestTrackChanges(t *testing.T) {
	h := hub.New(hub.Config{})
	base, _, _ := serve(t, ServerConfig{Hub: h})
	pub := publishHub(t, h, "s")
	defer pub.Close()
	for _, p := range testPackets(12) {
		pub.WritePacket(p)
	}
	ce := run(t)
	s := newSink()
	cfg := s.config()
	cfg.Engine = ce
	c, err := Play(cfg, base+"s")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s.wait(t, "tracks", func(s *sink) bool { return len(s.pkts) > 0 })
	local := hub.New(hub.Config{})
	relayed := newSink()
	rcfg := relayed.config()
	rcfg.Engine = ce
	if _, err := PlayToHub(rcfg, base+"s", local, "copy"); err != nil {
		t.Fatal(err)
	}
	relayed.wait(t, "relay", func(s *sink) bool { return len(s.pkts) > 0 })
	pushClosed := make(chan error, 1)
	ready := make(chan struct{})
	if _, err := PublishFromHub(ClientConfig{Engine: ce, OnReady: func() { close(ready) }, OnClose: func(err error) { pushClosed <- err }},
		base+"pushed", local, "copy", hub.SubscribeOptions{}); err != nil {
		t.Fatal(err)
	}
	<-ready

	// Other parameter sets, 1280x720.
	ps := readTestdata(t, "../codec/h264/testdata/h264_high_720.ps")
	cfg720, _ := h264.NewAVCConfig(ps[:1], ps[1:2])
	vc, _ := cfg720.Append(nil)
	tracks := testTracks(t)
	tracks[0] = &av.Track{Codec: av.CodecH264, Config: vc, Width: 1280, Height: 720}
	pub.SetTracks(tracks)
	for _, p := range testPackets(12) {
		pub.WritePacket(p)
	}
	s.wait(t, "new tracks", func(s *sink) bool { return len(s.tracks) == 2 && s.tracks[1][0].Width == 1280 })
	relayed.wait(t, "relay new tracks", func(s *sink) bool { return len(s.tracks) == 2 })
	select {
	case <-pushClosed:
	case <-time.After(testTimeout):
		t.Fatal("pusher kept on after a track change")
	}
	// Another codec: the player's streams are gone.
	tracks = testTracks(t)
	tracks[0] = &av.Track{Codec: av.CodecH265}
	pub.SetTracks(tracks)
	s.wait(t, "closed", func(s *sink) bool { return len(s.closed) > 0 })
	// And other tracks altogether.
	s2 := newSink()
	cfg2 := s2.config()
	cfg2.Engine = ce
	c2, _ := Play(cfg2, base+"s")
	defer c2.Close()
	s2.wait(t, "tracks", func(s *sink) bool { return len(s.tracks) > 0 })
	pub.SetTracks(tracks[:1])
	s2.wait(t, "closed", func(s *sink) bool { return len(s.closed) > 0 })
}

// TestRecordInBand records H.264 announced without parameter sets, which
// come in band: the hub's track is updated.
func TestRecordInBand(t *testing.T) {
	h := hub.New(hub.Config{})
	base, _, _ := serve(t, ServerConfig{Hub: h})
	ce := run(t)
	ready := make(chan struct{})
	c, err := Publish(ClientConfig{Engine: ce, OnReady: func() { close(ready) }}, base+"inband", []*av.Track{{Codec: av.CodecH264}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	<-ready
	s := newSink()
	if _, err := h.Subscribe("inband", s, hub.SubscribeOptions{}); err != nil {
		t.Fatal(err)
	}
	ps := readTestdata(t, "../codec/h264/testdata/h264_base_360.ps")
	for i := range 3 {
		au := h264.AppendAVCC(h264.AppendAVCC(h264.AppendAVCC(nil, ps[0]), ps[1]), []byte{0x65, byte(i), 2, 3})
		if err := c.WritePacket(&av.Packet{DTS: time.Duration(i) * 40 * time.Millisecond, Keyframe: true, Data: au}); err != nil {
			t.Fatal(err)
		}
	}
	s.wait(t, "track update", func(s *sink) bool { return len(s.tracks) == 2 && len(s.pkts) > 0 })
	if s.tracks[1][0].Width != 640 {
		t.Fatalf("%+v", s.tracks[1][0])
	}
}

// TestPauseAndJoin pauses, sets up another stream and plays again.
func TestPauseAndJoin(t *testing.T) {
	h := hub.New(hub.Config{})
	base, _, _ := serve(t, ServerConfig{Hub: h})
	pub := publishHub(t, h, "s")
	defer pub.Close()
	for _, p := range testPackets(12) {
		pub.WritePacket(p)
	}
	r := dialRaw(t, base)
	expect(t, r.do(MethodSetup, base+"s/trackID=0", "Transport: RTP/AVP/TCP;unicast"), StatusOK)
	expect(t, r.do(MethodPlay, base+"s"), StatusOK)
	for {
		if m, ok := r.next(true).(*Interleaved); ok && m.Channel == 0 {
			break
		}
	}
	expect(t, r.do(MethodSetup, base+"s/trackID=1", "Transport: RTP/AVP/TCP;unicast"), StatusMethodNotValidInState)
	expect(t, r.do(MethodPause, base+"s"), StatusOK)
	expect(t, r.do(MethodSetup, base+"s/trackID=1", "Transport: RTP/AVP/TCP;unicast"), StatusOK)
	resp := r.do(MethodPlay, base+"s")
	expect(t, resp, StatusOK)
	if infos := ParseRTPInfo(resp.Header.Get("RTP-Info")); len(infos) != 2 {
		t.Fatal(resp.Header)
	}
	for _, p := range testPackets(12) {
		pub.WritePacket(p)
	}
	for {
		if m, ok := r.next(true).(*Interleaved); ok && m.Channel == 2 {
			break
		}
	}
}

func TestHubErrors(t *testing.T) {
	h := hub.New(hub.Config{MaxSubscribersPerStream: 1})
	base, srv, e := serve(t, ServerConfig{Hub: h})
	pub := publishHub(t, h, "s")
	defer pub.Close()
	holder := newSink()
	sub, err := h.Subscribe("s", holder, hub.SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	r := dialRaw(t, base)
	expect(t, r.do(MethodDescribe, base+"s"), StatusNotEnoughBandwidth)
	sub.Close()
	h.Close()
	expect(t, r.do(MethodDescribe, base+"s"), StatusServiceUnavailable)
	// A busy UDP address.
	srv2 := NewServer(ServerConfig{UDPRTPAddr: "127.0.0.1:0", UDPRTCPAddr: "300.0.0.1:0"})
	if err := srv2.ListenUDP(e); err == nil {
		t.Fatal("bad address bound")
	}
	srv3 := NewServer(ServerConfig{UDPRTPAddr: "300.0.0.1:0"})
	if err := srv3.ListenUDP(e); err == nil {
		t.Fatal("bad address bound")
	}
	if bindingPort(e, 999) != 0 || bindingPort(e, 0) != 0 {
		t.Fatal("bindingPort")
	}
	srv.OnPriorityData(nil, nil)
	p := &probe{}
	p.OnPacket(nil)
	p.OnClose(nil)
	(&clientHandler{}).OnOpen(nil)
	(&clientHandler{}).OnPriorityData(nil, nil)
	u := &clientUDPHandler{}
	u.OnOpen(nil)
	u.OnPriorityData(nil, nil)
	u.OnClose(nil, nil)
	uh := &udpHandler{}
	uh.OnOpen(nil)
	uh.OnPriorityData(nil, nil)
	uh.OnClose(nil, nil)
	if (&ServerConn{}).User() != "" {
		t.Fatal("user")
	}
	// A non-RTSP connection's data is ignored.
	srv.OnData(dialedConn(t, e), []byte("x"))
}

// dialedConn returns a connection of e that the server did not open.
func dialedConn(t *testing.T, e *fib.Engine) *fib.Connection {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan *fib.Connection, 1)
	if err := e.DialWithHandler("tcp", ln.Addr().String(), time.Second, fib.HandlerFuncs{}, func(c *fib.Connection, err error) { done <- c }); err != nil {
		t.Fatal(err)
	}
	c := <-done
	if c == nil {
		t.Fatal("dial failed")
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestUDPSender(t *testing.T) {
	recv, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer recv.Close()
	e := run(t, fib.Binding{Network: "udp", Addr: "127.0.0.1:0", Handler: fib.HandlerFuncs{}})
	to := netip.MustParseAddrPort(recv.LocalAddr().String())
	u := newUDPSender(e, 0, to, 10)
	// While the peer opens, a little waits and the rest is dropped.
	u.send([]byte{1, 2, 3})
	u.send(make([]byte, 20))
	_ = recv.SetReadDeadline(time.Now().Add(testTimeout))
	buf := make([]byte, 100)
	n, _, err := recv.ReadFromUDP(buf)
	if err != nil || !bytes.Equal(buf[:n], []byte{1, 2, 3}) {
		t.Fatal(n, err)
	}
	u.send([]byte{4})
	if n, _, err := recv.ReadFromUDP(buf); err != nil || buf[0] != 4 || n != 1 {
		t.Fatal(n, err)
	}
	u.close()
	u.send([]byte{5})
	// A binding that is not there, and a close while opening.
	bad := newUDPSender(e, 99, to, 10)
	bad.send([]byte{1})
	gone := newUDPSender(e, 0, to, 10)
	gone.close()
	time.Sleep(50 * time.Millisecond)
	bad.mu.Lock()
	failed := bad.failed
	bad.mu.Unlock()
	if !failed {
		t.Fatal("open of a missing binding did not fail")
	}
}

func TestClientUDPKeepAlive(t *testing.T) {
	h := hub.New(hub.Config{})
	base, _, _ := serve(t, ServerConfig{Hub: h, SessionTimeout: 2 * time.Second})
	pub := publishHub(t, h, "s")
	defer pub.Close()
	ce := run(t)
	s := newSink()
	cfg := s.config()
	cfg.Engine, cfg.Transport, cfg.KeepAlive = ce, TransportUDP, 100*time.Millisecond
	cfg.ReorderDelay = 10 * time.Millisecond
	c, err := Play(cfg, base+"s")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s.wait(t, "tracks", func(s *sink) bool { return len(s.tracks) > 0 })
	// The session outlives its timeout on keep-alives and receiver
	// reports, and the stream plays.
	go func() {
		for i, p := range testPackets(80) {
			pub.WritePacket(p)
			if i%3 == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()
	time.Sleep(2500 * time.Millisecond)
	s.wait(t, "packets", func(s *sink) bool { return len(s.pkts) > 100 })
	s.mu.Lock()
	closed := len(s.closed)
	s.mu.Unlock()
	if closed != 0 {
		t.Fatal(s.closed)
	}
	// Datagrams from elsewhere are dropped.
	other, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	defer other.Close()
	c.mu.Lock()
	port := c.streams[0].rtpPort
	c.mu.Unlock()
	_, _ = other.WriteToUDP([]byte{0x80, 0, 0, 1}, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 2), Port: port})
	_ = strconv.Itoa(port)
}

func TestH265Session(t *testing.T) {
	h := hub.New(hub.Config{})
	base, _, _ := serve(t, ServerConfig{Hub: h})
	ps := readTestdata(t, "../codec/h265/testdata/h265_main_720.ps")
	cfg, err := h265.NewHEVCConfig(ps[:1], ps[1:2], ps[2:3])
	if err != nil {
		t.Fatal(err)
	}
	vc, _ := cfg.Append(nil)
	pub, _ := h.Publish("h", []*av.Track{{Codec: av.CodecH265, Config: vc}, {Codec: av.CodecOpus, SampleRate: 48000, Channels: 2}}, hub.PublishInfo{})
	defer pub.Close()
	pub.WritePacket(&av.Packet{Track: 0, Keyframe: true, Data: h264.AppendAVCC(nil, append([]byte{0x26, 0x01}, make([]byte, 3000)...))})
	pub.WritePacket(&av.Packet{Track: 1, Keyframe: true, Data: []byte{0xfc, 1, 2}})
	ce := run(t)
	s := newSink()
	ccfg := s.config()
	ccfg.Engine = ce
	c, _ := Play(ccfg, base+"h")
	defer c.Close()
	s.wait(t, "packets", func(s *sink) bool { return len(s.pkts) >= 2 })
	if s.tracks[0][0].Width != 1280 || s.tracks[0][1].Codec != av.CodecOpus {
		t.Fatalf("%+v", s.tracks[0])
	}
}

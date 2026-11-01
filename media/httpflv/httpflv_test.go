package httpflv

import (
	"bufio"
	"bytes"
	stdtls "crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/internal/outputwait"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/flv"
	"github.com/lesismal/fib/media/hub"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
	"github.com/lesismal/fib/websocket"
)

const testTimeout = 10 * time.Second

func run(t *testing.T, bindings ...fib.Binding) *fib.Engine {
	t.Helper()
	config := fib.DefaultConfig()
	config.Name = "httpflv-" + t.Name()
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

// serve runs a Handler on 127.0.0.1 and returns its address.
func serve(t *testing.T, config Config, tlsConfig *stdtls.Config) string {
	t.Helper()
	router := fibhttp.NewRouter()
	router.Handle("/*", NewHandler(config))
	var handler fib.Handler = fibhttp.NewHandler(router)
	if tlsConfig != nil {
		handler = fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), handler)
	}
	e := run(t, fib.Binding{Addr: "127.0.0.1:0", Handler: handler})
	addrs, err := e.BindingAddrs(0)
	if err != nil {
		t.Fatal(err)
	}
	return addrs[0].String()
}

func stream(t testing.TB) ([]*av.Track, []*av.Packet) {
	t.Helper()
	b, err := os.ReadFile("../flv/testdata/h264_aac.flv")
	if err != nil {
		t.Fatal(err)
	}
	var tracks []*av.Track
	var packets []*av.Packet
	dec := flv.NewDecoder(flv.DecoderConfig{
		OnTracks: func(tr []*av.Track) { tracks = tr },
		OnPacket: func(p *av.Packet) { packets = append(packets, p) },
	})
	if err := flv.NewDemuxer(flv.DemuxerConfig{}).Feed(b, func(tag flv.Tag) error {
		return dec.WriteTag(tag.Type, tag.Timestamp, tag.Data)
	}); err != nil {
		t.Fatal(err)
	}
	dec.Flush()
	return tracks, packets
}

// player demuxes and decodes what it is fed.
type player struct {
	mu      sync.Mutex
	dm      *flv.Demuxer
	dec     *flv.Decoder
	tracks  []*av.Track
	packets []*av.Packet
	err     error
}

func newPlayer() *player {
	p := &player{dm: flv.NewDemuxer(flv.DemuxerConfig{})}
	p.dec = flv.NewDecoder(flv.DecoderConfig{
		OnTracks: func(tr []*av.Track) { p.tracks = tr },
		OnPacket: func(pkt *av.Packet) { p.packets = append(p.packets, pkt) },
	})
	return p
}

func (p *player) feed(b []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err == nil {
		p.err = p.dm.Feed(b, func(tag flv.Tag) error { return p.dec.WriteTag(tag.Type, tag.Timestamp, tag.Data) })
	}
}

func (p *player) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.packets)
}

// check compares what the player got with what was published: the
// packets are the published ones from the first keyframe on, rebased to
// start at 0.
func (p *player) check(t *testing.T, tracks []*av.Track, packets []*av.Packet) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		t.Fatal(p.err)
	}
	if len(p.tracks) != 2 || p.tracks[0].Codec != tracks[0].Codec || !bytes.Equal(p.tracks[0].Config, tracks[0].Config) ||
		p.tracks[1].Codec != tracks[1].Codec {
		t.Fatalf("tracks %+v", p.tracks)
	}
	if len(p.packets) != len(packets) {
		t.Fatalf("%d packets of %d", len(p.packets), len(packets))
	}
	base := packets[0].DTS
	for i, got := range p.packets {
		w := packets[i]
		if got.Track != w.Track || got.Keyframe != w.Keyframe || !bytes.Equal(got.Data, w.Data) ||
			got.DTS != w.DTS-base || got.CTS() != w.CTS() {
			t.Fatalf("packet %d: %+v want %+v", i, got, w)
		}
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func subscribers(h *hub.Hub, path string) int {
	for _, s := range h.Streams() {
		if s.Path == path {
			return s.Subscribers
		}
	}
	return 0
}

func TestHTTPFLV(t *testing.T) {
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"http1", "http2"} {
		t.Run(mode, func(t *testing.T) {
			h := hub.New(hub.Config{})
			defer h.Close()
			var srvTLS *stdtls.Config
			client := &http.Client{Timeout: testTimeout}
			scheme := "http://"
			if mode == "http2" {
				srvTLS, scheme = serverTLS, "https://"
				client.Transport = &http.Transport{TLSClientConfig: clientTLS, ForceAttemptHTTP2: true}
			}
			addr := serve(t, Config{Hub: h, CORS: "*", Wait: time.Second}, srvTLS)
			tracks, packets := stream(t)
			resp, err := client.Get(scheme + addr + "/live/s.flv?token=1")
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "video/x-flv" || resp.Header.Get("Access-Control-Allow-Origin") != "*" {
				t.Fatalf("%d %v", resp.StatusCode, resp.Header)
			}
			if mode == "http2" && resp.ProtoMajor != 2 {
				t.Fatal("not HTTP/2")
			}
			waitFor(t, func() bool { return subscribers(h, "live/s") == 1 })
			pub, err := h.Publish("live/s", tracks, hub.PublishInfo{})
			if err != nil {
				t.Fatal(err)
			}
			p := newPlayer()
			done := make(chan error, 1)
			go func() {
				buf := make([]byte, 4096)
				for {
					n, err := resp.Body.Read(buf)
					p.feed(buf[:n])
					if err != nil {
						done <- err
						return
					}
				}
			}()
			for _, pkt := range packets {
				pub.WritePacket(pkt)
			}
			waitFor(t, func() bool { return p.count() == len(packets) })
			p.check(t, tracks, packets)
			// Unpublishing ends the response.
			pub.Close()
			select {
			case err := <-done:
				if err != io.EOF {
					t.Fatal(err)
				}
			case <-time.After(testTimeout):
				t.Fatal("response did not end")
			}
		})
	}
}

func TestHTTPFLVLate(t *testing.T) {
	// A player that comes after the publisher starts from the GOP cache,
	// and leaving drops its subscription.
	h := hub.New(hub.Config{})
	defer h.Close()
	addr := serve(t, Config{Hub: h}, nil)
	tracks, packets := stream(t)
	pub, err := h.Publish("live/s", tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	for _, pkt := range packets {
		pub.WritePacket(pkt)
	}
	resp, err := http.Get("http://" + addr + "/live/s.flv")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("CORS without asking")
	}
	p := newPlayer()
	buf := make([]byte, 4096)
	for p.count() == 0 {
		n, err := resp.Body.Read(buf)
		p.feed(buf[:n])
		if err != nil {
			t.Fatal(err)
		}
	}
	if !p.packets[0].Keyframe {
		t.Fatal("no keyframe first")
	}
	_ = resp.Body.Close()
	waitFor(t, func() bool { return subscribers(h, "live/s") == 0 })
	// A track change mid-stream reaches the player as new headers.
	resp, err = http.Get("http://" + addr + "/live/s.flv")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	p = newPlayer()
	// A track FLV cannot carry is left out, its packets with it.
	pub.SetTracks([]*av.Track{tracks[0], {Codec: av.CodecUnknown}})
	pub.WritePacket(&av.Packet{Track: 1, Keyframe: true, DTS: time.Hour, Data: []byte{1}})
	pub.WritePacket(&av.Packet{Track: 0, Keyframe: true, DTS: time.Hour, PTS: time.Hour, Data: packets[0].Data})
	for {
		n, err := resp.Body.Read(buf)
		p.feed(buf[:n])
		if err != nil {
			t.Fatal(err)
		}
		p.mu.Lock()
		ok := len(p.packets) > 0 && p.packets[len(p.packets)-1].DTS > time.Minute
		p.mu.Unlock()
		if ok {
			break
		}
	}
}

func TestRefusals(t *testing.T) {
	refuse := errors.New("no")
	h := hub.New(hub.Config{OnSubscribe: func(path string, _ hub.SubscribeInfo) error {
		if path == "live/hub" {
			return refuse
		}
		return nil
	}})
	defer h.Close()
	addr := serve(t, Config{Hub: h, OnPlay: func(c *fibhttp.Context, path, query string) error {
		if query == "bad" {
			return refuse
		}
		return nil
	}}, nil)
	pub, _ := h.Publish("live/s", nil, hub.PublishInfo{})
	defer pub.Close()
	cases := []struct {
		method, path string
		status       int
	}{
		{"GET", "/live/s", 404},
		{"GET", "/live/.flv", 404},
		{"GET", "/.flv", 404},
		{"GET", "/live/none.flv", 404},
		{"GET", "/live/s.flv?bad", 403},
		{"GET", "/live/hub.flv", 403},
		{"POST", "/live/s.flv", 405},
		{"HEAD", "/live/s.flv", 200},
	}
	for _, c := range cases {
		req, _ := http.NewRequest(c.method, "http://"+addr+c.path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != c.status {
			t.Errorf("%s %s: %d", c.method, c.path, resp.StatusCode)
		}
	}
}

// wsPlayer is a WebSocket handler feeding a player.
type wsPlayer struct {
	p      *player
	closed chan uint16
}

func (w *wsPlayer) OnOpen(*websocket.Connection, *http.Request) {}
func (w *wsPlayer) OnMessage(_ *websocket.Connection, op websocket.Opcode, b []byte) {
	if op == websocket.Binary {
		w.p.feed(b)
	}
}
func (w *wsPlayer) OnClose(_ *websocket.Connection, code uint16, _ string, _ error) { w.closed <- code }

func TestWebSocketFLV(t *testing.T) {
	h := hub.New(hub.Config{})
	defer h.Close()
	addr := serve(t, Config{Hub: h}, nil)
	tracks, packets := stream(t)
	pub, err := h.Publish("live/s", tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	e := run(t)
	dialer := websocket.NewDialer(e, websocket.DialerConfig{HandshakeTimeout: testTimeout, MaxMessageBytes: 1 << 20})
	w := &wsPlayer{p: newPlayer(), closed: make(chan uint16, 1)}
	if _, _, err := dialer.Go("ws://"+addr+"/live/s.flv", nil, w).Wait(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return subscribers(h, "live/s") == 1 })
	for _, pkt := range packets {
		pub.WritePacket(pkt)
	}
	waitFor(t, func() bool { return w.p.count() == len(packets) })
	w.p.check(t, tracks, packets)
	pub.Close()
	select {
	case code := <-w.closed:
		if code != websocket.CloseNormal {
			t.Fatal(code)
		}
	case <-time.After(testTimeout):
		t.Fatal("not closed")
	}
	// A stream that is not there closes the WebSocket.
	w = &wsPlayer{p: newPlayer(), closed: make(chan uint16, 1)}
	if _, _, err := dialer.Go("ws://"+addr+"/live/none.flv", nil, w).Wait(); err != nil {
		t.Fatal(err)
	}
	if code := <-w.closed; code != closePolicyViolation {
		t.Fatal(code)
	}
	// A player that goes away drops its subscription.
	pub, _ = h.Publish("live/s", tracks, hub.PublishInfo{})
	defer pub.Close()
	w = &wsPlayer{p: newPlayer(), closed: make(chan uint16, 1)}
	ws, _, err := dialer.Go("ws://"+addr+"/live/s.flv", nil, w).Wait()
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return subscribers(h, "live/s") == 1 })
	_ = ws.Close(websocket.CloseNormal, "")
	waitFor(t, func() bool { return subscribers(h, "live/s") == 0 })
	// A request that is not a valid handshake is refused by the upgrade.
	req, _ := http.NewRequest("GET", "http://"+addr+"/live/s.flv", nil)
	req.Header.Set("Upgrade", "websocket")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatal(resp.StatusCode)
	}
}

func TestSlowPlayer(t *testing.T) {
	// A player that stops reading has its packets dropped, then is
	// disconnected once its output stops moving.
	h := hub.New(hub.Config{})
	defer h.Close()
	addr := serve(t, Config{Hub: h, WriteQueueLimit: 64 << 10, MaxWriteQueue: 1 << 30, IdleTimeout: 500 * time.Millisecond}, nil)
	tracks, packets := stream(t)
	pub, err := h.Publish("live/s", tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.(*net.TCPConn).SetReadBuffer(4096)
	_, _ = io.WriteString(c, "GET /live/s.flv HTTP/1.1\r\nHost: x\r\n\r\n")
	waitFor(t, func() bool { return subscribers(h, "live/s") == 1 })
	big := bytes.Repeat([]byte{0, 0, 0, 1, 0x65}, 20000)
	deadline := time.Now().Add(testTimeout)
	for subscribers(h, "live/s") == 1 {
		if time.Now().After(deadline) {
			t.Fatal("slow player not disconnected")
		}
		pub.WritePacket(&av.Packet{Track: 0, Keyframe: true, Data: big})
		pub.WritePacket(&av.Packet{Track: 1, Keyframe: true, Data: packets[1].Data})
		time.Sleep(10 * time.Millisecond)
	}
	// What did arrive is a well-formed start of a response.
	_ = c.SetReadDeadline(time.Now().Add(testTimeout))
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "HTTP/1.1 200") {
		t.Fatal(line, err)
	}
}

func TestSinkEdges(t *testing.T) {
	h := NewHandler(Config{})
	s := &sink{h: h, mux: flv.NewMuxer(flv.EncoderConfig{})}
	// Tracks FLV cannot carry start nothing; packets before the start go
	// nowhere.
	s.OnTracks([]*av.Track{{Codec: av.CodecUnknown}})
	s.OnPacket(&av.Packet{})
	if s.started {
		t.Fatal("started")
	}
	// HTTP/2's flow control past its limit reads as over the queue limit.
	s.above.Store(true)
	if s.backlog() <= h.config.WriteQueueLimit {
		t.Fatal("backlog")
	}
	s.above.Store(false)
	saved := outputwait.Queued
	outputwait.Queued = nil
	if s.backlog() != 0 {
		t.Fatal("backlog without accounting")
	}
	outputwait.Queued = saved
	// end runs once, and with nothing to end.
	s.end()
	s.end()
	s.OnClose(nil)
	if !isUpgrade(&http.Request{ProtoMajor: 2, Method: http.MethodConnect}) {
		t.Fatal("extended CONNECT")
	}
	if _, _, ok := DefaultPath(&http.Request{URL: mustURL("/a/b/c.flv")}); !ok {
		t.Fatal("nested path")
	}
	var w websocketHandler
	w.OnOpen(nil, nil)
	w.OnMessage(nil, websocket.Binary, nil)
	// A play that ended while its subscription was being made closes it,
	// and one whose write failed meanwhile ends.
	hb := hub.New(hub.Config{})
	defer hb.Close()
	pub, _ := hb.Publish("p", nil, hub.PublishInfo{})
	defer pub.Close()
	for _, ended := range []bool{true, false} {
		s := &sink{h: h, mux: flv.NewMuxer(flv.EncoderConfig{}), closed: true, ended: ended}
		sub, err := hb.Subscribe("p", s, hub.SubscribeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if s.attach(sub) {
			t.Fatal("attached")
		}
		if subscribers(hb, "p") != 0 {
			t.Fatal("subscription kept")
		}
	}
	s = &sink{h: h, mux: flv.NewMuxer(flv.EncoderConfig{}), closed: true}
	s.OnTracks(nil)
}

func TestWebSocketWriteFails(t *testing.T) {
	// A sink whose WebSocket can no longer send stops.
	addr := serve(t, Config{Hub: hub.New(hub.Config{})}, nil)
	e := run(t)
	dialer := websocket.NewDialer(e, websocket.DialerConfig{HandshakeTimeout: testTimeout})
	w := &wsPlayer{p: newPlayer(), closed: make(chan uint16, 1)}
	ws, _, err := dialer.Go("ws://"+addr+"/live/none.flv", nil, w).Wait()
	if err != nil {
		t.Fatal(err)
	}
	<-w.closed
	s := &sink{h: NewHandler(Config{IdleTimeout: -1}), ws: ws, mux: flv.NewMuxer(flv.EncoderConfig{})}
	s.mu.Lock()
	s.writeLocked([]byte{1})
	closed := s.closed
	s.mu.Unlock()
	if !closed {
		t.Fatal("write to a closed WebSocket")
	}
}

func mustURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

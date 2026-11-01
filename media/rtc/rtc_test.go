//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package rtc

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/flv"
	"github.com/lesismal/fib/media/hub"
	"github.com/lesismal/fib/media/mpegts"
	"github.com/lesismal/fib/webrtc"
)

const waitTimeout = 10 * time.Second

// env is a fib engine serving a Server over HTTP, with a hub and a Mux.
type env struct {
	engine *fib.Engine
	hub    *hub.Hub
	srv    *Server
	base   string
}

func startEngine(t testing.TB, bindings ...fib.Binding) *fib.Engine {
	t.Helper()
	config := fib.DefaultConfig()
	config.LogSettings = false
	e, err := fib.New(config, bindings...)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- e.Run() }()
	t.Cleanup(func() {
		e.Stop()
		<-done
		_ = e.Close()
	})
	return e
}

func newEnv(t testing.TB, tune func(*Config)) *env { return newEnvWith(t, tune, nil) }

// newEnvWith is newEnv with routes of the test's own on the router.
func newEnvWith(t testing.TB, tune func(*Config), routes func(*fibhttp.Router)) *env {
	t.Helper()
	ev := &env{hub: hub.New(hub.Config{})}
	router := fibhttp.NewRouter()
	if routes != nil {
		routes(router)
	}
	router.Handle("/*", fibhttp.HandlerFunc(func(c *fibhttp.Context) { ev.srv.ServeHTTP(c) }))
	ev.engine = startEngine(t, fib.Binding{Addr: "127.0.0.1:0", Handler: fibhttp.NewHandler(router)})
	mux, err := webrtc.NewMux(ev.engine, webrtc.MuxConfig{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Hub: ev.hub, WebRTC: webrtc.Configuration{Mux: mux, ICELite: true}}
	if tune != nil {
		tune(&cfg)
	}
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ev.srv = srv
	t.Cleanup(func() {
		srv.Close()
		ev.hub.Close()
		_ = mux.Close()
	})
	addrs, err := ev.engine.BindingAddrs(0)
	if err != nil {
		t.Fatal(err)
	}
	ev.base = "http://" + addrs[0].String()
	return ev
}

// clientConfig is a client's configuration on the env's engine, its
// PeerConnection on a port of its own on loopback.
func (ev *env) clientConfig() ClientConfig {
	return ClientConfig{Engine: ev.engine, WebRTC: webrtc.Configuration{Engine: ev.engine, ListenAddr: "127.0.0.1:0"}}
}

// media is a stream read from a fixture.
type media struct {
	tracks  []*av.Track
	packets []*av.Packet
}

func loadTS(t testing.TB, name string) *media {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	m := &media{}
	d := mpegts.NewDemuxer(mpegts.DemuxerConfig{
		OnTracks: func(tr []*av.Track) { m.tracks = tr },
		OnPacket: func(p *av.Packet) { m.packets = append(m.packets, p) },
	})
	d.Write(b)
	d.Flush()
	return m
}

func loadFLV(t testing.TB, name string) *media {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	m := &media{}
	dec := flv.NewDecoder(flv.DecoderConfig{
		OnTracks: func(tr []*av.Track) { m.tracks = tr },
		OnPacket: func(p *av.Packet) { m.packets = append(m.packets, p) },
	})
	if err := flv.NewDemuxer(flv.DemuxerConfig{}).Feed(b, func(tag flv.Tag) error {
		return dec.WriteTag(tag.Type, tag.Timestamp, tag.Data)
	}); err != nil {
		t.Fatal(err)
	}
	dec.Flush()
	return m
}

func (m *media) span() time.Duration {
	return m.packets[len(m.packets)-1].DTS + 40*time.Millisecond
}

// feed writes m's packets to write, looping, paced by their times, until
// stop closes.
func feed(m *media, write func(*av.Packet), stop <-chan struct{}) {
	start := time.Now()
	for loop := time.Duration(0); ; loop += m.span() {
		for _, p := range m.packets {
			q := *p
			q.DTS += loop
			q.PTS += loop
			if d := time.Until(start.Add(q.DTS)); d > 0 {
				select {
				case <-stop:
					return
				case <-time.After(d):
				}
			}
			select {
			case <-stop:
				return
			default:
			}
			write(&q)
		}
	}
}

// publish publishes m into the env's hub at path, fed until the test ends.
func (ev *env) publish(t testing.TB, path string, m *media) {
	t.Helper()
	pub, err := ev.hub.Publish(path, m.tracks, hub.PublishInfo{Protocol: "test"})
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		feed(m, pub.WritePacket, stop)
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
		pub.Close()
	})
}

// collector gathers what a client or a hub subscription receives.
type collector struct {
	mu      sync.Mutex
	tracks  []*av.Track
	packets []*av.Packet
	ready   bool
	closed  chan error
}

func newCollector() *collector { return &collector{closed: make(chan error, 1)} }

func (c *collector) OnTracks(tr []*av.Track) {
	c.mu.Lock()
	c.tracks = tr
	c.mu.Unlock()
}

func (c *collector) OnPacket(p *av.Packet) {
	c.mu.Lock()
	c.packets = append(c.packets, p)
	c.mu.Unlock()
}

func (c *collector) OnClose(err error) {
	select {
	case c.closed <- err:
	default:
	}
}

func (c *collector) onReady() {
	c.mu.Lock()
	c.ready = true
	c.mu.Unlock()
}

func (c *collector) config(cfg ClientConfig) ClientConfig {
	cfg.OnTracks, cfg.OnPacket, cfg.OnClose, cfg.OnReady = c.OnTracks, c.OnPacket, c.OnClose, c.onReady
	return cfg
}

// wait waits until cond holds of what was collected.
func (c *collector) wait(t testing.TB, what string, cond func(tracks []*av.Track, packets []*av.Packet) bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		ok := cond(c.tracks, c.packets)
		c.mu.Unlock()
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t.Fatalf("timed out waiting for %s: %d tracks, %d packets", what, len(c.tracks), len(c.packets))
}

// counts counts the packets of each codec, keyframes of video.
func counts(tracks []*av.Track, packets []*av.Packet) (map[av.CodecID]int, int) {
	n := map[av.CodecID]int{}
	keys := 0
	for _, p := range packets {
		if p.Track < len(tracks) {
			n[tracks[p.Track].Codec]++
			if tracks[p.Track].Codec.IsVideo() && p.Keyframe {
				keys++
			}
		}
	}
	return n, keys
}

// contains reports whether some packet of want has data equal to b.
func contains(want []*av.Packet, b []byte) bool {
	for _, p := range want {
		if bytes.Equal(p.Data, b) {
			return true
		}
	}
	return false
}

// TestWHEPPlay plays an H.264 and Opus stream with a WHEP client: the
// video comes with its configuration, a keyframe first, and the packets
// as they were published.
func TestWHEPPlay(t *testing.T) {
	ev := newEnv(t, nil)
	m := loadTS(t, "testdata/h264_opus.ts")
	ev.publish(t, "live/a", m)
	col := newCollector()
	c, err := Pull(col.config(ev.clientConfig()), ev.base+"/whep/live/a")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	col.wait(t, "video and audio", func(tracks []*av.Track, packets []*av.Packet) bool {
		n, keys := counts(tracks, packets)
		return n[av.CodecH264] >= 30 && n[av.CodecOpus] >= 30 && keys >= 2
	})
	col.mu.Lock()
	tracks, packets := col.tracks, col.packets
	col.mu.Unlock()
	var video *av.Track
	for _, tr := range tracks {
		if tr.Codec == av.CodecH264 {
			video = tr
		}
	}
	if video == nil || video.Width != 160 || video.Height != 120 || len(video.Config) == 0 {
		t.Fatalf("video track %+v", video)
	}
	first := true
	same := 0
	for _, p := range packets {
		if tracks[p.Track].Codec != av.CodecH264 {
			if contains(m.packets, p.Data) {
				same++
			}
			continue
		}
		if first && !p.Keyframe {
			t.Fatal("first video packet is not a keyframe")
		}
		first = false
		if !p.Keyframe && contains(m.packets, p.Data) {
			same++
		}
	}
	if same < 40 {
		t.Fatalf("only %d packets as published", same)
	}
	if c.Resource() == "" || !strings.HasPrefix(c.Resource(), ev.base+DefaultResourcePrefix) {
		t.Fatalf("resource %q", c.Resource())
	}
	if ev.srv.Len() != 1 {
		t.Fatalf("%d sessions", ev.srv.Len())
	}
	c.Close()
	if err := <-col.closed; err != ErrClientClosed {
		t.Fatalf("closed with %v", err)
	}
	deadline := time.Now().Add(waitTimeout)
	for ev.srv.Len() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if ev.srv.Len() != 0 {
		t.Fatal("DELETE left the session")
	}
}

// TestWHIPPublish publishes H.265 and Opus with a WHIP client and reads
// it back from the hub.
func TestWHIPPublish(t *testing.T) {
	ev := newEnv(t, nil)
	m := loadTS(t, "../mpegts/testdata/h265_opus.ts")
	col := newCollector()
	cfg := ev.clientConfig()
	ready := make(chan struct{})
	cfg.OnReady = func() { close(ready) }
	c, err := Push(cfg, ev.base+"/whip/cam", m.tracks)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	stop := make(chan struct{})
	defer close(stop)
	go feed(m, func(p *av.Packet) { _ = c.WritePacket(p) }, stop)
	select {
	case <-ready:
	case <-time.After(waitTimeout):
		t.Fatal("not connected")
	}
	var sub *hub.Subscription
	deadline := time.Now().Add(waitTimeout)
	for sub == nil && time.Now().Before(deadline) {
		sub, err = ev.hub.Subscribe("cam", col, hub.SubscribeOptions{})
		if err != nil {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if sub == nil {
		t.Fatal(err)
	}
	defer sub.Close()
	col.wait(t, "video and audio", func(tracks []*av.Track, packets []*av.Packet) bool {
		n, keys := counts(tracks, packets)
		return n[av.CodecH265] >= 20 && n[av.CodecOpus] >= 20 && keys >= 1
	})
	col.mu.Lock()
	tracks, packets := col.tracks, col.packets
	col.mu.Unlock()
	same := 0
	for _, p := range packets {
		if tracks[p.Track].Codec == av.CodecOpus && contains(m.packets, p.Data) {
			same++
		}
	}
	if same < 20 {
		t.Fatalf("only %d Opus packets as published", same)
	}
	for _, tr := range tracks {
		if tr.Codec == av.CodecH265 && (tr.Width != 64 || tr.Height != 48 || len(tr.Config) == 0) {
			t.Fatalf("video track %+v", tr)
		}
	}
	if got := ev.hub.Streams(); len(got) != 1 || got[0].Publisher.Protocol != "whip" {
		t.Fatalf("streams %+v", got)
	}
}

// TestRelay pushes a hub stream to the WHIP endpoint with PushFromHub,
// and pulls it back from the WHEP endpoint into another path with
// PullToHub.
func TestRelay(t *testing.T) {
	ev := newEnv(t, nil)
	m := loadTS(t, "testdata/h264_opus.ts")
	ev.publish(t, "src", m)
	push, err := PushFromHub(ev.clientConfig(), ev.base+"/whip/mid", ev.hub, "src", hub.SubscribeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer push.Close()
	deadline := time.Now().Add(waitTimeout)
	for len(ev.hub.Streams()) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	var seen atomic.Int32
	pcfg := ev.clientConfig()
	pcfg.OnTracks = func([]*av.Track) { seen.Add(1) }
	pcfg.OnPacket = func(*av.Packet) { seen.Add(1) }
	pull, err := PullToHub(pcfg, ev.base+"/whep/mid", ev.hub, "copy")
	if err != nil {
		t.Fatal(err)
	}
	defer pull.Close()
	col := newCollector()
	var sub *hub.Subscription
	for sub == nil && time.Now().Before(deadline) {
		sub, err = ev.hub.Subscribe("copy", col, hub.SubscribeOptions{})
		if err != nil {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if sub == nil {
		t.Fatal(err)
	}
	defer sub.Close()
	col.wait(t, "relayed video and audio", func(tracks []*av.Track, packets []*av.Packet) bool {
		n, keys := counts(tracks, packets)
		return n[av.CodecH264] >= 20 && n[av.CodecOpus] >= 20 && keys >= 1
	})
	if len(pull.Tracks()) != 2 || len(push.Tracks()) != 2 || seen.Load() < 10 {
		t.Fatalf("tracks %v %v", pull.Tracks(), push.Tracks())
	}
}

// request sends a raw HTTP request to the env.
func (ev *env) request(t testing.TB, method, path, contentType, body string, header map[string]string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, ev.base+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return resp, string(b)
}

package httpflv

import (
	"bytes"
	stdtls "crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/netip"
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
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
	"github.com/lesismal/fib/websocket"
)

func streamFile(t testing.TB, name string) ([]*av.Track, []*av.Packet) {
	t.Helper()
	b, err := os.ReadFile("../flv/testdata/" + name)
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

// origin serves hub streams as HTTP-FLV, with extra routes, and returns
// its address.
func origin(t *testing.T, config Config, tlsConfig *stdtls.Config, routes func(*fibhttp.Router)) string {
	t.Helper()
	router := fibhttp.NewRouter()
	if routes != nil {
		routes(router)
	}
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

// recorder is a hub sink that keeps what it hears.
type recorder struct {
	mu      sync.Mutex
	tracks  [][]*av.Track
	packets []*av.Packet
	closed  bool
}

func (r *recorder) OnTracks(tracks []*av.Track) {
	r.mu.Lock()
	r.tracks = append(r.tracks, tracks)
	r.mu.Unlock()
}

func (r *recorder) OnPacket(pkt *av.Packet) {
	r.mu.Lock()
	r.packets = append(r.packets, pkt)
	r.mu.Unlock()
}

func (r *recorder) OnClose(error) {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.packets)
}

func (r *recorder) trackSets() [][]*av.Track {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slicesClone(r.tracks)
}

func (r *recorder) last() (int, time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.packets), r.packets[len(r.packets)-1].DTS
}

func slicesClone[T any](s []T) []T { return append([]T(nil), s...) }

// check compares the packets from the n-th on with what was published,
// rebased to start at base.
func (r *recorder) check(t *testing.T, from int, tracks []*av.Track, packets []*av.Packet, base time.Duration) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	last := r.tracks[len(r.tracks)-1]
	if len(last) != len(tracks) {
		t.Fatalf("tracks %+v", last)
	}
	for i := range last {
		if last[i].Codec != tracks[i].Codec || !bytes.Equal(last[i].Config, tracks[i].Config) {
			t.Fatalf("track %d: %+v want %+v", i, last[i], tracks[i])
		}
	}
	got := r.packets[from:]
	if len(got) != len(packets) {
		t.Fatalf("%d packets of %d", len(got), len(packets))
	}
	for i, g := range got {
		w := packets[i]
		if g.Track != w.Track || g.Keyframe != w.Keyframe || !bytes.Equal(g.Data, w.Data) ||
			g.DTS != w.DTS-packets[0].DTS+base || g.CTS() != w.CTS() {
			t.Fatalf("packet %d: %+v want %+v", i, g, w)
		}
	}
}

// pullResult is what a pull's OnClose heard.
type pullResult chan error

func (c pullResult) wait(t *testing.T) error {
	t.Helper()
	select {
	case err := <-c:
		return err
	case <-time.After(testTimeout):
		t.Fatal("pull did not end")
		return nil
	}
}

func pull(t *testing.T, config PullConfig, rawURL string, h *hub.Hub, path string) (*Puller, pullResult) {
	t.Helper()
	if config.Engine == nil {
		config.Engine = run(t)
	}
	done := make(pullResult, 2)
	config.OnClose = func(err error) { done <- err }
	p, err := Pull(config, rawURL, h, path)
	if err != nil {
		t.Fatal(err)
	}
	return p, done
}

// published reports whether path has a publisher, live or reserved.
func published(h *hub.Hub, path string) bool {
	for _, s := range h.Streams() {
		if s.Path == path {
			return s.Publishing || s.Reserved
		}
	}
	return false
}

func TestPull(t *testing.T) {
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"http1", "http2", "ws", "wss"} {
		for _, file := range []string{"h264_aac.flv", "hevc_aac.flv"} {
			t.Run(mode+"/"+file, func(t *testing.T) {
				a := hub.New(hub.Config{})
				defer a.Close()
				var info hub.PublishInfo
				b := hub.New(hub.Config{OnPublish: func(_ string, i hub.PublishInfo) error {
					info = i
					return nil
				}})
				defer b.Close()
				var srvTLS *stdtls.Config
				scheme := map[string]string{"http1": "http", "http2": "https", "ws": "ws", "wss": "wss"}[mode]
				if mode == "http2" || mode == "wss" {
					srvTLS = serverTLS
				}
				addr := origin(t, Config{Hub: a, OnPlay: func(c *fibhttp.Context, _, _ string) error {
					if c.Request.Header.Get("X-Token") != "t" {
						return errors.New("no token")
					}
					if mode == "http2" && c.Request.ProtoMajor != 2 {
						return errors.New("not HTTP/2")
					}
					return nil
				}}, srvTLS, nil)
				tracks, packets := streamFile(t, file)
				pub, err := a.Publish("live/s", tracks, hub.PublishInfo{})
				if err != nil {
					t.Fatal(err)
				}
				var got []*av.Packet
				var gotMu sync.Mutex
				p, done := pull(t, PullConfig{TLSConfig: clientTLS, Header: http.Header{"X-Token": {"t"}},
					OnPacket: func(pkt *av.Packet) {
						gotMu.Lock()
						got = append(got, pkt)
						gotMu.Unlock()
					},
				}, scheme+"://"+addr+"/live/s.flv?k=v", b, "copy/s")
				waitFor(t, func() bool { return published(b, "copy/s") })
				wantProto := "http-flv-pull"
				if strings.HasPrefix(mode, "ws") {
					wantProto = "ws-flv-pull"
				}
				if info.Protocol != wantProto || info.Query != "k=v" || info.Remote != netip.MustParseAddrPort(addr) {
					t.Fatalf("%+v", info)
				}
				rec := &recorder{}
				sub, err := b.Subscribe("copy/s", rec, hub.SubscribeOptions{Wait: testTimeout})
				if err != nil {
					t.Fatal(err)
				}
				defer sub.Close()
				for _, pkt := range packets {
					pub.WritePacket(pkt)
				}
				waitFor(t, func() bool { return rec.count() == len(packets) })
				rec.check(t, 0, tracks, packets, 0)
				gotMu.Lock()
				if len(got) != len(packets) {
					t.Fatalf("OnPacket heard %d", len(got))
				}
				gotMu.Unlock()
				// The origin's end is the pull's.
				pub.Close()
				if err := done.wait(t); err != nil {
					t.Fatal(err)
				}
				waitFor(t, func() bool { return !published(b, "copy/s") })
				p.Close()
				select {
				case err := <-done:
					t.Fatal("OnClose again", err)
				default:
				}
			})
		}
	}
}

func TestPullCodecChangeAndRepublish(t *testing.T) {
	a := hub.New(hub.Config{})
	defer a.Close()
	b := hub.New(hub.Config{})
	defer b.Close()
	addr := origin(t, Config{Hub: a, Wait: testTimeout}, nil, nil)
	p, done := pull(t, PullConfig{}, "http://"+addr+"/live/s.flv", b, "copy")
	defer p.Close()
	rec := &recorder{}
	sub, err := b.Subscribe("copy", rec, hub.SubscribeOptions{Wait: testTimeout})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	waitFor(t, func() bool { return subscribers(a, "live/s") == 1 })
	tracks, packets := streamFile(t, "h264_aac.flv")
	hevc, hevcPackets := streamFile(t, "hevc_aac.flv")
	pub, err := a.Publish("live/s", tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	for _, pkt := range packets {
		pub.WritePacket(pkt)
	}
	waitFor(t, func() bool { return rec.count() == len(packets) })
	n, lastDTS := rec.last()
	// The codec changes mid-stream: the pulled stream's tracks follow, and
	// its clock carries on.
	pub.SetTracks(hevc)
	shift := packets[len(packets)-1].DTS - packets[0].DTS + time.Second - hevcPackets[0].DTS
	for _, pkt := range hevcPackets {
		c := *pkt
		c.DTS += shift
		c.PTS += shift
		pub.WritePacket(&c)
	}
	waitFor(t, func() bool { return rec.count() == n+len(hevcPackets) })
	if sets := rec.trackSets(); len(sets) < 2 || sets[len(sets)-1][0].Codec != av.CodecH265 {
		t.Fatalf("%+v", sets)
	}
	rec.check(t, n, hevc, hevcPackets, lastDTS+time.Second)
	// The origin's publisher goes and comes back: the origin keeps its
	// player, so the pull goes on, with timestamps that keep increasing.
	pub.Close()
	pub, err = a.Publish("live/s", tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	n, lastDTS = rec.last()
	for _, pkt := range packets {
		pub.WritePacket(pkt)
	}
	waitFor(t, func() bool { return rec.count() == n+len(packets) })
	rec.mu.Lock()
	for _, pkt := range rec.packets[n:] {
		if pkt.DTS < lastDTS {
			t.Fatalf("timestamp went back: %v < %v", pkt.DTS, lastDTS)
		}
	}
	rec.mu.Unlock()
	if !published(b, "copy") {
		t.Fatal("pulled stream went")
	}
	p.Close()
	if err := done.wait(t); err != ErrClosed {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return subscribers(a, "live/s") == 0 })
	if published(b, "copy") {
		t.Fatal("still published")
	}
}

func TestPullCloseMidStream(t *testing.T) {
	a := hub.New(hub.Config{})
	defer a.Close()
	b := hub.New(hub.Config{})
	defer b.Close()
	addr := origin(t, Config{Hub: a}, nil, nil)
	tracks, packets := streamFile(t, "h264_aac.flv")
	pub, err := a.Publish("live/s", tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	var self atomic.Pointer[Puller]
	var count int
	p, done := pull(t, PullConfig{OnPacket: func(*av.Packet) {
		// Close from inside a callback: nothing is delivered after it.
		if count++; count == 3 {
			self.Load().Close()
		}
		if count > 3 {
			t.Error("packet after Close")
		}
	}}, "http://"+addr+"/live/s.flv", b, "copy")
	self.Store(p)
	waitFor(t, func() bool { return subscribers(a, "live/s") == 1 })
	for _, pkt := range packets {
		pub.WritePacket(pkt)
	}
	if err := done.wait(t); err != ErrClosed {
		t.Fatal(err)
	}
	if published(b, "copy") {
		t.Fatal("still published")
	}
	waitFor(t, func() bool { return subscribers(a, "live/s") == 0 })
	// Closed from outside, mid-stream, over WebSocket-FLV.
	p, done = pull(t, PullConfig{}, "ws://"+addr+"/live/s.flv", b, "copy")
	waitFor(t, func() bool { return published(b, "copy") })
	p.Close()
	if err := done.wait(t); err != ErrClosed {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return subscribers(a, "live/s") == 0 })
}

func TestPullFailures(t *testing.T) {
	a := hub.New(hub.Config{})
	defer a.Close()
	b := hub.New(hub.Config{})
	defer b.Close()
	addr := origin(t, Config{Hub: a}, nil, func(r *fibhttp.Router) {
		r.Get("/moved.flv", func(c *fibhttp.Context) {
			c.Header().Set("Location", "/live/s.flv?k=v")
			c.WriteHeader(http.StatusFound)
		})
		r.Get("/loop.flv", func(c *fibhttp.Context) {
			c.Header().Set("Location", "/loop.flv")
			c.WriteHeader(http.StatusTemporaryRedirect)
		})
		r.Get("/badloc.flv", func(c *fibhttp.Context) {
			c.Header().Set("Location", "%zz")
			c.WriteHeader(http.StatusFound)
		})
		r.Get("/garbage.flv", func(c *fibhttp.Context) {
			c.WriteHeader(http.StatusOK)
			_, _ = c.Write([]byte("this is not an FLV stream"))
		})
	})
	e := run(t)
	expect := func(rawURL string, config PullConfig, want func(error) bool) {
		t.Helper()
		config.Engine = e
		_, done := pull(t, config, rawURL, b, "copy")
		if err := done.wait(t); !want(err) {
			t.Fatalf("%s: %v", rawURL, err)
		}
		if published(b, "copy") {
			t.Fatalf("%s: left published", rawURL)
		}
	}
	base := "http://" + addr
	// 404: no stream.
	expect(base+"/live/none.flv", PullConfig{}, func(err error) bool {
		var se *StatusError
		return errors.As(err, &se) && se.StatusCode == 404 && se.Body != "" && strings.Contains(se.Error(), "404")
	})
	expect("ws://"+addr+"/none", PullConfig{}, func(err error) bool {
		var se *StatusError
		return errors.As(err, &se) && se.StatusCode == 404
	})
	// A WebSocket-FLV origin without the stream closes with 1008.
	expect("ws://"+addr+"/live/none.flv", PullConfig{}, func(err error) bool {
		return err != nil && strings.Contains(err.Error(), "1008")
	})
	// Redirects are followed, up to MaxRedirects.
	expect(base+"/loop.flv", PullConfig{}, func(err error) bool { return err == ErrTooManyRedirects })
	expect(base+"/moved.flv", PullConfig{MaxRedirects: -1}, func(err error) bool { return err == ErrTooManyRedirects })
	expect(base+"/badloc.flv", PullConfig{}, func(err error) bool { return err != nil })
	// Not FLV.
	expect(base+"/garbage.flv", PullConfig{}, func(err error) bool { return err == flv.ErrHeader })
	// Nothing listening.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := ln.Addr().String()
	_ = ln.Close()
	expect("http://"+dead+"/live/s.flv", PullConfig{}, func(err error) bool { return err != nil })
	expect("ws://"+dead+"/live/s.flv", PullConfig{}, func(err error) bool { return err != nil })
	// An origin that never answers.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	expect("http://"+ln.Addr().String()+"/live/s.flv", PullConfig{HeaderTimeout: 100 * time.Millisecond},
		func(err error) bool { return errors.Is(err, fibhttp.ErrResponseHeaderTimeout) })
	// The hub refuses the path: it has a publisher.
	busy, _ := b.Publish("copy", nil, hub.PublishInfo{})
	tracks, packets := streamFile(t, "h264_aac.flv")
	pub, _ := a.Publish("live/s", tracks, hub.PublishInfo{})
	defer pub.Close()
	_, done := pull(t, PullConfig{Engine: e}, base+"/moved.flv", b, "copy")
	if err := done.wait(t); err != hub.ErrStreamBusy {
		t.Fatal(err)
	}
	_, done = pull(t, PullConfig{Engine: e}, "ws://"+addr+"/live/s.flv", b, "copy")
	if err := done.wait(t); err != hub.ErrStreamBusy {
		t.Fatal(err)
	}
	busy.Close()
	waitFor(t, func() bool { return subscribers(a, "live/s") == 0 })
	// Followed to the stream, which is pulled without a hub.
	var got int
	var mu sync.Mutex
	p, done := pull(t, PullConfig{Engine: e, OnTracks: func([]*av.Track) {}, OnPacket: func(*av.Packet) {
		mu.Lock()
		got++
		mu.Unlock()
	}}, base+"/moved.flv", nil, "")
	waitFor(t, func() bool { return subscribers(a, "live/s") == 1 })
	for _, pkt := range packets {
		pub.WritePacket(pkt)
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return got == len(packets) })
	p.Close()
	if err := done.wait(t); err != ErrClosed {
		t.Fatal(err)
	}
}

func TestPullIdle(t *testing.T) {
	a := hub.New(hub.Config{})
	defer a.Close()
	b := hub.New(hub.Config{})
	defer b.Close()
	addr := origin(t, Config{Hub: a, Wait: testTimeout}, nil, nil)
	tracks, packets := streamFile(t, "h264_aac.flv")
	pub, err := a.Publish("live/s", tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	_, done := pull(t, PullConfig{IdleTimeout: 150 * time.Millisecond}, "http://"+addr+"/live/s.flv", b, "copy")
	waitFor(t, func() bool { return subscribers(a, "live/s") == 1 })
	// Packets keep coming for longer than the idle timeout, which then
	// ends the pull once they stop.
	start := time.Now()
	for _, pkt := range packets {
		pub.WritePacket(pkt)
		time.Sleep(10 * time.Millisecond)
		if time.Since(start) > 400*time.Millisecond {
			break
		}
	}
	select {
	case err := <-done:
		t.Fatal("ended while the origin sent", err)
	default:
	}
	if err := done.wait(t); err != ErrIdleTimeout {
		t.Fatal(err)
	}
	if published(b, "copy") {
		t.Fatal("still published")
	}
	// An origin waiting for its publisher sends nothing past its header.
	_, done = pull(t, PullConfig{IdleTimeout: 100 * time.Millisecond}, "ws://"+addr+"/live/none.flv", b, "copy")
	if err := done.wait(t); err != ErrIdleTimeout {
		t.Fatal(err)
	}
}

func TestPullArguments(t *testing.T) {
	e := run(t)
	for _, c := range []struct {
		config PullConfig
		url    string
	}{
		{PullConfig{}, "http://127.0.0.1:1/s.flv"},
		{PullConfig{Engine: e}, "rtmp://127.0.0.1/s"},
		{PullConfig{Engine: e}, "http://[::1"},
	} {
		if _, err := Pull(c.config, c.url, nil, ""); err == nil {
			t.Errorf("%s: no error", c.url)
		}
	}
}

func TestPullLateCallbacks(t *testing.T) {
	// An origin that accepts and never answers, so that nothing but the
	// calls below ends the pulls.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var conns []net.Conn
	var mu sync.Mutex
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	defer func() {
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
	}()
	e := run(t)
	// The origin's WebSocket closing with an error ends the pull with it.
	broken := errors.New("broken")
	p, done := pull(t, PullConfig{Engine: e}, "ws://"+ln.Addr().String()+"/s.flv", nil, "")
	w := wsPull{p}
	w.OnOpen(nil, nil)
	w.OnClose(nil, 0, "", broken)
	if err := done.wait(t); err != broken {
		t.Fatal(err)
	}
	// Once the pull has ended, what the client still delivers is dropped.
	w.OnClose(nil, 0, "", nil)
	w.OnMessage(nil, websocket.Binary, []byte("FLV"))
	p.onWebSocket(nil, nil, nil)
	// Closed before the origin answered: OnClose hears ErrClosed, once.
	p, done = pull(t, PullConfig{Engine: e}, "http://"+ln.Addr().String()+"/s.flv", nil, "")
	p.Close()
	p.Close()
	if err := done.wait(t); err != ErrClosed {
		t.Fatal(err)
	}
	p.onResponse(&fibhttp.ClientResponse{Response: &http.Response{StatusCode: 200}}, nil)
	p.request(p.url)
	select {
	case err := <-done:
		t.Fatal("OnClose again", err)
	default:
	}
}

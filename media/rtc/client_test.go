//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package rtc

import (
	"errors"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
)

func TestClientConfigErrors(t *testing.T) {
	ev := newEnv(t, nil)
	for _, u := range []string{"::", "rtsp://x/y"} {
		if _, err := Pull(ev.clientConfig(), u); err == nil {
			t.Fatalf("%q accepted", u)
		}
	}
	if _, err := Push(ClientConfig{}, "http://x/y", nil); err == nil {
		t.Fatal("no engine accepted")
	}
	hc := fibhttp.NewClient(ev.engine, fibhttp.DefaultClientConfig())
	defer hc.Close()
	if _, err := Pull(ClientConfig{HTTPClient: hc}, "http://x/y"); err == nil {
		t.Fatal("no PeerConnection engine accepted")
	}
	if _, err := PushFromHub(ClientConfig{}, "http://x/y", ev.hub, "x", hub.SubscribeOptions{}); err == nil {
		t.Fatal("no engine accepted")
	}
	if _, err := PushFromHub(ev.clientConfig(), ev.base+"/whip/x", ev.hub, "none", hub.SubscribeOptions{}); !errors.Is(err, hub.ErrNotFound) {
		t.Fatalf("missing stream: %v", err)
	}
}

// closedWith runs a client to its end and returns why it ended.
func closedWith(t *testing.T, start func(cfg ClientConfig) (*Client, error), cfg ClientConfig) (*Client, error) {
	t.Helper()
	closed := make(chan error, 1)
	cfg.OnClose = func(err error) { closed <- err }
	c, err := start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		return c, err
	case <-time.After(waitTimeout):
		t.Fatal("client did not end")
		return nil, nil
	}
}

func TestClientFailures(t *testing.T) {
	ev := newEnvWith(t, nil, func(r *fibhttp.Router) {
		r.Post("/nolocation", func(c *fibhttp.Context) { _ = c.Respond(201, contentTypeSDP, []byte("v=0")) })
		r.Post("/badanswer", func(c *fibhttp.Context) {
			reply(c, 201, contentTypeSDP, []byte("garbage"))
		})
		r.Post("/silent", func(c *fibhttp.Context) { c.Retain() })
	})
	m := loadTS(t, "testdata/h264_opus.ts")
	pull := func(u string) func(ClientConfig) (*Client, error) {
		return func(cfg ClientConfig) (*Client, error) { return Pull(cfg, ev.base+u) }
	}
	push := func(u string, tracks []*av.Track) func(ClientConfig) (*Client, error) {
		return func(cfg ClientConfig) (*Client, error) { return Push(cfg, ev.base+u, tracks) }
	}
	var se *StatusError
	if _, err := closedWith(t, pull("/whep/none"), ev.clientConfig()); !errors.As(err, &se) || se.StatusCode != 404 {
		t.Fatalf("404: %v", err)
	}
	if _, err := closedWith(t, pull("/nolocation"), ev.clientConfig()); err == nil || se.Error() == err.Error() {
		t.Fatalf("no location: %v", err)
	}
	cfg := ev.clientConfig()
	cfg.Header = map[string][]string{"X-Test": {"1"}}
	cfg.Token = "t"
	if _, err := closedWith(t, pull("/badanswer"), cfg); err == nil {
		t.Fatal("bad answer accepted")
	}
	if _, err := closedWith(t, push("/whip/x", []*av.Track{{Codec: av.CodecAAC}}), ev.clientConfig()); !errors.Is(err, ErrNoMedia) {
		t.Fatalf("aac push: %v", err)
	}
	if _, err := closedWith(t, push("/whep/none", m.tracks), ev.clientConfig()); !errors.As(err, &se) || se.StatusCode != 404 {
		t.Fatalf("push: %v", err)
	}
	// A server that never answers, through a client of the caller's own
	// without a timeout of its own.
	hc := fibhttp.NewClient(ev.engine, fibhttp.DefaultClientConfig())
	defer hc.Close()
	cfg = ev.clientConfig()
	cfg.HTTPClient, cfg.Timeout = hc, 200*time.Millisecond
	if _, err := closedWith(t, pull("/silent"), cfg); !errors.Is(err, ErrTimeout) {
		t.Fatalf("silent: %v", err)
	}
	// An engine-less PeerConnection configuration that cannot be used.
	cfg = ev.clientConfig()
	cfg.WebRTC.ListenAddr = "256.0.0.1:0"
	if _, err := closedWith(t, pull("/whep/x"), cfg); err == nil {
		t.Fatal("bad listen address accepted")
	}
}

func TestClientAPI(t *testing.T) {
	ev := newEnv(t, nil)
	m := loadTS(t, "testdata/h264_opus.ts")
	ev.publish(t, "live", m)
	col := newCollector()
	pull, err := Pull(col.config(ev.clientConfig()), ev.base+"/whep/live")
	if err != nil {
		t.Fatal(err)
	}
	if err := pull.WritePacket(m.packets[0]); !errors.Is(err, ErrNotPushing) {
		t.Fatalf("pull write: %v", err)
	}
	pull.SetTracks(m.tracks)
	col.wait(t, "packets", func(_ []*av.Track, packets []*av.Packet) bool { return len(packets) > 5 })
	if pull.PeerConnection() == nil || len(pull.Tracks()) != 2 {
		t.Fatal("pull state")
	}
	pull.Close()
	pull.Close()

	push, err := Push(ClientConfig{Engine: ev.engine}, ev.base+"/whip/cam", m.tracks)
	if err != nil {
		t.Fatal(err)
	}
	if push.Tracks() == nil {
		t.Fatal("push state")
	}
	push.SetTracks(m.tracks)
	push.RequestKeyFrame()
	push.Close()
	if err := push.WritePacket(m.packets[0]); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("closed write: %v", err)
	}
}

// TestRelayEnds covers PullToHub refused by the hub, and PushFromHub
// waiting for a stream that never comes.
func TestRelayEnds(t *testing.T) {
	ev := newEnv(t, nil)
	m := loadTS(t, "testdata/h264_opus.ts")
	ev.publish(t, "live", m)
	if _, err := ev.hub.Publish("busy", m.tracks, hub.PublishInfo{}); err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	cfg := ev.clientConfig()
	cfg.OnClose = func(err error) { closed <- err }
	if _, err := PullToHub(cfg, ev.base+"/whep/live", ev.hub, "busy"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if !errors.Is(err, hub.ErrStreamBusy) {
			t.Fatalf("closed with %v", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("refused relay not closed")
	}
	if _, err := PullToHub(ClientConfig{}, "x", ev.hub, "y"); err == nil {
		t.Fatal("bad pull accepted")
	}

	p, err := PushFromHub(ev.clientConfig(), ev.base+"/whip/out", ev.hub, "later", hub.SubscribeOptions{Wait: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	p.Close()

	// A stream that ends ends the push, and its session on the server.
	pub, err := ev.hub.Publish("short", m.tracks, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	cfg = ev.clientConfig()
	ready := make(chan struct{})
	cfg.OnReady = func() { close(ready) }
	cfg.OnClose = func(err error) { closed <- err }
	if _, err := PushFromHub(cfg, ev.base+"/whip/short-out", ev.hub, "short", hub.SubscribeOptions{}); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	go feed(m, pub.WritePacket, stop)
	select {
	case <-ready:
	case <-time.After(waitTimeout):
		t.Fatal("push not connected")
	}
	close(stop)
	pub.Close()
	select {
	case err := <-closed:
		if !errors.Is(err, hub.ErrUnpublished) {
			t.Fatalf("closed with %v", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("push not closed")
	}
	waitFor(t, "the pushed stream to go", func() bool {
		for _, s := range ev.hub.Streams() {
			if s.Path == "short-out" {
				return false
			}
		}
		return true
	})
}

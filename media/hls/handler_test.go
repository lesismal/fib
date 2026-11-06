//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package hls

import (
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/http"
	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
)

// serve starts a fib/http server for h and returns its base URL.
func serve(t *testing.T, h *Handler) string {
	t.Helper()
	return serveHandler(t, h, h)
}

// serveHandler serves handler, which h is behind, and returns its base
// URL.
func serveHandler(t *testing.T, h *Handler, handler http.Handler) string {
	t.Helper()
	config := fib.DefaultConfig()
	config.ReusePort = false
	config.LogSettings = false
	engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:0", Handler: http.NewHandler(handler)})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := engine.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() {
		h.Close()
		engine.Stop()
		<-done
		_ = engine.Close()
	})
	return "http://" + addr.String()
}

var client = &stdhttp.Client{Timeout: 10 * time.Second}

// start requests url in the background, which starts the Muxer of path,
// and returns once it runs, with the request's status to come.
func start(t *testing.T, h *Handler, url, path string) <-chan int {
	t.Helper()
	got := make(chan int, 1)
	go func() {
		resp, err := client.Get(url)
		if err != nil {
			got <- 0
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		got <- resp.StatusCode
	}()
	deadline := time.Now().Add(5 * time.Second)
	for h.Muxer(path) == nil {
		if time.Now().After(deadline) {
			t.Fatal("no muxer for", path)
		}
		time.Sleep(time.Millisecond)
	}
	return got
}

func get(t *testing.T, method, url string) (*stdhttp.Response, string) {
	t.Helper()
	req, _ := stdhttp.NewRequest(method, url, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(b)
}

func statusOf(t *testing.T, url string) int {
	t.Helper()
	resp, _ := get(t, "GET", url)
	return resp.StatusCode
}

// publish publishes an H.264+AAC stream at path and returns a func writing
// its packets up to a time.
func publish(t *testing.T, h *hub.Hub, path string, tracks []*av.Track) (*hub.Publisher, func(time.Duration)) {
	t.Helper()
	pub, err := h.Publish(path, tracks, hub.PublishInfo{Protocol: "test"})
	if err != nil {
		t.Fatal(err)
	}
	src := &source{video: true, audio: 1, gop: 50}
	if len(tracks) == 1 {
		src = &source{audio: 0}
	}
	return pub, func(to time.Duration) {
		for _, p := range src.until(to) {
			pub.WritePacket(p)
		}
	}
}

func TestHandler(t *testing.T) {
	hb := hub.New(hub.Config{})
	defer hb.Close()
	h := NewHandler(HandlerConfig{Hub: hb, Prefix: "/hls", AllowOrigin: "*"})
	base := serve(t, h) + "/hls/"
	pub, write := publish(t, hb, "live/s", []*av.Track{h264Track(t), aacTrack(t)})
	defer pub.Close()

	for _, path := range []string{"nope/index.m3u8", "index.m3u8", "live/s/seg-x-1.m4s", "../x"} {
		if code := statusOf(t, base+path); code != 404 {
			t.Fatal(path, code)
		}
	}
	if code := statusOf(t, strings.TrimSuffix(base, "hls/")+"other/live/s/index.m3u8"); code != 404 {
		t.Fatal(code)
	}
	if resp, _ := get(t, "POST", base+"live/s/index.m3u8"); resp.StatusCode != 405 {
		t.Fatal(resp.StatusCode)
	}
	if resp, _ := get(t, "OPTIONS", base+"live/s/index.m3u8"); resp.StatusCode != 204 || resp.Header.Get("Access-Control-Allow-Methods") == "" {
		t.Fatal(resp.StatusCode, resp.Header)
	}
	// A file of a stream with no muxer running is 404, and starts none.
	if code := statusOf(t, base+"live/s/init-x-1.mp4"); code != 404 || h.Muxer("live/s") != nil {
		t.Fatal(code)
	}

	// The playlist waits for the first segment.
	type result struct {
		resp *stdhttp.Response
		body string
	}
	got := make(chan result, 1)
	go func() {
		resp, body := get(t, "GET", base+"live/s/index.m3u8")
		got <- result{resp, body}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for h.Muxer("live/s") == nil {
		if time.Now().After(deadline) {
			t.Fatal("no muxer")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case r := <-got:
		t.Fatal("answered early", r.resp.StatusCode)
	case <-time.After(50 * time.Millisecond):
	}
	write(5 * time.Second)
	r := <-got
	if r.resp.StatusCode != 200 || r.resp.Header.Get("Content-Type") != PlaylistContentType || r.resp.Header.Get("Cache-Control") != "no-cache" || r.resp.Header.Get("Access-Control-Allow-Origin") != "*" || !strings.Contains(r.body, "#EXT-X-STREAM-INF:") {
		t.Fatal(r.resp.StatusCode, r.resp.Header, r.body)
	}
	resp, body := get(t, "GET", base+"live/s/media.m3u8")
	p := parsePlaylist(t, []byte(body))
	if resp.StatusCode != 200 || len(p.segs) != 2 || len(p.maps) != 1 {
		t.Fatal(body)
	}
	for _, name := range append(p.maps, p.segs...) {
		resp, data := get(t, "GET", base+"live/s/"+name)
		if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "video/mp4" || resp.Header.Get("Cache-Control") != "max-age=3600" || len(data) == 0 {
			t.Fatal(name, resp.StatusCode, resp.Header)
		}
	}
	if resp, body := get(t, "HEAD", base+"live/s/"+p.segs[0]); resp.StatusCode != 200 || body != "" || resp.ContentLength <= 0 {
		t.Fatal(resp.StatusCode, resp.ContentLength, len(body))
	}
	// Without Low-Latency HLS blocking parameters are ignored, though
	// checked.
	if resp, body := get(t, "GET", base+"live/s/media.m3u8?_HLS_msn=1"); resp.StatusCode != 200 || !strings.Contains(body, "#EXTINF") {
		t.Fatal(resp.StatusCode)
	}
	for _, q := range []string{"_HLS_msn=x", "_HLS_part=1", "_HLS_msn=1&_HLS_part=-1", "_HLS_msn=-2"} {
		if code := statusOf(t, base+"live/s/media.m3u8?"+q); code != 400 {
			t.Fatal(q, code)
		}
	}
	// The next segment is worth waiting for.
	next := strings.Replace(p.segs[1], "-1.m4s", "-2.m4s", 1)
	got2 := make(chan int, 1)
	go func() { got2 <- statusOf(t, base+"live/s/"+next) }()
	time.Sleep(50 * time.Millisecond)
	write(7 * time.Second)
	if code := <-got2; code != 200 {
		t.Fatal(code)
	}
	// One further on is not.
	if code := statusOf(t, base+"live/s/"+strings.Replace(next, "-2.m4s", "-9.m4s", 1)); code != 404 {
		t.Fatal(code)
	}
}

func TestHandlerLLHLS(t *testing.T) {
	hb := hub.New(hub.Config{})
	defer hb.Close()
	h := NewHandler(HandlerConfig{Hub: hb, Muxer: Config{PartDuration: 500 * time.Millisecond}, WaitTimeout: 300 * time.Millisecond})
	base := serve(t, h) + "/"
	pub, write := publish(t, hb, "s", []*av.Track{aacTrack(t)})
	defer pub.Close()
	// A playlist waiting for its first segment times out.
	if code := <-start(t, h, base+"s/index.m3u8", "s"); code != 503 {
		t.Fatal(code)
	}
	write(4100 * time.Millisecond)
	resp, body := get(t, "GET", base+"s/index.m3u8")
	p := parsePlaylist(t, []byte(body))
	if resp.StatusCode != 200 || p.hint == "" || len(p.segs) != 2 {
		t.Fatal(resp.StatusCode, body)
	}
	// Too far ahead.
	if code := statusOf(t, base+"s/index.m3u8?_HLS_msn=4"); code != 400 {
		t.Fatal(code)
	}
	// A blocking reload for the next part waits for it, and so does a
	// request for the part the preload hint names.
	got := make(chan string, 1)
	go func() {
		_, body := get(t, "GET", base+"s/index.m3u8?_HLS_msn=2&_HLS_part=0")
		got <- body
	}()
	hinted := make(chan int, 1)
	go func() { hinted <- statusOf(t, base+"s/"+p.hint) }()
	deadline := time.Now().Add(5 * time.Second)
	m := h.Muxer("s")
	for {
		m.mu.Lock()
		n := len(m.waiters)
		m.mu.Unlock()
		if n == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("not waiting")
		}
		time.Sleep(time.Millisecond)
	}
	write(4700 * time.Millisecond)
	if body := <-got; !strings.Contains(body, p.hint) || strings.Contains(body, "PRELOAD-HINT:TYPE=PART,URI=\""+p.hint) {
		t.Fatal(body)
	}
	if code := <-hinted; code != 200 {
		t.Fatal(code)
	}
	// A blocking reload that times out gets the playlist as it is.
	if resp, body := get(t, "GET", base+"s/index.m3u8?_HLS_msn=3"); resp.StatusCode != 200 || !strings.Contains(body, "#EXTINF") {
		t.Fatal(resp.StatusCode)
	}
	// A hinted part that does not come in time is 404.
	_, body = get(t, "GET", base+"s/index.m3u8")
	p = parsePlaylist(t, []byte(body))
	if code := statusOf(t, base+"s/"+p.hint); code != 404 {
		t.Fatal(code)
	}
}

func TestHandlerLimits(t *testing.T) {
	hb := hub.New(hub.Config{})
	defer hb.Close()
	h := NewHandler(HandlerConfig{Hub: hb, MaxStreams: 1, IdleTimeout: 200 * time.Millisecond, RepublishWait: -1})
	base := serve(t, h) + "/"
	pub1, write1 := publish(t, hb, "a", []*av.Track{h264Track(t), aacTrack(t)})
	pub2, _ := publish(t, hb, "b", []*av.Track{h264Track(t), aacTrack(t)})
	defer pub2.Close()
	got := start(t, h, base+"a/media.m3u8", "a")
	write1(4100 * time.Millisecond)
	if code := <-got; code != 200 {
		t.Fatal(code)
	}
	// One stream at a time.
	if code := statusOf(t, base+"b/index.m3u8"); code != 503 {
		t.Fatal(code)
	}
	// a's Muxer stops once idle.
	deadline := time.Now().Add(5 * time.Second)
	for h.Muxer("a") != nil {
		if time.Now().After(deadline) {
			t.Fatal("not stopped")
		}
		time.Sleep(10 * time.Millisecond)
	}
	got = start(t, h, base+"a/media.m3u8", "a")
	write1(6100 * time.Millisecond)
	if code := <-got; code != 200 {
		t.Fatal(code)
	}
	// A playlist whose publisher left ends; a new publisher starts it over.
	m := h.Muxer("a")
	pub1.Close()
	if !m.Ended() {
		t.Fatal("not ended")
	}
	if _, body := get(t, "GET", base+"a/media.m3u8"); !strings.Contains(body, "#EXT-X-ENDLIST") {
		t.Fatal(body)
	}
	pub3, write3 := publish(t, hb, "a", []*av.Track{h264Track(t), aacTrack(t)})
	defer pub3.Close()
	got2 := make(chan string, 1)
	go func() {
		_, body := get(t, "GET", base+"a/media.m3u8")
		got2 <- body
	}()
	for h.Muxer("a") == m || h.Muxer("a") == nil {
		if time.Now().After(deadline) {
			t.Fatal("not restarted")
		}
		time.Sleep(time.Millisecond)
	}
	write3(2100 * time.Millisecond)
	if body := <-got2; strings.Contains(body, "#EXT-X-ENDLIST") || !strings.Contains(body, "#EXTINF") {
		t.Fatal(body)
	}
	// A closed Handler serves nothing.
	h.Close()
	if code := statusOf(t, base+"a/media.m3u8"); code != 404 {
		t.Fatal(code)
	}
}

func TestHandlerWaiters(t *testing.T) {
	hb := hub.New(hub.Config{OnSubscribe: func(path string, info hub.SubscribeInfo) error {
		if path == "denied" {
			return errors.New("no")
		}
		return nil
	}})
	defer hb.Close()
	h := NewHandler(HandlerConfig{Hub: hb, Muxer: Config{MaxWaiters: 1, PartDuration: time.Second}, WaitTimeout: time.Second})
	base := serve(t, h) + "/"
	pubD, _ := publish(t, hb, "denied", []*av.Track{aacTrack(t)})
	defer pubD.Close()
	if code := statusOf(t, base+"denied/index.m3u8"); code != 404 {
		t.Fatal(code)
	}
	pub, write := publish(t, hb, "w", []*av.Track{aacTrack(t)})
	defer pub.Close()
	got := start(t, h, base+"w/index.m3u8", "w")
	write(2100 * time.Millisecond)
	if code := <-got; code != 200 {
		t.Fatal(code)
	}
	// A client that hangs up while waiting is let go.
	conn, err := net.Dial("tcp", strings.TrimPrefix(strings.TrimSuffix(base, "/"), "http://"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = conn.Write([]byte("GET /w/index.m3u8?_HLS_msn=1 HTTP/1.1\r\nHost: x\r\n\r\n"))
	m := h.Muxer("w")
	deadline := time.Now().Add(5 * time.Second)
	for !m.waiting() {
		if time.Now().After(deadline) {
			t.Fatal("not waiting")
		}
		time.Sleep(time.Millisecond)
	}
	// The one waiter allowed is taken: another is refused.
	if code := statusOf(t, base+"w/index.m3u8?_HLS_msn=1&_HLS_part=0"); code != 503 {
		t.Fatal(code)
	}
	conn.Close()
	for m.waiting() {
		if time.Now().After(deadline) {
			t.Fatal("waiter kept")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package hls

import (
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
)

// TestHandlerFormats plays one stream as fMP4 (the default) and as MPEG-TS
// (?format=ts) at once.
func TestHandlerFormats(t *testing.T) {
	hb := hub.New(hub.Config{})
	defer hb.Close()
	h := NewHandler(HandlerConfig{Hub: hb, Formats: []Format{MPEGTS}})
	base := serve(t, h) + "/"
	pub, err := hb.Publish("live/s", []*av.Track{h264Track(t), aacTrack(t)}, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	src := &source{video: true, audio: 1, gop: 50}
	write := func(to time.Duration) {
		for _, p := range avcc(src.until(to)) {
			pub.WritePacket(p)
		}
	}
	if code := statusOf(t, base+"live/s/index.m3u8?format=nope"); code != 404 {
		t.Fatal(code)
	}
	if h.FormatMuxer("live/s", "nope") != nil {
		t.Fatal("muxer of an unknown format")
	}
	gotTS := make(chan int, 1)
	go func() { gotTS <- statusOf(t, base+"live/s/index.m3u8?format=ts") }()
	deadline := time.Now().Add(5 * time.Second)
	for h.FormatMuxer("live/s", "ts") == nil {
		if time.Now().After(deadline) {
			t.Fatal("no TS muxer")
		}
		time.Sleep(time.Millisecond)
	}
	gotMP4 := start(t, h, base+"live/s/index.m3u8", "live/s")
	write(4100 * time.Millisecond)
	if a, b := <-gotTS, <-gotMP4; a != 200 || b != 200 {
		t.Fatal(a, b)
	}
	if h.FormatMuxer("live/s", "ts") == h.Muxer("live/s") || h.FormatMuxer("live/s", "fmp4") != h.Muxer("live/s") {
		t.Fatal("muxers")
	}

	// The multivariant playlist carries the format on to the media
	// playlist.
	_, body := get(t, "GET", base+"live/s/index.m3u8?format=ts")
	if !strings.Contains(body, "#EXT-X-VERSION:3\n") || !strings.Contains(body, "\nmedia.m3u8?format=ts\n") {
		t.Fatal(body)
	}
	_, body = get(t, "GET", base+"live/s/media.m3u8?format=ts")
	p := parsePlaylist(t, []byte(body))
	if len(p.segs) != 2 || len(p.maps) != 0 || !strings.HasSuffix(p.segs[0], ".ts") {
		t.Fatal(body)
	}
	for _, name := range p.segs {
		resp, data := get(t, "GET", base+"live/s/"+name)
		if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "video/mp2t" || len(data) == 0 || data[0] != 0x47 {
			t.Fatal(name, resp.StatusCode, resp.Header)
		}
	}
	// The default format is served alongside, its files found by name.
	_, body = get(t, "GET", base+"live/s/media.m3u8")
	p = parsePlaylist(t, []byte(body))
	if len(p.maps) != 1 || !strings.HasSuffix(p.segs[0], ".m4s") {
		t.Fatal(body)
	}
	if resp, _ := get(t, "GET", base+"live/s/"+p.segs[0]); resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "video/mp4" {
		t.Fatal(resp.StatusCode, resp.Header)
	}
}

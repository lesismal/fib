//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package hls

import (
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
)

// until polls cond for up to 5 s.
func until(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHandlerPlayWait(t *testing.T) {
	hb := hub.New(hub.Config{})
	defer hb.Close()
	h := NewHandler(HandlerConfig{Hub: hb, PlayWait: time.Hour})
	base := serve(t, h) + "/"

	// A request for a stream not yet published waits for it, and is
	// answered with its first segment.
	got := start(t, h, base+"live/s/index.m3u8", "live/s")
	m := h.Muxer("live/s")
	until(t, "not waiting", m.waiting)
	if n := h.playWaiters.Load(); n != 1 {
		t.Fatal(n)
	}
	pub, write := publish(t, hb, "live/s", []*av.Track{h264Track(t), aacTrack(t)})
	defer pub.Close()
	write(2100 * time.Millisecond)
	if code := <-got; code != 200 {
		t.Fatal(code)
	}
	if h.Muxer("live/s") != m {
		t.Fatal("muxer replaced")
	}
	until(t, "waiter kept", func() bool { return h.playWaiters.Load() == 0 })
	// A stream already published is served as before.
	if code := statusOf(t, base+"live/s/media.m3u8"); code != 200 {
		t.Fatal(code)
	}
}

func TestHandlerPlayWaitExpires(t *testing.T) {
	hb := hub.New(hub.Config{})
	defer hb.Close()
	h := NewHandler(HandlerConfig{Hub: hb, PlayWait: 50 * time.Millisecond, WaitTimeout: time.Hour})
	base := serve(t, h) + "/"

	// No publisher within PlayWait: 404, well before WaitTimeout.
	if code := statusOf(t, base+"s/index.m3u8"); code != 404 {
		t.Fatal(code)
	}
	m := h.Muxer("s")
	if m == nil || !m.Ended() {
		t.Fatal("wait did not end the muxer", m)
	}
	// A waiter is let go once it has been answered, so its count falls just
	// after the answer arrives.
	until(t, "the waiter not let go", func() bool { return h.playWaiters.Load() == 0 })
	// The next request waits again, on a new Muxer.
	got := start(t, h, base+"s/media.m3u8", "s")
	until(t, "not restarted", func() bool { return h.Muxer("s") != m })
	if code := <-got; code != 404 {
		t.Fatal(code)
	}
	// A segment request has nothing to wait for.
	if code := statusOf(t, base+"x/seg-x-1.m4s"); code != 404 {
		t.Fatal(code)
	}
}

func TestHandlerPlayWaiters(t *testing.T) {
	hb := hub.New(hub.Config{})
	defer hb.Close()
	h := NewHandler(HandlerConfig{Hub: hb, PlayWait: time.Hour, MaxPlayWaiters: 3})
	base := serve(t, h) + "/"
	addr := strings.TrimPrefix(strings.TrimSuffix(base, "/"), "http://")

	// Waiters on several streams, up to the bound.
	var conns []net.Conn
	for i := range 3 {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, conn)
		_, _ = conn.Write([]byte("GET /s" + strconv.Itoa(i%2) + "/index.m3u8 HTTP/1.1\r\nHost: x\r\n\r\n"))
		until(t, "not waiting", func() bool { return h.playWaiters.Load() == int64(i+1) })
	}
	// One more is refused, published or not; one for a live stream is not
	// a waiter.
	if code := statusOf(t, base+"s0/index.m3u8"); code != 503 {
		t.Fatal(code)
	}
	if code := statusOf(t, base+"s9/index.m3u8"); code != 503 || h.Muxer("s9") != nil {
		t.Fatal(code)
	}
	pub, write := publish(t, hb, "live", []*av.Track{aacTrack(t)})
	defer pub.Close()
	got := start(t, h, base+"live/index.m3u8", "live")
	write(2100 * time.Millisecond)
	if code := <-got; code != 200 {
		t.Fatal(code)
	}
	// Clients that hang up while waiting are let go.
	for _, conn := range conns {
		conn.Close()
	}
	until(t, "waiters kept", func() bool { return h.playWaiters.Load() == 0 })
	for _, p := range []string{"s0", "s1"} {
		if m := h.Muxer(p); m == nil || m.waiting() {
			t.Fatal(p, m)
		}
	}
}

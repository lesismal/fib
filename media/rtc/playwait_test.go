//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package rtc

import (
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/media/av"
	"github.com/lesismal/fib/media/hub"
)

// TestWHEPPlayWait has a viewer wait for a stream published after its
// offer.
func TestWHEPPlayWait(t *testing.T) {
	ev := newEnv(t, func(c *Config) { c.PlayWait = time.Hour })
	m := loadTS(t, "testdata/h264_opus.ts")
	col := newCollector()
	pull, err := Pull(col.config(ev.clientConfig()), ev.base+"/whep/live/a")
	if err != nil {
		t.Fatal(err)
	}
	defer pull.Close()
	waitFor(t, "the offer to wait", func() bool { return ev.srv.playWaiters.Load() == 1 })
	ev.publish(t, "live/a", m)
	col.wait(t, "video and audio", func(tracks []*av.Track, packets []*av.Packet) bool {
		n, _ := counts(tracks, packets)
		return n[av.CodecH264] >= 10 && n[av.CodecOpus] >= 10
	})
	if n := ev.srv.playWaiters.Load(); n != 0 {
		t.Fatal(n)
	}
	// A stream already published is answered at once, as before.
	col2 := newCollector()
	pull2, err := Pull(col2.config(ev.clientConfig()), ev.base+"/whep/live/a")
	if err != nil {
		t.Fatal(err)
	}
	defer pull2.Close()
	col2.wait(t, "video", func(tracks []*av.Track, packets []*av.Packet) bool {
		n, _ := counts(tracks, packets)
		return n[av.CodecH264] >= 10
	})
}

// TestWHEPPlayWaitExpires answers 404 when no publisher comes in time.
func TestWHEPPlayWaitExpires(t *testing.T) {
	ev := newEnv(t, func(c *Config) { c.PlayWait = 50 * time.Millisecond; c.RepublishWait = time.Hour })
	_, play := ev.playOffer(t)
	if resp, body := ev.request(t, "POST", "/whep/none", "application/sdp", play, nil); resp.StatusCode != 404 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if ev.srv.Len() != 0 || ev.srv.playWaiters.Load() != 0 {
		t.Fatal(ev.srv.Len(), ev.srv.playWaiters.Load())
	}
	// A stream WebRTC cannot carry, published while the offer waits.
	ev = newEnv(t, func(c *Config) { c.PlayWait = time.Hour })
	got := make(chan int, 1)
	go func() {
		resp, _ := ev.request(t, "POST", "/whep/aac", "application/sdp", play, nil)
		got <- resp.StatusCode
	}()
	waitFor(t, "the offer to wait", func() bool { return ev.srv.playWaiters.Load() == 1 })
	pub, err := ev.hub.Publish("aac", []*av.Track{{Codec: av.CodecAAC, Config: []byte{0x12, 0x10}}}, hub.PublishInfo{})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if code := <-got; code != 406 {
		t.Fatal(code)
	}
	waitFor(t, "the session to go", func() bool { return ev.srv.Len() == 0 })
}

// TestWHEPPlayWaiters bounds the offers waiting, lets go of one whose
// client hangs up, and answers those waiting when the server closes.
func TestWHEPPlayWaiters(t *testing.T) {
	ev := newEnv(t, func(c *Config) { c.PlayWait = time.Hour; c.MaxPlayWaiters = 2 })
	_, play := ev.playOffer(t)
	addr := strings.TrimPrefix(ev.base, "http://")
	raw := "POST /whep/s HTTP/1.1\r\nHost: x\r\nContent-Type: application/sdp\r\nContent-Length: " +
		strconv.Itoa(len(play)) + "\r\n\r\n" + play
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte(raw)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the first offer to wait", func() bool { return ev.srv.playWaiters.Load() == 1 })
	got := make(chan int, 1)
	go func() {
		resp, _ := ev.request(t, "POST", "/whep/s2", "application/sdp", play, nil)
		got <- resp.StatusCode
	}()
	waitFor(t, "the second offer to wait", func() bool { return ev.srv.playWaiters.Load() == 2 })
	// One more is refused.
	if resp, _ := ev.request(t, "POST", "/whep/s3", "application/sdp", play, nil); resp.StatusCode != 503 {
		t.Fatal(resp.StatusCode)
	}
	if n := ev.srv.Len(); n != 2 {
		t.Fatal(n)
	}
	// The client that hangs up is let go, with its session.
	conn.Close()
	waitFor(t, "the waiter to go", func() bool { return ev.srv.playWaiters.Load() == 1 && ev.srv.Len() == 1 })
	// Closing the server answers the one left.
	ev.srv.Close()
	if code := <-got; code != 503 {
		t.Fatal(code)
	}
	if n := ev.srv.playWaiters.Load(); n != 0 {
		t.Fatal(n)
	}
}

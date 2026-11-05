package rtsp

import (
	"strings"
	"sync"
	"testing"

	"github.com/lesismal/fib/media/hub"
)

// TestSessionStreamSwap is a client that SETs UP one stream and PLAYs
// another, which a hook checking each request by its own URL would let
// through: the session's stream is played or nothing is.
func TestSessionStreamSwap(t *testing.T) {
	h := hub.New(hub.Config{})
	var mu sync.Mutex
	var seen []string
	base, _, _ := serve(t, ServerConfig{Hub: h, OnRequest: func(c *ServerConn, req *Request) *Response {
		p, ok := c.SessionPath()
		mu.Lock()
		seen = append(seen, req.Method+" "+p+" "+map[bool]string{true: "session", false: "none"}[ok])
		mu.Unlock()
		return nil
	}})
	for _, p := range []string{"a", "secret"} {
		pub := publishHub(t, h, p)
		defer pub.Close()
	}

	r := dialRaw(t, base)
	expect(t, r.do(MethodSetup, base+"secret/trackID=0", "Transport: RTP/AVP/TCP;unicast"), StatusOK)
	for _, u := range []string{
		base + "a",
		base + "a/",
		base + "a?token=T",
		base + "a?token=T/",
		base + "a/trackID=0",
		base + "a?token=T/trackID=0",
		base + "secret/../a",
		base + "secretx",
		base + "secret/trackID=0/x",
		base + "secret/trackID=x",
		base,
		"*",
	} {
		expect(t, r.do(MethodPlay, u), StatusMethodNotValidInState)
	}
	expect(t, r.do(MethodRecord, base+"a"), StatusMethodNotValidInState)
	expect(t, r.do(MethodPlay, base+"secret"), StatusOK)
	expect(t, r.do(MethodTeardown, base+"secret"), StatusOK)
	expect(t, r.do(MethodOptions, "*"), StatusOK)
	mu.Lock()
	defer mu.Unlock()
	if len(seen) < 4 || seen[0] != "SETUP  none" || seen[1] != "PLAY secret session" ||
		seen[len(seen)-2] != "TEARDOWN secret session" || seen[len(seen)-1] != "OPTIONS  none" {
		t.Fatal(seen)
	}
}

// TestSessionPathForms plays and records with the URL forms clients use
// for the aggregate and for each track: the description's Content-Base,
// with and without a query, ffmpeg's track URLs resolved against a URL
// with a query, and another host for the server.
func TestSessionPathForms(t *testing.T) {
	h := hub.New(hub.Config{})
	var paths []string
	var mu sync.Mutex
	base, _, _ := serve(t, ServerConfig{Hub: h, OnRequest: func(c *ServerConn, req *Request) *Response {
		if req.Method == MethodPlay || req.Method == MethodRecord {
			p, _ := c.SessionPath()
			mu.Lock()
			paths = append(paths, p)
			mu.Unlock()
		}
		return nil
	}})
	pub := publishHub(t, h, "live/s")
	defer pub.Close()
	for _, p := range testPackets(12) {
		pub.WritePacket(p)
	}
	other := strings.Replace(base, "127.0.0.1", "localhost", 1)

	for _, play := range [][]string{
		{base + "live/s", base + "live/s/"},
		{base + "live/s", base + "live/s"},
		{base + "live/s?token=T", base + "live/s?token=T"},
		{base + "live/s?token=T", base + "live/s?token=T/"},
		{base + "live/s?token=T", base + "live/s?token=T/trackID=0"},
		{base + "live/s", base + "live/s/trackID=1"},
		{base + "live/s", other + "live/s"},
		{base + "live/s/", base + "/live/s//"},
	} {
		r := dialRaw(t, base)
		resp := r.do(MethodDescribe, play[0])
		expect(t, resp, StatusOK)
		// Tracks are set up against the Content-Base, the DESCRIBE's URL
		// with a slash added.
		setupBase, playURL := resp.Header.Get("Content-Base"), play[1]
		if want := strings.TrimSuffix(play[0], "/") + "/"; setupBase != want {
			t.Fatalf("Content-Base %s, want %s", setupBase, want)
		}
		expect(t, r.do(MethodSetup, setupBase+"trackID=0", "Transport: RTP/AVP/TCP;unicast;interleaved=0-1"), StatusOK)
		expect(t, r.do(MethodSetup, setupBase+"trackID=1", "Transport: RTP/AVP/TCP;unicast;interleaved=2-3"), StatusOK)
		// Slashes within the path are the path's: another stream.
		expect(t, r.do(MethodPlay, base+"live//s"), StatusMethodNotValidInState)
		expect(t, r.do(MethodPlay, playURL), StatusOK)
		// PAUSE and PLAY again by a track's URL.
		expect(t, r.do(MethodPause, playURL), StatusOK)
		expect(t, r.do(MethodPlay, setupBase+"trackID=1"), StatusOK)
	}

	// A stream with one track, set up and played by its own URL.
	single, _ := h.Publish("single", testTracks(t)[1:], hub.PublishInfo{})
	defer single.Close()
	r := dialRaw(t, base)
	expect(t, r.do(MethodSetup, base+"single?token=T", "Transport: RTP/AVP/TCP;unicast"), StatusOK)
	expect(t, r.do(MethodPlay, base+"single?token=T"), StatusOK)

	// A recorder RECORDs the URL it ANNOUNCEd, or with the slashes and
	// query it likes; not another stream.
	for _, rec := range [][]string{
		{base + "rec", base + "rec"},
		{base + "rec?token=T", base + "rec?token=T"},
		{base + "rec2", base + "rec2/"},
		{base + "rec3", other + "rec3?x=1"},
	} {
		r := dialRaw(t, base)
		expect(t, r.do(MethodAnnounce, rec[0], "Content-Type: application/sdp", "Body: "+testSDP), StatusOK)
		expect(t, r.do(MethodSetup, rec[0]+"/streamid=0", "Transport: RTP/AVP/TCP;unicast;interleaved=0-1;mode=record"), StatusOK)
		expect(t, r.do(MethodRecord, base+"elsewhere"), StatusMethodNotValidInState)
		if !strings.Contains(rec[0], "?") {
			// A track's URL is another stream's path; what follows a
			// query is the query's.
			expect(t, r.do(MethodRecord, rec[0]+"/streamid=0"), StatusMethodNotValidInState)
		}
		expect(t, r.do(MethodPlay, rec[1]), StatusMethodNotValidInState)
		expect(t, r.do(MethodRecord, rec[1]), StatusOK)
		expect(t, r.do(MethodTeardown, rec[1]), StatusOK)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, p := range paths {
		if p != "live/s" && p != "single" && !strings.HasPrefix(p, "rec") {
			t.Fatal(paths)
		}
	}
}

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"math"
	stdhttp "net/http"
	"strconv"
	"testing"
	"time"
)

// h2SendBody sends n bytes of body on stream id, in frames the default
// SETTINGS_MAX_FRAME_SIZE allows, the last with END_STREAM if end is set.
func h2SendBody(tc *h2TestConn, id uint32, n int, end bool) {
	chunk := make([]byte, h2DefaultMaxFrameSize)
	for n > 0 {
		m := min(n, len(chunk))
		n -= m
		tc.data(id, end && n == 0, chunk[:m])
	}
}

// h2Post opens stream id with a POST to path declaring a body of n bytes.
func h2Post(tc *h2TestConn, id uint32, path string, n int) {
	tc.headers(id, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", path,
		"content-length", strconv.Itoa(n))
}

// TestH2MaxBufferedBodyBytesDefaults checks how
// HTTP2Config.MaxBufferedBodyBytes resolves.
func TestH2MaxBufferedBodyBytesDefaults(t *testing.T) {
	for _, tt := range []struct {
		limit, maxBody, want int64
	}{
		{0, 16 << 20, 64 << 20},
		{0, 1000, 4000},
		{0, math.MaxInt64 / 2, math.MaxInt64},
		{-1, 16 << 20, 0},
		{1 << 30, 16 << 20, 1 << 30},
		// One body of the largest size still fits.
		{1000, 16 << 20, 16 << 20},
	} {
		c := HTTP2Config{MaxBufferedBodyBytes: tt.limit}
		if got := c.maxBufferedBodies(tt.maxBody); got != tt.want {
			t.Errorf("MaxBufferedBodyBytes %d, MaxBodyBytes %d: %d, want %d", tt.limit, tt.maxBody, got, tt.want)
		}
	}
	sc := newH2ServerConn(NewHandler(echoHandler()), nil, "")
	if want := DefaultConfig().MaxBodyBytes * DefaultH2BufferedBodies; sc.maxBuffered != want {
		t.Errorf("a default server's connection holds %d, want %d", sc.maxBuffered, want)
	}
}

// TestH2BufferedBodiesBoundedPerConnection checks that the bodies one
// connection collects whole are bounded together, not only each by
// MaxBodyBytes: the stream whose DATA would take them past
// MaxBufferedBodyBytes is reset with REFUSED_STREAM, while the others, and a
// body's last frame, which goes to its handler at once, carry on, and once a
// body has gone to its handler there is room for another.
func TestH2BufferedBodiesBoundedPerConnection(t *testing.T) {
	const (
		maxBody  = 100000
		budget   = 250000
		declared = 100000
		first    = 60000
	)
	config := DefaultConfig()
	config.MaxBodyBytes = maxBody
	config.HTTP2.MaxBufferedBodyBytes = budget
	config.HTTP2.MaxFrameSize = 64 << 10
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(strconv.Itoa(len(c.Body()))))
	})))
	tc := dialH2(t, addr)
	// Four bodies, 240000 bytes in all, fit.
	for _, id := range []uint32{1, 3, 5, 7} {
		h2Post(tc, id, "/", declared)
		h2SendBody(tc, id, first, false)
	}
	h2covSync(t, tc)
	// More on stream 1 would take the connection past its budget.
	h2SendBody(tc, 1, 20000, false)
	if code := h2covReset(t, tc, 1); code != H2RefusedStream {
		t.Fatalf("stream 1 reset with %v, want REFUSED_STREAM", code)
	}
	// Its body went with it: 180000 held, so stream 3 has room for the
	// rest of its own, whose last frame would fit regardless.
	h2SendBody(tc, 3, declared-first, true)
	if _, body := tc.response(3); string(body) != strconv.Itoa(declared) {
		t.Fatalf("stream 3: %q", body)
	}
	// 120000 held: a new body fits whole beside the two left.
	h2Post(tc, 9, "/", declared)
	h2SendBody(tc, 9, declared-1, false)
	h2SendBody(tc, 9, 1, true)
	if _, body := tc.response(9); string(body) != strconv.Itoa(declared) {
		t.Fatalf("stream 9: %q", body)
	}
	// A body's last frame goes to its handler however full the connection
	// is: 120000 held, 215000 with stream 11's first frames, and stream 5's
	// last 40000 in one frame would come to 255000.
	h2Post(tc, 11, "/", declared)
	h2SendBody(tc, 11, declared-5000, false)
	tc.data(5, true, make([]byte, declared-first))
	h2SendBody(tc, 7, declared-first, true)
	h2SendBody(tc, 11, 5000, true)
	for _, id := range []uint32{5, 7, 11} {
		if _, body := tc.response(id); string(body) != strconv.Itoa(declared) {
			t.Fatalf("stream %d: %q", id, body)
		}
	}
}

// TestH2BufferedBodiesCountedAgain checks that a body that goes with its
// stream away from the connection's reader, here reset by its read timeout,
// stops counting against the connection's budget: the reader's count runs
// high then, and is taken again before a stream is refused.
func TestH2BufferedBodiesCountedAgain(t *testing.T) {
	const budget = 250000
	config := DefaultConfig()
	config.MaxBodyBytes = budget
	config.HTTP2.MaxBufferedBodyBytes = budget
	config.ReadBodyTimeout = 200 * time.Millisecond
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(strconv.Itoa(len(c.Body()))))
	})))
	tc := dialH2(t, addr)
	h2Post(tc, 1, "/", budget)
	h2SendBody(tc, 1, 200000, false)
	if code := h2covReset(t, tc, 1); code != H2Cancel {
		t.Fatalf("stream 1 reset with %v, want CANCEL from its read timeout", code)
	}
	h2Post(tc, 3, "/", 210000)
	h2SendBody(tc, 3, 200000, false)
	h2SendBody(tc, 3, 10000, true)
	if _, body := tc.response(3); string(body) != "210000" {
		t.Fatalf("stream 3: %q", body)
	}
}

// TestH2TunnelQueuesBoundedPerConnection checks that what tunnels hold for
// handlers that have not accepted them yet counts against the connection's
// budget too: the tunnel that takes it past is reset with ENHANCE_YOUR_CALM,
// though each holds less than its own limit.
func TestH2TunnelQueuesBoundedPerConnection(t *testing.T) {
	const budget = 250000
	config := DefaultConfig()
	config.MaxBodyBytes = budget
	config.HTTP2.MaxBufferedBodyBytes = budget
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		<-release
	})))
	tc := dialH2(t, addr)
	h2covConnect(tc, 1, "/")
	h2SendBody(tc, 1, 150000, false)
	h2covConnect(tc, 3, "/")
	h2SendBody(tc, 3, 50000, false)
	h2covSync(t, tc)
	if st := tc.stream(3); st.done {
		t.Fatalf("stream 3 ended within the budget: reset %v", st.reset)
	}
	h2SendBody(tc, 3, 60000, false)
	if code := h2covReset(t, tc, 3); code != H2EnhanceYourCalm {
		t.Fatalf("stream 3 reset with %v, want ENHANCE_YOUR_CALM", code)
	}
	if st := tc.stream(1); st.done {
		t.Fatalf("stream 1 ended: reset %v", st.reset)
	}
}

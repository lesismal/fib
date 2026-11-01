//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"io"
	stdhttp "net/http"
	"strconv"
	"testing"
	"time"

	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// sharedBody is a body of n bytes whose every frame differs from the next, so
// that a frame sent twice, out of order, or beside the wrong header shows.
func sharedBody(n int) []byte {
	body := make([]byte, n)
	for i := range body {
		body[i] = byte(i/7 + i%251)
	}
	return body
}

// A whole body over h2c is sent beside its frames rather than copied into
// them, in every size and through every path its frames take: at once when
// the windows are open, a frame a turn as the client's WINDOW_UPDATEs open
// them, with trailers after it, and in one frame. What arrives is the body,
// in order, and a body or a tail of one too small to be worth a part of its
// own is still copied into its frame.
func TestH2SharedBodyArrivesWhole(t *testing.T) {
	sizes := []int{
		4 << 10,       // one frame, copied
		10 << 10,      // one frame, kept
		100<<10 + 5,   // frames kept, and a small tail copied
		1<<20 + 40000, // more frames than one array of frame headers holds
	}
	for _, size := range sizes {
		for _, open := range []bool{true, false} {
			for _, trailer := range []bool{false, true} {
				name := strconv.Itoa(size)
				if open {
					name += "/open"
				} else {
					name += "/acked"
				}
				if trailer {
					name += "/trailer"
				}
				t.Run(name, func(t *testing.T) {
					testH2SharedBody(t, size, open, trailer)
				})
			}
		}
	}
}

func testH2SharedBody(t *testing.T, size int, open, trailer bool) {
	body := sharedBody(size)
	shared := make(chan bool, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		response := Response{StatusCode: stdhttp.StatusOK, Body: body}
		if trailer {
			response.Trailer = stdhttp.Header{"X-Sum": {"done"}}
		}
		if err := c.WriteResponse(response); err != nil {
			t.Error(err)
		}
		sc := c.stream.sc
		sc.mu.Lock()
		shared <- c.stream.shared
		sc.mu.Unlock()
	})))
	var tc *h2TestConn
	if open {
		tc = dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), h2MaxWindow})
		tc.write(h2AppendWindowUpdate(nil, 0, h2MaxWindow-h2DefaultWindow))
	} else {
		// One frame's worth of window, so that the rest goes as readAcking
		// gives it back.
		tc = dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), h2DefaultMaxFrameSize})
	}
	tc.headers(1, true, get("/")...)
	st := tc.stream(1)
	var last h2TestFrame
	for !st.done {
		last = tc.read()
		if last.typ == h2FrameData && len(last.payload) > 0 {
			tc.write(h2AppendWindowUpdate(nil, 0, uint32(len(last.payload))))
			tc.write(h2AppendWindowUpdate(nil, 1, uint32(len(last.payload))))
		}
	}
	if st.resetSet {
		t.Fatalf("stream reset: %v", st.reset)
	}
	if !bytes.Equal(st.body, body) {
		t.Fatalf("got %d body bytes, want %d, first difference at %d", len(st.body), len(body), firstDifference(st.body, body))
	}
	if trailer && (last.typ != h2FrameHeaders || last.fields["x-sum"] != "done") {
		t.Fatalf("the stream ended with %v %v, not its trailer", last.typ, last.fields)
	}
	// A body of more than one frame waits in the stream as it is; one that
	// fits a frame goes out with its header, and is kept or copied there.
	if kept := <-shared; size > h2DefaultMaxFrameSize && !kept {
		t.Fatal("the stream copied a body it could have kept")
	}
}

func firstDifference(a, b []byte) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

// Two large bodies take turns at the connection's window, a frame each, and
// both arrive whole: the parts each stream hands the connection keep their
// order on the wire between the other stream's.
func TestH2SharedBodiesInterleave(t *testing.T) {
	bodies := map[string][]byte{"/a": sharedBody(300 << 10), "/b": bytes.Repeat([]byte("0123456789abcdef"), 20<<10)}
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		_ = c.WriteResponse(Response{StatusCode: stdhttp.StatusOK, Body: bodies[c.Request.URL.Path]})
	})))
	// The connection's window is its default, so the two share it a frame
	// a turn as readAcking gives it back.
	tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), h2MaxWindow})
	tc.headers(1, true, get("/a")...)
	tc.headers(3, true, get("/b")...)
	for !tc.stream(1).done || !tc.stream(3).done {
		if f := tc.read(); f.typ == h2FrameData && len(f.payload) > 0 {
			tc.write(h2AppendWindowUpdate(nil, 0, uint32(len(f.payload))))
		}
	}
	for id, path := range map[uint32]string{1: "/a", 3: "/b"} {
		if got := tc.stream(id).body; !bytes.Equal(got, bodies[path]) {
			t.Fatalf("stream %d: got %d bytes, want %d, first difference at %d", id, len(got), len(bodies[path]),
				firstDifference(got, bodies[path]))
		}
	}
}

// A body is copied into its frames, as it always was, wherever the connection
// cannot keep it as it is: over TLS, which seals what it is given; when it is
// the request's own buffer, which the server takes back; and when it is a
// *Copy call's buffer from the pool, which goes back to the pool.
func TestH2SharedBodyOnlyWhereKept(t *testing.T) {
	body := sharedBody(100 << 10)
	cases := []struct {
		name string
		tls  bool
		send func(c *Context) error
	}{
		{"TLS", true, func(c *Context) error {
			return c.WriteResponse(Response{StatusCode: stdhttp.StatusOK, Body: body})
		}},
		{"RequestBody", false, func(c *Context) error {
			return c.WriteResponse(Response{StatusCode: stdhttp.StatusOK, Body: c.Body()})
		}},
		{"Copy", false, func(c *Context) error {
			return c.WriteResponseCopy(Response{StatusCode: stdhttp.StatusOK, Body: body})
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			shared := make(chan bool, 1)
			handler := NewHandler(HandlerFunc(func(c *Context) {
				if err := tt.send(c); err != nil {
					t.Error(err)
				}
				sc := c.stream.sc
				sc.mu.Lock()
				shared <- c.stream.shared
				sc.mu.Unlock()
			}))
			if !tt.tls {
				addr := serve(t, handler)
				tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), h2MaxWindow})
				tc.write(h2AppendWindowUpdate(nil, 0, h2MaxWindow-h2DefaultWindow))
				tc.headers(1, false, ":method", "POST", ":scheme", "http", ":authority", "test", ":path", "/",
					"content-length", strconv.Itoa(len(body)))
				for rest := body; len(rest) > 0; {
					chunk := rest[:min(len(rest), h2DefaultMaxFrameSize)]
					rest = rest[len(chunk):]
					tc.data(1, len(rest) == 0, chunk)
				}
				if got := tc.readAcking(1).body; !bytes.Equal(got, body) {
					t.Fatalf("got %d bytes, want %d, first difference at %d", len(got), len(body), firstDifference(got, body))
				}
			} else {
				serverConfig, clientConfig, err := tlstest.Configs()
				if err != nil {
					t.Fatal(err)
				}
				addr := serve(t, fibtls.NewServer(ConfigureTLS(serverConfig), handler))
				client := &stdhttp.Client{Timeout: 20 * time.Second, Transport: &stdhttp.Transport{
					TLSClientConfig: clientConfig, ForceAttemptHTTP2: true}}
				defer client.CloseIdleConnections()
				resp, err := client.Get("https://" + addr + "/")
				if err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil || resp.ProtoMajor != 2 || !bytes.Equal(got, body) {
					t.Fatalf("%v, %s, got %d bytes, want %d", err, resp.Proto, len(got), len(body))
				}
			}
			if <-shared {
				t.Fatal("the stream kept a body it should have copied")
			}
		})
	}
}

// A connection that has ended sends nothing more, and framing goes on from an
// empty buffer.
func TestH2SendSharedOnClosedConnection(t *testing.T) {
	sc := &h2ServerConn{closed: true}
	if out := sc.sendSharedLocked([]byte("framed"), 1, 0, sharedBody(8<<10)); len(out) != 0 || cap(out) == 0 {
		t.Fatalf("sendSharedLocked on a closed connection returned %d bytes of %d", len(out), cap(out))
	}
	if sc.frameHeads != nil {
		t.Fatal("a closed connection wrote a frame header")
	}
}

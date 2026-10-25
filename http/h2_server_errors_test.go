//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"encoding/binary"
	"net"
	stdhttp "net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// h2covGoAway reads until GOAWAY and returns its error code.
func h2covGoAway(t *testing.T, tc *h2TestConn) H2ErrorCode {
	t.Helper()
	f := tc.readUntil(h2FrameGoAway)
	return H2ErrorCode(binary.BigEndian.Uint32(f.payload[4:8]))
}

// h2covReset reads until stream id is reset and returns the code.
func h2covReset(t *testing.T, tc *h2TestConn, id uint32) H2ErrorCode {
	t.Helper()
	st := tc.stream(id)
	for !st.done {
		tc.read()
	}
	if !st.resetSet {
		t.Fatalf("stream %d ended without RST_STREAM: %v", id, st.header)
	}
	return st.reset
}

// h2covSync sends a PING and reads until its ack, by when the server has
// handled every frame sent before it.
func h2covSync(t *testing.T, tc *h2TestConn) {
	t.Helper()
	tc.write(append(h2AppendFrameHeader(nil, h2FramePing, 0, 0, 8), "h2covsyn"...))
	for {
		f := tc.readUntil(h2FramePing)
		if f.has(h2FlagAck) && string(f.payload) == "h2covsyn" {
			return
		}
	}
}

// h2covDial dials addr and sends only the preface, leaving the first frame
// to the test.
func h2covDial(t *testing.T, addr string) *h2TestConn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	tc := newH2TestConn(t, c)
	tc.write([]byte(h2Preface))
	return tc
}

// TestH2ServerRejectsMalformedFrames sends one frame per framing rule RFC
// 9113 makes a connection error, each on a connection of its own, and checks
// the server answers with GOAWAY and the code the RFC names.
func TestH2ServerRejectsMalformedFrames(t *testing.T) {
	config := DefaultConfig()
	config.MaxHeaderBytes = 200
	addr := serve(t, NewHandlerWithConfig(config, echoHandler()))
	for _, tt := range []struct {
		name     string
		settings bool
		frames   []byte
		want     H2ErrorCode
	}{
		{"PING before SETTINGS", false,
			append(h2AppendFrameHeader(nil, h2FramePing, 0, 0, 8), "12345678"...), H2ProtocolError},
		{"short PING", true,
			append(h2AppendFrameHeader(nil, h2FramePing, 0, 0, 4), "1234"...), H2FrameSizeError},
		{"short GOAWAY", true,
			append(h2AppendFrameHeader(nil, h2FrameGoAway, 0, 0, 4), 0, 0, 0, 0), H2FrameSizeError},
		{"HEADERS with truncated priority", true,
			append(h2AppendFrameHeader(nil, h2FrameHeaders, h2FlagEndHeaders|h2FlagPriority, 1, 3), 0, 0, 0), H2FrameSizeError},
		{"padded DATA without pad length", true,
			h2AppendFrameHeader(nil, h2FrameData, h2FlagPadded, 1, 0), H2FrameSizeError},
		{"HEADERS block past MaxHeaderBytes", true,
			append(h2AppendFrameHeader(nil, h2FrameHeaders, 0, 1, 300), make([]byte, 300)...), H2EnhanceYourCalm},
		{"CONTINUATION block past MaxHeaderBytes", true,
			append(append(h2AppendFrameHeader(nil, h2FrameHeaders, 0, 1, 150), make([]byte, 150)...),
				append(h2AppendFrameHeader(nil, h2FrameContinuation, 0, 1, 150), make([]byte, 150)...)...), H2EnhanceYourCalm},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tc := h2covDial(t, addr)
			if tt.settings {
				tc.write(h2AppendSettings(nil))
			}
			tc.write(tt.frames)
			if code := h2covGoAway(t, tc); code != tt.want {
				t.Fatalf("GOAWAY %v, want %v", code, tt.want)
			}
		})
	}
}

// TestH2ServerSettingsOverflowingAStreamWindow checks that a new
// INITIAL_WINDOW_SIZE that would carry an open stream's window past 2^31-1
// is a FLOW_CONTROL_ERROR for the connection (RFC 9113 section 6.9.2).
func TestH2ServerSettingsOverflowingAStreamWindow(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-time.After(10 * time.Second):
		}
	})))
	t.Cleanup(func() { close(release) })
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/hold")...)
	<-entered
	tc.write(h2AppendWindowUpdate(nil, 1, h2MaxWindow-h2DefaultWindow))
	tc.write(h2AppendSettings(nil, [2]uint32{uint32(h2SettingInitialWindowSize), h2DefaultWindow + 1}))
	if code := h2covGoAway(t, tc); code != H2FlowControlError {
		t.Fatalf("GOAWAY %v, want %v", code, H2FlowControlError)
	}
}

// TestH2ServerPrefaceInPieces checks that a preface arriving over several
// reads is put together rather than taken for a bad one, on a server that
// speaks only HTTP/2 and so hands the connection over before it has all
// arrived.
func TestH2ServerPrefaceInPieces(t *testing.T) {
	config := DefaultConfig()
	config.HTTP2Only = true
	addr := serve(t, NewHandlerWithConfig(config, echoHandler()))
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	tc := newH2TestConn(t, c)
	// The server's SETTINGS arrives before the client has sent its preface
	// whole, and an acknowledgement now would land in the middle of it; it
	// goes with the rest instead.
	tc.holdSettingsAck = true
	tc.write([]byte(h2Preface[:10]))
	// The server's SETTINGS says it has taken the first piece.
	if f := tc.readRaw(); f.typ != h2FrameSettings {
		t.Fatalf("first frame %v, want SETTINGS", f.typ)
	}
	tc.holdSettingsAck = false
	tail := append([]byte(h2Preface[10:]), h2AppendSettings(nil)...)
	tail = append(tail, h2AppendFrameHeader(nil, h2FrameSettings, h2FlagAck, 0, 0)...)
	tc.write(tail)
	tc.headers(1, true, get("/pieces")...)
	if _, body := tc.response(1); string(body) != "GET /pieces " {
		t.Fatalf("body %q", body)
	}
}

// TestH2ServerIgnoresRequestsAfterClientGoAway checks that a client that has
// sent GOAWAY may not open more streams: a request after it is not served,
// while the frames around it are still answered.
func TestH2ServerIgnoresRequestsAfterClientGoAway(t *testing.T) {
	addr := serve(t, NewHandler(echoHandler()))
	tc := dialH2(t, addr)
	tc.write(h2AppendGoAway(nil, 0, H2NoError, ""))
	tc.headers(1, true, get("/late")...)
	h2covSync(t, tc)
	if st := tc.stream(1); st.header != nil || st.done {
		t.Fatalf("a request after GOAWAY was answered: %v", st.header)
	}
}

// TestH2ServerRejectsMalformedRequests sends requests RFC 9113 section 8.3
// calls malformed, which reset their stream with PROTOCOL_ERROR and leave the
// connection serving.
func TestH2ServerRejectsMalformedRequests(t *testing.T) {
	addr := serve(t, NewHandler(echoHandler()))
	tc := dialH2(t, addr)
	id := uint32(1)
	for _, tt := range []struct {
		name   string
		fields []string
	}{
		{"extended CONNECT without :scheme", []string{":method", "CONNECT", ":protocol", "websocket", ":authority", "x", ":path", "/"}},
		{"extended CONNECT with a bad :path", []string{":method", "CONNECT", ":protocol", "websocket", ":scheme", "http", ":authority", "x", ":path", "relative"}},
		{"CONNECT with :path", []string{":method", "CONNECT", ":authority", "x:443", ":path", "/"}},
		{"bad :path", get("relative")},
		{"invalid content-length", append(get("/"), "content-length", "-1")},
	} {
		tc.headers(id, true, tt.fields...)
		if code := h2covReset(t, tc, id); code != H2ProtocolError {
			t.Fatalf("%s: reset %v, want %v", tt.name, code, H2ProtocolError)
		}
		id += 2
	}
	// A trailer may not carry pseudo-headers.
	tc.headers(id, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", "/")
	tc.data(id, false, []byte("x"))
	tc.headers(id, true, ":path", "/again")
	if code := h2covReset(t, tc, id); code != H2ProtocolError {
		t.Fatalf("pseudo-header trailer: reset %v, want %v", code, H2ProtocolError)
	}
	id += 2
	tc.headers(id, true, get("/after")...)
	if _, body := tc.response(id); string(body) != "GET /after " {
		t.Fatalf("body %q", body)
	}
}

// TestH2ServerConnectAndHostHeader checks the request a plain CONNECT makes,
// which names only an authority, and that a request without :authority
// takes its Host from the Host field, as RFC 9113 section 8.3.1 allows.
func TestH2ServerConnectAndHostHeader(t *testing.T) {
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		_ = c.Respond(200, "text/plain", []byte(c.Request.Method+" "+c.Request.Host+" "+c.Request.RequestURI+
			" "+c.Request.Header.Get("Host")))
	})))
	tc := dialH2(t, addr)
	tc.headers(1, true, ":method", "CONNECT", ":authority", "example.com:443")
	if _, body := tc.response(1); string(body) != "CONNECT example.com:443 example.com:443 " {
		t.Fatalf("CONNECT: %q", body)
	}
	tc.headers(3, true, ":method", "GET", ":scheme", "http", ":path", "/h", "host", "from-header")
	if _, body := tc.response(3); string(body) != "GET from-header /h " {
		t.Fatalf("Host field: %q", body)
	}
}

// TestH2ServerHeaderListTooLarge checks that a header or trailer list
// larger, decoded, than MaxHeaderBytes is answered with 431, as HTTP/1 does,
// rather than ending the connection, and that a block spread over
// CONTINUATION frames past what the connection keeps is still served.
func TestH2ServerHeaderListTooLarge(t *testing.T) {
	config := DefaultConfig()
	config.MaxHeaderBytes = 400
	addr := serve(t, NewHandlerWithConfig(config, echoHandler()))
	tc := dialH2(t, addr)
	big := strings.Repeat("v", 150)

	tc.headers(1, true, append(get("/"), "x-a", big, "x-b", big, "x-c", big)...)
	if header, _ := tc.response(1); header[":status"] != "431" {
		t.Fatalf("header list: status %s, want 431", header[":status"])
	}

	tc.headers(3, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", "/")
	tc.data(3, false, []byte("x"))
	tc.headers(3, true, "x-a", big, "x-b", big, "x-c", big)
	if header, _ := tc.response(3); header[":status"] != "431" {
		t.Fatalf("trailer list: status %s, want 431", header[":status"])
	}

	tc.headers(5, true, get("/after")...)
	if _, body := tc.response(5); string(body) != "GET /after " {
		t.Fatalf("body %q", body)
	}
}

// TestH2ServerLargeHeaderBlockInContinuations checks that a header block too
// large to keep attached to the connection, arriving in CONTINUATION frames,
// is served, and the connection carries on.
func TestH2ServerLargeHeaderBlockInContinuations(t *testing.T) {
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		_ = c.Respond(200, "text/plain", []byte(c.Request.URL.Path+" "+strconv.Itoa(len(c.Request.Header.Get("X-Big")))))
	})))
	tc := dialH2(t, addr)
	block := tc.enc.Begin(nil)
	for _, kv := range [][2]string{{":method", "GET"}, {":scheme", "http"}, {":authority", "t"}, {":path", "/big"},
		{"x-big", strings.Repeat("~", 120000)}} {
		block = tc.enc.AppendField(block, kv[0], kv[1], false)
	}
	tc.write(h2AppendHeaderBlock(nil, 1, block, true, h2DefaultMaxFrameSize))
	if _, body := tc.response(1); string(body) != "/big 120000" {
		t.Fatalf("body %q", body)
	}
	tc.headers(3, true, get("/after")...)
	if _, body := tc.response(3); string(body) != "/after 0" {
		t.Fatalf("body %q", body)
	}
}

// TestConfigureTLSNil checks a nil config gets a new one offering h2 first.
func TestConfigureTLSNil(t *testing.T) {
	config := ConfigureTLS(nil)
	if strings.Join(config.NextProtos, ",") != "h2,http/1.1" {
		t.Fatalf("NextProtos %v", config.NextProtos)
	}
}

// TestH2ErrorStrings pins down how HTTP/2 errors read, unknown codes and
// peer-sent ones included.
func TestH2ErrorStrings(t *testing.T) {
	for _, tt := range []struct {
		err  error
		want string
	}{
		{&H2ConnError{Code: H2ProtocolError}, "http2: connection error: PROTOCOL_ERROR"},
		{&H2ConnError{Code: H2NoError, Reason: "bye", Remote: true}, "http2: peer sent GOAWAY: NO_ERROR: bye"},
		{&H2ConnError{Code: 0x99}, "http2: connection error: unknown error code 0x99"},
		{&H2StreamError{StreamID: 3, Code: H2Cancel}, "http2: stream 3 reset: CANCEL"},
	} {
		if got := tt.err.Error(); got != tt.want {
			t.Errorf("got %q, want %q", got, tt.want)
		}
	}
	if h2ValidHeaderName("") || h2ValidHeaderValue("a\x00b") || h2ValidHeaderValue(" a") || !h2ValidHeaderValue("a b") {
		t.Fatal("header validation")
	}
}

// TestH2ServerWithoutRecycledHeaders checks a server that recycles its
// streams but not its request headers, which then cannot keep a header's
// values in the recycled stream since a handler may keep the header.
func TestH2ServerWithoutRecycledHeaders(t *testing.T) {
	config := DefaultConfig()
	config.ReuseHeaders = false
	kept := make(chan stdhttp.Header, 2)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		kept <- c.Request.Header
		_ = c.Respond(200, "text/plain", []byte(c.Request.Header.Get("X-A")))
	})))
	tc := dialH2(t, addr)
	tc.headers(1, true, append(get("/"), "x-a", "one")...)
	if _, body := tc.response(1); string(body) != "one" {
		t.Fatalf("body %q", body)
	}
	tc.headers(3, true, append(get("/"), "x-a", "two")...)
	if _, body := tc.response(3); string(body) != "two" {
		t.Fatalf("body %q", body)
	}
	if first := <-kept; first.Get("X-A") != "one" {
		t.Fatalf("a kept header changed to %v", first)
	}
}

// TestH2ServerLargeRead checks a connection whose reads are larger than its
// buffer is worth keeping: a burst of frames read at once is all handled,
// and the buffer is let go of rather than kept with the connection.
func TestH2ServerLargeRead(t *testing.T) {
	engineConfig := fib.DefaultConfig()
	engineConfig.ReadBufferSize = 512 << 10
	server, err := fib.New(engineConfig, fib.Binding{Addr: "127.0.0.1:0", Handler: NewHandler(HandlerFunc(func(c *Context) {
		_ = c.Respond(200, "text/plain", []byte(strconv.Itoa(len(c.Body()))))
	}))})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	})
	tc := dialH2(t, addr.String())
	const size = 256 << 10
	tc.headers(1, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", "/burst",
		"content-length", strconv.Itoa(size))
	var burst []byte
	chunk := make([]byte, h2DefaultMaxFrameSize)
	for sent := 0; sent < size; sent += len(chunk) {
		burst = append(h2AppendFrameHeader(burst, h2FrameData, 0, 1, len(chunk)), chunk...)
	}
	tc.write(append(burst, h2AppendFrameHeader(nil, h2FrameData, h2FlagEndStream, 1, 0)...))
	if _, body := tc.response(1); string(body) != strconv.Itoa(size) {
		t.Fatalf("body %q, want %d", body, size)
	}
}

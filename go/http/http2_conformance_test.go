//go:build linux || darwin || windows

package http

// HTTP/2 conformance. The fib server is checked against net/http's client,
// against h2spec when it is installed, against curl, and against raw frames
// for what neither would send; the fib client against net/http's server and
// against a raw frame server of its own. Every test's name starts with
// TestHTTP2Conformance so that CI can run the suite on its own.

import (
	stdtls "crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/go/internal/hpack"
	"github.com/lesismal/fib/go/internal/tlstest"
	fibtls "github.com/lesismal/fib/go/tls"
)

// ---------------------------------------------------------------------------
// A raw HTTP/2 server, to drive the fib client through what net/http's server
// never does.

// h2RawServer is one connection of a hand-driven HTTP/2 server. It checks
// the client's flow control as it reads: a peer may send only as much as the
// windows this side has granted.
type h2RawServer struct {
	t    *testing.T
	c    net.Conn
	enc  *hpack.Encoder
	dec  *hpack.Decoder
	in   []byte
	cont []byte
	// contStream is the stream whose header block is still arriving.
	contStream uint32
	// initialWindow is the receive window this side advertised, connWindow
	// what the connection may still send, and acked whether the client has
	// acknowledged the SETTINGS that named initialWindow; until it has, a new
	// stream may still use the protocol's default window.
	initialWindow int64
	connWindow    int64
	acked         bool
}

// rawH2Server listens for cleartext HTTP/2 connections and runs serve for
// each, after the preface and this side's SETTINGS have been exchanged.
// settings are sent to the client in that SETTINGS frame.
func rawH2Server(t *testing.T, settings []([2]uint32), serve func(s *h2RawServer)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer endRawConn(c)
				_ = c.SetDeadline(time.Now().Add(20 * time.Second))
				s := &h2RawServer{t: t, c: c, enc: hpack.NewEncoder(), dec: hpack.NewDecoder(hpack.DefaultTableSize),
					initialWindow: h2DefaultWindow, connWindow: h2DefaultWindow}
				for _, setting := range settings {
					if h2SettingID(setting[0]) == h2SettingInitialWindowSize {
						s.initialWindow = int64(setting[1])
					}
				}
				s.readPreface()
				s.write(h2AppendSettings(nil, settings...))
				serve(s)
			}()
		}
	}()
	return ln.Addr().String()
}

// endRawConn ends a raw server's connection without resetting it. Closing a
// socket that still holds bytes the peer sent is abortive, and a reset throws
// away what the peer has received but not yet read — here, the response this
// server has just written. A client leaves such bytes behind on any healthy
// connection: its settings acknowledgement and its window updates, which this
// server reads only as far as the request it was waiting for. So the send side
// goes first, which is the client's cue to close, and the rest is read off
// until it does.
func endRawConn(c net.Conn) {
	defer c.Close()
	tcp, ok := c.(*net.TCPConn)
	if !ok {
		return
	}
	if err := tcp.CloseWrite(); err != nil {
		return
	}
	_ = tcp.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _ = io.Copy(io.Discard, tcp)
}

func (s *h2RawServer) readPreface() {
	preface := make([]byte, len(h2Preface))
	if _, err := io.ReadFull(s.c, preface); err != nil || string(preface) != h2Preface {
		s.t.Errorf("preface = %q, %v", preface, err)
		return
	}
}

func (s *h2RawServer) write(b []byte) {
	if _, err := s.c.Write(b); err != nil {
		s.t.Logf("raw server write: %v", err)
	}
}

// readFrame returns the next frame, reporting false once the connection ends.
func (s *h2RawServer) readFrame() (h2Frame, bool) {
	for {
		f, n, err := h2ReadFrame(s.in, h2MaxFrameSizeLimit)
		if err != nil {
			s.t.Errorf("raw server: %v", err)
			return h2Frame{}, false
		}
		if n > 0 {
			f.payload = append([]byte(nil), f.payload...)
			s.in = s.in[n:]
			return f, true
		}
		buf := make([]byte, 32<<10)
		m, err := s.c.Read(buf)
		if err != nil {
			return h2Frame{}, false
		}
		s.in = append(s.in, buf[:m]...)
	}
}

// h2RawRequest is a request the raw server received.
type h2RawRequest struct {
	id     uint32
	fields map[string]string
	body   []byte
	// dataFrames is how many DATA frames its body arrived in, and maxFrame
	// the largest of them. window is what the stream may still send.
	dataFrames, maxFrame int
	window               int64
}

// nextRequest reads frames until one stream's request is complete, handling
// the connection's own frames on the way and calling each on every frame.
func (s *h2RawServer) nextRequest(each ...func(h2Frame)) *h2RawRequest {
	pending := map[uint32]*h2RawRequest{}
	for {
		f, ok := s.readFrame()
		if !ok {
			return nil
		}
		for _, fn := range each {
			fn(f)
		}
		switch f.typ {
		case h2FrameSettings:
			if f.has(h2FlagAck) {
				s.acked = true
			} else {
				s.write(h2AppendFrameHeader(nil, h2FrameSettings, h2FlagAck, 0, 0))
			}
			continue
		case h2FramePing:
			if !f.has(h2FlagAck) {
				s.write(append(h2AppendFrameHeader(nil, h2FramePing, h2FlagAck, 0, 8), f.payload...))
			}
			continue
		case h2FrameHeaders, h2FrameContinuation:
			block := f.payload
			if f.typ == h2FrameHeaders {
				if f.has(h2FlagPriority) {
					block = block[5:]
				}
				s.contStream, s.cont = f.streamID, append([]byte(nil), block...)
			} else {
				s.cont = append(s.cont, block...)
			}
			if !f.has(h2FlagEndHeaders) {
				continue
			}
			r := pending[s.contStream]
			if r == nil {
				// A stream opened before the client acknowledged this side's
				// SETTINGS may still be using the default window.
				window := s.initialWindow
				if !s.acked {
					window = max(window, h2DefaultWindow)
				}
				r = &h2RawRequest{id: s.contStream, fields: map[string]string{}, window: window}
				pending[s.contStream] = r
			}
			if err := s.dec.Decode(s.cont, func(hf hpack.HeaderField) error {
				r.fields[hf.Name] = hf.Value
				return nil
			}); err != nil {
				s.t.Errorf("raw server hpack: %v", err)
				return nil
			}
			if f.has(h2FlagEndStream) {
				return r
			}
		case h2FrameData:
			r := pending[f.streamID]
			if r == nil {
				continue
			}
			// Flow control is exact: what arrives must fit in what was
			// granted, on the stream and on the connection alike.
			r.window -= int64(f.length)
			s.connWindow -= int64(f.length)
			if r.window < 0 {
				s.t.Errorf("stream %d: %d bytes past its flow-control window", f.streamID, -r.window)
			}
			if s.connWindow < 0 {
				s.t.Errorf("connection: %d bytes past its flow-control window", -s.connWindow)
			}
			r.body = append(r.body, f.payload...)
			r.dataFrames++
			r.maxFrame = max(r.maxFrame, len(f.payload))
			// Give the window back, so a client with more to send carries on.
			r.window += int64(f.length)
			s.connWindow += int64(f.length)
			s.write(h2AppendWindowUpdate(nil, 0, f.length))
			s.write(h2AppendWindowUpdate(nil, f.streamID, f.length))
			if f.has(h2FlagEndStream) {
				return r
			}
		}
	}
}

// respond answers one request, with the header fields given as name and value
// pairs and the body in one DATA frame.
func (s *h2RawServer) respond(id uint32, body string, fields ...string) {
	block := s.enc.Begin(nil)
	for i := 0; i < len(fields); i += 2 {
		block = s.enc.AppendField(block, fields[i], fields[i+1], false)
	}
	out := h2AppendHeaderBlock(nil, id, block, body == "", h2DefaultMaxFrameSize)
	if body != "" {
		out = append(h2AppendFrameHeader(out, h2FrameData, h2FlagEndStream, id, len(body)), body...)
	}
	s.write(out)
}

// ok answers a request with 200 and body.
func (s *h2RawServer) ok(id uint32, body string) {
	s.respond(id, body, ":status", "200", "content-length", strconv.Itoa(len(body)))
}

func (s *h2RawServer) rst(id uint32, code H2ErrorCode) { s.write(h2AppendRSTStream(nil, id, code)) }

func (s *h2RawServer) goAway(last uint32, code H2ErrorCode) {
	s.write(h2AppendGoAway(nil, last, code, ""))
}

// ---------------------------------------------------------------------------
// The fib server

// h2ConformanceHandler answers with what it received, so that a peer can
// check the request reached it whole.
func h2ConformanceHandler() Handler {
	return HandlerFunc(func(c *Context, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/size":
			n, _ := strconv.Atoi(r.URL.Query().Get("n"))
			_ = c.Respond(200, "application/octet-stream", make([]byte, n))
		case "/status":
			n, _ := strconv.Atoi(r.URL.Query().Get("code"))
			_ = c.Respond(n, "text/plain", nil)
		case "/trailers":
			c.Header().Set("Trailer", "X-Checksum")
			_, _ = c.Write([]byte("body"))
			c.Header().Set("X-Checksum", "42")
		default:
			header := stdhttp.Header{"Content-Type": {"text/plain"}, "X-Proto": {r.Proto}}
			if r.Trailer.Get("X-Sum") != "" {
				header.Set("X-Sum", r.Trailer.Get("X-Sum"))
			}
			_ = c.WriteResponse(Response{StatusCode: 200, Header: header,
				Body: fmt.Appendf(nil, "%s %s %d", r.Method, r.URL.RequestURI(), len(body))})
		}
	})
}

// serveH2 runs a fib HTTP server, over TLS with ALPN when secure, and returns
// its address.
func serveH2(t *testing.T, secure bool, config Config) string {
	t.Helper()
	handler := NewHandlerWithConfig(config, h2ConformanceHandler())
	if !secure {
		return serve(t, handler)
	}
	serverConfig, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	return serve(t, fibtls.NewServer(ConfigureTLS(serverConfig), handler))
}

// netHTTPClient is net/http's client speaking HTTP/2, over TLS or cleartext.
func netHTTPClient(t *testing.T, secure bool) *stdhttp.Client {
	t.Helper()
	transport := &stdhttp.Transport{}
	if secure {
		_, clientConfig, err := tlstest.Configs()
		if err != nil {
			t.Fatal(err)
		}
		transport.TLSClientConfig, transport.ForceAttemptHTTP2 = clientConfig, true
	} else {
		transport.Protocols = new(stdhttp.Protocols)
		transport.Protocols.SetUnencryptedHTTP2(true)
	}
	t.Cleanup(transport.CloseIdleConnections)
	return &stdhttp.Client{Transport: transport, Timeout: 20 * time.Second}
}

func scheme(secure bool) string {
	if secure {
		return "https://"
	}
	return "http://"
}

// TestHTTP2ConformanceServerWithNetHTTP drives the fib server with net/http's
// HTTP/2 client, over TLS with ALPN and in cleartext with prior knowledge.
func TestHTTP2ConformanceServerWithNetHTTP(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(map[bool]string{false: "h2c", true: "h2"}[secure], func(t *testing.T) {
			addr := serveH2(t, secure, DefaultConfig())
			client := netHTTPClient(t, secure)
			url := scheme(secure) + addr

			// Methods and bodies.
			for _, tt := range []struct{ method, path, body, want string }{
				{"GET", "/a?q=1", "", "GET /a?q=1 0"},
				{"POST", "/b", "hello", "POST /b 5"},
				{"PUT", "/c", strings.Repeat("x", 100000), "PUT /c 100000"},
				{"DELETE", "/d", "", "DELETE /d 0"},
				{"OPTIONS", "/e", "", "OPTIONS /e 0"},
			} {
				req, err := stdhttp.NewRequest(tt.method, url+tt.path, strings.NewReader(tt.body))
				if err != nil {
					t.Fatal(err)
				}
				resp, err := client.Do(req)
				if err != nil {
					t.Fatalf("%s: %v", tt.method, err)
				}
				got, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.ProtoMajor != 2 || string(got) != tt.want {
					t.Fatalf("%s: %s %q, want %q", tt.method, resp.Proto, got, tt.want)
				}
			}

			// HEAD has the length but no body.
			resp, err := client.Head(url + "/h")
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.ContentLength != int64(len("HEAD /h 0")) || len(body) != 0 {
				t.Fatalf("HEAD: length %d, body %q", resp.ContentLength, body)
			}

			// Bodiless statuses.
			for _, code := range []int{204, 304} {
				resp, err := client.Get(fmt.Sprintf("%s/status?code=%d", url, code))
				if err != nil {
					t.Fatal(err)
				}
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != code || len(body) != 0 {
					t.Fatalf("%d: status %d body %q", code, resp.StatusCode, body)
				}
			}

			// A response with trailers.
			resp, err = client.Get(url + "/trailers")
			if err != nil {
				t.Fatal(err)
			}
			body, _ = io.ReadAll(resp.Body)
			if string(body) != "body" || resp.Trailer.Get("X-Checksum") != "42" {
				t.Fatalf("trailers: %q %v", body, resp.Trailer)
			}
			resp.Body.Close()

			// A response past the default flow-control windows.
			resp, err = client.Get(fmt.Sprintf("%s/size?n=%d", url, 5<<20))
			if err != nil {
				t.Fatal(err)
			}
			n, _ := io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if n != 5<<20 {
				t.Fatalf("size: %d bytes", n)
			}

			// Many requests at once share one connection.
			var wg sync.WaitGroup
			for i := range 50 {
				wg.Go(func() {
					resp, err := client.Post(fmt.Sprintf("%s/p/%d", url, i), "text/plain", strings.NewReader("x"))
					if err != nil {
						t.Error(err)
						return
					}
					got, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					if want := fmt.Sprintf("POST /p/%d 1", i); string(got) != want {
						t.Errorf("got %q, want %q", got, want)
					}
				})
			}
			wg.Wait()
		})
	}
}

// A GOAWAY from the client says that the client is closing the connection,
// not that the server should close it under the frames the client sent in the
// same breath. Answering those is what tells the client the connection ended
// in order: a socket closed while bytes it was sent sit unread is reset, and a
// reset costs the client the answers it did get along with the ones it did
// not. This is h2spec's generic 3.8 and 7.1, which follow their GOAWAY with a
// PING and expect it acknowledged or the connection cleanly closed.
func TestHTTP2ConformanceServerAnswersFramesAfterAClientGoAway(t *testing.T) {
	tc := dialH2(t, serveH2(t, false, DefaultConfig()))
	tc.write(h2AppendGoAway(nil, 0, H2NoError, ""))
	ping := h2AppendFrameHeader(nil, h2FramePing, 0, 0, 8)
	tc.write(append(ping, "h2spec  "...))
	if f := tc.readUntil(h2FramePing); !f.has(h2FlagAck) {
		t.Fatal("the PING behind the client's GOAWAY was not acknowledged")
	}
}

// TestHTTP2ConformanceServerStreamStates checks the stream-state rules of RFC
// 9113 section 5.1 with frames net/http's client would never send.
func TestHTTP2ConformanceServerStreamStates(t *testing.T) {
	t.Run("data after end stream", func(t *testing.T) {
		tc := dialH2(t, serveH2(t, false, DefaultConfig()))
		tc.headers(1, true, get("/")...)
		tc.response(1)
		tc.data(1, false, []byte("late"))
		f := tc.readUntil(h2FrameRSTStream)
		if code := H2ErrorCode(binary.BigEndian.Uint32(f.payload)); f.streamID != 1 || code != H2StreamClosed {
			t.Fatalf("stream %d code %v", f.streamID, code)
		}
	})
	t.Run("headers on closed stream", func(t *testing.T) {
		tc := dialH2(t, serveH2(t, false, DefaultConfig()))
		tc.headers(3, true, get("/")...)
		tc.response(3)
		tc.headers(1, true, get("/")...) // lower than one already used
		f := tc.readUntil(h2FrameGoAway)
		if code := H2ErrorCode(binary.BigEndian.Uint32(f.payload[4:])); code != H2ProtocolError {
			t.Fatalf("GOAWAY %v", code)
		}
	})
	t.Run("data on idle stream", func(t *testing.T) {
		tc := dialH2(t, serveH2(t, false, DefaultConfig()))
		tc.data(7, false, []byte("x"))
		f := tc.readUntil(h2FrameGoAway)
		if code := H2ErrorCode(binary.BigEndian.Uint32(f.payload[4:])); code != H2ProtocolError {
			t.Fatalf("GOAWAY %v", code)
		}
	})
	t.Run("priority depending on itself", func(t *testing.T) {
		tc := dialH2(t, serveH2(t, false, DefaultConfig()))
		payload := binary.BigEndian.AppendUint32(nil, 1)
		tc.write(append(h2AppendFrameHeader(nil, h2FramePriority, 0, 1, 5), append(payload, 0)...))
		f := tc.readUntil(h2FrameRSTStream)
		if code := H2ErrorCode(binary.BigEndian.Uint32(f.payload)); f.streamID != 1 || code != H2ProtocolError {
			t.Fatalf("stream %d code %v", f.streamID, code)
		}
	})
}

// TestHTTP2ConformanceServerHTTP2Only checks that an HTTP/2-only server ends
// a connection that does not start with the preface, rather than answering it
// in HTTP/1.
func TestHTTP2ConformanceServerHTTP2Only(t *testing.T) {
	config := DefaultConfig()
	config.HTTP2Only = true
	addr := serveH2(t, false, config)
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err = io.WriteString(c, "GET / HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(got), "HTTP/1.1 ") {
		t.Fatalf("answered in HTTP/1: %q", got)
	}
	// The server's own preface, then GOAWAY, then the connection closes.
	var sawGoAway bool
	for len(got) >= h2FrameHeaderLen {
		f, n, err := h2ReadFrame(got, h2MaxFrameSizeLimit)
		if err != nil || n == 0 {
			break
		}
		got = got[n:]
		if f.typ == h2FrameGoAway {
			sawGoAway = H2ErrorCode(binary.BigEndian.Uint32(f.payload[4:])) == H2ProtocolError
		}
	}
	if !sawGoAway {
		t.Fatal("no GOAWAY(PROTOCOL_ERROR)")
	}
}

// TestHTTP2ConformanceServerWithH2Spec runs h2spec, which checks the server
// against RFC 9113 and RFC 7541 case by case. CI installs it; elsewhere the
// test is skipped unless FIB_REQUIRE_H2SPEC is set.
func TestHTTP2ConformanceServerWithH2Spec(t *testing.T) {
	h2spec, err := exec.LookPath("h2spec")
	if err != nil {
		if os.Getenv("FIB_REQUIRE_H2SPEC") != "" {
			t.Fatalf("h2spec is required but missing: %v", err)
		}
		t.Skip("h2spec not installed")
	}
	// h2spec speaks HTTP/2 and nothing else, so the server serves nothing
	// else either: an invalid preface has to end the connection.
	config := DefaultConfig()
	config.HTTP2Only = true
	for _, secure := range []bool{false, true} {
		name := map[bool]string{false: "h2c", true: "h2"}[secure]
		t.Run(name, func(t *testing.T) {
			host, port, err := net.SplitHostPort(serveH2(t, secure, config))
			if err != nil {
				t.Fatal(err)
			}
			args := []string{"-h", host, "-p", port, "--timeout", "5"}
			if secure {
				args = append(args, "-t", "-k")
			}
			out, err := exec.Command(h2spec, args...).CombinedOutput()
			summary := lastLine(string(out))
			t.Logf("h2spec %s: %s", name, summary)
			if err != nil || !strings.Contains(summary, "0 failed") {
				t.Fatalf("h2spec reported failures: %s\n%s", summary, out)
			}
		})
	}
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}

// TestHTTP2ConformanceServerWithCurl checks the three ways curl speaks
// HTTP/2: prior knowledge, the HTTP/1.1 upgrade, and ALPN over TLS.
func TestHTTP2ConformanceServerWithCurl(t *testing.T) {
	curl, err := exec.LookPath("curl")
	if err != nil {
		if os.Getenv("FIB_REQUIRE_CURL") != "" {
			t.Fatalf("curl is required but missing: %v", err)
		}
		t.Skip("curl not installed")
	}
	if out, err := exec.Command(curl, "--version").Output(); err == nil && !strings.Contains(string(out), "HTTP2") {
		t.Skip("curl was built without HTTP/2")
	}
	plain := serveH2(t, false, DefaultConfig())
	secure := serveH2(t, true, DefaultConfig())
	caFile := writeTestCA(t)
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{"prior knowledge", []string{"--http2-prior-knowledge", "-d", "hi", "http://" + plain + "/pk"}, "POST /pk 2"},
		{"upgrade", []string{"--http2", "http://" + plain + "/up"}, "GET /up 0"},
		{"alpn", []string{"--http2", "--cacert", caFile, "-d", "hi", "https://" + secure + "/tls"}, "POST /tls 2"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"-sS", "--max-time", "20", "-w", "|%{http_version}"}, tt.args...)
			out, err := exec.Command(curl, args...).CombinedOutput()
			if err != nil {
				t.Fatalf("curl: %v: %s", err, out)
			}
			if want := tt.want + "|2"; string(out) != want {
				t.Fatalf("got %q, want %q", out, want)
			}
		})
	}
}

// writeTestCA writes the test certificate where curl can trust it.
func writeTestCA(t *testing.T) string {
	t.Helper()
	serverConfig, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ca.pem")
	block := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverConfig.Certificates[0].Certificate[0]})
	if err := os.WriteFile(path, block, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// ---------------------------------------------------------------------------
// The fib client

// TestHTTP2ConformanceClientWithNetHTTP drives the fib client against
// net/http's HTTP/2 server, over TLS with ALPN and in cleartext.
func TestHTTP2ConformanceClientWithNetHTTP(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(map[bool]string{false: "h2c", true: "h2"}[secure], func(t *testing.T) {
			var streams atomic.Int64
			handler := stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
				streams.Add(1)
				body, _ := io.ReadAll(r.Body)
				switch r.URL.Path {
				case "/size":
					n, _ := strconv.Atoi(r.URL.Query().Get("n"))
					_, _ = w.Write(make([]byte, n))
				case "/status":
					n, _ := strconv.Atoi(r.URL.Query().Get("code"))
					w.WriteHeader(n)
				case "/trailers":
					w.Header().Set("Trailer", "X-Checksum")
					_, _ = io.WriteString(w, "body")
					w.Header().Set("X-Checksum", "42")
				case "/hints":
					w.WriteHeader(stdhttp.StatusEarlyHints)
					_, _ = io.WriteString(w, "hinted")
				default:
					w.Header().Set("X-Proto", r.Proto)
					fmt.Fprintf(w, "%s %s %d", r.Method, r.URL.RequestURI(), len(body))
				}
			})
			ts := httptest.NewUnstartedServer(handler)
			if secure {
				ts.EnableHTTP2 = true
				ts.StartTLS()
			} else {
				ts.Config.Protocols = new(stdhttp.Protocols)
				ts.Config.Protocols.SetUnencryptedHTTP2(true)
				ts.Start()
			}
			t.Cleanup(ts.Close)

			config := DefaultClientConfig()
			if secure {
				config.TLSConfig = &stdtls.Config{RootCAs: certPool(t, ts)}
			} else {
				config.UnencryptedHTTP2 = true
			}
			client := newTestClient(t, config)

			for _, tt := range []struct{ method, path, body, want string }{
				{"GET", "/a?q=1", "", "GET /a?q=1 0"},
				{"POST", "/b", "hello", "POST /b 5"},
				{"PUT", "/c", strings.Repeat("x", 100000), "PUT /c 100000"},
				{"DELETE", "/d", "", "DELETE /d 0"},
			} {
				resp, err := client.Go(mustRequest(t, tt.method, ts.URL+tt.path, strings.NewReader(tt.body))).Wait()
				if err != nil {
					t.Fatalf("%s: %v", tt.method, err)
				}
				if got := readBody(t, resp); resp.ProtoMajor != 2 || got != tt.want {
					t.Fatalf("%s: %s %q, want %q", tt.method, resp.Proto, got, tt.want)
				}
			}

			// HEAD keeps the length and drops the body.
			resp, err := client.Go(mustRequest(t, stdhttp.MethodHead, ts.URL+"/h", nil)).Wait()
			if err != nil {
				t.Fatal(err)
			}
			if resp.ContentLength != int64(len("HEAD /h 0")) || readBody(t, resp) != "" {
				t.Fatalf("HEAD: length %d", resp.ContentLength)
			}

			// Bodiless statuses, trailers, and a body past the windows.
			for _, code := range []int{204, 304} {
				resp, err := client.Go(mustRequest(t, "GET", fmt.Sprintf("%s/status?code=%d", ts.URL, code), nil)).Wait()
				if err != nil {
					t.Fatal(err)
				}
				if resp.StatusCode != code || readBody(t, resp) != "" {
					t.Fatalf("%d: status %d", code, resp.StatusCode)
				}
			}
			resp, err = client.Go(mustRequest(t, "GET", ts.URL+"/trailers", nil)).Wait()
			if err != nil {
				t.Fatal(err)
			}
			if got := readBody(t, resp); got != "body" || resp.Trailer.Get("X-Checksum") != "42" {
				t.Fatalf("trailers: %q %v", got, resp.Trailer)
			}
			resp, err = client.Go(mustRequest(t, "GET", ts.URL+"/hints", nil)).Wait()
			if err != nil {
				t.Fatal(err)
			}
			if got := readBody(t, resp); resp.StatusCode != 200 || got != "hinted" {
				t.Fatalf("early hints: %d %q", resp.StatusCode, got)
			}
			resp, err = client.Go(mustRequest(t, "GET", fmt.Sprintf("%s/size?n=%d", ts.URL, 5<<20), nil)).Wait()
			if err != nil {
				t.Fatal(err)
			}
			if got := readBody(t, resp); len(got) != 5<<20 {
				t.Fatalf("size: %d bytes", len(got))
			}

			// Many requests at once, all on one connection.
			before := streams.Load()
			var wg sync.WaitGroup
			for i := range 50 {
				wg.Go(func() {
					resp, err := client.Go(mustRequest(t, "POST", fmt.Sprintf("%s/p/%d", ts.URL, i), strings.NewReader("x"))).Wait()
					if err != nil {
						t.Error(err)
						return
					}
					if want := fmt.Sprintf("POST /p/%d 1", i); readBody(t, resp) != want {
						t.Errorf("want %q", want)
					}
				})
			}
			wg.Wait()
			if got := streams.Load() - before; got != 50 {
				t.Fatalf("%d requests reached the server, want 50", got)
			}
		})
	}
}

func certPool(t *testing.T, ts *httptest.Server) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(ts.Certificate())
	return pool
}

// TestHTTP2ConformanceClientPreface checks what the client opens a connection
// with: the preface, its settings, and a request's pseudo-header fields.
func TestHTTP2ConformanceClientPreface(t *testing.T) {
	type result struct {
		settings map[h2SettingID]uint32
		fields   map[string]string
	}
	results := make(chan result, 1)
	addr := rawH2Server(t, nil, func(s *h2RawServer) {
		settings := map[h2SettingID]uint32{}
		req := s.nextRequest(func(f h2Frame) {
			if f.typ == h2FrameSettings && !f.has(h2FlagAck) {
				_ = h2ParseSettings(f.payload, func(id h2SettingID, v uint32) error {
					settings[id] = v
					return nil
				})
			}
		})
		if req == nil {
			return
		}
		results <- result{settings, req.fields}
		s.ok(req.id, "done")
	})
	config := DefaultClientConfig()
	config.UnencryptedHTTP2 = true
	client := newTestClient(t, config)
	req := mustRequest(t, stdhttp.MethodPost, "http://"+addr+"/path?q=1", strings.NewReader("body"))
	req.Header.Set("X-Custom", "v")
	req.Header.Set("Connection", "keep-alive") // HTTP/2 must not carry this
	resp, err := client.Go(req).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "done" {
		t.Fatalf("body %q", got)
	}
	got := <-results
	if push, ok := got.settings[h2SettingEnablePush]; !ok || push != 0 {
		t.Errorf("ENABLE_PUSH = %d, %v", push, ok)
	}
	if _, ok := got.settings[h2SettingInitialWindowSize]; !ok {
		t.Errorf("no INITIAL_WINDOW_SIZE in %v", got.settings)
	}
	want := map[string]string{
		":method": "POST", ":scheme": "http", ":authority": addr, ":path": "/path?q=1",
		"x-custom": "v", "content-length": "4",
	}
	for name, value := range want {
		if got.fields[name] != value {
			t.Errorf("%s = %q, want %q", name, got.fields[name], value)
		}
	}
	if _, ok := got.fields["connection"]; ok {
		t.Errorf("connection-specific header was sent: %v", got.fields)
	}
}

// TestHTTP2ConformanceClientRetries checks the two cases in which a request
// the server did not process is sent again: REFUSED_STREAM and a GOAWAY that
// leaves it past the last processed stream.
func TestHTTP2ConformanceClientRetries(t *testing.T) {
	t.Run("refused stream", func(t *testing.T) {
		var attempts atomic.Int64
		addr := rawH2Server(t, nil, func(s *h2RawServer) {
			for {
				req := s.nextRequest()
				if req == nil {
					return
				}
				if attempts.Add(1) == 1 {
					s.rst(req.id, H2RefusedStream)
					continue
				}
				s.ok(req.id, "second try")
			}
		})
		config := DefaultClientConfig()
		config.UnencryptedHTTP2 = true
		client := newTestClient(t, config)
		resp, err := client.Go(mustRequest(t, "POST", "http://"+addr+"/", strings.NewReader("x"))).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if got := readBody(t, resp); got != "second try" || attempts.Load() != 2 {
			t.Fatalf("body %q after %d attempts", got, attempts.Load())
		}
	})

	t.Run("goaway", func(t *testing.T) {
		var conns, attempts atomic.Int64
		addr := rawH2Server(t, nil, func(s *h2RawServer) {
			first := conns.Add(1) == 1
			for {
				req := s.nextRequest()
				if req == nil {
					return
				}
				attempts.Add(1)
				if first {
					// Nothing was processed, so the client must send it again.
					s.goAway(0, H2NoError)
					continue
				}
				s.ok(req.id, "new connection")
			}
		})
		config := DefaultClientConfig()
		config.UnencryptedHTTP2 = true
		client := newTestClient(t, config)
		resp, err := client.Go(mustRequest(t, "POST", "http://"+addr+"/", strings.NewReader("x"))).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if got := readBody(t, resp); got != "new connection" {
			t.Fatalf("body %q", got)
		}
		if conns.Load() != 2 || attempts.Load() != 2 {
			t.Fatalf("%d connections, %d attempts", conns.Load(), attempts.Load())
		}
	})
}

// TestHTTP2ConformanceClientStreamErrors checks that a reset stream fails its
// own request, and only that request.
func TestHTTP2ConformanceClientStreamErrors(t *testing.T) {
	addr := rawH2Server(t, nil, func(s *h2RawServer) {
		for {
			req := s.nextRequest()
			if req == nil {
				return
			}
			if strings.HasSuffix(req.fields[":path"], "/reset") {
				s.rst(req.id, H2InternalError)
				continue
			}
			s.ok(req.id, "fine")
		}
	})
	config := DefaultClientConfig()
	config.UnencryptedHTTP2 = true
	client := newTestClient(t, config)
	_, err := client.Go(mustRequest(t, "POST", "http://"+addr+"/reset", strings.NewReader("x"))).Wait()
	var streamErr *H2StreamError
	if !errors.As(err, &streamErr) || streamErr.Code != H2InternalError {
		t.Fatalf("err = %v", err)
	}
	// The connection carries on.
	resp, err := client.Go(mustRequest(t, "GET", "http://"+addr+"/ok", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "fine" {
		t.Fatalf("body %q", got)
	}
}

// TestHTTP2ConformanceClientRespectsFlowControl checks that the client keeps
// its DATA frames within the window a server advertises, and within its
// maximum frame size. The raw server checks every frame against the window it
// granted; this test makes the window a small one.
func TestHTTP2ConformanceClientRespectsFlowControl(t *testing.T) {
	const window, body = 4096, 200000
	type report struct{ size, frames, maxFrame int }
	reports := make(chan report, 1)
	addr := rawH2Server(t, []([2]uint32){{uint32(h2SettingInitialWindowSize), window}}, func(s *h2RawServer) {
		for {
			req := s.nextRequest()
			if req == nil {
				return
			}
			if req.fields[":path"] == "/warmup" {
				s.ok(req.id, "warm")
				continue
			}
			reports <- report{len(req.body), req.dataFrames, req.maxFrame}
			s.ok(req.id, "uploaded")
		}
	})
	config := DefaultClientConfig()
	config.UnencryptedHTTP2 = true
	client := newTestClient(t, config)
	// The warm-up request is answered only after the client has acknowledged
	// the server's SETTINGS, so the small window is in force for the upload.
	if _, err := client.Go(mustRequest(t, "GET", "http://"+addr+"/warmup", nil)).Wait(); err != nil {
		t.Fatal(err)
	}
	resp, err := client.Go(mustRequest(t, "POST", "http://"+addr+"/", strings.NewReader(strings.Repeat("u", body)))).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "uploaded" {
		t.Fatalf("body %q", got)
	}
	got := <-reports
	if got.size != body {
		t.Fatalf("server received %d bytes, want %d", got.size, body)
	}
	if got.maxFrame > h2DefaultMaxFrameSize {
		t.Fatalf("DATA frame of %d bytes exceeds the maximum frame size", got.maxFrame)
	}
	if got.frames < body/window {
		t.Fatalf("%d DATA frames for %d bytes through a window of %d", got.frames, body, window)
	}
}

// TestHTTP2ConformanceClientConcurrencyLimit checks that the client keeps to
// the server's SETTINGS_MAX_CONCURRENT_STREAMS.
func TestHTTP2ConformanceClientConcurrencyLimit(t *testing.T) {
	const limit = 2
	var open, peak atomic.Int64
	addr := rawH2Server(t, []([2]uint32){{uint32(h2SettingMaxConcurrentStreams), limit}}, func(s *h2RawServer) {
		var ids []uint32
		for {
			req := s.nextRequest()
			if req == nil {
				return
			}
			n := open.Add(1)
			for {
				top := peak.Load()
				if n <= top || peak.CompareAndSwap(top, n) {
					break
				}
			}
			ids = append(ids, req.id)
			// Answer the previous one only once another has arrived, so the
			// streams overlap as far as the client allows.
			if len(ids) >= 1 {
				s.ok(ids[0], "ok")
				ids = ids[1:]
				open.Add(-1)
			}
		}
	})
	config := DefaultClientConfig()
	config.UnencryptedHTTP2 = true
	config.MaxConnsPerHost = 1
	client := newTestClient(t, config)
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Go(func() {
			resp, err := client.Go(mustRequest(t, "GET", fmt.Sprintf("http://%s/%d", addr, i), nil)).Wait()
			if err != nil {
				t.Error(err)
				return
			}
			if got := readBody(t, resp); got != "ok" {
				t.Errorf("body %q", got)
			}
		})
	}
	wg.Wait()
	if peak.Load() > limit {
		t.Fatalf("%d streams were open at once, limit is %d", peak.Load(), limit)
	}
}

// TestHTTP2ConformanceClientRejectsPush checks that a server that pushes
// although the client disabled push ends the connection.
func TestHTTP2ConformanceClientRejectsPush(t *testing.T) {
	goAway := make(chan H2ErrorCode, 1)
	addr := rawH2Server(t, nil, func(s *h2RawServer) {
		req := s.nextRequest()
		if req == nil {
			return
		}
		block := s.enc.Begin(nil)
		for _, kv := range [][2]string{{":method", "GET"}, {":scheme", "http"}, {":authority", "x"}, {":path", "/pushed"}} {
			block = s.enc.AppendField(block, kv[0], kv[1], false)
		}
		s.write(h2AppendPushPromise(nil, req.id, 2, block, h2DefaultMaxFrameSize))
		for {
			f, ok := s.readFrame()
			if !ok {
				goAway <- H2NoError // the connection ended without one
				return
			}
			if f.typ == h2FrameGoAway {
				goAway <- H2ErrorCode(binary.BigEndian.Uint32(f.payload[4:]))
				return
			}
		}
	})
	config := DefaultClientConfig()
	config.UnencryptedHTTP2 = true
	client := newTestClient(t, config)
	if _, err := client.Go(mustRequest(t, "GET", "http://"+addr+"/", nil)).Wait(); err == nil {
		t.Fatal("request succeeded although the server pushed")
	}
	if code := <-goAway; code != H2ProtocolError {
		t.Fatalf("GOAWAY %v, want PROTOCOL_ERROR", code)
	}
}

// TestHTTP2ConformanceClientMalformedResponses checks that responses HTTP/2
// forbids fail their request rather than reaching the caller.
func TestHTTP2ConformanceClientMalformedResponses(t *testing.T) {
	for _, tt := range []struct {
		name   string
		fields []string
		body   string
	}{
		{"no status", []string{"content-length", "0"}, ""},
		{"connection header", []string{":status", "200", "connection", "keep-alive"}, ""},
		{"content-length mismatch", []string{":status", "200", "content-length", "100"}, "short"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			addr := rawH2Server(t, nil, func(s *h2RawServer) {
				req := s.nextRequest()
				if req == nil {
					return
				}
				s.respond(req.id, tt.body, tt.fields...)
				for {
					if _, ok := s.readFrame(); !ok {
						return
					}
				}
			})
			config := DefaultClientConfig()
			config.UnencryptedHTTP2 = true
			client := newTestClient(t, config)
			if resp, err := client.Go(mustRequest(t, "GET", "http://"+addr+"/", nil)).Wait(); err == nil {
				t.Fatalf("got %v, want an error", resp.Status)
			}
		})
	}
}

// TestHTTP2ConformanceClientSendsTrailers checks that a request's trailers
// travel in a HEADERS frame of their own, after the body, and reach both
// net/http's server and the fib server.
func TestHTTP2ConformanceClientSendsTrailers(t *testing.T) {
	newRequest := func(t *testing.T, url string) *stdhttp.Request {
		req := mustRequest(t, stdhttp.MethodPost, url, strings.NewReader("payload"))
		req.Trailer = stdhttp.Header{"X-Sum": {"42"}}
		return req
	}

	t.Run("raw server", func(t *testing.T) {
		type frames struct {
			order   []h2FrameType
			trailer map[string]string
		}
		got := make(chan frames, 1)
		addr := rawH2Server(t, nil, func(s *h2RawServer) {
			var order []h2FrameType
			req := s.nextRequest(func(f h2Frame) {
				if f.typ == h2FrameHeaders || f.typ == h2FrameData {
					order = append(order, f.typ)
				}
			})
			if req == nil {
				return
			}
			got <- frames{order, req.fields}
			s.ok(req.id, "ok")
		})
		config := DefaultClientConfig()
		config.UnencryptedHTTP2 = true
		client := newTestClient(t, config)
		if _, err := client.Go(newRequest(t, "http://"+addr+"/")).Wait(); err != nil {
			t.Fatal(err)
		}
		f := <-got
		if len(f.order) != 3 || f.order[0] != h2FrameHeaders || f.order[1] != h2FrameData || f.order[2] != h2FrameHeaders {
			t.Fatalf("frames %v, want HEADERS, DATA, HEADERS", f.order)
		}
		if f.trailer["x-sum"] != "42" {
			t.Fatalf("trailer field %v", f.trailer)
		}
	})

	t.Run("net/http server", func(t *testing.T) {
		ts := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
			body, _ := io.ReadAll(r.Body)
			fmt.Fprintf(w, "%s|%s", body, r.Trailer.Get("X-Sum"))
		}))
		ts.Config.Protocols = new(stdhttp.Protocols)
		ts.Config.Protocols.SetUnencryptedHTTP2(true)
		ts.Start()
		t.Cleanup(ts.Close)
		config := DefaultClientConfig()
		config.UnencryptedHTTP2 = true
		client := newTestClient(t, config)
		resp, err := client.Go(newRequest(t, ts.URL+"/")).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if got := readBody(t, resp); got != "payload|42" {
			t.Fatalf("got %q, want %q", got, "payload|42")
		}
	})

	t.Run("fib server", func(t *testing.T) {
		addr := serveH2(t, false, DefaultConfig())
		config := DefaultClientConfig()
		config.UnencryptedHTTP2 = true
		client := newTestClient(t, config)
		resp, err := client.Go(newRequest(t, "http://"+addr+"/")).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if got := resp.Header.Get("X-Sum"); got != "42" {
			t.Fatalf("the server saw trailer %q", got)
		}
	})
}

// TestHTTP2ConformanceClientReadsContinuationAndAnswersPing checks that the
// client puts a header block split across CONTINUATION frames back together,
// and answers the server's PING.
func TestHTTP2ConformanceClientReadsContinuationAndAnswersPing(t *testing.T) {
	pinged := make(chan []byte, 1)
	addr := rawH2Server(t, nil, func(s *h2RawServer) {
		req := s.nextRequest()
		if req == nil {
			return
		}
		s.write(append(h2AppendFrameHeader(nil, h2FramePing, 0, 0, 8), "fibping!"...))
		for {
			f, ok := s.readFrame()
			if !ok {
				return
			}
			if f.typ == h2FramePing && f.has(h2FlagAck) {
				pinged <- f.payload
				break
			}
		}
		// A header block in frames of five bytes at a time, so that most of
		// it arrives in CONTINUATION frames.
		block := s.enc.Begin(nil)
		block = s.enc.AppendField(block, ":status", "200", false)
		block = s.enc.AppendField(block, "content-length", "4", false)
		for i := range 20 {
			block = s.enc.AppendField(block, fmt.Sprintf("x-long-header-name-%d", i), strings.Repeat("v", 60), false)
		}
		out := h2AppendHeaderBlock(nil, req.id, block, false, 5)
		out = append(h2AppendFrameHeader(out, h2FrameData, h2FlagEndStream, req.id, 4), "body"...)
		s.write(out)
		for {
			if _, ok := s.readFrame(); !ok {
				return
			}
		}
	})
	config := DefaultClientConfig()
	config.UnencryptedHTTP2 = true
	client := newTestClient(t, config)
	resp, err := client.Go(mustRequest(t, "GET", "http://"+addr+"/", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "body" {
		t.Fatalf("body %q", got)
	}
	if got := resp.Header.Get("X-Long-Header-Name-19"); got != strings.Repeat("v", 60) {
		t.Fatalf("header from CONTINUATION = %q", got)
	}
	if got := <-pinged; string(got) != "fibping!" {
		t.Fatalf("PING ack carried %q", got)
	}
}

// TestHTTP2ConformanceFibClientAndServer runs both ends of HTTP/2 on fib,
// over TLS and in cleartext.
func TestHTTP2ConformanceFibClientAndServer(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(map[bool]string{false: "h2c", true: "h2"}[secure], func(t *testing.T) {
			addr := serveH2(t, secure, DefaultConfig())
			config := DefaultClientConfig()
			if secure {
				_, clientConfig, err := tlstest.Configs()
				if err != nil {
					t.Fatal(err)
				}
				config.TLSConfig = clientConfig
			} else {
				config.UnencryptedHTTP2 = true
			}
			client := newTestClient(t, config)
			url := scheme(secure) + addr
			var wg sync.WaitGroup
			for i := range 30 {
				wg.Go(func() {
					body := strings.Repeat("b", i*1000)
					resp, err := client.Go(mustRequest(t, "POST", fmt.Sprintf("%s/x/%d", url, i), strings.NewReader(body))).Wait()
					if err != nil {
						t.Error(err)
						return
					}
					want := fmt.Sprintf("POST /x/%d %d", i, len(body))
					if got := readBody(t, resp); resp.ProtoMajor != 2 || got != want {
						t.Errorf("%s %q, want %q", resp.Proto, got, want)
					}
				})
			}
			wg.Wait()
			resp, err := client.Go(mustRequest(t, "GET", fmt.Sprintf("%s/size?n=%d", url, 3<<20), nil)).Wait()
			if err != nil {
				t.Fatal(err)
			}
			if got := readBody(t, resp); len(got) != 3<<20 {
				t.Fatalf("%d bytes", len(got))
			}
		})
	}
}

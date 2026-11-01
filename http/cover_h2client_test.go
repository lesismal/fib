//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/hpack"
)

// cvH2cPeer is the server side of one h2c connection the client dialed to a
// fake server, which writes frames by hand.
type cvH2cPeer struct {
	conn net.Conn
	in   []byte
	enc  *hpack.Encoder
	dec  *hpack.Decoder
}

// cvH2cServe runs a fake HTTP/2 server with prior knowledge: serve gets each
// connection once the client's preface has been read. i counts connections.
func cvH2cServe(t *testing.T, serve func(p *cvH2cPeer, i int)) string {
	t.Helper()
	return rawServer(t, func(conn net.Conn, i int) {
		_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
		preface := make([]byte, len(h2Preface))
		if _, err := io.ReadFull(conn, preface); err != nil || string(preface) != h2Preface {
			return
		}
		serve(&cvH2cPeer{conn: conn, enc: hpack.NewEncoder(), dec: hpack.NewDecoder(hpack.DefaultTableSize)}, i)
	})
}

func (p *cvH2cPeer) write(b []byte) { _, _ = p.conn.Write(b) }

func (p *cvH2cPeer) frame(typ h2FrameType, flags uint8, id uint32, payload ...byte) {
	p.write(append(h2AppendFrameHeader(nil, typ, flags, id, len(payload)), payload...))
}

func (p *cvH2cPeer) settings(settings ...[2]uint32) { p.write(h2AppendSettings(nil, settings...)) }

// block encodes fields, given as name and value pairs.
func (p *cvH2cPeer) block(fields ...string) []byte {
	block := p.enc.Begin(nil)
	for i := 0; i < len(fields); i += 2 {
		block = p.enc.AppendField(block, fields[i], fields[i+1], false)
	}
	return block
}

func (p *cvH2cPeer) headers(id uint32, endStream bool, fields ...string) {
	p.write(h2AppendHeaderBlock(nil, id, p.block(fields...), endStream, h2DefaultMaxFrameSize))
}

// next reads the client's next frame, or reports false once it has closed.
func (p *cvH2cPeer) next() (h2Frame, bool) {
	buf := make([]byte, 64<<10)
	for {
		f, n, err := h2ReadFrame(p.in, h2MaxFrameSizeLimit)
		if err != nil {
			return h2Frame{}, false
		}
		if n > 0 {
			f.payload = append([]byte(nil), f.payload...)
			p.in = p.in[n:]
			return f, true
		}
		m, err := p.conn.Read(buf)
		p.in = append(p.in, buf[:m]...)
		if err != nil && m == 0 {
			return h2Frame{}, false
		}
	}
}

// await reads frames until one of typ, decoding every header block on the way
// so that the decoder keeps up with the client's encoder.
func (p *cvH2cPeer) await(typ h2FrameType) (h2Frame, map[string]string, bool) {
	for {
		f, ok := p.next()
		if !ok {
			return f, nil, false
		}
		var fields map[string]string
		if f.typ == h2FrameHeaders {
			fields = map[string]string{}
			_ = p.dec.Decode(f.payload, func(hf hpack.HeaderField) error {
				fields[hf.Name] = hf.Value
				return nil
			})
		}
		if f.typ == typ {
			return f, fields, true
		}
	}
}

// request waits for the HEADERS of the client's next request.
func (p *cvH2cPeer) request() (uint32, map[string]string, bool) {
	f, fields, ok := p.await(h2FrameHeaders)
	return f.streamID, fields, ok
}

// drain reads until the client closes the connection.
func (p *cvH2cPeer) drain() {
	for {
		if _, ok := p.next(); !ok {
			return
		}
	}
}

// cvH2cClient is a client that speaks h2c, with a timeout to keep a broken
// test from hanging.
func cvH2cClient(t *testing.T, mutate func(*ClientConfig)) *Client {
	t.Helper()
	config := DefaultClientConfig()
	config.Timeout = 10 * time.Second
	config.UnencryptedHTTP2 = true
	if mutate != nil {
		mutate(&config)
	}
	return newTestClient(t, config)
}

func cvH2cUint32(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }

// Frames that break the protocol end the whole connection with GOAWAY, and
// the request on it fails with the connection error.
func TestCoverH2ClientConnectionErrors(t *testing.T) {
	smallHeaders := func(c *ClientConfig) { c.MaxResponseHeaderBytes = 64 }
	cases := []struct {
		name       string
		config     func(*ClientConfig)
		noSettings bool
		frames     func(p *cvH2cPeer)
		code       H2ErrorCode
	}{
		{"preface not SETTINGS", nil, true, func(p *cvH2cPeer) {
			p.frame(h2FramePing, 0, 0, make([]byte, 8)...)
		}, H2ProtocolError},
		{"preface is SETTINGS ack", nil, true, func(p *cvH2cPeer) {
			p.frame(h2FrameSettings, h2FlagAck, 0)
		}, H2ProtocolError},
		{"HEADERS on stream 0", nil, false, func(p *cvH2cPeer) {
			p.write(h2AppendHeaderBlock(nil, 0, p.block(":status", "200"), true, h2DefaultMaxFrameSize))
		}, H2ProtocolError},
		{"HEADERS priority truncated", nil, false, func(p *cvH2cPeer) {
			p.frame(h2FrameHeaders, h2FlagEndHeaders|h2FlagPriority, 1, 1, 2, 3)
		}, H2FrameSizeError},
		{"CONTINUATION expected", nil, false, func(p *cvH2cPeer) {
			p.frame(h2FrameHeaders, 0, 1, p.block(":status", "200")...)
			p.frame(h2FramePing, 0, 0, make([]byte, 8)...)
		}, H2ProtocolError},
		{"CONTINUATION unexpected", nil, false, func(p *cvH2cPeer) {
			p.frame(h2FrameContinuation, h2FlagEndHeaders, 1, p.block(":status", "200")...)
		}, H2ProtocolError},
		{"HEADERS fragment too large", smallHeaders, false, func(p *cvH2cPeer) {
			p.frame(h2FrameHeaders, 0, 1, make([]byte, 100)...)
		}, H2EnhanceYourCalm},
		{"CONTINUATION too large", smallHeaders, false, func(p *cvH2cPeer) {
			p.frame(h2FrameHeaders, 0, 1, make([]byte, 40)...)
			p.frame(h2FrameContinuation, 0, 1, make([]byte, 40)...)
		}, H2EnhanceYourCalm},
		{"RST_STREAM on stream 0", nil, false, func(p *cvH2cPeer) {
			p.frame(h2FrameRSTStream, 0, 0, cvH2cUint32(uint32(H2Cancel))...)
		}, H2ProtocolError},
		{"PING short", nil, false, func(p *cvH2cPeer) {
			p.frame(h2FramePing, 0, 0, 1, 2, 3, 4)
		}, H2ProtocolError},
		{"GOAWAY short", nil, false, func(p *cvH2cPeer) {
			p.frame(h2FrameGoAway, 0, 0, 1, 2, 3, 4)
		}, H2ProtocolError},
		{"SETTINGS on a stream", nil, false, func(p *cvH2cPeer) {
			p.frame(h2FrameSettings, 0, 1)
		}, H2ProtocolError},
		{"SETTINGS ack with payload", nil, false, func(p *cvH2cPeer) {
			p.frame(h2FrameSettings, h2FlagAck, 0, make([]byte, 6)...)
		}, H2FrameSizeError},
		{"ENABLE_PUSH 2", nil, false, func(p *cvH2cPeer) {
			p.settings([2]uint32{uint32(h2SettingEnablePush), 2})
		}, H2ProtocolError},
		{"INITIAL_WINDOW_SIZE too large", nil, false, func(p *cvH2cPeer) {
			p.settings([2]uint32{uint32(h2SettingInitialWindowSize), 1 << 31})
		}, H2FlowControlError},
		{"MAX_FRAME_SIZE too small", nil, false, func(p *cvH2cPeer) {
			p.settings([2]uint32{uint32(h2SettingMaxFrameSize), 100})
		}, H2ProtocolError},
		{"stream window overflow by SETTINGS", nil, false, func(p *cvH2cPeer) {
			p.write(h2AppendWindowUpdate(nil, 1, h2MaxWindow-h2DefaultWindow))
			p.settings([2]uint32{uint32(h2SettingInitialWindowSize), h2DefaultWindow + 1})
		}, H2FlowControlError},
		{"WINDOW_UPDATE length", nil, false, func(p *cvH2cPeer) {
			p.frame(h2FrameWindowUpdate, 0, 0, 0, 0, 1)
		}, H2FrameSizeError},
		{"WINDOW_UPDATE of 0", nil, false, func(p *cvH2cPeer) {
			p.write(h2AppendWindowUpdate(nil, 0, 0))
		}, H2ProtocolError},
		{"connection window overflow", nil, false, func(p *cvH2cPeer) {
			p.write(h2AppendWindowUpdate(nil, 0, h2MaxWindow))
		}, H2FlowControlError},
		{"DATA on stream 0", nil, false, func(p *cvH2cPeer) {
			p.frame(h2FrameData, h2FlagEndStream, 0, 'x')
		}, H2ProtocolError},
		{"DATA past the connection window", func(c *ClientConfig) {
			// Frames larger than the window, which never drops below half
			// of it otherwise.
			c.HTTP2.ConnWindow, c.HTTP2.MaxFrameSize = h2DefaultWindow, 1<<17
		}, false, func(p *cvH2cPeer) {
			p.headers(1, false, ":status", "200")
			p.frame(h2FrameData, 0, 1, make([]byte, h2DefaultWindow+1)...)
		}, H2FlowControlError},
		{"bad HPACK", nil, false, func(p *cvH2cPeer) {
			p.frame(h2FrameHeaders, h2FlagEndHeaders|h2FlagEndStream, 1, 0x80)
		}, H2CompressionError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			goAway := make(chan H2ErrorCode, 1)
			url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
				if !tc.noSettings {
					p.settings()
				}
				if _, _, ok := p.request(); !ok {
					return
				}
				tc.frames(p)
				f, _, ok := p.await(h2FrameGoAway)
				if ok && len(f.payload) >= 8 {
					goAway <- H2ErrorCode(binary.BigEndian.Uint32(f.payload[4:]))
				}
				p.drain()
			})
			client := cvH2cClient(t, tc.config)
			// POST is not retried on another connection when this one fails.
			_, err := client.Go(mustRequest(t, "POST", url, nil)).Wait()
			var connErr *H2ConnError
			if !errors.As(err, &connErr) || connErr.Code != tc.code {
				t.Fatalf("err = %v, want a connection error %v", err, tc.code)
			}
			if code := <-goAway; code != tc.code {
				t.Fatalf("GOAWAY code %v, want %v", code, tc.code)
			}
		})
	}
}

// Frames that break only one stream reset it and fail its request, with the
// stream error or with the limit the response broke.
func TestCoverH2ClientStreamErrors(t *testing.T) {
	cases := []struct {
		name   string
		config func(*ClientConfig)
		frames func(p *cvH2cPeer, id uint32)
		code   H2ErrorCode
		err    error
	}{
		{"WINDOW_UPDATE of 0", nil, func(p *cvH2cPeer, id uint32) {
			p.write(h2AppendWindowUpdate(nil, id, 0))
		}, H2ProtocolError, nil},
		{"stream window overflow", nil, func(p *cvH2cPeer, id uint32) {
			p.write(h2AppendWindowUpdate(nil, id, h2MaxWindow))
		}, H2FlowControlError, nil},
		{"DATA before HEADERS", nil, func(p *cvH2cPeer, id uint32) {
			p.frame(h2FrameData, h2FlagEndStream, id, 'x')
		}, H2ProtocolError, nil},
		{"trailers without END_STREAM", nil, func(p *cvH2cPeer, id uint32) {
			p.headers(id, false, ":status", "200")
			p.headers(id, false, "x-t", "v")
		}, H2ProtocolError, nil},
		{"pseudo-header in trailers", nil, func(p *cvH2cPeer, id uint32) {
			p.headers(id, false, ":status", "200")
			p.headers(id, true, ":status", "200")
		}, H2ProtocolError, nil},
		{"1xx ends the stream", nil, func(p *cvH2cPeer, id uint32) {
			p.headers(id, true, ":status", "103")
		}, H2ProtocolError, nil},
		{"header list too large", func(c *ClientConfig) { c.MaxResponseHeaderBytes = 256 }, func(p *cvH2cPeer, id uint32) {
			fields := []string{":status", "200"}
			for i := range 8 {
				fields = append(fields, "x-h"+strconv.Itoa(i), "0123456789abcdef")
			}
			p.headers(id, true, fields...)
		}, H2Cancel, ErrResponseHeaderTooLarge},
		{"Content-Length too large", func(c *ClientConfig) { c.MaxResponseBodyBytes = 10 }, func(p *cvH2cPeer, id uint32) {
			p.headers(id, false, ":status", "200", "content-length", "100")
		}, H2Cancel, ErrResponseBodyTooLarge},
		{"body too large", func(c *ClientConfig) { c.MaxResponseBodyBytes = 10 }, func(p *cvH2cPeer, id uint32) {
			p.headers(id, false, ":status", "200")
			p.frame(h2FrameData, 0, id, make([]byte, 20)...)
		}, H2Cancel, ErrResponseBodyTooLarge},
		{"DATA past the stream window", func(c *ClientConfig) {
			c.HTTP2.StreamWindow, c.HTTP2.MaxFrameSize = h2DefaultWindow, 1<<17
		}, func(p *cvH2cPeer, id uint32) {
			p.headers(id, false, ":status", "200")
			p.frame(h2FrameData, 0, id, make([]byte, h2DefaultWindow+1)...)
		}, H2FlowControlError, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reset := make(chan H2ErrorCode, 1)
			url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
				p.settings()
				id, _, ok := p.request()
				if !ok {
					return
				}
				tc.frames(p, id)
				f, _, ok := p.await(h2FrameRSTStream)
				if ok && f.streamID == id && len(f.payload) == 4 {
					reset <- H2ErrorCode(binary.BigEndian.Uint32(f.payload))
				}
				p.drain()
			})
			client := cvH2cClient(t, tc.config)
			_, err := client.Go(mustRequest(t, "GET", url, nil)).Wait()
			if tc.err != nil {
				if !errors.Is(err, tc.err) {
					t.Fatalf("err = %v, want %v", err, tc.err)
				}
			} else {
				var streamErr *H2StreamError
				if !errors.As(err, &streamErr) || streamErr.Code != tc.code {
					t.Fatalf("err = %v, want a stream error %v", err, tc.code)
				}
			}
			if code := <-reset; code != tc.code {
				t.Fatalf("RST_STREAM code %v, want %v", code, tc.code)
			}
		})
	}
}

// PRIORITY frames, frames of unknown types and HEADERS for a stream the
// client does not have are passed over, and a HEADERS frame's priority
// fields are skipped.
func TestCoverH2ClientIgnoredFrames(t *testing.T) {
	url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
		p.settings()
		id, _, ok := p.request()
		if !ok {
			return
		}
		p.frame(h2FramePriority, 0, id, 0, 0, 0, 0, 16)
		p.frame(0xa, 0, 0, 1, 2, 3)
		p.headers(id+2, true, ":status", "200")
		priority := append([]byte{0, 0, 0, 0, 16}, p.block(":status", "200", "x-p", "1")...)
		p.frame(h2FrameHeaders, h2FlagEndHeaders|h2FlagEndStream|h2FlagPriority, id, priority...)
		p.drain()
	})
	client := cvH2cClient(t, nil)
	resp, err := client.Go(mustRequest(t, "GET", url, nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || resp.Header.Get("X-P") != "1" {
		t.Fatalf("status %d, X-P %q", resp.StatusCode, resp.Header.Get("X-P"))
	}
}

// A header block spread over CONTINUATION frames, larger than the client
// keeps buffers for, arrives whole.
func TestCoverH2ClientLargeHeaderBlock(t *testing.T) {
	big := strings.Repeat("a", 200000)
	url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
		p.settings()
		id, _, ok := p.request()
		if !ok {
			return
		}
		p.headers(id, true, ":status", "200", "x-big", big)
		p.drain()
	})
	client := cvH2cClient(t, nil)
	resp, err := client.Go(mustRequest(t, "GET", url, nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Header.Get("X-Big"); got != big {
		t.Fatalf("X-Big of %d bytes, want %d", len(got), len(big))
	}
}

// cvH2cEngine runs a client-only engine whose reads take up to readBuffer
// bytes at a time.
func cvH2cEngine(t *testing.T, readBuffer int) *fib.Engine {
	t.Helper()
	config := fib.DefaultConfig()
	config.ReadBufferSize = readBuffer
	engine, err := fib.New(config)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		_ = engine.Close()
	})
	return engine
}

// Frames that arrive in reads larger than the client keeps its input buffer
// for are taken whole, and the buffer is let go once they are.
func TestCoverH2ClientLargeReads(t *testing.T) {
	const size = 512 << 10
	pinged := make(chan bool, 1)
	url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
		p.settings()
		id, _, ok := p.request()
		if !ok {
			return
		}
		out := h2AppendHeaderBlock(nil, id, p.block(":status", "200", "content-length", strconv.Itoa(size)), false,
			h2DefaultMaxFrameSize)
		chunk := make([]byte, h2DefaultMaxFrameSize)
		for sent := 0; sent < size; sent += len(chunk) {
			flags := uint8(0)
			if sent+len(chunk) == size {
				flags = h2FlagEndStream
			}
			out = h2AppendFrameHeader(out, h2FrameData, flags, id, len(chunk))
			out = append(out, chunk...)
		}
		p.write(out)
		p.frame(h2FramePing, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8)
		for {
			f, _, ok := p.await(h2FramePing)
			if !ok {
				pinged <- false
				return
			}
			if f.has(h2FlagAck) {
				pinged <- true
				break
			}
		}
		p.drain()
	})
	config := DefaultClientConfig()
	config.Timeout = 10 * time.Second
	config.UnencryptedHTTP2 = true
	client := NewClient(cvH2cEngine(t, 1<<20), config)
	t.Cleanup(client.Close)
	resp, err := client.Go(mustRequest(t, "GET", url, nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if n := len(readBody(t, resp)); n != size {
		t.Fatalf("body of %d bytes, want %d", n, size)
	}
	if !<-pinged {
		t.Fatal("PING not acknowledged")
	}
}

// A streamed body whose reader has stopped holds the stream's window shut,
// and a server that sends past it has the stream reset.
func TestCoverH2ClientStreamedBodyOverWindow(t *testing.T) {
	reset := make(chan H2ErrorCode, 1)
	url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
		p.settings()
		id, _, ok := p.request()
		if !ok {
			return
		}
		p.headers(id, false, ":status", "200")
		chunk := make([]byte, h2DefaultMaxFrameSize)
		var out []byte
		for sent := 0; sent < h2StreamWindow+4*len(chunk); sent += len(chunk) {
			out = h2AppendFrameHeader(out, h2FrameData, 0, id, len(chunk))
			out = append(out, chunk...)
		}
		p.write(out)
		for {
			f, _, ok := p.await(h2FrameRSTStream)
			if !ok {
				return
			}
			if f.streamID == id && len(f.payload) == 4 {
				reset <- H2ErrorCode(binary.BigEndian.Uint32(f.payload))
				break
			}
		}
		p.drain()
	})
	client := cvH2cClient(t, func(c *ClientConfig) { c.StreamResponseBody = true })
	client.Do(mustRequest(t, "GET", url, nil), func(cr *ClientResponse, err error) {
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = cr.Body.Read(nil) // taken, but never read
	})
	if code := <-reset; code != H2FlowControlError {
		t.Fatalf("RST_STREAM code %v, want %v", code, H2FlowControlError)
	}
}

// The pseudo-headers of a request with no Host, with no method, and of a
// CONNECT, which has neither :scheme nor :path.
func TestCoverH2ClientRequestPseudoHeaders(t *testing.T) {
	seen := make(chan map[string]string, 3)
	url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
		p.settings()
		for {
			id, fields, ok := p.request()
			if !ok {
				return
			}
			seen <- fields
			p.headers(id, true, ":status", "200")
		}
	})
	host := strings.TrimPrefix(url, "http://")
	client := cvH2cClient(t, nil)

	req := mustRequest(t, "GET", url+"/x", nil)
	req.Host = ""
	req.Method = ""
	if _, err := client.Go(req).Wait(); err != nil {
		t.Fatal(err)
	}
	fields := <-seen
	if fields[":authority"] != host || fields[":method"] != "GET" || fields[":path"] != "/x" {
		t.Fatalf("fields %v", fields)
	}

	req = mustRequest(t, "CONNECT", url, nil)
	if _, err := client.Go(req).Wait(); err != nil {
		t.Fatal(err)
	}
	fields = <-seen
	_, scheme := fields[":scheme"]
	_, path := fields[":path"]
	if fields[":method"] != "CONNECT" || fields[":authority"] != host || scheme || path {
		t.Fatalf("CONNECT fields %v", fields)
	}
}

// Header and trailer fields HTTP/2 cannot carry fail the request before any
// of it is encoded, and the connection carries on.
func TestCoverH2ClientInvalidRequestFields(t *testing.T) {
	url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
		p.settings()
		for {
			id, _, ok := p.request()
			if !ok {
				return
			}
			p.headers(id, true, ":status", "200")
		}
	})
	client := cvH2cClient(t, nil)
	cases := []struct {
		name string
		set  func(req *stdhttp.Request)
		want string
	}{
		{"header name", func(req *stdhttp.Request) { req.Header["Bad Name"] = []string{"v"} }, "invalid request header name"},
		{"header value", func(req *stdhttp.Request) { req.Header["X-Bad"] = []string{"a\x01b"} }, "invalid request header value"},
		{"trailer name", func(req *stdhttp.Request) { req.Trailer = stdhttp.Header{"Bad Name": {"v"}} }, "invalid request trailer name"},
		{"trailer value", func(req *stdhttp.Request) { req.Trailer = stdhttp.Header{"X-Bad": {"a\x01b"}} }, "invalid request trailer value"},
	}
	for _, tc := range cases {
		req := mustRequest(t, "GET", url, nil)
		tc.set(req)
		if _, err := client.Go(req).Wait(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: err = %v, want %q", tc.name, err, tc.want)
		}
	}
	if _, err := client.Go(mustRequest(t, "GET", url, nil)).Wait(); err != nil {
		t.Fatal(err)
	}
}

// A request with trailers and no body sends its header block without
// END_STREAM, and the trailers end the stream.
func TestCoverH2ClientTrailersWithoutBody(t *testing.T) {
	trailer := make(chan map[string]string, 1)
	url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
		p.settings()
		id, fields, ok := p.request()
		if !ok {
			return
		}
		f, tr, ok := p.await(h2FrameHeaders)
		if !ok {
			return
		}
		if !f.has(h2FlagEndStream) || fields["trailer"] != "x-t" {
			tr = nil
		}
		trailer <- tr
		p.headers(id, true, ":status", "200")
		p.drain()
	})
	client := cvH2cClient(t, nil)
	req := mustRequest(t, "POST", url, nil)
	req.Trailer = stdhttp.Header{"X-T": {"v"}}
	if _, err := client.Go(req).Wait(); err != nil {
		t.Fatal(err)
	}
	if tr := <-trailer; tr["x-t"] != "v" {
		t.Fatalf("trailers %v", tr)
	}
}

// cvH2cTrailerBody is a body of unknown length that, once read, sets the
// request's trailers, some of which HTTP/2 cannot carry.
type cvH2cTrailerBody struct {
	req  *stdhttp.Request
	left int
}

func (b *cvH2cTrailerBody) Read(p []byte) (int, error) {
	if b.left == 0 {
		b.req.Trailer.Set("X-Ok", "v")
		b.req.Trailer["Bad Name"] = []string{"x"}
		b.req.Trailer["Connection"] = []string{"close"}
		return 0, io.EOF
	}
	n := min(len(p), b.left)
	for i := range n {
		p[i] = 'b'
	}
	b.left -= n
	return n, nil
}

func (b *cvH2cTrailerBody) Close() error { return nil }

// Trailers set while a streamed body is read go out without those HTTP/2
// cannot carry.
func TestCoverH2ClientStreamedTrailerSkipsInvalid(t *testing.T) {
	trailer := make(chan map[string]string, 1)
	url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
		p.settings()
		id, _, ok := p.request()
		if !ok {
			return
		}
		_, tr, ok := p.await(h2FrameHeaders)
		if !ok {
			return
		}
		trailer <- tr
		p.headers(id, true, ":status", "200")
		p.drain()
	})
	client := cvH2cClient(t, nil)
	req := mustRequest(t, "POST", url, nil)
	req.Body = &cvH2cTrailerBody{req: req, left: 1000}
	req.Trailer = stdhttp.Header{"X-Ok": nil}
	if _, err := client.Go(req).Wait(); err != nil {
		t.Fatal(err)
	}
	if tr := <-trailer; len(tr) != 1 || tr["x-ok"] != "v" {
		t.Fatalf("trailers %v, want only x-ok", tr)
	}
}

// cvH2cEndless is a body of unknown length that never ends.
type cvH2cEndless struct{}

func (cvH2cEndless) Read(p []byte) (int, error) { return len(p), nil }

// A streamed body waiting for window its server never gives stops once the
// server resets the stream.
func TestCoverH2ClientStreamedBodyStreamReset(t *testing.T) {
	url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
		p.settings()
		id, _, ok := p.request()
		if !ok {
			return
		}
		// The windows are 65535 bytes; once all of them has arrived the
		// sender is waiting for more.
		for got := 0; got < h2DefaultWindow; {
			f, _, ok := p.await(h2FrameData)
			if !ok {
				return
			}
			got += len(f.payload)
		}
		p.write(h2AppendRSTStream(nil, id, H2Cancel))
		p.drain()
	})
	client := cvH2cClient(t, nil)
	req := mustRequest(t, "POST", url, nil)
	req.Body = io.NopCloser(cvH2cEndless{})
	_, err := client.Go(req).Wait()
	var streamErr *H2StreamError
	if !errors.As(err, &streamErr) || streamErr.Code != H2Cancel {
		t.Fatalf("err = %v, want a stream error %v", err, H2Cancel)
	}
}

// The guards against a request that ended, or a connection that is going
// away, before the stream was opened, and against cancelling a stream the
// connection no longer has. Each is a window between taking a connection
// and using it, so they are driven directly.
func TestCoverH2ClientSendGuards(t *testing.T) {
	goAway, gone := make(chan struct{}), make(chan struct{})
	url := cvH2cServe(t, func(p *cvH2cPeer, i int) {
		p.settings()
		id, _, ok := p.request()
		if !ok {
			return
		}
		p.headers(id, true, ":status", "200", "x-conn", strconv.Itoa(i))
		if i == 0 {
			<-goAway
			p.write(h2AppendGoAway(nil, id, H2NoError, ""))
			p.drain()
			close(gone)
			return
		}
		p.drain()
	})
	client := cvH2cClient(t, nil)
	if _, err := client.Go(mustRequest(t, "GET", url, nil)).Wait(); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	cc := client.hosts[hostTarget{addr: strings.TrimPrefix(url, "http://")}].h2[0]
	client.mu.Unlock()
	hc := cc.h2

	// A stream the connection does not have.
	hc.cancel(&clientRequest{streamID: 99})

	// A request settled while it waited gives its stream back unsent.
	settled := &clientRequest{req: mustRequest(t, "GET", url, nil)}
	settled.done.Store(true)
	client.mu.Lock()
	cc.streams++
	client.mu.Unlock()
	hc.send(settled)
	client.mu.Lock()
	streams := cc.streams
	client.mu.Unlock()
	if streams != 0 {
		t.Fatalf("streams = %d after a settled request, want 0", streams)
	}

	// After GOAWAY the request goes out on another connection.
	close(goAway)
	<-gone
	result := make(chan string, 1)
	r := &clientRequest{req: mustRequest(t, "GET", url, nil), callback: func(cr *ClientResponse, err error) {
		if err != nil {
			result <- err.Error()
			return
		}
		result <- cr.Header.Get("X-Conn")
	}}
	client.mu.Lock()
	cc.streams++
	client.mu.Unlock()
	hc.send(r)
	if got := <-result; got != "1" {
		t.Fatalf("resent request: %q, want an answer on connection 1", got)
	}
}

// An HTTP/2 connection idle for IdleConnTimeout is closed.
func TestCoverH2ClientIdleTimeout(t *testing.T) {
	closed := make(chan struct{})
	url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
		p.settings()
		id, _, ok := p.request()
		if !ok {
			return
		}
		p.headers(id, true, ":status", "200")
		p.drain()
		close(closed)
	})
	client := cvH2cClient(t, func(c *ClientConfig) { c.IdleConnTimeout = 10 * time.Millisecond })
	if _, err := client.Go(mustRequest(t, "GET", url, nil)).Wait(); err != nil {
		t.Fatal(err)
	}
	<-closed
	client.mu.Lock()
	n := len(client.hosts[hostTarget{addr: strings.TrimPrefix(url, "http://")}].h2)
	client.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d HTTP/2 connections pooled after the idle timeout", n)
	}
}

// h2NewResponse refuses a pseudo-header after a field or a second :status, a
// 101, and a malformed Content-Length.
func TestCoverH2ClientNewResponseRejects(t *testing.T) {
	req := mustRequest(t, "GET", "http://example.com", nil)
	cases := [][]hpack.HeaderField{
		{{Name: ":status", Value: "200"}, {Name: ":status", Value: "200"}},
		{{Name: "x", Value: "1"}, {Name: ":status", Value: "200"}},
		{{Name: ":path", Value: "/"}},
		{{Name: ":status", Value: "101"}},
		{{Name: ":status", Value: "200"}, {Name: "content-length", Value: "-1"}},
		{{Name: ":status", Value: "200"}, {Name: "content-length", Value: "x"}},
	}
	for _, fields := range cases {
		if resp, err := h2NewResponse(fields, req); err == nil {
			t.Fatalf("%v: got %v, want an error", fields, resp)
		}
	}
}

// With AdaptiveWindow, the client measures what arrives in a round trip
// with a PING, and a server that filled the stream window within it has the
// windows grown, with SETTINGS and a WINDOW_UPDATE; the response still
// arrives whole.
func TestCoverH2ClientAdaptiveWindow(t *testing.T) {
	grown := make(chan uint32, 1)
	url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
		p.settings()
		id, _, ok := p.request()
		if !ok {
			return
		}
		p.headers(id, false, ":status", "200")
		chunk := make([]byte, h2DefaultMaxFrameSize)
		for sent := 0; sent+len(chunk) <= h2DefaultWindow; sent += len(chunk) {
			p.frame(h2FrameData, 0, id, chunk...)
		}
		// The client's measurement started with the first DATA; the window
		// is all but full, so the acknowledgement shows that the window held
		// the server back.
		for {
			f, ok := p.next()
			if !ok {
				return
			}
			if f.typ == h2FramePing && !f.has(h2FlagAck) {
				p.frame(h2FramePing, h2FlagAck, 0, f.payload...)
				break
			}
		}
		for {
			f, ok := p.next()
			if !ok {
				return
			}
			if f.typ != h2FrameSettings || f.has(h2FlagAck) {
				continue
			}
			_ = h2ParseSettings(f.payload, func(id h2SettingID, value uint32) error {
				if id == h2SettingInitialWindowSize {
					grown <- value
				}
				return nil
			})
			break
		}
		p.frame(h2FrameData, h2FlagEndStream, id, chunk[:10]...)
		p.drain()
	})
	client := cvH2cClient(t, func(c *ClientConfig) {
		c.HTTP2 = HTTP2Config{StreamWindow: h2DefaultWindow, AdaptiveWindow: true}
	})
	resp, err := client.Go(mustRequest(t, "GET", url, nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	want := h2DefaultWindow/h2DefaultMaxFrameSize*h2DefaultMaxFrameSize + 10
	if n := len(readBody(t, resp)); n != want {
		t.Fatalf("body of %d bytes, want %d", n, want)
	}
	if window := cvCliWait(t, grown, "the grown window"); window <= h2DefaultWindow {
		t.Fatalf("window set to %d", window)
	}
}

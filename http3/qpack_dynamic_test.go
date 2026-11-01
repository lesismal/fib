//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"bytes"
	"errors"
	"io"
	stdhttp "net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/qpack"
	"github.com/lesismal/fib/http3/quic"
)

// withQPACK turns a client's dynamic table on.
func withQPACK(capacity, blocked int) func(*ClientConfig) {
	return func(c *ClientConfig) {
		c.QPACKTableCapacity, c.QPACKBlockedStreams = capacity, blocked
	}
}

// clientEncoder is the QPACK encoder of the client's one connection.
func clientEncoder(client *Client) *qpack.Encoder {
	client.mu.Lock()
	defer client.mu.Unlock()
	for _, cc := range client.conns {
		return cc.peer.qpackEncoder()
	}
	return nil
}

// A fib client and server with the dynamic table on both ways: large fields
// that repeat, in headers and trailers of requests and responses, arrive
// intact, and each encoder has entries the other side acknowledged.
func TestQPACKDynamicTable(t *testing.T) {
	cookie := "session=" + strings.Repeat("c", 600)
	token := "Bearer " + strings.Repeat("t", 200)
	meta := strings.Repeat("m", 300)
	var serverEncoder atomic.Pointer[qpack.Encoder]
	url := startServer(t, Config{QPACKTableCapacity: 4096, QPACKBlockedStreams: 8}, func(c *fibhttp.Context) {
		if rs := csStreamOf(c); rs != nil {
			serverEncoder.Store(rs.encoder())
		}
		body, _ := io.ReadAll(c.Request.Body)
		_ = c.WriteResponse(fibhttp.Response{StatusCode: 200,
			Header: stdhttp.Header{
				"X-Meta":        {meta},
				"X-Cookie":      {c.Request.Header.Get("Cookie")},
				"X-Auth":        {c.Request.Header.Get("Authorization")},
				"X-Rpc":         {c.Request.Header.Get("X-Rpc-Service")},
				"X-Req-Trailer": {c.Request.Trailer.Get("X-Req-Sum")},
			},
			Body:    body,
			Trailer: stdhttp.Header{"X-Sum": {meta}}})
	})
	client := newClient(t, withQPACK(4096, 8))
	deadline := time.Now().Add(csWait)
	for i := 0; ; i++ {
		req := mustRequest(t, stdhttp.MethodPost, url+"/rpc", strings.NewReader("payload"))
		req.Header.Set("Cookie", cookie)
		req.Header.Set("Authorization", token)
		req.Header.Set("X-Rpc-Service", "inventory.v1.Items/List")
		req.Trailer = stdhttp.Header{"X-Req-Sum": {meta}}
		resp, err := client.Go(req).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if body := readBody(t, resp); body != "payload" || resp.Header.Get("X-Meta") != meta ||
			resp.Header.Get("X-Cookie") != cookie || resp.Header.Get("X-Auth") != token ||
			resp.Header.Get("X-Rpc") != "inventory.v1.Items/List" || resp.Header.Get("X-Req-Trailer") != meta ||
			resp.Trailer.Get("X-Sum") != meta {
			t.Fatalf("request %d: body %q, header %v, trailer %v", i, body, resp.Header, resp.Trailer)
		}
		// The acknowledgments travel on their own streams: the loop goes on
		// until both have come, from the third request on.
		_, clientKnown := clientEncoder(client).Counts()
		_, serverKnown := serverEncoder.Load().Counts()
		if i >= 2 && clientKnown > 0 && serverKnown > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after %d requests the client has %d entries acknowledged, the server %d", i+1, clientKnown, serverKnown)
		}
	}
}

// A dynamic table on one side only is a table on neither: the side that
// has one sees the peer allow none, and the side that does not refuses
// inserts it never gets.
func TestQPACKDynamicTableOneSided(t *testing.T) {
	for _, serverOn := range []bool{true, false} {
		config := Config{}
		configure := withQPACK(4096, 8)
		if serverOn {
			config, configure = Config{QPACKTableCapacity: 4096, QPACKBlockedStreams: 8}, nil
		}
		var serverEncoder atomic.Pointer[qpack.Encoder]
		url := startServer(t, config, func(c *fibhttp.Context) {
			if rs := csStreamOf(c); rs != nil {
				serverEncoder.Store(rs.encoder())
			}
			_ = c.WriteResponse(fibhttp.Response{StatusCode: 200, Header: stdhttp.Header{"X-Meta": {strings.Repeat("m", 100)}},
				Body: []byte(c.Request.Header.Get("X-Big"))})
		})
		client := newClient(t, configure)
		for i := 0; i < 4; i++ {
			req := mustRequest(t, stdhttp.MethodGet, url+"/", nil)
			req.Header.Set("X-Big", strings.Repeat("b", 100))
			resp, err := client.Go(req).Wait()
			if err != nil {
				t.Fatal(err)
			}
			if body := readBody(t, resp); body != strings.Repeat("b", 100) {
				t.Fatalf("server on %v: body %q", serverOn, body)
			}
		}
		enc := clientEncoder(client)
		if serverOn {
			enc = serverEncoder.Load()
		}
		if inserted, _ := enc.Counts(); inserted != 0 {
			t.Fatalf("server on %v: %d entries inserted into a table the peer does not allow", serverOn, inserted)
		}
	}
}

// The peer's SETTINGS may arrive before this side has turned its table on,
// and the encoder starts all the same, at the lesser of the two
// capacities.
func TestQPACKSettingsFirst(t *testing.T) {
	p := &peerStreams{}
	settings := appendSettings([]byte{streamControl}, [2]uint64{settingQPACKMaxTableCapacity, 100})
	if err := newUniStream(p, nil, nil).handle(settings, false); err != nil {
		t.Fatal(err)
	}
	var wrote []byte
	p.setQPACK(&dynamicQPACK{enc: qpack.NewEncoder(64, func(b []byte) error {
		wrote = append(wrote, b...)
		return nil
	})})
	if !bytes.Equal(wrote, []byte{0x3f, 64 - 31}) {
		t.Fatalf("encoder stream %x, want a capacity of 64", wrote)
	}
}

// rawStreams collects what a raw peer receives on each stream.
type rawStreams struct {
	mu      sync.Mutex
	data    map[*quic.Stream][]byte
	fin     map[*quic.Stream]bool
	changed chan struct{}
}

func newRawStreams() *rawStreams {
	return &rawStreams{data: map[*quic.Stream][]byte{}, fin: map[*quic.Stream]bool{}, changed: make(chan struct{}, 1)}
}

func (r *rawStreams) on(s *quic.Stream, data []byte, fin bool) {
	r.mu.Lock()
	r.data[s] = append(r.data[s], data...)
	if fin {
		r.fin[s] = true
	}
	r.mu.Unlock()
	select {
	case r.changed <- struct{}{}:
	default:
	}
}

// wait waits for cond, which runs with r held, to hold.
func (r *rawStreams) wait(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.After(csWait)
	for {
		r.mu.Lock()
		ok := cond()
		r.mu.Unlock()
		if ok {
			return
		}
		select {
		case <-r.changed:
		case <-deadline:
			t.Fatalf("no %s", what)
		}
	}
}

// decoderStream is what has arrived on the peer's QPACK decoder stream,
// after its type; r is held.
func (r *rawStreams) decoderStream() []byte {
	for s, data := range r.data {
		if !s.Bidirectional() && len(data) > 0 && data[0] == streamQPACKDecoder {
			return data[1:]
		}
	}
	return nil
}

// response reads a response whose header is encoded without the dynamic
// table: its fields and body.
func rawResponse(t *testing.T, data []byte) ([]qpack.HeaderField, string) {
	t.Helper()
	var fields []qpack.HeaderField
	var body []byte
	p := frameParser{maxFrame: 1 << 20}
	err := p.feed(data, func(chunk []byte) error {
		body = append(body, chunk...)
		return nil
	}, func(typ uint64, payload []byte) error {
		if typ == frameHeaders && fields == nil {
			return qpack.Decode(payload, 0, func(f qpack.HeaderField) error {
				fields = append(fields, f)
				return nil
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return fields, string(body)
}

// The encoder stream instructions a raw client sends: a capacity of 4096,
// :authority localhost and x-dyn v1.
var rawInserts = []byte{
	0x3f, 0xe1, 0x1f,
	0xc0, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
	0x45, 'x', '-', 'd', 'y', 'n', 0x02, 'v', '1',
}

// rawBlockedHeaders is a POST whose :authority and x-dyn are the first two
// entries of the table, after the section's base, which the server waits
// for.
var rawBlockedHeaders = appendHeadersFrame(nil, []byte{0x03, 0x81, 0xc0 | 20, 0xc0 | 23, 0x10, 0xc1, 0x11})

// dynamicEcho answers with the authority, x-dyn and the body.
func dynamicEcho(c *fibhttp.Context) {
	body, _ := io.ReadAll(c.Request.Body)
	_ = c.WriteResponse(fibhttp.Response{StatusCode: 200,
		Body: []byte(c.Request.Host + " " + c.Request.Header.Get("X-Dyn") + " " + string(body))})
}

func dialQPACK(t *testing.T, config Config) (*rawConn, *rawStreams, *quic.Stream) {
	t.Helper()
	r := dialRaw(t, startServer(t, config, dynamicEcho))
	got := newRawStreams()
	r.setOnStream(got.on)
	r.rawControl(t)
	encoder, err := openUni(r.qc, streamQPACKEncoder)
	if err != nil {
		t.Fatal(err)
	}
	return r, got, encoder
}

// A request whose header refers to entries the encoder stream has not
// brought yet waits for them, along with the body that follows it, and is
// served once they come; the server acknowledges the section. A server
// that streams request bodies gives back what it held as it arrived.
func TestQPACKBlockedRequest(t *testing.T) {
	for _, streamed := range []bool{false, true} {
		r, got, encoder := dialQPACK(t, Config{QPACKTableCapacity: 4096, QPACKBlockedStreams: 1, StreamRequestBody: streamed})
		s, err := r.qc.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		_ = s.Write(rawBlockedHeaders, false)
		_ = s.Write(append(appendFrameHeader(nil, frameData, 4), "body"...), true)
		_ = encoder.Write(rawInserts, false)
		got.wait(t, "response", func() bool { return got.fin[s] })
		got.mu.Lock()
		fields, body := rawResponse(t, got.data[s])
		got.mu.Unlock()
		if len(fields) == 0 || fields[0].Value != "200" || body != "localhost v1 body" {
			t.Fatalf("streamed %v: response %v %q", streamed, fields, body)
		}
		got.wait(t, "section acknowledgment", func() bool { return bytes.HasSuffix(got.decoderStream(), []byte{0x80}) })
	}
}

// What the client's QPACK streams carry has to fit what the server's table
// holds and what its encoder sent.
func TestQPACKStreamErrors(t *testing.T) {
	r, _, encoder := dialQPACK(t, Config{QPACKTableCapacity: 4096})
	// A duplicate of an entry the table does not have.
	_ = encoder.Write([]byte{0x00}, false)
	r.expectClose(t, ErrCodeQPACKEncoderStreamError)

	r, _, _ = dialQPACK(t, Config{QPACKTableCapacity: 4096})
	decoder, err := openUni(r.qc, streamQPACKDecoder)
	if err != nil {
		t.Fatal(err)
	}
	// An acknowledgment of a section the server never sent.
	_ = decoder.Write([]byte{0x80}, false)
	r.expectClose(t, ErrCodeQPACKDecoderStreamError)
}

// A server whose client lets it open too few streams for its QPACK streams
// cannot serve it.
func TestQPACKStreamsRefused(t *testing.T) {
	url := startServer(t, Config{QPACKTableCapacity: 4096}, echo)
	client := newClient(t, func(c *ClientConfig) { c.Transport.MaxIncomingUniStreams = 1 })
	if _, err := client.Go(mustRequest(t, stdhttp.MethodGet, url+"/", nil)).Wait(); err == nil {
		t.Fatal("served without the server's control stream")
	}
}

// A server allows as many blocked streams as it said: one more is a
// decompression failure (RFC 9204 section 2.1.2).
func TestQPACKBlockedStreamsLimit(t *testing.T) {
	r, _, _ := dialQPACK(t, Config{QPACKTableCapacity: 4096, QPACKBlockedStreams: 1})
	for i := 0; i < 2; i++ {
		s, err := r.qc.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		_ = s.Write(rawBlockedHeaders, false)
	}
	r.expectClose(t, ErrCodeQPACKDecompressionFailed)
}

// A blocked stream the client resets is cancelled on the decoder stream,
// and no longer counts against the limit.
func TestQPACKBlockedStreamReset(t *testing.T) {
	r, got, encoder := dialQPACK(t, Config{QPACKTableCapacity: 4096, QPACKBlockedStreams: 1})
	s, err := r.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write(rawBlockedHeaders, false)
	// A request that needs no table, answered once the server has the
	// blocked one: a reset any sooner could overtake its header.
	plain, err := r.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = plain.Write(headersFrame(
		qpack.HeaderField{Name: ":method", Value: "GET"},
		qpack.HeaderField{Name: ":scheme", Value: "https"},
		qpack.HeaderField{Name: ":authority", Value: "localhost"},
		qpack.HeaderField{Name: ":path", Value: "/"},
	), true)
	got.wait(t, "response", func() bool { return got.fin[plain] })
	s.Reset(uint64(ErrCodeRequestCancelled))
	got.wait(t, "stream cancellation", func() bool { return bytes.Contains(got.decoderStream(), []byte{0x40}) })
	next, err := r.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = next.Write(rawBlockedHeaders, false)
	_ = next.Write(append(appendFrameHeader(nil, frameData, 1), "x"...), true)
	_ = encoder.Write(rawInserts, false)
	got.wait(t, "response", func() bool { return got.fin[next] })
	got.mu.Lock()
	_, body := rawResponse(t, got.data[next])
	got.mu.Unlock()
	if body != "localhost v1 x" {
		t.Fatalf("body %q", body)
	}
}

// A blocked stream that has more arrive behind its section than it may
// hold is reset, and cancelled on the decoder stream.
func TestQPACKBlockedStreamOverflow(t *testing.T) {
	r, got, _ := dialQPACK(t, Config{MaxHeaderBytes: 1024, QPACKTableCapacity: 4096, QPACKBlockedStreams: 1})
	s, err := r.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write(rawBlockedHeaders, false)
	chunk := make([]byte, 64<<10)
	_ = s.Write(appendFrameHeader(nil, frameData, 2<<20), false)
	for sent := 0; sent <= 1024+maxHeldBytes; sent += len(chunk) {
		_ = s.Write(chunk, false)
	}
	select {
	case code := <-r.resets:
		if ErrorCode(code) != ErrCodeExcessiveLoad {
			t.Fatalf("reset with %v, want %v", ErrorCode(code), ErrCodeExcessiveLoad)
		}
	case <-time.After(csWait):
		t.Fatal("not reset")
	}
	got.wait(t, "stream cancellation", func() bool { return bytes.Contains(got.decoderStream(), []byte{0x40}) })
}

// A server's dynamic table, as a peer that may make the client wait fills
// it: the encoder stream, opened at the handshake, and what it writes on it.
type rawQPACKServer struct {
	encoder *quic.Stream
	// uni collects what the client sends on its unidirectional streams.
	uni *rawStreams
}

// startQPACKServer serves each request by respond, given the server's
// encoder stream.
func startQPACKServer(t *testing.T, respond func(q *rawQPACKServer, s *quic.Stream)) (*rawServer, *rawQPACKServer, string) {
	t.Helper()
	q := &rawQPACKServer{uni: newRawStreams()}
	server, url := startRawServerFor(t, &rawServer{
		setup: func(control *quic.Stream) {
			q.encoder, _ = openUni(control.Conn(), streamQPACKEncoder)
		},
		respond: func(_ *rawServer, s *quic.Stream) {
			// Written apart, outside the connection's calls, so that the
			// pieces arrive apart.
			go respond(q, s)
		},
		onUni: func(s *quic.Stream, data []byte) { q.uni.on(s, data, false) },
	})
	return server, q, url
}

// The server's inserts: a capacity of 4096 and x-dyn v.
var rawServerInserts = []byte{0x3f, 0xe1, 0x1f, 0x45, 'x', '-', 'd', 'y', 'n', 0x01, 'v'}

// rawBlockedResponse is a 200 whose x-dyn is the table's first entry.
var rawBlockedResponse = appendHeadersFrame(nil, []byte{0x02, 0x80, 0xc0 | 25, 0x10})

// A response whose header refers to entries the encoder stream has not
// brought yet waits for them, with its body, and the client acknowledges
// it. A client that streams response bodies gives back what it held as it
// arrived, and not again as the body is consumed.
func TestClientQPACKBlockedResponse(t *testing.T) {
	for _, streamed := range []bool{false, true} {
		_, q, url := startQPACKServer(t, func(q *rawQPACKServer, s *quic.Stream) {
			_ = s.Write(rawBlockedResponse, false)
			_ = s.Write(append(appendFrameHeader(nil, frameData, 2), "ok"...), true)
			_ = q.encoder.Write(rawServerInserts, false)
		})
		client := newClient(t, func(c *ClientConfig) {
			c.QPACKTableCapacity, c.QPACKBlockedStreams, c.StreamResponseBody = 4096, 4, streamed
		})
		if streamed {
			var run streamRun
			run.run(t, client, mustRequest(t, stdhttp.MethodGet, url+"/", nil))
			if string(run.data) != "ok" || run.err != nil {
				t.Fatalf("streamed body %q, %v", run.data, run.err)
			}
		} else {
			resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, url+"/", nil)).Wait()
			if err != nil {
				t.Fatal(err)
			}
			if body := readBody(t, resp); body != "ok" || resp.Header.Get("X-Dyn") != "v" {
				t.Fatalf("body %q, header %v", body, resp.Header)
			}
		}
		q.uni.wait(t, "section acknowledgment", func() bool { return bytes.HasSuffix(q.uni.decoderStream(), []byte{0x80}) })
	}
}

// A client allows as many blocked streams as it said, here none.
func TestClientQPACKBlockedStreamsLimit(t *testing.T) {
	server, _, url := startQPACKServer(t, func(_ *rawQPACKServer, s *quic.Stream) {
		_ = s.Write(rawBlockedResponse, true)
	})
	client := newClient(t, withQPACK(4096, 0))
	go func() { _, _ = client.Go(mustRequest(t, stdhttp.MethodGet, url+"/", nil)).Wait() }()
	select {
	case err := <-server.closed:
		var appErr *quic.ApplicationError
		if !errors.As(err, &appErr) || ErrorCode(appErr.Code) != ErrCodeQPACKDecompressionFailed {
			t.Fatalf("closed with %v", err)
		}
	case <-time.After(csWait):
		t.Fatal("not closed")
	}
}

// A blocked response the server resets is cancelled on the client's
// decoder stream.
func TestClientQPACKBlockedResponseReset(t *testing.T) {
	_, q, url := startQPACKServer(t, func(_ *rawQPACKServer, s *quic.Stream) {
		_ = s.Write(rawBlockedResponse, false)
		s.Reset(uint64(ErrCodeInternalError))
	})
	client := newClient(t, withQPACK(4096, 4))
	if _, err := client.Go(mustRequest(t, stdhttp.MethodGet, url+"/", nil)).Wait(); err == nil {
		t.Fatal("a reset response succeeded")
	}
	q.uni.wait(t, "stream cancellation", func() bool { return bytes.Contains(q.uni.decoderStream(), []byte{0x40}) })
}

// A blocked response with more behind it than the client holds fails, and
// is cancelled on the decoder stream.
func TestClientQPACKBlockedResponseOverflow(t *testing.T) {
	_, q, url := startQPACKServer(t, func(_ *rawQPACKServer, s *quic.Stream) {
		_ = s.Write(rawBlockedResponse, false)
		chunk := make([]byte, 64<<10)
		_ = s.Write(appendFrameHeader(nil, frameData, 2<<20), false)
		for sent := 0; sent <= 1024+maxHeldBytes; sent += len(chunk) {
			if s.Write(chunk, false) != nil {
				return
			}
		}
	})
	client := newClient(t, func(c *ClientConfig) {
		c.QPACKTableCapacity, c.QPACKBlockedStreams, c.MaxResponseHeaderBytes = 4096, 4, 1024
	})
	if _, err := client.Go(mustRequest(t, stdhttp.MethodGet, url+"/", nil)).Wait(); !errors.Is(err, errHeaderTooLarge) {
		t.Fatalf("got %v, want %v", err, errHeaderTooLarge)
	}
	q.uni.wait(t, "stream cancellation", func() bool { return bytes.Contains(q.uni.decoderStream(), []byte{0x40}) })
}

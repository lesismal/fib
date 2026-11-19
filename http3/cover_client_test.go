//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/lesismal/fib/bufferpool"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/qpack"
	"github.com/lesismal/fib/http3/quic"
	"github.com/lesismal/fib/internal/reqbody"
	fibtls "github.com/lesismal/fib/tls"
)

// ccvConn is the client's one connection, which a request has brought up.
func ccvConn(t *testing.T, client *Client) *clientConn {
	t.Helper()
	client.mu.Lock()
	defer client.mu.Unlock()
	for _, cc := range client.conns {
		return cc
	}
	t.Fatal("the client has no connection")
	return nil
}

// ccvWait waits for a Future, failing the test if it never settles.
func ccvWait(t *testing.T, f *Future) (*stdhttp.Response, error) {
	t.Helper()
	select {
	case <-f.Done():
	case <-time.After(15 * time.Second):
		t.Fatal("the request never settled")
	}
	return f.Wait()
}

// ccvRequest is req as Do would queue it, its callback reporting its outcome
// on the channel it returns. A body that streams becomes its upload.
func ccvRequest(t *testing.T, req *stdhttp.Request) (*clientRequest, chan error) {
	t.Helper()
	outcome := make(chan error, 1)
	r := &clientRequest{req: req, callback: func(_ *fibhttp.ClientResponse, err error) { outcome <- err }}
	length := int64(0)
	if reqbody.Streams(req) {
		r.upload = reqbody.NewSource(req)
		length = reqbody.Declared(req)
	} else if req.Body != nil {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatal(err)
		}
		r.body, length = body, int64(len(body))
	}
	r.declared = length
	fields, trailer, err := requestFields(req, length)
	if err != nil {
		t.Fatal(err)
	}
	r.fields, r.trailer = fields, trailer
	return r, outcome
}

// Zero and negative limits in a ClientConfig take the defaults, a missing TLS
// config is made, and one of the caller's is copied rather than changed to
// carry HTTP/3's ALPN and TLS 1.3. A client that sends early data keeps the
// sessions to send it with, in a cache of its own when given none.
func TestCcNewClientDefaults(t *testing.T) {
	defaults := DefaultClientConfig()
	for _, config := range []ClientConfig{{}, {
		MaxResponseHeaderBytes: -1, MaxResponseBodyBytes: -1, HandshakeTimeout: -1, MaxIdleTimeout: -1,
	}} {
		c := NewClient(nil, config)
		got := c.config
		if got.MaxResponseHeaderBytes != defaults.MaxResponseHeaderBytes || got.MaxResponseBodyBytes != defaults.MaxResponseBodyBytes ||
			got.HandshakeTimeout != defaults.HandshakeTimeout || got.MaxIdleTimeout != defaults.MaxIdleTimeout {
			t.Fatalf("limits %+v, want the defaults %+v", got, defaults)
		}
		if c.tlsConfig == nil || len(c.tlsConfig.NextProtos) != 1 || c.tlsConfig.NextProtos[0] != NextProto ||
			c.tlsConfig.MinVersion != tls.VersionTLS13 {
			t.Fatalf("TLS config %+v", c.tlsConfig)
		}
		if c.sessions != nil {
			t.Fatal("a client without early data keeps sessions of its own")
		}
	}
	own := &tls.Config{MinVersion: tls.VersionTLS12}
	if c := NewClient(nil, ClientConfig{TLSConfig: own, EarlyData: true}); c.sessions == nil {
		t.Fatal("a client with early data and no SessionCache has no cache")
	}
	if own.MinVersion != tls.VersionTLS12 || own.NextProtos != nil {
		t.Fatalf("the caller's TLS config was changed: %+v", own)
	}
}

// What Do settles without a connection: a request whose body cannot be read
// fails with the read's error, and a nil callback is allowed. A body small
// enough to be read whole is sent behind its HEADERS with the trailer the
// request carries, announced in a Trailer field (RFC 9114 section 4.1).
func TestCcDoEdgeCases(t *testing.T) {
	server, base := startScriptServer(t, nil)
	type sent struct {
		header, trailer []qpack.HeaderField
		body            string
	}
	received := make(chan sent, 1)
	server.handle("/trailer", func(s *quic.Stream, _ int) {
		var got sent
		p := frameParser{maxFrame: 1 << 20}
		_ = p.feed(s.Context.(*scriptBuffer).data, func(b []byte) error {
			got.body += string(b)
			return nil
		}, func(typ uint64, payload []byte) error {
			if typ != frameHeaders {
				return nil
			}
			var fields []qpack.HeaderField
			_ = qpack.Decode(payload, 0, func(f qpack.HeaderField) error {
				fields = append(fields, f)
				return nil
			})
			if got.header == nil {
				got.header = fields
			} else {
				got.trailer = fields
			}
			return nil
		})
		received <- got
		writeFrames(s, responseFrames("200", "content-length", "2"), dataFrame([]byte("ok")))
	})
	client := newClient(t, nil)

	// No callback: the request still goes.
	client.Do(mustRequest(t, stdhttp.MethodGet, base+"/nil-callback", nil), nil)
	server.awaitArrival(t, "/nil-callback")

	boom := errors.New("boom")
	req := mustRequest(t, stdhttp.MethodPost, base+"/unreadable", nil)
	req.Body, req.ContentLength = io.NopCloser(iotest.ErrReader(boom)), 5
	if _, err := ccvWait(t, client.Go(req)); !errors.Is(err, boom) {
		t.Fatalf("unreadable body: %v", err)
	}

	req = mustRequest(t, stdhttp.MethodPost, base+"/trailer", strings.NewReader("hello"))
	req.Trailer = stdhttp.Header{"X-Sum": {"7"}}
	checkOK(t, client.Go(req), "ok")
	got := <-received
	announced := false
	for _, f := range got.header {
		announced = announced || f.Name == "trailer" && f.Value == "x-sum"
	}
	if got.body != "hello" || !announced || len(got.trailer) != 1 || got.trailer[0] != (qpack.HeaderField{Name: "x-sum", Value: "7"}) {
		t.Fatalf("the server got %+v", got)
	}
}

// A connection that never comes up fails what waits on it and is forgotten,
// so the next request dials again: an address that does not resolve, fails
// at once or once the resolver has said; a TLS config that cannot make a
// TLS 1.3 handshake fails as QUIC starts it, with either handshake.
func TestCcDialFailures(t *testing.T) {
	_, base := startScriptServer(t, nil)
	client := newClient(t, nil)
	for _, url := range []string{"https://127.0.0.1:99999/", "https://localhost:99999/"} {
		for i := 0; i < 2; i++ {
			if _, err := ccvWait(t, client.Go(mustRequest(t, stdhttp.MethodGet, url, nil))); err == nil {
				t.Fatalf("%s: a request to a port out of range succeeded", url)
			}
		}
	}
	client.mu.Lock()
	conns := len(client.conns)
	client.mu.Unlock()
	if conns != 0 {
		t.Fatalf("%d failed connections kept", conns)
	}
	for _, blocking := range []bool{false, true} {
		client := newClient(t, func(config *ClientConfig) {
			config.TLSConfig.MaxVersion = tls.VersionTLS12
			config.BlockingTLSHandshake = blocking
		})
		if _, err := ccvWait(t, client.Go(mustRequest(t, stdhttp.MethodGet, base+"/", nil))); err == nil {
			t.Fatalf("blocking %v: a client held to TLS 1.2 made an HTTP/3 connection", blocking)
		}
	}
}

// A TLS config without a ServerName verifies the server under the request's
// host.
func TestCcServerNameFromURL(t *testing.T) {
	_, base := startScriptServer(t, nil)
	client := newClient(t, func(config *ClientConfig) { config.TLSConfig.ServerName = "" })
	resp, err := ccvWait(t, client.Go(mustRequest(t, stdhttp.MethodGet, base+"/", nil)))
	if err != nil || resp.StatusCode != stdhttp.StatusOK {
		t.Fatalf("%v, %v", resp, err)
	}
	if resp.TLS == nil || resp.TLS.ServerName != "localhost" {
		t.Fatalf("connection state %+v", resp.TLS)
	}
}

// The UDP socket's handler hands a datagram to the QUIC connection once
// there is one, and drops it before; the calls fib makes that have nothing to
// do are harmless. A datagram that is not QUIC is dropped by the connection,
// which goes on.
func TestCcClientPathCallbacks(t *testing.T) {
	p := clientPath{&clientConn{}}
	p.OnOpen(nil)
	p.OnPriorityData(nil, []byte{1})
	p.OnData(nil, []byte{1})

	_, base := startScriptServer(t, nil)
	client := newClient(t, nil)
	checkOK(t, client.Go(mustRequest(t, stdhttp.MethodGet, base+"/", nil)), "")
	junk := bufferpool.Get(64)
	for i := range junk {
		junk[i] = 0x40
	}
	clientPath{ccvConn(t, client)}.OnData(nil, junk)
	checkOK(t, client.Go(mustRequest(t, stdhttp.MethodGet, base+"/", nil)), "")
}

// A request whose stream refuses its first write fails with the stream's
// error, its streaming body closed, and the stream is let go; a stream the
// client has already finished with takes no notice of a reset after.
func TestCcSendOnClosedStream(t *testing.T) {
	_, base := startScriptServer(t, nil)
	client := newClient(t, nil)
	checkOK(t, client.Go(mustRequest(t, stdhttp.MethodGet, base+"/", nil)), "")
	cc := ccvConn(t, client)

	closed := make(chan struct{})
	upload := mustRequest(t, stdhttp.MethodPost, base+"/upload", nil)
	upload.Body = closeNotifier{strings.NewReader("streamed"), closed}
	for _, req := range []*stdhttp.Request{
		mustRequest(t, stdhttp.MethodPost, base+"/whole", strings.NewReader("whole")),
		upload,
	} {
		r, outcome := ccvRequest(t, req)
		cc.mu.Lock()
		qc := cc.qc
		cc.mu.Unlock()
		s, err := qc.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		s.Reset(uint64(ErrCodeRequestCancelled))
		st := &clientStream{cc: cc, s: s, r: r}
		if !r.attach(st) {
			t.Fatal("a fresh request would not attach")
		}
		st.send()
		select {
		case err := <-outcome:
			if !errors.Is(err, quic.ErrClosed) {
				t.Fatalf("%s: %v, want the stream's error", req.URL.Path, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: the request did not fail", req.URL.Path)
		}
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the streaming body was not closed")
	}
	// Done already: a reset has nothing to settle, and touches nothing.
	(&clientStream{done: true}).reset(ErrCodeInternalError)

	checkOK(t, client.Go(mustRequest(t, stdhttp.MethodGet, base+"/", nil)), "")
}

// A request settled already, by its timeout or context while its stream was
// being opened or its response was arriving, is neither attached to the
// stream nor handed the response's stream.
func TestCcSettledRequestGuards(t *testing.T) {
	r := &clientRequest{done: true}
	if r.attach(&clientStream{}) || r.stream != nil {
		t.Fatal("a settled request took a stream")
	}
	if r.begin(&fibhttp.ResponseStream{}) || r.fired {
		t.Fatal("a settled request began a response")
	}
}

// A connection that ends with no error given, as Abort(nil) ends it, fails
// what is in flight on it as closed.
func TestCcConnectionEndsWithoutError(t *testing.T) {
	server, base, _ := startHoldServer(t)
	client := newClient(t, nil)
	held := client.Go(mustRequest(t, stdhttp.MethodGet, base+"/hold", nil))
	server.awaitArrival(t, "/hold")
	// The dial's callback stores qc under the connection's lock, on the
	// engine's goroutine: it is read under it here too.
	cc := ccvConn(t, client)
	cc.mu.Lock()
	qc := cc.qc
	cc.mu.Unlock()
	qc.Abort(nil)
	if _, err := ccvWait(t, held); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("in flight: %v, want net.ErrClosed", err)
	}
}

// A frame other than DATA longer than the client takes is refused as soon as
// its header says so, before the rest of it arrives, by closing the
// connection with H3_EXCESSIVE_LOAD.
func TestCcFrameTooLarge(t *testing.T) {
	server, base := startScriptServer(t, nil)
	server.handle("/huge", func(s *quic.Stream, _ int) {
		_ = s.Write(appendFrameHeader(nil, frameHeaders, 1<<20), false)
	})
	client := newClient(t, func(config *ClientConfig) { config.MaxResponseHeaderBytes = 4096 })
	if _, err := ccvWait(t, client.Go(mustRequest(t, stdhttp.MethodGet, base+"/huge", nil))); err == nil {
		t.Fatal("the request succeeded")
	}
	server.expectClose(t, "/huge", ErrCodeExcessiveLoad)
}

// Before the handshake of a resumed session is over, only requests that are
// safe to repeat go as 0-RTT data: a DELETE waits for the handshake while the
// GET behind it goes. A ticket a connection has used already has its early
// data turned away (RFC 8446 section 8), and the GET then goes again, and is
// answered; both count as early data rejected.
func TestCcEarlyDataReplayedTicket(t *testing.T) {
	url := startServer(t, Config{EarlyData: true}, earlyHandler)
	e := newEarlyClients(t)
	if _, body := do(t, e.new(), "GET", url+"/first", ""); body != `GET early=""` {
		t.Fatalf("first: %q", body)
	}
	e.waitSession()
	replay := fibtls.NewQUICSessionCache(4)
	replay.Put("localhost", e.cache.Get("localhost"))
	// The ticket's own connection, whose early data is taken.
	if _, body := do(t, e.new(), "GET", url+"/second", ""); body != `GET early="1"` {
		t.Fatalf("second: %q", body)
	}

	third := newClient(t, func(c *ClientConfig) {
		c.EarlyData = true
		c.SessionCache = replay
	})
	del := third.Go(mustRequest(t, stdhttp.MethodDelete, url+"/delete", nil))
	get := third.Go(mustRequest(t, stdhttp.MethodGet, url+"/replayed", nil))
	for name, f := range map[string]*Future{"DELETE": del, "GET": get} {
		resp, err := ccvWait(t, f)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if body := readBody(t, resp); body != name+` early=""` {
			t.Fatalf("%s: %q, want it taken after the handshake", name, body)
		}
	}
	if st := third.Stats(); st.EarlyAccepted != 0 || st.EarlyRejected != st.EarlySent {
		t.Fatalf("stats %+v, want any early data rejected", st)
	}
}

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/quic"
	"github.com/lesismal/fib/internal/reqbody"
)

// ceAwaitFieldLimit waits for the client's SETTINGS to have given rs's
// connection a field section limit.
func ceAwaitFieldLimit(rs *requestStream) bool {
	deadline := time.Now().Add(csWait)
	for time.Now().Before(deadline) {
		if _, limited := rs.fieldLimit(); limited {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// The checks a request stream makes before it touches its QUIC stream, on
// streams with no connection: a response that has not begun streaming has
// nothing to end, one already written cannot begin again or be upgraded,
// and the helpers that read fields or write a trailer take nothing.
func TestCeRequestStreamRefusals(t *testing.T) {
	if err := (&requestStream{}).EndResponse(nil, true); !errors.Is(err, errStreamClosed) {
		t.Fatalf("ending a response that never began: %v", err)
	}
	req := mustRequest(t, stdhttp.MethodConnect, "https://localhost/", nil)
	if err := (&requestStream{responded: true}).BeginResponse(req, stdhttp.StatusOK, nil); !errors.Is(err, fibhttp.ErrResponseWritten) {
		t.Fatalf("beginning a response twice: %v", err)
	}
	req.Header.Set(":protocol", "ccv")
	if _, err := (&requestStream{tunnel: &streamTunnel{}, responded: true}).Upgrade(req, "ccv", nil); !errors.Is(err, fibhttp.ErrResponseWritten) {
		t.Fatalf("upgrading an answered stream: %v", err)
	}
	if isContinue(nil) {
		t.Fatal("no fields read as a 100 Continue")
	}
	if out := appendTrailer(nil, nil); len(out) != 0 {
		t.Fatalf("an empty trailer appended %x", out)
	}
	// What has nothing to act on does nothing.
	h := NewHandler(nil, nil)
	h.OnPriorityData(nil, nil)
	h.OnEngineClose(&fib.Connection{})
	(&serverConn{}).OnStreamsAvailable(nil)
	if (&peerPath{peer: &udpPeer{c: &fib.Connection{}}}).NotifyWritable(func() {}) {
		t.Fatal("a connection that is not UDP took a writable callback")
	}
}

// A client's field section limit bounds the header of a response that
// streams, an interim response's and a tunnel's 200, each refused before it
// is sent; and a QUIC stream reset under the handler fails each write a
// response that streams makes, and a tunnel's 200.
func TestCeResponseStreamOnServer(t *testing.T) {
	results := make(ccvResults, 16)
	big := stdhttp.Header{"X-Big": {strings.Repeat("b", 700)}}
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
		rs := csStreamOf(c)
		switch c.Request.URL.Path {
		case "/limit":
			if !ceAwaitFieldLimit(rs) {
				results.report("limit", errors.New("no field section limit"))
				return
			}
			results.report("begin too large", rs.BeginResponse(c.Request, stdhttp.StatusOK, big))
			results.report("interim too large", rs.WriteInterim(stdhttp.StatusEarlyHints, big))
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
		case "/connect-limit":
			if !ceAwaitFieldLimit(rs) {
				results.report("limit", errors.New("no field section limit"))
				return
			}
			_, err := c.Upgrade("ccv", big, &ccvTunnel{})
			results.report("upgrade too large", err)
			_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", []byte("bad"))
		case "/notify":
			// Nothing is waiting to be sent, so there is nothing to wait for.
			if rs.NotifyWritable(func(error) {}) {
				results.report("notify", errors.New("a stream with room took a callback"))
			} else {
				results.report("notify", nil)
			}
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
		case "/reset":
			rs.s.Reset(uint64(ErrCodeInternalError))
			results.report("begin", rs.BeginResponse(c.Request, stdhttp.StatusOK, nil))
			results.report("body", rs.WriteBody([]byte("x"), false))
			_, err := rs.WriteBodyNoWait([]byte("x"))
			results.report("body no wait", err)
			results.report("end", rs.EndResponse(nil, true))
		case "/connect-reset":
			rs.s.Reset(uint64(ErrCodeInternalError))
			_, err := c.Upgrade("ccv", nil, &ccvTunnel{})
			results.report("upgrade on reset", err)
		}
	})
	w := dialH3Wire(t, nil, base)
	if _, err := openControl(w.qc, 600); err != nil {
		t.Fatal(err)
	}

	id := w.send(t, h3Request{path: "/limit"}, false)
	for _, what := range []string{"begin too large", "interim too large"} {
		if err := results.expect(t, what); !errors.Is(err, errFieldSectionTooLarge) {
			t.Fatalf("%s: %v", what, err)
		}
	}
	if status, body := w.response(id); status != stdhttp.StatusOK || string(body) != "ok" {
		t.Fatalf("limit: %d %q", status, body)
	}

	s := csOpen(t, w, ccvConnect("/connect-limit"), false)
	if err := results.expect(t, "upgrade too large"); !errors.Is(err, errFieldSectionTooLarge) {
		t.Fatalf("upgrade with a header past the limit: %v", err)
	}
	w.await(s.ID(), "the refusal", func(st *h3WireStream) bool { return st.status == stdhttp.StatusBadRequest })

	id = w.send(t, h3Request{path: "/notify"}, false)
	if err := results.expect(t, "notify"); err != nil {
		t.Fatal(err)
	}
	if status, _ := w.response(id); status != stdhttp.StatusOK {
		t.Fatalf("notify: %d", status)
	}

	id = w.send(t, h3Request{path: "/reset"}, false)
	for _, what := range []string{"begin", "body", "body no wait", "end"} {
		if err := results.expect(t, what); !errors.Is(err, errStreamClosed) {
			t.Fatalf("%s on a reset stream: %v", what, err)
		}
	}
	w.await(id, "the reset", func(st *h3WireStream) bool { return st.reset })

	s = csOpen(t, w, ccvConnect("/connect-reset"), false)
	if err := results.expect(t, "upgrade on reset"); !errors.Is(err, errStreamClosed) {
		t.Fatalf("upgrading a reset stream: %v", err)
	}
	w.await(s.ID(), "the reset", func(st *h3WireStream) bool { return st.reset })
}

// A server that leaves requests to the collector serves them as one that
// pools them does; a whole response whose trailer has nothing HTTP/3 can
// carry is sent without one; and a response abandoned under running read
// and write deadlines stops both.
func TestCeServerWithoutReuse(t *testing.T) {
	results := make(ccvResults, 4)
	base := startServer(t, Config{DisableReuse: true, StreamRequestBody: true, WriteTimeout: time.Minute},
		func(c *fibhttp.Context) {
			switch c.Request.URL.Path {
			case "/trailer":
				_ = c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusOK, Body: []byte("body"),
					Trailer: stdhttp.Header{"Content-Type": {"text/plain"}}})
			case "/abandon":
				results.report("read deadline", c.SetReadDeadline(time.Now().Add(time.Minute)))
				c.Header()["Bad Name"] = []string{"x"}
				_, _ = c.WriteString("x")
				results.report("flush", c.FlushError())
			}
		})
	w := dialH3Wire(t, nil, base)
	id := w.send(t, h3Request{path: "/trailer"}, false)
	if status, body := w.response(id); status != stdhttp.StatusOK || string(body) != "body" {
		t.Fatalf("trailer: %d %q", status, body)
	}
	w.mu.Lock()
	trailer := w.streams[id].trailer
	w.mu.Unlock()
	if len(trailer) != 0 {
		t.Fatalf("sent the trailer %v", trailer)
	}

	// The request's body is still coming, so its read deadline is running.
	s := csOpen(t, w, csFrames(csRequest(stdhttp.MethodPost, "/abandon", "content-length", "10"), dataFrame(make([]byte, 2))), false)
	if err := results.expect(t, "read deadline"); err != nil {
		t.Fatal(err)
	}
	if err := results.expect(t, "flush"); err == nil {
		t.Fatal("a bad header began a response")
	}
	csAwaitReset(t, w, s)
}

// A request trailer past the server's field section limit fails the request
// rather than leave the server a body it would take for whole, whether the
// body is read from a Reader or pushed through a BodyWriter; and a request
// whose :protocol HTTP/3 cannot carry is refused before it is sent.
func TestCeClientRequestLimits(t *testing.T) {
	base := startServer(t, Config{StreamRequestBody: true, MaxHeaderBytes: 1024}, func(c *fibhttp.Context) {
		if c.Request.Method == stdhttp.MethodGet {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
			return
		}
		c.OnBody(func(_ []byte, fin bool, err error) {
			if fin || err != nil {
				_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("uploaded"))
			}
		})
	})
	client := newClient(t, nil)
	if resp, err := ccvWait(t, client.Go(mustRequest(t, stdhttp.MethodGet, base+"/", nil))); err != nil {
		t.Fatal(err)
	} else {
		_ = readBody(t, resp)
	}
	cc := ccvConn(t, client)
	deadline := time.Now().Add(csWait)
	for _, limited := cc.peer.fieldLimit(); !limited; _, limited = cc.peer.fieldLimit() {
		if time.Now().After(deadline) {
			t.Fatal("the server's SETTINGS never arrived")
		}
		time.Sleep(time.Millisecond)
	}
	big := strings.Repeat("t", 2000)

	req := mustRequest(t, stdhttp.MethodPost, base+"/upload", nil)
	// The trailer's value is set once the body has been read, as a checksum
	// of it would be.
	req.Trailer = stdhttp.Header{"X-Crc": nil}
	req.Body = io.NopCloser(&ceAtEOF{r: strings.NewReader("streamed"), eof: func() { req.Trailer.Set("X-Crc", big) }})
	if _, err := ccvWait(t, client.Go(req)); !errors.Is(err, errFieldSectionTooLarge) {
		t.Fatalf("a read body's trailer past the limit: %v", err)
	}

	body := fibhttp.NewBodyWriter(0)
	req = mustRequest(t, stdhttp.MethodPost, base+"/upload", nil)
	req.Body = body
	req.Trailer = stdhttp.Header{"X-Crc": nil}
	future := client.Go(req)
	if _, err := body.Write([]byte("pushed")); err != nil {
		t.Fatal(err)
	}
	_ = body.CloseWithTrailer(stdhttp.Header{"X-Crc": {big}})
	if _, err := ccvWait(t, future); !errors.Is(err, errFieldSectionTooLarge) {
		t.Fatalf("a pushed body's trailer past the limit: %v", err)
	}

	req = mustRequest(t, stdhttp.MethodConnect, base+"/", nil)
	req.Header[":protocol"] = []string{"bad\x00protocol"}
	if _, _, err := requestFields(req, 0); err == nil || !strings.Contains(err.Error(), "invalid :protocol") {
		t.Fatalf("a :protocol with a NUL: %v", err)
	}
}

// A response stream that ends before any HEADERS fails its request; a
// pushed body already sent cannot go again when the server rejects its
// request; and an error with no HTTP/3 code closes the connection with
// H3_INTERNAL_ERROR.
func TestCeClientScriptedResponses(t *testing.T) {
	server, base := startScriptServer(t, nil)
	server.handle("/push-rejected", func(s *quic.Stream, _ int) { s.Reset(uint64(ErrCodeRequestRejected)) })
	server.handle("/empty", func(s *quic.Stream, _ int) { writeFrames(s) })
	client := newClient(t, nil)

	body := fibhttp.NewBodyWriter(0)
	req := mustRequest(t, stdhttp.MethodPost, base+"/push-rejected", nil)
	req.Body = body
	future := client.Go(req)
	if _, err := body.Write([]byte("pushed")); err != nil {
		t.Fatal(err)
	}
	_ = body.Close()
	if _, err := ccvWait(t, future); !errors.Is(err, reqbody.ErrNotReplayable) {
		t.Fatalf("a rejected request whose pushed body had gone: %v", err)
	}
	if _, err := ccvWait(t, client.Go(mustRequest(t, stdhttp.MethodGet, base+"/empty", nil))); err == nil ||
		!strings.Contains(err.Error(), "ended before its header") {
		t.Fatalf("a response without a header: %v", err)
	}
	cc := ccvConn(t, client)
	cc.mu.Lock()
	qc := cc.qc
	cc.mu.Unlock()
	closeWith(qc, errors.New("no code"))
	server.expectClose(t, "/empty", ErrCodeInternalError)
}

// ceAtEOF is a Reader that calls eof as it first reports io.EOF.
type ceAtEOF struct {
	r   io.Reader
	eof func()
}

func (e *ceAtEOF) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	if err == io.EOF && e.eof != nil {
		e.eof()
		e.eof = nil
	}
	return n, err
}

// A Retry token sent back from another address than the one it was made for
// is let through to the QUIC handshake, which answers it with INVALID_TOKEN
// rather than a second Retry (RFC 9000 section 8.1.2).
func TestCeRetryTokenFromAnotherAddress(t *testing.T) {
	base := startServer(t, Config{RetryHandshakes: -1}, echo)
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	server, err := net.ResolveUDPAddr("udp", "127.0.0.1:"+u.Port())
	if err != nil {
		t.Fatal(err)
	}
	first, other := csUDP(t, server), csUDP(t, server)
	csWrite(t, first, csLongPacket(csInitial, quic.Version1, 1200))
	retry := csRead(t, first)
	if retry[0]&0x30 != 0x30 {
		t.Fatalf("answered with %x, not a Retry", retry[0])
	}
	// The Retry: its first byte and version, the connection IDs, the token,
	// and the 16 bytes of its integrity tag.
	rest := retry[5:]
	rest = rest[1+int(rest[0]):]
	scid := rest[1 : 1+int(rest[0])]
	token := rest[1+int(rest[0]) : len(rest)-16]

	b := []byte{0xc0 | csInitial<<4}
	b = binary.BigEndian.AppendUint32(b, quic.Version1)
	b = append(b, byte(len(scid)))
	b = append(b, scid...)
	b = append(b, 8)
	b = append(b, csRandom(8)...)
	b = quic.AppendVarint(b, uint64(len(token)))
	b = append(b, token...)
	n := 1200 - len(b) - 2
	b = append(b, byte(0x40|n>>8), byte(n))
	b = append(b, csRandom(n)...)
	csWrite(t, other, b)
	if answer := csRead(t, other); answer[0]&0x30 == 0x30 {
		t.Fatal("a Retry token from another address was answered with a second Retry")
	}
}

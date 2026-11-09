//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// cvWrResult is what a handler under test reports back: the path it served
// and the error, or errors, it saw.
type cvWrResult struct {
	path string
	errs []error
}

// cvWrWait takes the next result off results, failing the test if none
// arrives in time.
func cvWrWait(t *testing.T, results <-chan cvWrResult) cvWrResult {
	t.Helper()
	select {
	case r := <-results:
		return r
	case <-time.After(10 * time.Second):
		t.Fatal("the handler never reported")
		return cvWrResult{}
	}
}

// cvWrStream is a Stream served outside this package, which records what it
// was asked to send.
type cvWrStream struct {
	whole    *Response
	interims []int
}

func (s *cvWrStream) WriteResponse(_ *stdhttp.Request, response Response) error {
	s.whole = &response
	return nil
}

func (s *cvWrStream) WriteInterim(status int, _ stdhttp.Header) error {
	s.interims = append(s.interims, status)
	return nil
}

func (s *cvWrStream) Push(*stdhttp.Request, string, *stdhttp.PushOptions) error {
	return stdhttp.ErrNotSupported
}

// cvWrStreamer is a cvWrStream that sends a response as it is written, and
// takes deadlines.
type cvWrStreamer struct {
	cvWrStream
	status        int
	body          []byte
	ended         bool
	complete      bool
	trailer       stdhttp.Header
	read, written time.Time
}

func (s *cvWrStreamer) BeginResponse(_ *stdhttp.Request, status int, _ stdhttp.Header) error {
	s.status = status
	return nil
}

func (s *cvWrStreamer) WriteBody(p []byte, _ bool) error {
	s.body = append(s.body, p...)
	return nil
}

func (s *cvWrStreamer) EndResponse(trailer stdhttp.Header, complete bool) error {
	s.ended, s.complete, s.trailer = true, complete, trailer
	return nil
}

func (s *cvWrStreamer) SetReadDeadline(t time.Time) error  { s.read = t; return nil }
func (s *cvWrStreamer) SetWriteDeadline(t time.Time) error { s.written = t; return nil }

// cvWrSeekless is a regular file whose position cannot be read.
type cvWrSeekless struct{ *os.File }

func (cvWrSeekless) Seek(int64, int) (int64, error) { return 0, errors.New("no seeking") }

// A stream that cannot send a response as it is written has the
// ResponseWriter methods hold all of it and send it whole, and has deadlines
// refused; one that can streams it and is handed the deadlines.
func TestCoverWriterExternalStreams(t *testing.T) {
	req, _ := stdhttp.NewRequest("GET", "/", nil)
	whole := new(cvWrStream)
	c := NewStreamContext(nil, req, whole)
	if !c.holdsWhole() {
		t.Fatal("a stream that cannot stream is not held whole")
	}
	if err := c.SetWriteDeadline(time.Now()); err != stdhttp.ErrNotSupported {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	if err := c.SetReadDeadline(time.Now()); err != stdhttp.ErrNotSupported {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	big := strings.Repeat("w", writerBufferSize+1)
	if _, err := c.WriteString(big); err != nil {
		t.Fatal(err)
	}
	if err := c.Finish(); err != nil {
		t.Fatal(err)
	}
	if whole.whole == nil || string(whole.whole.Body) != big {
		t.Fatalf("the held response was not sent whole: %+v", whole.whole)
	}

	streamer := new(cvWrStreamer)
	c = NewStreamContext(nil, req, streamer)
	if c.holdsWhole() {
		t.Fatal("a stream that streams is held whole")
	}
	at := time.Now().Add(time.Hour)
	if c.SetWriteDeadline(at) != nil || c.SetReadDeadline(at) != nil || !streamer.read.Equal(at) || !streamer.written.Equal(at) {
		t.Fatalf("deadlines not handed on: read %v, write %v", streamer.read, streamer.written)
	}
	c.Header().Set("Trailer", "X-Sum")
	if _, err := c.WriteString(big); err != nil {
		t.Fatal(err)
	}
	c.Header().Set("X-Sum", "1")
	if err := c.Finish(); err != nil {
		t.Fatal(err)
	}
	if streamer.status != 200 || string(streamer.body) != big || !streamer.ended || !streamer.complete ||
		streamer.trailer.Get("X-Sum") != "1" {
		t.Fatalf("streamed response: %+v", streamer)
	}
}

// sendableFileOf turns down what it cannot hand to sendfile: a file that is
// not regular, and one whose position cannot be read.
func TestCoverWriterSendableFileRefusals(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	if _, ok := sendableFileOf(r); ok {
		t.Fatal("a pipe taken for a regular file")
	}
	path, _ := randomFile(t, 100)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, ok := sendableFileOf(cvWrSeekless{f}); ok {
		t.Fatal("a file whose position cannot be read taken for one that can be sent")
	}
}

// appendResponseHead refuses a status no response has, and names one it has
// no text for "Status".
func TestCoverWriterResponseHeadStatus(t *testing.T) {
	req, _ := stdhttp.NewRequest("GET", "/", nil)
	if _, err := appendResponseHead(nil, req, responseHead{status: 42, contentLength: -1}); err == nil {
		t.Fatal("status 42 accepted")
	}
	out, err := appendResponseHead(nil, req, responseHead{status: 599, contentLength: -1})
	if err != nil || !strings.HasPrefix(string(out), "HTTP/1.1 599 Status\r\n") {
		t.Fatalf("%q, %v", out, err)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("WriteHeader(42) did not panic")
		}
	}()
	(&Context{}).WriteHeader(42)
}

// Once a response is written whole, the ResponseWriter methods refuse to add
// to it, and a second status is ignored.
func TestCoverWriterAfterTheResponse(t *testing.T) {
	results := make(chan cvWrResult, 1)
	addr := serveHTTP1(t, func(c *Context) {
		_ = c.Respond(201, "text/plain", []byte("first"))
		c.WriteHeader(500)
		_, werr := c.Write([]byte("x"))
		_, rerr := c.ReadFrom(strings.NewReader("x"))
		results <- cvWrResult{errs: []error{werr, c.FlushError(), rerr}}
	})
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	if resp, body := conn.response("GET"); resp.StatusCode != 201 || body != "first" {
		t.Fatalf("status %d, body %q", resp.StatusCode, body)
	}
	for i, err := range cvWrWait(t, results).errs {
		if err != ErrResponseWritten {
			t.Fatalf("call %d after the response: %v", i, err)
		}
	}
}

// A second WriteHeader is ignored, and a status net/http has no text for is
// sent with "Status".
func TestCoverWriterUnknownStatus(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context) {
		c.WriteHeader(599)
		c.WriteHeader(200)
		_, _ = c.WriteString("odd")
	})
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	if head := conn.head(); !strings.HasPrefix(head, "HTTP/1.1 599 Status\r\n") {
		t.Fatalf("head %q", head)
	}
}

// A 304 keeps the Content-Length its handler declared, which describes the
// representation rather than a body, and a flushed response to HEAD that
// declared none is sent with neither a length nor chunks.
func TestCoverWriterHeadFraming(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context) {
		switch c.Request.URL.Path {
		case "/304":
			c.Header().Set("Content-Length", "42")
			c.WriteHeader(304)
		default:
			_, _ = c.WriteString("abc")
			c.Flush()
		}
	})
	conn := dialRaw(t, addr)
	conn.send("GET /304 HTTP/1.1\r\nHost: test\r\n\r\n")
	if head := conn.head(); !strings.HasPrefix(head, "HTTP/1.1 304 ") || !strings.Contains(head, "Content-Length: 42\r\n") {
		t.Fatalf("304 head %q", head)
	}
	conn.send("HEAD /head HTTP/1.1\r\nHost: test\r\n\r\n")
	head := conn.head()
	if !strings.HasPrefix(head, "HTTP/1.1 200 ") || strings.Contains(head, "Content-Length") ||
		strings.Contains(head, "Transfer-Encoding") {
		t.Fatalf("HEAD head %q", head)
	}
	// Nothing followed the head: the next response is the next request's.
	conn.send("GET /304 HTTP/1.1\r\nHost: test\r\n\r\n")
	if head = conn.head(); !strings.HasPrefix(head, "HTTP/1.1 304 ") {
		t.Fatalf("next head %q", head)
	}
}

// A trailer only set through http.TrailerPrefix makes a short response that
// is sent once its handler is done chunked, so that the trailer has
// somewhere to go; a trailer value that cannot be sent is dropped from a
// chunked body already out, which still ends properly.
func TestCoverWriterTrailers(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context) {
		switch c.Request.URL.Path {
		case "/prefixed":
			_, _ = c.WriteString("short")
			c.Header().Set(stdhttp.TrailerPrefix+"X-Late", "late")
		default:
			c.Header().Set("Trailer", "X-Bad")
			_, _ = c.WriteString("body")
			c.Flush()
			c.Header().Set("X-Bad", "a\nb")
		}
	})
	client, _ := stdClient(t)
	req, _ := stdhttp.NewRequest("GET", "http://"+addr+"/prefixed", nil)
	resp, body := do(t, client, req)
	if body != "short" || resp.Trailer.Get("X-Late") != "late" {
		t.Fatalf("prefixed: %q, trailer %v", body, resp.Trailer)
	}
	req, _ = stdhttp.NewRequest("GET", "http://"+addr+"/bad", nil)
	resp, body = do(t, client, req)
	if body != "body" || resp.Trailer.Get("X-Bad") != "" {
		t.Fatalf("bad trailer: %q, trailer %v", body, resp.Trailer)
	}
}

// OnFinish reports no body for a response to HEAD, and for a status that has
// none, on HTTP/1 and on an HTTP/2 stream that was flushed.
func TestCoverWriterOnFinishWithoutBody(t *testing.T) {
	sizes := make(chan int64, 2)
	handler := NewHandler(HandlerFunc(func(c *Context) {
		c.OnFinish(func(_ int, _ stdhttp.Header, size int64) { sizes <- size })
		if c.Request.Method == stdhttp.MethodHead {
			_, _ = c.WriteString("not sent")
			return
		}
		c.WriteHeader(204)
		c.Flush()
	}))
	addr := serve(t, handler)
	h1, _ := stdClient(t)
	for _, client := range []*stdhttp.Client{h1, netHTTPClient(t, false)} {
		method := "HEAD"
		if client != h1 {
			method = "GET"
		}
		req, _ := stdhttp.NewRequest(method, "http://"+addr+"/", nil)
		do(t, client, req)
		select {
		case size := <-sizes:
			if size != 0 {
				t.Fatalf("%s: OnFinish saw %d bytes", method, size)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: OnFinish never ran", method)
		}
	}
}

// Respond refuses a Content-Type that cannot be sent, leaving the response
// to be answered another way.
func TestCoverWriterInvalidContentType(t *testing.T) {
	results := make(chan cvWrResult, 1)
	addr := serveHTTP1(t, func(c *Context) {
		err := c.Respond(200, "text/plain\nX-Injected: 1", []byte("no"))
		results <- cvWrResult{errs: []error{err}}
		_ = c.Respond(500, "text/plain", []byte("fallback"))
	})
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	resp, body := conn.response("GET")
	if resp.StatusCode != 500 || body != "fallback" || resp.Header.Get("X-Injected") != "" {
		t.Fatalf("status %d, body %q, header %v", resp.StatusCode, body, resp.Header)
	}
	if err := cvWrWait(t, results).errs[0]; err == nil {
		t.Fatal("an invalid Content-Type was accepted")
	}
}

// ReadFrom's sendfile path reports what stops it: an error a write met
// before, a write deadline that passed before the header went or after it
// did, and a connection that closed under it, chunked or not.
func TestCoverWriterReadFromFailures(t *testing.T) {
	path, _ := randomFile(t, minSendFileSize+4000)
	results := make(chan cvWrResult, 1)
	addr := serveHTTP1(t, func(c *Context) {
		f, err := os.Open(path)
		if err != nil {
			results <- cvWrResult{c.Request.URL.Path, []error{err}}
			return
		}
		defer f.Close()
		var errs []error
		switch c.Request.URL.Path {
		case "/failed":
			// A write that failed is what ReadFrom reports from then on.
			c.Conn.Close()
			_, err = c.Write(make([]byte, writerBufferSize+1))
			errs = append(errs, err)
		case "/expired":
			_ = c.SetWriteDeadline(time.Now().Add(-time.Second))
		case "/expired-later":
			errs = append(errs, c.FlushError())
			_ = c.SetWriteDeadline(time.Now().Add(-time.Second))
		case "/closed-chunked":
			errs = append(errs, c.FlushError())
			c.Conn.Close()
		case "/closed-length":
			c.Header().Set("Content-Length", "1000000")
			errs = append(errs, c.FlushError())
			c.Conn.Close()
		}
		_, err = c.ReadFrom(f)
		results <- cvWrResult{c.Request.URL.Path, append(errs, err)}
	})
	for _, path := range []string{"/failed", "/expired", "/expired-later", "/closed-chunked", "/closed-length"} {
		conn := dialRaw(t, addr)
		conn.send("GET " + path + " HTTP/1.1\r\nHost: test\r\n\r\n")
		r := cvWrWait(t, results)
		got := r.errs[len(r.errs)-1]
		switch path {
		case "/failed":
			if got == nil || got != r.errs[0] {
				t.Fatalf("%s: Write failed with %v, ReadFrom with %v", path, r.errs[0], got)
			}
		case "/expired", "/expired-later":
			if !errors.Is(got, os.ErrDeadlineExceeded) {
				t.Fatalf("%s: %v", path, r.errs)
			}
		default:
			if r.errs[0] != nil || got == nil {
				t.Fatalf("%s: %v", path, r.errs)
			}
		}
		conn.Close()
	}
}

// A response whose header cannot be sent fails Finish, and is finished for
// the ResponseWriter methods even so.
func TestCoverWriterFinishWithAnInvalidHeader(t *testing.T) {
	results := make(chan cvWrResult, 1)
	addr := serveHTTP1(t, func(c *Context) {
		c.Header().Set("X-Bad", "a\nb")
		c.WriteHeader(200)
		ferr := c.Finish()
		_, werr := c.Write([]byte("x"))
		results <- cvWrResult{errs: []error{ferr, werr}}
	})
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	r := cvWrWait(t, results)
	if r.errs[0] == nil || r.errs[1] != ErrResponseWritten {
		t.Fatalf("Finish: %v, Write after it: %v", r.errs[0], r.errs[1])
	}
}

// A response held back whole whose header cannot be sent must not be lost
// with the connection kept, or the response to the request pipelined behind
// it reads as this one's.
func TestCoverWriterInvalidHeaderClosesConnection(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context) {
		if c.Request.URL.Path == "/bad" {
			c.Header().Set("X-Bad", "a\nb")
		}
		_, _ = c.WriteString(c.Request.URL.Path)
	})
	conn := dialRaw(t, addr)
	conn.send("GET /bad HTTP/1.1\r\nHost: test\r\n\r\nGET /good HTTP/1.1\r\nHost: test\r\n\r\n")
	resp, err := stdhttp.ReadResponse(conn.r, &stdhttp.Request{Method: "GET"})
	if err != nil {
		return // the connection ended instead, which is fine
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) == "/good" {
		t.Fatal("the response to /good was read as the one to /bad")
	}
}

// cvWrCancelled serves one request that its handler retains, resets the
// client's connection under it, and returns the Context once OnCancel has
// reported the close. The caller still has to Release it.
func cvWrCancelled(t *testing.T, config Config, request string) *Context {
	t.Helper()
	started := make(chan *Context, 1)
	cancelled := make(chan error, 1)
	addr := serveStreamingServer(t, config, func(c *Context) {
		c.Retain()
		c.OnCancel(func(err error) { cancelled <- err })
		started <- c
	})
	conn := dialRaw(t, addr)
	conn.send(request)
	var c *Context
	select {
	case c = <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the handler never ran")
	}
	// Reset rather than closed: a FIN on a retained request cancels it but
	// leaves its response open (see Context.OnCancel), and these tests want
	// one cut off.
	_ = conn.Conn.(*net.TCPConn).SetLinger(0)
	conn.Close()
	select {
	case err := <-cancelled:
		if err == nil {
			t.Fatal("OnCancel was given no reason")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("OnCancel never ran")
	}
	return c
}

// A callback registered on a request that has already been cancelled hears
// of it at once: OnCancel's in place, OnBody's on the handler pool, with the
// error the body failed with when it was streaming.
func TestCoverWriterCallbacksAfterCancel(t *testing.T) {
	for _, tc := range []struct {
		name    string
		config  Config
		request string
	}{
		{"whole", DefaultConfig(), "GET / HTTP/1.1\r\nHost: test\r\n\r\n"},
		{"streamed", immediateStreamingConfig(), "POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 100\r\n\r\n0123456789"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := cvWrCancelled(t, tc.config, tc.request)
			var late error
			c.OnCancel(func(err error) { late = err })
			if late == nil {
				t.Fatal("OnCancel on a cancelled request did not run at once")
			}
			body := make(chan error, 1)
			c.OnBody(func(data []byte, fin bool, err error) {
				if len(data) != 0 || !fin {
					err = errors.New("the failure was not a last, empty call")
				}
				body <- err
			})
			select {
			case err := <-body:
				if err == nil {
					t.Fatal("OnBody on a cancelled request was given no error")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("OnBody on a cancelled request was never called")
			}
			c.Release()
		})
	}
}

// A Release the handler took no Retain for gives back its own hold, which
// finishes the response there and then: what it writes after that is
// refused, and OnCancel and OnBody registered then deliver nothing.
func TestCoverWriterReleaseTooMany(t *testing.T) {
	results := make(chan cvWrResult, 1)
	addr := serveHTTP1(t, func(c *Context) {
		c.OnCancel(nil)
		c.OnBody(nil)
		_, _ = c.WriteString("early")
		c.Release()
		_, err := c.WriteString("late")
		c.OnCancel(func(error) { err = errors.New("OnCancel ran on a finished response") })
		c.OnBody(func([]byte, bool, error) { err = errors.New("OnBody ran in place") })
		results <- cvWrResult{errs: []error{err}}
	})
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	if resp, body := conn.response("GET"); resp.StatusCode != 200 || body != "early" {
		t.Fatalf("status %d, body %q", resp.StatusCode, body)
	}
	if err := cvWrWait(t, results).errs[0]; err != ErrResponseWritten {
		t.Fatalf("after the early finish: %v", err)
	}
	// The connection carries on with the next request.
	conn.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	if _, body := conn.response("GET"); body != "early" {
		t.Fatalf("next body %q", body)
	}
	cvWrWait(t, results)
}

// A second OnBody while the handler runs replaces the first callback, and
// the one handover owed at its return delivers to the second.
func TestCoverWriterOnBodyTwice(t *testing.T) {
	var first atomic.Bool
	addr := serveHTTP1(t, func(c *Context) {
		c.OnBody(func([]byte, bool, error) { first.Store(true) })
		c.OnBody(func(data []byte, fin bool, err error) {
			if fin && err == nil {
				_ = c.Respond(200, "text/plain", data)
			}
		})
	})
	conn := dialRaw(t, addr)
	conn.send("POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 7\r\n\r\npayload")
	if _, body := conn.response("POST"); body != "payload" || first.Load() {
		t.Fatalf("body %q, first callback called: %v", body, first.Load())
	}
}

// A request takes at most 65535 Retains at a time; the next one panics, and
// the ones taken still release as usual.
func TestCoverWriterTooManyRetains(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context) {
		for i := 0; i < int(ctxHolds); i++ {
			c.Retain()
		}
		panicked := func() (p bool) {
			defer func() { p = recover() != nil }()
			c.Retain()
			return false
		}()
		_ = c.Respond(200, "text/plain", []byte(strconv.FormatBool(panicked)))
		for i := 0; i < int(ctxHolds); i++ {
			c.Release()
		}
	})
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	if _, body := conn.response("GET"); body != "true" {
		t.Fatalf("panicked: %s", body)
	}
}

// A Context recycled after a static route served it drops the route state,
// which is the endpoint's own, rather than resetting it for the next request.
func TestCoverWriterRecycleSharedRoute(t *testing.T) {
	router := NewRouter()
	router.Get("/static", func(c *Context) { _ = c.Respond(200, "text/plain", []byte(c.Request.URL.Path)) })
	server := NewHandlerWithConfig(DefaultConfig(), router)
	setReuseAll(&server.config)
	addr := serve(t, server)
	conn := dialRaw(t, addr)
	for i := 0; i < 3; i++ {
		conn.send("GET /static HTTP/1.1\r\nHost: test\r\n\r\n")
		if _, body := conn.response("GET"); body != "/static" {
			t.Fatalf("request %d: %q", i, body)
		}
	}
}

// The states a request reaches only between goroutines, set up by hand: a
// request cancelled before its handler began is not begun, a body callback
// registered on one cancelled before its body was looked at is told why, a
// Release on a Context never served does nothing, and Query on a recycled
// Context finds nothing.
func TestCoverWriterContextStates(t *testing.T) {
	req, _ := stdhttp.NewRequest("GET", "/?a=1", nil)
	c := &Context{Request: req}
	c.word.Store(withState(0, ctxCancelled))
	c.err = net.ErrClosed
	if c.begin(true, false) {
		t.Fatal("a cancelled request was begun")
	}
	body := make(chan error, 1)
	c.OnBody(func(_ []byte, _ bool, err error) { body <- err })
	select {
	case err := <-body:
		if err != net.ErrClosed {
			t.Fatalf("OnBody was told %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("OnBody was never called")
	}

	fresh := new(Context)
	fresh.Release()
	if fresh.word.Load() != 0 {
		t.Fatalf("a Release on a Context never served changed it: %x", fresh.word.Load())
	}
	if fresh.Query("a") != "" {
		t.Fatal("Query found a value with no request")
	}
}

// A body deadline set on a finished response is refused, and one set for a
// request whose body is not the one arriving is kept for armRead.
func TestCoverWriterBodyDeadlineSet(t *testing.T) {
	var b bodyDeadline
	c := new(Context)
	at := time.Now().Add(time.Minute)
	if err := b.set(c, at); err != nil || b.owner != c || !b.at.Equal(at) || b.applied {
		t.Fatalf("set: %v, owner %p, at %v, applied %v", err, b.owner, b.at, b.applied)
	}
	c.word.Store(withState(0, ctxDone))
	if err := b.set(c, at); err != ErrResponseWritten {
		t.Fatalf("set on a finished response: %v", err)
	}
}

// Reset clears what a server kept on the parser for the connection it
// served.
func TestCoverWriterParserResetServerState(t *testing.T) {
	p := NewParser(DefaultConfig())
	p.liveContext.Store(new(Context))
	p.writeDeadline = time.Now()
	p.openBody(new(Context))
	p.quietSince = 1
	p.Reset()
	if p.liveContext.Load() != nil || !p.writeDeadline.IsZero() || p.bodyDeadline != nil || p.bodyArmed || p.quietSince != 0 {
		t.Fatal("server state left after Reset")
	}
}

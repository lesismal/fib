//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// tick writes "tick\n" n times, flushing each and pausing after each but the
// last, and reports the first error.
func tick(c *Context, n int, pause time.Duration) error {
	for i := 0; i < n; i++ {
		if i > 0 {
			time.Sleep(pause)
		}
		if _, err := c.WriteString("tick\n"); err != nil {
			return err
		}
		if err := c.FlushError(); err != nil {
			return err
		}
	}
	return nil
}

// streamedResponse reads a response whose body may be cut short, and reports
// the body and what cut it.
func (c *rawConn) streamedResponse() (string, error) {
	c.t.Helper()
	resp, err := stdhttp.ReadResponse(c.r, &stdhttp.Request{Method: stdhttp.MethodGet})
	if err != nil {
		return "", err
	}
	body, err := io.ReadAll(resp.Body)
	return string(body), err
}

// waitErr reports the outcome of a handler running away from the test.
func waitErr(t *testing.T, errs <-chan error) error {
	t.Helper()
	select {
	case err := <-errs:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("the handler never finished")
		return nil
	}
}

// http.ResponseController reaches Context's deadlines, as issue #5 asks:
// lifting either succeeds; a read deadline for a body that has all arrived
// has nothing to bound, so it succeeds and leaves the connection alone; and
// both report ErrResponseWritten once the response is finished.
func TestResponseControllerDeadlines(t *testing.T) {
	after := make(chan [2]error, 1)
	handler := HandlerFunc(func(c *Context) {
		rc := stdhttp.NewResponseController(c)
		w := rc.SetWriteDeadline(time.Time{})
		r := rc.SetReadDeadline(time.Time{})
		past := rc.SetReadDeadline(time.Now().Add(-time.Second))
		_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "write=%v read=%v past=%v", w, r, past))
		after <- [2]error{rc.SetWriteDeadline(time.Time{}), rc.SetReadDeadline(time.Time{})}
	})
	const want = "write=<nil> read=<nil> past=<nil>"
	checkAfter := func(proto string) {
		t.Helper()
		errs := <-after
		for i, err := range errs {
			if !errors.Is(err, ErrResponseWritten) {
				t.Errorf("%s: deadline %d set after the response: %v, want %v", proto, i, err, ErrResponseWritten)
			}
		}
	}

	addr := serve(t, NewHandler(handler))
	conn := dialRaw(t, addr)
	for i := 0; i < 2; i++ {
		// The second request finds the connection still open.
		conn.send("POST /x HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\n\r\nbody")
		if _, body := conn.response(stdhttp.MethodPost); body != want {
			t.Fatalf("HTTP/1 request %d: %q, want %q", i, body, want)
		}
		checkAfter("HTTP/1")
	}

	tc := dialH2(t, addr)
	tc.headers(1, true, get("/x")...)
	if _, body := tc.response(1); string(body) != want {
		t.Fatalf("HTTP/2: %q, want %q", body, want)
	}
	checkAfter("HTTP/2")
}

// A handler that lifts its write deadline streams for as long as it likes
// under WriteTimeout, on HTTP/1 and HTTP/2, where one that does not is cut
// off by it.
func TestSetWriteDeadlineLiftsWriteTimeout(t *testing.T) {
	config := DefaultConfig()
	config.WriteTimeout = 200 * time.Millisecond
	errs := make(chan error, 1)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		if c.Request.URL.Path == "/lifted" {
			if err := c.SetWriteDeadline(time.Time{}); err != nil {
				errs <- err
				return
			}
		}
		errs <- tick(c, 4, 150*time.Millisecond)
	})))
	want := strings.Repeat("tick\n", 4)

	conn := dialRaw(t, addr)
	conn.send("GET /lifted HTTP/1.1\r\nHost: test\r\n\r\n")
	if body, err := conn.streamedResponse(); err != nil || body != want {
		t.Fatalf("HTTP/1 lifted: %q, %v", body, err)
	}
	if err := waitErr(t, errs); err != nil {
		t.Fatalf("HTTP/1 lifted: the handler's writes failed: %v", err)
	}
	conn = dialRaw(t, addr)
	conn.send("GET /bounded HTTP/1.1\r\nHost: test\r\n\r\n")
	if body, err := conn.streamedResponse(); err == nil {
		t.Fatalf("HTTP/1 bounded: the whole body %q arrived past WriteTimeout", body)
	}
	if err := waitErr(t, errs); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("HTTP/1 bounded: the handler's writes reported %v, want %v", err, os.ErrDeadlineExceeded)
	}

	tc := dialH2(t, addr)
	tc.headers(1, true, get("/lifted")...)
	if _, body := tc.response(1); string(body) != want {
		t.Fatalf("HTTP/2 lifted: %q, want %q", body, want)
	}
	if err := waitErr(t, errs); err != nil {
		t.Fatalf("HTTP/2 lifted: the handler's writes failed: %v", err)
	}
	tc.headers(3, true, get("/bounded")...)
	st := tc.stream(3)
	for !st.done {
		tc.read()
	}
	if !st.resetSet || st.reset != H2InternalError {
		t.Fatalf("HTTP/2 bounded: the stream ended with reset %v (%v), want %v", st.resetSet, st.reset, H2InternalError)
	}
	if err := waitErr(t, errs); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("HTTP/2 bounded: the handler's writes reported %v, want %v", err, os.ErrDeadlineExceeded)
	}
}

// A handler can bound its response without Config.WriteTimeout, and a
// deadline already past ends it at its next write.
func TestSetWriteDeadlineWithoutWriteTimeout(t *testing.T) {
	errs := make(chan error, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		at := time.Now().Add(200 * time.Millisecond)
		if c.Request.URL.Path == "/past" {
			at = time.Now().Add(-time.Second)
		}
		if err := c.SetWriteDeadline(at); err != nil {
			errs <- err
			return
		}
		errs <- tick(c, 6, 150*time.Millisecond)
	})))
	for _, path := range []string{"/soon", "/past"} {
		conn := dialRaw(t, addr)
		conn.send("GET " + path + " HTTP/1.1\r\nHost: test\r\n\r\n")
		if body, err := conn.streamedResponse(); err == nil {
			t.Fatalf("HTTP/1 %s: the whole body %q arrived past the deadline", path, body)
		}
		if err := waitErr(t, errs); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("HTTP/1 %s: the handler's writes reported %v, want %v", path, err, os.ErrDeadlineExceeded)
		}
	}

	// A client that stops taking the body leaves the handler waiting for
	// room, which the deadline lets go of.
	addr = serve(t, NewHandler(HandlerFunc(func(c *Context) {
		if err := c.SetWriteDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
			errs <- err
			return
		}
		chunk := make([]byte, 16<<10)
		for {
			if _, err := c.Write(chunk); err != nil {
				errs <- err
				return
			}
		}
	})))
	tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 1000})
	tc.headers(1, true, get("/stall")...)
	st := tc.stream(1)
	for !st.done {
		tc.read()
	}
	if !st.resetSet || st.reset != H2InternalError {
		t.Fatalf("HTTP/2: the stream ended with reset %v (%v), want %v", st.resetSet, st.reset, H2InternalError)
	}
	if err := waitErr(t, errs); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("HTTP/2: the handler's writes reported %v, want %v", err, os.ErrDeadlineExceeded)
	}
}

// A deadline a handler sets is its response's alone: the next request on the
// connection, which Config.WriteTimeout does not bound, streams long past it.
func TestSetWriteDeadlineEndsWithItsResponse(t *testing.T) {
	errs := make(chan error, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		if c.Request.URL.Path == "/first" {
			errs <- c.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("first"))
			return
		}
		errs <- tick(c, 3, 100*time.Millisecond)
	})))
	conn := dialRaw(t, addr)
	conn.send("GET /first HTTP/1.1\r\nHost: test\r\n\r\n")
	if _, body := conn.response(stdhttp.MethodGet); body != "first" {
		t.Fatalf("first: %q", body)
	}
	if err := waitErr(t, errs); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	conn.send("GET /second HTTP/1.1\r\nHost: test\r\n\r\n")
	if body, err := conn.streamedResponse(); err != nil || body != strings.Repeat("tick\n", 3) {
		t.Fatalf("second: %q, %v", body, err)
	}
	if err := waitErr(t, errs); err != nil {
		t.Fatalf("second: the handler's writes failed: %v", err)
	}
}

// sendSlowUpload sends a POST whose 8KB body stops halfway for pause.
func sendSlowUpload(conn *rawConn, path string, pause time.Duration) {
	conn.t.Helper()
	conn.send(fmt.Sprintf("POST %s HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", path, 8<<10))
	half := bytes.Repeat([]byte("x"), 4<<10)
	if _, err := conn.Write(half); err != nil {
		conn.t.Fatal(err)
	}
	time.Sleep(pause)
	if _, err := conn.Write(half); err != nil {
		conn.t.Fatal(err)
	}
}

// countBody answers with the size of the body OnBody delivers, or reports
// what ended it early.
func countBody(c *Context, errs chan<- error, first func()) {
	total := 0
	c.OnBody(func(data []byte, fin bool, err error) {
		if first != nil {
			first()
			first = nil
		}
		total += len(data)
		if err != nil {
			errs <- err
			return
		}
		if fin {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(strconv.Itoa(total)))
			errs <- nil
		}
	})
}

// A handler that lifts its read deadline takes an upload as slow as it is
// under ReadTimeout, whether it lifts it as it runs, from the body's first
// callback on the connection's worker, or from a goroutine of its own.
func TestSetReadDeadlineLiftsReadTimeout(t *testing.T) {
	config := streamingConfig()
	config.ReadTimeout = 300 * time.Millisecond
	errs := make(chan error, 2)
	lift := func(c *Context) {
		if err := stdhttp.NewResponseController(c).SetReadDeadline(time.Time{}); err != nil {
			errs <- err
		}
	}
	addr := serveStreamingServer(t, config, func(c *Context) {
		switch c.Request.URL.Path {
		case "/handler":
			lift(c)
			countBody(c, errs, nil)
		case "/callback":
			countBody(c, errs, func() { lift(c) })
		case "/goroutine":
			c.Retain()
			countBody(c, errs, nil)
			go func() {
				defer c.Release()
				lift(c)
			}()
		case "/none":
			countBody(c, errs, nil)
		}
	})
	for _, path := range []string{"/handler", "/callback", "/goroutine"} {
		conn := dialRaw(t, addr)
		sendSlowUpload(conn, path, 800*time.Millisecond)
		if _, body := conn.response(stdhttp.MethodPost); body != strconv.Itoa(8<<10) {
			t.Fatalf("%s: %q", path, body)
		}
		if err := waitErr(t, errs); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
	conn := dialRaw(t, addr)
	conn.send(fmt.Sprintf("POST /none HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", 8<<10))
	conn.send(strings.Repeat("x", 4<<10))
	if !conn.closed() {
		t.Fatal("an upload whose handler kept ReadTimeout outlasted it")
	}
	if err := waitErr(t, errs); err == nil {
		t.Fatal("a body cut short by ReadTimeout read as a complete one")
	}
}

// A handler can bound a streamed body without Config.ReadTimeout. The
// deadline is that body's alone: one that arrives in time leaves the
// connection to wait for its next request as long as it likes.
func TestSetReadDeadlineBoundsAStreamedBody(t *testing.T) {
	errs := make(chan error, 1)
	addr := serveStreamingServer(t, streamingConfig(), func(c *Context) {
		if err := c.SetReadDeadline(time.Now().Add(400 * time.Millisecond)); err != nil {
			errs <- err
			return
		}
		countBody(c, errs, nil)
	})
	conn := dialRaw(t, addr)
	conn.send(fmt.Sprintf("POST /slow HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", 8<<10))
	conn.send(strings.Repeat("x", 4<<10))
	if !conn.closed() {
		t.Fatal("a body that outlasted its handler's deadline did not time out")
	}
	if err := waitErr(t, errs); err == nil {
		t.Fatal("a body cut short by the deadline read as a complete one")
	}

	conn = dialRaw(t, addr)
	sendSlowUpload(conn, "/quick", 0)
	if _, body := conn.response(stdhttp.MethodPost); body != strconv.Itoa(8<<10) {
		t.Fatalf("quick: %q", body)
	}
	if err := waitErr(t, errs); err != nil {
		t.Fatal(err)
	}
	time.Sleep(800 * time.Millisecond)
	// Past that deadline, the connection still serves.
	conn.send("POST /next HTTP/1.1\r\nHost: test\r\nContent-Length: 2\r\n\r\nok")
	if _, body := conn.response(stdhttp.MethodPost); body != "2" {
		t.Fatalf("next: %q", body)
	}
	if err := waitErr(t, errs); err != nil {
		t.Fatal(err)
	}
}

// readDeadlineHandler serves the read deadline tests of the multiplexed
// protocols: /whole reports what bounding a body that has all arrived does,
// /bounded gives its body 300ms, and /lifted gives it 200ms and then lifts
// that. A body answers with its size, or with 408 and what ended it early.
func readDeadlineHandler(c *Context) {
	var err error
	switch c.Request.URL.Path {
	case "/whole":
		zero := c.SetReadDeadline(time.Time{})
		later := c.SetReadDeadline(time.Now().Add(time.Minute))
		_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "zero=%v later=%v", zero, later))
		return
	case "/bounded":
		err = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	case "/lifted":
		err = errors.Join(c.SetReadDeadline(time.Now().Add(200*time.Millisecond)), c.SetReadDeadline(time.Time{}))
	}
	if err != nil {
		_ = c.Respond(stdhttp.StatusInternalServerError, "text/plain", []byte(err.Error()))
		return
	}
	total := 0
	c.OnBody(func(data []byte, fin bool, err error) {
		total += len(data)
		switch {
		case err != nil:
			_ = c.Respond(stdhttp.StatusRequestTimeout, "text/plain", []byte(err.Error()))
		case fin:
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(strconv.Itoa(total)))
		}
	})
}

// A read deadline on HTTP/2 bounds the stream's body, not the connection:
// once it has passed, a body still arriving fails with
// os.ErrDeadlineExceeded, the handler still answers, and the connection goes
// on serving. Lifting a deadline before it passes leaves the body unbounded,
// and a body that has all arrived has nothing to bound.
func TestH2SetReadDeadline(t *testing.T) {
	// The handler runs as soon as the header is in, body or no body.
	addr := serveStreamingServer(t, immediateStreamingConfig(), readDeadlineHandler)
	tc := dialH2(t, addr)
	wait := func(id uint32) *h2TestStream {
		t.Helper()
		st := tc.stream(id)
		for !st.done {
			tc.read()
		}
		return st
	}
	post := func(id uint32, path string) {
		tc.headers(id, false, ":method", "POST", ":scheme", "http", ":authority", "test", ":path", path,
			"content-length", "100")
		tc.data(id, false, make([]byte, 10))
	}

	tc.headers(1, true, get("/whole")...)
	if _, body := tc.response(1); string(body) != "zero=<nil> later=<nil>" {
		t.Fatalf("a body that has arrived: %q", body)
	}

	start := time.Now()
	post(3, "/bounded")
	st := wait(3)
	if status, body := st.header[":status"], string(st.body); status != "408" || body != os.ErrDeadlineExceeded.Error() {
		t.Fatalf("a body past its deadline: %s %q", status, body)
	}
	if waited := time.Since(start); waited < 250*time.Millisecond {
		t.Fatalf("the body failed after %v, before its deadline", waited)
	}

	post(5, "/lifted")
	time.Sleep(500 * time.Millisecond)
	tc.data(5, true, make([]byte, 90))
	if _, body := tc.response(5); string(body) != "100" {
		t.Fatalf("a body whose deadline was lifted: %q", body)
	}

	tc.headers(7, true, get("/whole")...)
	if _, body := tc.response(7); string(body) != "zero=<nil> later=<nil>" {
		t.Fatalf("a request after the deadline: %q", body)
	}
}

// A write deadline already past fails an HTTP/1 response at the first Flush,
// which would send its header and the event Write held back with it, so the
// client gets nothing; and once it has failed, every Write and Flush after
// it reports the same error, so a loop that checks either one stops (issue
// #5).
func TestPastWriteDeadlineFailsTheFirstFlush(t *testing.T) {
	results := make(chan string, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		rc := stdhttp.NewResponseController(c)
		out := fmt.Sprintf("set=%v", rc.SetWriteDeadline(time.Now().Add(-time.Second)))
		c.Header().Set("Content-Type", "text/event-stream")
		c.WriteHeader(stdhttp.StatusOK)
		for i := 0; i < 3; i++ {
			_, w := fmt.Fprintf(c, "data: %d\n\n", i)
			out += fmt.Sprintf(" | write%d=%v flush%d=%v", i, w, i, rc.Flush())
		}
		results <- out
	})))
	conn := dialRaw(t, addr)
	conn.send("GET /events HTTP/1.1\r\nHost: test\r\n\r\n")
	got, _ := io.ReadAll(conn.r)
	if len(got) != 0 {
		t.Fatalf("the client got %q from a response past its deadline", got)
	}
	timeout := os.ErrDeadlineExceeded
	want := fmt.Sprintf("set=<nil> | write0=<nil> flush0=%v | write1=%v flush1=%v | write2=%v flush2=%v",
		timeout, timeout, timeout, timeout, timeout)
	if out := <-results; out != want {
		t.Fatalf("got  %s\nwant %s", out, want)
	}
}

// Config.WriteTimeout refuses an HTTP/1 response that only begins to stream
// once it has passed, from the Flush that would send its header. One sent
// whole as the handler returns is still sent.
func TestWriteTimeoutRefusesALateStreamingResponse(t *testing.T) {
	config := DefaultConfig()
	config.WriteTimeout = 150 * time.Millisecond
	errs := make(chan error, 1)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		time.Sleep(300 * time.Millisecond)
		if c.Request.URL.Path == "/whole" {
			errs <- c.Respond(stdhttp.StatusOK, "text/plain", []byte("late but whole"))
			return
		}
		_, _ = c.WriteString("late")
		errs <- c.FlushError()
	})))
	conn := dialRaw(t, addr)
	conn.send("GET /streaming HTTP/1.1\r\nHost: test\r\n\r\n")
	if got, _ := io.ReadAll(conn.r); len(got) != 0 {
		t.Fatalf("the client got %q from a response that began past WriteTimeout", got)
	}
	if err := waitErr(t, errs); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("the late Flush reported %v, want %v", err, os.ErrDeadlineExceeded)
	}
	conn = dialRaw(t, addr)
	conn.send("GET /whole HTTP/1.1\r\nHost: test\r\n\r\n")
	if _, body := conn.response(stdhttp.MethodGet); body != "late but whole" {
		t.Fatalf("whole: %q", body)
	}
	if err := waitErr(t, errs); err != nil {
		t.Fatal(err)
	}
}

// On HTTP/2 a Flush made once WriteTimeout has reset the stream reports it,
// even with nothing written in between, and so does every Write and Flush
// after it.
func TestH2FlushReportsAResetStream(t *testing.T) {
	config := DefaultConfig()
	// A second from the handler's start to its first Flush: a runner that
	// stalls longer than that has the first Flush fail too, which the test
	// reports as such rather than as the calls after it.
	config.WriteTimeout = time.Second
	results := make(chan [3]error, 1)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		_, _ = c.WriteString("first")
		if err := c.FlushError(); err != nil {
			results <- [3]error{fmt.Errorf("the first Flush, before WriteTimeout, failed too: %v", err)}
			return
		}
		time.Sleep(1500 * time.Millisecond)
		flush := c.FlushError()
		_, write := c.WriteString(strings.Repeat("x", 32<<10))
		results <- [3]error{flush, write, c.FlushError()}
	})))
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/events")...)
	st := tc.stream(1)
	for !st.done {
		tc.read()
	}
	if !st.resetSet || st.reset != H2InternalError {
		t.Fatalf("the stream ended with reset %v (%v), want %v", st.resetSet, st.reset, H2InternalError)
	}
	var errs [3]error
	select {
	case errs = <-results:
	case <-time.After(10 * time.Second):
		t.Fatal("the handler never finished")
	}
	for i, err := range errs {
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("call %d after the reset reported %v, want %v", i, err, os.ErrDeadlineExceeded)
		}
	}
}

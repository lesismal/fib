//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package recover_test

import (
	"bytes"
	stdhttp "net/http"
	"strings"
	"sync"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/log"
	"github.com/lesismal/fib/middleware/mwtest"
	"github.com/lesismal/fib/middleware/recover"
)

// lockedBuffer is fib's log for a test, written from the
// server's goroutines.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLog sends fib's log to a buffer, in FormatLine, until the test ends.
func captureLog(t *testing.T) *lockedBuffer {
	out := &lockedBuffer{}
	log.Setup(log.Config{Output: out})
	t.Cleanup(func() { log.SetLogger(nil) })
	return out
}

// TestRecoverAbortHandler checks a panic with http.ErrAbortHandler: it is not
// logged, on HTTP/1 it ends the connection rather than answering, so that the
// client sees the response cut short, and on HTTP/2 it is answered with 500,
// the connection carrying other streams. A request Next skips is not
// guarded at all, so its handler's panic is the server's to deal with.
func TestRecoverAbortHandler(t *testing.T) {
	out := captureLog(t)
	handler := recover.New(recover.Config{
		Next: func(c *fibhttp.Context) bool { return c.Request.URL.Path == "/skip" },
	})(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		if c.Request.URL.Path == "/skip" {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("skipped"))
			return
		}
		panic(stdhttp.ErrAbortHandler)
	}))
	url := mwtest.Serve(t, handler)
	clients := mwtest.Clients(t)
	req, err := stdhttp.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := clients[0].Client.Do(req); err == nil {
		resp.Body.Close()
		t.Errorf("HTTP/1.1: an aborted handler answered %d, want the connection closed", resp.StatusCode)
	}
	if resp, _ := clients[1].Do(t, "GET", url, nil, ""); resp.StatusCode != stdhttp.StatusInternalServerError {
		t.Errorf("HTTP/2: an aborted handler answered %d, want 500", resp.StatusCode)
	}
	if _, body := clients[0].Do(t, "GET", url+"/skip", nil, ""); body != "skipped" {
		t.Errorf("skipped request answered %q", body)
	}
	if got := out.String(); strings.Contains(got, "panic serving") {
		t.Errorf("ErrAbortHandler was logged: %q", got)
	}
}

// TestRecoverDefaultLog checks the default Log, which writes the panic to
// fib's log with its stack, or without it when DisableStackTrace is set.
func TestRecoverDefaultLog(t *testing.T) {
	for _, disable := range []bool{false, true} {
		out := captureLog(t)
		handler := recover.New(recover.Config{DisableStackTrace: disable})(fibhttp.HandlerFunc(
			func(*fibhttp.Context) { panic("kaboom") }))
		url := mwtest.Serve(t, handler)
		resp, _ := mwtest.Clients(t)[0].Do(t, "GET", url+"/where", nil, "")
		if resp.StatusCode != stdhttp.StatusInternalServerError {
			t.Errorf("DisableStackTrace %v: answered %d", disable, resp.StatusCode)
		}
		// The line is logged before the 500 is written, so it is there by
		// the time the client has the response.
		got := out.String()
		if !strings.Contains(got, "ERROR http: panic serving method=GET url=/where panic=kaboom") {
			t.Errorf("DisableStackTrace %v: logged %q", disable, got)
		}
		if hasStack := strings.Contains(got, "goroutine "); hasStack == disable {
			t.Errorf("DisableStackTrace %v: stack logged %v", disable, hasStack)
		}
	}
}

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	"io"
	stdhttp "net/http"
	"testing"
	"time"
)

// cvHeaderGate is an OnHeaderAsync hook that hands its resume to the test,
// having set its name in the header's X-Hooks.
type cvHeaderGate struct {
	name    string
	resumes chan func(error)
}

func newCvHeaderGate(name string) *cvHeaderGate {
	return &cvHeaderGate{name: name, resumes: make(chan func(error), 1)}
}

func (g *cvHeaderGate) hook(_ int, h stdhttp.Header, resume func(error)) {
	h.Add("X-Hooks", g.name)
	g.resumes <- resume
}

// next takes the hook's resume, failing the test if the hook is not called.
func (g *cvHeaderGate) next(t *testing.T) func(error) {
	t.Helper()
	select {
	case resume := <-g.resumes:
		return resume
	case <-time.After(10 * time.Second):
		t.Fatalf("hook %s was never called", g.name)
		return nil
	}
}

// Two OnHeaderAsync hooks hold the header in turn, the one registered last
// first, whether the response is sent whole or streams.
func TestCoverAsyncHeaderTwoHooks(t *testing.T) {
	outer, inner := newCvHeaderGate("outer"), newCvHeaderGate("inner")
	returned := make(chan struct{}, 1)
	addr := serveHTTP1(t, func(c *Context) {
		defer func() { returned <- struct{}{} }()
		c.OnHeaderAsync(outer.hook)
		c.OnHeaderAsync(inner.hook)
		if c.Request.URL.Path == "/whole" {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("whole"))
			return
		}
		c.WriteHeader(stdhttp.StatusOK)
		_, _ = c.WriteString("streamed")
		c.Flush()
	})
	for _, path := range []string{"/whole", "/streamed"} {
		conn := dialRaw(t, addr)
		conn.send("GET " + path + " HTTP/1.1\r\nHost: test\r\n\r\n")
		resume := inner.next(t)
		// Resumed once the handler is done writing, which it does not
		// synchronize with the resume.
		cvCliWait(t, returned, "the handler to return")
		resume(nil)
		outer.next(t)(nil)
		resp, body := conn.response("GET")
		if got := resp.Header.Values("X-Hooks"); len(got) != 2 || got[0] != "inner" || got[1] != "outer" ||
			body != path[1:] {
			t.Fatalf("%s: hooks %v, body %q", path, got, body)
		}
	}
}

// Finish called while the header is held is left to the resume, which ends
// the body after what was written meanwhile; OnWritable given then runs
// once the resume has sent it, and OnWritable given once the header has gone
// is the connection's own.
func TestCoverAsyncHeaderFinishWhileHeld(t *testing.T) {
	gate := newCvHeaderGate("gate")
	writable := make(chan error, 1)
	results := make(chan cvWrResult, 1)
	addr := serveHTTP1(t, func(c *Context) {
		c.OnHeaderAsync(gate.hook)
		c.Retain()
		c.WriteHeader(stdhttp.StatusOK)
		c.Header().Set("Content-Type", "text/plain")
		_, err := c.WriteString("held")
		c.Flush()
		errs := []error{err}
		if !c.OnWritable(func(err error) {
			writable <- err
			if c.OnWritable(func(error) {}) {
				writable <- errors.New("OnWritable armed once the header went, with nothing queued")
			}
			c.Release()
		}) {
			errs = append(errs, errors.New("OnWritable not kept while the header is held"))
			c.Release()
		}
		errs = append(errs, c.Finish())
		results <- cvWrResult{c.Request.URL.Path, errs}
	})
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	resume := gate.next(t)
	for _, err := range cvWrWait(t, results).errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	resume(nil)
	if _, body := conn.response("GET"); body != "held" {
		t.Fatalf("body %q", body)
	}
	select {
	case err := <-writable:
		if err != nil {
			t.Fatalf("OnWritable: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("OnWritable never ran")
	}
	select {
	case err := <-writable:
		t.Fatal(err)
	default:
	}
}

// A hook that resumes in place lets the header go at once: OnWritable then
// is the connection's own, which has nothing queued.
func TestCoverAsyncHeaderResumedInPlace(t *testing.T) {
	results := make(chan cvWrResult, 1)
	addr := serveHTTP1(t, func(c *Context) {
		c.OnHeaderAsync(func(_ int, _ stdhttp.Header, resume func(error)) { resume(nil) })
		c.WriteHeader(stdhttp.StatusOK)
		c.Flush()
		var errs []error
		if c.OnWritable(func(error) {}) {
			errs = append(errs, errors.New("OnWritable armed with nothing queued"))
		}
		_, err := c.WriteString("body")
		results <- cvWrResult{c.Request.URL.Path, append(errs, err)}
	})
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	if _, body := conn.response("GET"); body != "body" {
		t.Fatalf("body %q", body)
	}
	for _, err := range cvWrWait(t, results).errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// OnWritable given while the header is held runs with the error a failing
// resume gives, and the connection closes.
func TestCoverAsyncHeaderWritableFails(t *testing.T) {
	gate := newCvHeaderGate("gate")
	writable := make(chan error, 1)
	ready := make(chan struct{})
	addr := serveHTTP1(t, func(c *Context) {
		c.OnHeaderAsync(gate.hook)
		c.WriteHeader(stdhttp.StatusOK)
		c.Flush()
		c.Retain()
		if !c.OnWritable(func(err error) {
			writable <- err
			c.Release()
		}) {
			writable <- nil
			c.Release()
		}
		close(ready)
	})
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	broken := errors.New("refused")
	resume := gate.next(t)
	cvCliWait(t, ready, "the handler to arm OnWritable")
	resume(broken)
	select {
	case err := <-writable:
		if err != broken {
			t.Fatalf("OnWritable: %v, want the resume's error", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("OnWritable never ran")
	}
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if got, _ := io.ReadAll(conn.r); len(got) != 0 {
		t.Fatalf("sent %q for a failed header", got)
	}
}

// A Write waiting for room while the header is held returns the error the
// hook resumes with.
func TestCoverAsyncHeaderWriteWaitFails(t *testing.T) {
	gate := newCvHeaderGate("gate")
	results := make(chan cvWrResult, 1)
	addr := serveHTTP1(t, func(c *Context) {
		c.OnHeaderAsync(gate.hook)
		// The hook is called from inside the Write, as it sets the status,
		// so the test's resume comes while the Write waits.
		_, err := c.Write(make([]byte, maxHeldBody+1))
		results <- cvWrResult{c.Request.URL.Path, []error{err}}
	})
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	broken := errors.New("refused")
	gate.next(t)(broken)
	if errs := cvWrWait(t, results).errs; errs[0] != broken {
		t.Fatalf("Write = %v, want the resume's error", errs[0])
	}
}

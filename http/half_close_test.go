//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"context"
	"io"
	"net"
	stdhttp "net/http"
	"testing"
	"time"
)

// halfCloseClient dials addr, sends requests, shuts its writing side, as
// `nc -N` or an HTTP/1.0 client does, and returns the connection with a
// reader over it.
func halfCloseClient(t *testing.T, addr, requests string) (*net.TCPConn, *bufio.Reader) {
	t.Helper()
	raw, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	conn := raw.(*net.TCPConn)
	t.Cleanup(func() { conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.WriteString(conn, requests); err != nil {
		t.Fatal(err)
	}
	if err := conn.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	return conn, bufio.NewReader(conn)
}

// readHalfClosedResponse reads one response and its whole body.
func readHalfClosedResponse(t *testing.T, r *bufio.Reader) (int, string) {
	t.Helper()
	resp, err := stdhttp.ReadResponse(r, nil)
	if err != nil {
		t.Fatalf("reading the response: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("reading the body: %v", err)
	}
	return resp.StatusCode, string(body)
}

// expectServerClose checks that the server closed the connection once it had
// answered, with nothing more sent.
func expectServerClose(t *testing.T, r *bufio.Reader) {
	t.Helper()
	rest, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("waiting for the server to close: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("the server sent %q after the last response", rest)
	}
}

// TestAsyncAnswersAHalfClosedClient covers a client that shuts its writing
// side right after its request, as `nc -N` does, while the handler answers
// later through Async: the end of input is not the client going, so the
// response still reaches it, and the connection then closes.
func TestAsyncAnswersAHalfClosedClient(t *testing.T) {
	addr := serveStreamingServer(t, DefaultConfig(), func(c *Context) {
		time.AfterFunc(50*time.Millisecond, c.Async(func() {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("late "+c.Request.URL.Path))
		}))
	})
	_, r := halfCloseClient(t, addr, "GET /delay HTTP/1.1\r\nHost: test\r\n\r\n")
	if status, body := readHalfClosedResponse(t, r); status != stdhttp.StatusOK || body != "late /delay" {
		t.Fatalf("got %d %q, want 200 %q", status, body, "late /delay")
	}
	expectServerClose(t, r)
}

// TestAsyncAnswersPipelinedRequestsBeforeAHalfClose checks that requests
// pipelined behind a retained one before the client ended its side are all
// answered, in order, before the connection closes.
func TestAsyncAnswersPipelinedRequestsBeforeAHalfClose(t *testing.T) {
	addr := serveStreamingServer(t, DefaultConfig(), func(c *Context) {
		if c.Request.URL.Path == "/inline" {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("now /inline"))
			return
		}
		time.AfterFunc(50*time.Millisecond, c.Async(func() {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("late "+c.Request.URL.Path))
		}))
	})
	_, r := halfCloseClient(t, addr, "GET /one HTTP/1.1\r\nHost: test\r\n\r\n"+
		"GET /inline HTTP/1.1\r\nHost: test\r\n\r\n"+
		"GET /two HTTP/1.1\r\nHost: test\r\n\r\n")
	for _, want := range []string{"late /one", "now /inline", "late /two"} {
		if status, body := readHalfClosedResponse(t, r); status != stdhttp.StatusOK || body != want {
			t.Fatalf("got %d %q, want 200 %q", status, body, want)
		}
	}
	expectServerClose(t, r)
}

// TestHalfCloseWithNothingOwedCloses checks that a client ending its side
// with nothing to answer, or only part of a request, is closed at once, as
// before.
func TestHalfCloseWithNothingOwedCloses(t *testing.T) {
	addr := serveStreamingServer(t, DefaultConfig(), func(c *Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	})
	for name, requests := range map[string]string{
		"idle":     "",
		"partial":  "GET /cut HTTP/1.1\r\nHost: te",
		"answered": "GET /a HTTP/1.1\r\nHost: test\r\n\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, r := halfCloseClient(t, addr, requests)
			if name == "answered" {
				if status, body := readHalfClosedResponse(t, r); status != stdhttp.StatusOK || body != "ok" {
					t.Fatalf("got %d %q, want 200 %q", status, body, "ok")
				}
			}
			expectServerClose(t, r)
		})
	}
}

// TestHalfCloseCancelsWithoutCuttingOff checks the net/http way a FIN is
// taken on a retained request: the request is cancelled with io.EOF, as
// OnCancel, Err and Context report, at once rather than when the handler is
// done, but its response is not cut off, and what the handler writes after
// still reaches a client that only shut its writing side. A request
// pipelined before the FIN is served the same way, and an OnCancel
// registered after the FIN runs at once.
func TestHalfCloseCancelsWithoutCuttingOff(t *testing.T) {
	type seen struct {
		path     string
		onCancel error
		late     error
		err      error
		ctxCause error
		writeErr error
	}
	results := make(chan seen, 4)
	addr := serveStreamingServer(t, DefaultConfig(), func(c *Context) {
		heard := make(chan error, 1)
		c.OnCancel(func(err error) { heard <- err })
		ctx := c.Context()
		go c.Async(func() {
			s := seen{path: c.Request.URL.Path}
			select {
			case s.onCancel = <-heard:
			case <-time.After(5 * time.Second):
			}
			c.OnCancel(func(err error) { s.late = err })
			s.err = c.Err()
			select {
			case <-ctx.Done():
				s.ctxCause = context.Cause(ctx)
			default:
			}
			s.writeErr = c.Respond(stdhttp.StatusOK, "text/plain", []byte("late "+s.path))
			results <- s
		})()
	})
	_, r := halfCloseClient(t, addr, "GET /one HTTP/1.1\r\nHost: test\r\n\r\n"+
		"GET /two HTTP/1.1\r\nHost: test\r\n\r\n")
	for _, path := range []string{"/one", "/two"} {
		if status, body := readHalfClosedResponse(t, r); status != stdhttp.StatusOK || body != "late "+path {
			t.Fatalf("got %d %q, want 200 %q", status, body, "late "+path)
		}
		s := <-results
		if s.path != path || s.onCancel != io.EOF || s.late != io.EOF || s.err != io.EOF ||
			s.ctxCause != io.EOF || s.writeErr != nil {
			t.Fatalf("%s: OnCancel %v, OnCancel after %v, Err %v, Context cause %v, Respond %v; "+
				"want io.EOF for all but Respond, which succeeds", path, s.onCancel, s.late, s.err, s.ctxCause, s.writeErr)
		}
	}
	expectServerClose(t, r)
}

// TestHalfCloseThenResetCutsOff checks that a client that ends its side and
// then goes for good, which resets the connection, cuts off a request
// retained across the half-close: OnCancel, heard at the FIN, is not called
// again, and the response is refused with the reason.
func TestHalfCloseThenResetCutsOff(t *testing.T) {
	cancelled := make(chan error, 2)
	started := make(chan *Context, 1)
	addr := serveStreamingServer(t, DefaultConfig(), func(c *Context) {
		c.Retain()
		c.OnCancel(func(err error) { cancelled <- err })
		started <- c
	})
	conn, _ := halfCloseClient(t, addr, "GET /held HTTP/1.1\r\nHost: test\r\n\r\n")
	c := <-started
	select {
	case err := <-cancelled:
		if err != io.EOF {
			t.Fatalf("OnCancel was given %v at the FIN, want io.EOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnCancel never ran at the FIN")
	}
	if err := c.cancelErr(); err != nil {
		t.Fatalf("the FIN cut the response off: %v", err)
	}
	_ = conn.SetLinger(0)
	conn.Close()
	deadline := time.Now().Add(5 * time.Second)
	for c.cancelErr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("the reset never cut the response off")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := c.Respond(stdhttp.StatusOK, "text/plain", []byte("too late")); err == nil {
		t.Fatal("a response after the reset was accepted")
	}
	c.Release()
	select {
	case err := <-cancelled:
		t.Fatalf("OnCancel ran again, with %v", err)
	case <-time.After(100 * time.Millisecond):
	}
}

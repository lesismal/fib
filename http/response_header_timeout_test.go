//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// TestResponseHeaderTimeout checks, over HTTP/1.1 and h2c, that a response
// whose header takes longer than ResponseHeaderTimeout fails with
// ErrResponseHeaderTimeout, that an interim response does not end the wait,
// and that a body slower than it, once the header is in, does not fail; and
// that Timeout, when it is the shorter, still fails the request as before.
func TestResponseHeaderTimeout(t *testing.T) {
	const wait = 150 * time.Millisecond
	ts := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		switch r.URL.Path {
		case "/head":
			time.Sleep(3 * wait)
		case "/interim":
			w.WriteHeader(stdhttp.StatusEarlyHints)
			time.Sleep(3 * wait)
		case "/body":
			w.WriteHeader(stdhttp.StatusOK)
			w.(stdhttp.Flusher).Flush()
			time.Sleep(3 * wait)
		}
		_, _ = w.Write([]byte("ok"))
	}))
	ts.Config.Protocols = new(stdhttp.Protocols)
	ts.Config.Protocols.SetHTTP1(true)
	ts.Config.Protocols.SetUnencryptedHTTP2(true)
	ts.Start()
	t.Cleanup(ts.Close)
	for _, h2c := range []bool{false, true} {
		for _, timeout := range []time.Duration{10 * time.Second, wait} {
			config := DefaultClientConfig()
			config.UnencryptedHTTP2 = h2c
			config.Timeout = timeout
			config.ResponseHeaderTimeout = wait
			if timeout == wait {
				config.ResponseHeaderTimeout = 10 * time.Second
			}
			client := newTestClient(t, config)
			for path, want := range map[string]error{"/": nil, "/head": ErrResponseHeaderTimeout, "/interim": ErrResponseHeaderTimeout, "/body": nil} {
				if timeout == wait && want != nil {
					want = errRequestTimeout
				} else if timeout == wait && path == "/body" {
					want = errRequestTimeout
				}
				start := time.Now()
				resp, err := client.Go(mustRequest(t, "GET", ts.URL+path, nil)).Wait()
				if err == nil {
					if got := readBody(t, resp); got != "ok" {
						t.Errorf("h2c=%v timeout=%v %s: body %q", h2c, timeout, path, got)
					}
				}
				if !errors.Is(err, want) || want == nil && err != nil {
					t.Errorf("h2c=%v timeout=%v %s: err = %v, want %v", h2c, timeout, path, err, want)
				}
				if want != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
					t.Errorf("h2c=%v timeout=%v %s: %v is not os.ErrDeadlineExceeded", h2c, timeout, path, err)
				}
				if want != nil && time.Since(start) > 2*wait+time.Second {
					t.Errorf("h2c=%v timeout=%v %s: failed after %v", h2c, timeout, path, time.Since(start))
				}
			}
		}
	}
}

// TestResponseHeaderTimeoutSkipsRetryWait checks that a request sent again,
// after the kept connection it went out on turned out closed, is not failed
// by ResponseHeaderTimeout while it waits for a new connection: the wait for
// the header starts again once it has been sent on that one.
func TestResponseHeaderTimeoutSkipsRetryWait(t *testing.T) {
	// The header wait also covers the first try, from its send until the
	// client sees the hang-up; the retry's dial takes twice it. Its rare
	// failures, once taken for a slow runner, were a late send's report
	// starting the wait for the retry (see TestResponseHeaderTimeoutLateSend).
	const wait = 2 * time.Second
	url := rawServer(t, func(conn net.Conn, _ int) {
		reader := bufio.NewReader(conn)
		for served := 0; ; served++ {
			if _, err := stdhttp.ReadRequest(reader); err != nil {
				return
			}
			if served == 1 {
				// Hang up on the second request, unanswered.
				return
			}
			_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
		}
	})
	var dials atomic.Int32
	config := DefaultClientConfig()
	config.ResponseHeaderTimeout = wait
	config.Dial = func(engine *fib.Engine, network, addr string, timeout time.Duration,
		handler fib.Handler, done func(*fib.Connection, error)) error {
		if dials.Add(1) > 1 {
			// The retry's connection takes longer than the header may.
			time.Sleep(2 * wait)
		}
		return engine.DialWithHandler(network, addr, timeout, handler, done)
	}
	client := newTestClient(t, config)
	for i := 0; i < 2; i++ {
		resp, err := client.Go(mustRequest(t, "GET", url, nil)).Wait()
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if got := readBody(t, resp); got != "ok" {
			t.Fatalf("request %d: body %q", i, got)
		}
	}
	if n := dials.Load(); n != 2 {
		t.Fatalf("%d dials, want 2: one reused and dropped, one for the retry", n)
	}
}

// TestResponseHeaderTimeoutLateSend checks the race behind
// TestResponseHeaderTimeoutSkipsRetryWait's rare failures: the send that put
// a request on a kept connection returns only after the server's hang-up has
// been dealt with, OnClose having taken the request off the connection and
// queued it to be sent again. That send's late report must not start the
// wait for the header, which would then run out while the request waits for
// its new connection, but leave it to the send on that one.
func TestResponseHeaderTimeoutLateSend(t *testing.T) {
	const wait = 150 * time.Millisecond
	url := rawServer(t, func(conn net.Conn, _ int) {
		reader := bufio.NewReader(conn)
		for {
			if _, err := stdhttp.ReadRequest(reader); err != nil {
				return
			}
			_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
		}
	})
	dial := make(chan struct{})
	config := DefaultClientConfig()
	config.ResponseHeaderTimeout = wait
	config.Dial = func(engine *fib.Engine, network, addr string, timeout time.Duration,
		handler fib.Handler, done func(*fib.Connection, error)) error {
		// The new connection takes longer than the header may.
		go func() {
			<-dial
			if err := engine.DialWithHandler(network, addr, timeout, handler, done); err != nil {
				done(nil, err)
			}
		}()
		return nil
	}
	client := newTestClient(t, config)
	type result struct {
		body string
		err  error
	}
	results := make(chan result, 1)
	r := client.do(mustRequest(t, "GET", url, nil), func(resp *ClientResponse, err error) {
		if err != nil {
			results <- result{err: err}
			return
		}
		body, err := io.ReadAll(resp.Body)
		results <- result{string(body), err}
	}, false, nil)
	// What OnClose did on the connection the server hung up on: r is off it
	// and back in the queue, where it waits for the dial. Then that
	// connection's send returns.
	old := &clientConn{}
	if !r.attach(old) {
		t.Fatal("request settled before it was sent")
	}
	r.detach()
	r.requestSent(old)
	time.Sleep(3 * wait)
	close(dial)
	got := <-results
	if got.err != nil || got.body != "ok" {
		t.Fatalf("body %q, err %v: the late send started the header wait", got.body, got.err)
	}
}

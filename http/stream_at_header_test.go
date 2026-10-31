//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"io"
	"net"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"
)

// atHeaderServer serves bodies past 64KB as they arrive, and those whose
// Content-Type is application/grpc from their headers. Its handler reports
// each request on started, with the first piece of its body, then answers
// with all of it.
func atHeaderServer(t *testing.T) (string, <-chan string) {
	t.Helper()
	config := DefaultConfig()
	config.StreamRequestBody = true
	config.StreamRequestBodyThreshold = 64 << 10
	config.StreamRequestBodyAtHeader = func(r *stdhttp.Request) bool {
		return r.Header.Get("Content-Type") == "application/grpc"
	}
	started := make(chan string, 4)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		if c.BodyComplete() {
			started <- "whole " + string(c.Body())
			_ = c.Respond(200, "text/plain", c.Body())
			return
		}
		var got []byte
		c.OnBody(func(data []byte, fin bool, err error) {
			if len(got) == 0 && len(data) > 0 {
				started <- "streamed " + string(data)
			}
			got = append(got, data...)
			if fin || err != nil {
				_ = c.Respond(200, "text/plain", append(got, " "+cvH2sErrString(err)...))
			}
		})
	})))
	return addr, started
}

// notYet fails the test if the handler runs within a while.
func notYet(t *testing.T, started <-chan string) {
	t.Helper()
	select {
	case got := <-started:
		t.Fatalf("the handler ran before the body ended: %s", got)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestStreamRequestBodyAtHeaderH2 checks that an HTTP/2 request picked by
// StreamRequestBodyAtHeader reaches its handler with its first small DATA
// frame, with or without a content-length, and that one not picked still
// waits for the threshold or the end.
func TestStreamRequestBodyAtHeaderH2(t *testing.T) {
	addr, started := atHeaderServer(t)
	tc := dialH2(t, addr)

	tc.headers(1, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", "/", "content-type", "application/grpc")
	tc.data(1, false, []byte("first"))
	if got := cvH2sWait(t, started); got != "streamed first" {
		t.Fatalf("handler saw %q", got)
	}
	tc.data(1, true, []byte(" second"))
	if _, body := tc.response(1); string(body) != "first second <nil>" {
		t.Fatalf("body %q", body)
	}

	tc.headers(3, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", "/", "content-type", "application/grpc",
		"content-length", "8")
	tc.data(3, false, []byte("abc"))
	if got := cvH2sWait(t, started); got != "streamed abc" {
		t.Fatalf("handler saw %q", got)
	}
	tc.data(3, true, []byte("defgh"))
	if _, body := tc.response(3); string(body) != "abcdefgh <nil>" {
		t.Fatalf("body %q", body)
	}

	tc.headers(5, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", "/", "content-type", "text/plain")
	tc.data(5, false, []byte("held"))
	notYet(t, started)
	tc.data(5, true, []byte(" whole"))
	if got := cvH2sWait(t, started); got != "whole held whole" {
		t.Fatalf("handler saw %q", got)
	}
	if _, body := tc.response(5); string(body) != "held whole" {
		t.Fatalf("body %q", body)
	}
}

// TestStreamRequestBodyAtHeaderH1 checks the same of HTTP/1.1, chunked and
// with a Content-Length.
func TestStreamRequestBodyAtHeaderH1(t *testing.T) {
	addr, started := atHeaderServer(t)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(conn)
	send := func(s string) {
		t.Helper()
		if _, err := io.WriteString(conn, s); err != nil {
			t.Fatal(err)
		}
	}
	answer := func() string {
		t.Helper()
		resp, err := stdhttp.ReadResponse(br, nil)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		return string(body)
	}

	send("POST / HTTP/1.1\r\nHost: t\r\nContent-Type: application/grpc\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n")
	if got := cvH2sWait(t, started); got != "streamed first" {
		t.Fatalf("handler saw %q", got)
	}
	send("7\r\n second\r\n0\r\n\r\n")
	if got := answer(); got != "first second <nil>" {
		t.Fatalf("body %q", got)
	}

	send("POST / HTTP/1.1\r\nHost: t\r\nContent-Type: application/grpc\r\nContent-Length: 8\r\n\r\nabc")
	if got := cvH2sWait(t, started); got != "streamed abc" {
		t.Fatalf("handler saw %q", got)
	}
	send("defgh")
	if got := answer(); got != "abcdefgh <nil>" {
		t.Fatalf("body %q", got)
	}

	send("POST / HTTP/1.1\r\nHost: t\r\nContent-Type: text/plain\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nheld\r\n")
	notYet(t, started)
	send("6\r\n whole\r\n0\r\n\r\n")
	if got := cvH2sWait(t, started); got != "whole held whole" {
		t.Fatalf("handler saw %q", got)
	}
	if got := answer(); got != "held whole" {
		t.Fatalf("body %q", got)
	}
}

// TestH2ClientSendsTETrailers checks that the HTTP/2 client keeps a request's
// "TE: trailers", the one value HTTP/2 allows, and drops any other.
func TestH2ClientSendsTETrailers(t *testing.T) {
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		_ = c.Respond(200, "text/plain", []byte(strings.Join(c.Request.Header["Te"], ",")))
	})))
	client := cvH2cClient(t, nil)
	for _, tc := range []struct{ te, want string }{{"trailers", "trailers"}, {"Trailers, gzip", "trailers"}, {"gzip", ""}} {
		req, _ := stdhttp.NewRequest(stdhttp.MethodGet, "http://"+addr+"/", nil)
		req.Header.Set("TE", tc.te)
		resp, err := client.Go(req).Wait()
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(body) != tc.want {
			t.Errorf("TE %q reached the server as %q, want %q", tc.te, body, tc.want)
		}
	}
}

// TestStreamRequestBodyAtHeaderLargeH1 checks that an HTTP/1 request picked by
// StreamRequestBodyAtHeader streams from its header even when its
// Content-Length is past StreamRequestBodyThreshold, as the option promises:
// such a request waited for the threshold, after sending 100 Continue, so that
// a handler could not refuse the upload with LimitBody before the client had
// begun sending it.
func TestStreamRequestBodyAtHeaderLargeH1(t *testing.T) {
	config := DefaultConfig()
	config.StreamRequestBody = true
	config.StreamRequestBodyThreshold = 1 << 20
	config.StreamRequestBodyAtHeader = func(r *stdhttp.Request) bool { return r.URL.Path == "/upload" }
	ran := make(chan struct{}, 1)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		ran <- struct{}{}
		_ = c.LimitBody(1 << 10)
	})))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.WriteString(conn, "POST /upload HTTP/1.1\r\nHost: t\r\nContent-Length: 8388608\r\nExpect: 100-continue\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler did not run at the header")
	}
	resp, err := stdhttp.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != stdhttp.StatusRequestEntityTooLarge {
		t.Fatalf("answered %d before the body, want 413 and no 100 Continue", resp.StatusCode)
	}
}

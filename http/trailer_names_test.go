//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"io"
	stdhttp "net/http"
	"strings"
	"testing"
)

// A trailer name that is not a token is left out of the Trailer field that
// announces the trailers, rather than written into it raw: copied from
// untrusted input, it would inject a field, or a response, of its own.
func TestTrailerNamesNotInjected(t *testing.T) {
	const evil = "X-Bad\r\nInjected: 1"
	if names := announcedTrailers(stdhttp.Header{"Trailer": {"X-Ok, " + evil + ", X-Two"}}); strings.Join(names, ",") != "X-Ok,X-Two" {
		t.Fatalf("announced %q", names)
	}
	req := &stdhttp.Request{Method: stdhttp.MethodGet, ProtoMajor: 1, ProtoMinor: 1}
	out, err := marshalResponse(req, Response{StatusCode: 200, Body: []byte("x"),
		Trailer: stdhttp.Header{"X-T": {"1"}, evil: {"2"}}}, false)
	if err != nil || bytes.Contains(out, []byte("Injected")) || !bytes.Contains(out, []byte("Trailer: X-T\r\n")) {
		t.Fatalf("marshalled %q, %v", out, err)
	}
	if _, err := appendResponseHead(nil, req, responseHead{status: 200, chunked: true, contentLength: -1,
		trailers: []string{"X-Ok", evil}}); err == nil {
		t.Fatal("a trailer name with a CRLF was written")
	}

	addr := serveHTTP1(t, func(c *Context) {
		c.Header().Set("Trailer", "X-Ok, "+evil)
		_, _ = c.WriteString("body")
		c.Flush()
		c.Header().Set("X-Ok", "1")
	})
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: a\r\n\r\n")
	resp, body := conn.response(stdhttp.MethodGet)
	if resp.StatusCode != stdhttp.StatusOK || body != "body" {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
	if _, bad := resp.Header["Injected"]; bad {
		t.Fatalf("header %v", resp.Header)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.Trailer.Get("X-Ok") != "1" || len(resp.Trailer) != 1 {
		t.Fatalf("trailer %v", resp.Trailer)
	}
}

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	stdhttp "net/http"
	"strings"
	"sync/atomic"
	"testing"
)

// A bare LF in a request's head or trailer section is refused: net/http's
// reader would end the section at it, dropping what the parser framed past
// it, a request a proxy in front passed on. So is obs-fold in the head.
func TestParserRefusesBareLFAndObsFold(t *testing.T) {
	for name, raw := range map[string]string{
		"head":          "GET / HTTP/1.1\r\nHost: a\r\nX: y\n\nGET /admin HTTP/1.1\r\nHost: a\r\n\r\n",
		"request line":  "GET / HTTP/1.1\nHost: a\r\n\r\n",
		"head end":      "GET / HTTP/1.1\r\nHost: a\r\nX: y\n\r\n\r\n",
		"trailer":       chunkedHead + "0\r\nX: a\n\nGET /hidden HTTP/1.1\r\nHost: a\r\n\r\n",
		"trailer extra": chunkedHead + "1;e\r\na\r\n0\r\nX: a\nY: b\r\n\r\n",
		"fold":          "POST / HTTP/1.1\r\nHost: a\r\nContent-Length:\r\n 1\r\n\r\na",
		"fold tab":      "GET / HTTP/1.1\r\nHost: a\r\nX: a\r\n\tb\r\n\r\n",
	} {
		for _, stream := range []bool{false, true} {
			config := DefaultConfig()
			config.StreamRequestBody = stream
			parser := NewParser(config)
			reqs, err := parser.Feed([]byte(raw))
			if err == nil && stream && len(reqs) == 1 && parser.stream != nil {
				_, _, err = parser.stream.absorb(parser.buffer)
			}
			if !errors.Is(err, ErrMalformed) {
				t.Errorf("%s (stream %v): %d requests, error %v, want ErrMalformed", name, stream, len(reqs), err)
			}
		}
	}
	// What a well-formed one looks like still parses, the slow path's
	// included.
	for _, raw := range []string{
		"GET / HTTP/1.1\r\nHost: a\r\nX: y\r\nX: z\r\n\r\n",
		"PROPFIND / HTTP/1.1\r\nHost: a\r\n\r\n",
		chunkedHead + "1;e\r\na\r\n0\r\nX-T: 1\r\n\r\n",
	} {
		parser := NewParser(DefaultConfig())
		if _, complete, err := parser.FeedOne([]byte(raw)); err != nil || !complete {
			t.Errorf("%q: FeedOne = %v, %v", raw, complete, err)
		}
	}
}

// The server answers such a request with 400 and serves neither it nor what
// was hidden in it.
func TestHTTP1ConformanceServerBareLF(t *testing.T) {
	var served atomic.Int64
	addr := serveHTTP1(t, func(c *Context) {
		served.Add(1)
		c.String(stdhttp.StatusOK, "ok")
	})
	for _, raw := range []string{
		"GET / HTTP/1.1\r\nHost: a\r\nX: y\n\nGET /admin HTTP/1.1\r\nHost: a\r\n\r\n",
		chunkedHead + "0\r\nX: a\n\nGET /hidden HTTP/1.1\r\nHost: a\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: a\r\nContent-Length:\r\n 1\r\n\r\na",
	} {
		conn := dialRaw(t, addr)
		conn.send(raw)
		if resp, _ := conn.response(stdhttp.MethodGet); resp.StatusCode != stdhttp.StatusBadRequest {
			t.Errorf("%q: status %d, want 400", raw, resp.StatusCode)
		}
	}
	if n := served.Load(); n != 0 {
		t.Fatalf("served %d requests", n)
	}
}

// The client's response parser refuses a bare LF in a head it leaves to
// net/http, and in a trailer section, for the same reason.
func TestResponseParserRefusesBareLF(t *testing.T) {
	req, _ := stdhttp.NewRequest(stdhttp.MethodGet, "http://a/", nil)
	for name, raw := range map[string]string{
		"head":         "HTTP/1.1 200 OK\r\nX: y\n\nContent-Length: 0\r\n\r\n",
		"coded head":   "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\nX: y\n\r\n\r\n",
		"trailer":      "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nX: a\n\nY: b\r\n\r\n",
		"trailer ext":  "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1;e\r\na\r\n0\r\nX: a\nY: b\r\n\r\n",
		"chunk bad LF": "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1;e\nx\r\na\r\n0\r\n\r\n",
	} {
		p := &responseParser{maxHeader: 1 << 20, maxBody: 1 << 20}
		_, err := p.feed([]byte(raw), req)
		if !errors.Is(err, ErrMalformedResponse) {
			t.Errorf("%s: error %v, want ErrMalformedResponse", name, err)
		}
		p.reset()
	}
	// A folded line is a client's to take (RFC 9112 section 5.2).
	p := &responseParser{maxHeader: 1 << 20, maxBody: 1 << 20}
	defer p.reset()
	resp, err := p.feed([]byte("HTTP/1.1 200 OK\r\nX: a\r\n b\r\nContent-Length: 0\r\n\r\n"), req)
	if err != nil || resp == nil || !strings.HasPrefix(resp.Header.Get("X"), "a") {
		t.Fatalf("folded response: %v, %v", resp, err)
	}
}

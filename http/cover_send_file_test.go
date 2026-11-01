//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"errors"
	"io"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// cvSendFile is a file of size bytes, open for the test, and its content.
func cvSendFile(t *testing.T, size int) (*os.File, []byte) {
	t.Helper()
	data := bytes.Repeat([]byte("0123456789abcdef"), size/16+1)[:size]
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f, data
}

// The cases SendFile settles without sending any of the file: a range it
// refuses, a response already written, nothing to send, HEAD, which counts
// the range as its length, and a status that has no body; and the file
// pieces it reads itself, from a file shorter than the range, and past a
// Content-Length the handler declared.
func TestCoverSendFileWithoutSending(t *testing.T) {
	f, data := cvSendFile(t, 64<<10)
	small, _ := cvSendFile(t, 10)
	results := make(chan cvWrResult, 1)
	addr := serveHTTP1(t, func(c *Context) {
		path := c.Request.URL.Path
		done := func(err error) { results <- cvWrResult{path, []error{err}} }
		switch path {
		case "/invalid":
			c.SendFile(nil, 0, 1, done)
		case "/written":
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("first"))
			c.SendFile(f, 0, 1, done)
		case "/empty":
			c.SendFile(f, 0, 0, nil)
			results <- cvWrResult{path, nil}
		case "/head":
			c.SendFile(f, 0, int64(len(data)), done)
		case "/nobody":
			c.WriteHeader(stdhttp.StatusNoContent)
			c.SendFile(f, 0, int64(len(data)), done)
		case "/short":
			c.SendFile(small, 0, 100, done)
		case "/declared":
			c.Header().Set("Content-Length", "2")
			c.SendFile(small, 0, 5, done)
		case "/chunked":
			// Begun with no length, the response is chunked, and the file
			// goes as one chunk.
			c.WriteHeader(stdhttp.StatusOK)
			c.Flush()
			c.SendFile(f, 0, int64(len(data)), done)
		}
	})
	for _, tc := range []struct {
		method, path string
		want         error
		body         string
	}{
		{"GET", "/invalid", errors.New("http: invalid SendFile range"), ""},
		{"GET", "/written", ErrResponseWritten, "first"},
		{"GET", "/empty", nil, ""},
		{"HEAD", "/head", nil, ""},
		{"GET", "/nobody", stdhttp.ErrBodyNotAllowed, ""},
		{"GET", "/short", io.ErrUnexpectedEOF, ""},
		{"GET", "/declared", stdhttp.ErrContentLength, ""},
		{"GET", "/chunked", nil, string(data)},
	} {
		conn := dialRaw(t, addr)
		conn.send(tc.method + " " + tc.path + " HTTP/1.1\r\nHost: test\r\n\r\n")
		r := cvWrWait(t, results)
		var got error
		if len(r.errs) > 0 {
			got = r.errs[0]
		}
		if got != tc.want && (got == nil || tc.want == nil || got.Error() != tc.want.Error()) {
			t.Errorf("%s %s: done(%v), want %v", tc.method, tc.path, got, tc.want)
		}
		switch tc.path {
		case "/short", "/declared":
			// The response ends short, which closes the connection.
			continue
		}
		resp, body := conn.response(tc.method)
		if body != tc.body {
			t.Errorf("%s %s: body %.20q, want %.20q", tc.method, tc.path, body, tc.body)
		}
		if tc.path == "/head" && resp.ContentLength != int64(len(data)) {
			t.Errorf("HEAD: Content-Length %d, want %d", resp.ContentLength, len(data))
		}
		if tc.path == "/chunked" && !strings.EqualFold(strings.Join(resp.TransferEncoding, ","), "chunked") {
			t.Errorf("chunked: Transfer-Encoding %v", resp.TransferEncoding)
		}
	}
}

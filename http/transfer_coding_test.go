//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"
	"testing"
)

// codingCase is a transfer coding's name and how to send a body in it.
type codingCase struct {
	name   string
	encode func(*testing.T, []byte) []byte
}

// codings are the transfer codings the server and client decode, with gzip
// under its two names and deflate in both the zlib and the raw shapes
// senders use.
func codings() []codingCase {
	return []codingCase{
		{"gzip", gzipBody},
		{"x-gzip", gzipBody},
		{"deflate", zlibBody},
		{"deflate", rawDeflateBody},
	}
}

func gzipBody(t *testing.T, data []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	w := gzip.NewWriter(&out)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func zlibBody(t *testing.T, data []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	w := zlib.NewWriter(&out)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func rawDeflateBody(t *testing.T, data []byte) []byte {
	t.Helper()
	var out bytes.Buffer
	w, err := flate.NewWriter(&out, flate.DefaultCompression)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

// chunkedBody frames data as a chunked body in small chunks, so that a
// streamed decoder meets the body in more than one piece.
func chunkedBody(data []byte) string {
	var b strings.Builder
	for len(data) > 0 {
		n := min(len(data), 7)
		fmt.Fprintf(&b, "%x\r\n", n)
		b.Write(data[:n])
		b.WriteString("\r\n")
		data = data[n:]
	}
	b.WriteString("0\r\n\r\n")
	return b.String()
}

// codedRequest is a POST whose body is data in coding, chunked.
func codedRequest(coding string, data []byte) string {
	return "POST / HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: " + coding + ", chunked\r\n\r\n" +
		chunkedBody(data)
}

// TestTransferCodingRequest decodes gzip, x-gzip and both shapes of deflate
// on HTTP/1 requests, buffered whole and streamed.
func TestTransferCodingRequest(t *testing.T) {
	payload := bytes.Repeat([]byte("a request body sent in a transfer coding. "), 200)
	for _, tc := range codings() {
		for _, stream := range []bool{false, true} {
			name := tc.name + "/buffered"
			config := DefaultConfig()
			if stream {
				name = tc.name + "/streamed"
				config = immediateStreamingConfig()
			}
			t.Run(name, func(t *testing.T) {
				addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(collectBody(sizeAndDigest))))
				conn := dialRaw(t, addr)
				body := tc.encode(t, payload)
				conn.send(codedRequest(tc.name, body))
				resp, got := conn.response(stdhttp.MethodPost)
				if resp.StatusCode != stdhttp.StatusOK || got != wantSizeAndDigest(payload) {
					t.Fatalf("status %d, body %q, want %q", resp.StatusCode, got, wantSizeAndDigest(payload))
				}
				// The connection is still framed for the request after it.
				conn.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
				if resp, _ := conn.response(stdhttp.MethodGet); resp.StatusCode != stdhttp.StatusOK {
					t.Fatalf("request after a coded one: status %d", resp.StatusCode)
				}
			})
		}
	}
}

// A body sent in gzip but not chunked cannot be framed, and one in a coding
// the server does not decode is refused, as one in more than one coding is.
func TestTransferCodingRequestRefused(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context) { _ = c.Respond(200, "", []byte("ok")) })
	for _, tc := range []struct {
		name, request string
		status        int
	}{
		{"gzip without chunked", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: gzip\r\n\r\n", 400},
		{"compress", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: compress, chunked\r\n\r\n", 501},
		{"br", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: br, chunked\r\n\r\n", 501},
		{"two codings", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: gzip, deflate, chunked\r\n\r\n", 501},
		{"chunked twice", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked, chunked\r\n\r\n", 400},
		{"chunked before gzip", "POST / HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked, gzip\r\n\r\n", 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := dialRaw(t, addr)
			conn.send(tc.request)
			resp, _ := conn.response(stdhttp.MethodPost)
			if resp.StatusCode != tc.status {
				t.Fatalf("status %d, want %d", resp.StatusCode, tc.status)
			}
			if !conn.closed() {
				t.Fatal("connection kept open after a refused request")
			}
		})
	}
}

// A body that does not decode fails the request rather than reaching the
// handler as if it had.
func TestTransferCodingRequestMalformed(t *testing.T) {
	payload := bytes.Repeat([]byte("not a gzip stream"), 100)
	for _, stream := range []bool{false, true} {
		name := "buffered"
		config := DefaultConfig()
		if stream {
			name = "streamed"
			config = immediateStreamingConfig()
		}
		t.Run(name, func(t *testing.T) {
			addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(collectBody(func(c *Context, _ int64, _ []byte, err error) {
				if err == nil {
					_ = c.Respond(stdhttp.StatusOK, "text/plain", nil)
					return
				}
				_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", []byte(err.Error()))
			}))))
			conn := dialRaw(t, addr)
			conn.send(codedRequest("gzip", payload))
			resp, body := conn.response(stdhttp.MethodPost)
			// A request buffered whole is refused before its handler runs, so
			// the reason is the status; a streamed one is told to the handler.
			if resp.StatusCode != stdhttp.StatusBadRequest || stream && !strings.Contains(body, "transfer coding") {
				t.Fatalf("status %d, body %q", resp.StatusCode, body)
			}
		})
	}
}

// MaxBodyBytes and MaxStreamedBodyBytes bound what a coded request decodes
// to, not what it was sent as: a small gzip body that inflates past them is
// refused.
func TestTransferCodingRequestDecodedLimit(t *testing.T) {
	payload := bytes.Repeat([]byte("z"), 64<<10)
	encoded := gzipBody(t, payload)
	if len(encoded) >= 1<<10 {
		t.Fatalf("the coded body is %d bytes, too big to prove the point", len(encoded))
	}

	t.Run("buffered", func(t *testing.T) {
		config := DefaultConfig()
		config.MaxBodyBytes = 1 << 10
		addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(collectBody(sizeAndDigest))))
		conn := dialRaw(t, addr)
		conn.send(codedRequest("gzip", encoded))
		resp, _ := conn.response(stdhttp.MethodPost)
		if resp.StatusCode != stdhttp.StatusRequestEntityTooLarge {
			t.Fatalf("status %d, want 413", resp.StatusCode)
		}
	})

	t.Run("streamed", func(t *testing.T) {
		config := immediateStreamingConfig()
		config.MaxStreamedBodyBytes = 1 << 10
		addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(collectBody(func(c *Context, _ int64, _ []byte, err error) {
			if errors.Is(err, ErrBodyTooLarge) {
				_ = c.Respond(stdhttp.StatusRequestEntityTooLarge, "text/plain", []byte("too large"))
				return
			}
			_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", []byte(fmt.Sprint(err)))
		}))))
		conn := dialRaw(t, addr)
		conn.send(codedRequest("gzip", encoded))
		resp, body := conn.response(stdhttp.MethodPost)
		if resp.StatusCode != stdhttp.StatusRequestEntityTooLarge {
			t.Fatalf("status %d, body %q, want 413", resp.StatusCode, body)
		}
	})
}

// Content-Encoding is the caller's: decoding a transfer coding leaves the
// header and the bytes it names alone.
func TestTransferCodingLeavesContentEncoding(t *testing.T) {
	inner := gzipBody(t, []byte("the representation inside the coding"))
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", []byte(err.Error()))
			return
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%s %x", c.Request.Header.Get("Content-Encoding"), body))
	})))
	conn := dialRaw(t, addr)
	conn.send("POST / HTTP/1.1\r\nHost: test\r\nContent-Encoding: gzip\r\nTransfer-Encoding: gzip, chunked\r\n\r\n" +
		chunkedBody(gzipBody(t, inner)))
	resp, body := conn.response(stdhttp.MethodPost)
	if resp.StatusCode != stdhttp.StatusOK || body != fmt.Sprintf("gzip %x", inner) {
		t.Fatalf("status %d, body %q", resp.StatusCode, body)
	}
}

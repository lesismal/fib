//go:build linux || darwin || windows

package compress_test

import (
	"compress/flate"
	"compress/gzip"
	"io"
	stdhttp "net/http"
	"strings"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware"
	"github.com/lesismal/fib/middleware/compress"
	"github.com/lesismal/fib/middleware/etag"
	"github.com/lesismal/fib/middleware/internal/mwtest"
)

var text = strings.Repeat("the quick brown fox jumps over the lazy dog\n", 500)

func serveText(t *testing.T) string {
	return mwtest.Serve(t, middleware.Chain(fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
		switch r.URL.Path {
		case "/writer":
			// Long enough that HTTP/1 would stream it, were it not held.
			c.Header().Set("Content-Type", "text/plain")
			for i := 0; i < len(text); i += 1000 {
				_, _ = io.WriteString(c, text[i:min(i+1000, len(text))])
				c.Flush()
			}
		case "/short":
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("short"))
		case "/image":
			_ = c.Respond(stdhttp.StatusOK, "image/png", []byte(text))
		case "/encoded":
			_ = c.WriteResponse(fibhttp.Response{
				Header: stdhttp.Header{"Content-Type": {"text/plain"}, "Content-Encoding": {"br"}},
				Body:   []byte(text),
			})
		default:
			_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte(text))
		}
	}), compress.New(), etag.New()))
}

func decode(t *testing.T, encoding, body string) string {
	t.Helper()
	var r io.Reader
	switch encoding {
	case "gzip":
		zr, err := gzip.NewReader(strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		r = zr
	case "deflate":
		r = flate.NewReader(strings.NewReader(body))
	default:
		return body
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestCompress(t *testing.T) {
	url := serveText(t)
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		for _, tc := range []struct{ path, accept, want string }{
			{"/", "gzip, deflate, br", "gzip"},
			{"/", "deflate;q=1, gzip;q=0.5", "deflate"},
			{"/", "gzip;q=0, *", "deflate"},
			{"/", "identity", ""},
			{"/", "", ""},
			{"/writer", "gzip", "gzip"},
			{"/short", "gzip", ""},
			{"/image", "gzip", ""},
		} {
			header := stdhttp.Header{}
			if tc.accept != "" {
				header.Set("Accept-Encoding", tc.accept)
			}
			resp, body := c.Do(t, "GET", url+tc.path, header, "")
			got := resp.Header.Get("Content-Encoding")
			if got != tc.want {
				t.Errorf("%s with %q: Content-Encoding %q, want %q", tc.path, tc.accept, got, tc.want)
				continue
			}
			want := text
			if tc.path == "/short" {
				want = "short"
			}
			if decoded := decode(t, got, body); decoded != want {
				t.Errorf("%s with %q: body decodes to %d bytes, want %d", tc.path, tc.accept, len(decoded), len(want))
			}
			if tc.want != "" && len(body) >= len(want) {
				t.Errorf("%s: compressed to %d bytes from %d", tc.path, len(body), len(want))
			}
			if vary := resp.Header.Get("Vary"); (tc.path == "/" || tc.path == "/writer") != (vary == "Accept-Encoding") {
				t.Errorf("%s: Vary %q", tc.path, vary)
			}
		}
		resp, body := c.Do(t, "GET", url+"/encoded", stdhttp.Header{"Accept-Encoding": {"gzip"}}, "")
		if resp.Header.Get("Content-Encoding") != "br" || body != text {
			t.Errorf("an encoded body was encoded again: %q", resp.Header.Get("Content-Encoding"))
		}
	})
}

// TestCompressETag checks that compress makes etag's ETag weak, and that the
// weak one still matches when it comes back.
func TestCompressETag(t *testing.T) {
	url := serveText(t)
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		gz := stdhttp.Header{"Accept-Encoding": {"gzip"}}
		resp, _ := c.Do(t, "GET", url, gz, "")
		tag := resp.Header.Get("Etag")
		if !strings.HasPrefix(tag, `W/"`) {
			t.Fatalf("ETag of a compressed body is %q, want a weak one", tag)
		}
		resp, body := c.Do(t, "GET", url, stdhttp.Header{"Accept-Encoding": {"gzip"}, "If-None-Match": {tag}}, "")
		if resp.StatusCode != stdhttp.StatusNotModified || body != "" || resp.Header.Get("Content-Encoding") != "" {
			t.Errorf("revalidation: %d, %d bytes, Content-Encoding %q", resp.StatusCode, len(body), resp.Header.Get("Content-Encoding"))
		}
	})
}

func TestCompressible(t *testing.T) {
	for mediaType, want := range map[string]bool{
		"text/html": true, "application/json": true, "application/problem+json": true,
		"image/svg+xml": true, "image/png": false, "application/zip": false, "video/mp4": false,
	} {
		if got := compress.Compressible(mediaType); got != want {
			t.Errorf("Compressible(%q) = %v", mediaType, got)
		}
	}
}

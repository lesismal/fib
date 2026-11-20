//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package compress_test

import (
	"compress/flate"
	"compress/gzip"
	"crypto/rand"
	"fmt"
	"io"
	stdhttp "net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/compress"
	"github.com/lesismal/fib/middleware/mwtest"
)

// TestCompressSkipsWhatItMustNotTouch checks the responses compress leaves
// as they are: those its Next skips, partial ones, those whose Cache-Control
// forbids transforming them, those that compress to no fewer bytes, and the
// response to HEAD, which still says in Vary that a GET's would differ.
func TestCompressSkipsWhatItMustNotTouch(t *testing.T) {
	random := make([]byte, 4096)
	_, _ = rand.Read(random)
	handler := compress.New(compress.Config{
		Next: func(c *fibhttp.Context) bool { return c.Request.URL.Path == "/skip" },
	})(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		header := stdhttp.Header{"Content-Type": {"text/plain"}}
		body := []byte(text)
		switch c.Request.URL.Path {
		case "/range":
			header.Set("Content-Range", "bytes 0-"+strconv.Itoa(len(text)-1)+"/"+strconv.Itoa(len(text)))
		case "/no-transform":
			header.Set("Cache-Control", "public, No-Transform")
		case "/random":
			body = random
		case "/declared":
			// A HEAD response that declares its length without a body.
			header.Set("Content-Length", strconv.Itoa(len(text)))
			body = nil
		}
		_ = c.WriteResponse(fibhttp.Response{Header: header, Body: body})
	}))
	url := mwtest.Serve(t, handler)
	gz := stdhttp.Header{"Accept-Encoding": {"gzip"}}
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		for _, path := range []string{"/skip", "/range", "/no-transform", "/random"} {
			resp, body := c.Do(t, "GET", url+path, gz, "")
			if enc := resp.Header.Get("Content-Encoding"); enc != "" {
				t.Errorf("%s: Content-Encoding %q, want none", path, enc)
			}
			want := text
			if path == "/random" {
				want = string(random)
			}
			if body != want {
				t.Errorf("%s: body of %d bytes, want %d", path, len(body), len(want))
			}
			// Only the incompressible body was eligible, and so varies.
			if vary := resp.Header.Get("Vary"); (path == "/random") != (vary == "Accept-Encoding") {
				t.Errorf("%s: Vary %q", path, vary)
			}
		}
		for _, path := range []string{"/", "/declared"} {
			resp, _ := c.Do(t, "HEAD", url+path, gz, "")
			if enc := resp.Header.Get("Content-Encoding"); enc != "" {
				t.Errorf("HEAD %s: Content-Encoding %q, want none", path, enc)
			}
			if vary := resp.Header.Get("Vary"); vary != "Accept-Encoding" {
				t.Errorf("HEAD %s: Vary %q, want Accept-Encoding", path, vary)
			}
		}
	})
}

// TestCompressLevels checks that each level compresses a body that decodes
// back to the original, and that MinLength set lower compresses a body the
// default would leave alone.
func TestCompressLevels(t *testing.T) {
	for _, level := range []compress.Level{0, compress.LevelBestSpeed, compress.LevelDefault, 4, compress.LevelBestCompression} {
		short := text[:200]
		handler := compress.New(compress.Config{Level: level, MinLength: 100})(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(short))
		}))
		url := mwtest.Serve(t, handler)
		c := mwtest.Clients(t)[0]
		for _, encoding := range []string{"gzip", "deflate"} {
			resp, body := c.Do(t, "GET", url, stdhttp.Header{"Accept-Encoding": {encoding}}, "")
			if got := resp.Header.Get("Content-Encoding"); got != encoding {
				t.Fatalf("level %d: Content-Encoding %q, want %q", level, got, encoding)
			}
			if decoded := decode(t, encoding, body); decoded != short {
				t.Errorf("level %d %s: decodes to %d bytes, want %d", level, encoding, len(decoded), len(short))
			}
		}
	}
}

// countingEncoder is a standard library Encoder that counts the bodies it
// finishes.
type countingEncoder struct {
	compress.Encoder
	closed *atomic.Int64
}

func (e countingEncoder) Close() error {
	e.closed.Add(1)
	return e.Encoder.Close()
}

// TestCompressEncoders checks that the Encoders a Config hands over are the
// ones that compress, made with its Level, LevelBestSpeed when left out; and
// that New panics on a level the Encoders refuse.
func TestCompressEncoders(t *testing.T) {
	for _, tt := range []struct {
		level compress.Level
		want  int
	}{{0, 1}, {compress.LevelDefault, -1}, {7, 7}} {
		var levels sync.Map
		var gzipped, deflated atomic.Int64
		handler := compress.New(compress.Config{
			Level: tt.level,
			Gzip: func(w io.Writer, level int) (compress.Encoder, error) {
				levels.Store(level, true)
				zw, err := gzip.NewWriterLevel(w, level)
				return countingEncoder{zw, &gzipped}, err
			},
			Deflate: func(w io.Writer, level int) (compress.Encoder, error) {
				levels.Store(level, true)
				fw, err := flate.NewWriter(w, level)
				return countingEncoder{fw, &deflated}, err
			},
		})(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(text))
		}))
		url := mwtest.Serve(t, handler)
		c := mwtest.Clients(t)[0]
		for _, encoding := range []string{"gzip", "deflate", "gzip"} {
			resp, body := c.Do(t, "GET", url, stdhttp.Header{"Accept-Encoding": {encoding}}, "")
			if got := resp.Header.Get("Content-Encoding"); got != encoding || decode(t, encoding, body) != text {
				t.Fatalf("level %d %s: Content-Encoding %q, %d bytes", tt.level, encoding, got, len(body))
			}
		}
		if gzipped.Load() != 2 || deflated.Load() != 1 {
			t.Errorf("level %d: %d gzip and %d deflate bodies, want 2 and 1", tt.level, gzipped.Load(), deflated.Load())
		}
		n := 0
		levels.Range(func(key, _ any) bool {
			if n++; key.(int) != tt.want {
				t.Errorf("level %d: an Encoder made at %d, want %d", tt.level, key, tt.want)
			}
			return true
		})
	}

	defer func() {
		if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), "gzip level 42") {
			t.Errorf("level 42: panic %v", r)
		}
	}()
	compress.New(compress.Config{Level: 42})
}

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// gzipFilter compresses a body as it is written, as a compression
// middleware's filter does.
type gzipFilter struct {
	buf bytes.Buffer
	w   *gzip.Writer
}

func newGzipFilter() *gzipFilter {
	f := &gzipFilter{}
	f.w = gzip.NewWriter(&f.buf)
	return f
}

func (f *gzipFilter) Filter(p []byte) ([]byte, error) {
	f.buf.Reset()
	_, err := f.w.Write(p)
	return f.buf.Bytes(), err
}

func (f *gzipFilter) Flush() ([]byte, error) {
	f.buf.Reset()
	err := f.w.Flush()
	return f.buf.Bytes(), err
}

func (f *gzipFilter) Close() ([]byte, error) {
	f.buf.Reset()
	err := f.w.Close()
	return f.buf.Bytes(), err
}

// upperFilter upper-cases a body, in a buffer of its own it reuses.
type upperFilter struct{ buf []byte }

func (f *upperFilter) Filter(p []byte) ([]byte, error) {
	f.buf = append(f.buf[:0], bytes.ToUpper(p)...)
	return f.buf, nil
}

func (f *upperFilter) Close() ([]byte, error) { return []byte("!"), nil }

// failFilter fails the body.
type failFilter struct{}

var errFilter = errors.New("filter failed")

func (failFilter) Filter([]byte) ([]byte, error) { return nil, errFilter }
func (failFilter) Close() ([]byte, error)        { return nil, nil }

// TestBodyFilter writes a response each way a handler writes one through a
// gzip filter, and checks that what arrives, over HTTP/1.1 and HTTP/2,
// decompresses to the body written, without a Content-Length that would not
// match it.
func TestBodyFilter(t *testing.T) {
	long := strings.Repeat("a long body that compresses well. ", 6000) // ~200KB
	file := filepath.Join(t.TempDir(), "f.txt")
	if err := os.WriteFile(file, []byte(long), 0o644); err != nil {
		t.Fatal(err)
	}
	write := map[string]func(c *Context){
		"respond": func(c *Context) { _ = c.Respond(200, "text/plain", []byte(long)) },
		"small":   func(c *Context) { _, _ = io.WriteString(c, "short") },
		"declared": func(c *Context) {
			c.Header().Set("Content-Length", fmt.Sprint(len(long)))
			_, _ = io.WriteString(c, long)
		},
		"streamed": func(c *Context) {
			for i := 0; i < len(long); i += 7000 {
				_, _ = io.WriteString(c, long[i:min(i+7000, len(long))])
				c.Flush()
			}
		},
		"nowait": func(c *Context) {
			for i := 0; i < len(long); i += 9000 {
				if _, err := c.WriteNoWait([]byte(long[i:min(i+9000, len(long))])); err != nil {
					return
				}
			}
		},
		"readfrom": func(c *Context) {
			f, err := os.Open(file)
			if err != nil {
				return
			}
			defer f.Close()
			_, _ = c.ReadFrom(f)
		},
	}
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.OnBodyFilter(func(status int, h stdhttp.Header) BodyFilter {
			h.Set("Content-Encoding", "gzip")
			return newGzipFilter()
		})
		write[strings.TrimPrefix(c.Request.URL.Path, "/")](c)
	})))
	for _, client := range hookClients(t) {
		client.Transport.(*stdhttp.Transport).DisableCompression = true
		for name := range write {
			t.Run(client.proto+"/"+name, func(t *testing.T) {
				resp, err := client.Get("http://" + addr + "/" + name)
				if err != nil {
					t.Fatal(err)
				}
				raw, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				if resp.Header.Get("Content-Encoding") != "gzip" {
					t.Fatalf("Content-Encoding %q", resp.Header.Get("Content-Encoding"))
				}
				if resp.ContentLength >= 0 && resp.ContentLength != int64(len(raw)) {
					t.Errorf("Content-Length %d for %d bytes", resp.ContentLength, len(raw))
				}
				zr, err := gzip.NewReader(bytes.NewReader(raw))
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(zr)
				want := long
				if name == "small" {
					want = "short"
				}
				if err != nil || string(body) != want {
					t.Errorf("%v: %d bytes, want %d", err, len(body), len(want))
				}
				if name != "small" && len(raw) >= len(long)/10 {
					t.Errorf("%d bytes on the wire for %d", len(raw), len(long))
				}
			})
		}
	}
}

// TestBodyFilterFlush checks that what a handler flushes reaches the client
// through the filter before the body ends, as server-sent events need.
func TestBodyFilterFlush(t *testing.T) {
	got := make(chan struct{})
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.OnBodyFilter(func(int, stdhttp.Header) BodyFilter { return newGzipFilter() })
		_, _ = io.WriteString(c, "event one\n")
		c.Flush()
		select {
		case <-got:
		case <-time.After(5 * time.Second):
		}
		_, _ = io.WriteString(c, "event two\n")
	})))
	for _, client := range hookClients(t) {
		client.Transport.(*stdhttp.Transport).DisableCompression = true
		resp, err := client.Get("http://" + addr + "/")
		if err != nil {
			t.Fatal(err)
		}
		zr, err := gzip.NewReader(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		r := bufio.NewReader(zr)
		start := time.Now()
		line, err := r.ReadString('\n')
		if err != nil || line != "event one\n" || time.Since(start) > 2*time.Second {
			t.Fatalf("%s: %q %v after %v", client.proto, line, err, time.Since(start))
		}
		got <- struct{}{}
		rest, _ := io.ReadAll(r)
		resp.Body.Close()
		if string(rest) != "event two\n" {
			t.Errorf("%s: then %q", client.proto, rest)
		}
	}
}

// TestBodyFilterChainAndSkips checks filters in a chain, a hook that
// returns none, HEAD, a status without a body, and a filter that fails.
func TestBodyFilterChainAndSkips(t *testing.T) {
	failed := make(chan error, 2)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		path := c.Request.URL.Path
		// Registered first, so run last: on what upper made.
		c.OnBodyFilter(func(_ int, h stdhttp.Header) BodyFilter {
			if path == "/none" {
				return nil
			}
			h.Set("Content-Encoding", "gzip")
			return newGzipFilter()
		})
		c.OnBodyFilter(func(int, stdhttp.Header) BodyFilter {
			if path == "/fail" {
				return failFilter{}
			}
			return &upperFilter{}
		})
		switch path {
		case "/204":
			c.WriteHeader(stdhttp.StatusNoContent)
		case "/whole":
			_ = c.Respond(200, "text/plain", []byte("whole body"))
		case "/fail":
			c.WriteHeader(200)
			_, err := io.WriteString(c, "x")
			failed <- err
		default:
			c.Header().Set("Content-Length", "9")
			_, _ = io.WriteString(c, "some body")
		}
	})))
	for _, client := range hookClients(t) {
		client.Transport.(*stdhttp.Transport).DisableCompression = true
		get := func(method, path string) (*stdhttp.Response, string) {
			req, _ := stdhttp.NewRequest(method, "http://"+addr+path, nil)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("%s %s %s: %v", client.proto, method, path, err)
			}
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.Header.Get("Content-Encoding") == "gzip" && len(raw) > 0 {
				zr, err := gzip.NewReader(bytes.NewReader(raw))
				if err != nil {
					t.Fatalf("%s %s: %v", client.proto, path, err)
				}
				raw, _ = io.ReadAll(zr)
			}
			return resp, string(raw)
		}
		if _, body := get("GET", "/chain"); body != "SOME BODY!" {
			t.Errorf("%s chain: %q", client.proto, body)
		}
		if _, body := get("GET", "/whole"); body != "WHOLE BODY!" {
			t.Errorf("%s whole: %q", client.proto, body)
		}
		if resp, body := get("GET", "/none"); body != "SOME BODY!" || resp.Header.Get("Content-Encoding") != "" {
			t.Errorf("%s none: %q %v", client.proto, body, resp.Header)
		}
		if resp, body := get("HEAD", "/chain"); body != "" || resp.ContentLength != 9 {
			t.Errorf("%s HEAD: %q, Content-Length %d", client.proto, body, resp.ContentLength)
		}
		if resp, _ := get("GET", "/204"); resp.StatusCode != 204 || resp.Header.Get("Content-Encoding") != "" {
			t.Errorf("%s 204: %d %v", client.proto, resp.StatusCode, resp.Header)
		}
		// The body cannot be carried on: the client gets an error or a
		// response cut short, never one that looks whole, and Write the
		// filter's error.
		req, _ := stdhttp.NewRequest("GET", "http://"+addr+"/fail", nil)
		if resp, err := client.Do(req); err == nil {
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err == nil {
				t.Errorf("%s fail: answered %d %q as if whole", client.proto, resp.StatusCode, body)
			}
		}
		select {
		case err := <-failed:
			if !errors.Is(err, errFilter) {
				t.Errorf("%s fail: Write reported %v", client.proto, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s fail: the handler did not write", client.proto)
		}
	}
}

// recordFilter passes a body through and reports how it ends: "close", or
// "abort" with the error, on ends.
type recordFilter struct {
	ends     chan string
	closeErr error
	ended    atomic.Int32
}

// end returns what, or that the filter was ended before.
func (f *recordFilter) end(what string) string {
	if f.ended.Add(1) > 1 {
		return what + " again"
	}
	return what
}

func (f *recordFilter) Filter(p []byte) ([]byte, error) { return p, nil }

func (f *recordFilter) Close() ([]byte, error) {
	f.ends <- f.end("close")
	return nil, f.closeErr
}

func (f *recordFilter) Abort(err error) {
	if err == nil {
		f.ends <- "abort without a reason"
		return
	}
	f.ends <- f.end("abort")
}

// TestBodyFilterAbort checks that a filter whose response ends without its
// body ending is aborted, once, and not closed: a response to HEAD, one
// whose client goes in the middle of it, one retained and cancelled, and
// the filters after one whose Close fails; and that one that ends is closed
// and not aborted.
func TestBodyFilterAbort(t *testing.T) {
	ends := make(chan string, 16)
	gone := make(chan struct{}, 1)
	chunk := []byte(strings.Repeat("x", 16<<10))
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		path := c.Request.URL.Path
		if path == "/chain" {
			// Registered first, so run last: aborted when the other's
			// Close fails.
			c.OnBodyFilter(func(int, stdhttp.Header) BodyFilter { return &recordFilter{ends: ends} })
			c.OnBodyFilter(func(int, stdhttp.Header) BodyFilter {
				return &recordFilter{ends: make(chan string, 1), closeErr: errFilter}
			})
		} else {
			c.OnBodyFilter(func(int, stdhttp.Header) BodyFilter { return &recordFilter{ends: ends} })
		}
		switch path {
		case "/whole", "/chain":
			_ = c.Respond(200, "text/plain", chunk)
		case "/streamed":
			for range 4 {
				_, _ = c.Write(chunk)
			}
		case "/gone":
			_, _ = c.Write(chunk)
			c.Flush()
			select {
			case <-gone:
			case <-time.After(5 * time.Second):
			}
			for i := 0; i < 1000; i++ {
				if _, err := c.Write(chunk); err != nil {
					return
				}
				c.Flush()
				time.Sleep(time.Millisecond)
			}
		case "/retained":
			c.Retain()
			c.OnCancel(func(error) { c.Release() })
			_, _ = c.Write(chunk)
			c.Flush()
		}
	})))
	want := func(t *testing.T, what string) {
		t.Helper()
		select {
		case got := <-ends:
			if got != what {
				t.Fatalf("%s, want %s", got, what)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("no %s", what)
		}
		select {
		case got := <-ends:
			t.Fatalf("then %s", got)
		case <-time.After(50 * time.Millisecond):
		}
	}
	for _, client := range hookClients(t) {
		t.Run(client.proto, func(t *testing.T) {
			for _, tt := range []struct{ method, path, end string }{
				{"GET", "/whole", "close"},
				{"GET", "/streamed", "close"},
				{"HEAD", "/whole", "abort"},
				{"HEAD", "/streamed", "abort"},
			} {
				req, _ := stdhttp.NewRequest(tt.method, "http://"+addr+tt.path, nil)
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				want(t, tt.end)
			}
			if resp, err := client.Get("http://" + addr + "/chain"); err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
			// The failed response's filters are aborted, and so are those
			// of the error response fib then makes, whose own Close fails.
			for got := range ends {
				if got != "abort" {
					t.Fatalf("/chain: %s", got)
				}
				if len(ends) == 0 {
					time.Sleep(50 * time.Millisecond)
					if len(ends) == 0 {
						break
					}
				}
			}
			for _, path := range []string{"/gone", "/retained"} {
				ctx, cancel := context.WithCancel(context.Background())
				req, _ := stdhttp.NewRequestWithContext(ctx, "GET", "http://"+addr+path, nil)
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := io.ReadFull(resp.Body, make([]byte, 1024)); err != nil {
					t.Fatal(err)
				}
				cancel()
				resp.Body.Close()
				gone <- struct{}{}
				want(t, "abort")
				select {
				case <-gone:
				default:
				}
			}
		})
	}
}

// TestBodyFilterLengthAfterFilter checks that a hook that runs after one
// that returned a filter is given no Content-Length, which would be the
// length of the body before that filter, whether the response is sent whole
// or streamed with a length declared.
func TestBodyFilterLengthAfterFilter(t *testing.T) {
	lengths := make(chan string, 4)
	body := strings.Repeat("x", 100<<10)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		// Registered first, so run last: after the upper-casing filter.
		c.OnBodyFilter(func(_ int, h stdhttp.Header) BodyFilter {
			lengths <- "after:" + h.Get("Content-Length")
			return nil
		})
		c.OnBodyFilter(func(_ int, h stdhttp.Header) BodyFilter {
			lengths <- "first:" + h.Get("Content-Length")
			return &upperFilter{}
		})
		if c.Request.URL.Path == "/whole" {
			_ = c.Respond(200, "text/plain", []byte(body))
			return
		}
		c.Header().Set("Content-Length", fmt.Sprint(len(body)))
		_, _ = io.WriteString(c, body)
	})))
	for _, client := range hookClients(t) {
		for _, path := range []string{"/whole", "/declared"} {
			resp, err := client.Get("http://" + addr + path)
			if err != nil {
				t.Fatal(err)
			}
			got, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if string(got) != strings.ToUpper(body)+"!" {
				t.Errorf("%s %s: %d bytes", client.proto, path, len(got))
			}
			first, after := <-lengths, <-lengths
			if first != "first:"+fmt.Sprint(len(body)) || after != "after:" {
				t.Errorf("%s %s: %s, then %s", client.proto, path, first, after)
			}
		}
	}
}

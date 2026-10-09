//go:build linux || darwin || windows

package http

import (
	"bytes"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newTestFileCache serves a directory holding a.css and its .br and .gz
// twins, and plain.txt with none.
func newTestFileCache(t *testing.T, watch bool) (*FileCache, string, string) {
	t.Helper()
	dir := t.TempDir()
	for name, body := range map[string]string{
		"a.css": "body{color:red}", "a.css.br": "BROTLI", "a.css.gz": "GZIP", "plain.txt": "plain",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	fc, err := NewFileCache(FileCacheConfig{Root: dir, Precompressed: true})
	if err != nil {
		t.Fatal(err)
	}
	if !watch {
		_ = fc.Close()
	}
	t.Cleanup(func() { _ = fc.Close() })
	addr := serveHTTP1(t, func(c *Context) {
		fc.ServeFile(c, strings.TrimPrefix(c.Request.URL.Path, "/static/"))
	})
	return fc, dir, addr
}

func getStatic(t *testing.T, addr, name string, header ...string) (*stdhttp.Response, string) {
	t.Helper()
	req, _ := stdhttp.NewRequest(stdhttp.MethodGet, "http://"+addr+"/static/"+name, nil)
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	transport := &stdhttp.Transport{DisableCompression: true}
	defer transport.CloseIdleConnections()
	resp, err := (&stdhttp.Client{Transport: transport, Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp, string(body)
}

func TestFileCacheServesFilesAndTwins(t *testing.T) {
	_, _, addr := newTestFileCache(t, true)
	for _, tc := range []struct {
		name, accept, body, encoding string
		status                       int
	}{
		{"a.css", "", "body{color:red}", "", 200},
		{"a.css", "gzip", "GZIP", "gzip", 200},
		{"a.css", "br;q=1, gzip;q=0.8", "BROTLI", "br", 200},
		{"a.css", "br;q=0.5, gzip;q=0.8", "GZIP", "gzip", 200},
		{"a.css", "br;q=0, gzip;q=0", "body{color:red}", "", 200},
		{"a.css", "*", "BROTLI", "br", 200},
		{"plain.txt", "br, gzip", "plain", "", 200},
		{"missing.css", "", "404 page not found\n", "", 404},
		{"../" + "a.css", "", "body{color:red}", "", 200},
		{"a.css.br", "", "404 page not found\n", "", 404},
	} {
		resp, body := getStatic(t, addr, tc.name, "Accept-Encoding", tc.accept)
		if resp.StatusCode != tc.status || body != tc.body || resp.Header.Get("Content-Encoding") != tc.encoding {
			t.Errorf("%s with %q: %d %q encoding %q; want %d %q %q", tc.name, tc.accept, resp.StatusCode, body,
				resp.Header.Get("Content-Encoding"), tc.status, tc.body, tc.encoding)
		}
		if tc.status == 200 {
			if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, map[bool]string{true: "text/css", false: "text/plain"}[strings.HasPrefix(tc.name, "a.css") || strings.HasSuffix(tc.name, "a.css")]) {
				t.Errorf("%s: Content-Type %q", tc.name, got)
			}
			if resp.ContentLength != int64(len(tc.body)) || resp.Header.Get("Last-Modified") == "" {
				t.Errorf("%s: Content-Length %d, Last-Modified %q", tc.name, resp.ContentLength, resp.Header.Get("Last-Modified"))
			}
			if tc.encoding != "" && resp.Header.Get("Vary") != "Accept-Encoding" {
				t.Errorf("%s: Vary %q", tc.name, resp.Header.Get("Vary"))
			}
		}
	}
	// Conditions and ranges go through ServeContent, over the same bytes.
	resp, body := getStatic(t, addr, "a.css", "Range", "bytes=0-3")
	if resp.StatusCode != 206 || body != "body" {
		t.Errorf("range: %d %q", resp.StatusCode, body)
	}
	resp, _ = getStatic(t, addr, "a.css", "If-Modified-Since", time.Now().Add(time.Hour).UTC().Format(stdhttp.TimeFormat))
	if resp.StatusCode != 304 {
		t.Errorf("If-Modified-Since: %d", resp.StatusCode)
	}
	req, _ := stdhttp.NewRequest(stdhttp.MethodHead, "http://"+addr+"/static/plain.txt", nil)
	head, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	head.Body.Close()
	if head.StatusCode != 200 || head.ContentLength != int64(len("plain")) {
		t.Errorf("HEAD: %d, Content-Length %d", head.StatusCode, head.ContentLength)
	}
}

// A file replaced on disk, with one of the same length as HttpArena's check
// replaces it, is served anew at once, its twins too, whether the cache is
// told by a watch or checks each file as it serves it; and so is a file that
// appears after it was asked for, or goes.
func TestFileCacheFollowsTheDisk(t *testing.T) {
	for _, watch := range []bool{true, false} {
		t.Run(fmt.Sprintf("watch=%v", watch), func(t *testing.T) {
			fc, dir, addr := newTestFileCache(t, watch)
			if watch && fc.watch == nil {
				t.Skip("no inotify here")
			}
			replace := func(name, body string) {
				tmp := filepath.Join(dir, ".tmp")
				if err := os.WriteFile(tmp, []byte(body), 0o644); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
					t.Fatal(err)
				}
			}
			expect := func(name, accept, want string) {
				t.Helper()
				deadline := time.Now().Add(2 * time.Second)
				for {
					_, body := getStatic(t, addr, name, "Accept-Encoding", accept)
					if body == want {
						return
					}
					if time.Now().After(deadline) {
						t.Fatalf("%s with %q: still %q, want %q", name, accept, body, want)
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
			expect("a.css", "", "body{color:red}")
			expect("a.css", "br", "BROTLI")
			replace("a.css", "body{color:blu}")
			replace("a.css.br", "BROTLX")
			expect("a.css", "", "body{color:blu}")
			expect("a.css", "br", "BROTLX")
			expect("new.txt", "", "404 page not found\n")
			replace("new.txt", "new")
			expect("new.txt", "", "new")
			if err := os.Remove(filepath.Join(dir, "plain.txt")); err != nil {
				t.Fatal(err)
			}
			expect("plain.txt", "", "404 page not found\n")
		})
	}
}

// A file served from memory, under a watch, leaves without allocating.
func TestFileCacheServesWithoutAllocating(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.css"), bytes.Repeat([]byte("x"), 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	fc, err := NewFileCache(FileCacheConfig{Root: dir, Precompressed: true})
	if err != nil {
		t.Fatal(err)
	}
	defer fc.Close()
	if fc.watch == nil {
		t.Skip("without a watch each request stats its file")
	}
	req, _ := stdhttp.NewRequest(stdhttp.MethodGet, "/a.css", nil)
	req.Header.Set("Accept-Encoding", "br;q=1, gzip;q=0.8")
	c := &Context{Request: req, external: discardStream{}}
	fc.ServeFile(c, "a.css")
	allocs := testing.AllocsPerRun(100, func() {
		*c = Context{Request: req, external: discardStream{}}
		fc.ServeFile(c, "a.css")
	})
	if allocs > 0 {
		t.Fatalf("a cached file took %v allocations to serve", allocs)
	}
}

// discardStream is a Stream that takes a response and keeps nothing of it.
type discardStream struct{}

func (discardStream) WriteResponse(*stdhttp.Request, Response) error            { return nil }
func (discardStream) WriteInterim(int, stdhttp.Header) error                    { return nil }
func (discardStream) Push(*stdhttp.Request, string, *stdhttp.PushOptions) error { return nil }

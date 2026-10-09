//go:build linux || darwin || windows

// Package statictest is the static-file conformance suite the HTTP/1, HTTP/2
// and HTTP/3 tests share: one set of files, one handler serving them four
// ways, and the checks a client of any version must see pass.
//
// The four routes are /cache/ (FileCache, which keeps small files in memory
// and serves the larger from disk), /serve/ (net/http's ServeFile, so
// ServeContent over a file), /fs/ (http.FileServer) and /copy/ (io.Copy from
// an *os.File, the plain sendfile path, with no length announced).
package statictest

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
)

// Sizes of the files Setup writes. Big is well past FileCache's default
// in-memory limit and past every flow-control and congestion window a fresh
// connection starts with.
const (
	MidSize = 300<<10 + 17
	BigSize = 9<<20 + 123
)

// Files maps file names to their contents.
type Files map[string][]byte

// Setup writes the test files into a temporary directory.
func Setup(t testing.TB) (string, Files) {
	t.Helper()
	dir := t.TempDir()
	random := func(size int) []byte {
		data := make([]byte, size)
		rand.New(rand.NewSource(int64(size))).Read(data)
		return data
	}
	files := Files{
		"small.txt": []byte("hello static\n"),
		"empty.txt": {},
		"mid.bin":   random(MidSize),
		"big.bin":   random(BigSize),
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir, files
}

// Handler serves dir on /cache/, /serve/, /fs/ and /copy/.
func Handler(t testing.TB, dir string) fibhttp.HandlerFunc {
	t.Helper()
	fc, err := fibhttp.NewFileCache(fibhttp.FileCacheConfig{Root: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fc.Close() })
	fileServer := stdhttp.FileServer(stdhttp.Dir(dir))
	return func(c *fibhttp.Context) {
		route, name, _ := strings.Cut(strings.TrimPrefix(c.Request.URL.Path, "/"), "/")
		switch route {
		case "cache":
			fc.ServeFile(c, name)
		case "serve":
			stdhttp.ServeFile(c, c.Request, filepath.Join(dir, filepath.Clean("/"+name)))
		case "fs":
			stdhttp.StripPrefix("/fs", fileServer).ServeHTTP(c, c.Request)
		case "copy":
			f, err := os.Open(filepath.Join(dir, filepath.Clean("/"+name)))
			if err != nil {
				c.WriteHeader(stdhttp.StatusNotFound)
				return
			}
			defer f.Close()
			c.Header().Set("Content-Type", "application/octet-stream")
			_, _ = io.Copy(c, f)
		default:
			c.WriteHeader(stdhttp.StatusNotFound)
		}
	}
}

// Fetch sends a request and returns the response with its body read to the
// end. A transport error is reported with t.Errorf (safe from any goroutine)
// and returns status 0.
type Fetch func(method, path string, header map[string]string) (status int, header2 stdhttp.Header, body []byte)

// Run checks the server Fetch talks to, which Handler serves.
func Run(t *testing.T, fetch Fetch, files Files) {
	t.Helper()
	get := func(path string, header ...string) (int, stdhttp.Header, []byte) {
		h := map[string]string{}
		for i := 0; i+1 < len(header); i += 2 {
			h[header[i]] = header[i+1]
		}
		return fetch(stdhttp.MethodGet, path, h)
	}

	for _, route := range []string{"cache", "serve", "fs", "copy"} {
		route := route
		t.Run(route, func(t *testing.T) {
			for name, data := range files {
				path := "/" + route + "/" + name
				status, header, body := get(path)
				if status != stdhttp.StatusOK || !bytes.Equal(body, data) {
					t.Fatalf("GET %s: status %d, %d bytes, want 200 and %d", path, status, len(body), len(data))
				}
				if route != "copy" && header.Get("Content-Length") != fmt.Sprint(len(data)) {
					t.Fatalf("GET %s: Content-Length %q, want %d", path, header.Get("Content-Length"), len(data))
				}
			}

			status, _, _ := get("/" + route + "/missing.bin")
			if status != stdhttp.StatusNotFound {
				t.Fatalf("missing file: status %d", status)
			}
			if route == "copy" {
				return
			}

			big, path := files["big.bin"], "/"+route+"/big.bin"
			status, header, body := fetch(stdhttp.MethodHead, path, nil)
			if status != stdhttp.StatusOK || len(body) != 0 || header.Get("Content-Length") != fmt.Sprint(len(big)) {
				t.Fatalf("HEAD: status %d, %d bytes, Content-Length %q", status, len(body), header.Get("Content-Length"))
			}

			status, header, body = get(path, "Range", "bytes=100-199")
			if status != stdhttp.StatusPartialContent || !bytes.Equal(body, big[100:200]) ||
				header.Get("Content-Range") != fmt.Sprintf("bytes 100-199/%d", len(big)) {
				t.Fatalf("range: status %d, %d bytes, Content-Range %q", status, len(body), header.Get("Content-Range"))
			}
			// A range that spans most of the file, and so most of the
			// send path's chunks.
			from, to := 1<<20+3, len(big)-(2<<20)
			status, _, body = get(path, "Range", fmt.Sprintf("bytes=%d-%d", from, to))
			if status != stdhttp.StatusPartialContent || !bytes.Equal(body, big[from:to+1]) {
				t.Fatalf("long range: status %d, %d bytes", status, len(body))
			}
			status, _, body = get(path, "Range", "bytes=-70000")
			if status != stdhttp.StatusPartialContent || !bytes.Equal(body, big[len(big)-70000:]) {
				t.Fatalf("suffix range: status %d, %d bytes", status, len(body))
			}
			status, _, body = get(path, "Range", fmt.Sprintf("bytes=%d-", len(big)-5))
			if status != stdhttp.StatusPartialContent || !bytes.Equal(body, big[len(big)-5:]) {
				t.Fatalf("open range: status %d, %d bytes", status, len(body))
			}
			status, header, body = get(path, "Range", "bytes=0-9,20-29")
			if status != stdhttp.StatusPartialContent || !strings.HasPrefix(header.Get("Content-Type"), "multipart/byteranges") ||
				!bytes.Contains(body, big[20:30]) {
				t.Fatalf("multipart range: status %d, Content-Type %q", status, header.Get("Content-Type"))
			}
			status, _, _ = get(path, "Range", fmt.Sprintf("bytes=%d-", len(big)+10))
			if status != stdhttp.StatusRequestedRangeNotSatisfiable {
				t.Fatalf("unsatisfiable range: status %d", status)
			}

			_, header, _ = fetch(stdhttp.MethodHead, path, nil)
			if lm := header.Get("Last-Modified"); lm == "" {
				t.Fatalf("no Last-Modified")
			} else if status, _, body = get(path, "If-Modified-Since", lm); status != stdhttp.StatusNotModified || len(body) != 0 {
				t.Fatalf("conditional: status %d, %d bytes", status, len(body))
			}
			if etag := header.Get("Etag"); etag != "" {
				if status, _, body = get(path, "If-None-Match", etag); status != stdhttp.StatusNotModified || len(body) != 0 {
					t.Fatalf("If-None-Match: status %d, %d bytes", status, len(body))
				}
			}
		})
	}

	// Several big downloads at once, on one connection when the protocol
	// multiplexes, and a ranged one among them.
	t.Run("concurrent", func(t *testing.T) {
		big := files["big.bin"]
		var wg sync.WaitGroup
		errs := make(chan string, 16)
		for i := 0; i < 8; i++ {
			route := []string{"cache", "serve", "fs", "copy"}[i%4]
			wg.Add(1)
			go func() {
				defer wg.Done()
				status, _, body := get("/" + route + "/big.bin")
				if status != stdhttp.StatusOK || !bytes.Equal(body, big) {
					errs <- fmt.Sprintf("%s: status %d, %d bytes", route, status, len(body))
				}
			}()
		}
		wg.Wait()
		close(errs)
		for e := range errs {
			t.Error(e)
		}
	})
}

//go:build linux || darwin || windows

package http

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

func uploadConfig() Config {
	config := DefaultConfig()
	config.StreamRequestBody = true
	config.StreamRequestBodyBuffer = 64 << 10
	return config
}

func uploadBytes(n int) []byte {
	b := make([]byte, n)
	rng := rand.New(rand.NewPCG(7, 11))
	for i := range b {
		b[i] = byte(rng.Uint32())
	}
	return b
}

// result is what a done callback was called with.
type result struct {
	saved Saved
	err   error
}

// collect returns a done callback that sends what it is called with on a
// channel, and answers the request the way a handler would.
func collect(c *Context, results chan<- result, answer bool) func(Saved, error) {
	return func(saved Saved, err error) {
		results <- result{saved, err}
		if answer {
			_ = c.RespondSaved(saved, err)
		}
	}
}

func waitResult(t *testing.T, results <-chan result) result {
	t.Helper()
	select {
	case r := <-results:
		return r
	case <-time.After(10 * time.Second):
		t.Fatal("done was not called")
		return result{}
	}
}

func noResult(t *testing.T, results <-chan result) {
	t.Helper()
	select {
	case r := <-results:
		t.Fatalf("done called again: %+v", r)
	case <-time.After(100 * time.Millisecond):
	}
}

// swapFS restores uploadFS when the test ends.
func swapFS(t *testing.T) {
	t.Helper()
	saved := uploadFS
	t.Cleanup(func() { uploadFS = saved })
}

// faultyFile fails the calls it is told to.
type faultyFile struct {
	uploadFile
	// failWriteAt fails the n-th Write and every one after (1 is the first).
	failWriteAt int
	writes      int
	failRead    bool
	failClose   bool
}

func (f *faultyFile) Write(p []byte) (int, error) {
	f.writes++
	if f.failWriteAt > 0 && f.writes >= f.failWriteAt {
		return 0, errBoom
	}
	return f.uploadFile.Write(p)
}

func (f *faultyFile) Read(p []byte) (int, error) {
	if f.failRead {
		return 0, errBoom
	}
	return f.uploadFile.Read(p)
}

func (f *faultyFile) Close() error {
	err := f.uploadFile.Close()
	if f.failClose {
		return errBoom
	}
	return err
}

// faultOpens makes the files opened with flag's access mode (os.O_RDONLY or
// os.O_WRONLY) faulty.
func faultOpens(t *testing.T, readOnly bool, fault func(*faultyFile)) {
	t.Helper()
	swapFS(t)
	real := uploadFS.open
	uploadFS.open = func(name string, flag int, perm os.FileMode) (uploadFile, error) {
		file, err := real(name, flag, perm)
		if err != nil {
			return nil, err
		}
		if (flag&os.O_WRONLY == 0) == readOnly {
			f := &faultyFile{uploadFile: file}
			fault(f)
			return f, nil
		}
		return file, nil
	}
}

func sendUpload(t *testing.T, conn *rawConn, method, target, extra string, body []byte) {
	t.Helper()
	conn.send(fmt.Sprintf("%s %s HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n%s\r\n", method, target, len(body), extra))
	if len(body) > 0 {
		if _, err := conn.Write(body); err != nil {
			t.Fatal(err)
		}
	}
}

func partialSize(path string) int64 {
	info, err := os.Stat(path + PartialSuffix)
	if err != nil {
		return -1
	}
	return info.Size()
}

func TestOffsetErrorText(t *testing.T) {
	if got := (&OffsetError{Have: 42}).Error(); !strings.Contains(got, "42") {
		t.Fatalf("Error() = %q", got)
	}
}

func TestSaveBodyStoresALargeBody(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "big.bin")
	results := make(chan result, 2)
	streamedAtHandler := make(chan bool, 1)
	addr := serveStreamingServer(t, uploadConfig(), func(c *Context, r *stdhttp.Request) {
		streamedAtHandler <- !c.BodyComplete()
		c.SaveBody(path, collect(c, results, true))
	})
	data := uploadBytes(8<<20 + 321)
	conn := dialRaw(t, addr)
	sendUpload(t, conn, "POST", "/u", "", data)

	got := waitResult(t, results)
	if got.err != nil || !got.saved.Complete || got.saved.Size != int64(len(data)) || got.saved.Total != got.saved.Size ||
		got.saved.Path != path || got.saved.SHA256 != sha256.Sum256(data) {
		t.Fatalf("saved = %+v, err %v", got.saved, got.err)
	}
	resp, body := conn.response("POST")
	if resp.StatusCode != 200 || body != fmt.Sprintf("stored bytes=%d sha256=%x\n", len(data), sha256.Sum256(data)) {
		t.Fatalf("response %d %q", resp.StatusCode, body)
	}
	if !<-streamedAtHandler {
		t.Fatal("the handler ran after the whole body had arrived")
	}
	if stored, err := os.ReadFile(path); err != nil || !bytes.Equal(stored, data) {
		t.Fatalf("stored file differs (err %v)", err)
	}
	if partialSize(path) != -1 {
		t.Fatal("the partial file is still there")
	}
	noResult(t, results)
}

// With streaming off the body has arrived whole before the handler runs, and
// SaveBody stores it from there.
func TestSaveBodyStoresABodyThatArrivedWhole(t *testing.T) {
	path := filepath.Join(t.TempDir(), "small.txt")
	addr := serveStreamingServer(t, DefaultConfig(), func(c *Context, r *stdhttp.Request) {
		c.SaveBody(path, nil)
	})
	conn := dialRaw(t, addr)
	sendUpload(t, conn, "PUT", "/u", "", []byte("hello"))
	resp, body := conn.response("PUT")
	if resp.StatusCode != 200 || body != fmt.Sprintf("stored bytes=5 sha256=%x\n", sha256.Sum256([]byte("hello"))) {
		t.Fatalf("response %d %q", resp.StatusCode, body)
	}
	if stored, _ := os.ReadFile(path); string(stored) != "hello" {
		t.Fatalf("stored %q", stored)
	}
}

func TestSaveBodyCannotCreateTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "dir", "f")
	results := make(chan result, 1)
	addr := serveStreamingServer(t, uploadConfig(), func(c *Context, r *stdhttp.Request) {
		c.SaveBody(path, collect(c, results, true))
	})
	conn := dialRaw(t, addr)
	sendUpload(t, conn, "POST", "/u", "", []byte("x"))
	if got := waitResult(t, results); got.err == nil || got.saved.Path != path {
		t.Fatalf("got %+v, err %v", got.saved, got.err)
	}
	if resp, _ := conn.response("POST"); resp.StatusCode != 500 {
		t.Fatalf("status %d, want 500", resp.StatusCode)
	}
}

// A connection that goes before the body ends leaves nothing behind.
func TestSaveBodyConnectionLostMidBody(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lost.bin")
	results := make(chan result, 2)
	addr := serveStreamingServer(t, uploadConfig(), func(c *Context, r *stdhttp.Request) {
		c.SaveBody(path, collect(c, results, true))
	})
	conn := dialRaw(t, addr)
	conn.send("POST /u HTTP/1.1\r\nHost: test\r\nContent-Length: 4194304\r\n\r\n")
	if _, err := conn.Write(uploadBytes(1 << 20)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	conn.Close()

	got := waitResult(t, results)
	if got.err == nil || got.saved.Complete {
		t.Fatalf("got %+v, err %v; want a failure", got.saved, got.err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("the file exists: %v", err)
	}
	if partialSize(path) != -1 {
		t.Fatal("the partial file was left behind")
	}
	noResult(t, results)
}

// A body whose length is announced past the limit is refused before the
// handler runs; one that only turns out to be too long, being chunked, is
// stopped as it grows, and SaveBody is told.
func TestSaveBodyPastTheLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "big.bin")
	config := uploadConfig()
	config.MaxStreamedBodyBytes = 256 << 10
	results := make(chan result, 2)
	addr := serveStreamingServer(t, config, func(c *Context, r *stdhttp.Request) {
		c.SaveBody(path, collect(c, results, true))
	})
	conn := dialRaw(t, addr)
	conn.send("POST /u HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n")
	go func() {
		piece := uploadBytes(64 << 10)
		for i := 0; i < 32; i++ {
			if _, err := fmt.Fprintf(conn, "%x\r\n%s\r\n", len(piece), piece); err != nil {
				return
			}
		}
	}()

	got := waitResult(t, results)
	if !errors.Is(got.err, ErrBodyTooLarge) {
		t.Fatalf("err = %v, want ErrBodyTooLarge", got.err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) || partialSize(path) != -1 {
		t.Fatal("a file was left behind")
	}
}

func TestSaveBodyWriteFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "w.bin")
	faultOpens(t, false, func(f *faultyFile) { f.failWriteAt = 2 })
	results := make(chan result, 2)
	addr := serveStreamingServer(t, uploadConfig(), func(c *Context, r *stdhttp.Request) {
		c.SaveBody(path, collect(c, results, false))
	})
	conn := dialRaw(t, addr)
	conn.send("POST /u HTTP/1.1\r\nHost: test\r\nContent-Length: 4194304\r\n\r\n")
	go func() { _, _ = conn.Write(uploadBytes(4 << 20)) }()
	if got := waitResult(t, results); !errors.Is(got.err, errBoom) {
		t.Fatalf("err = %v, want the write error", got.err)
	}
	// What is left of the body is ignored: done is not called again.
	noResult(t, results)
	if partialSize(path) != -1 {
		t.Fatal("the partial file was left behind")
	}
}

func TestSaveBodyCloseFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.bin")
	faultOpens(t, false, func(f *faultyFile) { f.failClose = true })
	results := make(chan result, 1)
	addr := serveStreamingServer(t, uploadConfig(), func(c *Context, r *stdhttp.Request) {
		c.SaveBody(path, collect(c, results, true))
	})
	conn := dialRaw(t, addr)
	sendUpload(t, conn, "POST", "/u", "", []byte("data"))
	if got := waitResult(t, results); !errors.Is(got.err, errBoom) {
		t.Fatalf("err = %v, want the close error", got.err)
	}
	if partialSize(path) != -1 {
		t.Fatal("the partial file was left behind")
	}
}

func TestSaveBodyRenameFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "r.bin")
	swapFS(t)
	uploadFS.rename = func(string, string) error { return errBoom }
	results := make(chan result, 1)
	addr := serveStreamingServer(t, uploadConfig(), func(c *Context, r *stdhttp.Request) {
		c.SaveBody(path, collect(c, results, true))
	})
	conn := dialRaw(t, addr)
	sendUpload(t, conn, "POST", "/u", "", []byte("data"))
	if got := waitResult(t, results); !errors.Is(got.err, errBoom) {
		t.Fatalf("err = %v, want the rename error", got.err)
	}
	if partialSize(path) != -1 {
		t.Fatal("the partial file was left behind")
	}
}

func TestRespondSaved(t *testing.T) {
	cases := []struct {
		path   string
		saved  Saved
		err    error
		status int
		offset string
		text   string
	}{
		{"/complete", Saved{Size: 3, Total: 3, Complete: true}, nil, 200, "", "stored bytes=3"},
		{"/partial", Saved{Size: 10, Total: 30}, nil, 202, "10", "received bytes=10 of 30"},
		{"/offset", Saved{}, &OffsetError{Have: 7}, 409, "7", "stored offset 7"},
		{"/range", Saved{}, ErrBadContentRange, 400, "", "Content-Range"},
		{"/large", Saved{}, ErrBodyTooLarge, 413, "", "too large"},
		{"/other", Saved{}, errBoom, 500, "", "boom"},
	}
	addr := serveStreamingServer(t, DefaultConfig(), func(c *Context, r *stdhttp.Request) {
		for _, tc := range cases {
			if r.URL.Path == tc.path {
				_ = c.RespondSaved(tc.saved, tc.err)
			}
		}
	})
	conn := dialRaw(t, addr)
	for _, tc := range cases {
		conn.send("GET " + tc.path + " HTTP/1.1\r\nHost: test\r\n\r\n")
		resp, body := conn.response("GET")
		if resp.StatusCode != tc.status || resp.Header.Get("Upload-Offset") != tc.offset || !strings.Contains(body, tc.text) {
			t.Errorf("%s: %d offset %q body %q", tc.path, resp.StatusCode, resp.Header.Get("Upload-Offset"), body)
		}
	}
}

func TestParseContentRange(t *testing.T) {
	cases := []struct {
		in                 string
		first, last, total int64
		probe, ok          bool
	}{
		{"bytes 0-99/1000", 0, 99, 1000, false, true},
		{"bytes 100-199/200", 100, 199, 200, false, true},
		{"bytes */1000", 0, 0, 1000, true, true},
		{"", 0, 0, 0, false, false},
		{"items 0-1/2", 0, 0, 0, false, false},
		{"bytes 0-1", 0, 0, 0, false, false},
		{"bytes 0-1/x", 0, 0, 0, false, false},
		{"bytes 0-1/0", 0, 0, 0, false, false},
		{"bytes */0", 0, 0, 0, false, false},
		{"bytes 5/10", 0, 0, 0, false, false},
		{"bytes x-5/10", 0, 0, 0, false, false},
		{"bytes -1-5/10", 0, 0, 0, false, false},
		{"bytes 0-y/10", 0, 0, 0, false, false},
		{"bytes 5-4/10", 0, 0, 0, false, false},
		{"bytes 0-10/10", 0, 0, 0, false, false},
	}
	for _, tc := range cases {
		first, last, total, probe, ok := parseContentRange(tc.in)
		if first != tc.first || last != tc.last || total != tc.total || probe != tc.probe || ok != tc.ok {
			t.Errorf("%q = %d %d %d %v %v", tc.in, first, last, total, probe, ok)
		}
	}
}

// resumableServer answers every request with SaveBodyResumable at path.
func resumableServer(t *testing.T, path string, results chan<- result) string {
	return serveStreamingServer(t, uploadConfig(), func(c *Context, r *stdhttp.Request) {
		c.SaveBodyResumable(path, collect(c, results, true))
	})
}

func chunkRange(start, end, total int) string {
	return fmt.Sprintf("Content-Range: bytes %d-%d/%d\r\n", start, end, total)
}

func TestSaveBodyResumableInChunks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "r.bin")
	results := make(chan result, 8)
	addr := resumableServer(t, path, results)
	data := uploadBytes(3<<20 + 5)
	conn := dialRaw(t, addr)

	probe := func() (int, string, string) {
		conn.send(fmt.Sprintf("PUT /r HTTP/1.1\r\nHost: test\r\nContent-Length: 0\r\nContent-Range: bytes */%d\r\n\r\n", len(data)))
		resp, body := conn.response("PUT")
		return resp.StatusCode, resp.Header.Get("Upload-Offset"), body
	}
	if status, offset, _ := probe(); status != 202 || offset != "0" {
		t.Fatalf("probe before: %d offset %q", status, offset)
	}
	waitResult(t, results)

	const piece = 1 << 20
	for start := 0; start < len(data); start += piece {
		end := min(start+piece, len(data)) - 1
		sendUpload(t, conn, "PATCH", "/r", chunkRange(start, end, len(data)), data[start:end+1])
		resp, body := conn.response("PATCH")
		got := waitResult(t, results)
		if got.err != nil {
			t.Fatalf("chunk at %d: %v", start, got.err)
		}
		if end+1 < len(data) {
			if resp.StatusCode != 202 || resp.Header.Get("Upload-Offset") != strconv.Itoa(end+1) || got.saved.Complete || got.saved.Size != int64(end+1) {
				t.Fatalf("chunk at %d: %d %q %+v", start, resp.StatusCode, body, got.saved)
			}
			if status, offset, _ := probe(); status != 202 || offset != strconv.Itoa(end+1) {
				t.Fatalf("probe after %d: %d offset %q", end+1, status, offset)
			}
			waitResult(t, results)
		} else if resp.StatusCode != 200 || !got.saved.Complete || got.saved.SHA256 != sha256.Sum256(data) ||
			got.saved.Size != int64(len(data)) || !strings.Contains(body, "stored bytes=") {
			t.Fatalf("last chunk: %d %q %+v", resp.StatusCode, body, got.saved)
		}
	}
	if stored, err := os.ReadFile(path); err != nil || !bytes.Equal(stored, data) {
		t.Fatalf("stored file differs (err %v)", err)
	}
	if partialSize(path) != -1 {
		t.Fatal("the partial file is still there")
	}
	// A probe of a finished upload says so, with the digest.
	if status, _, body := probe(); status != 200 || !strings.Contains(body, fmt.Sprintf("sha256=%x", sha256.Sum256(data))) {
		t.Fatalf("probe after: %d %q", status, body)
	}
	if got := waitResult(t, results); !got.saved.Complete {
		t.Fatalf("probe after: %+v", got.saved)
	}
}

func TestSaveBodyResumableWrongOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "o.bin")
	results := make(chan result, 8)
	addr := resumableServer(t, path, results)
	data := uploadBytes(1000)
	conn := dialRaw(t, addr)

	sendUpload(t, conn, "PUT", "/r", chunkRange(0, 399, 1000), data[:400])
	conn.response("PUT")
	waitResult(t, results)

	// Skipping ahead is refused, and says where to continue. The body is not
	// read, so the connection is not reused after it.
	conn2 := dialRaw(t, addr)
	sendUpload(t, conn2, "PUT", "/r", chunkRange(500, 999, 1000), data[500:])
	resp, _ := conn2.response("PUT")
	got := waitResult(t, results)
	var offset *OffsetError
	if resp.StatusCode != 409 || resp.Header.Get("Upload-Offset") != "400" || !errors.As(got.err, &offset) || offset.Have != 400 || got.saved.Size != 400 {
		t.Fatalf("status %d offset %q err %v saved %+v", resp.StatusCode, resp.Header.Get("Upload-Offset"), got.err, got.saved)
	}
	if partialSize(path) != 400 {
		t.Fatalf("partial size %d, want 400", partialSize(path))
	}

	// Starting at 0 begins the upload over.
	conn3 := dialRaw(t, addr)
	sendUpload(t, conn3, "PUT", "/r", chunkRange(0, 99, 1000), data[:100])
	if resp, _ := conn3.response("PUT"); resp.StatusCode != 202 || resp.Header.Get("Upload-Offset") != "100" {
		t.Fatalf("restart: %d offset %q", resp.StatusCode, resp.Header.Get("Upload-Offset"))
	}
	waitResult(t, results)
	if partialSize(path) != 100 {
		t.Fatalf("partial size %d, want 100", partialSize(path))
	}
}

func TestSaveBodyResumableBadRanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.bin")
	results := make(chan result, 32)
	addr := resumableServer(t, path, results)
	cases := []struct {
		name, header string
		body         int
	}{
		{"missing", "", 10},
		{"malformed", "Content-Range: bytes nonsense\r\n", 10},
		{"short body", chunkRange(0, 19, 100), 10},
		{"long body", chunkRange(0, 4, 100), 10},
		{"probe with a body", "Content-Range: bytes */100\r\n", 10},
	}
	for _, tc := range cases {
		conn := dialRaw(t, addr)
		sendUpload(t, conn, "PUT", "/r", tc.header, uploadBytes(tc.body))
		resp, body := conn.response("PUT")
		got := waitResult(t, results)
		if resp.StatusCode != 400 || !errors.Is(got.err, ErrBadContentRange) || !strings.Contains(body, "Content-Range") {
			t.Errorf("%s: %d %q err %v", tc.name, resp.StatusCode, body, got.err)
		}
	}
	if partialSize(path) != -1 {
		t.Fatal("a rejected request created the partial file")
	}
}

// A request cut short keeps what arrived, and the upload goes on from there.
func TestSaveBodyResumableContinuesAfterAnInterruptedChunk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "i.bin")
	results := make(chan result, 8)
	addr := resumableServer(t, path, results)
	data := uploadBytes(4 << 20)

	conn := dialRaw(t, addr)
	conn.send(fmt.Sprintf("PUT /r HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n%s\r\n", len(data), chunkRange(0, len(data)-1, len(data))))
	if _, err := conn.Write(data[:1<<20+17]); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	conn.Close()

	got := waitResult(t, results)
	if got.err == nil || got.saved.Complete || got.saved.Size <= 0 || got.saved.Size > int64(1<<20+17) {
		t.Fatalf("got %+v, err %v", got.saved, got.err)
	}
	kept := got.saved.Size
	if partialSize(path) != kept {
		t.Fatalf("partial size %d, want the %d bytes that arrived", partialSize(path), kept)
	}

	// Ask where to continue, then send the rest.
	conn2 := dialRaw(t, addr)
	conn2.send(fmt.Sprintf("PUT /r HTTP/1.1\r\nHost: test\r\nContent-Length: 0\r\nContent-Range: bytes */%d\r\n\r\n", len(data)))
	resp, _ := conn2.response("PUT")
	waitResult(t, results)
	if resp.Header.Get("Upload-Offset") != strconv.FormatInt(kept, 10) {
		t.Fatalf("offset %q, want %d", resp.Header.Get("Upload-Offset"), kept)
	}
	sendUpload(t, conn2, "PUT", "/r", chunkRange(int(kept), len(data)-1, len(data)), data[kept:])
	resp, _ = conn2.response("PUT")
	last := waitResult(t, results)
	if resp.StatusCode != 200 || !last.saved.Complete || last.saved.SHA256 != sha256.Sum256(data) {
		t.Fatalf("last: %d %+v err %v", resp.StatusCode, last.saved, last.err)
	}
	if stored, _ := os.ReadFile(path); !bytes.Equal(stored, data) {
		t.Fatal("the continued upload differs from the file")
	}
}

// A chunk is as long as its Content-Length says, so one past the limit is
// refused with its header, before SaveBodyResumable is reached.
func TestSaveBodyResumableChunkPastTheLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "l.bin")
	config := uploadConfig()
	config.MaxStreamedBodyBytes = 128 << 10
	results := make(chan result, 2)
	addr := serveStreamingServer(t, config, func(c *Context, r *stdhttp.Request) {
		c.SaveBodyResumable(path, collect(c, results, true))
	})
	conn := dialRaw(t, addr)
	conn.send("PUT /r HTTP/1.1\r\nHost: test\r\nContent-Length: 1048576\r\n" + chunkRange(0, 1<<20-1, 1<<20) + "\r\n")
	if resp, _ := conn.response("PUT"); resp.StatusCode != 413 {
		t.Fatalf("status %d, want 413", resp.StatusCode)
	}
	noResult(t, results)
	if partialSize(path) != -1 {
		t.Fatal("a refused chunk created the partial file")
	}
}

func TestSaveBodyResumableFileErrors(t *testing.T) {
	data := uploadBytes(600)
	t.Run("stat fails", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "e.bin")
		swapFS(t)
		uploadFS.size = func(string) (int64, error) { return 0, errBoom }
		results := make(chan result, 2)
		conn := dialRaw(t, resumableServer(t, path, results))
		sendUpload(t, conn, "PUT", "/r", chunkRange(0, 599, 600), data)
		if got := waitResult(t, results); !errors.Is(got.err, errBoom) || got.saved.Total != 600 {
			t.Fatalf("got %+v, err %v", got.saved, got.err)
		}
		if resp, _ := conn.response("PUT"); resp.StatusCode != 500 {
			t.Fatalf("status %d", resp.StatusCode)
		}
	})

	t.Run("open fails", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "no", "dir", "e.bin")
		results := make(chan result, 2)
		conn := dialRaw(t, resumableServer(t, path, results))
		sendUpload(t, conn, "PUT", "/r", chunkRange(0, 599, 600), data)
		if got := waitResult(t, results); got.err == nil || got.saved.Size != 0 {
			t.Fatalf("got %+v, err %v", got.saved, got.err)
		}
	})

	t.Run("write fails keeps the part", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "e.bin")
		faultOpens(t, false, func(f *faultyFile) { f.failWriteAt = 2 })
		results := make(chan result, 2)
		conn := dialRaw(t, resumableServer(t, path, results))
		big := uploadBytes(1 << 20)
		conn.send(fmt.Sprintf("PUT /r HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n%s\r\n", len(big), chunkRange(0, len(big)-1, len(big))))
		go func() { _, _ = conn.Write(big) }()
		got := waitResult(t, results)
		if !errors.Is(got.err, errBoom) {
			t.Fatalf("err = %v", got.err)
		}
		if size := partialSize(path); size < 0 {
			t.Fatal("the part of the upload that was stored was removed")
		}
		noResult(t, results)
	})

	t.Run("close fails", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "e.bin")
		faultOpens(t, false, func(f *faultyFile) { f.failClose = true })
		results := make(chan result, 2)
		conn := dialRaw(t, resumableServer(t, path, results))
		sendUpload(t, conn, "PUT", "/r", chunkRange(0, 599, 600), data)
		if got := waitResult(t, results); !errors.Is(got.err, errBoom) {
			t.Fatalf("err = %v", got.err)
		}
	})

	t.Run("rename fails", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "e.bin")
		swapFS(t)
		uploadFS.rename = func(string, string) error { return errBoom }
		results := make(chan result, 2)
		conn := dialRaw(t, resumableServer(t, path, results))
		sendUpload(t, conn, "PUT", "/r", chunkRange(0, 599, 600), data)
		if got := waitResult(t, results); !errors.Is(got.err, errBoom) {
			t.Fatalf("err = %v", got.err)
		}
		if partialSize(path) != 600 {
			t.Fatalf("partial size %d: a resumable upload keeps its part", partialSize(path))
		}
	})

	t.Run("reading the digest back fails", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "e.bin")
		faultOpens(t, true, func(f *faultyFile) { f.failRead = true })
		results := make(chan result, 2)
		conn := dialRaw(t, resumableServer(t, path, results))
		sendUpload(t, conn, "PUT", "/r", chunkRange(0, 599, 600), data)
		got := waitResult(t, results)
		if !errors.Is(got.err, errBoom) || !got.saved.Complete {
			t.Fatalf("got %+v, err %v", got.saved, got.err)
		}
	})

	t.Run("a probe finds a finished file but cannot read it", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "e.bin")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		faultOpens(t, true, func(f *faultyFile) { f.failRead = true })
		results := make(chan result, 2)
		conn := dialRaw(t, resumableServer(t, path, results))
		sendUpload(t, conn, "PUT", "/r", "Content-Range: bytes */600\r\n", nil)
		if got := waitResult(t, results); !errors.Is(got.err, errBoom) {
			t.Fatalf("err = %v", got.err)
		}
	})

	t.Run("a probe finds a file of another size", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "e.bin")
		if err := os.WriteFile(path, data[:100], 0o644); err != nil {
			t.Fatal(err)
		}
		results := make(chan result, 2)
		conn := dialRaw(t, resumableServer(t, path, results))
		sendUpload(t, conn, "PUT", "/r", "Content-Range: bytes */600\r\n", nil)
		if got := waitResult(t, results); got.err != nil || got.saved.Complete || got.saved.Size != 0 {
			t.Fatalf("got %+v, err %v", got.saved, got.err)
		}
	})
}

func TestFileSHA256(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(path, []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	if sum, err := fileSHA256(path); err != nil || sum != sha256.Sum256([]byte("abc")) {
		t.Fatalf("sum %x err %v", sum, err)
	}
	if _, err := fileSHA256(path + ".missing"); err == nil {
		t.Fatal("hashed a file that is not there")
	}
	faultOpens(t, true, func(f *faultyFile) { f.failClose = true })
	if _, err := fileSHA256(path); !errors.Is(err, errBoom) {
		t.Fatalf("close error = %v", err)
	}
}

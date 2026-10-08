//go:build linux || darwin || windows

package service

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func TestMain(m *testing.M) {
	log.SetOutput(io.Discard)
	os.Exit(m.Run())
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rng := rand.New(rand.NewPCG(3, 5))
	for i := range b {
		b[i] = byte(rng.Uint32())
	}
	return b
}

// startServer runs the server on a free port, storing into a fresh directory.
func startServer(t *testing.T, maxSize int64) (base, dir string) {
	t.Helper()
	dir = t.TempDir()
	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:0"
	engine, err := fib.Bind(config, fibhttp.NewHandlerWithConfig(Config(maxSize), Handler(dir)))
	if err != nil {
		t.Fatal(err)
	}
	addr, err := engine.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		<-done
		_ = engine.Close()
	})
	return "http://" + addr.String(), dir
}

func send(t *testing.T, method, url string, header http.Header, body io.Reader, length int64) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = length
	req.Header = header
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	text, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, string(text)
}

// rawBody opens a connection, sends a request header announcing length body
// bytes and the first sent of them, and returns the connection, which the
// caller closes to cut the body short.
func rawBody(t *testing.T, base, method, target, extra string, length int, sent []byte) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(base, "http://"), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if _, err := fmt.Fprintf(conn, "%s %s HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n%s\r\n", method, target, length, extra); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(sent); err != nil {
		t.Fatal(err)
	}
	return conn
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func dirEntries(dir string) []string {
	entries, _ := os.ReadDir(dir)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// settledSize waits for the size of path to stop changing and returns it, or
// -1 if there is no such file.
func settledSize(path string) int64 {
	last := int64(-2)
	for stable := 0; stable < 4; {
		size := int64(-1)
		if info, err := os.Stat(path); err == nil {
			size = info.Size()
		}
		if size == last {
			stable++
		} else {
			stable, last = 0, size
		}
		time.Sleep(25 * time.Millisecond)
	}
	return last
}

func TestUploadStoresALargeBody(t *testing.T) {
	base, dir := startServer(t, 0)
	data := randomBytes(32<<20 + 99)
	for _, method := range []string{http.MethodPost, http.MethodPut} {
		name := strings.ToLower(method) + ".bin"
		resp, text := send(t, method, base+"/upload?name="+name, nil, bytes.NewReader(data), int64(len(data)))
		want := fmt.Sprintf("stored bytes=%d sha256=%x\n", len(data), sha256.Sum256(data))
		if resp.StatusCode != 200 || text != want {
			t.Fatalf("%s: %d %q", method, resp.StatusCode, text)
		}
		if stored, err := os.ReadFile(filepath.Join(dir, name)); err != nil || !bytes.Equal(stored, data) {
			t.Fatalf("%s: the stored file differs (err %v)", method, err)
		}
	}
	if names := dirEntries(dir); len(names) != 2 {
		t.Fatalf("directory holds %v", names)
	}
}

// The handler runs, and the body reaches the disk, while the client still
// holds the rest of it back.
func TestUploadStreamsBeforeTheBodyIsComplete(t *testing.T) {
	base, dir := startServer(t, 0)
	data := randomBytes(4 << 20)
	conn := rawBody(t, base, "POST", "/upload?name=slow.bin", "", len(data), data[:len(data)/2])
	eventually(t, "half the body on disk", func() bool {
		info, err := os.Stat(filepath.Join(dir, "slow.bin.part"))
		return err == nil && info.Size() > 0
	})
	if _, err := conn.Write(data[len(data)/2:]); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the upload to finish", func() bool {
		stored, err := os.ReadFile(filepath.Join(dir, "slow.bin"))
		return err == nil && bytes.Equal(stored, data)
	})
}

func TestUploadConnectionLostMidBody(t *testing.T) {
	base, dir := startServer(t, 0)
	data := randomBytes(8 << 20)
	conn := rawBody(t, base, "POST", "/upload?name=lost.bin", "", len(data), data[:2<<20])
	eventually(t, "the body to start arriving", func() bool { return len(dirEntries(dir)) == 1 })
	conn.Close()
	// Nothing of a failed upload is left, and the server goes on serving.
	eventually(t, "the partial file to be removed", func() bool { return len(dirEntries(dir)) == 0 })
	resp, _ := send(t, "POST", base+"/upload?name=after.bin", nil, strings.NewReader("ok"), 2)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d after the failed upload", resp.StatusCode)
	}
}

func TestUploadPastTheLimit(t *testing.T) {
	base, dir := startServer(t, 256<<10)
	// Announced past the limit: refused before the handler runs.
	data := randomBytes(1 << 20)
	req, _ := http.NewRequest("POST", base+"/upload?name=big.bin", bytes.NewReader(data))
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode != 413 {
			t.Fatalf("status %d, want 413", resp.StatusCode)
		}
	}
	// Chunked, so it is only found out as it grows: the handler is told.
	conn2, err := net.Dial("tcp", strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	fmt.Fprint(conn2, "POST /upload?name=chunked.bin HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n")
	piece := randomBytes(64 << 10)
	for i := 0; i < 16; i++ {
		if _, err := fmt.Fprintf(conn2, "%x\r\n%s\r\n", len(piece), piece); err != nil {
			break
		}
	}
	// Neither was kept, nor a partial file of either.
	eventually(t, "the oversized uploads to leave nothing", func() bool { return len(dirEntries(dir)) == 0 })
}

func TestResumeInChunksAcrossInterruptions(t *testing.T) {
	base, dir := startServer(t, 0)
	data := randomBytes(24<<20 + 7)
	total := len(data)
	path := filepath.Join(dir, "r.bin")
	rangeOf := func(from, to int) http.Header {
		return http.Header{"Content-Range": {fmt.Sprintf("bytes %d-%d/%d", from, to, total)}}
	}
	probe := func() (int, string, string) {
		resp, text := send(t, "PUT", base+"/resume?name=r.bin", http.Header{"Content-Range": {fmt.Sprintf("bytes */%d", total)}}, nil, 0)
		return resp.StatusCode, resp.Header.Get("Upload-Offset"), text
	}
	if status, offset, _ := probe(); status != 202 || offset != "0" {
		t.Fatalf("probe: %d offset %q", status, offset)
	}

	// The first chunk arrives whole.
	const chunk = 8 << 20
	resp, _ := send(t, "PUT", base+"/resume?name=r.bin", rangeOf(0, chunk-1), bytes.NewReader(data[:chunk]), chunk)
	if resp.StatusCode != 202 || resp.Header.Get("Upload-Offset") != strconv.Itoa(chunk) {
		t.Fatalf("first chunk: %d offset %q", resp.StatusCode, resp.Header.Get("Upload-Offset"))
	}

	// The second is cut off partway through.
	conn := rawBody(t, base, "PATCH", "/resume?name=r.bin", fmt.Sprintf("Content-Range: bytes %d-%d/%d\r\n", chunk, 2*chunk-1, total), chunk, data[chunk:chunk+3<<20])
	eventually(t, "part of the second chunk on disk", func() bool { return settledSize(path+".part") > chunk })
	conn.Close()
	kept := settledSize(path + ".part")
	if kept <= chunk || kept >= 2*chunk {
		t.Fatalf("the server kept %d bytes, expected part of the second chunk", kept)
	}
	if status, offset, _ := probe(); status != 202 || offset != strconv.FormatInt(kept, 10) {
		t.Fatalf("probe after the interruption: %d offset %q, want %d", status, offset, kept)
	}

	// A chunk in the wrong place is refused with the place it belongs.
	resp, _ = send(t, "PUT", base+"/resume?name=r.bin", rangeOf(total-100, total-1), bytes.NewReader(data[total-100:]), 100)
	if resp.StatusCode != 409 || resp.Header.Get("Upload-Offset") != strconv.FormatInt(kept, 10) {
		t.Fatalf("misplaced chunk: %d offset %q", resp.StatusCode, resp.Header.Get("Upload-Offset"))
	}

	// The rest goes in one request from where the server has it.
	resp, text := send(t, "PUT", base+"/resume?name=r.bin", rangeOf(int(kept), total-1), bytes.NewReader(data[kept:]), int64(total)-kept)
	if resp.StatusCode != 200 || text != fmt.Sprintf("stored bytes=%d sha256=%x\n", total, sha256.Sum256(data)) {
		t.Fatalf("last chunk: %d %q", resp.StatusCode, text)
	}
	if stored, err := os.ReadFile(path); err != nil || !bytes.Equal(stored, data) {
		t.Fatalf("the stored file differs (err %v)", err)
	}
	if status, _, text := probe(); status != 200 || !strings.Contains(text, "stored") {
		t.Fatalf("probe of the finished upload: %d %q", status, text)
	}
	if names := dirEntries(dir); len(names) != 1 {
		t.Fatalf("directory holds %v", names)
	}
}

func TestEchoesALargeBody(t *testing.T) {
	base, _ := startServer(t, 0)
	data := randomBytes(48<<20 + 13)
	resp, text := send(t, "POST", base+"/echo", nil, bytes.NewReader(data), int64(len(data)))
	if resp.StatusCode != 200 || len(text) != len(data) || sha256.Sum256([]byte(text)) != sha256.Sum256(data) {
		t.Fatalf("echo: %d, %d bytes back of %d", resp.StatusCode, len(text), len(data))
	}
	// An empty body is answered with an empty one.
	resp, text = send(t, "POST", base+"/echo", nil, nil, 0)
	if resp.StatusCode != 200 || text != "" {
		t.Fatalf("empty echo: %d %q", resp.StatusCode, text)
	}
}

func TestEchoConnectionLostMidBody(t *testing.T) {
	base, _ := startServer(t, 0)
	data := randomBytes(16 << 20)
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(base, "http://"), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "POST /echo HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", len(data))
	// The body goes out from another goroutine: the server stops reading it
	// while this end is not reading the echo, so one goroutine doing both
	// would wait on itself.
	go func() { _, _ = conn.Write(data[:3<<20]) }()
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(conn, make([]byte, 1<<10)); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	resp, text := send(t, "POST", base+"/echo", nil, strings.NewReader("still serving"), 13)
	if resp.StatusCode != 200 || text != "still serving" {
		t.Fatalf("after the lost connection: %d %q", resp.StatusCode, text)
	}
}

func TestEchoPastTheLimit(t *testing.T) {
	base, _ := startServer(t, 128<<10)
	conn, err := net.Dial("tcp", strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprint(conn, "POST /echo HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n")
	piece := randomBytes(32 << 10)
	for i := 0; i < 32; i++ {
		if _, err := fmt.Fprintf(conn, "%x\r\n%s\r\n", len(piece), piece); err != nil {
			break
		}
	}
	// The connection is closed on a body that outgrows the limit, and the
	// server goes on.
	resp, text := send(t, "POST", base+"/echo", nil, strings.NewReader("fine"), 4)
	if resp.StatusCode != 200 || text != "fine" {
		t.Fatalf("after the oversized body: %d %q", resp.StatusCode, text)
	}
}

func TestRoutingAndNames(t *testing.T) {
	base, dir := startServer(t, 0)
	cases := []struct {
		method, target string
		status         int
	}{
		{"GET", "/upload?name=a", 405},
		{"GET", "/resume?name=a", 405},
		{"GET", "/echo", 405},
		{"GET", "/nothing", 404},
		{"POST", "/upload", 400},
		{"POST", "/upload?name=.", 400},
		{"POST", "/upload?name=%2F", 400},
		{"PUT", "/resume", 400},
	}
	for _, tc := range cases {
		resp, _ := send(t, tc.method, base+tc.target, nil, strings.NewReader("x"), 1)
		if resp.StatusCode != tc.status {
			t.Errorf("%s %s: status %d, want %d", tc.method, tc.target, resp.StatusCode, tc.status)
		}
	}
	// A name that climbs out of the directory is cut down to its last element.
	if resp, _ := send(t, "POST", base+"/upload?name=../../escape.txt", nil, strings.NewReader("x"), 1); resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if names := dirEntries(dir); len(names) != 1 || names[0] != "escape.txt" {
		t.Fatalf("directory holds %v", names)
	}
}

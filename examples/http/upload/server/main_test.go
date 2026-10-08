//go:build linux || darwin || windows

package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// startServer runs the upload server on a free port, storing into a fresh
// directory, and returns its URL and that directory.
func startServer(t *testing.T, maxSize int64) (base, dir string) {
	t.Helper()
	dir = t.TempDir()
	engine, err := newEngine("127.0.0.1:0", dir, maxSize)
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

func randomBytes(n int) []byte {
	var key [32]byte
	b := make([]byte, n)
	_, _ = io.ReadFull(rand.NewChaCha8(key), b)
	return b
}

func do(t *testing.T, method, url string, body io.Reader, length int64) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = length
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reply, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(reply)
}

func TestUploadStoresWhatWasSent(t *testing.T) {
	base, dir := startServer(t, 0)
	data := randomBytes(8<<20 + 123)
	for _, method := range []string{http.MethodPost, http.MethodPut} {
		name := strings.ToLower(method) + ".bin"
		status, reply := do(t, method, base+"/upload?name="+name, bytes.NewReader(data), int64(len(data)))
		if status != http.StatusOK {
			t.Fatalf("%s: status %d: %s", method, status, reply)
		}
		want := fmt.Sprintf("stored %s bytes=%d sha256=%x\n", name, len(data), sha256.Sum256(data))
		if reply != want {
			t.Fatalf("%s: reply %q, want %q", method, reply, want)
		}
		got, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("%s: stored file differs from what was sent (err %v)", method, err)
		}
	}
	if parts, _ := filepath.Glob(filepath.Join(dir, "*.part")); len(parts) != 0 {
		t.Fatalf("partial files left behind: %v", parts)
	}
}

// The handler must run, and the body must be written out, while the client
// still holds the rest of it back: a server that read the whole body first
// would show nothing on disk until the last byte was sent.
func TestUploadStreamsBeforeBodyIsComplete(t *testing.T) {
	base, dir := startServer(t, 0)
	data := randomBytes(4 << 20)
	half := len(data) / 2

	pr, pw := io.Pipe()
	type result struct {
		status int
		reply  string
	}
	done := make(chan result, 1)
	go func() {
		status, reply := do(t, http.MethodPost, base+"/upload?name=slow.bin", pr, int64(len(data)))
		done <- result{status, reply}
	}()

	if _, err := pw.Write(data[:half]); err != nil {
		t.Fatal(err)
	}
	partial := filepath.Join(dir, "slow.bin.part")
	deadline := time.Now().Add(10 * time.Second)
	for {
		if info, err := os.Stat(partial); err == nil && info.Size() > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("nothing was written while the client still held half the body back")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case r := <-done:
		t.Fatalf("answered before the body was complete: %d %s", r.status, r.reply)
	default:
	}

	if _, err := pw.Write(data[half:]); err != nil {
		t.Fatal(err)
	}
	_ = pw.Close()
	r := <-done
	if r.status != http.StatusOK || !strings.Contains(r.reply, fmt.Sprintf("sha256=%x", sha256.Sum256(data))) {
		t.Fatalf("got %d %q", r.status, r.reply)
	}
	if _, err := os.Stat(partial); !os.IsNotExist(err) {
		t.Fatalf("partial file still there after the upload: %v", err)
	}
}

func TestUploadOverTheLimitLeavesNothing(t *testing.T) {
	base, dir := startServer(t, 1<<20)
	data := randomBytes(4 << 20)
	req, err := http.NewRequest(http.MethodPost, base+"/upload?name=big.bin", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	if resp, err := client.Do(req); err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatal("an upload past the limit was accepted")
		}
	}
	// The server removes the partial file once it has given up on the body.
	deadline := time.Now().Add(5 * time.Second)
	for {
		entries, _ := os.ReadDir(dir)
		if len(entries) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("files left behind: %v", entries)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestUploadRejectsBadRequests(t *testing.T) {
	base, dir := startServer(t, 0)
	body := []byte("hello")

	if status, _ := do(t, http.MethodGet, base+"/upload?name=a", nil, 0); status != http.StatusMethodNotAllowed {
		t.Errorf("GET: status %d, want 405", status)
	}
	if status, _ := do(t, http.MethodPost, base+"/other?name=a", bytes.NewReader(body), int64(len(body))); status != http.StatusNotFound {
		t.Errorf("other path: status %d, want 404", status)
	}
	if status, _ := do(t, http.MethodPost, base+"/upload", bytes.NewReader(body), int64(len(body))); status != http.StatusBadRequest {
		t.Errorf("no name: status %d, want 400", status)
	}

	// A name that climbs out of the directory is cut down to its last element.
	status, reply := do(t, http.MethodPost, base+"/upload?name=../../escape.txt", bytes.NewReader(body), int64(len(body)))
	if status != http.StatusOK {
		t.Fatalf("traversal name: status %d: %s", status, reply)
	}
	if got, err := os.ReadFile(filepath.Join(dir, "escape.txt")); err != nil || string(got) != "hello" {
		t.Fatalf("file not stored inside the directory: %q, %v", got, err)
	}
}

//go:build linux || darwin || windows

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	log.SetOutput(io.Discard)
	os.Exit(m.Run())
}

// startServer runs the server on a free port, storing into a fresh directory.
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

func send(t *testing.T, method, url string, body io.Reader, length int64) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = length
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

// The endpoints themselves are tested in package service; this checks that
// the server serves them.
func TestServerServesTheEndpoints(t *testing.T) {
	base, dir := startServer(t, 0)
	data := bytes.Repeat([]byte("upload "), 1<<17)
	resp, text := send(t, "POST", base+"/upload?name=a.bin", bytes.NewReader(data), int64(len(data)))
	if resp.StatusCode != 200 || text != fmt.Sprintf("stored bytes=%d sha256=%x\n", len(data), sha256.Sum256(data)) {
		t.Fatalf("upload: %d %q", resp.StatusCode, text)
	}
	if stored, _ := os.ReadFile(filepath.Join(dir, "a.bin")); !bytes.Equal(stored, data) {
		t.Fatal("stored file differs")
	}
}

func TestNewEngineErrors(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := newEngine("127.0.0.1:0", filepath.Join(file, "sub"), 0); err == nil {
		t.Error("created a directory inside a file")
	}
	if _, err := newEngine("not an address", t.TempDir(), 0); err == nil {
		t.Error("bound a bad address")
	}
}

func TestRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var out bytes.Buffer
	addrs := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"-addr", "127.0.0.1:0", "-dir", t.TempDir()}, &out, func(addr string) { addrs <- addr })
	}()
	addr := <-addrs
	if resp, _ := send(t, "POST", "http://"+addr+"/echo", strings.NewReader("hi"), 2); resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "upload server listening on http://"+addr) {
		t.Fatalf("output %q", out.String())
	}

	// Without a ready callback, and with the errors a bad start gives.
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if err := run(ctx2, []string{"-addr", "127.0.0.1:0", "-dir", t.TempDir()}, io.Discard, nil); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, []string{"-nope"}, io.Discard, nil); err == nil {
		t.Error("accepted an unknown flag")
	}
	if err := run(ctx, []string{"-addr", "not an address", "-dir", t.TempDir()}, io.Discard, nil); err == nil {
		t.Error("started on a bad address")
	}
}

func TestMainReportsAStartupError(t *testing.T) {
	var mu sync.Mutex
	var got error
	oldFatal, oldArgs := fatal, os.Args
	defer func() { fatal, os.Args = oldFatal, oldArgs }()
	fatal = func(err error) { mu.Lock(); got = err; mu.Unlock() }
	os.Args = []string{"server", "-nope"}
	main()
	mu.Lock()
	defer mu.Unlock()
	if got == nil {
		t.Fatal("fatal was not called")
	}
}

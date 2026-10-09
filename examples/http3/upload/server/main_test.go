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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/http/upload/uploadtest"
	"github.com/lesismal/fib/http3"
)

func TestMain(m *testing.M) {
	log.SetOutput(io.Discard)
	os.Exit(m.Run())
}

// startServer runs the server on a free UDP port, storing into a fresh
// directory, and returns its URL, the directory, and a function that makes an
// HTTP/3 client for it.
func startServer(t *testing.T, maxSize int64) (base, dir string, newClient func() *http3.Client) {
	t.Helper()
	serverTLS, clientTLS, _ := uploadtest.TLS(t)
	dir = t.TempDir()
	engine, err := newEngine("127.0.0.1:0", dir, maxSize, serverTLS)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := engine.LocalUDPAddr()
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
	clientEngine, err := fib.NewEngine(fib.DefaultConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	clientDone := make(chan error, 1)
	go func() { clientDone <- clientEngine.Run() }()
	t.Cleanup(func() {
		clientEngine.Stop()
		<-clientDone
		_ = clientEngine.Close()
	})
	newClient = func() *http3.Client {
		config := http3.DefaultClientConfig()
		config.TLSConfig = clientTLS
		config.Timeout = 2 * time.Minute
		config.MaxResponseBodyBytes = 1 << 30
		return http3.NewClient(clientEngine, config)
	}
	return "https://" + addr.String(), dir, newClient
}

func send(t *testing.T, client *http3.Client, method, url string, header http.Header, body []byte) (*http.Response, string) {
	t.Helper()
	resp, text, err := trySend(client, method, url, header, body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, text
}

func trySend(client *http3.Client, method, url string, header http.Header, body []byte) (*http.Response, string, error) {
	return trySendContext(context.Background(), client, method, url, header, body)
}

func trySendContext(ctx context.Context, client *http3.Client, method, url string, header http.Header, body []byte) (*http.Response, string, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return nil, "", err
	}
	req.ContentLength = int64(len(body))
	req.Header = header
	resp, err := client.Go(req).Wait()
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	text, err := io.ReadAll(resp.Body)
	return resp, string(text), err
}

func TestUploadResumeAndEchoOverHTTP3(t *testing.T) {
	base, dir, newClient := startServer(t, 0)
	client := newClient()
	defer client.Close()
	data := uploadtest.Bytes(24<<20+31, 1)
	want := fmt.Sprintf("stored bytes=%d sha256=%x\n", len(data), sha256.Sum256(data))

	resp, text := send(t, client, "POST", base+"/upload?name=up.bin", nil, data)
	if resp.ProtoMajor != 3 || resp.StatusCode != 200 || text != want {
		t.Fatalf("upload: %s %d %q", resp.Proto, resp.StatusCode, text)
	}
	if stored, _ := os.ReadFile(filepath.Join(dir, "up.bin")); !bytes.Equal(stored, data) {
		t.Fatal("uploaded file differs")
	}

	const chunk = 6 << 20
	var last string
	for start := 0; start < len(data); start += chunk {
		end := min(start+chunk, len(data))
		header := http.Header{"Content-Range": {fmt.Sprintf("bytes %d-%d/%d", start, end-1, len(data))}}
		resp, last = send(t, client, "PUT", base+"/resume?name=re.bin", header, data[start:end])
		if resp.ProtoMajor != 3 {
			t.Fatalf("chunk at %d over %s", start, resp.Proto)
		}
	}
	if resp.StatusCode != 200 || last != want {
		t.Fatalf("last chunk: %d %q", resp.StatusCode, last)
	}

	resp, text = send(t, client, "POST", base+"/echo", nil, data)
	if resp.ProtoMajor != 3 || resp.StatusCode != 200 || len(text) != len(data) || sha256.Sum256([]byte(text)) != sha256.Sum256(data) {
		t.Fatalf("echo: %s %d, %d bytes back of %d", resp.Proto, resp.StatusCode, len(text), len(data))
	}
	if resp, text := send(t, client, "POST", base+"/echo", nil, nil); resp.StatusCode != 200 || text != "" {
		t.Fatalf("empty echo: %d %q", resp.StatusCode, text)
	}
	if names := uploadtest.Entries(dir); len(names) != 2 {
		t.Fatalf("directory holds %v", names)
	}
}

// uploadCut starts a request with a large body and cancels it once the server
// has begun storing it: the client abandons the request's stream, which ends
// the body under the server's handler.
//
// The client holds the whole body before it sends any of it, so the body has
// to be large enough to be still on its way when the cancel comes: 64 MiB is,
// with a few MiB at most arrived on a fast machine. A larger one only costs
// memory, which the race detector multiplies, on a CI runner that has the
// other packages' tests running beside these.
func uploadCut(t *testing.T, newClient func() *http3.Client, method, url string, header http.Header, body []byte, stored string) {
	t.Helper()
	client := newClient()
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failed := make(chan error, 1)
	go func() {
		_, _, err := trySendContext(ctx, client, method, url, header, body)
		failed <- err
	}()
	uploadtest.Eventually(t, "the body to start arriving", func() bool {
		info, err := os.Stat(stored)
		return err == nil && info.Size() > 0
	})
	cancel()
	if err := <-failed; err == nil {
		t.Fatal("a cancelled request succeeded")
	}
}

func TestUploadCancelledMidBody(t *testing.T) {
	base, dir, newClient := startServer(t, 0)
	data := uploadtest.Bytes(64<<20, 2)
	uploadCut(t, newClient, "POST", base+"/upload?name=cut.bin", nil, data, filepath.Join(dir, "cut.bin.part"))
	// Nothing of it is kept, and the server serves the next client.
	uploadtest.Eventually(t, "the partial file to be removed", func() bool { return len(uploadtest.Entries(dir)) == 0 })
	client := newClient()
	defer client.Close()
	if resp, text := send(t, client, "POST", base+"/echo", nil, []byte("after")); resp.StatusCode != 200 || text != "after" {
		t.Fatalf("after the cut: %d %q", resp.StatusCode, text)
	}
}

func TestResumeAfterACancelledChunk(t *testing.T) {
	base, dir, newClient := startServer(t, 0)
	total := 64 << 20
	data := uploadtest.Bytes(total, 3)
	path := filepath.Join(dir, "cut.bin")
	header := http.Header{"Content-Range": {fmt.Sprintf("bytes 0-%d/%d", total-1, total)}}
	uploadCut(t, newClient, "PUT", base+"/resume?name=cut.bin", header, data, path+".part")

	// What arrived is kept, the server says so, and the rest goes on from there.
	kept := uploadtest.SettledSize(path + ".part")
	if kept <= 0 || kept >= int64(total) {
		t.Fatalf("kept %d bytes of %d", kept, total)
	}
	client := newClient()
	defer client.Close()
	resp, _ := send(t, client, "PUT", base+"/resume?name=cut.bin", http.Header{"Content-Range": {fmt.Sprintf("bytes */%d", total)}}, nil)
	if resp.StatusCode != 202 || resp.Header.Get("Upload-Offset") != strconv.FormatInt(kept, 10) {
		t.Fatalf("probe: %d offset %q, want %d", resp.StatusCode, resp.Header.Get("Upload-Offset"), kept)
	}
	resp, text := send(t, client, "PUT", base+"/resume?name=cut.bin",
		http.Header{"Content-Range": {fmt.Sprintf("bytes %d-%d/%d", kept, total-1, total)}}, data[kept:])
	if resp.StatusCode != 200 || text != fmt.Sprintf("stored bytes=%d sha256=%x\n", total, sha256.Sum256(data)) {
		t.Fatalf("rest: %d %q", resp.StatusCode, text)
	}
	if stored, _ := os.ReadFile(path); !bytes.Equal(stored, data) {
		t.Fatal("the resumed file differs")
	}
}

func TestEchoCancelledMidBody(t *testing.T) {
	base, dir, newClient := startServer(t, 0)
	client := newClient()
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failed := make(chan error, 1)
	go func() {
		_, _, err := trySendContext(ctx, client, "POST", base+"/echo", nil, uploadtest.Bytes(64<<20, 4))
		failed <- err
	}()
	// An echo leaves no file to watch; give it time to be under way.
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-failed
	next := newClient()
	defer next.Close()
	if resp, text := send(t, next, "POST", base+"/echo", nil, []byte("still serving")); resp.StatusCode != 200 || text != "still serving" {
		t.Fatalf("after the cancel: %d %q", resp.StatusCode, text)
	}
	if names := uploadtest.Entries(dir); len(names) != 0 {
		t.Fatalf("directory holds %v", names)
	}
}

func TestBodyAnnouncedPastTheLimit(t *testing.T) {
	base, dir, newClient := startServer(t, 1<<20)
	client := newClient()
	defer client.Close()
	resp, _ := send(t, client, "POST", base+"/upload?name=big.bin", nil, uploadtest.Bytes(4<<20, 5))
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want 413", resp.StatusCode)
	}
	if names := uploadtest.Entries(dir); len(names) != 0 {
		t.Fatalf("directory holds %v", names)
	}
}

func TestNewEngineErrors(t *testing.T) {
	serverTLS, _, _ := uploadtest.TLS(t)
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := newEngine("127.0.0.1:0", filepath.Join(file, "sub"), 0, serverTLS); err == nil {
		t.Error("created a directory inside a file")
	}
	if _, err := newEngine("not an address", t.TempDir(), 0, serverTLS); err == nil {
		t.Error("bound a bad address")
	}
}

func TestRun(t *testing.T) {
	certOut := filepath.Join(t.TempDir(), "cert.pem")
	args := []string{"-addr", "127.0.0.1:0", "-dir", t.TempDir(), "-cert-out", certOut}
	ctx, cancel := context.WithCancel(context.Background())
	var out bytes.Buffer
	addrs := make(chan string, 1)
	done := make(chan error, 1)
	go func() { done <- run(ctx, args, &out, func(addr string) { addrs <- addr }) }()
	addr := <-addrs
	if _, err := os.Stat(certOut); err != nil {
		t.Fatalf("the certificate was not written: %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "HTTP/3 upload server listening on https://"+addr) {
		t.Fatalf("output %q", out.String())
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if err := run(ctx2, args, io.Discard, nil); err != nil {
		t.Fatal(err)
	}
	if err := run(ctx, []string{"-nope"}, io.Discard, nil); err == nil {
		t.Error("accepted an unknown flag")
	}
	if err := run(ctx, []string{"-cert", filepath.Join(t.TempDir(), "missing.pem"), "-key", "missing.key"}, io.Discard, nil); err == nil {
		t.Error("started with a certificate that is not there")
	}
	if err := run(ctx, []string{"-addr", "not an address", "-cert-out", certOut, "-dir", t.TempDir()}, io.Discard, nil); err == nil {
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

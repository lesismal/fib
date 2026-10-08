//go:build linux || darwin || windows

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/examples/http/upload/uploadtest"
)

func TestMain(m *testing.M) {
	log.SetOutput(io.Discard)
	os.Exit(m.Run())
}

// startServer runs the server on a free port, storing into a fresh directory,
// and returns an HTTP/2 client for it that can be told how to dial.
func startServer(t *testing.T, maxSize int64) (base, dir string, client *http.Client, dials *dialLog) {
	t.Helper()
	serverTLS, clientTLS, _ := uploadtest.TLS(t)
	dir = t.TempDir()
	engine, err := newEngine("127.0.0.1:0", dir, maxSize, serverTLS)
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
	dials = &dialLog{}
	client = &http.Client{Timeout: 90 * time.Second, Transport: &http.Transport{
		TLSClientConfig:   clientTLS,
		ForceAttemptHTTP2: true,
		DialTLSContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := (&tls.Dialer{Config: clientTLS.Clone()}).DialContext(ctx, network, address)
			if err == nil {
				dials.add(conn)
			}
			return conn, err
		},
	}}
	t.Cleanup(client.CloseIdleConnections)
	return "https://" + addr.String(), dir, client, dials
}

// dialLog remembers the connections a client dialed, so that a test can cut
// one.
type dialLog struct {
	mu    sync.Mutex
	conns []net.Conn
}

func (d *dialLog) add(c net.Conn) { d.mu.Lock(); d.conns = append(d.conns, c); d.mu.Unlock() }

func (d *dialLog) cutAll() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, c := range d.conns {
		c.Close()
	}
}

func do(t *testing.T, client *http.Client, method, url string, header http.Header, body io.Reader, length int64) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = length
	req.Header = header
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

func TestUploadResumeAndEchoOverHTTP2(t *testing.T) {
	base, dir, client, _ := startServer(t, 0)
	data := uploadtest.Bytes(24<<20+31, 1)
	want := fmt.Sprintf("stored bytes=%d sha256=%x\n", len(data), sha256.Sum256(data))

	resp, text := do(t, client, "POST", base+"/upload?name=up.bin", nil, bytes.NewReader(data), int64(len(data)))
	if resp.ProtoMajor != 2 || resp.StatusCode != 200 || text != want {
		t.Fatalf("upload: %s %d %q", resp.Proto, resp.StatusCode, text)
	}
	if stored, _ := os.ReadFile(filepath.Join(dir, "up.bin")); !bytes.Equal(stored, data) {
		t.Fatal("uploaded file differs")
	}

	// In chunks, the way a resuming client sends them, each its own stream.
	const chunk = 6 << 20
	var last string
	for start := 0; start < len(data); start += chunk {
		end := min(start+chunk, len(data))
		header := http.Header{"Content-Range": {fmt.Sprintf("bytes %d-%d/%d", start, end-1, len(data))}}
		resp, last = do(t, client, "PUT", base+"/resume?name=re.bin", header, bytes.NewReader(data[start:end]), int64(end-start))
		if resp.ProtoMajor != 2 {
			t.Fatalf("chunk at %d over %s", start, resp.Proto)
		}
	}
	if resp.StatusCode != 200 || last != want {
		t.Fatalf("last chunk: %d %q", resp.StatusCode, last)
	}

	resp, text = do(t, client, "POST", base+"/echo", nil, bytes.NewReader(data), int64(len(data)))
	if resp.ProtoMajor != 2 || resp.StatusCode != 200 || len(text) != len(data) || sha256.Sum256([]byte(text)) != sha256.Sum256(data) {
		t.Fatalf("echo: %s %d, %d bytes back of %d", resp.Proto, resp.StatusCode, len(text), len(data))
	}
	if names := uploadtest.Entries(dir); len(names) != 2 {
		t.Fatalf("directory holds %v", names)
	}
}

// failingBody sends some bytes and then fails, as a file that cannot be read
// to the end would, which makes the HTTP/2 client reset the stream.
type failingBody struct {
	data []byte
	sent bool
}

func (b *failingBody) Read(p []byte) (int, error) {
	if b.sent {
		return 0, io.ErrUnexpectedEOF
	}
	b.sent = true
	return copy(p, b.data), nil
}

func TestUploadStreamResetMidBody(t *testing.T) {
	base, dir, client, _ := startServer(t, 0)
	data := uploadtest.Bytes(8<<20, 2)
	req, _ := http.NewRequest("POST", base+"/upload?name=reset.bin", &failingBody{data: data[:1<<20]})
	req.ContentLength = int64(len(data))
	if resp, err := client.Do(req); err == nil {
		resp.Body.Close()
		t.Fatal("an upload whose body failed succeeded")
	}
	// Nothing of it is kept, and the same connection serves the next request.
	uploadtest.Eventually(t, "the partial file to be removed", func() bool { return len(uploadtest.Entries(dir)) == 0 })
	resp, text := do(t, client, "POST", base+"/echo", nil, strings.NewReader("after"), 5)
	if resp.StatusCode != 200 || text != "after" {
		t.Fatalf("after the reset: %d %q", resp.StatusCode, text)
	}
}

func TestResumeAfterTheConnectionIsCut(t *testing.T) {
	base, dir, client, dials := startServer(t, 0)
	data := uploadtest.Bytes(12<<20, 3)
	total := len(data)
	path := filepath.Join(dir, "cut.bin")

	// A chunk of 8MiB goes out slowly enough to cut the connection under it.
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write(data[:3<<20])
	}()
	header := http.Header{"Content-Range": {fmt.Sprintf("bytes 0-%d/%d", 8<<20-1, total)}}
	req, _ := http.NewRequest("PUT", base+"/resume?name=cut.bin", pr)
	req.ContentLength = 8 << 20
	req.Header = header
	failed := make(chan error, 1)
	go func() {
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		failed <- err
	}()
	uploadtest.Eventually(t, "part of the chunk on disk", func() bool { return uploadtest.SettledSize(path+".part") > 1<<20 })
	dials.cutAll()
	if err := <-failed; err == nil {
		t.Fatal("a chunk on a cut connection succeeded")
	}
	_ = pw.Close()

	// What arrived is kept, the server says so, and the rest goes on from there.
	kept := uploadtest.SettledSize(path + ".part")
	if kept <= 0 || kept > 8<<20 {
		t.Fatalf("kept %d bytes", kept)
	}
	resp, _ := do(t, client, "PUT", base+"/resume?name=cut.bin", http.Header{"Content-Range": {fmt.Sprintf("bytes */%d", total)}}, nil, 0)
	if resp.StatusCode != 202 || resp.Header.Get("Upload-Offset") != strconv.FormatInt(kept, 10) {
		t.Fatalf("probe: %d offset %q, want %d", resp.StatusCode, resp.Header.Get("Upload-Offset"), kept)
	}
	resp, text := do(t, client, "PUT", base+"/resume?name=cut.bin",
		http.Header{"Content-Range": {fmt.Sprintf("bytes %d-%d/%d", kept, total-1, total)}}, bytes.NewReader(data[kept:]), int64(total)-kept)
	if resp.StatusCode != 200 || text != fmt.Sprintf("stored bytes=%d sha256=%x\n", total, sha256.Sum256(data)) {
		t.Fatalf("rest: %d %q", resp.StatusCode, text)
	}
	if stored, _ := os.ReadFile(path); !bytes.Equal(stored, data) {
		t.Fatal("the resumed file differs")
	}
}

func TestEchoConnectionCutMidBody(t *testing.T) {
	base, _, client, dials := startServer(t, 0)
	pr, pw := io.Pipe()
	req, _ := http.NewRequest("POST", base+"/echo", pr)
	req.ContentLength = 32 << 20
	failed := make(chan error, 1)
	go func() {
		resp, err := client.Do(req)
		if err == nil {
			_, err = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		failed <- err
	}()
	go func() { _, _ = pw.Write(uploadtest.Bytes(2<<20, 4)) }()
	time.Sleep(300 * time.Millisecond)
	dials.cutAll()
	if err := <-failed; err == nil {
		t.Fatal("an echo on a cut connection succeeded")
	}
	_ = pw.Close()
	// A new connection is dialed for the next request, and the server serves it.
	resp, text := do(t, client, "POST", base+"/echo", nil, strings.NewReader("still serving"), 13)
	if resp.StatusCode != 200 || text != "still serving" {
		t.Fatalf("after the cut: %d %q", resp.StatusCode, text)
	}
}

func TestUnboundedBodyPastTheLimit(t *testing.T) {
	base, dir, client, _ := startServer(t, 1<<20)
	// No Content-Length, so the limit is found out as the body grows.
	req, _ := http.NewRequest("POST", base+"/upload?name=big.bin", io.LimitReader(zeroReader{}, 8<<20))
	req.ContentLength = -1
	if resp, err := client.Do(req); err == nil {
		resp.Body.Close()
		if resp.StatusCode == 200 {
			t.Fatal("an upload past the limit was stored")
		}
	}
	uploadtest.Eventually(t, "nothing of it to be left", func() bool { return len(uploadtest.Entries(dir)) == 0 })
	resp, text := do(t, client, "POST", base+"/echo", nil, strings.NewReader("ok"), 2)
	if resp.StatusCode != 200 || text != "ok" {
		t.Fatalf("after the limit: %d %q", resp.StatusCode, text)
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }

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
	if !strings.Contains(out.String(), "HTTP/2 upload server listening on https://"+addr) {
		t.Fatalf("output %q", out.String())
	}

	// Without a ready callback, and with the errors a bad start gives.
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

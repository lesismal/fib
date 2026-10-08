//go:build linux || darwin || windows

// Command client uploads a file to the upload server, streaming it from disk
// with its Content-Length known, so that neither end holds the file in
// memory. It prints the SHA-256 it computed while sending and the server's
// answer, which carries the SHA-256 of what arrived.
//
// It uses net/http's client: fib's own client buffers a request body whole,
// which is what an upload of this size must not do.
//
//	go run ./examples/http/upload/mkfile -size 1GiB -o /tmp/big.bin
//	go run ./examples/http/upload/client -file /tmp/big.bin
package main

import (
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

func main() {
	server := flag.String("url", "http://127.0.0.1:8080/upload", "upload URL")
	path := flag.String("file", "upload-test.bin", "file to upload (see ./examples/http/upload/mkfile)")
	name := flag.String("name", "", "name to store it under, default the file's name")
	method := flag.String("method", http.MethodPost, "POST or PUT")
	flag.Parse()

	file, err := os.Open(*path)
	if err != nil {
		fatal(err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		fatal(err)
	}
	if *name == "" {
		*name = filepath.Base(*path)
	}
	target, err := url.Parse(*server)
	if err != nil {
		fatal(err)
	}
	q := target.Query()
	q.Set("name", *name)
	target.RawQuery = q.Encode()

	// The body is hashed and counted as the transport reads it.
	sum := sha256.New()
	var sent atomic.Int64
	body := io.TeeReader(file, io.MultiWriter(sum, countWriter{&sent}))

	req, err := http.NewRequest(*method, target.String(), io.NopCloser(body))
	if err != nil {
		fatal(err)
	}
	// A known length makes this a plain Content-Length upload rather than a
	// chunked one, and lets the server tell how far along it is. The body is
	// wrapped so that the transport cannot rewind it; the file is sent once.
	req.ContentLength = info.Size()
	req.Header.Set("Content-Type", "application/octet-stream")

	done := make(chan struct{})
	go progress(&sent, info.Size(), done)

	started := time.Now()
	resp, err := http.DefaultClient.Do(req)
	close(done)
	if err != nil {
		fatal(err)
	}
	defer resp.Body.Close()
	reply, _ := io.ReadAll(resp.Body)
	elapsed := time.Since(started)

	fmt.Printf("\nsent      bytes=%d sha256=%x\n", sent.Load(), sum.Sum(nil))
	fmt.Printf("server    %s: %s", resp.Status, reply)
	fmt.Printf("took %v (%.1f MiB/s)\n", elapsed.Round(time.Millisecond), float64(sent.Load())/(1<<20)/elapsed.Seconds())
	if resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
	if want := fmt.Sprintf("sha256=%x", sum.Sum(nil)); !strings.Contains(string(reply), want) {
		fmt.Println("MISMATCH: the server's SHA-256 differs from what was sent")
		os.Exit(1)
	}
	fmt.Println("OK: the server stored exactly what was sent")
}

type countWriter struct{ n *atomic.Int64 }

func (w countWriter) Write(p []byte) (int, error) { w.n.Add(int64(len(p))); return len(p), nil }

func progress(sent *atomic.Int64, total int64, done <-chan struct{}) {
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-done:
			return
		case <-tick.C:
			n := sent.Load()
			fmt.Printf("\r%6.1f%%  %d / %d bytes", 100*float64(n)/float64(max(total, 1)), n, total)
		}
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

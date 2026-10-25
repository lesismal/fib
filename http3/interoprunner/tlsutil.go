package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// The Interop Runner mounts the server's certificate chain and key at
// /certs, and points SSLKEYLOGFILE at a file in the logs directory. CERT and
// KEY are overridden only for running the image by hand, outside the runner.
func certPath() string {
	if v := os.Getenv("CERT"); v != "" {
		return v
	}
	return "/certs/cert.pem"
}

func keyPath() string {
	if v := os.Getenv("KEY"); v != "" {
		return v
	}
	return "/certs/priv.key"
}

// wwwDir is where the runner mounts the files the server serves.
func wwwDir() string {
	if v := os.Getenv("WWW"); v != "" {
		return v
	}
	return "/www"
}

func downloadsDir() string {
	if v := os.Getenv("DOWNLOADS"); v != "" {
		return v
	}
	return "/downloads"
}

func loadServerCertificate() (tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(certPath(), keyPath())
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("load %s and %s: %w", certPath(), keyPath(), err)
	}
	return cert, nil
}

// keyLogWriter opens SSLKEYLOGFILE, which the runner reads the TLS secrets
// from when it checks a capture, or returns nil when it is not set. The
// writer is shared by every connection, whose handshakes may write at once.
func keyLogWriter() io.Writer {
	path := os.Getenv("SSLKEYLOGFILE")
	if path == "" {
		return nil
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0o755)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Printf("cannot open SSLKEYLOGFILE %s: %v", path, err)
		return nil
	}
	return &lockedWriter{f: f}
}

type lockedWriter struct {
	mu sync.Mutex
	f  *os.File
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Write(p)
}

// prepareDirs creates the directories the runner mounts, so that writing a
// qlog or a log does not fail when the image runs outside the runner.
func prepareDirs() {
	for _, dir := range []string{downloadsDir(), os.Getenv("QLOGDIR"), os.Getenv("LOGS")} {
		if dir != "" {
			_ = os.MkdirAll(dir, 0o755)
		}
	}
}

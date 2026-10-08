//go:build linux || darwin || windows

// Package uploadtest holds what the upload example's tests share.
package uploadtest

import (
	"crypto/tls"
	"flag"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesismal/fib/examples/certs"
)

// Bytes is n pseudo-random bytes, the same every time for the same seed.
func Bytes(n int, seed uint64) []byte {
	b := make([]byte, n)
	rng := rand.New(rand.NewPCG(seed, seed+1))
	for i := range b {
		b[i] = byte(rng.Uint32())
	}
	return b
}

// TLS issues a self-signed certificate the way the example servers do, and
// returns the server's TLS config, the client's, and the certificate file.
func TLS(t *testing.T) (server, client *tls.Config, certFile string) {
	t.Helper()
	certFile = filepath.Join(t.TempDir(), "cert.pem")
	serverFlags := flag.NewFlagSet("server", flag.ContinueOnError)
	serverFlags.SetOutput(io.Discard)
	serverSide := certs.ServerFlagsOn(serverFlags)
	if err := serverFlags.Parse([]string{"-cert-out", certFile}); err != nil {
		t.Fatal(err)
	}
	server, err := serverSide.Config()
	if err != nil {
		t.Fatal(err)
	}
	clientFlags := flag.NewFlagSet("client", flag.ContinueOnError)
	clientFlags.SetOutput(io.Discard)
	clientSide := certs.ClientFlagsOn(clientFlags)
	if err := clientFlags.Parse([]string{"-ca", certFile}); err != nil {
		t.Fatal(err)
	}
	client, err = clientSide.Config()
	if err != nil {
		t.Fatal(err)
	}
	return server, client, certFile
}

// Eventually waits for cond, failing the test if it takes too long.
func Eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Entries are the names in dir.
func Entries(dir string) []string {
	entries, _ := os.ReadDir(dir)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// SettledSize waits for the size of path to stop changing and returns it, or
// -1 if there is no such file.
func SettledSize(path string) int64 {
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

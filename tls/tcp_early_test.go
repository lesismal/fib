//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"bytes"
	stdtls "crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// earlyEcho answers early data with "early:" and the data, and the data that
// follows the handshake with "data:" and the data.
type earlyEcho struct {
	fib.HandlerFuncs
}

func newEarlyEcho() earlyEcho {
	return earlyEcho{fib.HandlerFuncs{Data: func(c *fib.Connection, b []byte) {
		_ = c.Send(append([]byte("data:"), b...))
	}}}
}

func (earlyEcho) OnEarlyData(c *fib.Connection, b []byte) {
	_ = c.Send(append([]byte("early:"), b...))
}

// opensslClient is an OpenSSL 3 s_client, the peer for early data over TCP.
func opensslClient(t *testing.T) string {
	t.Helper()
	if runtime.GOOS != "linux" {
		// The peers are OpenSSL 3's, which only the Linux runs are known to have.
		t.Skip("OpenSSL interop runs on Linux")
	}
	path, err := exec.LookPath("openssl")
	if err != nil {
		t.Skip("no openssl")
	}
	out, _ := exec.Command(path, "version").Output()
	if !strings.HasPrefix(string(out), "OpenSSL 3") {
		t.Skipf("need OpenSSL 3, have %q", strings.TrimSpace(string(out)))
	}
	return path
}

// sClient runs s_client against addr, sending lines on stdin a moment after
// the handshake, and returns what it printed.
func sClient(t *testing.T, openssl, addr string, args []string, lines string) string {
	t.Helper()
	cmd := exec.Command(openssl, append([]string{"s_client", "-connect", addr, "-tls1_3",
		"-servername", "localhost"}, args...)...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(400 * time.Millisecond)
		_, _ = stdin.Write([]byte(lines))
		time.Sleep(600 * time.Millisecond)
		_ = stdin.Close()
	}()
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		t.Fatalf("s_client did not finish: %s", out.String())
	}
	return out.String()
}

// TestServerTakesEarlyDataFromOpenSSL has OpenSSL's s_client send early data
// over TLS 1.3 in both of the suites this package protects: AES-GCM's, and
// ChaCha20-Poly1305's, whose early keys are its AEAD's.
func TestServerTakesEarlyDataFromOpenSSL(t *testing.T) {
	openssl := opensslClient(t)
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"AES-GCM", nil},
		{"ChaCha20-Poly1305", []string{"-ciphersuites", "TLS_CHACHA20_POLY1305_SHA256"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testServerTakesEarlyDataFromOpenSSL(t, openssl, tc.args)
		})
	}
}

func testServerTakesEarlyDataFromOpenSSL(t *testing.T, openssl string, suiteArgs []string) {
	serverConfig, _ := tlsConfigs(t)
	serverConfig.MinVersion = 0
	args := func(more ...string) []string { return append(append([]string{}, suiteArgs...), more...) }
	for _, tc := range []struct {
		name  string
		early bool
	}{{"accepted", true}, {"not enabled", false}} {
		t.Run(tc.name, func(t *testing.T) {
			handler := NewServer(serverConfig, newEarlyEcho())
			handler.EarlyData = tc.early
			_, addr := startEchoServer(t, fib.DefaultConfig(), handler)
			dir := t.TempDir()
			session := filepath.Join(dir, "session")
			first := sClient(t, openssl, addr, args("-sess_out", session), "first\n")
			if !strings.Contains(first, "data:first") {
				t.Fatalf("first connection: %s", first)
			}
			earlyFile := filepath.Join(dir, "early")
			if err := os.WriteFile(earlyFile, []byte("zero-rtt\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			second := sClient(t, openssl, addr, args("-sess_in", session, "-early_data", earlyFile), "second\n")
			t.Log(second)
			if !strings.Contains(second, "Reused, TLSv1.3") {
				t.Fatalf("the session was not resumed: %s", second)
			}
			accepted := strings.Contains(second, "Early data was accepted")
			if accepted != tc.early {
				t.Fatalf("early data accepted = %v, want %v", accepted, tc.early)
			}
			if tc.early && !strings.Contains(second, "early:zero-rtt") {
				t.Fatalf("no answer to the early data: %s", second)
			}
			if !strings.Contains(second, "data:second") {
				t.Fatalf("no answer after the handshake: %s", second)
			}
			if !tc.early {
				return
			}
			// The same session again, as a replay of the first would be: the
			// handshake resumes, and the early data is turned away.
			replay := sClient(t, openssl, addr, args("-sess_in", session, "-early_data", earlyFile), "third\n")
			if !strings.Contains(replay, "Early data was rejected") || strings.Contains(replay, "early:zero-rtt") {
				t.Fatalf("a replay: %s", replay)
			}
			if !strings.Contains(replay, "Reused, TLSv1.3") || !strings.Contains(replay, "data:third") {
				t.Fatalf("the replayed session did not go on: %s", replay)
			}
		})
	}
}

// resumingClient dials with the cache and collects what comes back. The first
// write is made as soon as the dial completes, which is before the handshake.
func resumingClient(t *testing.T, engine *fib.Engine, addr string, config *stdtls.Config, cache QUICSessionCache, early bool, write string) string {
	t.Helper()
	got := make(chan string, 8)
	var mu sync.Mutex
	var received []byte
	handler := fib.HandlerFuncs{Data: func(c *fib.Connection, b []byte) {
		mu.Lock()
		received = append(received, b...)
		mu.Unlock()
		select {
		case got <- "":
		default:
		}
	}}
	dialed := make(chan error, 1)
	err := DialWithOptions(engine, "tcp", addr, 5*time.Second, config, DialOptions{SessionCache: cache, EarlyData: early}, handler,
		func(c *fib.Connection, err error) {
			if err == nil {
				err = c.Send([]byte(write))
			}
			dialed <- err
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-dialed; err != nil {
		t.Fatal(err)
	}
	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		s := string(received)
		mu.Unlock()
		if strings.HasSuffix(s, write) {
			return s
		}
		select {
		case <-got:
		case <-deadline:
			t.Fatalf("no answer to %q, have %q", write, s)
		}
	}
}

// TestClientSendsEarlyData has two engines send early data to each other in
// both TLS 1.3 suites the record layer protects: the early keys are derived as
// the traffic keys are, and are the suite's AEAD's.
func TestClientSendsEarlyData(t *testing.T) {
	for _, tc := range []struct {
		name   string
		chacha bool
	}{
		{"AES-GCM", false},
		{"ChaCha20-Poly1305", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.chacha {
				old := aesGCMHardware
				defer func() { aesGCMHardware = old }()
				aesGCMHardware = false
			}
			testClientSendsEarlyData(t)
		})
	}
}

func testClientSendsEarlyData(t *testing.T) {
	serverConfig, clientConfig := tlsConfigs(t)
	clientConfig.MinVersion = stdtls.VersionTLS13
	for _, tc := range []struct {
		name        string
		serverEarly bool
		clientEarly bool
		want        string
	}{
		{"accepted", true, true, "early:"},
		{"the client does not use it", true, false, "data:"},
		{"the server does not take it", false, true, "data:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := NewServer(serverConfig, newEarlyEcho())
			server.EarlyData = tc.serverEarly
			_, addr := startEchoServer(t, fib.DefaultConfig(), server)
			engine, err := fib.New(fib.DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			go engine.Run()
			t.Cleanup(engine.Stop)
			time.Sleep(100 * time.Millisecond)
			cache := NewQUICSessionCache(4)

			if got := resumingClient(t, engine, addr, clientConfig, cache, tc.clientEarly, "first"); got != "data:first" {
				t.Fatalf("first: %q", got)
			}
			var saved *QUICSession
			for deadline := time.Now().Add(3 * time.Second); saved == nil && time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
				saved = cache.Get("localhost")
			}
			if saved == nil {
				t.Skip("no ticket")
			}
			if got := resumingClient(t, engine, addr, clientConfig, cache, tc.clientEarly, "zero-rtt"); got != tc.want+"zero-rtt" {
				t.Fatalf("resumed: %q, want %q", got, tc.want+"zero-rtt")
			}
			if tc.want != "early:" {
				return
			}
			// The ticket of the first connection again, as a replay of it would
			// use it: the early data is turned away and sent again.
			// (once the ticket of the second connection has arrived, or it would
			// take the place of this one)
			for deadline := time.Now().Add(3 * time.Second); cache.Get("localhost") == saved && time.Now().Before(deadline) || cache.Get("localhost") == nil && time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			}
			cache.Put("localhost", saved)
			if got := resumingClient(t, engine, addr, clientConfig, cache, true, "replayed"); got != "data:replayed" {
				t.Fatalf("a replay: %q", got)
			}
		})
	}
}

// TestClientEarlyDataToOpenSSL sends early data to OpenSSL's s_server.
// TestClientEarlyDataToOpenSSL sends early data to OpenSSL's s_server in both
// TLS 1.3 suites this package protects, so that both its AEADs' early keys are
// checked against a peer that is not this package.
func TestClientEarlyDataToOpenSSL(t *testing.T) {
	openssl := opensslClient(t)
	for _, tc := range []struct {
		name   string
		args   []string
		chacha bool
	}{
		{"AES-GCM", nil, false},
		{"ChaCha20-Poly1305", []string{"-ciphersuites", "TLS_CHACHA20_POLY1305_SHA256"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.chacha {
				old := aesGCMHardware
				defer func() { aesGCMHardware = old }()
				aesGCMHardware = false
			}
			testClientEarlyDataToOpenSSL(t, openssl, tc.args)
		})
	}
}

func testClientEarlyDataToOpenSSL(t *testing.T, openssl string, suiteArgs []string) {
	serverConfig, clientConfig := tlsConfigs(t)
	clientConfig.MinVersion = stdtls.VersionTLS13
	cert := serverConfig.Certificates[0]
	der, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_, port, _ := net.SplitHostPort(addr)
	l.Close()
	cmd := exec.Command(openssl, append([]string{"s_server", "-accept", port, "-cert", certFile, "-key", keyFile, "-tls1_3",
		"-early_data", "-no_anti_replay", "-num_tickets", "1", "-naccept", "2", "-ign_eof"}, suiteArgs...)...)
	var out lockedBuffer
	cmd.Stdout, cmd.Stderr = &out, &out
	stdin, _ := cmd.StdinPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stdin.Close(); _ = cmd.Process.Kill(); _ = cmd.Wait() })
	time.Sleep(500 * time.Millisecond)

	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	go engine.Run()
	t.Cleanup(engine.Stop)
	time.Sleep(100 * time.Millisecond)
	cache := NewQUICSessionCache(4)
	send := func(write string) *fib.Connection {
		t.Helper()
		conns := make(chan *fib.Connection, 1)
		dialed := make(chan error, 1)
		err := DialWithOptions(engine, "tcp", addr, 5*time.Second, clientConfig, DialOptions{SessionCache: cache, EarlyData: true},
			fib.HandlerFuncs{}, func(c *fib.Connection, err error) {
				if err == nil {
					err = c.Send([]byte(write))
				}
				conns <- c
				dialed <- err
			})
		if err != nil {
			t.Fatal(err)
		}
		if err := <-dialed; err != nil {
			t.Fatal(err)
		}
		return <-conns
	}
	first := send("first-line\n")
	for deadline := time.Now().Add(3 * time.Second); cache.Get("localhost") == nil && time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
	}
	if cache.Get("localhost") == nil {
		t.Fatalf("no ticket from s_server: %s", out.text())
	}
	// s_server serves one connection at a time.
	first.Close()
	time.Sleep(300 * time.Millisecond)
	send("zero-rtt-line\n")
	time.Sleep(2 * time.Second)
	log := out.text()
	if !strings.Contains(log, "Early data received") || !strings.Contains(log, "zero-rtt-line") {
		t.Fatalf("s_server did not take the early data:\n%s", log)
	}
}

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"bytes"
	stdtls "crypto/tls"
	"io"
	"math/rand"
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

func writeFile(t testing.TB, size int) (*os.File, []byte) {
	t.Helper()
	data := make([]byte, size)
	rand.New(rand.NewSource(int64(size))).Read(data)
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f, data
}

// fileServer serves TLS, settling on version and suite, with the kernel's TLS
// or without, and hands the test each connection's layer once its handshake
// is done; onData, if set, runs on each read.
func fileServer(t testing.TB, kernel bool, version, suite uint16, onData func(*fib.Connection, []byte)) (layerOf, *stdtls.Config, string) {
	t.Helper()
	serverConfig, clientConfig := suiteConfigs(t, version, suite)
	layers := make(layerOf, 4)
	h := NewServer(serverConfig, &settledHandler{HandlerFuncs: fib.HandlerFuncs{Data: onData}, layers: layers})
	h.NoKernelTLS = !kernel
	_, addr := startEchoServer(t, fib.DefaultConfig(), h)
	return layers, clientConfig, addr
}

func dialFileServer(t testing.TB, addr string, config *stdtls.Config) *stdtls.Conn {
	conn, _ := dialRecording(t, addr, config)
	return conn
}

// recordingConn keeps what it reads, so that a test can look at the records
// a server sent.
type recordingConn struct {
	net.Conn
	mu   sync.Mutex
	read []byte
}

func (c *recordingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.mu.Lock()
	c.read = append(c.read, p[:n]...)
	c.mu.Unlock()
	return n, err
}

// lastRecordType is the type in the header of the last whole record read.
func (c *recordingConn) lastRecordType() byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	var typ byte
	for b := c.read; len(b) >= recordHeaderLen; {
		n := recordHeaderLen + (int(b[3])<<8 | int(b[4]))
		if len(b) < n {
			break
		}
		typ, b = b[0], b[n:]
	}
	return typ
}

func dialRecording(t testing.TB, addr string, config *stdtls.Config) (*stdtls.Conn, *recordingConn) {
	t.Helper()
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	rec := &recordingConn{Conn: raw}
	conn := stdtls.Client(rec, config)
	t.Cleanup(func() { conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	if err := conn.Handshake(); err != nil {
		t.Fatal(err)
	}
	return conn, rec
}

// wantKernel checks that the connection sealed in the kernel where it could,
// and here where it could not or was told not to.
func wantKernel(t *testing.T, l *layer, kernel bool) {
	t.Helper()
	l.wmu.Lock()
	state, fits := l.ktls, kernelTLSFits(l.tx)
	l.wmu.Unlock()
	switch {
	case kernel && fits && state == ktlsOn:
	case kernel && fits && kernelTLSMissing.Load():
		t.Log("the kernel has no TLS: sealed here")
	case kernel && runtime.GOOS == "linux" && fits:
		t.Fatalf("kernel TLS state %d, want on", state)
	case (!kernel || !fits) && state == ktlsOn:
		t.Fatal("sealed in the kernel where it should not have been")
	}
}

var fileSuites = []struct {
	name    string
	version uint16
	suite   uint16
}{
	{"TLS13", stdtls.VersionTLS13, 0},
	{"TLS12-AES128-GCM", stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256},
	{"TLS12-AES256-GCM", stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384},
	{"TLS12-CHACHA20", stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256},
	{"TLS12-AES128-CBC", stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256},
}

// A file sent over TLS reaches a standard client whole, behind what was sent
// before it and ahead of what was sent after, whether it was sent from the
// handler, corked with its replies, or from outside it, over every suite this
// package seals, with the kernel's TLS and without.
func TestSendFileOverTLS(t *testing.T) {
	const size = 1<<20 + 7
	f, data := writeFile(t, size)
	const offset, count = 333, size - 333 - 5
	want := append(append([]byte("head"), data[offset:offset+count]...), "tail"...)
	for _, s := range fileSuites {
		for _, kernel := range []bool{true, false} {
			name := s.name + "/userland"
			if kernel {
				name = s.name + "/kernel"
			}
			t.Run(name, func(t *testing.T) {
				send := func(c *fib.Connection) {
					if err := c.Send([]byte("head")); err != nil {
						t.Error(err)
					}
					if err := c.SendFile(f, offset, count); err != nil {
						t.Error(err)
					}
					if err := c.Send([]byte("tail")); err != nil {
						t.Error(err)
					}
				}
				layers, config, addr := fileServer(t, kernel, s.version, s.suite, func(c *fib.Connection, _ []byte) { send(c) })
				conn, rec := dialRecording(t, addr, config)
				l := <-layers
				// From outside the handler, then from it, twice over, on one
				// connection.
				for i := 0; i < 4; i++ {
					if i%2 == 0 {
						if !l.CanSendFile() {
							t.Fatal("the layer cannot send a file")
						}
						send(l.c)
					} else if _, err := conn.Write([]byte("x")); err != nil {
						t.Fatal(err)
					}
					got := make([]byte, len(want))
					if _, err := io.ReadFull(conn, got); err != nil {
						t.Fatalf("round %d: %v", i, err)
					}
					if !bytes.Equal(got, want) {
						t.Fatalf("round %d: the client read other bytes than were sent", i)
					}
				}
				wantKernel(t, l, kernel)
				// The stream goes on after the files, and ends with a
				// close_notify, whoever seals it.
				if err := l.c.Send([]byte("bye")); err != nil {
					t.Fatal(err)
				}
				l.c.CloseAfterSend()
				rest, err := io.ReadAll(conn)
				if err != nil || string(rest) != "bye" {
					t.Fatalf("read %q, %v after the files", rest, err)
				}
				// TLS 1.2 shows a record's type, and the last is the alert.
				if s.version == stdtls.VersionTLS12 {
					if typ := rec.lastRecordType(); typ != recordTypeAlert {
						t.Fatalf("the last record is of type %d, not the close_notify's", typ)
					}
				}
			})
		}
	}
}

// A file sent to a client that reads nothing yet takes no more memory than a
// chunk sealed, the rest of it waiting in the file, and reaches the client
// whole once it reads, slowly at first: over AES-GCM, sealed on the layer's own
// record layer or, where the kernel has TLS, in the kernel, and over
// ChaCha20-Poly1305, which the kernel's TLS has no suite for and only the
// layer seals, so that draining it is what the suite being on the layer buys.
func TestSendFileOverTLSSlowReader(t *testing.T) {
	const size = 48 << 20
	f, data := writeFile(t, size)
	for _, tc := range []struct {
		name           string
		version, suite uint16
		kernel         bool
	}{
		{"TLS13/kernel", stdtls.VersionTLS13, 0, true},
		{"TLS13/userland", stdtls.VersionTLS13, 0, false},
		{"TLS12-CHACHA20/kernel", stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256, true},
		{"TLS12-CHACHA20/userland", stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			layers, config, addr := fileServer(t, tc.kernel, tc.version, tc.suite, nil)
			conn := dialFileServer(t, addr, config)
			slowReaderFile(t, <-layers, conn, f, data, size, tc.kernel)
		})
	}
}

// slowReaderFile sends a file of size bytes over l to a client that reads
// nothing for a moment, checks that the heap does not hold it, and then reads
// it all back through conn, slowly at first.
func slowReaderFile(t *testing.T, l *layer, conn net.Conn, f *os.File, data []byte, size int, kernel bool) {
	t.Helper()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	if err := l.c.SendFile(f, 0, int64(size)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	runtime.GC()
	runtime.ReadMemStats(&after)
	if grown := int64(after.HeapInuse) - int64(before.HeapInuse); grown > 8<<20 {
		t.Fatalf("the heap grew by %d bytes with a %d-byte file unread", grown, size)
	}
	got := make([]byte, size)
	// Slowly at first, a little at a time.
	read := 0
	for i := 0; i < 50; i++ {
		n, err := conn.Read(got[read:min(read+4096, size)])
		if err != nil {
			t.Fatal(err)
		}
		read += n
		time.Sleep(2 * time.Millisecond)
	}
	if _, err := io.ReadFull(conn, got[read:]); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("the client read other bytes than the file's")
	}
	wantKernel(t, l, kernel)
}

// A file shorter than its range ends the connection, after what it had, and
// the client sees the stream end without a close_notify.
func TestSendFileOverTLSShortFile(t *testing.T) {
	const size = 300<<10 + 9
	f, data := writeFile(t, size)
	for _, kernel := range []bool{true, false} {
		t.Run(map[bool]string{true: "kernel", false: "userland"}[kernel], func(t *testing.T) {
			layers, config, addr := fileServer(t, kernel, stdtls.VersionTLS13, 0, nil)
			conn := dialFileServer(t, addr, config)
			l := <-layers
			_ = l.c.SendFile(f, 0, size+1000)
			// crypto/tls reports the end of a stream between records as
			// io.EOF, close_notify or not; what tells is the length.
			got, _ := io.ReadAll(conn)
			if len(got) > size || !bytes.Equal(got, data[:len(got)]) {
				t.Fatalf("the client read %d bytes, or they differ", len(got))
			}
		})
	}
}

// A connection that stays with crypto/tls, because its Config has
// GetConfigForClient and the handshake logs its keys to a Config of its own,
// cannot carry a file, and SendFile reads it through the layer instead.
func TestSendFileThroughCryptoTLS(t *testing.T) {
	const size = 200<<10 + 1
	f, data := writeFile(t, size)
	serverConfig, clientConfig := suiteConfigs(t, stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256)
	serverConfig.GetConfigForClient = func(*stdtls.ClientHelloInfo) (*stdtls.Config, error) { return nil, nil }
	layers := make(layerOf, 4)
	h := NewServer(serverConfig, &settledHandler{layers: layers})
	h.NoKernelTLS = true
	_, addr := startEchoServer(t, fib.DefaultConfig(), h)
	conn := dialFileServer(t, addr, clientConfig)
	l := <-layers
	if l.CanSendFile() || l.c.QueuesFiles() {
		t.Fatal("a connection on crypto/tls says it can carry a file")
	}
	if err := l.c.SendFile(f, 0, size); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, size)
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("the client read other bytes than the file's")
	}
}

// Where the kernel seals, a KeyUpdate the peer asks for goes out through it,
// and the kernel moves to the next keys behind it; a kernel that cannot, as
// Linux before 6.14 cannot, ends the connection.
func TestKernelTLSKeyUpdate(t *testing.T) {
	const size = 256 << 10
	f, data := writeFile(t, size)
	layers, config, addr := fileServer(t, true, stdtls.VersionTLS13, 0, nil)
	conn := dialFileServer(t, addr, config)
	l := <-layers
	if err := l.c.SendFile(f, 0, size); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, size)
	if _, err := io.ReadFull(conn, got); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("file: %v", err)
	}
	l.wmu.Lock()
	on := l.ktls == ktlsOn
	l.wmu.Unlock()
	if !on {
		t.Skip("the kernel does not seal here")
	}
	// The peer asked; the queue is empty, so it goes now.
	updated := l.updateWriteKeys()
	l.wmu.Lock()
	owed := l.rekeyOwed
	l.wmu.Unlock()
	if !updated {
		if _, err := conn.Read(make([]byte, 1)); err == nil {
			t.Fatal("the connection carried on after its kernel refused the next keys")
		}
		t.Skip("the kernel cannot take new keys")
	}
	if owed {
		t.Fatal("the KeyUpdate is still owed with nothing queued")
	}
	for i := 0; i < 3; i++ {
		msg := bytes.Repeat([]byte{byte('a' + i)}, 40000)
		if err := l.c.Send(msg); err != nil {
			t.Fatal(err)
		}
		back := make([]byte, len(msg))
		if _, err := io.ReadFull(conn, back); err != nil || !bytes.Equal(back, msg) {
			t.Fatalf("after the key update: %v", err)
		}
	}
}

// A connection whose handshake runs through crypto/tls and settles on
// ChaCha20-Poly1305 leaves it for the layer as the AES-GCM ones do, and a file
// sent over it drains with the socket: the heap does not hold the file while
// the peer reads nothing. crypto/tls picks its TLS 1.3 suites from whether the
// CPU has AES-GCM instructions, once at startup, so the test runs itself again
// with GODEBUG=cpu.aes=off for the TLS 1.3 case, where both ends prefer
// ChaCha20; the TLS 1.2 case only has to be offered the suite.
func TestSendFileOverTLSChaCha20(t *testing.T) {
	if os.Getenv("FIB_TLS_CHACHA20") == "1" {
		testSendFileOverTLSChaCha20(t)
		return
	}
	if runtime.GOOS == "windows" {
		// Windows keeps AES on with GODEBUG=cpu.aes=off: both ends settle on
		// TLS_AES_128_GCM_SHA256 there, so the suite is not this test's to
		// force as it is here. The other platforms run it.
		t.Skip("Windows does not turn AES off for crypto/tls with GODEBUG=cpu.aes=off")
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestSendFileOverTLSChaCha20$", "-test.v")
	// cpu.aes=off goes with whatever else GODEBUG was set to, since a second
	// GODEBUG variable in the environment is not what the child reads.
	env := os.Environ()
	for i, kv := range env {
		if strings.HasPrefix(kv, "GODEBUG=") {
			env[i] += ",cpu.aes=off"
			break
		}
		if i == len(env)-1 {
			env = append(env, "GODEBUG=cpu.aes=off")
		}
	}
	if len(env) == 0 {
		env = append(env, "GODEBUG=cpu.aes=off")
	}
	cmd.Env = append(env, "FIB_TLS_CHACHA20=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the file over ChaCha20-Poly1305 failed: %v\n%s", err, out)
	}
}

// testSendFileOverTLSChaCha20 is the body of TestSendFileOverTLSChaCha20, which
// the child process runs.
func testSendFileOverTLSChaCha20(t *testing.T) {
	const size = 48 << 20
	f, data := writeFile(t, size)
	for _, tc := range []struct {
		name    string
		version uint16
		suite   uint16
	}{
		{"TLS13", stdtls.VersionTLS13, 0},
		{"TLS12", stdtls.VersionTLS12, stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			serverConfig, clientConfig := suiteConfigs(t, tc.version, tc.suite)
			layers := make(layerOf, 4)
			h := NewServer(serverConfig, &settledHandler{layers: layers})
			// Through crypto/tls, where such a connection stayed before the
			// record layer had the suite.
			h.Blocking = true
			_, addr := startEchoServer(t, fib.DefaultConfig(), h)
			conn := dialFileServer(t, addr, clientConfig)
			l := <-layers
			want := stdtls.TLS_CHACHA20_POLY1305_SHA256
			if tc.suite != 0 {
				want = tc.suite
			}
			if state := conn.ConnectionState(); state.CipherSuite != want {
				t.Fatalf("the client settled on %#04x, want %#04x", state.CipherSuite, want)
			}
			if !taken(l) {
				t.Fatal("the connection stayed on crypto/tls")
			}
			if !l.CanSendFile() || !l.c.QueuesFiles() {
				t.Fatal("the connection cannot carry a file as it drains")
			}
			slowReaderFile(t, l, conn, f, data, size, true)
			if tc.version == stdtls.VersionTLS13 {
				// A KeyUpdate derives the next generation of ChaCha20 keys,
				// and what follows is sealed and opened under them, the
				// client's own under its next keys too.
				requestKeyUpdate(t, l)
				msg := bytes.Repeat([]byte("k"), 4000)
				if err := l.c.Send(msg); err != nil {
					t.Fatal(err)
				}
				back := make([]byte, len(msg))
				if _, err := io.ReadFull(conn, back); err != nil || !bytes.Equal(back, msg) {
					t.Fatalf("after the key update: %v", err)
				}
			}
		})
	}
}

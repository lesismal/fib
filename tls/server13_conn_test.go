//go:build linux || darwin || windows

package tls

import (
	"bytes"
	stdtls "crypto/tls"
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

func countFallbacks(t *testing.T) *atomic.Int64 {
	t.Helper()
	var n atomic.Int64
	nativeFellBack = func() { n.Add(1) }
	t.Cleanup(func() { nativeFellBack = nil })
	return &n
}

// greeter sends a greeting the moment a connection opens, which waits for the
// handshake, and echoes after that.
func greeter(greeting string) fib.Handler {
	return fib.HandlerFuncs{
		Open: func(c *fib.Connection) { _ = c.Send([]byte(greeting)) },
		Data: func(c *fib.Connection, b []byte) { _ = c.Send(b) },
	}
}

// Clients of TLS 1.3 are served without a worker, clients that offer no TLS
// 1.3 by crypto/tls, and both get what the server sent as soon as it opened.
func TestNativeServerServesStandardClients(t *testing.T) {
	started := countNative(t)
	fellBack := countFallbacks(t)
	serverConfig, clientConfig := tlsConfigs(t)
	serverConfig.NextProtos = []string{"fib-test"}
	_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, greeter("welcome;")))

	const each = 8
	var wg sync.WaitGroup
	run := func(version uint16, n int) {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(value byte) {
				defer wg.Done()
				config := clientConfig.Clone()
				config.MinVersion, config.MaxVersion = version, version
				config.NextProtos = []string{"fib-test"}
				conn, err := stdtls.Dial("tcp", addr, config)
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
				state := conn.ConnectionState()
				if state.Version != version || state.NegotiatedProtocol != "fib-test" {
					t.Errorf("state %+v", state)
				}
				greeting := make([]byte, len("welcome;"))
				if _, err := io.ReadFull(conn, greeting); err != nil || string(greeting) != "welcome;" {
					t.Errorf("greeting %q: %v", greeting, err)
					return
				}
				payload := bytes.Repeat([]byte{value}, 512<<10)
				go func() { _, _ = conn.Write(payload) }()
				got := make([]byte, len(payload))
				if _, err := io.ReadFull(conn, got); err != nil || !bytes.Equal(got, payload) {
					t.Errorf("echo: %v", err)
				}
			}(byte(i))
		}
	}
	run(stdtls.VersionTLS13, each)
	run(stdtls.VersionTLS12, each)
	wg.Wait()
	if n := started.Load(); n != 2*each {
		t.Fatalf("%d handshakes began without a worker, want %d", n, 2*each)
	}
	if n := fellBack.Load(); n != each {
		t.Fatalf("%d handed to crypto/tls, want the %d TLS 1.2 clients", n, each)
	}
}

// slowConn writes a few bytes at a time, so that a hello arrives in pieces.
type slowConn struct {
	net.Conn
	step int
}

func (c slowConn) Write(p []byte) (int, error) {
	for len(p) > 0 {
		n := min(c.step, len(p))
		if _, err := c.Conn.Write(p[:n]); err != nil {
			return 0, err
		}
		p = p[n:]
		time.Sleep(200 * time.Microsecond)
	}
	return len(p), nil
}

// A hello that arrives a byte at a time is waited for, and a TLS 1.2 one that
// does is replayed to crypto/tls whole.
func TestNativeServerWaitsForSlowHellos(t *testing.T) {
	fellBack := countFallbacks(t)
	serverConfig, clientConfig := tlsConfigs(t)
	_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, echoHandler()))
	for _, version := range []uint16{stdtls.VersionTLS13, stdtls.VersionTLS12} {
		raw, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		config := clientConfig.Clone()
		config.MinVersion, config.MaxVersion = version, version
		conn := stdtls.Client(slowConn{raw, 1}, config)
		_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
		if err := conn.Handshake(); err != nil {
			t.Fatalf("%x: %v", version, err)
		}
		if _, err := conn.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		reply := make([]byte, 4)
		if _, err := io.ReadFull(conn, reply); err != nil || string(reply) != "ping" {
			t.Fatalf("%x: %q %v", version, reply, err)
		}
		conn.Close()
	}
	if n := fellBack.Load(); n != 1 {
		t.Fatalf("%d handed to crypto/tls, want 1", n)
	}
}

// Connections that connect and say nothing cost no goroutine while they wait,
// and are closed when the handshake times out.
func TestNativeServerWaitsWithoutGoroutines(t *testing.T) {
	serverConfig, _ := tlsConfigs(t)
	closed := make(chan error, 1000)
	handler := NewServer(serverConfig, fib.HandlerFuncs{Close: func(_ *fib.Connection, err error) { closed <- err }})
	handler.HandshakeTimeout = 1500 * time.Millisecond
	_, addr := startEchoServer(t, fib.DefaultConfig(), handler)
	time.Sleep(100 * time.Millisecond)
	before := runtime.NumGoroutine()
	const conns = 400
	var held []net.Conn
	for i := 0; i < conns; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, conn)
	}
	defer func() {
		for _, c := range held {
			c.Close()
		}
	}()
	time.Sleep(700 * time.Millisecond)
	// The client side of these connections costs no goroutines either, so any
	// growth is the server's.
	if grown := runtime.NumGoroutine() - before; grown > conns/10 {
		t.Fatalf("%d goroutines more while %d handshakes waited", grown, conns)
	}
	for i := 0; i < conns; i++ {
		select {
		case err := <-closed:
			if !errors.Is(err, errHandshakeTimeout) {
				t.Fatalf("closed with %v, want a handshake timeout", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d of %d handshakes timed out", i, conns)
		}
	}
}

// A handshake the server refuses closes the connection with the reason.
func TestNativeServerRefusals(t *testing.T) {
	serverConfig, clientConfig := tlsConfigs(t)
	serverConfig.NextProtos = []string{"a"}
	closed := make(chan error, 4)
	_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, fib.HandlerFuncs{
		Close: func(_ *fib.Connection, err error) { closed <- err },
	}))
	clientConfig.NextProtos = []string{"b"}
	if _, err := stdtls.Dial("tcp", addr, clientConfig); err == nil {
		t.Fatal("a client with no protocol in common was served")
	}
	select {
	case err := <-closed:
		if err == nil {
			t.Fatal("closed without an error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the connection never closed")
	}
}

// Both ends without a worker.
func TestNativeClientToNativeServer(t *testing.T) {
	started := countNative(t)
	fellBack := countFallbacks(t)
	serverConfig, clientConfig := tlsConfigs(t)
	_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, echoHandler()))
	client, _ := startEchoServer(t, fib.DefaultConfig(), nil)
	h := newCollector()
	payload := bytes.Repeat([]byte("both-ends-"), 100<<10)
	err := Dial(client, "tcp", addr, 5*time.Second, clientConfig, h, func(c *fib.Connection, err error) {
		if err == nil {
			_ = c.Send(payload)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := h.waitFor(t, len(payload)); !bytes.Equal(got, payload) {
		t.Fatal("echo mismatch")
	}
	h.mu.Lock()
	state := h.state
	h.mu.Unlock()
	if state.Version != stdtls.VersionTLS13 {
		t.Fatalf("version %x", state.Version)
	}
	if n, f := started.Load(), fellBack.Load(); n != 2 || f != 0 {
		t.Fatalf("%d handshakes began without a worker, %d fell back, want 2 and 0", n, f)
	}
}

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

// nativeConfig is a client Config for the handshake that runs without a worker.
func nativeConfig(t *testing.T) (server, client *stdtls.Config) {
	t.Helper()
	server, client = tlsConfigs(t)
	client.MinVersion = stdtls.VersionTLS13
	server.MinVersion = stdtls.VersionTLS13
	return server, client
}

// countNative makes the test count the handshakes that ran without a worker.
func countNative(t *testing.T) *atomic.Int64 {
	t.Helper()
	var n atomic.Int64
	nativeStarted = func() { n.Add(1) }
	t.Cleanup(func() { nativeStarted = nil })
	return &n
}

// stdEchoServer is a crypto/tls server that echoes, after greeting.
func stdEchoServer(t *testing.T, config *stdtls.Config, greeting string) string {
	t.Helper()
	listener, err := stdtls.Listen("tcp", "127.0.0.1:0", config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				if greeting != "" {
					_, _ = io.WriteString(conn, greeting)
				}
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return listener.Addr().String()
}

// collector is a client handler that gathers what it is sent.
type collector struct {
	mu      sync.Mutex
	data    []byte
	state   stdtls.ConnectionState
	shaken  bool
	closed  chan error
	changed chan struct{}
}

func newCollector() *collector {
	return &collector{closed: make(chan error, 2), changed: make(chan struct{}, 1)}
}

func (c *collector) OnOpen(*fib.Connection)                 {}
func (c *collector) OnPriorityData(*fib.Connection, []byte) {}
func (c *collector) OnClose(_ *fib.Connection, err error)   { c.closed <- err }
func (c *collector) OnHandshake(_ *fib.Connection, state stdtls.ConnectionState) {
	c.mu.Lock()
	c.state, c.shaken = state, true
	c.mu.Unlock()
}
func (c *collector) OnData(_ *fib.Connection, b []byte) {
	c.mu.Lock()
	if !c.shaken {
		panic("plaintext before OnHandshake")
	}
	c.data = append(c.data, b...)
	c.mu.Unlock()
	select {
	case c.changed <- struct{}{}:
	default:
	}
}

func (c *collector) waitFor(t *testing.T, n int) []byte {
	t.Helper()
	deadline := time.After(15 * time.Second)
	for {
		c.mu.Lock()
		got := bytes.Clone(c.data)
		c.mu.Unlock()
		if len(got) >= n {
			return got
		}
		select {
		case <-c.changed:
		case err := <-c.closed:
			t.Fatalf("closed after %d of %d bytes: %v", len(got), n, err)
		case <-deadline:
			t.Fatalf("got %d of %d bytes", len(got), n)
		}
	}
}

// The client handshakes without a worker against crypto/tls, sends before the
// handshake has finished, and receives what the server sends right behind its
// Finished.
func TestNativeDialAgainstStandardServer(t *testing.T) {
	started := countNative(t)
	serverConfig, clientConfig := nativeConfig(t)
	serverConfig.NextProtos = []string{"fib-test"}
	clientConfig.NextProtos = []string{"fib-test"}
	addr := stdEchoServer(t, serverConfig, "greeting;")
	client, _ := startEchoServer(t, fib.DefaultConfig(), nil)

	payload := bytes.Repeat([]byte("native-"), 100<<10) // 700 KiB, many records
	h := newCollector()
	clientConfig.ServerName = "" // Dial fills it in from the address
	err := Dial(client, "tcp", addr, 5*time.Second, clientConfig, h, func(c *fib.Connection, err error) {
		if err != nil {
			t.Error(err)
			return
		}
		if _, ok := ConnectionState(c); ok {
			t.Error("handshake reported complete before it started")
		}
		if err := c.Send(payload); err != nil {
			t.Error(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	got := h.waitFor(t, len("greeting;")+len(payload))
	if string(got[:9]) != "greeting;" || !bytes.Equal(got[9:], payload) {
		t.Fatalf("echo mismatch: %d bytes", len(got))
	}
	h.mu.Lock()
	state := h.state
	h.mu.Unlock()
	if state.Version != stdtls.VersionTLS13 || state.NegotiatedProtocol != "fib-test" || len(state.PeerCertificates) == 0 {
		t.Fatalf("state %+v", state)
	}
	if n := started.Load(); n != 1 {
		t.Fatalf("%d handshakes ran without a worker, want 1", n)
	}
}

// A Config that allows a version below TLS 1.2 keeps to crypto/tls, and the
// default Config does not.
func TestConfigsThatKeepToCryptoTLS(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version uint16
		native  int64
	}{{"default", 0, 1}, {"TLS 1.1 allowed", stdtls.VersionTLS11, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			started := countNative(t)
			serverConfig, clientConfig := tlsConfigs(t)
			clientConfig.MinVersion = tc.version
			addr := stdEchoServer(t, serverConfig, "")
			client, _ := startEchoServer(t, fib.DefaultConfig(), nil)
			h := newCollector()
			clientConfig.ServerName = ""
			err := Dial(client, "tcp", addr, 5*time.Second, clientConfig, h, func(c *fib.Connection, err error) {
				_ = c.Send([]byte("ping"))
			})
			if err != nil {
				t.Fatal(err)
			}
			h.waitFor(t, 4)
			if n := started.Load(); n != tc.native {
				t.Fatalf("%d handshakes ran without a worker, want %d", n, tc.native)
			}
		})
	}
}

// Both ends on engines: the server is this package's crypto/tls one.
func TestNativeDialAgainstEngineServer(t *testing.T) {
	started := countNative(t)
	serverConfig, clientConfig := nativeConfig(t)
	_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, echoHandler()))
	client, _ := startEchoServer(t, fib.DefaultConfig(), nil)

	const conns = 20
	var wg sync.WaitGroup
	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func(value byte) {
			defer wg.Done()
			h := newCollector()
			payload := bytes.Repeat([]byte{value}, 256<<10)
			err := Dial(client, "tcp", addr, 5*time.Second, clientConfig, h, func(c *fib.Connection, err error) {
				if err == nil {
					_ = c.Send(payload)
				}
			})
			if err != nil {
				t.Error(err)
				return
			}
			got := h.waitFor(t, len(payload))
			if !bytes.Equal(got, payload) {
				t.Errorf("connection %d: echo mismatch", value)
			}
		}(byte(i))
	}
	wg.Wait()
	if n := started.Load(); n != 2*conns {
		t.Fatalf("%d handshakes ran without a worker, want %d", n, conns)
	}
}

func TestNativeHandshakeFailures(t *testing.T) {
	serverConfig, clientConfig := nativeConfig(t)
	client, _ := startEchoServer(t, fib.DefaultConfig(), nil)
	dial := func(t *testing.T, addr string, config *stdtls.Config) error {
		t.Helper()
		h := newCollector()
		if err := Dial(client, "tcp", addr, 5*time.Second, config, h, nil); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-h.closed:
			if err == nil {
				t.Fatal("closed without an error")
			}
			return err
		case <-time.After(10 * time.Second):
			t.Fatal("never closed")
		}
		return nil
	}
	t.Run("untrusted certificate", func(t *testing.T) {
		addr := stdEchoServer(t, serverConfig, "")
		err := dial(t, addr, &stdtls.Config{MinVersion: stdtls.VersionTLS13})
		var verify *stdtls.CertificateVerificationError
		if !errors.As(err, &verify) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("TLS 1.2 server", func(t *testing.T) {
		old := serverConfig.Clone()
		old.MinVersion, old.MaxVersion = stdtls.VersionTLS12, stdtls.VersionTLS12
		addr := stdEchoServer(t, old, "")
		if err := dial(t, addr, clientConfig); err == nil {
			t.Fatal("no error")
		}
	})
	t.Run("not a TLS server", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		go func() {
			conn, err := listener.Accept()
			if err == nil {
				_, _ = io.WriteString(conn, "HTTP/1.1 400 Bad Request\r\n\r\n")
				time.Sleep(time.Second)
				conn.Close()
			}
		}()
		if err := dial(t, listener.Addr().String(), clientConfig); err == nil {
			t.Fatal("no error")
		}
	})
	t.Run("server hangs up", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		go func() {
			conn, err := listener.Accept()
			if err == nil {
				buf := make([]byte, 100)
				_, _ = conn.Read(buf)
				conn.Close()
			}
		}()
		if err := dial(t, listener.Addr().String(), clientConfig); err == nil {
			t.Fatal("no error")
		}
	})
}

// A handshake that is not answered costs no goroutine, and times out.
func TestNativeHandshakeWaitsWithoutGoroutines(t *testing.T) {
	_, clientConfig := nativeConfig(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var held []net.Conn
	var mu sync.Mutex
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, conn) // accepted, and never answered
			mu.Unlock()
		}
	}()
	defer func() {
		mu.Lock()
		for _, c := range held {
			c.Close()
		}
		mu.Unlock()
	}()

	client, _ := startEchoServer(t, fib.DefaultConfig(), nil)
	time.Sleep(100 * time.Millisecond)
	before := runtime.NumGoroutine()
	const conns = 400
	closed := make(chan error, conns)
	handler := fib.HandlerFuncs{Close: func(_ *fib.Connection, err error) { closed <- err }}
	h := NewClient(clientConfig, handler)
	h.HandshakeTimeout = 1500 * time.Millisecond
	clientConfig.ServerName = "localhost"
	for i := 0; i < conns; i++ {
		if err := client.DialWithHandler("tcp", listener.Addr().String(), 5*time.Second, h, nil); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(700 * time.Millisecond)
	during := runtime.NumGoroutine()
	// A worker per handshake would add a goroutine per connection.
	if grown := during - before; grown > conns/10 {
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

// CloseAfterSend before the handshake is over ends the stream with close_notify
// once what was sent has gone out.
func TestNativeCloseAfterSend(t *testing.T) {
	serverConfig, clientConfig := nativeConfig(t)
	listener, err := stdtls.Listen("tcp", "127.0.0.1:0", serverConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	received := make(chan []byte, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		got, _ := io.ReadAll(conn) // to the close_notify
		received <- got
	}()
	client, _ := startEchoServer(t, fib.DefaultConfig(), nil)
	payload := bytes.Repeat([]byte("close-after-send"), 40<<10)
	h := newCollector()
	clientConfig.ServerName = ""
	err = Dial(client, "tcp", listener.Addr().String(), 5*time.Second, clientConfig, h, func(c *fib.Connection, err error) {
		if err == nil {
			_ = c.Send(payload)
			c.CloseAfterSend()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-received:
		if !bytes.Equal(got, payload) {
			t.Fatalf("the server read %d of %d bytes", len(got), len(payload))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the server never saw the end of the stream")
	}
	select {
	case <-h.closed:
	case <-time.After(10 * time.Second):
		t.Fatal("never closed")
	}
}

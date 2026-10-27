//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package tls

import (
	"bytes"
	"context"
	"crypto/sha256"
	stdtls "crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/bufferpool"
)

// covConnRecorder is a wrapped handler that records every callback it gets.
type covConnRecorder struct {
	mu         sync.Mutex
	opens      int
	data       []byte
	priority   []byte
	hangups    int
	handshakes []stdtls.ConnectionState
	closes     []error
	open       func(*fib.Connection)
	onData     func(*fib.Connection, []byte)
	onClose    func(*fib.Connection, error)
	closed     chan error
	changed    chan struct{}
}

func newCovConnRecorder() *covConnRecorder {
	return &covConnRecorder{closed: make(chan error, 4), changed: make(chan struct{}, 1)}
}

func (r *covConnRecorder) OnOpen(c *fib.Connection) {
	r.mu.Lock()
	r.opens++
	r.mu.Unlock()
	if r.open != nil {
		r.open(c)
	}
}

func (r *covConnRecorder) OnData(c *fib.Connection, b []byte) {
	r.mu.Lock()
	r.data = append(r.data, b...)
	r.mu.Unlock()
	if r.onData != nil {
		r.onData(c, b)
	}
	select {
	case r.changed <- struct{}{}:
	default:
	}
}

func (r *covConnRecorder) OnPriorityData(_ *fib.Connection, b []byte) {
	r.mu.Lock()
	r.priority = append(r.priority, b...)
	r.mu.Unlock()
}

func (r *covConnRecorder) OnHangup(*fib.Connection) {
	r.mu.Lock()
	r.hangups++
	r.mu.Unlock()
}

func (r *covConnRecorder) OnHandshake(_ *fib.Connection, state stdtls.ConnectionState) {
	r.mu.Lock()
	r.handshakes = append(r.handshakes, state)
	r.mu.Unlock()
}

func (r *covConnRecorder) OnClose(c *fib.Connection, err error) {
	r.mu.Lock()
	r.closes = append(r.closes, err)
	r.mu.Unlock()
	if r.onClose != nil {
		r.onClose(c, err)
	}
	r.closed <- err
}

func (r *covConnRecorder) snapshot() (opens int, data []byte, handshakes int, closes int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.opens, bytes.Clone(r.data), len(r.handshakes), len(r.closes)
}

func (r *covConnRecorder) waitClose(t *testing.T) error {
	t.Helper()
	select {
	case err := <-r.closed:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("the connection never closed")
	}
	return nil
}

func (r *covConnRecorder) waitData(t *testing.T, want string) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		_, got, _, _ := r.snapshot()
		if string(got) == want {
			return
		}
		select {
		case <-r.changed:
		case <-deadline:
			t.Fatalf("received %q, want %q", got, want)
		}
	}
}

// covConnClosed returns a connection that has closed and been released by its
// engine, whose sends fail.
func covConnClosed(t *testing.T) *fib.Connection {
	t.Helper()
	conns := make(chan *fib.Connection, 1)
	_, addr := startEchoServer(t, fib.DefaultConfig(), fib.HandlerFuncs{
		Close: func(c *fib.Connection, _ error) { conns <- c },
	})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	var c *fib.Connection
	select {
	case c = <-conns:
	case <-time.After(10 * time.Second):
		t.Fatal("the connection never closed")
	}
	for deadline := time.Now().Add(10 * time.Second); c.FD() != -1; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the connection's socket was never released")
		}
	}
	if err := c.SendRaw([]byte("x")); err == nil {
		t.Fatal("a closed connection took a send")
	}
	return c
}

// covConnLayer is a layer over c in the state OnOpen leaves one in.
func covConnLayer(c *fib.Connection, owner *Handler) *layer {
	l := &layer{c: c, handshaking: true, settling: true, owner: owner}
	l.cond.L = &l.mu
	return l
}

func covConnKeys(t *testing.T) *recordKeys {
	t.Helper()
	k, err := newKeys13(suite{keyLen: 16, hash: sha256.New}, sha256.New, bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// covConnFakeHS is a handshake without a worker whose every step is scripted.
type covConnFakeHS struct {
	out    []byte
	done   bool
	rx, tx *recordKeys
}

func (h *covConnFakeHS) process([]byte) (int, []byte, bool, *hsError) {
	return 0, h.out, h.done, nil
}
func (h *covConnFakeHS) alertRecord(uint8) []byte { return nil }
func (h *covConnFakeHS) connectionState() stdtls.ConnectionState {
	return stdtls.ConnectionState{Version: stdtls.VersionTLS13, HandshakeComplete: true}
}
func (h *covConnFakeHS) recordKeys() (*recordKeys, *recordKeys) { return h.rx, h.tx }

// covConnFakeEarlyHS is a scripted server handshake that takes early data.
type covConnFakeEarlyHS struct {
	covConnFakeHS
	early *recordKeys
}

func (h *covConnFakeEarlyHS) takeEarly() []byte           { return nil }
func (h *covConnFakeEarlyHS) earlyWriteKeys() *recordKeys { return h.early }
func (h *covConnFakeEarlyHS) takeTicket() []byte          { return nil }

func covConnHeld(s string) [][]byte {
	return [][]byte{bufferpool.Join(nil, []byte(s), nil)}
}

// Dial with a nil Config takes the server name from the address, which the
// test certificate's issuer, not being trusted by the system, fails; one
// without a port, a Unix socket's path, is taken whole; and a nil handler
// means the engine's, which does nothing.
func TestCovConnDialDefaults(t *testing.T) {
	serverConfig, clientConfig := tlsConfigs(t)
	t.Run("nil config", func(t *testing.T) {
		_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, echoHandler()))
		client, _ := startEchoServer(t, fib.DefaultConfig(), nil)
		h := newCovConnRecorder()
		if err := Dial(client, "tcp", addr, 5*time.Second, nil, h, nil); err != nil {
			t.Fatal(err)
		}
		var verify *stdtls.CertificateVerificationError
		if err := h.waitClose(t); !errors.As(err, &verify) {
			t.Fatalf("closed with %v, want a certificate verification error", err)
		}
	})
	t.Run("unix path as server name", func(t *testing.T) {
		dir, err := os.MkdirTemp("", "fib")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)
		path := filepath.Join(dir, "s.sock")
		config := fib.DefaultConfig()
		server, err := fib.New(config, fib.Binding{Network: "unix", Addr: path, Handler: NewServer(serverConfig, echoHandler())})
		if err != nil {
			t.Fatal(err)
		}
		runDone := make(chan error, 1)
		go func() { runDone <- server.Run() }()
		defer func() {
			server.Stop()
			<-runDone
			_ = server.Close()
		}()
		client, _ := startEchoServer(t, fib.DefaultConfig(), nil)
		h := newCovConnRecorder()
		config2 := clientConfig.Clone()
		config2.ServerName = ""
		if err := Dial(client, "unix", path, 5*time.Second, config2, h, nil); err != nil {
			t.Fatal(err)
		}
		var host x509.HostnameError
		err = h.waitClose(t)
		if !errors.As(err, &host) || host.Host != path {
			t.Fatalf("closed with %v, want a hostname error for %q", err, path)
		}
	})
	t.Run("engine handler", func(t *testing.T) {
		// The engine's handler does nothing, so it is the server that hears
		// the connection it serves.
		h := newCovConnRecorder()
		_, addr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, h))
		client, _ := startEchoServer(t, fib.DefaultConfig(), nil)
		err := Dial(client, "tcp", addr, 5*time.Second, clientConfig, nil, func(c *fib.Connection, err error) {
			if err == nil {
				_ = c.Send([]byte("engine's"))
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		h.waitData(t, "engine's")
		if _, _, n, _ := h.snapshot(); n != 1 {
			t.Fatalf("OnHandshake ran %d times on the server's handler", n)
		}
	})
}

// A Handler with no wrapped handler serves the handshake all the same.
func TestCovConnNilInnerHandler(t *testing.T) {
	for _, blocking := range []bool{false, true} {
		serverConfig, clientConfig := tlsConfigs(t)
		server := NewServer(serverConfig, nil)
		server.Blocking = blocking
		_, addr := startEchoServer(t, fib.DefaultConfig(), server)
		conn, err := stdtls.Dial("tcp", addr, clientConfig)
		if err != nil {
			t.Fatalf("blocking %v: %v", blocking, err)
		}
		if !conn.ConnectionState().HandshakeComplete {
			t.Fatalf("blocking %v: handshake incomplete", blocking)
		}
		if _, err := conn.Write([]byte("dropped")); err != nil {
			t.Fatal(err)
		}
		// With nothing to answer, the server closes only when the client does.
		_ = conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		var ne net.Error
		if _, err := conn.Read(make([]byte, 1)); !errors.As(err, &ne) || !ne.Timeout() {
			t.Fatalf("blocking %v: read %v, want a timeout", blocking, err)
		}
		conn.Close()
	}
}

// Out-of-band bytes and word of a hangup pass straight to the wrapped handler,
// when it takes them.
func TestCovConnPassThrough(t *testing.T) {
	r := newCovConnRecorder()
	h := NewServer(nil, r)
	h.OnPriorityData(nil, []byte("oob"))
	h.OnHangup(nil)
	h.OnHangup(nil)
	r.mu.Lock()
	priority, hangups := string(r.priority), r.hangups
	r.mu.Unlock()
	if priority != "oob" || hangups != 2 {
		t.Fatalf("priority %q, hangups %d", priority, hangups)
	}
	// A wrapped handler that does not take hangups is not told of them.
	NewServer(nil, fib.HandlerFuncs{}).OnHangup(nil)
}

// A peer that closes while a crypto/tls handshake waits for it fails the
// handshake, and the wrapped handler hears of the close once, from the
// handshake's worker.
func TestCovConnBlockingClosedMidHandshake(t *testing.T) {
	serverConfig, _ := tlsConfigs(t)
	r := newCovConnRecorder()
	server := NewServer(serverConfig, r)
	server.Blocking = true
	_, addr := startEchoServer(t, fib.DefaultConfig(), server)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	// The start of a handshake record that never arrives whole.
	if _, err := conn.Write([]byte{22, 3, 1, 0, 200, 1, 0, 0}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	conn.Close()
	if err := r.waitClose(t); err == nil {
		t.Fatal("closed without an error")
	}
	select {
	case err := <-r.closed:
		t.Fatalf("OnClose ran again, with %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if opens, _, shaken, _ := r.snapshot(); opens != 1 || shaken != 0 {
		t.Fatalf("opens %d, handshakes %d", opens, shaken)
	}
}

// A crypto/tls handshake that times out abandons the transport, which closes
// the connection.
func TestCovConnBlockingHandshakeTimeout(t *testing.T) {
	serverConfig, _ := tlsConfigs(t)
	r := newCovConnRecorder()
	server := NewServer(serverConfig, r)
	server.Blocking = true
	server.HandshakeTimeout = 100 * time.Millisecond
	_, addr := startEchoServer(t, fib.DefaultConfig(), server)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	err = r.waitClose(t)
	if !errors.Is(err, errHandshakeTimeout) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("closed with %v, want a handshake timeout", err)
	}
	// The server closed its end.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("read from a connection the server closed")
	}
}

// CloseAfterSend from OnOpen, under a crypto/tls handshake, sends what was
// sent before it and close_notify once the handshake is done; a second one does
// nothing, and a send after it fails.
func TestCovConnBlockingCloseAfterSendInOpen(t *testing.T) {
	serverConfig, clientConfig := tlsConfigs(t)
	sendErr := make(chan error, 1)
	r := newCovConnRecorder()
	r.open = func(c *fib.Connection) {
		_ = c.Send([]byte("bye"))
		c.CloseAfterSend()
		c.CloseAfterSend()
		sendErr <- c.Send([]byte("late"))
	}
	server := NewServer(serverConfig, r)
	server.Blocking = true
	_, addr := startEchoServer(t, fib.DefaultConfig(), server)
	conn, err := stdtls.Dial("tcp", addr, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	got, err := io.ReadAll(conn)
	if err != nil || string(got) != "bye" {
		t.Fatalf("read %q, %v; want \"bye\" up to close_notify", got, err)
	}
	if err := <-sendErr; !errors.Is(err, syscall.EPIPE) {
		t.Fatalf("send after CloseAfterSend: %v", err)
	}
}

// A connection crypto/tls keeps, because the Config has GetConfigForClient and
// its own Config would log the keys instead (see fastConfig): SendParts joins
// its parts into one record, and CloseAfterSend after the handshake has
// crypto/tls send close_notify.
func TestCovConnCryptoTLSRecords(t *testing.T) {
	serverConfig, clientConfig := tlsConfigs(t)
	chacha := []uint16{stdtls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305}
	serverConfig.MaxVersion, serverConfig.CipherSuites = stdtls.VersionTLS12, chacha
	clientConfig.MaxVersion, clientConfig.CipherSuites = stdtls.VersionTLS12, chacha
	serverConfig.GetConfigForClient = func(*stdtls.ClientHelloInfo) (*stdtls.Config, error) { return nil, nil }
	states := make(chan stdtls.ConnectionState, 1)
	r := newCovConnRecorder()
	r.onData = func(c *fib.Connection, b []byte) {
		if l := c.Layer().(*layer); l.tx != nil {
			t.Error("the connection took a suite over from crypto/tls")
		}
		if err := c.Layer().(*layer).Send(nil, nil); err != nil {
			t.Errorf("an empty send: %v", err)
		}
		state, _ := ConnectionState(c)
		states <- state
		_ = c.SendParts(b[:1], b[1:])
		c.CloseAfterSend()
	}
	server := NewServer(serverConfig, r)
	server.Blocking = true
	_, addr := startEchoServer(t, fib.DefaultConfig(), server)
	conn, err := stdtls.Dial("tcp", addr, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := conn.Write([]byte("parts")); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(conn)
	if err != nil || string(got) != "parts" {
		t.Fatalf("read %q, %v; want \"parts\" up to close_notify", got, err)
	}
	if state := <-states; state.CipherSuite != chacha[0] || state.Version != stdtls.VersionTLS12 {
		t.Fatalf("state: suite %x, version %x", state.CipherSuite, state.Version)
	}
}

// The layer stands in for a net.Conn to crypto/tls: it has no local address
// of its own, the connection's remote one while it has one, and deadlines that
// do nothing.
func TestCovConnLayerAsNetConn(t *testing.T) {
	serverConfig, clientConfig := tlsConfigs(t)
	type addrs struct{ local, remote, conn net.Addr }
	opened := make(chan addrs, 1)
	r := newCovConnRecorder()
	r.open = func(c *fib.Connection) {
		l := c.Layer().(*layer)
		for _, set := range []func(time.Time) error{l.SetDeadline, l.SetReadDeadline, l.SetWriteDeadline} {
			if err := set(time.Now()); err != nil {
				t.Errorf("deadline: %v", err)
			}
		}
		opened <- addrs{l.LocalAddr(), l.RemoteAddr(), c.RemoteAddr()}
	}
	_, srvAddr := startEchoServer(t, fib.DefaultConfig(), NewServer(serverConfig, r))
	conn, err := stdtls.Dial("tcp", srvAddr, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	got := <-opened
	if got.local.Network() != "tcp" || got.local.String() != "fib" {
		t.Fatalf("local address %s/%s", got.local.Network(), got.local)
	}
	if got.remote.String() != got.conn.String() || got.remote.String() != conn.LocalAddr().String() {
		t.Fatalf("remote address %v, connection's %v, client's %v", got.remote, got.conn, conn.LocalAddr())
	}

	// Once the connection's socket is gone it has no remote address either.
	l := covConnLayer(covConnClosed(t), nil)
	if remote := l.RemoteAddr(); remote != (addr{}) {
		t.Fatalf("remote address of a released connection: %v", remote)
	}
}

// ConnectionState reports false for a connection without TLS.
func TestCovConnConnectionStateWithoutTLS(t *testing.T) {
	results := make(chan bool, 1)
	_, addr := startEchoServer(t, fib.DefaultConfig(), fib.HandlerFuncs{
		Open: func(c *fib.Connection) {
			_, ok := ConnectionState(c)
			results <- ok
		},
	})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	select {
	case ok := <-results:
		if ok {
			t.Fatal("a plain connection reported TLS state")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("never opened")
	}
}

// What arrives after the layer closed is dropped; what a drain left of a
// round's bytes is kept, unless the layer has closed; and a handshake without
// a worker that has failed, or whose connection has closed, takes no more.
func TestCovConnInputAfterClose(t *testing.T) {
	r := newCovConnRecorder()
	l := &layer{closed: true}
	l.feed(r, []byte("late"))
	if _, data, _, _ := r.snapshot(); len(data) != 0 || l.in != nil {
		t.Fatalf("a closed layer took %q, kept %q", data, l.in)
	}

	l = &layer{src: []byte("rest")}
	l.releaseSrc()
	if string(l.in) != "rest" || l.src != nil {
		t.Fatalf("kept %q, src %q", l.in, l.src)
	}
	l = &layer{src: []byte("rest"), closed: true}
	l.releaseSrc()
	if l.in != nil || l.src != nil {
		t.Fatalf("a closed layer kept %q, src %q", l.in, l.src)
	}

	// The handshake is never consulted: a nil one would panic.
	for _, l := range []*layer{{nfailed: true}, {closed: true}} {
		l.feedNative(r, nil, []byte("late"))
		if l.nbuf != nil {
			t.Fatalf("kept %q", l.nbuf)
		}
	}
}

// Read records what it hands crypto/tls from a round's own bytes while a
// capture is collecting the handshake.
func TestCovConnReadCapturesRound(t *testing.T) {
	record := []byte{23, 3, 3, 0, 2, 'h', 'i'}
	c := &capture{client: true}
	l := &layer{src: record, capture: c}
	p := make([]byte, 64)
	n, err := l.Read(p)
	if err != nil || !bytes.Equal(p[:n], record) {
		t.Fatalf("read %q, %v", p[:n], err)
	}
	if !bytes.Equal(c.read, record) {
		t.Fatalf("captured %q", c.read)
	}
	c.release()
}

// crypto/tls writing to a connection that has closed is told so.
func TestCovConnWriteToClosed(t *testing.T) {
	l := covConnLayer(covConnClosed(t), nil)
	if n, err := l.Write([]byte("record")); err == nil || n != 0 {
		t.Fatalf("wrote %d, %v", n, err)
	}
}

// A handshake without a worker whose flight cannot be sent fails, and what was
// sent meanwhile is dropped.
func TestCovConnNativeFlightFails(t *testing.T) {
	r := newCovConnRecorder()
	l := covConnLayer(covConnClosed(t), nil)
	l.pending = covConnHeld("held")
	hs := &covConnFakeHS{out: []byte("flight")}
	l.native.Store(&nativeBox{hs})
	l.feed(r, []byte("hello"))
	if !l.nfailed || !l.failed || l.pending != nil || l.nbuf != nil {
		t.Fatalf("nfailed %v, failed %v, pending %d, nbuf %q", l.nfailed, l.failed, len(l.pending), l.nbuf)
	}
	if err := l.Send([]byte("more"), nil); !errors.Is(err, syscall.EPIPE) {
		t.Fatalf("send after the failure: %v", err)
	}
}

// A server that takes early data fails if what waited cannot be sent once it
// can, and never tells the handler the handshake happened.
func TestCovConnServeEarlyFails(t *testing.T) {
	r := newCovConnRecorder()
	l := covConnLayer(covConnClosed(t), nil)
	l.pending = covConnHeld("held")
	hs := &covConnFakeEarlyHS{early: covConnKeys(t)}
	l.native.Store(&nativeBox{hs})
	l.feed(r, []byte("hello"))
	if !l.earlyOpened || !l.nfailed || !l.failed || l.ready || l.nbuf != nil {
		t.Fatalf("earlyOpened %v, nfailed %v, failed %v, ready %v, nbuf %q", l.earlyOpened, l.nfailed, l.failed, l.ready, l.nbuf)
	}
	if _, _, shaken, _ := r.snapshot(); shaken != 0 {
		t.Fatal("OnHandshake ran for a failed connection")
	}
}

// A handshake without a worker that completes on a connection that cannot
// take what waited for it fails rather than reporting the handshake.
func TestCovConnCompleteNativeFails(t *testing.T) {
	r := newCovConnRecorder()
	l := covConnLayer(covConnClosed(t), nil)
	l.pending = covConnHeld("held")
	l.closeAfterSend = true
	hs := &covConnFakeHS{done: true, rx: covConnKeys(t), tx: covConnKeys(t)}
	l.native.Store(&nativeBox{hs})
	l.feed(r, []byte("behind the finished"))
	if !l.failed || l.ready || l.native.Load() != nil || l.handshaking || l.settling || l.in != nil {
		t.Fatalf("failed %v, ready %v, handshaking %v, settling %v, in %q", l.failed, l.ready, l.handshaking, l.settling, l.in)
	}
	if _, data, shaken, closes := r.snapshot(); shaken != 0 || len(data) != 0 || closes != 0 {
		t.Fatalf("handshakes %d, data %q, closes %d", shaken, data, closes)
	}
}

// covConnOuter sees the closes the engine reports to a Handler.
type covConnOuter struct {
	*Handler
	closed chan error
}

func (o covConnOuter) OnClose(c *fib.Connection, err error) {
	o.closed <- err
	o.Handler.OnClose(c, err)
}

// A client whose Config names no server and does not skip verification fails
// before it sends its hello, and the connection closes with why: the wrapped
// handler is opened, and told of the close.
func TestCovConnClientConfigRefused(t *testing.T) {
	_, addr := startEchoServer(t, fib.DefaultConfig(), echoHandler())
	client, _ := startEchoServer(t, fib.DefaultConfig(), nil)
	r := newCovConnRecorder()
	outer := covConnOuter{NewClient(&stdtls.Config{MinVersion: stdtls.VersionTLS13}, r), make(chan error, 1)}
	if err := client.DialWithHandler("tcp", addr, 5*time.Second, outer, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-outer.closed:
		if err == nil || !strings.Contains(err.Error(), "ServerName") {
			t.Fatalf("closed with %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the connection never closed")
	}
	if err := r.waitClose(t); err == nil || !strings.Contains(err.Error(), "ServerName") {
		t.Fatalf("the wrapped handler closed with %v", err)
	}
	if opens, _, _, closes := r.snapshot(); opens != 1 || closes != 1 {
		t.Fatalf("opened %d times, closed %d times", opens, closes)
	}
}

// covConnShutOuter shuts the connection's writing side before the TLS
// handler opens it, so the first send the layer makes is refused.
type covConnShutOuter struct {
	covConnOuter
}

func (o covConnShutOuter) OnOpen(c *fib.Connection) {
	_ = c.CloseWrite()
	o.Handler.OnOpen(c)
}

// A resuming client with early data sends its hello before OnOpen, so the
// early data the wrapped handler writes there follows it; when that send is
// refused, the wrapped handler is still opened, and told of the close.
func TestCovConnEarlyHelloRefused(t *testing.T) {
	serverConfig, clientConfig := tlsConfigs(t)
	clientConfig.MinVersion = stdtls.VersionTLS13
	server := NewServer(serverConfig, newEarlyEcho())
	server.EarlyData = true
	_, addr := startEchoServer(t, fib.DefaultConfig(), server)
	client, _ := startEchoServer(t, fib.DefaultConfig(), nil)
	cache := NewQUICSessionCache(4)
	if got := resumingClient(t, client, addr, clientConfig, cache, true, "first"); got != "data:first" {
		t.Fatalf("first: %q", got)
	}
	for deadline := time.Now().Add(3 * time.Second); cache.Get("localhost") == nil && time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
	}
	if cache.Get("localhost") == nil {
		t.Skip("no ticket")
	}

	r := newCovConnRecorder()
	early := make(chan bool, 1)
	r.onClose = func(c *fib.Connection, _ error) {
		l := c.Layer().(*layer)
		early <- l.early != nil && l.native.Load() == nil
	}
	h := NewClient(clientConfig, r)
	h.SessionCache, h.EarlyData = cache, true
	outer := covConnShutOuter{covConnOuter{h, make(chan error, 1)}}
	if err := client.DialWithHandler("tcp", addr, 5*time.Second, outer, nil); err != nil {
		t.Fatal(err)
	}
	refused := func(err error) bool {
		return errors.Is(err, syscall.EPIPE) || errors.Is(err, io.ErrClosedPipe)
	}
	select {
	case err := <-outer.closed:
		if !refused(err) {
			t.Fatalf("closed with %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the connection never closed")
	}
	if err := r.waitClose(t); !refused(err) {
		t.Fatalf("the wrapped handler closed with %v", err)
	}
	if !<-early {
		t.Fatal("the hello was not sent as the start of early data")
	}
	if opens, _, _, closes := r.snapshot(); opens != 1 || closes != 1 {
		t.Fatalf("opened %d times, closed %d times", opens, closes)
	}
}

// A handshake without a worker that fails drops what OnOpen sent, and the
// wrapped handler's CloseAfterSend after that just closes.
func TestCovConnNativeFailureDropsPending(t *testing.T) {
	serverConfig, _ := nativeConfig(t)
	addr := stdEchoServer(t, serverConfig, "")
	client, _ := startEchoServer(t, fib.DefaultConfig(), nil)
	results := make(chan error, 1)
	r := newCovConnRecorder()
	r.open = func(c *fib.Connection) { _ = c.Send([]byte("never sent")) }
	r.onClose = func(c *fib.Connection, _ error) {
		l := c.Layer().(*layer)
		l.wmu.Lock()
		failed, pending := l.failed, len(l.pending)
		l.wmu.Unlock()
		if !failed || pending != 0 {
			t.Errorf("failed %v, %d sends still held", failed, pending)
		}
		c.CloseAfterSend()
		results <- c.Send([]byte("after"))
	}
	// No RootCAs: the server's certificate is not trusted.
	config := &stdtls.Config{ServerName: "localhost", MinVersion: stdtls.VersionTLS13}
	if err := Dial(client, "tcp", addr, 5*time.Second, config, r, nil); err != nil {
		t.Fatal(err)
	}
	var verify *stdtls.CertificateVerificationError
	if err := r.waitClose(t); !errors.As(err, &verify) {
		t.Fatalf("closed with %v", err)
	}
	if err := <-results; !errors.Is(err, syscall.EPIPE) {
		t.Fatalf("send after the failure: %v", err)
	}
}

// covConnEarlyServer answers like earlyEcho, greets every connection from
// OnOpen, and ends the second with CloseAfterSend there too.
type covConnEarlyServer struct {
	opens      atomic.Int32
	mu         sync.Mutex
	handshakes []stdtls.ConnectionState
}

func (s *covConnEarlyServer) OnOpen(c *fib.Connection) {
	_ = c.Send([]byte("hi;"))
	if s.opens.Add(1) == 2 {
		c.CloseAfterSend()
	}
}
func (s *covConnEarlyServer) OnData(c *fib.Connection, b []byte) {
	_ = c.Send(append([]byte("data:"), b...))
}
func (s *covConnEarlyServer) OnEarlyData(c *fib.Connection, b []byte) {
	_ = c.Send(append([]byte("early:"), b...))
}
func (s *covConnEarlyServer) OnPriorityData(*fib.Connection, []byte) {}
func (s *covConnEarlyServer) OnClose(*fib.Connection, error)         {}
func (s *covConnEarlyServer) OnHandshake(_ *fib.Connection, state stdtls.ConnectionState) {
	s.mu.Lock()
	s.handshakes = append(s.handshakes, state)
	s.mu.Unlock()
}

// A server that takes early data sends what OnOpen sent as soon as its
// flight is out, and tells the handler of the handshake then; the
// close_notify OnOpen asked for follows once the handshake is over, and
// nothing is sent after the CloseAfterSend, the answer to the early data
// included. A client that would send early data whose
// hello cannot be sent fails before its handshake starts.
func TestCovConnEarlyServerFlushesOpen(t *testing.T) {
	serverConfig, clientConfig := tlsConfigs(t)
	clientConfig.MinVersion = stdtls.VersionTLS13
	inner := &covConnEarlyServer{}
	server := NewServer(serverConfig, inner)
	server.EarlyData = true
	_, addr := startEchoServer(t, fib.DefaultConfig(), server)
	client, _ := startEchoServer(t, fib.DefaultConfig(), nil)
	cache := NewQUICSessionCache(4)

	dial := func(write string) *covConnRecorder {
		t.Helper()
		r := newCovConnRecorder()
		err := DialWithOptions(client, "tcp", addr, 5*time.Second, clientConfig, DialOptions{SessionCache: cache, EarlyData: true}, r,
			func(c *fib.Connection, err error) {
				if err == nil {
					_ = c.Send([]byte(write))
				}
			})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	dial("first").waitData(t, "hi;data:first")
	var saved *QUICSession
	for deadline := time.Now().Add(5 * time.Second); saved == nil && time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		saved = cache.Get("localhost")
	}
	if saved == nil {
		t.Fatal("no ticket")
	}

	second := dial("zero-rtt")
	if err := second.waitClose(t); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("closed with %v", err)
	}
	if _, got, _, _ := second.snapshot(); string(got) != "hi;" {
		t.Fatalf("received %q, want \"hi;\"", got)
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		inner.mu.Lock()
		n := len(inner.handshakes)
		inner.mu.Unlock()
		if n == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d handshakes reported, want 2", n)
		}
	}
	inner.mu.Lock()
	resumed := inner.handshakes[1].DidResume
	inner.mu.Unlock()
	if !resumed {
		t.Fatal("the second connection did not resume")
	}

	// The saved ticket allows early data, so the hello goes out first, and
	// with it failing nothing else is set going.
	sessions := NewQUICSessionCache(4)
	sessions.Put("localhost", saved)
	r := newCovConnRecorder()
	h := &Handler{Config: clientConfig, Handler: r, Client: true, SessionCache: sessions, EarlyData: true, HandshakeTimeout: -1}
	c := covConnClosed(t)
	l := covConnLayer(c, h)
	l.client = true
	h.openNative(c, l)
	if opens, _, _, _ := r.snapshot(); opens != 1 || l.early == nil || l.native.Load() != nil || c.Layer() != fib.Layer(l) {
		t.Fatalf("opens %d, early %v, native %v", opens, l.early != nil, l.native.Load() != nil)
	}

	// Without early data the hello goes out once the handshake is set up.
	r = newCovConnRecorder()
	h = &Handler{Config: clientConfig, Handler: r, Client: true, HandshakeTimeout: -1}
	l = covConnLayer(c, h)
	l.client = true
	h.openNative(c, l)
	if opens, _, _, _ := r.snapshot(); opens != 1 || l.early != nil || l.native.Load() == nil {
		t.Fatalf("opens %d, early %v, native %v", opens, l.early != nil, l.native.Load() != nil)
	}
}

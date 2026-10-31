//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// tunnelProbe is a TunnelHandler that hands the tunnel to the test, and
// echoes what arrives unless the test is holding it.
type tunnelProbe struct {
	opened chan *Tunnel
	closed chan error
	mu     sync.Mutex
	got    []byte
	quiet  bool
}

func newTunnelProbe() *tunnelProbe {
	return &tunnelProbe{opened: make(chan *Tunnel, 1), closed: make(chan error, 1)}
}

func (p *tunnelProbe) OnTunnelOpen(t *Tunnel) { p.opened <- t }
func (p *tunnelProbe) OnTunnelData(t *Tunnel, data []byte) {
	p.mu.Lock()
	p.got = append(p.got, data...)
	quiet := p.quiet
	p.mu.Unlock()
	if !quiet {
		_ = t.Send(data)
	}
}
func (p *tunnelProbe) OnTunnelClose(_ *Tunnel, err error) { p.closed <- err }

func (p *tunnelProbe) received() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.got)
}

func waitFor[T any](t *testing.T, ch chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("timed out")
		panic("unreachable")
	}
}

func dialConnect(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func readFull(t *testing.T, conn net.Conn, want string) {
	t.Helper()
	got := make([]byte, len(want))
	if _, err := io.ReadFull(conn, got); err != nil || string(got) != want {
		t.Fatalf("read %q (%v), want %q", got, err, want)
	}
}

// A CONNECT over HTTP/1.1 switches the connection to a tunnel: the 200 goes
// out with the handler's header and no length, and what the client sent
// behind the request is the tunnel's first data.
func TestConnectOverHTTP1(t *testing.T) {
	probe := newTunnelProbe()
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		if _, err := c.Connect(stdhttp.Header{"X-Proxy": {"fib"}, "Content-Length": {"9"}}, probe); err != nil {
			_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", []byte(err.Error()))
		}
	})))
	conn := dialConnect(t, addr)
	_, _ = conn.Write([]byte("CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\nearly"))
	readFull(t, conn, "HTTP/1.1 200 OK\r\nX-Proxy: fib\r\n\r\nearly")
	tunnel := waitFor(t, probe.opened)
	_, _ = conn.Write([]byte("later"))
	readFull(t, conn, "later")
	// On Windows a send stays counted until its completion is taken, which
	// can come after the peer has read it.
	for deadline := time.Now().Add(5 * time.Second); runtime.GOOS == "windows" &&
		tunnel.Unsent() != 0 && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	if tunnel.Unsent() != 0 {
		t.Fatalf("%d bytes unsent with nothing waiting", tunnel.Unsent())
	}
	if err := tunnel.WaitSendable(0); err != nil {
		t.Fatal(err)
	}
	tunnel.HoldReads(true)
	_, _ = conn.Write([]byte("held"))
	time.Sleep(50 * time.Millisecond)
	if n := probe.received(); n != len("earlylater") {
		t.Fatalf("%d bytes arrived under the hold", n)
	}
	tunnel.HoldReads(false)
	readFull(t, conn, "held")
	_ = conn.Close()
	waitFor(t, probe.closed)
}

// Connect takes only a CONNECT without content, and Upgrade does not take
// one; neither can follow a response.
func TestConnectRefuses(t *testing.T) {
	results := make(chan error, 8)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		switch c.Request.URL.Path {
		case "/nil":
			_, err := c.Connect(nil, nil)
			results <- err
		case "/written":
			_ = c.Respond(stdhttp.StatusOK, "text/plain", nil)
			_, err := c.Connect(nil, newTunnelProbe())
			results <- err
		default:
			_, err := c.Connect(nil, newTunnelProbe())
			results <- err
			if c.Request.Method == stdhttp.MethodConnect {
				_, err = c.Upgrade("echo", nil, newTunnelProbe())
				results <- err
			}
		}
		if !c.wrote {
			_ = c.Respond(stdhttp.StatusTeapot, "text/plain", nil)
		}
	})))
	for _, tc := range []struct {
		req  string
		want []error
	}{
		{"GET / HTTP/1.1\r\nHost: x\r\n\r\n", []error{ErrNotUpgradable}},
		{"GET /nil HTTP/1.1\r\nHost: x\r\n\r\n", []error{errors.New("http: invalid upgrade")}},
		{"GET /written HTTP/1.1\r\nHost: x\r\n\r\n", []error{ErrResponseWritten}},
		{"CONNECT x:1 HTTP/1.1\r\nHost: x:1\r\nContent-Length: 2\r\n\r\nab", []error{ErrNotUpgradable, ErrNotUpgradable}},
	} {
		conn := dialConnect(t, addr)
		_, _ = conn.Write([]byte(tc.req))
		for _, want := range tc.want {
			if err := waitFor(t, results); err == nil || err.Error() != want.Error() {
				t.Errorf("%q: %v, want %v", tc.req, err, want)
			}
		}
	}
}

// h2Connect opens a CONNECT to a host and port on stream id.
func h2Connect(tc *h2TestConn, id uint32, authority string) {
	tc.headers(id, false, ":method", "CONNECT", ":authority", authority)
}

// A CONNECT over HTTP/2 switches its stream to a tunnel, which the handler
// paces: holding its reads keeps the window from coming back, and what it
// sends waits for the client's window, which WaitSendable waits for.
func TestConnectOverHTTP2(t *testing.T) {
	probe := newTunnelProbe()
	results := make(chan error, 4)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		if _, ok := c.Request.Header[":protocol"]; ok {
			_, err := c.Connect(nil, probe)
			results <- err
			_ = c.Respond(stdhttp.StatusTeapot, "text/plain", nil)
			return
		}
		if _, err := c.Upgrade("echo", nil, probe); err != ErrNotUpgradable {
			results <- err
		}
		if _, err := c.Connect(stdhttp.Header{"X-Proxy": {"fib"}}, probe); err != nil {
			results <- err
		}
	})))
	tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 100})
	h2covConnect(tc, 1, "/")
	if err := waitFor(t, results); err != ErrNotUpgradable {
		t.Fatalf("Connect on an extended CONNECT: %v", err)
	}
	h2Connect(tc, 3, "example.com:443")
	if f := tc.readUntil(h2FrameHeaders); f.streamID == 1 {
		f = tc.readUntil(h2FrameHeaders)
		if f.fields[":status"] != "200" || f.fields["x-proxy"] != "fib" || f.has(h2FlagEndStream) {
			t.Fatalf("response %v", f.fields)
		}
	}
	tunnel := waitFor(t, probe.opened)

	// What the tunnel sends past the client's window of 100 waits.
	probe.mu.Lock()
	probe.quiet = true
	probe.mu.Unlock()
	if err := tunnel.Send(make([]byte, 1000)); err != nil {
		t.Fatal(err)
	}
	if n := tunnel.Unsent(); n != 900 {
		t.Fatalf("%d bytes unsent, want 900", n)
	}
	sendable := make(chan error, 1)
	go func() { sendable <- tunnel.WaitSendable(0) }()
	select {
	case err := <-sendable:
		t.Fatalf("WaitSendable returned %v with 900 bytes unsent", err)
	case <-time.After(50 * time.Millisecond):
	}
	if tunnel.NotifySendable(900, func(error) { t.Error("NotifySendable ran within its limit") }) {
		t.Fatal("NotifySendable reported 900 bytes unsent past a limit of 900")
	}
	notified := make(chan error, 1)
	if !tunnel.NotifySendable(100, func(err error) { notified <- err }) {
		t.Fatal("NotifySendable reported 900 bytes unsent within a limit of 100")
	}
	select {
	case err := <-notified:
		t.Fatalf("NotifySendable ran with %v with 900 bytes unsent", err)
	case <-time.After(50 * time.Millisecond):
	}
	tc.write(h2AppendWindowUpdate(nil, 3, 1000))
	if err := waitFor(t, sendable); err != nil {
		t.Fatal(err)
	}
	if err := waitFor(t, notified); err != nil {
		t.Fatal(err)
	}

	// Held, the tunnel's reads give no window back; released, they do.
	tunnel.HoldReads(true)
	chunk := make([]byte, h2DefaultMaxFrameSize)
	sent := 0
	for ; sent < h2StreamWindow/2+len(chunk); sent += len(chunk) {
		tc.data(3, false, chunk)
	}
	tc.write(h2AppendFrameHeader(nil, h2FramePing, 0, 0, 8))
	tc.write(make([]byte, 8))
	for {
		f := tc.read()
		if f.typ == h2FrameWindowUpdate && f.streamID == 3 {
			t.Fatal("window given back under the hold")
		}
		if f.typ == h2FramePing {
			break
		}
	}
	tunnel.HoldReads(false)
	for {
		if f := tc.read(); f.typ == h2FrameWindowUpdate && f.streamID == 3 {
			break
		}
	}

	// A reset stream ends a writer's wait.
	if err := tunnel.Send(make([]byte, 1000)); err != nil {
		t.Fatal(err)
	}
	go func() { sendable <- tunnel.WaitSendable(0) }()
	if !tunnel.NotifySendable(0, func(err error) { notified <- err }) {
		t.Fatal("NotifySendable reported nothing unsent")
	}
	tc.write(h2AppendRSTStream(nil, 3, H2Cancel))
	if err := waitFor(t, sendable); err == nil {
		t.Fatal("WaitSendable on a reset stream returned nil")
	}
	if err := waitFor(t, notified); err == nil {
		t.Fatal("NotifySendable on a reset stream ran with nil")
	}
	if !tunnel.NotifySendable(0, func(err error) { notified <- err }) {
		t.Fatal("NotifySendable on a reset stream reported false")
	}
	if err := waitFor(t, notified); err == nil {
		t.Fatal("NotifySendable called on a reset stream ran with nil")
	}
	waitFor(t, probe.closed)
}

// echoServer is an echo server of the net package's.
func echoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return ln.Addr().String()
}

// connectThrough sends a CONNECT for target with extra header lines, and
// returns the status line.
func connectThrough(t *testing.T, proxy, target, extra string) (net.Conn, string) {
	t.Helper()
	conn := dialConnect(t, proxy)
	_, _ = conn.Write([]byte("CONNECT " + target + " HTTP/1.1\r\nHost: " + target + "\r\n" + extra + "\r\n"))
	var head []byte
	b := make([]byte, 1)
	for !bytes.HasSuffix(head, []byte("\r\n\r\n")) {
		if _, err := conn.Read(b); err != nil {
			t.Fatalf("read %q: %v", head, err)
		}
		head = append(head, b[0])
	}
	line, _, _ := strings.Cut(string(head), "\r\n")
	return conn, line
}

func basic(user, pass string) string {
	return "Proxy-Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass)) + "\r\n"
}

func TestConnectProxy(t *testing.T) {
	target := echoServer(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedAddr := ln.Addr().String()
	_ = ln.Close()
	proxy := serve(t, NewHandler(NewConnectProxy(ConnectProxyConfig{
		Authenticate: func(u, p string) bool { return u == "me" && p == "pw" },
		Allow: func(r *stdhttp.Request) error {
			if strings.HasPrefix(r.Host, "10.") {
				return errors.New("no")
			}
			return nil
		},
	})))
	login := basic("me", "pw")
	for _, tc := range []struct {
		target, extra, status string
	}{
		{target, "", "HTTP/1.1 407 Proxy Authentication Required"},
		{target, "Proxy-Authorization: Bearer x\r\n", "HTTP/1.1 407 Proxy Authentication Required"},
		{target, "Proxy-Authorization: Basic !!\r\n", "HTTP/1.1 407 Proxy Authentication Required"},
		{target, "Proxy-Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte("me")) + "\r\n",
			"HTTP/1.1 407 Proxy Authentication Required"},
		{target, basic("me", "no"), "HTTP/1.1 407 Proxy Authentication Required"},
		{"10.0.0.1:443", login, "HTTP/1.1 403 Forbidden"},
		{"nohost", login, "HTTP/1.1 400 Bad Request"},
		{"h:0", login, "HTTP/1.1 400 Bad Request"},
		{closedAddr, login, "HTTP/1.1 502 Bad Gateway"},
	} {
		if _, line := connectThrough(t, proxy, tc.target, tc.extra); line != tc.status {
			t.Errorf("%s %q: %q, want %q", tc.target, tc.extra, line, tc.status)
		}
	}

	conn, line := connectThrough(t, proxy, target, login)
	if line != "HTTP/1.1 200 OK" {
		t.Fatalf("status %q", line)
	}
	_, _ = conn.Write([]byte("through"))
	readFull(t, conn, "through")

	// Not a CONNECT.
	conn = dialConnect(t, proxy)
	_, _ = conn.Write([]byte("GET http://x/ HTTP/1.1\r\nHost: x\r\n\r\n"))
	readFull(t, conn, "HTTP/1.1 405 Method Not Allowed\r\n")
}

// What the proxy cannot reach in time is 504; a request it cannot switch is
// 400, closing the connection it dialed; anything but a CONNECT goes to
// Next.
func TestConnectProxyEdges(t *testing.T) {
	proxy := serve(t, NewHandler(NewConnectProxy(ConnectProxyConfig{
		DialTimeout: -1,
		Dial: func(*fib.Engine, string, string, time.Duration, fib.Handler, func(*fib.Connection, error)) error {
			return os.ErrDeadlineExceeded
		},
		Next: HandlerFunc(func(c *Context) { _ = c.Respond(stdhttp.StatusTeapot, "text/plain", nil) }),
	})))
	if _, line := connectThrough(t, proxy, "x:1", ""); line != "HTTP/1.1 504 Gateway Timeout" {
		t.Errorf("status %q", line)
	}
	conn := dialConnect(t, proxy)
	_, _ = conn.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n"))
	readFull(t, conn, "HTTP/1.1 418")

	targetClosed := make(chan struct{}, 1)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				_, _ = c.Read(make([]byte, 1))
				_ = c.Close()
				select {
				case targetClosed <- struct{}{}:
				default:
				}
			}()
		}
	}()
	proxy = serve(t, NewHandler(NewConnectProxy(ConnectProxyConfig{})))
	conn = dialConnect(t, proxy)
	_, _ = conn.Write([]byte("CONNECT " + ln.Addr().String() + " HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\n\r\nx"))
	readFull(t, conn, "HTTP/1.1 400 Bad Request\r\n")
	_ = conn.Close()
	waitFor(t, targetClosed)
}

// A target that closes before the tunnel opens ends the tunnel as soon as
// it does.
func TestConnectProxyTargetGoneFirst(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	// The target resets once the proxy has its connection: a reset that
	// beat the connect's completion would fail the dial instead.
	dialed := make(chan struct{})
	go func() {
		c, err := ln.Accept()
		if err == nil {
			select {
			case <-dialed:
			case <-time.After(10 * time.Second):
			}
			_ = c.(*net.TCPConn).SetLinger(0)
			_ = c.Close()
		}
	}()
	proxy := serve(t, NewHandler(NewConnectProxy(ConnectProxyConfig{
		// Report the dial only once the target has closed.
		Dial: func(engine *fib.Engine, network, addr string, timeout time.Duration, h fib.Handler,
			done func(*fib.Connection, error)) error {
			closed := make(chan struct{})
			watch := closeWatch{Handler: h, closed: closed}
			return engine.DialWithHandler(network, addr, timeout, watch, func(c *fib.Connection, err error) {
				if err != nil {
					done(c, err)
					return
				}
				close(dialed)
				go func() {
					<-closed
					done(c, nil)
				}()
			})
		},
	})))
	conn, line := connectThrough(t, proxy, ln.Addr().String(), "")
	if line != "HTTP/1.1 200 OK" {
		t.Fatalf("status %q", line)
	}
	if n, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatalf("read %d bytes from a tunnel whose target had gone", n)
	}
}

// The handler returns while the dial is under way, and the dial's callback
// answers the request.
func TestConnectProxyDoesNotWait(t *testing.T) {
	target := echoServer(t)
	returned := make(chan struct{})
	connect := NewConnectProxy(ConnectProxyConfig{
		Dial: func(engine *fib.Engine, network, addr string, timeout time.Duration, h fib.Handler,
			done func(*fib.Connection, error)) error {
			go func() {
				// A handler that waited for the dial would never return.
				select {
				case <-returned:
				case <-time.After(10 * time.Second):
				}
				if err := engine.DialWithHandler(network, addr, timeout, h, done); err != nil {
					done(nil, err)
				}
			}()
			return nil
		},
	})
	proxy := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		connect.ServeHTTP(c)
		if c.Request.Method == stdhttp.MethodConnect {
			close(returned)
		}
	})))
	conn, line := connectThrough(t, proxy, target, "")
	if line != "HTTP/1.1 200 OK" {
		t.Fatalf("status %q", line)
	}
	select {
	case <-returned:
	default:
		t.Fatal("answered before the handler returned")
	}
	_, _ = conn.Write([]byte("through"))
	readFull(t, conn, "through")
}

// A tunnel that carries something more often than its idle timeout stays
// open, and one silent for longer is closed, with its target. The timeout is
// a second, five times the gap between the writes, so that a runner that
// stalls the test for a few hundred milliseconds, as Windows CI's has, does
// not leave the tunnel idle for the whole of it.
func TestConnectProxyIdleTimeout(t *testing.T) {
	const idle = time.Second
	target := echoServer(t)
	proxy := serve(t, NewHandler(NewConnectProxy(ConnectProxyConfig{IdleTimeout: idle})))
	conn, line := connectThrough(t, proxy, target, "")
	if line != "HTTP/1.1 200 OK" {
		t.Fatalf("status %q", line)
	}
	for range 5 {
		time.Sleep(idle / 5)
		_, _ = conn.Write([]byte("x"))
		readFull(t, conn, "x")
	}
	start := time.Now()
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if n, err := conn.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("read %d bytes (%v) from an idle tunnel", n, err)
	}
	if waited := time.Since(start); waited < idle/2 || waited > 5*idle {
		t.Fatalf("closed after %v, want about %v", waited, idle)
	}
}

// closeWatch is a handler that says when its connection has closed.
type closeWatch struct {
	fib.Handler
	closed chan struct{}
}

func (w closeWatch) OnClose(c *fib.Connection, err error) {
	w.Handler.OnClose(c, err)
	close(w.closed)
}

// A transfer far larger than the relay queues arrives whole both ways, over
// HTTP/1.1 and HTTP/2.
func TestConnectProxyBackpressure(t *testing.T) {
	target := echoServer(t)
	proxy := serve(t, NewHandler(NewConnectProxy(ConnectProxyConfig{MaxQueuedBytes: 4 << 10})))
	payload := make([]byte, 16<<20)
	_, _ = rand.Read(payload)

	conn, line := connectThrough(t, proxy, target, "")
	if line != "HTTP/1.1 200 OK" {
		t.Fatalf("status %q", line)
	}
	_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
	go func() { _, _ = conn.Write(payload) }()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("HTTP/1.1: %v", err)
	}

	tc := dialH2(t, proxy)
	_ = tc.c.SetDeadline(time.Now().Add(60 * time.Second))
	h2Connect(tc, 1, target)
	if f := tc.readUntil(h2FrameHeaders); f.fields[":status"] != "200" {
		t.Fatalf("HTTP/2 status %v", f.fields)
	}
	h2Payload := payload[:4<<20]
	// The client's side of flow control: the server's windows, which it
	// gives back as the relay takes what it sent.
	connWindow, streamWindow := h2ConnWindow, h2StreamWindow
	sent, received := 0, 0
	for received < len(h2Payload) {
		for sent < len(h2Payload) && connWindow > 0 && streamWindow > 0 {
			n := min(len(h2Payload)-sent, h2DefaultMaxFrameSize, connWindow, streamWindow)
			tc.data(1, false, h2Payload[sent:sent+n])
			sent += n
			connWindow -= n
			streamWindow -= n
		}
		f := tc.read()
		switch {
		case f.typ == h2FrameWindowUpdate && f.streamID == 0:
			connWindow += int(f.payload[0])<<24 | int(f.payload[1])<<16 | int(f.payload[2])<<8 | int(f.payload[3])
		case f.typ == h2FrameWindowUpdate:
			streamWindow += int(f.payload[0])<<24 | int(f.payload[1])<<16 | int(f.payload[2])<<8 | int(f.payload[3])
		case f.typ == h2FrameData:
			if !bytes.Equal(f.payload, h2Payload[received:received+len(f.payload)]) {
				t.Fatalf("HTTP/2: bytes %d.. differ", received)
			}
			received += len(f.payload)
			tc.write(h2AppendWindowUpdate(h2AppendWindowUpdate(nil, 0, uint32(len(f.payload))), 1,
				uint32(len(f.payload))))
		}
	}
}

// A header HTTP/1 cannot carry is refused before anything is sent, and a
// tunnel whose writer cannot pace it neither holds nor waits.
func TestConnectEdges(t *testing.T) {
	results := make(chan error, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		_, err := c.Connect(stdhttp.Header{"Bad Name": {"x"}}, newTunnelProbe())
		results <- err
		_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", nil)
	})))
	conn := dialConnect(t, addr)
	_, _ = conn.Write([]byte("CONNECT x:1 HTTP/1.1\r\nHost: x:1\r\n\r\n"))
	if err := waitFor(t, results); err == nil {
		t.Fatal("a header with a space in its name went out")
	}
	readFull(t, conn, "HTTP/1.1 400")

	tunnel := NewTunnelFeed(nil, plainWriter{}, 0).Tunnel()
	tunnel.HoldReads(true)
	if tunnel.Unsent() != 0 || tunnel.WaitSendable(0) != nil {
		t.Fatal("a writer that cannot pace reported a backlog")
	}
	(&relayTarget{}).OnPriorityData(nil, nil)
}

// plainWriter is a TunnelWriter that cannot pace its tunnel.
type plainWriter struct{}

func (plainWriter) SendParts(_, _ []byte) error { return nil }
func (plainWriter) SendOwned([]byte) error      { return nil }
func (plainWriter) CloseAfterSend()             {}
func (plainWriter) Close() error                { return nil }

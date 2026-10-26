//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"context"
	stdtls "crypto/tls"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/internal/reqbody"
	"github.com/lesismal/fib/tlstest"
)

// cvCliWait fails the test unless ch is closed or sent on within a few seconds.
func cvCliWait[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
	var zero T
	return zero
}

// cvCliReadUntilEOF reports on closed once the peer of conn has closed it.
func cvCliReadUntilEOF(conn net.Conn, closed chan<- struct{}) {
	_, _ = io.Copy(io.Discard, conn)
	close(closed)
}

// cvCliOK is the response the raw servers here answer with.
const cvCliOK = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"

// NewClient fills the limits it cannot do without, and offers HTTP/2 through
// ALPN; closing a client twice is harmless.
func TestCvCliNewClientDefaults(t *testing.T) {
	c := NewClient(nil, ClientConfig{})
	defaults := DefaultClientConfig()
	if c.config.MaxResponseHeaderBytes != defaults.MaxResponseHeaderBytes ||
		c.config.MaxResponseBodyBytes != defaults.MaxResponseBodyBytes ||
		c.config.StreamResponseBodyBuffer != DefaultStreamResponseBodyBuffer {
		t.Fatalf("config %+v", c.config)
	}
	if got := c.tlsConfig.NextProtos; len(got) != 2 || got[0] != "h2" {
		t.Fatalf("NextProtos %v", got)
	}
	c.Close()
	c.Close()
}

// requestTarget defaults the port to the scheme's and refuses a request with
// nowhere to go.
func TestCvCliRequestTarget(t *testing.T) {
	for _, tc := range []struct {
		url    string
		addr   string
		secure bool
	}{
		{"http://example.com/", "example.com:80", false},
		{"https://example.com/", "example.com:443", true},
		{"https://example.com:8443/", "example.com:8443", true},
	} {
		u, _ := url.Parse(tc.url)
		target, err := requestTarget(&stdhttp.Request{URL: u})
		if err != nil || target.addr != tc.addr || target.secure != tc.secure {
			t.Fatalf("%s: %+v %v", tc.url, target, err)
		}
	}
	if _, err := requestTarget(&stdhttp.Request{}); err == nil {
		t.Fatal("a request without a URL was accepted")
	}
	if _, err := requestTarget(&stdhttp.Request{URL: &url.URL{Scheme: "http", Path: "/"}}); err == nil {
		t.Fatal("a URL without a host was accepted")
	}
}

// cvCliErrBody is a small body of a declared length that fails to read.
type cvCliErrBody struct{ closed bool }

func (b *cvCliErrBody) Read([]byte) (int, error) { return 0, errors.New("cvcli: read failed") }
func (b *cvCliErrBody) Close() error             { b.closed = true; return nil }

// cvCliBody is a body of unknown length, which therefore streams, that
// reports its Close.
type cvCliBody struct {
	once   sync.Once
	closed chan struct{}
}

func newCvCliBody() *cvCliBody                { return &cvCliBody{closed: make(chan struct{})} }
func (b *cvCliBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *cvCliBody) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}

func (b *cvCliBody) waitClosed(t *testing.T) {
	t.Helper()
	cvCliWait(t, b.closed, "the request body to be closed")
}

// Do fails a request it cannot frame, or whose small body fails to read, on
// the caller's goroutine, and takes a nil callback.
func TestCvCliDoRejectsUnframable(t *testing.T) {
	client := NewClient(nil, DefaultClientConfig())
	defer client.Close()
	do := func(req *stdhttp.Request) error {
		var got error
		called := false
		client.Do(req, func(_ *ClientResponse, err error) { got, called = err, true })
		if !called {
			t.Fatal("callback did not run before Do returned")
		}
		return got
	}

	// A nil callback is allowed, even for a request that fails at once.
	client.Do(mustRequest(t, "GET", "ftp://127.0.0.1/", nil), nil)

	body := &cvCliErrBody{}
	req := mustRequest(t, "POST", "http://127.0.0.1:1/", nil)
	req.Body, req.ContentLength = body, 5
	if err := do(req); err == nil || !strings.Contains(err.Error(), "read failed") {
		t.Fatalf("failing body: %v", err)
	}
	if !body.closed {
		t.Fatal("failing body was not closed")
	}

	// A control byte in an opaque URL is refused by net/http's framing, for a
	// whole body and for a streamed one.
	req = mustRequest(t, "GET", "http://127.0.0.1:1/", nil)
	req.URL.Opaque = "/a\x01b"
	if err := do(req); err == nil {
		t.Fatal("a control byte in the URL was sent")
	}
	streamed := newCvCliBody()
	req = mustRequest(t, "POST", "http://127.0.0.1:1/", nil)
	req.URL.Opaque = "/a\x01b"
	req.Body = streamed
	if err := do(req); err == nil {
		t.Fatal("a control byte in the URL was sent with a streamed body")
	}
	streamed.waitClosed(t)

	// An HTTP/1.0 request whose request line cannot be rewritten.
	req = mustRequest(t, "GET", "http://127.0.0.1:1/", nil)
	req.Method = "GET\r\nX:"
	req.ProtoMajor, req.ProtoMinor = 1, 0
	if err := do(req); err == nil || !strings.Contains(err.Error(), "as HTTP/1.0") {
		t.Fatalf("HTTP/1.0 request line: %v", err)
	}
}

// A dial the engine refuses at once fails the request, over http and https.
func TestCvCliDialOnStoppedEngine(t *testing.T) {
	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	engine.Stop()
	<-runDone
	defer engine.Close()
	client := NewClient(engine, DefaultClientConfig())
	defer client.Close()
	for _, u := range []string{"http://127.0.0.1:1/", "https://127.0.0.1:1/"} {
		if _, err := client.Go(mustRequest(t, "GET", u, nil)).Wait(); err == nil {
			t.Fatalf("%s: a dial on a stopped engine succeeded", u)
		}
	}
}

// An https dial to a port nobody listens on fails the request.
func TestCvCliTLSDialRefused(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	client := newTestClient(t, DefaultClientConfig())
	if _, err := client.Go(mustRequest(t, "GET", "https://"+addr+"/", nil)).Wait(); err == nil {
		t.Fatal("a refused https dial succeeded")
	}
}

// A server that hangs up during the TLS handshake fails the dial, and so the
// request, rather than leaving it waiting.
func TestCvCliTLSHandshakeHangUp(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
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
			conn.Close()
		}
	}()
	client := newTestClient(t, DefaultClientConfig())
	if _, err := client.Go(mustRequest(t, "GET", "https://"+listener.Addr().String()+"/", nil)).Wait(); err == nil {
		t.Fatal("a request over a failed handshake succeeded")
	}
}

// An idle HTTP/1 connection is closed once IdleConnTimeout passes.
func TestCvCliIdleConnTimeoutHTTP1(t *testing.T) {
	closed := make(chan struct{})
	u := rawServer(t, func(conn net.Conn, i int) {
		if _, err := stdhttp.ReadRequest(bufio.NewReader(conn)); err != nil {
			return
		}
		_, _ = io.WriteString(conn, cvCliOK)
		if i == 0 {
			cvCliReadUntilEOF(conn, closed)
		}
	})
	config := DefaultClientConfig()
	config.IdleConnTimeout = 10 * time.Millisecond
	client := newTestClient(t, config)
	if _, err := client.Go(mustRequest(t, "GET", u, nil)).Wait(); err != nil {
		t.Fatal(err)
	}
	cvCliWait(t, closed, "the idle connection to close")
	client.mu.Lock()
	defer client.mu.Unlock()
	for _, h := range client.hosts {
		if len(h.idle) != 0 {
			t.Fatalf("%d idle connections left", len(h.idle))
		}
	}
}

// A connection that goes idle again and again keeps the idle timer it was
// first given, and is not closed by one it has outlived: with requests
// spaced closer than IdleConnTimeout, they all take the one connection.
func TestCvCliIdleTimerReused(t *testing.T) {
	var conns atomic.Int32
	u := rawServer(t, func(conn net.Conn, _ int) {
		conns.Add(1)
		r := bufio.NewReader(conn)
		for {
			if _, err := stdhttp.ReadRequest(r); err != nil {
				return
			}
			if _, err := io.WriteString(conn, cvCliOK); err != nil {
				return
			}
		}
	})
	config := DefaultClientConfig()
	// Long enough that the timer does not fire while the loop runs: what it
	// checks is that the connection keeps the timer it was given, not what
	// happens when one fires mid-loop, which a slow runner's clock decides.
	config.IdleConnTimeout = 2 * time.Second
	client := newTestClient(t, config)
	var timer *time.Timer
	for i := 0; i < 8; i++ {
		resp, err := client.Go(mustRequest(t, "GET", u, nil)).Wait()
		if err != nil {
			t.Fatal(err)
		}
		readBody(t, resp)
		var idle *clientConn
		for deadline := time.Now().Add(time.Second); idle == nil && time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			client.mu.Lock()
			for _, h := range client.hosts {
				if len(h.idle) == 1 {
					idle = h.idle[0]
				}
			}
			client.mu.Unlock()
		}
		if idle == nil {
			t.Fatal("the connection was not kept idle")
		}
		client.mu.Lock()
		got := idle.idleTimer
		client.mu.Unlock()
		if timer == nil {
			timer = got
		} else if got != timer {
			t.Fatalf("request %d: the connection went idle with another timer", i)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if n := conns.Load(); n != 1 {
		t.Fatalf("the requests took %d connections, want 1", n)
	}
}

// The idle timer is not reset each time a connection goes idle; one set
// earlier fires, and is set again for what is left. The connection closes
// IdleConnTimeout after it last went idle, not after it first did.
func TestCvCliIdleTimerSetAgain(t *testing.T) {
	closed := make(chan time.Time, 1)
	u := rawServer(t, func(conn net.Conn, _ int) {
		r := bufio.NewReader(conn)
		for {
			if _, err := stdhttp.ReadRequest(r); err != nil {
				closed <- time.Now()
				return
			}
			if _, err := io.WriteString(conn, cvCliOK); err != nil {
				return
			}
		}
	})
	const timeout = 200 * time.Millisecond
	config := DefaultClientConfig()
	config.IdleConnTimeout = timeout
	client := newTestClient(t, config)
	var last time.Time
	for range 3 {
		resp, err := client.Go(mustRequest(t, "GET", u, nil)).Wait()
		if err != nil {
			t.Fatal(err)
		}
		readBody(t, resp)
		last = time.Now()
		time.Sleep(timeout * 3 / 4)
	}
	at := cvCliWait(t, closed, "the idle connection to close")
	// 10ms: last is taken after the body was read, so the client's own mark
	// came a little before it, and a runner's timer granularity is coarse.
	if early := last.Add(timeout).Sub(at); early > 10*time.Millisecond {
		t.Fatalf("closed %v before IdleConnTimeout after it last went idle", early)
	}
}

// With MaxConnsPerHost reached, MaxPendingPerHost requests wait for the
// connection and the next fails at once with ErrTooManyPending; one that
// ended while it waited makes room for another.
func TestCvCliMaxPendingPerHost(t *testing.T) {
	asked := make(chan struct{}, 8)
	release := make(chan struct{})
	u := rawServer(t, func(conn net.Conn, _ int) {
		r := bufio.NewReader(conn)
		for {
			if _, err := stdhttp.ReadRequest(r); err != nil {
				return
			}
			asked <- struct{}{}
			<-release
			if _, err := io.WriteString(conn, cvCliOK); err != nil {
				return
			}
		}
	})
	config := DefaultClientConfig()
	config.MaxConnsPerHost = 1
	config.MaxPendingPerHost = 2
	client := newTestClient(t, config)
	results := make(chan error, 8)
	send := func(ctx context.Context) {
		req := mustRequest(t, "GET", u, nil).WithContext(ctx)
		client.Do(req, func(cr *ClientResponse, err error) { results <- err })
	}
	send(context.Background())
	cvCliWait(t, asked, "the first request to reach the server")
	ctx, cancel := context.WithCancel(context.Background())
	send(ctx)
	send(context.Background())
	send(context.Background())
	if err := cvCliWait(t, results, "the request over the cap"); !errors.Is(err, ErrTooManyPending) {
		t.Fatalf("the request over the cap got %v, want %v", err, ErrTooManyPending)
	}
	cancel()
	if err := cvCliWait(t, results, "the cancelled request"); !errors.Is(err, context.Canceled) {
		t.Fatalf("the cancelled request got %v", err)
	}
	send(context.Background())
	close(release)
	for i := 0; i < 3; i++ {
		if err := cvCliWait(t, results, "the requests let in"); err != nil {
			t.Fatalf("a request let in got %v", err)
		}
	}
}

// An idle HTTP/2 connection is closed once IdleConnTimeout passes.
func TestCvCliIdleConnTimeoutHTTP2(t *testing.T) {
	closed := make(chan struct{})
	var once sync.Once
	ts := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		_, _ = io.WriteString(w, r.Proto)
	}))
	ts.EnableHTTP2 = true
	ts.Config.ConnState = func(_ net.Conn, state stdhttp.ConnState) {
		if state == stdhttp.StateClosed {
			once.Do(func() { close(closed) })
		}
	}
	ts.StartTLS()
	t.Cleanup(ts.Close)
	config := DefaultClientConfig()
	config.TLSConfig = ts.Client().Transport.(*stdhttp.Transport).TLSClientConfig.Clone()
	config.TLSConfig.NextProtos = nil
	config.IdleConnTimeout = 10 * time.Millisecond
	client := newTestClient(t, config)
	resp, err := client.Go(mustRequest(t, "GET", ts.URL, nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "HTTP/2.0" {
		t.Fatalf("proto %q", got)
	}
	cvCliWait(t, closed, "the idle HTTP/2 connection to close")
}

// An HTTP/2 connection whose handshake completes after the client was closed,
// with the request that asked for it already failed, is closed at once.
func TestCvCliHTTP2DialedAfterClose(t *testing.T) {
	serverConfig, clientConfig, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	serverConfig = serverConfig.Clone()
	serverConfig.NextProtos = []string{"h2"}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	accepted, handshake, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		close(accepted)
		<-handshake
		tc := stdtls.Server(conn, serverConfig)
		if tc.Handshake() != nil {
			close(closed)
			return
		}
		cvCliReadUntilEOF(tc, closed)
	}()
	config := DefaultClientConfig()
	config.TLSConfig = clientConfig
	client := NewClient(startClientEngine(t), config)
	future := client.Go(mustRequest(t, "GET", "https://"+listener.Addr().String()+"/", nil))
	cvCliWait(t, accepted, "the dial")
	client.Close()
	if _, err := future.Wait(); !errors.Is(err, ErrClientClosed) {
		t.Fatalf("err = %v, want ErrClientClosed", err)
	}
	close(handshake)
	cvCliWait(t, closed, "the late HTTP/2 connection to close")
}

// A request settled while it waited for a connection is skipped when the
// queue is next matched with connections.
func TestCvCliDispatchSkipsSettledWaiter(t *testing.T) {
	received, release := make(chan struct{}, 4), make(chan struct{})
	u := rawServer(t, func(conn net.Conn, _ int) {
		reader := bufio.NewReader(conn)
		for {
			req, err := stdhttp.ReadRequest(reader)
			if err != nil {
				return
			}
			received <- struct{}{}
			if req.URL.Path == "/hold" {
				<-release
			}
			_, _ = io.WriteString(conn, cvCliOK)
		}
	})
	config := DefaultClientConfig()
	config.MaxConnsPerHost = 1
	client := newTestClient(t, config)
	held := client.Go(mustRequest(t, "GET", u+"/hold", nil))
	cvCliWait(t, received, "the held request")

	ctx, cancel := context.WithCancel(context.Background())
	settled := client.Go(mustRequest(t, "GET", u+"/settled", nil).WithContext(ctx))
	cancel()
	if _, err := settled.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter: %v", err)
	}
	next := client.Go(mustRequest(t, "GET", u+"/next", nil))
	close(release)
	for _, f := range []*Future{held, next} {
		resp, err := f.Wait()
		if err != nil {
			t.Fatal(err)
		}
		if got := readBody(t, resp); got != "ok" {
			t.Fatalf("body %q", got)
		}
	}
}

// Bytes on an idle connection answer nothing, so the connection is closed.
func TestCvCliUnsolicitedBytesCloseIdleConn(t *testing.T) {
	answered, closed := make(chan struct{}), make(chan struct{})
	u := rawServer(t, func(conn net.Conn, i int) {
		reader := bufio.NewReader(conn)
		if _, err := stdhttp.ReadRequest(reader); err != nil {
			return
		}
		_, _ = io.WriteString(conn, cvCliOK)
		if i != 0 {
			return
		}
		<-answered
		_, _ = io.WriteString(conn, "unasked")
		cvCliReadUntilEOF(conn, closed)
	})
	client := newTestClient(t, DefaultClientConfig())
	if _, err := client.Go(mustRequest(t, "GET", u, nil)).Wait(); err != nil {
		t.Fatal(err)
	}
	close(answered)
	cvCliWait(t, closed, "the client to drop the connection")
	// The next request dials afresh and is answered.
	if _, err := client.Go(mustRequest(t, "GET", u, nil)).Wait(); err != nil {
		t.Fatal(err)
	}
}

// A streaming callback that gives the response up while the rest of it is in
// the same read leaves the connection closed, not kept.
func TestCvCliStreamGivenUpInCallbackDropsConn(t *testing.T) {
	closed := make(chan struct{})
	u := rawServer(t, func(conn net.Conn, i int) {
		if _, err := stdhttp.ReadRequest(bufio.NewReader(conn)); err != nil {
			return
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n0123456789")
		if i == 0 {
			cvCliReadUntilEOF(conn, closed)
		}
	})
	client := newTestClient(t, streamConfig(0))
	called := make(chan struct{})
	client.Do(mustRequest(t, "GET", u, nil), func(cr *ClientResponse, err error) {
		if err == nil {
			cr.Close()
		}
		close(called)
	})
	cvCliWait(t, called, "the callback")
	cvCliWait(t, closed, "the connection to close")
	client.mu.Lock()
	defer client.mu.Unlock()
	for _, h := range client.hosts {
		if len(h.idle) != 0 {
			t.Fatal("a given up connection was kept")
		}
	}
}

// The request's own state guards: a settled request neither begins a stream
// nor attaches to a connection, an abort with no connection only fails it,
// and a streamed body without GetBody is not sent twice.
func TestCvCliClientRequestGuards(t *testing.T) {
	var got error
	r := &clientRequest{req: mustRequest(t, "GET", "http://x/", nil), callback: func(_ *ClientResponse, err error) { got = err }}
	r.abort(errRequestTimeout)
	if !errors.Is(got, errRequestTimeout) {
		t.Fatalf("abort: %v", got)
	}
	if r.begin(nil) {
		t.Fatal("a settled request began a stream")
	}
	if r.attach(&clientConn{}) {
		t.Fatal("a settled request attached to a connection")
	}
	r.abort(errRequestTimeout) // a second abort finds it settled

	req := mustRequest(t, "GET", "http://x/", nil)
	req.Body = newCvCliBody()
	up := &clientRequest{req: req, upload: reqbody.NewSource(req)}
	defer up.upload.Close()
	if up.retryable() {
		t.Fatal("a GET whose streamed body has no GetBody is retryable")
	}
}

// send hands a request back to the queue when the connection closed in the
// meantime, and gives a connection on to the next request when the one it was
// to carry settled first; release ignores a closed connection.
func TestCvCliSendGuards(t *testing.T) {
	config := DefaultClientConfig()
	config.IdleConnTimeout = 0
	c := NewClient(nil, config)
	h := &hostPool{target: hostTarget{addr: "x:80"}}

	settled := &clientRequest{req: mustRequest(t, "GET", "http://x/", nil), callback: func(*ClientResponse, error) {}}
	settled.done.Store(true)
	cc := &clientConn{client: c, host: h}
	cc.send(settled)
	if len(h.idle) != 1 || h.idle[0] != cc || cc.current != nil {
		t.Fatalf("connection not kept idle: %d idle", len(h.idle))
	}

	closedConn := &clientConn{client: c, host: h, closed: true}
	c.release(closedConn)
	if len(h.idle) != 1 {
		t.Fatal("a closed connection was kept")
	}

	c.Close()
	var got error
	r := &clientRequest{req: mustRequest(t, "GET", "http://x/", nil), callback: func(_ *ClientResponse, err error) { got = err }}
	closedConn.send(r)
	if !errors.Is(got, ErrClientClosed) {
		t.Fatalf("request on a closed connection of a closed client: %v", got)
	}

	// Callbacks the engine and the parser make that have nothing to act on.
	cc = &clientConn{}
	cc.OnPriorityData(nil, nil)
	cc.bodyStart(&stdhttp.Response{})
	cc.bodyEnd()
}

// A whole response's OnBody: nil is ignored, one registered inside the
// callback is served when it returns, one registered after it on a goroutine
// of its own, and once delivered the body is not delivered again. Consumed
// follows.
func TestCvCliDeliverResponseOnBody(t *testing.T) {
	if NewBufferedBody(nil) != stdhttp.NoBody {
		t.Fatal("an empty buffered body is not NoBody")
	}
	body := NewBufferedBody([]byte("abc"))
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}

	var cr *ClientResponse
	var got []string
	DeliverResponse(&stdhttp.Response{Body: NewBufferedBody([]byte("abc"))}, nil, func(r *ClientResponse, _ error) {
		cr = r
		r.OnBody(nil)
		if r.Consumed() != 0 {
			t.Error("consumed before delivery")
		}
		r.OnBody(func(data []byte, fin bool, err error) { got = append(got, string(data)) })
	})
	if len(got) != 1 || got[0] != "abc" || cr.Consumed() != 3 {
		t.Fatalf("delivered %q, consumed %d", got, cr.Consumed())
	}
	cr.OnBody(func([]byte, bool, error) { t.Error("delivered twice") })
	cr.deliverWhole()

	// Registered late, on a body that is not a buffered one.
	late := make(chan string, 1)
	DeliverResponse(&stdhttp.Response{Body: io.NopCloser(strings.NewReader("xyz"))}, nil, func(r *ClientResponse, _ error) { cr = r })
	cr.OnBody(func(data []byte, fin bool, err error) { late <- string(data) })
	if got := cvCliWait(t, late, "the late OnBody"); got != "xyz" {
		t.Fatalf("late OnBody got %q", got)
	}
}

// Consumed on a whole response whose body is not a buffered one, such as an
// empty body (NoBody), once OnBody has delivered it.
func TestCvCliConsumedAfterEmptyBodyDelivered(t *testing.T) {
	var cr *ClientResponse
	DeliverResponse(&stdhttp.Response{Body: stdhttp.NoBody}, nil, func(r *ClientResponse, _ error) {
		cr = r
		r.OnBody(func([]byte, bool, error) {})
	})
	if n := cr.Consumed(); n != 0 {
		t.Fatalf("consumed %d", n)
	}
	DeliverResponse(&stdhttp.Response{Body: io.NopCloser(strings.NewReader("xyz"))}, nil, func(r *ClientResponse, _ error) {
		cr = r
		r.OnBody(func([]byte, bool, error) {})
	})
	if n := cr.Consumed(); n != 3 {
		t.Fatalf("consumed %d of a 3-byte body", n)
	}
}

// cvCliStream builds a ResponseStream with hooks that record what it did.
type cvCliStream struct {
	s       *ResponseStream
	mu      sync.Mutex
	begin   bool
	holds   []bool
	whole   *stdhttp.Response
	aborted error
	called  chan *ClientResponse
	data    []byte
	fin     chan error
	onCall  func(*ClientResponse)
}

func newCvCliStream(threshold int64, buffer int, detached, begin bool, onCall func(*ClientResponse)) *cvCliStream {
	cs := &cvCliStream{begin: begin, called: make(chan *ClientResponse, 1), fin: make(chan error, 1), onCall: onCall}
	head := &stdhttp.Response{StatusCode: 200, ContentLength: -1, Header: stdhttp.Header{}}
	req := &stdhttp.Request{Method: "GET"}
	cs.s = NewResponseStream(head, req, StreamOptions{Threshold: threshold, Buffer: buffer, Detached: detached}, StreamHooks{
		Begin: func(*ResponseStream) bool { return cs.begin },
		Callback: func(cr *ClientResponse) {
			if cs.onCall != nil {
				cs.onCall(cr)
			}
			cs.called <- cr
		},
		Whole: func(resp *stdhttp.Response) { cs.whole = resp },
		Done:  func() bool { return true },
		Abort: func(err error) { cs.mu.Lock(); cs.aborted = err; cs.mu.Unlock() },
		Hold:  func(hold bool) { cs.mu.Lock(); cs.holds = append(cs.holds, hold); cs.mu.Unlock() },
	})
	return cs
}

func (cs *cvCliStream) take(data []byte, fin bool, err error) {
	cs.mu.Lock()
	cs.data = append(cs.data, data...)
	cs.mu.Unlock()
	if fin {
		cs.fin <- err
	}
}

// The default buffer applies when none is given, a request that has ended
// runs no callback, and a body that ends before the threshold with nothing
// in it is delivered whole as NoBody.
func TestCvCliStreamStartAndWhole(t *testing.T) {
	cs := newCvCliStream(0, 0, false, false, nil)
	if cs.s.highWater != DefaultStreamResponseBodyBuffer {
		t.Fatalf("highWater %d", cs.s.highWater)
	}
	cs.s.Start()
	select {
	case <-cs.called:
		t.Fatal("callback ran for an ended request")
	default:
	}
	if err := cs.s.cr.Response.Body.Close(); err != nil {
		t.Fatal(err)
	}

	cs = newCvCliStream(100, 0, false, true, nil)
	cs.s.Start()
	cs.s.End()
	if cs.whole == nil || cs.whole.Body != stdhttp.NoBody {
		t.Fatalf("whole %+v", cs.whole)
	}
}

// A detached stream hands its body to OnBody's function on a goroutine of its
// own, whether the function was registered inside the callback or after it,
// and drops data that arrives after the end.
func TestCvCliStreamDetached(t *testing.T) {
	for _, late := range []bool{false, true} {
		var cs *cvCliStream
		cs = newCvCliStream(0, 0, true, true, func(cr *ClientResponse) {
			// Data while the callback runs waits for it to return.
			cs.s.Data([]byte("a"))
			if late {
				if _, err := cr.Body.Read(make([]byte, 1)); err != nil {
					t.Errorf("read inside the callback: %v", err)
				}
				return
			}
			cr.OnBody(cs.take)
		})
		cs.s.Start()
		cr := cvCliWait(t, cs.called, "the callback")
		if late {
			cr.OnBody(cs.take)
		}
		cs.s.Data([]byte("bc"))
		cs.s.End()
		cs.s.Data([]byte("late"))
		if err := cvCliWait(t, cs.fin, "the end of the body"); err != nil {
			t.Fatal(err)
		}
		cs.mu.Lock()
		got := string(cs.data)
		cs.mu.Unlock()
		want := "abc"
		if late {
			want = "bc"
		}
		// The byte read inside the callback counts as consumed too.
		if got != want || cr.Consumed() != 3 {
			t.Fatalf("late=%v: got %q, consumed %d", late, got, cr.Consumed())
		}
	}
}

// OnBody registered after a callback that read the body is served from the
// pool; one registered after a callback that left the body is told it was
// abandoned, and so is a Read.
func TestCvCliStreamLateOnBody(t *testing.T) {
	cs := newCvCliStream(0, 0, false, true, func(cr *ClientResponse) {
		_, _ = cr.Body.Read(make([]byte, 1))
	})
	cs.s.Start()
	cr := cvCliWait(t, cs.called, "the callback")
	cs.s.Data([]byte("xy"))
	cr.OnBody(cs.take)
	cs.s.End()
	if err := cvCliWait(t, cs.fin, "the end of the body"); err != nil {
		t.Fatal(err)
	}
	if _, err := cr.Body.Read(make([]byte, 1)); !errors.Is(err, ErrBodyTaken) {
		t.Fatalf("Read after OnBody: %v", err)
	}

	cs = newCvCliStream(0, 0, false, true, nil)
	cs.s.Start()
	cr = cvCliWait(t, cs.called, "the callback")
	if _, err := cr.Body.Read(make([]byte, 1)); !errors.Is(err, ErrResponseAbandoned) {
		t.Fatalf("Read of an abandoned body: %v", err)
	}
	cr.OnBody(cs.take)
	if err := cvCliWait(t, cs.fin, "the abandonment"); !errors.Is(err, ErrResponseAbandoned) {
		t.Fatalf("OnBody of an abandoned body: %v", err)
	}
}

// Reads held for a slow reader resume when the body ends, and a failed body
// reports its error to Read.
func TestCvCliStreamHoldAndFail(t *testing.T) {
	cs := newCvCliStream(0, 4, false, true, func(cr *ClientResponse) {
		_, _ = cr.Body.Read(make([]byte, 1))
	})
	cs.s.Start()
	cvCliWait(t, cs.called, "the callback")
	cs.s.Data([]byte("0123456789"))
	cs.s.End()
	cs.mu.Lock()
	holds := append([]bool(nil), cs.holds...)
	cs.mu.Unlock()
	if len(holds) != 2 || !holds[0] || holds[1] {
		t.Fatalf("holds %v", holds)
	}

	cs = newCvCliStream(0, 0, false, true, func(cr *ClientResponse) {
		_, _ = cr.Body.Read(make([]byte, 1))
	})
	cs.s.Start()
	cr := cvCliWait(t, cs.called, "the callback")
	boom := errors.New("cvcli: boom")
	cs.s.Fail(boom)
	if _, err := cr.Body.Read(make([]byte, 1)); !errors.Is(err, boom) {
		t.Fatalf("Read of a failed body: %v", err)
	}
}

// cvCliSink records what a parser streams.
type cvCliSink struct {
	starts, ends int
	data         []byte
}

func (s *cvCliSink) bodyStart(*stdhttp.Response) { s.starts++ }
func (s *cvCliSink) bodyData(b []byte)           { s.data = append(s.data, b...) }
func (s *cvCliSink) bodyEnd()                    { s.ends++ }

// The response parser's limits and refusals, buffered and streamed.
func TestCvCliResponseParserErrors(t *testing.T) {
	get := &stdhttp.Request{Method: "GET"}
	for _, tc := range []struct {
		name     string
		stream   bool
		input    string
		want     error
		contains string
	}{
		{"header without end too large", false, "HTTP/1.1 200 OK\r\nX: " + strings.Repeat("a", 80), ErrResponseHeaderTooLarge, ""},
		{"header too large", false, "HTTP/1.1 200 OK\r\nX: " + strings.Repeat("a", 80) + "\r\n\r\n", ErrResponseHeaderTooLarge, ""},
		{"content length too large", false, "HTTP/1.1 200 OK\r\nContent-Length: 17\r\n\r\n", ErrResponseBodyTooLarge, ""},
		{"until close too large", false, "HTTP/1.0 200 OK\r\n\r\n" + strings.Repeat("b", 17), ErrResponseBodyTooLarge, ""},
		{"chunk size net/http refuses", false, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n 1\r\nb\r\n0\r\n\r\n", ErrMalformedResponse, ""},
		{"unsupported transfer coding", false, "HTTP/1.1 200 OK\r\nTransfer-Encoding: br\r\n\r\n", ErrMalformedResponse, "transfer coding"},
		{"streamed bad chunk size", true, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n", ErrMalformedResponse, ""},
		{"streamed bad trailer", true, "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nno colon here\r\n\r\n", ErrMalformedResponse, ""},
		{"streamed until close too large", true, "HTTP/1.0 200 OK\r\n\r\n" + strings.Repeat("b", 9), ErrResponseBodyTooLarge, ""},
	} {
		p := &responseParser{maxHeader: 64, maxBody: 16}
		if tc.stream {
			p.sink, p.streamOK, p.maxStreamed = &cvCliSink{}, true, 8
		}
		resp, err := p.feed([]byte(tc.input), get)
		if resp != nil || !errors.Is(err, tc.want) || !strings.Contains(err.Error(), tc.contains) {
			t.Errorf("%s: %v, %v", tc.name, resp, err)
		}
	}

	// finish on a response that is not ended by the close has nothing.
	p := &responseParser{maxHeader: 64, maxBody: 16}
	if resp, err := p.finish(); resp != nil || err != nil {
		t.Fatalf("finish without a response: %v, %v", resp, err)
	}
	// A large decode buffer is let go between responses.
	p.scratch = make([]byte, maxRetainedBuffer+1)
	p.reset()
	if p.scratch != nil {
		t.Fatal("large scratch kept")
	}
}

// clientParseError speaks of responses, and passes other errors on.
func TestCvCliClientParseError(t *testing.T) {
	if err := clientParseError(ErrHeaderTooLarge); err != ErrResponseHeaderTooLarge {
		t.Fatal(err)
	}
	if err := clientParseError(ErrBodyTooLarge); err != ErrResponseBodyTooLarge {
		t.Fatal(err)
	}
	other := errors.New("cvcli: other")
	if err := clientParseError(other); err != other {
		t.Fatal(err)
	}
}

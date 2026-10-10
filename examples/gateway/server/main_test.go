//go:build linux || darwin || windows

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/examples/gateway/server/proxy"
	"github.com/lesismal/fib/examples/gateway/upstream/backend"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
	"github.com/lesismal/fib/tlstest"
	"github.com/lesismal/fib/websocket"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

// freePort finds a port that is free for both TCP and UDP, since HTTPS and
// HTTP/3 share one number.
func freePort(t *testing.T) int {
	t.Helper()
	for i := 0; i < 20; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := l.Addr().(*net.TCPAddr).Port
		pc, err := net.ListenPacket("udp", fmt.Sprintf("127.0.0.1:%d", port))
		_ = l.Close()
		if err == nil {
			_ = pc.Close()
			return port
		}
	}
	t.Fatal("no port free for both TCP and UDP")
	return 0
}

func start(t *testing.T, engines ...*fib.Engine) {
	t.Helper()
	for _, engine := range engines {
		done := make(chan error, 1)
		go func() { done <- engine.Run() }()
		t.Cleanup(func() {
			engine.Stop()
			if err := <-done; err != nil {
				t.Error(err)
			}
			_ = engine.Close()
		})
	}
}

// lab is a gateway with an upstream behind it, and the clients to drive it.
type lab struct {
	// tlsAddr and plainAddr are the gateway's.
	tlsAddr, plainAddr string
	clients            []namedClient
	tlsConfig          *tls.Config
	engine             *fib.Engine
	// canceled is told of each /hang request the upstream sees abandoned.
	canceled chan string
}

type namedClient struct {
	name string
	doer doer
}

type doer interface {
	Do(req *stdhttp.Request, callback func(*fibhttp.ClientResponse, error))
}

// upstreamExtras answers the paths the tests need on top of the backend's.
func (l *lab) upstreamExtras(next fibhttp.HandlerFunc) fibhttp.HandlerFunc {
	return func(c *fibhttp.Context) {
		switch c.Request.URL.Path {
		case "/hang":
			// Never answers, and says so when the request is abandoned.
			c.Retain()
			c.OnCancel(func(error) {
				l.canceled <- c.Request.Proto
				c.Release()
			})
		case "/headers":
			var b strings.Builder
			for name, values := range c.Request.Header {
				fmt.Fprintf(&b, "%s: %s\n", name, strings.Join(values, ","))
			}
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(b.String()))
		case "/cut":
			// Promises a megabyte and hangs up after a little.
			c.Header().Set("Content-Length", "1048576")
			_, _ = c.Write(bytes.Repeat([]byte("x"), 100<<10))
			c.Flush()
			if c.Request.ProtoMajor == 1 {
				c.Conn.Close()
			}
		default:
			next(c)
		}
	}
}

type labOptions struct {
	timeout        time.Duration
	maxRequestBody int64
}

func newLab(t *testing.T, options labOptions) *lab {
	t.Helper()
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	// The gateway's and the clients' dials handshake without a worker.
	clientTLS.MinVersion = tls.VersionTLS13
	l := &lab{tlsConfig: clientTLS, canceled: make(chan string, 16)}

	upPort, upPlain := freePort(t), freePort(t)
	upstream, err := backend.NewEnginesFor(l.upstreamExtras(backend.DefaultHandler()), serverTLS,
		fmt.Sprintf("127.0.0.1:%d", upPort), fmt.Sprintf("127.0.0.1:%d", upPlain))
	if err != nil {
		t.Fatal(err)
	}
	start(t, upstream...)

	var routes []proxy.Route
	for _, spec := range []string{
		fmt.Sprintf("/h1=http://127.0.0.1:%d", upPlain),
		fmt.Sprintf("/h2=https://127.0.0.1:%d", upPort),
		fmt.Sprintf("/h3=h3://127.0.0.1:%d", upPort),
		"/dead=http://127.0.0.1:1",
	} {
		route, err := proxy.ParseRoute(spec)
		if err != nil {
			t.Fatal(err)
		}
		routes = append(routes, route)
	}
	gwPort, gwPlain := freePort(t), freePort(t)
	gateway, err := proxy.New(proxy.Config{
		Routes:         routes,
		TLSConfig:      clientTLS,
		Timeout:        options.timeout,
		MaxRequestBody: options.maxRequestBody,
		Subprotocols:   []string{"chat", "superchat"},
		AltSvc:         http3.AltSvc(gwPort),
	})
	if err != nil {
		t.Fatal(err)
	}
	l.tlsAddr, l.plainAddr = fmt.Sprintf("127.0.0.1:%d", gwPort), fmt.Sprintf("127.0.0.1:%d", gwPlain)
	engines, err := newEngines(gateway, serverTLS, l.tlsAddr, l.plainAddr)
	if err != nil {
		t.Fatal(err)
	}
	gateway.Attach(engines[0])
	start(t, engines...)
	// Registered last, so it runs first: the upstream clients close before the
	// engines they dial through.
	t.Cleanup(gateway.Close)

	// The clients dial through an engine of their own.
	clientEngine, err := fib.NewEngine(fib.DefaultConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	start(t, clientEngine)
	l.engine = clientEngine
	httpConfig := fibhttp.DefaultClientConfig()
	httpConfig.TLSConfig = clientTLS
	httpConfig.StreamResponseBody = true
	httpConfig.DisableHTTP2 = true
	h1 := fibhttp.NewClient(clientEngine, httpConfig)
	httpConfig.DisableHTTP2 = false
	h2 := fibhttp.NewClient(clientEngine, httpConfig)
	h3Config := http3.DefaultClientConfig()
	h3Config.TLSConfig = clientTLS
	h3Config.StreamResponseBody = true
	h3 := http3.NewClient(clientEngine, h3Config)
	t.Cleanup(h1.Close)
	t.Cleanup(h2.Close)
	t.Cleanup(h3.Close)
	l.clients = []namedClient{{"HTTP/1.1", h1}, {"HTTP/2", h2}, {"HTTP/3", h3}}
	return l
}

func (l *lab) url(path string) string { return "https://" + l.tlsAddr + path }

// reply is one response, collected from the callbacks.
type reply struct {
	status  int
	proto   string
	header  stdhttp.Header
	trailer stdhttp.Header
	body    []byte
	pieces  int
	err     error
}

// get sends req and waits for the whole response, taking its body with OnBody.
func get(t *testing.T, d doer, req *stdhttp.Request) reply {
	t.Helper()
	var r reply
	done := make(chan struct{})
	d.Do(req, func(resp *fibhttp.ClientResponse, err error) {
		if err != nil {
			r.err = err
			close(done)
			return
		}
		r.status, r.proto, r.header = resp.StatusCode, resp.Proto, resp.Header
		resp.OnBody(func(data []byte, fin bool, err error) {
			if err != nil {
				r.err = err
			}
			if len(data) > 0 {
				r.pieces++
				r.body = append(r.body, data...)
			}
			if fin {
				r.trailer = resp.Trailer()
				close(done)
			}
		})
	})
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out")
	}
	return r
}

func request(t *testing.T, method, url string, body []byte) *stdhttp.Request {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := stdhttp.NewRequest(method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func pattern(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte('a' + i%26)
	}
	return out
}

// TestEveryProtocolToEveryUpstream sends the same requests in over HTTP/1.1,
// HTTP/2 and HTTP/3, out over each, and checks what the upstream saw and what
// came back.
func TestEveryProtocolToEveryUpstream(t *testing.T) {
	l := newLab(t, labOptions{})
	const size = 3 << 20
	for _, client := range l.clients {
		for _, upstream := range []string{"h1", "h2", "h3"} {
			t.Run(client.name+"/"+upstream, func(t *testing.T) {
				base := l.url("/" + upstream)

				r := get(t, client.doer, request(t, "POST", base+"/echo?x=1", []byte("hello gateway")))
				wantProto := map[string]string{"h1": "HTTP/1.1", "h2": "HTTP/2.0", "h3": "HTTP/3.0"}[upstream]
				if r.err != nil || r.status != 200 || !strings.HasPrefix(string(r.body), wantProto+" POST /echo?x=1\n") ||
					!strings.Contains(string(r.body), "body: hello gateway") ||
					!strings.Contains(string(r.body), "X-Forwarded-For: 127.0.0.1") ||
					!strings.Contains(string(r.body), "X-Forwarded-Proto: https") {
					t.Fatalf("echo: err %v, status %d, body %q (upstream should have been %s)", r.err, r.status, r.body, wantProto)
				}

				r = get(t, client.doer, request(t, "GET", fmt.Sprintf("%s/download?size=%d", base, size), nil))
				if r.err != nil || r.status != 200 || !bytes.Equal(r.body, pattern(size)) {
					t.Fatalf("download: err %v, status %d, %d bytes", r.err, r.status, len(r.body))
				}
				if r.pieces < 2 {
					t.Fatalf("download arrived in %d piece", r.pieces)
				}

				start := time.Now()
				r = get(t, client.doer, request(t, "GET", base+"/stream?n=4&delay=100ms", nil))
				if r.err != nil || string(r.body) != "line 1 of 4\nline 2 of 4\nline 3 of 4\nline 4 of 4\n" {
					t.Fatalf("stream: err %v, body %q", r.err, r.body)
				}
				if r.trailer.Get("X-Lines") != "4" {
					t.Fatalf("trailer %v", r.trailer)
				}
				if r.pieces < 2 || time.Since(start) < 300*time.Millisecond {
					t.Fatalf("stream arrived in %d pieces over %v: it was not relayed as it was sent", r.pieces, time.Since(start))
				}
			})
		}
	}
}

// TestLargeUpload sends bodies the gateway takes with OnBody, which it then
// forwards whole, on every protocol.
func TestLargeUpload(t *testing.T) {
	l := newLab(t, labOptions{})
	body := pattern(6 << 20)
	sum := sha256.Sum256(body)
	want := fmt.Sprintf("received %d bytes sha256=%s", len(body), hex.EncodeToString(sum[:]))
	for _, client := range l.clients {
		for _, upstream := range []string{"h1", "h2", "h3"} {
			t.Run(client.name+"/"+upstream, func(t *testing.T) {
				r := get(t, client.doer, request(t, "POST", l.url("/"+upstream+"/upload"), body))
				if r.err != nil || r.status != 200 || !strings.Contains(string(r.body), want) {
					t.Fatalf("err %v, status %d, body %q", r.err, r.status, r.body)
				}
			})
		}
	}
}

func TestRequestBodyTooLarge(t *testing.T) {
	l := newLab(t, labOptions{maxRequestBody: 1 << 20})
	for _, client := range l.clients {
		t.Run(client.name, func(t *testing.T) {
			r := get(t, client.doer, request(t, "POST", l.url("/h1/upload"), pattern(2<<20)))
			if r.err != nil || r.status != stdhttp.StatusRequestEntityTooLarge {
				t.Fatalf("err %v, status %d", r.err, r.status)
			}
		})
	}
}

func TestHeadersAndAltSvc(t *testing.T) {
	l := newLab(t, labOptions{})
	for _, client := range l.clients {
		t.Run(client.name, func(t *testing.T) {
			req := request(t, "GET", l.url("/h1/headers"), nil)
			req.Header.Set("X-Custom", "kept")
			req.Header.Set("Connection", "X-Hop")
			req.Header.Set("X-Hop", "dropped")
			req.Header.Set("Keep-Alive", "timeout=5")
			r := get(t, client.doer, req)
			if r.err != nil || r.status != 200 {
				t.Fatalf("err %v, status %d", r.err, r.status)
			}
			seen := string(r.body)
			if !strings.Contains(seen, "X-Custom: kept") || !strings.Contains(seen, "Via: ") {
				t.Fatalf("upstream saw %q", seen)
			}
			// A Connection header names headers for one hop only. HTTP/2 and
			// HTTP/3 have no such header, so only an HTTP/1.1 client can send
			// the pair; Keep-Alive is dropped for all.
			if strings.Contains(seen, "Keep-Alive:") || client.name == "HTTP/1.1" && strings.Contains(seen, "X-Hop:") {
				t.Fatalf("a hop-by-hop header reached the upstream: %q", seen)
			}
			altSvc := r.header.Get("Alt-Svc")
			if client.name == "HTTP/3" && altSvc != "" || client.name != "HTTP/3" && altSvc == "" {
				t.Fatalf("Alt-Svc %q on a %s response", altSvc, client.name)
			}
		})
	}
}

func TestUnknownRouteAndBadGateway(t *testing.T) {
	l := newLab(t, labOptions{})
	for _, client := range l.clients {
		t.Run(client.name, func(t *testing.T) {
			if r := get(t, client.doer, request(t, "GET", l.url("/nowhere"), nil)); r.err != nil || r.status != 404 {
				t.Fatalf("unknown route: err %v, status %d", r.err, r.status)
			}
			if r := get(t, client.doer, request(t, "GET", l.url("/dead/echo"), nil)); r.err != nil || r.status != 502 {
				t.Fatalf("dead upstream: err %v, status %d", r.err, r.status)
			}
			if r := get(t, client.doer, request(t, "GET", l.url("/h2/status?code=418"), nil)); r.err != nil || r.status != 418 {
				t.Fatalf("status passthrough: err %v, status %d", r.err, r.status)
			}
		})
	}
}

func TestUpstreamTimeout(t *testing.T) {
	l := newLab(t, labOptions{timeout: 300 * time.Millisecond})
	for _, client := range l.clients {
		t.Run(client.name, func(t *testing.T) {
			r := get(t, client.doer, request(t, "GET", l.url("/h1/hang"), nil))
			if r.err != nil || r.status != stdhttp.StatusGatewayTimeout {
				t.Fatalf("err %v, status %d", r.err, r.status)
			}
		})
	}
}

// TestClientCancelReachesUpstream checks that a client that gives up cancels
// the request the gateway made for it.
func TestClientCancelReachesUpstream(t *testing.T) {
	l := newLab(t, labOptions{})
	for _, client := range l.clients {
		for _, upstream := range []string{"h1", "h2", "h3"} {
			t.Run(client.name+"/"+upstream, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				req := request(t, "GET", l.url("/"+upstream+"/hang"), nil).WithContext(ctx)
				done := make(chan error, 1)
				client.doer.Do(req, func(_ *fibhttp.ClientResponse, err error) { done <- err })
				time.Sleep(300 * time.Millisecond)
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("client: %v", err)
				}
				select {
				case <-l.canceled:
				case <-time.After(5 * time.Second):
					t.Fatal("the upstream never saw the request abandoned")
				}
			})
		}
	}
}

// TestUpstreamFailsMidBody checks that a response that stops short does not
// look complete to the client.
func TestUpstreamFailsMidBody(t *testing.T) {
	l := newLab(t, labOptions{})
	for _, client := range l.clients {
		t.Run(client.name, func(t *testing.T) {
			r := get(t, client.doer, request(t, "GET", l.url("/h1/cut"), nil))
			t.Logf("err %v, %d bytes, trailer %v", r.err, len(r.body), r.trailer)
			if r.err == nil {
				t.Fatalf("a response cut short arrived as complete: %d of %d bytes", len(r.body), 1<<20)
			}
		})
	}
}

func TestConcurrentDownloads(t *testing.T) {
	l := newLab(t, labOptions{})
	const size = 1 << 20
	for _, client := range l.clients {
		t.Run(client.name, func(t *testing.T) {
			var wg sync.WaitGroup
			for i := 0; i < 40; i++ {
				upstream := []string{"h1", "h2", "h3"}[i%3]
				wg.Add(1)
				go func() {
					defer wg.Done()
					r := get(t, client.doer, request(t, "GET", fmt.Sprintf("%s/download?size=%d", l.url("/"+upstream), size), nil))
					if r.err != nil || !bytes.Equal(r.body, pattern(size)) {
						t.Errorf("%s: err %v, %d bytes", upstream, r.err, len(r.body))
					}
				}()
			}
			wg.Wait()
		})
	}
}

// wsClient opens a WebSocket and collects what it receives.
type wsClient struct {
	conn     *websocket.Connection
	messages chan []byte
	closed   chan string
}

func (l *lab) dialWebSocket(t *testing.T, url string, subprotocols ...string) (*wsClient, error) {
	t.Helper()
	config := websocket.DefaultDialerConfig()
	config.HandshakeTimeout = 5 * time.Second
	config.Subprotocols = subprotocols
	config.TLSConfig = l.tlsConfig
	c := &wsClient{messages: make(chan []byte, 8), closed: make(chan string, 1)}
	type result struct {
		conn *websocket.Connection
		err  error
	}
	done := make(chan result, 1)
	websocket.NewDialer(l.engine, config).Dial(url, nil, websocket.HandlerFuncs{
		Message: func(_ *websocket.Connection, _ websocket.Opcode, data []byte) {
			c.messages <- append([]byte(nil), data...)
		},
		Close: func(_ *websocket.Connection, code uint16, reason string, err error) {
			c.closed <- fmt.Sprintf("%d %s", code, reason)
		},
	}, func(conn *websocket.Connection, _ *stdhttp.Response, err error) { done <- result{conn, err} })
	r := <-done
	c.conn = r.conn
	return c, r.err
}

func (c *wsClient) expect(t *testing.T, want []byte) {
	t.Helper()
	select {
	case got := <-c.messages:
		if !bytes.Equal(got, want) {
			t.Fatalf("echoed %d bytes, want %d", len(got), len(want))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no echo")
	}
}

func TestWebSocketRelay(t *testing.T) {
	l := newLab(t, labOptions{})
	for _, scheme := range []string{"ws", "wss"} {
		for _, upstream := range []string{"h1", "h2", "h3"} {
			t.Run(scheme+"/"+upstream, func(t *testing.T) {
				addr := l.plainAddr
				if scheme == "wss" {
					addr = l.tlsAddr
				}
				c, err := l.dialWebSocket(t, scheme+"://"+addr+"/"+upstream+"/ws", "nope", "superchat")
				if err != nil {
					t.Fatal(err)
				}
				if got := c.conn.Subprotocol(); got != "superchat" {
					t.Fatalf("subprotocol %q", got)
				}
				// Sent at once, before the gateway need have reached the upstream.
				c.conn.WriteText("first")
				c.expect(t, []byte("first"))
				big := bytes.Repeat([]byte("0123456789abcdef"), 64<<10)
				c.conn.WriteBinary(big)
				c.expect(t, big)

				// The upstream closing is relayed with its code and reason.
				c.conn.WriteText("close")
				select {
				case got := <-c.closed:
					if got != "4002 upstream says bye" {
						t.Fatalf("closed %q", got)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("the upstream's close never arrived")
				}
			})
		}
	}
}

func TestWebSocketClientClose(t *testing.T) {
	l := newLab(t, labOptions{})
	c, err := l.dialWebSocket(t, "wss://"+l.tlsAddr+"/h1/ws")
	if err != nil {
		t.Fatal(err)
	}
	c.conn.WriteText("hi")
	c.expect(t, []byte("hi"))
	_ = c.conn.Close(4001, "bye")
	select {
	case got := <-c.closed:
		if !strings.HasPrefix(got, "4001 ") {
			t.Fatalf("closed %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("never closed")
	}
}

func TestWebSocketUpstreamUnreachable(t *testing.T) {
	l := newLab(t, labOptions{})
	c, err := l.dialWebSocket(t, "wss://"+l.tlsAddr+"/dead/ws")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-c.closed:
		if !strings.HasPrefix(got, "1014 ") {
			t.Fatalf("closed %q, want 1014 bad gateway", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the client was left connected to a gateway with no upstream")
	}
}

// TestSlowClient downloads through the gateway from a client that takes its
// body slower than the upstream sends it, which the gateway has to hold back
// by flow control rather than buffer or drop.
func TestSlowClient(t *testing.T) {
	l := newLab(t, labOptions{})
	const size = 8 << 20
	for _, client := range l.clients {
		for _, upstream := range []string{"h1", "h2", "h3"} {
			t.Run(client.name+"/"+upstream, func(t *testing.T) {
				req := request(t, "GET", fmt.Sprintf("%s/download?size=%d", l.url("/"+upstream), size), nil)
				var got int64
				var bad error
				done := make(chan error, 1)
				client.doer.Do(req, func(resp *fibhttp.ClientResponse, err error) {
					if err != nil {
						done <- err
						return
					}
					resp.OnBody(func(data []byte, fin bool, err error) {
						if err != nil {
							done <- err
							return
						}
						for i, b := range data {
							if b != byte('a'+(got+int64(i))%26) && bad == nil {
								bad = fmt.Errorf("byte %d is %q", got+int64(i), b)
							}
						}
						got += int64(len(data))
						time.Sleep(200 * time.Microsecond)
						if fin {
							done <- bad
						}
					})
				})
				select {
				case err := <-done:
					if err != nil || got != size {
						t.Fatalf("err %v, %d of %d bytes", err, got, size)
					}
				case <-time.After(30 * time.Second):
					t.Fatalf("stalled at %d of %d bytes", got, size)
				}
			})
		}
	}
}

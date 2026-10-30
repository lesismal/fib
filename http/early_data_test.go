//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	stdtls "crypto/tls"
	stdhttp "net/http"
	"strings"
	"sync"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/hpack"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// earlyServer starts a TLS HTTP server configured by configure on top of
// DefaultConfig, and returns its address.
func earlyServer(t *testing.T, configure func(*Config), handler Handler) string {
	t.Helper()
	config := DefaultConfig()
	if configure != nil {
		configure(&config)
	}
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	serverTLS.MinVersion = stdtls.VersionTLS13
	return serve(t, fibtls.NewServer(ConfigureTLS(serverTLS), NewHandlerWithConfig(config, handler)))
}

// earlyRawClient dials with a session cache and early data enabled, and sends
// request bytes as soon as the connection is open, before its handshake.
type earlyRawClient struct {
	engine    *fib.Engine
	tlsConfig *stdtls.Config
	cache     fibtls.QUICSessionCache
}

func newEarlyRawClient(t *testing.T, serverName string) *earlyRawClient {
	t.Helper()
	_, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	clientTLS.MinVersion = stdtls.VersionTLS13
	clientTLS.ServerName = serverName
	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		<-runDone
		_ = engine.Close()
	})
	return &earlyRawClient{engine: engine, tlsConfig: clientTLS, cache: fibtls.NewQUICSessionCache(4)}
}

// waitSession waits for the server's session ticket to reach the cache.
func (c *earlyRawClient) waitSession(t *testing.T) {
	t.Helper()
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if c.cache.Get(c.tlsConfig.ServerName) != nil {
			return
		}
	}
	t.Fatal("no session ticket")
}

// exchange sends req as the connection's first bytes and returns what the
// server answers, up to want, a moment after want first holds, or the
// connection's close.
func (c *earlyRawClient) exchange(t *testing.T, addr string, req []byte, want func([]byte) bool) []byte {
	t.Helper()
	var mu sync.Mutex
	var data []byte
	var closed bool
	var closeErr error
	wake := make(chan struct{}, 1)
	nudge := func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	conns := make(chan *fib.Connection, 1)
	dialed := make(chan error, 1)
	handler := fib.HandlerFuncs{
		Data: func(_ *fib.Connection, b []byte) {
			mu.Lock()
			data = append(data, b...)
			mu.Unlock()
			nudge()
		},
		Close: func(_ *fib.Connection, err error) {
			mu.Lock()
			closed, closeErr = true, err
			mu.Unlock()
			nudge()
		},
	}
	err := fibtls.DialWithOptions(c.engine, "tcp", addr, 5*time.Second, c.tlsConfig,
		fibtls.DialOptions{SessionCache: c.cache, EarlyData: true}, handler,
		func(conn *fib.Connection, err error) {
			if err == nil {
				err = conn.Send(req)
			}
			conns <- conn
			dialed <- err
		})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-dialed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the dial never completed")
	}
	var conn *fib.Connection
	select {
	case conn = <-conns:
	case <-time.After(10 * time.Second):
		t.Fatal("the dial reported no connection")
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		mu.Lock()
		got := append([]byte(nil), data...)
		ended, why := closed, closeErr
		mu.Unlock()
		if want != nil && want(got) {
			break
		}
		select {
		case <-wake:
		case <-deadline.C:
			if ended {
				t.Fatalf("timed out; got %q, and the connection closed: %v", got, why)
			}
			t.Fatalf("timed out; got %q, the connection still open", got)
		}
	}
	// What want saw may be the start of the rest, so give the round a moment
	// to deliver all of it, then close the connection.
	time.Sleep(30 * time.Millisecond)
	conn.Close()
	mu.Lock()
	defer mu.Unlock()
	return append([]byte(nil), data...)
}

// TestServerEarlyDataHTTP1 serves a GET that arrived as 0-RTT data with
// Early-Data: 1, and answers a POST in it 425 without running its handler.
func TestServerEarlyDataHTTP1(t *testing.T) {
	seen := make(chan string, 4)
	handler := HandlerFunc(func(c *Context) {
		seen <- c.Request.Method + " early=" + c.Request.Header.Get("Early-Data")
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte("ok"))
	})
	addr := earlyServer(t, func(c *Config) { c.EarlyData, c.DisableHTTP2 = true, true }, handler)
	client := newEarlyRawClient(t, "localhost")

	get := func(path string) []byte {
		return []byte("GET " + path + " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	}
	// A connection with no session to resume gets the ticket that makes the
	// next one early.
	warm := func() {
		if got := client.exchange(t, addr, get("/warm"), bodyEndsWith("ok")); !bytes.Contains(got, []byte(" 200 ")) {
			t.Fatalf("warm-up response %q", got)
		}
		wantSeen(t, seen, "GET early=")
		client.waitSession(t)
	}
	warm()
	post := []byte("POST /unsafe HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	if got := client.exchange(t, addr, post, bodyEndsWith("Too Early\n")); !bytes.Contains(got, []byte(" 425 ")) {
		t.Fatalf("early POST response %q", got)
	}
	select {
	case got := <-seen:
		t.Fatalf("the handler ran for a POST in early data: %q", got)
	case <-time.After(100 * time.Millisecond):
	}
	// The POST's connection may have carried a ticket; the next warm-up has
	// to be a full handshake, so that its ticket makes the GET early. The
	// cache is a new one rather than the old one emptied, since the POST's
	// ticket can reach the cache its dial was given after the exchange ends.
	client.cache = fibtls.NewQUICSessionCache(4)
	warm()
	if got := client.exchange(t, addr, get("/early"), bodyEndsWith("ok")); !bytes.Contains(got, []byte(" 200 ")) {
		t.Fatalf("early GET response %q", got)
	}
	wantSeen(t, seen, "GET early=1")
}

func bodyEndsWith(body string) func([]byte) bool {
	return func(got []byte) bool { return bytes.Contains(got, []byte("\r\n\r\n"+body)) }
}

// wantSeen waits for what the handler saw, so that a request that never
// reached it fails the test rather than hanging it.
func wantSeen(t *testing.T, seen <-chan string, want string) {
	t.Helper()
	select {
	case got := <-seen:
		if got != want {
			t.Fatalf("the handler saw %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("the handler did not see %q", want)
	}
}

// h2EarlyPayload is the connection preface, an empty SETTINGS and one request
// in a HEADERS frame.
func h2EarlyPayload(method, path string) []byte {
	enc := hpack.NewEncoder()
	block := enc.Begin(nil)
	block = enc.AppendField(block, ":method", method, false)
	block = enc.AppendField(block, ":scheme", "https", false)
	block = enc.AppendField(block, ":authority", "localhost", false)
	block = enc.AppendField(block, ":path", path, false)
	out := append([]byte(h2Preface), h2AppendSettings(nil)...)
	return h2AppendHeaderBlock(out, 1, block, true, h2DefaultMaxFrameSize)
}

// h2Reply is the first response HEADERS block on stream 1, decoded.
func h2Reply(t *testing.T, data []byte) (status string, ok bool) {
	t.Helper()
	dec := hpack.NewDecoder(hpack.DefaultTableSize)
	for len(data) > 0 {
		f, n, err := h2ReadFrame(data, h2DefaultMaxFrameSize)
		if err != nil || n == 0 {
			return "", false
		}
		data = data[n:]
		if f.typ != h2FrameHeaders || f.streamID != 1 {
			continue
		}
		_ = dec.Decode(f.payload, func(field hpack.HeaderField) error {
			if field.Name == ":status" {
				status, ok = field.Value, true
			}
			return nil
		})
		return status, ok
	}
	return "", false
}

// TestServerEarlyDataHTTP2 serves a GET whose HEADERS arrived in 0-RTT data
// with Early-Data: 1, and answers a POST in it 425.
func TestServerEarlyDataHTTP2(t *testing.T) {
	seen := make(chan string, 4)
	handler := HandlerFunc(func(c *Context) {
		seen <- c.Request.Method + " early=" + c.Request.Header.Get("Early-Data")
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte("ok"))
	})
	addr := earlyServer(t, func(c *Config) { c.EarlyData = true }, handler)
	client := newEarlyRawClient(t, "localhost")
	wantReply := func(got []byte) bool { _, ok := h2Reply(t, got); return ok }
	exchange := func(method, path string) []byte {
		got := client.exchange(t, addr, h2EarlyPayload(method, path), wantReply)
		if status, ok := h2Reply(t, got); !ok {
			t.Fatalf("%s %s: no HTTP/2 response in %q", method, path, got)
		} else if status != "200" && status != "425" {
			t.Fatalf("%s %s: status %q, response %q", method, path, status, got)
		}
		return got
	}
	// A connection with no session to resume gets the ticket that makes the
	// next one early.
	warm := func() {
		exchange(stdhttp.MethodGet, "/warm")
		wantSeen(t, seen, "GET early=")
		client.waitSession(t)
	}
	warm()
	if status, _ := h2Reply(t, exchange(stdhttp.MethodPost, "/unsafe")); status != "425" {
		t.Fatalf("early POST status = %q", status)
	}
	select {
	case got := <-seen:
		t.Fatalf("the handler ran for a POST in early data: %q", got)
	case <-time.After(100 * time.Millisecond):
	}
	// The POST's connection may have carried a ticket; the next warm-up has
	// to be a full handshake, so that its ticket makes the GET early. The
	// cache is a new one rather than the old one emptied, since the POST's
	// ticket can reach the cache its dial was given after the exchange ends.
	client.cache = fibtls.NewQUICSessionCache(4)
	warm()
	got := exchange(stdhttp.MethodGet, "/early")
	if status, _ := h2Reply(t, got); status != "200" {
		t.Fatalf("early GET status = %q; response %q", status, got)
	}
	wantSeen(t, seen, "GET early=1")
}

// TestClientEarlyDataHTTP1 sends a GET as 0-RTT data from http.Client and has
// it served from early data, with Early-Data: 1.
func TestClientEarlyDataHTTP1(t *testing.T) {
	seen := make(chan string, 4)
	handler := HandlerFunc(func(c *Context) {
		seen <- c.Request.Header.Get("Early-Data")
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte("ok"))
	})
	addr := earlyServer(t, func(c *Config) { c.EarlyData, c.DisableHTTP2 = true, true }, handler)

	_, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	clientTLS.MinVersion = stdtls.VersionTLS13
	config := DefaultClientConfig()
	config.TLSConfig = clientTLS
	config.DisableHTTP2 = true
	config.EarlyData = true
	config.SessionCache = fibtls.NewQUICSessionCache(4)
	config.MaxIdleConnsPerHost = 0 // each request dials, or nothing is early
	client := newTestClient(t, config)

	get := func(path string) *stdhttp.Response {
		t.Helper()
		req := mustRequest(t, stdhttp.MethodGet, "https://"+addr+path, nil)
		req.Host = "localhost"
		future := client.Go(req)
		resp, err := future.Wait()
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if body := readBody(t, resp); body != "ok" {
			t.Fatalf("%s: body %q", path, body)
		}
		return resp
	}
	get("/warm")
	wantSeen(t, seen, "")
	// The ticket may arrive a moment after the response; wait for it.
	deadline := time.Now().Add(3 * time.Second)
	for config.SessionCache.Get("localhost") == nil && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if config.SessionCache.Get("localhost") == nil {
		t.Fatal("no session ticket")
	}
	get("/early")
	wantSeen(t, seen, "1")
}

// TestClientEarlyDataRetriesOnTooEarly answers an early request 425, and has
// the client send it again once its handshake is over.
func TestClientEarlyDataRetriesOnTooEarly(t *testing.T) {
	var mu sync.Mutex
	var schemes []string
	handler := HandlerFunc(func(c *Context) {
		mu.Lock()
		schemes = append(schemes, c.Request.Header.Get("Early-Data"))
		mu.Unlock()
		if c.Request.Header.Get("Early-Data") != "" {
			_ = c.Respond(stdhttp.StatusTooEarly, "text/plain; charset=utf-8", []byte("too early"))
			return
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte("ok"))
	})
	addr := earlyServer(t, func(c *Config) { c.EarlyData, c.DisableHTTP2 = true, true }, handler)

	_, clientTLS, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	clientTLS.MinVersion = stdtls.VersionTLS13
	config := DefaultClientConfig()
	config.TLSConfig = clientTLS
	config.DisableHTTP2 = true
	config.EarlyData = true
	config.SessionCache = fibtls.NewQUICSessionCache(4)
	config.MaxIdleConnsPerHost = 0
	client := newTestClient(t, config)

	resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, "https://"+addr+"/warm", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	_ = readBody(t, resp)
	deadline := time.Now().Add(3 * time.Second)
	for config.SessionCache.Get("localhost") == nil && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	resp, err = client.Go(mustRequest(t, stdhttp.MethodGet, "https://"+addr+"/retry", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if body := readBody(t, resp); body != "ok" || resp.StatusCode != 200 {
		t.Fatalf("retried request: %d %q", resp.StatusCode, body)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"", "1", ""}
	if strings.Join(schemes, ",") != strings.Join(want, ",") {
		t.Fatalf("handler saw Early-Data %q, want %q", schemes, want)
	}
}

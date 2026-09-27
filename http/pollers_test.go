//go:build linux || darwin

package http

import (
	"bytes"
	"fmt"
	"io"
	stdhttp "net/http"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
)

// servePollers runs an engine with one poller serving handler, so every
// connection shares a loop, and returns its address.
func servePollers(t *testing.T, name string, handler fib.Handler) string {
	t.Helper()
	config := fib.DefaultConfig()
	config.Name = name
	config.Addr = "127.0.0.1:0"
	config.IOPollers = true
	config.IOPollerCount = 1
	server, err := fib.Bind(config, handler)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	})
	return addr.String()
}

// With ReadOnPollers, where the engine runs rounds on its pollers, an HTTP/1
// connection is read and parsed on its poller's loop, and its handlers run on the engine's pool
// of workers: one that blocks holds up neither the other connections on its
// loop nor the loop itself, and the requests pipelined behind it wait their
// turn and are answered in order.
func TestHTTP1HandlersLeaveThePollerLoop(t *testing.T) {
	type seen struct {
		onWorkers, onLoop bool
		pool              string
	}
	entered := make(chan seen, 1)
	release := make(chan struct{})
	config := DefaultConfig()
	config.ReadOnPollers = true
	addr := servePollers(t, "http1-offload", NewHandlerWithConfig(config, HandlerFunc(func(c *Context, r *stdhttp.Request) {
		if r.URL.Path == "/block" {
			pool := c.Conn.Engine().HandlerPool()
			entered <- seen{
				onWorkers: c.Conn.RunsOnWorkers(),
				onLoop:    bytes.Contains(debug.Stack(), []byte("taskpool.(*inlineBackend)")),
				pool:      pool.Name(),
			}
			<-release
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(r.URL.Path))
	})))
	blocked := dialRaw(t, addr)
	blocked.send("GET /block HTTP/1.1\r\nHost: x\r\n\r\nGET /behind HTTP/1.1\r\nHost: x\r\n\r\n")
	got := <-entered
	if got.onWorkers || got.onLoop || got.pool != "http1-offload-workers" {
		close(release)
		t.Fatalf("handler ran with RunsOnWorkers %v, on the loop %v, handler pool %q; want false, false, http1-offload-workers",
			got.onWorkers, got.onLoop, got.pool)
	}
	other := dialRaw(t, addr)
	other.send("GET /fast HTTP/1.1\r\nHost: x\r\n\r\n")
	if _, body := other.response("GET"); body != "/fast" {
		t.Fatalf("other connection got %q", body)
	}
	close(release)
	for _, want := range []string{"/block", "/behind"} {
		if _, body := blocked.response("GET"); body != want {
			t.Fatalf("blocked connection got %q, want %q", body, want)
		}
	}
}

// By default an HTTP/1 connection runs its whole round on the engine's
// workers even where the engine runs rounds on its pollers, so one handler
// that blocks holds up only its own connection.
func TestHTTP1RoundsRunOnWorkersByDefault(t *testing.T) {
	entered := make(chan bool, 1)
	release := make(chan struct{})
	addr := servePollers(t, "http1-workers", NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
		if r.URL.Path == "/block" {
			entered <- c.Conn.RunsOnWorkers()
			<-release
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(r.URL.Path))
	})))
	blocked := dialRaw(t, addr)
	blocked.send("GET /block HTTP/1.1\r\nHost: x\r\n\r\n")
	if onWorkers := <-entered; !onWorkers {
		close(release)
		t.Fatal("an HTTP/1 connection was not set to run on workers")
	}
	other := dialRaw(t, addr)
	other.send("GET /fast HTTP/1.1\r\nHost: x\r\n\r\n")
	if _, body := other.response("GET"); body != "/fast" {
		t.Fatalf("other connection got %q", body)
	}
	close(release)
	if _, body := blocked.response("GET"); body != "/block" {
		t.Fatalf("blocked connection got %q", body)
	}
}

// What a client pipelines behind a request whose handler is running is
// buffered only up to StreamRequestBodyBuffer: past it the connection stops
// reading, and it reads again, and answers every request in order, once the
// handler returns.
func TestHTTP1PipelinedBacklogHoldsReads(t *testing.T) {
	const requests = 2000
	config := DefaultConfig()
	config.ReadOnPollers = true
	config.StreamRequestBodyBuffer = 4 << 10
	held := make(chan bool, 1)
	release := make(chan struct{})
	addr := servePollers(t, "http1-backlog", NewHandlerWithConfig(config, HandlerFunc(func(c *Context, r *stdhttp.Request) {
		if r.URL.Path == "/0" {
			deadline := time.Now().Add(5 * time.Second)
			for !c.Conn.ReadsHeld() && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			held <- c.Conn.ReadsHeld()
			<-release
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(r.URL.Path))
	})))
	conn := dialRaw(t, addr)
	go func() {
		var b strings.Builder
		for i := 0; i < requests; i++ {
			fmt.Fprintf(&b, "GET /%d HTTP/1.1\r\nHost: x\r\n\r\n", i)
		}
		// The write outlasts the server's reads being held, so it runs
		// beside the test; the responses it reads tell the rest.
		_, _ = io.WriteString(conn.Conn, b.String())
	}()
	wasHeld := <-held
	close(release)
	if !wasHeld {
		t.Fatal("the connection kept reading behind a running handler")
	}
	for i := 0; i < requests; i++ {
		if _, body := conn.response("GET"); body != fmt.Sprintf("/%d", i) {
			t.Fatalf("response %d is for %q", i, body)
		}
	}
}

// Without pollers an HTTP/1 connection's rounds run on the engine's workers,
// handlers and all, and HTTP/2's handlers run on the stream pool, apart from
// the workers that read the connection.
func TestHandlerPoolWithoutPollers(t *testing.T) {
	type seen struct {
		onWorkers bool
		pool      string
	}
	got := make(chan seen, 2)
	config := fib.DefaultConfig()
	config.Name = "no-pollers"
	config.Addr = "127.0.0.1:0"
	config.IOPollers = false
	server, err := fib.Bind(config, NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
		got <- seen{onWorkers: c.Conn.RunsOnWorkers(), pool: c.Conn.Engine().HandlerPool().Name()}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(r.Proto))
	})))
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		<-runDone
		_ = server.Close()
	})
	if server.RoundsOnPollers() {
		t.Fatal("an engine without pollers reports rounds on pollers")
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	raw := dialRaw(t, addr.String())
	raw.send("GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	if _, body := raw.response("GET"); body != "HTTP/1.1" {
		t.Fatalf("body %q", body)
	}
	if s := <-got; !s.onWorkers || s.pool != "no-pollers-streams" {
		t.Fatalf("HTTP/1 RunsOnWorkers %v, handler pool %q; want true, no-pollers-streams", s.onWorkers, s.pool)
	}
	tc := dialH2(t, addr.String())
	tc.headers(1, true, get("/")...)
	if _, body := tc.response(1); string(body) != "HTTP/2.0" {
		t.Fatalf("body %q", body)
	}
	if s := <-got; s.onWorkers || s.pool != "no-pollers-streams" {
		t.Fatalf("HTTP/2 RunsOnWorkers %v, handler pool %q; want false, no-pollers-streams", s.onWorkers, s.pool)
	}
}

// A connection that opens with the HTTP/2 preface goes back to the engine's
// own pool for its reads, which under pollers is its poller's loop, and its
// requests run on the engine's pool of workers, the handler pool there.
func TestHTTP2LeavesWorkersUnderPollers(t *testing.T) {
	type seen struct {
		onWorkers bool
		pool      string
	}
	got := make(chan seen, 1)
	addr := servePollers(t, "http2-engine-pool", NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
		got <- seen{onWorkers: c.Conn.RunsOnWorkers(), pool: c.Conn.Engine().HandlerPool().Name()}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(r.Proto))
	})))
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/")...)
	if _, body := tc.response(1); string(body) != "HTTP/2.0" {
		t.Fatalf("body %q", body)
	}
	select {
	case s := <-got:
		if s.onWorkers {
			t.Fatal("an HTTP/2 connection still runs its rounds on workers")
		}
		if s.pool != "http2-engine-pool-workers" {
			t.Fatalf("handler pool %q, want the engine's workers", s.pool)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never ran")
	}
}

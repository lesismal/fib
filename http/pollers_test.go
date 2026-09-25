//go:build linux || darwin

package http

import (
	stdhttp "net/http"
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

// An HTTP/1 handler runs on a worker even where the engine runs rounds on
// its pollers, so one that blocks holds up only its own connection, not the
// others on its loop.
func TestHTTP1HandlersRunOnWorkersUnderPollers(t *testing.T) {
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

// A connection that opens with the HTTP/2 preface goes back to the engine's
// own pool for its reads, and its requests run on the stream pool.
func TestHTTP2LeavesWorkersUnderPollers(t *testing.T) {
	onWorkers := make(chan bool, 1)
	addr := servePollers(t, "http2-engine-pool", NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
		onWorkers <- c.Conn.RunsOnWorkers()
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(r.Proto))
	})))
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/")...)
	if _, body := tc.response(1); string(body) != "HTTP/2.0" {
		t.Fatalf("body %q", body)
	}
	select {
	case on := <-onWorkers:
		if on {
			t.Fatal("an HTTP/2 connection still runs its rounds on workers")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler never ran")
	}
}

//go:build linux || darwin

package http

import (
	"fmt"
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

// An HTTP/1 connection runs its whole round on the engine's workers, and a
// poller only hands it on, so one handler that blocks holds up only its own
// connection, not the others on its loop.
func TestHTTP1RoundsRunOnWorkers(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	addr := servePollers(t, "http1-workers", NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
		if r.URL.Path == "/block" {
			entered <- struct{}{}
			<-release
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(r.URL.Path))
	})))
	blocked := dialRaw(t, addr)
	blocked.send("GET /block HTTP/1.1\r\nHost: x\r\n\r\n")
	<-entered
	other := dialRaw(t, addr)
	other.send("GET /fast HTTP/1.1\r\nHost: x\r\n\r\n")
	if _, body := other.response("GET"); body != "/fast" {
		close(release)
		t.Fatalf("other connection got %q", body)
	}
	close(release)
	if _, body := blocked.response("GET"); body != "/block" {
		t.Fatalf("blocked connection got %q", body)
	}
}

// HTTP/2's handlers run on the stream pool, apart from the workers that read
// the connection, with pollers or without; HTTP/1's run in the connection's
// round, and see the same pool as the engine's handler pool.
func TestHandlerPoolIsTheStreamPool(t *testing.T) {
	for _, pollers := range []bool{false, true} {
		name := fmt.Sprintf("handler-pool-%v", pollers)
		got := make(chan string, 2)
		config := fib.DefaultConfig()
		config.Name = name
		config.Addr = "127.0.0.1:0"
		config.IOPollers = pollers
		server, err := fib.Bind(config, NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
			got <- c.Conn.Engine().HandlerPool().Name()
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(r.Proto))
		})))
		if err != nil {
			t.Fatal(err)
		}
		runDone := make(chan error, 1)
		go func() { runDone <- server.Run() }()
		addr, err := server.LocalAddr()
		if err != nil {
			t.Fatal(err)
		}
		raw := dialRaw(t, addr.String())
		raw.send("GET / HTTP/1.1\r\nHost: x\r\n\r\n")
		if _, body := raw.response("GET"); body != "HTTP/1.1" {
			t.Fatalf("body %q", body)
		}
		tc := dialH2(t, addr.String())
		tc.headers(1, true, get("/")...)
		if _, body := tc.response(1); string(body) != "HTTP/2.0" {
			t.Fatalf("body %q", body)
		}
		for _, proto := range []string{"HTTP/1", "HTTP/2"} {
			select {
			case pool := <-got:
				if pool != name+"-streams" {
					t.Fatalf("pollers %v: %s handler pool %q, want %s-streams", pollers, proto, pool, name)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("pollers %v: the %s handler never ran", pollers, proto)
			}
		}
		server.Stop()
		<-runDone
		_ = server.Close()
	}
}

//go:build linux || darwin || windows

package http

import (
	"bytes"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"testing"
	"time"

	fib "github.com/lesismal/fib/go"
)

func timeoutConfig(header, read, idle time.Duration) Config {
	config := DefaultConfig()
	config.ReadHeaderTimeout, config.ReadTimeout, config.IdleTimeout = header, read, idle
	return config
}

// echoPath answers with the request's path once its body has been read.
func echoPath(c *Context, r *stdhttp.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(r.URL.Path))
}

func TestServerReadHeaderTimeoutClosesASlowHeader(t *testing.T) {
	addr := serveStreamingServer(t, timeoutConfig(200*time.Millisecond, 0, 0), echoPath)
	conn := dialRaw(t, addr)
	conn.send("GET /slow HTTP/1.1\r\nHost: test\r\n")
	// The header never ends.
	if !conn.closed() {
		t.Fatal("a header that never ended did not time out")
	}
}

func TestServerReadHeaderTimeoutLeavesACompleteHeaderAlone(t *testing.T) {
	addr := serveStreamingServer(t, timeoutConfig(200*time.Millisecond, 2*time.Second, 0), echoPath)
	conn := dialRaw(t, addr)
	conn.send("POST /body HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\n\r\n")
	// Past the header timeout, but the header is in: ReadTimeout bounds the
	// body now.
	time.Sleep(500 * time.Millisecond)
	conn.send("data")
	if _, body := conn.response(stdhttp.MethodPost); body != "/body" {
		t.Fatalf("body = %q, want %q", body, "/body")
	}
}

func TestServerReadTimeoutClosesASlowBody(t *testing.T) {
	addr := serveStreamingServer(t, timeoutConfig(0, 300*time.Millisecond, 0), echoPath)
	conn := dialRaw(t, addr)
	conn.send("POST /drip HTTP/1.1\r\nHost: test\r\nContent-Length: 1000\r\n\r\n")
	conn.send("a few bytes")
	if !conn.closed() {
		t.Fatal("a body that never finished did not time out")
	}
}

// TestServerReadTimeoutClosesASlowStreamedBody checks that the timeout follows
// a body the handler is already reading, which is the case worth having it for.
func TestServerReadTimeoutClosesASlowStreamedBody(t *testing.T) {
	config := timeoutConfig(0, 400*time.Millisecond, 0)
	config.StreamRequestBodyThreshold = 1 << 10
	failed := make(chan error, 1)
	addr := serveStreamingServer(t, config, func(c *Context, r *stdhttp.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		failed <- err
	})
	conn := dialRaw(t, addr)
	conn.send(fmt.Sprintf("POST /upload HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", 8<<20))
	if _, err := conn.Write(bytes.Repeat([]byte("x"), 4<<10)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-failed:
		if err == nil {
			t.Fatal("a body cut short by the read timeout read as a complete one")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a streamed body that stopped arriving did not time out")
	}
}

// TestServerReadTimeoutDoesNotCutOffAHandler checks that the deadline is
// dropped once the request has all arrived: a handler slower than ReadTimeout
// still gets to answer.
func TestServerReadTimeoutDoesNotCutOffAHandler(t *testing.T) {
	addr := serveStreamingServer(t, timeoutConfig(0, 200*time.Millisecond, 0),
		func(c *Context, r *stdhttp.Request) {
			time.Sleep(600 * time.Millisecond)
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("slow but answered"))
		})
	conn := dialRaw(t, addr)
	conn.send("GET /slow HTTP/1.1\r\nHost: test\r\n\r\n")
	if _, body := conn.response(stdhttp.MethodGet); body != "slow but answered" {
		t.Fatalf("body = %q", body)
	}
}

func TestServerIdleTimeoutClosesAQuietConnection(t *testing.T) {
	addr := serveStreamingServer(t, timeoutConfig(0, 0, 300*time.Millisecond), echoPath)
	conn := dialRaw(t, addr)
	conn.send("GET /one HTTP/1.1\r\nHost: test\r\n\r\n")
	if _, body := conn.response(stdhttp.MethodGet); body != "/one" {
		t.Fatalf("body = %q, want %q", body, "/one")
	}
	if !conn.closed() {
		t.Fatal("a connection that went quiet between requests stayed open")
	}
}

func TestServerIdleTimeoutIsRefreshedByEachRequest(t *testing.T) {
	addr := serveStreamingServer(t, timeoutConfig(0, 0, 400*time.Millisecond), echoPath)
	conn := dialRaw(t, addr)
	for i := 0; i < 4; i++ {
		path := fmt.Sprintf("/r%d", i)
		conn.send("GET " + path + " HTTP/1.1\r\nHost: test\r\n\r\n")
		if _, body := conn.response(stdhttp.MethodGet); body != path {
			t.Fatalf("body = %q, want %q", body, path)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestServerIdleTimeoutBoundsAConnectionThatSaysNothing checks the deadline
// armed at OnOpen.
func TestServerIdleTimeoutBoundsAConnectionThatSaysNothing(t *testing.T) {
	addr := serveStreamingServer(t, timeoutConfig(0, 0, 250*time.Millisecond), echoPath)
	conn := dialRaw(t, addr)
	if !conn.closed() {
		t.Fatal("a connection that never sent anything stayed open")
	}
}

func TestServerWithoutTimeoutsHoldsAConnectionOpen(t *testing.T) {
	addr := serveStreamingServer(t, DefaultConfig(), echoPath)
	conn := dialRaw(t, addr)
	conn.send("GET /one HTTP/1.1\r\nHost: test\r\n\r\n")
	if _, body := conn.response(stdhttp.MethodGet); body != "/one" {
		t.Fatalf("body = %q", body)
	}
	time.Sleep(400 * time.Millisecond)
	conn.send("GET /two HTTP/1.1\r\nHost: test\r\n\r\n")
	if _, body := conn.response(stdhttp.MethodGet); body != "/two" {
		t.Fatalf("body = %q", body)
	}
}

// TestServerTimeoutsDoNotFollowAConnectionIntoHTTP2 checks that a connection
// that becomes HTTP/2 is not closed by the HTTP/1 parser's idle timeout, which
// nothing would refresh once HTTP/2 is serving it.
func TestServerTimeoutsDoNotFollowAConnectionIntoHTTP2(t *testing.T) {
	addr := serveStreamingServer(t, timeoutConfig(0, 0, 300*time.Millisecond), echoPath)
	conn := dialRaw(t, addr)
	conn.send(h2Preface)
	if _, err := conn.Write(h2AppendSettings(nil)); err != nil {
		t.Fatal(err)
	}
	// Well past the HTTP/1 idle timeout the connection is still HTTP/2's.
	time.Sleep(700 * time.Millisecond)
	ping := h2AppendFrameHeader(nil, h2FramePing, 0, 0, 8)
	ping = append(ping, 1, 2, 3, 4, 5, 6, 7, 8)
	if _, err := conn.Write(ping); err != nil {
		t.Fatalf("the HTTP/2 connection was closed: %v", err)
	}
	var buffered []byte
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		frame, n, err := h2ReadFrame(buffered, h2MaxFrameSizeLimit)
		if err != nil {
			t.Fatalf("reading a frame: %v", err)
		}
		if n > 0 {
			buffered = buffered[n:]
			if frame.typ == h2FramePing && frame.has(h2FlagAck) {
				return
			}
			continue
		}
		buf := make([]byte, 4096)
		m, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("the HTTP/2 connection was closed: %v", err)
		}
		buffered = append(buffered, buf[:m]...)
	}
	t.Fatal("no PING ack came back")
}

// closeReporter forwards to an HTTP server handler and reports why each
// connection closed.
type closeReporter struct {
	*ServerHandler
	closed chan error
}

func (r closeReporter) OnClose(c *fib.Connection, err error) {
	r.ServerHandler.OnClose(c, err)
	select {
	case r.closed <- err:
	default:
	}
}

// TestServerTimeoutReachesOnClose checks that a connection the read timeout
// ended says so, rather than looking like an ordinary close.
func TestServerTimeoutReachesOnClose(t *testing.T) {
	closed := make(chan error, 1)
	handler := NewHandlerWithConfig(timeoutConfig(150*time.Millisecond, 0, 0), HandlerFunc(echoPath))
	addr := serve(t, closeReporter{ServerHandler: handler, closed: closed})
	conn := dialRaw(t, addr)
	conn.send("GET /x HTTP/1.1\r\n")
	select {
	case err := <-closed:
		if !os.IsTimeout(err) {
			t.Fatalf("OnClose error = %v, want a timeout", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the connection never closed")
	}
}

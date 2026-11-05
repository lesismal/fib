//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"
)

// The idle timeouts these tests set with SetIdleTimeout: a short one well
// under the server's, and a long one well over it. A loaded CI runner can
// oversleep a pause by a good part of the short one, so the checks on how
// soon a connection closed allow for that.
const (
	overrideShort = 300 * time.Millisecond
	overrideLong  = 3 * time.Second
)

// idleOverrideHandler sets the connection's idle timeout by the request's
// path: /short and /long give it overrideShort and overrideLong, /reset the
// server's back, /slow answers after twice overrideShort, /retain sets it to
// overrideShort from another goroutine before answering there, and anything
// else leaves it alone. Each answers with its path.
func idleOverrideHandler(c *Context) {
	path := c.Request.URL.Path
	var err error
	switch path {
	case "/short":
		err = c.SetIdleTimeout(overrideShort)
	case "/long":
		err = c.SetIdleTimeout(overrideLong)
	case "/reset":
		err = c.SetIdleTimeout(0)
	case "/slow":
		time.Sleep(2 * overrideShort)
	case "/retain":
		c.Retain()
		go func() {
			defer c.Release()
			if err := c.SetIdleTimeout(overrideShort); err != nil {
				_ = c.Respond(stdhttp.StatusInternalServerError, "text/plain", []byte(err.Error()))
				return
			}
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(path))
		}()
		return
	}
	if err != nil {
		_ = c.Respond(stdhttp.StatusInternalServerError, "text/plain", []byte(err.Error()))
		return
	}
	_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(path))
	if err := c.SetIdleTimeout(time.Second); !errors.Is(err, ErrResponseWritten) {
		panic("SetIdleTimeout after the response: " + errString(err))
	}
}

// get1 sends a GET for path on conn and checks the answer.
func get1(t *testing.T, conn *rawConn, path string) {
	t.Helper()
	conn.send("GET " + path + " HTTP/1.1\r\nHost: test\r\n\r\n")
	if resp, body := conn.response(stdhttp.MethodGet); resp.StatusCode != stdhttp.StatusOK || body != path {
		t.Fatalf("GET %s: %d %q", path, resp.StatusCode, body)
	}
}

// closesWithin checks that conn closes, and not later than within after now.
func closesWithin(t *testing.T, conn *rawConn, within time.Duration) {
	t.Helper()
	start := time.Now()
	if !conn.closed() {
		t.Fatal("the connection stayed open")
	}
	if waited := time.Since(start); waited > within {
		t.Fatalf("closed after %v, want within %v", waited, within)
	}
}

// A connection given a short idle timeout is closed after it, though the
// server's is long; one given a long one outlives the server's, request
// after request, until a handler gives it the server's back. Without any
// timeout of the server's the handler's still applies, and does not cut off
// a later handler slower than itself.
func TestSetIdleTimeoutHTTP1(t *testing.T) {
	t.Run("short", func(t *testing.T) {
		addr := serveStreamingServer(t, timeoutConfig(0, 0, time.Minute), idleOverrideHandler)
		conn := dialRaw(t, addr)
		get1(t, conn, "/short")
		closesWithin(t, conn, 3*time.Second)
	})
	t.Run("long, kept until reset", func(t *testing.T) {
		const serverIdle = 300 * time.Millisecond
		addr := serveStreamingServer(t, timeoutConfig(0, 0, serverIdle), idleOverrideHandler)
		conn := dialRaw(t, addr)
		get1(t, conn, "/long")
		// Past the server's idle timeout, twice: the second request does not
		// set it, and the long one carries on.
		time.Sleep(3 * serverIdle)
		get1(t, conn, "/")
		time.Sleep(3 * serverIdle)
		get1(t, conn, "/reset")
		closesWithin(t, conn, overrideLong/2)
	})
	t.Run("no server timeouts", func(t *testing.T) {
		addr := serveStreamingServer(t, Config{}, idleOverrideHandler)
		conn := dialRaw(t, addr)
		get1(t, conn, "/short")
		// The handler's time is its own, as with the server's timeouts.
		get1(t, conn, "/slow")
		closesWithin(t, conn, 3*time.Second)

		conn = dialRaw(t, addr)
		get1(t, conn, "/short")
		get1(t, conn, "/reset")
		time.Sleep(3 * overrideShort)
		get1(t, conn, "/")
	})
	t.Run("retained", func(t *testing.T) {
		addr := serveStreamingServer(t, timeoutConfig(0, 0, time.Minute), idleOverrideHandler)
		conn := dialRaw(t, addr)
		get1(t, conn, "/retain")
		closesWithin(t, conn, 3*time.Second)
	})
}

// Requests pipelined behind the one that sets the idle timeout are all
// answered, and the timeout bounds the connection once they have been.
func TestSetIdleTimeoutHTTP1Pipelined(t *testing.T) {
	t.Run("short", func(t *testing.T) {
		addr := serveStreamingServer(t, timeoutConfig(0, 0, time.Minute), idleOverrideHandler)
		conn := dialRaw(t, addr)
		paths := []string{"/short", "/", "/slow", "/"}
		var batch strings.Builder
		for _, path := range paths {
			batch.WriteString("GET " + path + " HTTP/1.1\r\nHost: test\r\n\r\n")
		}
		conn.send(batch.String())
		for _, path := range paths {
			if resp, body := conn.response(stdhttp.MethodGet); resp.StatusCode != stdhttp.StatusOK || body != path {
				t.Fatalf("GET %s: %d %q", path, resp.StatusCode, body)
			}
		}
		closesWithin(t, conn, 3*time.Second)
	})
	t.Run("long", func(t *testing.T) {
		const serverIdle = 300 * time.Millisecond
		addr := serveStreamingServer(t, timeoutConfig(0, 0, serverIdle), idleOverrideHandler)
		conn := dialRaw(t, addr)
		conn.send("GET /long HTTP/1.1\r\nHost: test\r\n\r\nGET / HTTP/1.1\r\nHost: test\r\n\r\n")
		for _, path := range []string{"/long", "/"} {
			if _, body := conn.response(stdhttp.MethodGet); body != path {
				t.Fatalf("body %q, want %q", body, path)
			}
		}
		time.Sleep(3 * serverIdle)
		get1(t, conn, "/")
	})
}

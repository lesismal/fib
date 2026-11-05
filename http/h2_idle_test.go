//go:build linux || darwin || windows

package http

import (
	stdtls "crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/internal/debughook"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// h2TestIdle is the IdleTimeout these tests give the server. A loaded CI
// runner can leave the server's worker unscheduled for a good part of a
// shorter one, and the connection is then retired with a request it has not
// read yet in its socket, as a connection waiting on a read deadline would be.
const h2TestIdle = 500 * time.Millisecond

// idleH2Handler answers at once, or after a pause longer than h2TestIdle for
// /slow.
func idleH2Handler() Handler {
	return HandlerFunc(func(c *Context) {
		if c.Request.URL.Path == "/slow" {
			time.Sleep(2 * h2TestIdle)
		}
		_ = c.Respond(200, "text/plain", []byte("ok"))
	})
}

// dialIdleH2 opens an HTTP/2 connection to a server with config, in
// cleartext or over TLS.
func dialIdleH2(t *testing.T, config Config, secure bool) *h2TestConn {
	t.Helper()
	handler := &closeRecorder{ServerHandler: NewHandlerWithConfig(config, idleH2Handler()), start: time.Now()}
	debughook.SetTrace(true)
	t.Cleanup(func() {
		debughook.SetTrace(false)
		if t.Failed() {
			// Diagnostic: why the server ended the connection, and when,
			// and the deadlines it was given.
			handler.mu.Lock()
			t.Logf("server closes: %v", handler.closes)
			for _, c := range handler.conns {
				var lines []string
				for _, l := range strings.Split(debughook.ConnTrace(c), "\n") {
					if strings.Contains(l, "deadline") || strings.Contains(l, "event") || strings.Contains(l, "read n=") || strings.Contains(l, "round") {
						if i := strings.Index(l, " | "); i >= 0 {
							l = l[:i]
						}
						lines = append(lines, l)
					}
				}
				trace := strings.Join(lines, "\n")
				if len(trace) > 3500 {
					trace = trace[len(trace)-3500:]
				}
				fmt.Printf("\n::error title=%s deadlines::%s\n", t.Name(), strings.ReplaceAll(trace, "\n", "%0A"))
			}
			handler.mu.Unlock()
		}
	})
	if !secure {
		return dialH2(t, serve(t, handler))
	}
	serverConfig, clientConfig, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	addr := serve(t, fibtls.NewServer(ConfigureTLS(serverConfig), handler))
	clientConfig.NextProtos = []string{"h2"}
	conn, err := stdtls.Dial("tcp", addr, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	tc := newH2TestConn(t, conn)
	tc.write(append([]byte(h2Preface), h2AppendSettings(nil)...))
	return tc
}

// closeRecorder is a ServerHandler that records the errors its connections
// close with, and when, for a failing test to report. Diagnostic only.
type closeRecorder struct {
	*ServerHandler
	mu     sync.Mutex
	start  time.Time
	closes []string
	conns  []*fib.Connection
}

func (h *closeRecorder) OnOpen(c *fib.Connection) {
	h.mu.Lock()
	h.conns = append(h.conns, c)
	h.mu.Unlock()
	h.ServerHandler.OnOpen(c)
}

func (h *closeRecorder) OnClose(c *fib.Connection, err error) {
	h.mu.Lock()
	h.closes = append(h.closes, fmt.Sprintf("%v %v after the server was made", err, time.Since(h.start)))
	h.mu.Unlock()
	h.ServerHandler.OnClose(c, err)
}

// expectIdleGoAway reads until the server's GOAWAY, checks it is NO_ERROR
// naming last as the last stream, and that the connection then closes.
func expectIdleGoAway(t *testing.T, tc *h2TestConn, last uint32) {
	t.Helper()
	f := tc.readUntil(h2FrameGoAway)
	if id, code := binary.BigEndian.Uint32(f.payload)&0x7fffffff, H2ErrorCode(binary.BigEndian.Uint32(f.payload[4:])); id != last || code != H2NoError {
		t.Fatalf("GOAWAY last stream %d code %v, want %d NO_ERROR", id, code, last)
	}
	buf := make([]byte, 1024)
	for {
		if _, err := tc.c.Read(buf); err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatal("the connection stayed open after its GOAWAY")
			}
			return
		}
	}
}

// An HTTP/2 connection that carries no stream for IdleTimeout is retired
// with GOAWAY and closed, in cleartext and over TLS, whether it never asked
// for anything or went quiet after its requests; one with a stream open is
// left alone however long the stream takes.
func TestH2IdleTimeout(t *testing.T) {
	const idle = h2TestIdle
	config := DefaultConfig()
	config.IdleTimeout = idle
	for _, secure := range []bool{false, true} {
		name := map[bool]string{false: "h2c", true: "tls"}[secure]
		t.Run(name+"/fresh", func(t *testing.T) {
			tc := dialIdleH2(t, config, secure)
			start := time.Now()
			expectIdleGoAway(t, tc, 0)
			if waited := time.Since(start); waited < idle/2 {
				t.Fatalf("retired after %v", waited)
			}
		})
		t.Run(name+"/after requests", func(t *testing.T) {
			tc := dialIdleH2(t, config, secure)
			tc.headers(1, true, get("/")...)
			tc.response(1)
			tc.headers(3, true, get("/")...)
			tc.response(3)
			done := time.Now()
			expectIdleGoAway(t, tc, 3)
			if waited := time.Since(done); waited < idle/2 {
				t.Fatalf("retired %v after the last response", waited)
			}
		})
		t.Run(name+"/busy", func(t *testing.T) {
			tc := dialIdleH2(t, config, secure)
			tc.headers(1, true, get("/slow")...)
			// Longer than IdleTimeout, and the stream is still open: the
			// response comes, not a GOAWAY ahead of it.
			if _, body := tc.response(1); string(body) != "ok" {
				t.Fatalf("body %q", body)
			}
			expectIdleGoAway(t, tc, 1)
		})
	}
}

// IdleTimeout falls back to ReadTimeout, as on HTTP/1, and a negative one
// leaves an idle connection open.
func TestH2IdleTimeoutSettings(t *testing.T) {
	t.Run("fallback", func(t *testing.T) {
		config := Config{MaxHeaderBytes: 1 << 20, MaxBodyBytes: 1 << 20, ReadTimeout: h2TestIdle}
		expectIdleGoAway(t, dialIdleH2(t, config, false), 0)
	})
	t.Run("none", func(t *testing.T) {
		config := DefaultConfig()
		config.IdleTimeout = -1
		tc := dialIdleH2(t, config, false)
		_ = tc.c.SetReadDeadline(time.Now().Add(2 * h2TestIdle))
		buf := make([]byte, 1024)
		for {
			_, err := tc.c.Read(buf)
			if err == nil {
				continue
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return
			}
			if err == io.EOF {
				t.Fatal("an idle connection was closed without an IdleTimeout")
			}
			t.Fatal(err)
		}
	})
}

// A timer that fires as a stream opens, or for an idleness that began again
// since, leaves the connection alone.
func TestH2IdleExpiredStale(t *testing.T) {
	config := DefaultConfig()
	config.IdleTimeout = time.Hour
	sc := newH2ServerConn(NewHandlerWithConfig(config, nil), nil, "")
	sc.idleSince = time.Now()
	sc.idleExpired()
	sc.streams[1] = &h2ServerStream{}
	sc.idleSince = time.Time{}
	sc.idleExpired()
	if sc.goAway || sc.closed {
		t.Fatal("a stale idle timer retired the connection")
	}
}

// On HTTP/2 the idle timeout is the connection's: it bounds how long the
// connection may carry no stream, from when its last one ends, until a
// handler sets it again.
func TestSetIdleTimeoutH2(t *testing.T) {
	ok := func(tc *h2TestConn, id uint32, path string) {
		t.Helper()
		tc.headers(id, true, get(path)...)
		if _, body := tc.response(id); string(body) != path {
			t.Fatalf("stream %d: body %q, want %q", id, body, path)
		}
	}
	t.Run("short", func(t *testing.T) {
		config := DefaultConfig()
		config.IdleTimeout = time.Minute
		tc := dialH2(t, serve(t, NewHandlerWithConfig(config, HandlerFunc(idleOverrideHandler))))
		ok(tc, 1, "/short")
		ok(tc, 3, "/")
		start := time.Now()
		expectIdleGoAway(t, tc, 3)
		if waited := time.Since(start); waited > 3*time.Second {
			t.Fatalf("retired after %v", waited)
		}
	})
	t.Run("long, kept until reset", func(t *testing.T) {
		config := DefaultConfig()
		config.IdleTimeout = h2TestIdle
		tc := dialH2(t, serve(t, NewHandlerWithConfig(config, HandlerFunc(idleOverrideHandler))))
		ok(tc, 1, "/long")
		time.Sleep(3 * h2TestIdle)
		ok(tc, 3, "/")
		time.Sleep(3 * h2TestIdle)
		ok(tc, 5, "/reset")
		start := time.Now()
		expectIdleGoAway(t, tc, 5)
		if waited := time.Since(start); waited > overrideLong/2 {
			t.Fatalf("retired after %v", waited)
		}
	})
}

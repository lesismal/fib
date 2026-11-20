//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package websocket

import (
	"bufio"
	"encoding/base64"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"strings"
	"testing"
	"time"

	epollhttp "github.com/lesismal/fib/http"
)

// handshakeHandlers are the two ways a ServerHandler reads a handshake: as
// bytes alone, when nothing needs the request, and as an *http.Request.
var handshakeHandlers = []struct {
	name    string
	handler HandlerFuncs
}{
	{"minimal", HandlerFuncs{Message: func(c *Connection, op Opcode, p []byte) { _ = c.WriteMessage(op, p) }}},
	{"request", HandlerFuncs{Open: func(*Connection, *stdhttp.Request) {},
		Message: func(c *Connection, op Opcode, p []byte) { _ = c.WriteMessage(op, p) }}},
}

func serverAddr(url string) string {
	return strings.TrimSuffix(strings.TrimPrefix(url, "ws://"), "/ws")
}

func handshakeFor(target string) string {
	key := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef"))
	return "GET " + target + " HTTP/1.1\r\nHost: test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + key + "\r\n\r\n"
}

// handshakeStatus sends request to addr in the given pieces, pause apart, and
// returns the status of the response.
func handshakeStatus(t *testing.T, addr string, pause time.Duration, pieces ...string) int {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	for i, piece := range pieces {
		if i > 0 {
			time.Sleep(pause)
		}
		if _, err := io.WriteString(conn, piece); err != nil {
			t.Fatal(err)
		}
	}
	response, err := stdhttp.ReadResponse(bufio.NewReader(conn), &stdhttp.Request{Method: stdhttp.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	return response.StatusCode
}

// closedWithin reports how long the server took to close a connection that
// sent sent and then nothing, failing if it has not within limit.
func closedWithin(t *testing.T, addr, sent string, limit time.Duration) time.Duration {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	start := time.Now()
	if sent != "" {
		if _, err := io.WriteString(conn, sent); err != nil {
			t.Fatal(err)
		}
	}
	_ = conn.SetReadDeadline(start.Add(limit))
	var buf [64]byte
	for {
		n, err := conn.Read(buf[:])
		if n > 0 {
			t.Fatalf("the server answered %q", buf[:n])
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("the connection was still open after %v", limit)
		}
		if err != nil {
			return time.Since(start)
		}
	}
}

// A connection that does not complete its handshake within HandshakeTimeout
// is closed, whether it sent nothing or part of a request head; one that
// completes it in pieces within the time stays open past it.
func TestHandshakeTimeout(t *testing.T) {
	const timeout = 100 * time.Millisecond
	for _, tc := range handshakeHandlers {
		t.Run(tc.name, func(t *testing.T) {
			config := DefaultConfig()
			config.HandshakeTimeout = timeout
			url := startWebSocketServer(t, config, tc.handler)
			addr := serverAddr(url)
			for _, sent := range []string{"", "GET /ws HTTP/1.1\r\nHost: test\r\n"} {
				if took := closedWithin(t, addr, sent, 5*time.Second); took < timeout/2 {
					t.Fatalf("closed after %v, before the timeout of %v", took, timeout)
				}
			}
			request := handshakeFor("/ws")
			half := len(request) / 2
			if status := handshakeStatus(t, addr, 20*time.Millisecond, request[:half], request[half:]); status != stdhttp.StatusSwitchingProtocols {
				t.Fatalf("a handshake in two pieces got %d", status)
			}

			client := newClientRecorder()
			ws, _, err := NewDialer(startClientEngine(t), DefaultDialerConfig()).Go(url, nil, client.handler()).Wait()
			if err != nil {
				t.Fatal(err)
			}
			defer ws.Close(CloseNormal, "")
			time.Sleep(3 * timeout)
			if err := ws.WriteText("still here"); err != nil {
				t.Fatal(err)
			}
			if event := client.next(t); string(event.Payload) != "still here" {
				t.Fatalf("echo %q", event.Payload)
			}
		})
	}
}

// A HandshakeTimeout below zero sets no limit, and zero means the default.
func TestHandshakeTimeoutDisabledAndDefault(t *testing.T) {
	if got := NewHandlerWithConfig(Config{}, nil).config.HandshakeTimeout; got != DefaultHandshakeTimeout {
		t.Fatalf("a zero HandshakeTimeout became %v", got)
	}
	if got := DefaultConfig().HandshakeTimeout; got != DefaultHandshakeTimeout {
		t.Fatalf("DefaultConfig's HandshakeTimeout is %v", got)
	}
	config := DefaultConfig()
	config.HandshakeTimeout = -1
	addr := serverAddr(startWebSocketServer(t, config, HandlerFuncs{}))
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	var buf [1]byte
	if _, err := conn.Read(buf[:]); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("a silent connection without a limit read %v", err)
	}
}

// With Config.Path set, a handshake for any other path is answered 404, and
// one for it, with a query or percent-encoded, opens.
func TestHandshakePath(t *testing.T) {
	for _, tc := range handshakeHandlers {
		t.Run(tc.name, func(t *testing.T) {
			config := DefaultConfig()
			config.Path = "/ws"
			addr := serverAddr(startWebSocketServer(t, config, tc.handler))
			for target, want := range map[string]int{
				"/ws":       stdhttp.StatusSwitchingProtocols,
				"/ws?id=1":  stdhttp.StatusSwitchingProtocols,
				"/w%73":     stdhttp.StatusSwitchingProtocols,
				"/":         stdhttp.StatusNotFound,
				"/ws/":      stdhttp.StatusNotFound,
				"/other?ws": stdhttp.StatusNotFound,
				"/w%74":     stdhttp.StatusNotFound,
			} {
				if got := handshakeStatus(t, addr, 0, handshakeFor(target)); got != want {
					t.Errorf("%s: status %d, want %d", target, got, want)
				}
			}
		})
	}
}

// Upgrade answers a request for a path other than Config.Path with 404.
func TestUpgradePath(t *testing.T) {
	config := DefaultConfig()
	config.Path = "/ws"
	ws := NewHandlerWithConfig(config, HandlerFuncs{})
	addr := serveHTTP(t, func(c *epollhttp.Context) { _, _ = ws.Upgrade(c, nil) })
	dialer := NewDialer(startClientEngine(t), DefaultDialerConfig())
	conn, _, err := dialer.Go("ws://"+addr+"/ws", nil, nil).Wait()
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close(CloseNormal, "")
	_, resp, err := dialer.Go("ws://"+addr+"/other", nil, nil).Wait()
	if !errors.Is(err, ErrBadHandshake) || resp == nil || resp.StatusCode != stdhttp.StatusNotFound {
		t.Fatalf("a request for /other got %v, %v", resp, err)
	}
}

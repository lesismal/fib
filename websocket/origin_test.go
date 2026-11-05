//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package websocket

import (
	"bufio"
	"errors"
	stdhttp "net/http"
	"strings"
	"testing"
)

func TestSameOrigin(t *testing.T) {
	for _, tc := range []struct {
		origin, host string
		want         bool
	}{
		{"http://test", "test", true},
		{"https://TEST", "test", true},
		{"http://test:8080", "test:8080", true},
		{"http://test:8080", "test", false},
		{"http://test/path?q#f", "test", true},
		{"http://user@test", "test", true},
		{"http://evil", "test", false},
		{"http://test.evil", "test", false},
		{"null", "test", false},
		{"test", "test", false},
		{"://test", "test", false},
		{"http://", "", false},
	} {
		if got := sameOrigin([]byte(tc.origin), []byte(tc.host)); got != tc.want {
			t.Errorf("sameOrigin(%q, %q) = %v", tc.origin, tc.host, got)
		}
	}
	request := func(host string, origins ...string) *stdhttp.Request {
		r := &stdhttp.Request{Host: host, Header: stdhttp.Header{}}
		for _, o := range origins {
			r.Header.Add("Origin", o)
		}
		return r
	}
	if !CheckSameOrigin(request("test")) || !CheckSameOrigin(request("test", "http://test")) ||
		CheckSameOrigin(request("test", "http://evil")) ||
		CheckSameOrigin(request("test", "http://test", "http://evil")) {
		t.Fatal("CheckSameOrigin")
	}
}

// The minimal path checks Origin itself, without allocating for a request
// that has none.
func TestMinimalHandshakeOrigin(t *testing.T) {
	h := NewHandler(nil)
	for _, tc := range []struct {
		name, header string
		want         error
	}{
		{"none", "", nil},
		{"same", "Origin: http://test\r\n", nil},
		{"same, other case", "origin: HTTPS://Test\r\n", nil},
		{"cross", "Origin: http://evil\r\n", errOriginRejected},
		{"null", "Origin: null\r\n", errOriginRejected},
		{"two", "Origin: http://test\r\nOrigin: http://evil\r\n", errOriginRejected},
	} {
		_, _, _, complete, err := h.validateMinimalHandshake([]byte(minimalHandshake + tc.header + "\r\n"))
		if !errors.Is(err, tc.want) || err == nil && !complete {
			t.Errorf("%s: complete %v err %v, want %v", tc.name, complete, err, tc.want)
		}
	}
	data := []byte(minimalHandshake + "\r\n")
	if allocs := testing.AllocsPerRun(100, func() { _, _, _, _, _ = h.validateMinimalHandshake(data) }); allocs != 0 {
		t.Fatalf("minimal handshake with no Origin: %v allocs", allocs)
	}
}

// With CheckOrigin nil, a handshake from another site's page is refused with
// 403 on both the minimal path and the one that parses a request, and one
// with no Origin or the server's own is accepted. CheckOrigin returning true
// accepts any.
func TestServerDefaultOriginCheck(t *testing.T) {
	allowAll := DefaultConfig()
	allowAll.CheckOrigin = func(*stdhttp.Request) bool { return true }
	for _, tc := range []struct {
		name    string
		config  Config
		handler Handler
		cross   int
	}{
		{"minimal", DefaultConfig(), HandlerFuncs{}, stdhttp.StatusForbidden},
		{"request", DefaultConfig(), HandlerFuncs{Open: func(*Connection, *stdhttp.Request) {}}, stdhttp.StatusForbidden},
		{"allow all", allowAll, HandlerFuncs{}, stdhttp.StatusSwitchingProtocols},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url := startWebSocketServer(t, tc.config, tc.handler)
			for _, origin := range []struct {
				header string
				status int
			}{
				{"", stdhttp.StatusSwitchingProtocols},
				{"Origin: http://test\r\n", stdhttp.StatusSwitchingProtocols},
				{"Origin: http://evil\r\n", tc.cross},
			} {
				conn := rawWebSocketConn(t, url)
				writeString(t, conn, minimalHandshake+origin.header+"\r\n")
				if resp := readStatus(t, bufio.NewReader(conn)); resp.StatusCode != origin.status {
					t.Fatalf("%q: status %d, want %d", origin.header, resp.StatusCode, origin.status)
				}
			}
		})
	}
}

// Upgrade applies the same default over HTTP/1.1 and to an HTTP/2 extended
// CONNECT, whose host is its :authority.
func TestUpgradeDefaultOriginCheck(t *testing.T) {
	addr, _ := upgradeServer(t, DefaultConfig())
	for _, tc := range []struct {
		origin string
		status int
	}{
		{"http://test", stdhttp.StatusSwitchingProtocols},
		{"http://evil", stdhttp.StatusForbidden},
	} {
		conn := rawWebSocketConn(t, "ws://"+addr+"/ws")
		writeString(t, conn, strings.Replace(minimalHandshake, "/chat", "/ws", 1)+"Origin: "+tc.origin+"\r\n\r\n")
		if resp := readStatus(t, bufio.NewReader(conn)); resp.StatusCode != tc.status {
			t.Fatalf("HTTP/1.1 %s: status %d, want %d", tc.origin, resp.StatusCode, tc.status)
		}
	}

	c, _ := dialH2(t, addr)
	for i, tc := range []struct{ origin, status string }{
		{"http://evil", "403"},
		{"http://test", "200"},
	} {
		stream := uint32(2*i + 1)
		c.headers(stream, false, ":method", "CONNECT", ":protocol", "websocket", ":scheme", "http",
			":path", "/ws", ":authority", "test", "sec-websocket-version", "13", "origin", tc.origin)
		f := c.read()
		for f.stream != stream || f.typ != 0x1 {
			f = c.read()
		}
		if fields := c.decode(f.payload); fields[":status"] != tc.status {
			t.Fatalf("HTTP/2 %s: response %v", tc.origin, fields)
		}
	}
}

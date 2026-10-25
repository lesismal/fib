//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	"os"
	"testing"
	"time"
)

// TestH2ServerSettingsTimeout checks that a client that does not
// acknowledge the server's SETTINGS within SettingsTimeout has its
// connection ended with GOAWAY(SETTINGS_TIMEOUT), and that one that does,
// or a server without the limit, keeps it.
func TestH2ServerSettingsTimeout(t *testing.T) {
	const timeout = 200 * time.Millisecond
	t.Run("unacknowledged", func(t *testing.T) {
		config := DefaultConfig()
		config.SettingsTimeout = timeout
		tc := dialH2(t, serve(t, NewHandlerWithConfig(config, echoHandler())))
		tc.holdSettingsAck = true
		start := time.Now()
		if code := h2covGoAway(t, tc); code != H2SettingsTimeout {
			t.Fatalf("GOAWAY %v, want SETTINGS_TIMEOUT", code)
		}
		if waited := time.Since(start); waited < timeout/2 {
			t.Fatalf("ended after %v", waited)
		}
	})
	t.Run("acknowledged", func(t *testing.T) {
		config := DefaultConfig()
		config.SettingsTimeout = timeout
		tc := dialH2(t, serve(t, NewHandlerWithConfig(config, echoHandler())))
		h2covSync(t, tc)
		time.Sleep(2 * timeout)
		if _, body := tc.response1(1); string(body) != "GET / " {
			t.Fatalf("body %q", body)
		}
	})
	t.Run("no limit", func(t *testing.T) {
		config := DefaultConfig()
		config.SettingsTimeout = -1
		tc := dialH2(t, serve(t, NewHandlerWithConfig(config, echoHandler())))
		tc.holdSettingsAck = true
		h2covSync(t, tc)
		time.Sleep(2 * timeout)
		if _, body := tc.response1(1); string(body) != "GET / " {
			t.Fatalf("body %q", body)
		}
	})
}

// TestH2ServerPingKeepalive checks that a server with a PingInterval sends
// PING to a client that has gone quiet, keeps the connection of one that
// answers, and closes the connection of one that stops answering, failing
// the streams it carries with ErrH2PingTimeout.
func TestH2ServerPingKeepalive(t *testing.T) {
	const interval, timeout = 100 * time.Millisecond, 200 * time.Millisecond
	config := DefaultConfig()
	config.PingInterval = interval
	config.PingTimeout = timeout
	cancelled := make(chan error, 1)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		if c.Request.URL.Path != "/hang" {
			_ = c.Respond(200, "text/plain", []byte("ok"))
			return
		}
		// Never answered: the stream stays open until the connection goes.
		c.Retain()
		c.OnCancel(func(err error) {
			cancelled <- err
			c.Release()
		})
	})))
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/hang")...)

	// A client that answers keeps its connection, however quiet it is
	// otherwise.
	pings := 0
	for start := time.Now(); time.Since(start) < 6*interval+2*timeout; {
		f := tc.read()
		if f.typ != h2FramePing || f.has(h2FlagAck) {
			continue
		}
		if string(f.payload) != string(h2PingPayload[:]) {
			t.Fatalf("PING payload %q", f.payload)
		}
		pings++
		tc.write(append(h2AppendFrameHeader(nil, h2FramePing, h2FlagAck, 0, 8), f.payload...))
	}
	if pings < 2 {
		t.Fatalf("%d PINGs to a quiet client", pings)
	}
	if _, body := tc.response1(3); string(body) != "ok" {
		t.Fatalf("body %q", body)
	}

	// One that stops answering loses it.
	start := time.Now()
	buf := make([]byte, 1024)
	for {
		if _, err := tc.c.Read(buf); err != nil {
			// Closed at once, with whatever was unread: EOF, or a reset.
			if errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatal("the connection of a client that stopped answering stayed open")
			}
			break
		}
	}
	if waited := time.Since(start); waited < timeout || waited > 2*interval+timeout+2*time.Second {
		t.Fatalf("closed %v after the client stopped answering", waited)
	}
	select {
	case err := <-cancelled:
		if !errors.Is(err, ErrH2PingTimeout) {
			t.Fatalf("the open stream's handler was told %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the open stream's handler was not told")
	}
}

// TestH2ServerPingKeepaliveCountsDrainingOutput checks that a client that
// takes the server's output, but whose frames the server does not read
// while that output backs up, is not taken for gone.
func TestH2ServerPingKeepaliveCountsDrainingOutput(t *testing.T) {
	sc := newH2ServerConn(NewHandlerWithConfig(Config{PingInterval: time.Hour}, nil), nil, "")
	sc.mu.Lock()
	sc.armLivenessLocked()
	sc.pingPending = 1000
	sc.pingOut = true
	sc.mu.Unlock()
	// Nothing read, but output shrank (to none, without a connection):
	// alive, and no PING goes out to a connection that has none.
	sc.pingTick()
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.closed || sc.pingOut {
		t.Fatalf("closed %v, PING out %v", sc.closed, sc.pingOut)
	}
	sc.stopLivenessLocked()
}

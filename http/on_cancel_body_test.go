//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"io"
	"net"
	"testing"
	"time"
)

// bodyCancelServer serves a request whose body its handler takes through
// OnBody, behind a middleware that registers only OnCancel, as one releasing
// what it holds for the request would. Neither answers. The body's error and
// the middleware's cancellation are reported on the channels it returns.
func bodyCancelServer(t *testing.T) (addr string, bodyErr, cancelled <-chan error) {
	t.Helper()
	bodyErrs, cancels := make(chan error, 4), make(chan error, 4)
	handler := HandlerFunc(func(c *Context) {
		c.OnBody(func(data []byte, fin bool, err error) {
			if err != nil {
				bodyErrs <- err
			}
		})
	})
	middleware := HandlerFunc(func(c *Context) {
		c.OnCancel(func(err error) { cancels <- err })
		handler(c)
	})
	config := DefaultConfig()
	config.StreamRequestBody = true
	return serve(t, NewHandlerWithConfig(config, middleware)), bodyErrs, cancels
}

// heard waits for an error on ch, and fails the test if none comes.
func heard(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		if err == nil {
			t.Fatalf("%s was given no reason", what)
		}
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("%s never heard of it", what)
		return nil
	}
}

// TestOnCancelHearsBodyFailureH1 checks that a middleware's OnCancel runs for
// an HTTP/1 request whose body, which the handler takes through OnBody, fails
// part way, as OnBody's callback hears: the connection closing, and a
// malformed chunk. OnBody's last call was the last hold on the request, and
// let go with it the request ended unanswered, before the cancellation that
// would have run OnCancel's callbacks.
func TestOnCancelHearsBodyFailureH1(t *testing.T) {
	for _, tc := range []struct {
		name, head, rest string
	}{
		{"closed", "POST / HTTP/1.1\r\nHost: t\r\nContent-Length: 100000\r\n\r\nabc", ""},
		{"malformed", "POST / HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n", "zz\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, bodyErr, cancelled := bodyCancelServer(t)
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if _, err := io.WriteString(conn, tc.head); err != nil {
				t.Fatal(err)
			}
			time.Sleep(100 * time.Millisecond)
			if tc.rest == "" {
				conn.Close()
			} else if _, err := io.WriteString(conn, tc.rest); err != nil {
				t.Fatal(err)
			}
			heard(t, bodyErr, "OnBody")
			heard(t, cancelled, "OnCancel")
		})
	}
}

// TestOnCancelHearsBodyFailureH2 checks the same of an HTTP/2 stream the
// client resets part way through its body, and of one whose connection
// closes.
func TestOnCancelHearsBodyFailureH2(t *testing.T) {
	for _, reset := range []bool{true, false} {
		addr, bodyErr, cancelled := bodyCancelServer(t)
		tc := dialH2(t, addr)
		tc.headers(1, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", "/", "content-length", "100000")
		tc.data(1, false, []byte("abc"))
		time.Sleep(100 * time.Millisecond)
		if reset {
			tc.write(h2AppendRSTStream(nil, 1, H2Cancel))
		} else {
			tc.c.Close()
		}
		heard(t, bodyErr, "OnBody")
		heard(t, cancelled, "OnCancel")
	}
}

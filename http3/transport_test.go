//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"errors"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/quic"
)

// A client does not send a request whose header the server's
// SETTINGS_MAX_FIELD_SECTION_SIZE says it would refuse, nor one whose trailer
// it would; it fails instead.
func TestClientHonoursPeerFieldSectionLimit(t *testing.T) {
	url := startServer(t, Config{MaxHeaderBytes: 600}, func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	})
	client := newClient(t, nil)
	// The first request has the server's SETTINGS arrive.
	if _, err := client.Go(mustRequest(t, "GET", url+"/", nil)).Wait(); err != nil {
		t.Fatal(err)
	}
	req := mustRequest(t, "GET", url+"/big", nil)
	req.Header.Set("X-Big", strings.Repeat("b", 700))
	if _, err := client.Go(req).Wait(); !errors.Is(err, errFieldSectionTooLarge) {
		t.Fatalf("big header: %v", err)
	}
	req = mustRequest(t, "POST", url+"/trailer", strings.NewReader("body"))
	req.Trailer = stdhttp.Header{"X-Big": {strings.Repeat("t", 700)}}
	if _, err := client.Go(req).Wait(); !errors.Is(err, errFieldSectionTooLarge) {
		t.Fatalf("big trailer: %v", err)
	}
	// What fits still goes.
	req = mustRequest(t, "GET", url+"/small", nil)
	req.Header.Set("X-Small", strings.Repeat("s", 100))
	resp, err := client.Go(req).Wait()
	if err != nil || readBody(t, resp) != "ok" {
		t.Fatalf("small header: %v", err)
	}
}

// A server does not send a response header, or a trailer, larger than its
// client's SETTINGS_MAX_FIELD_SECTION_SIZE: the write fails, and the handler
// can answer otherwise, or, for a trailer behind a body already sent, the
// stream is reset so that the client cannot take the body for whole.
func TestServerHonoursPeerFieldSectionLimit(t *testing.T) {
	writeErr := make(chan error, 4)
	url := startServer(t, Config{}, func(c *fibhttp.Context) {
		switch c.Request.URL.Path {
		case "/header":
			err := c.WriteResponse(fibhttp.Response{StatusCode: 200, Header: stdhttp.Header{"X-Big": {strings.Repeat("h", 700)}}})
			writeErr <- err
			_ = c.Respond(stdhttp.StatusInternalServerError, "text/plain", []byte("too big"))
		case "/trailer":
			c.Header().Set("Trailer", "X-Big")
			_, _ = c.WriteString("body")
			c.Flush()
			c.Header().Set("X-Big", strings.Repeat("t", 700))
		default:
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
		}
	})
	client := newClient(t, func(config *ClientConfig) { config.MaxResponseHeaderBytes = 600 })
	if _, err := client.Go(mustRequest(t, "GET", url+"/", nil)).Wait(); err != nil {
		t.Fatal(err)
	}
	// The client's SETTINGS reach the server with its first request, or
	// just after: a request sent once the server has them is answered
	// within them.
	time.Sleep(50 * time.Millisecond)
	resp, err := client.Go(mustRequest(t, "GET", url+"/header", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 500 || readBody(t, resp) != "too big" {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if err := <-writeErr; !errors.Is(err, errFieldSectionTooLarge) {
		t.Fatalf("WriteResponse: %v", err)
	}
	if _, err := client.Go(mustRequest(t, "GET", url+"/trailer", nil)).Wait(); err == nil {
		t.Fatal("a response whose trailer could not be sent passed for whole")
	}
}

// The transport settings reach QUIC, on both ends.
func TestTransportConfigApplied(t *testing.T) {
	transport := TransportConfig{
		KeepAlivePeriod:       time.Second,
		StreamReceiveWindow:   1 << 18,
		ConnReceiveWindow:     1 << 22,
		MaxIncomingUniStreams: 8,
		MaxAckDelay:           5 * time.Millisecond,
		StreamResetBurst:      10,
		StreamResetRate:       2,
		ControlFrameBurst:     20,
		ControlFrameRate:      3,
		KeyUpdateInterval:     time.Minute,
		ActiveConnectionIDs:   3,
	}
	h := NewHandlerWithConfig(Config{Transport: transport}, nil)
	q := h.quic
	if q.KeepAlivePeriod != time.Second || q.StreamReceiveWindow != 1<<18 || q.ConnReceiveWindow != 1<<22 ||
		q.MaxIncomingUniStreams != 8 || q.MaxAckDelay != 5*time.Millisecond ||
		q.StreamResetBurst != 10 || q.StreamResetRate != 2 || q.ControlFrameBurst != 20 || q.ControlFrameRate != 3 ||
		q.KeyUpdateInterval != time.Minute || q.ActiveConnectionIDs != 3 {
		t.Fatalf("server QUIC config %+v", q)
	}
	// The client's, which the same apply sets.
	var c quic.Config
	transport.apply(&c)
	if c.KeepAlivePeriod != q.KeepAlivePeriod || c.MaxAckDelay != q.MaxAckDelay || c.ActiveConnectionIDs != 3 ||
		c.ControlFrameBurst != 20 || c.KeyUpdateInterval != time.Minute || c.StreamReceiveWindow != 1<<18 {
		t.Fatalf("applied %+v", c)
	}
}

// A client with a keep-alive period keeps a connection that carries no
// request past the QUIC idle timeout, and uses it for the next.
func TestClientKeepAlive(t *testing.T) {
	url := startServer(t, Config{MaxIdleTimeout: 300 * time.Millisecond}, func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	})
	client := newClient(t, func(config *ClientConfig) {
		config.MaxIdleTimeout = 300 * time.Millisecond
		config.Transport.KeepAlivePeriod = 50 * time.Millisecond
	})
	conn := func() *clientConn {
		client.mu.Lock()
		defer client.mu.Unlock()
		for _, cc := range client.conns {
			return cc
		}
		return nil
	}
	if _, err := client.Go(mustRequest(t, "GET", url+"/", nil)).Wait(); err != nil {
		t.Fatal(err)
	}
	first := conn()
	time.Sleep(time.Second)
	if _, err := client.Go(mustRequest(t, "GET", url+"/", nil)).Wait(); err != nil {
		t.Fatal(err)
	}
	if conn() != first {
		t.Fatal("the connection idled out despite the keep-alive")
	}
}

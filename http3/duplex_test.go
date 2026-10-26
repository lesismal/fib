//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"errors"
	"io"
	stdhttp "net/http"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
)

// TestServeDuplexStreamsBothWays drives ServeDuplex over HTTP/3 with a
// request whose body is still arriving when the response begins: the server
// reads four bytes, writes and flushes an answer, and only then reads the
// rest. The client gets that answer and writes the rest of its body after
// it, so either direction waiting for the other to finish would time out.
func TestServeDuplexStreamsBothWays(t *testing.T) {
	serverErr := make(chan error, 1)
	url := startServer(t, Config{StreamRequestBody: true}, func(c *fibhttp.Context) {
		if err := c.ServeDuplex(func(d *fibhttp.Duplex) {
			head := make([]byte, 4)
			if _, err := io.ReadFull(d, head); err != nil {
				serverErr <- err
				return
			}
			if string(head) != "ping" {
				serverErr <- errors.New("first body bytes: " + string(head))
				return
			}
			if _, err := d.Write([]byte("one:")); err != nil {
				serverErr <- err
				return
			}
			if err := d.Flush(); err != nil {
				serverErr <- err
				return
			}
			rest, err := io.ReadAll(d)
			if err != nil {
				serverErr <- err
				return
			}
			if string(rest) != "pong" {
				serverErr <- errors.New("rest of body: " + string(rest))
				return
			}
			if _, err := d.Write([]byte("two:" + string(rest))); err != nil {
				serverErr <- err
				return
			}
			serverErr <- nil
		}); err != nil {
			serverErr <- err
		}
	})
	client := newClient(t, func(config *ClientConfig) {
		config.StreamResponseBody = true
		config.StreamResponseBodyThreshold = 0
		config.Timeout = 10 * time.Second
	})

	pr, pw := io.Pipe()
	req := mustRequest(t, stdhttp.MethodPost, url+"/duplex", pr)
	ping := make(chan error, 1)
	go func() {
		_, err := pw.Write([]byte("ping"))
		ping <- err
	}()

	done := make(chan error, 1)
	var got []byte
	client.Do(req, func(cr *fibhttp.ClientResponse, err error) {
		if err != nil {
			done <- err
			return
		}
		// The response has begun while the request body is still open:
		// write the rest of it now.
		if _, err := pw.Write([]byte("pong")); err != nil {
			done <- err
			return
		}
		if err := pw.Close(); err != nil {
			done <- err
			return
		}
		cr.OnBody(func(data []byte, fin bool, err error) {
			if err != nil {
				done <- err
				return
			}
			got = append(got, data...)
			if fin {
				done <- nil
			}
		})
	})
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the response: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the response never ended")
	}
	if err := <-ping; err != nil {
		t.Fatalf("writing the first body bytes: %v", err)
	}
	if string(got) != "one:two:pong" {
		t.Fatalf("response body %q, want %q", got, "one:two:pong")
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("the handler: %v", err)
	}
}

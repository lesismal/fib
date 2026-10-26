//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"context"
	"errors"
	"io"
	stdhttp "net/http"
	"strings"
	"testing"
	"time"
)

// h2cClient is net/http's client speaking HTTP/2 without TLS, with prior
// knowledge, as the tests of the server's cleartext HTTP/2 use.
func h2cClient() *stdhttp.Client {
	var protocols stdhttp.Protocols
	protocols.SetUnencryptedHTTP2(true)
	return &stdhttp.Client{
		Timeout:   10 * time.Second,
		Transport: &stdhttp.Transport{Protocols: &protocols},
	}
}

// TestServeDuplexH2C drives a request over h2c whose body and response are
// used at once: the server reads the first four bytes, answers and flushes,
// and only then reads the rest. The client reads that answer before it sends
// the rest of its body, so either direction waiting for the other to finish
// would time out.
func TestServeDuplexH2C(t *testing.T) {
	serverErr := make(chan error, 1)
	config := DefaultConfig()
	config.StreamRequestBody = true
	handler := NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		err := c.ServeDuplex(func(d *Duplex) {
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
		})
		if err != nil {
			serverErr <- err
		}
	}))
	addr := serve(t, handler)

	pr, pw := io.Pipe()
	req, err := stdhttp.NewRequest(stdhttp.MethodPost, "http://"+addr+"/duplex", pr)
	if err != nil {
		t.Fatal(err)
	}
	ping := make(chan error, 1)
	go func() {
		_, err := pw.Write([]byte("ping"))
		ping <- err
	}()

	client := h2cClient()
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("the request: %v", err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("response protocol %s, want HTTP/2", resp.Proto)
	}
	head := make([]byte, 4)
	if _, err := io.ReadFull(resp.Body, head); err != nil {
		t.Fatalf("the first response bytes: %v", err)
	}
	if string(head) != "one:" {
		t.Fatalf("the first response bytes are %q, want %q", head, "one:")
	}
	if err := <-ping; err != nil {
		t.Fatalf("writing the first body bytes: %v", err)
	}
	if _, err := pw.Write([]byte("pong")); err != nil {
		t.Fatalf("writing the rest of the body: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("closing the body: %v", err)
	}
	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("the rest of the response: %v", err)
	}
	if string(rest) != "two:pong" {
		t.Fatalf("the rest of the response is %q, want %q", rest, "two:pong")
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("the handler: %v", err)
	}
}

// TestServeDuplexWholeBody checks that a body that arrived whole is read
// without waiting, and that the duplex handler can still write a response.
func TestServeDuplexWholeBody(t *testing.T) {
	serverErr := make(chan error, 1)
	handler := NewHandler(HandlerFunc(func(c *Context) {
		if err := c.ServeDuplex(func(d *Duplex) {
			body, err := io.ReadAll(d)
			if err != nil {
				serverErr <- err
				return
			}
			if string(body) != "whole" {
				serverErr <- errors.New("body: " + string(body))
				return
			}
			if _, err := d.Write([]byte("got:" + string(body))); err != nil {
				serverErr <- err
				return
			}
			serverErr <- nil
		}); err != nil {
			serverErr <- err
		}
	}))
	addr := serve(t, handler)
	client := h2cClient()
	defer client.CloseIdleConnections()
	resp, err := client.Post("http://"+addr+"/duplex", "text/plain", strings.NewReader("whole"))
	if err != nil {
		t.Fatalf("the request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("the response: %v", err)
	}
	if string(body) != "got:whole" {
		t.Fatalf("response body %q, want %q", body, "got:whole")
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("the handler: %v", err)
	}
}

// TestServeDuplexRejectsWrittenResponse checks the ways ServeDuplex refuses a
// request it cannot serve: no handler, and a response that has already begun.
func TestServeDuplexRejectsWrittenResponse(t *testing.T) {
	results := make(chan error, 2)
	handler := NewHandler(HandlerFunc(func(c *Context) {
		if err := c.ServeDuplex(nil); err == nil {
			results <- errors.New("ServeDuplex(nil) succeeded")
		} else {
			results <- nil
		}
		c.WriteHeader(stdhttp.StatusOK)
		if err := c.ServeDuplex(func(*Duplex) {}); !errors.Is(err, ErrResponseWritten) {
			results <- errors.New("ServeDuplex after WriteHeader: " + errString(err))
		} else {
			results <- nil
		}
	}))
	addr := serve(t, handler)
	client := h2cClient()
	defer client.CloseIdleConnections()
	resp, err := client.Get("http://" + addr + "/duplex")
	if err != nil {
		t.Fatalf("the request: %v", err)
	}
	resp.Body.Close()
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
}

func errString(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// TestServeDuplexReadUnblocksOnCancel checks that a Duplex blocked in Read is
// woken when the client goes away, and reports the request's error rather
// than waiting forever.
func TestServeDuplexReadUnblocksOnCancel(t *testing.T) {
	started := make(chan struct{})
	readErr := make(chan error, 1)
	config := DefaultConfig()
	config.StreamRequestBody = true
	handler := NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		_ = c.ServeDuplex(func(d *Duplex) {
			close(started)
			_, err := io.ReadFull(d, make([]byte, 2))
			readErr <- err
		})
	}))
	addr := serve(t, handler)

	pr, pw := io.Pipe()
	one := make(chan error, 1)
	go func() {
		_, err := pw.Write([]byte("x"))
		one <- err
	}()
	reqCtx, cancelReq := context.WithCancel(context.Background())
	req, err := stdhttp.NewRequestWithContext(reqCtx, stdhttp.MethodPost, "http://"+addr+"/duplex", pr)
	if err != nil {
		t.Fatal(err)
	}
	client := h2cClient()
	defer client.CloseIdleConnections()
	done := make(chan error, 1)
	go func() {
		resp, err := client.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the duplex handler never ran")
	}
	cancelReq()
	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("Read after the client went away returned no error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Read did not return after the client went away")
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the client request did not return")
	}
}

// TestServeDuplexHTTP1 checks the same exchange over HTTP/1.1, where the
// response may begin before the request body ends; a client that keeps
// sending after it reads the first response bytes gets both directions.
func TestServeDuplexHTTP1(t *testing.T) {
	serverErr := make(chan error, 1)
	config := DefaultConfig()
	config.StreamRequestBody = true
	handler := NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		if err := c.ServeDuplex(func(d *Duplex) {
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
	}))
	addr := serve(t, handler)

	pr, pw := io.Pipe()
	req, err := stdhttp.NewRequest(stdhttp.MethodPost, "http://"+addr+"/duplex", pr)
	if err != nil {
		t.Fatal(err)
	}
	ping := make(chan error, 1)
	go func() {
		_, err := pw.Write([]byte("ping"))
		ping <- err
	}()
	client := &stdhttp.Client{Timeout: 10 * time.Second}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("the request: %v", err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 1 {
		t.Fatalf("response protocol %s, want HTTP/1.x", resp.Proto)
	}
	head := make([]byte, 4)
	if _, err := io.ReadFull(resp.Body, head); err != nil {
		t.Fatalf("the first response bytes: %v", err)
	}
	if string(head) != "one:" {
		t.Fatalf("the first response bytes are %q, want %q", head, "one:")
	}
	if err := <-ping; err != nil {
		t.Fatalf("writing the first body bytes: %v", err)
	}
	if _, err := pw.Write([]byte("pong")); err != nil {
		t.Fatalf("writing the rest of the body: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("closing the body: %v", err)
	}
	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("the rest of the response: %v", err)
	}
	if string(rest) != "two:pong" {
		t.Fatalf("the rest of the response is %q, want %q", rest, "two:pong")
	}
	if err := <-serverErr; err != nil {
		t.Fatalf("the handler: %v", err)
	}
}

// TestServeDuplexCloseEndsResponse checks Close's own contract: it flushes
// what the handler wrote and ends the response there and then, so a client has
// the whole answer while the handler is still working. It covers HTTP/2 and
// HTTP/1.1, where the end of the response has to be flushed by the same call.
func TestServeDuplexCloseEndsResponse(t *testing.T) {
	for _, tc := range []struct {
		name   string
		client *stdhttp.Client
	}{
		{"h2c", h2cClient()},
		{"http1", &stdhttp.Client{Timeout: 10 * time.Second}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.client.CloseIdleConnections()
			release := make(chan struct{}, 1)
			// A fatal above must not leave the handler waiting forever.
			defer func() {
				select {
				case release <- struct{}{}:
				default:
				}
			}()
			results := make(chan error, 2)
			handler := NewHandler(HandlerFunc(func(c *Context) {
				if err := c.ServeDuplex(func(d *Duplex) {
					// Written but not flushed: Close sends it.
					if _, err := d.Write([]byte("bye")); err != nil {
						results <- err
						results <- nil
						return
					}
					err := d.Close()
					results <- err
					if err == nil {
						if err := d.Err(); !errors.Is(err, ErrDuplexClosed) {
							results <- errors.New("Err after Close: " + errString(err))
						} else {
							results <- nil
						}
					}
					// The client must already have the whole response.
					<-release
				}); err != nil {
					results <- err
					results <- nil
				}
			}))
			addr := serve(t, handler)
			resp, err := tc.client.Get("http://" + addr + "/duplex")
			if err != nil {
				t.Fatalf("the response did not end at Close: %v", err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatalf("the response body: %v", err)
			}
			if string(body) != "bye" {
				t.Fatalf("response body %q, want %q", body, "bye")
			}
			release <- struct{}{}
			for i := 0; i < 2; i++ {
				select {
				case err := <-results:
					if err != nil {
						t.Fatalf("the handler: %v", err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("the handler did not return")
				}
			}
		})
	}
}

// TestServeDuplexCloseWithoutWrite checks that closing a duplex that never
// wrote still ends the response with an empty one, rather than leaving the
// client waiting for the handler's return.
func TestServeDuplexCloseWithoutWrite(t *testing.T) {
	release := make(chan struct{}, 1)
	closed := make(chan error, 1)
	defer func() {
		select {
		case release <- struct{}{}:
		default:
		}
	}()
	handler := NewHandler(HandlerFunc(func(c *Context) {
		if err := c.ServeDuplex(func(d *Duplex) {
			closed <- d.Close()
			<-release
		}); err != nil {
			closed <- err
		}
	}))
	addr := serve(t, handler)
	client := &stdhttp.Client{Timeout: 10 * time.Second}
	defer client.CloseIdleConnections()
	resp, err := client.Get("http://" + addr + "/duplex")
	if err != nil {
		t.Fatalf("the response did not end at Close: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("the response body: %v", err)
	}
	if resp.StatusCode != stdhttp.StatusOK || len(body) != 0 {
		t.Fatalf("response %d with body %q, want 200 with none", resp.StatusCode, body)
	}
	release <- struct{}{}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}
}

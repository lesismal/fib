//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"context"
	"io"
	stdhttp "net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// asyncHeaderLater resumes after d on a goroutine of its own, having set
// X-Async in the header, as a hook handing its work to a pool does.
func asyncHeaderLater(d time.Duration, err error) func(int, stdhttp.Header, func(error)) {
	return func(status int, h stdhttp.Header, resume func(error)) {
		go func() {
			time.Sleep(d)
			h.Set("X-Async", "set")
			resume(err)
		}()
	}
}

// TestOnHeaderAsync has a response written each way a handler writes one,
// with its header held by an OnHeaderAsync hook that resumes after the
// handler has returned, and checks over HTTP/1 and HTTP/2 that the header
// carries what the hook set, after the hooks registered after it and before
// those registered before it, and the body is whole.
func TestOnHeaderAsync(t *testing.T) {
	long := strings.Repeat("y", 200*1000) // three times what is held
	write := map[string]struct {
		body string
		fn   func(c *Context)
	}{
		"respond": {"body", func(c *Context) {
			body := []byte("body")
			_ = c.Respond(stdhttp.StatusAccepted, "text/plain", body)
			// The held response has a copy of its own.
			copy(body, "XXXX")
		}},
		"writer": {"bo" + "dy", func(c *Context) {
			c.WriteHeader(stdhttp.StatusAccepted)
			_, _ = io.WriteString(c, "bo")
			c.Flush()
			_, _ = io.WriteString(c, "dy")
		}},
		"long": {long, func(c *Context) {
			// Past what is held, Write waits for the resume.
			c.WriteHeader(stdhttp.StatusAccepted)
			for i := 0; i < len(long); i += 1000 {
				_, _ = io.WriteString(c, long[i:i+1000])
			}
		}},
		"nothing": {"", func(c *Context) { c.WriteHeader(stdhttp.StatusAccepted) }},
	}
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		var order []string
		c.OnHeader(func(status int, h stdhttp.Header) {
			order = append(order, "outer:"+h.Get("X-Async"))
			h.Set("X-Order", strings.Join(order, ","))
		})
		c.OnHeaderAsync(asyncHeaderLater(50*time.Millisecond, nil))
		c.OnHeader(func(status int, h stdhttp.Header) {
			order = append(order, "inner:"+h.Get("X-Async"))
		})
		write[strings.TrimPrefix(c.Request.URL.Path, "/")].fn(c)
	})))
	for _, client := range hookClients(t) {
		for name, tt := range write {
			t.Run(client.proto+"/"+name, func(t *testing.T) {
				resp, err := client.Get("http://" + addr + "/" + name)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				if resp.StatusCode != stdhttp.StatusAccepted || resp.Proto != client.proto {
					t.Fatalf("%s %d", resp.Proto, resp.StatusCode)
				}
				if got := resp.Header.Get("X-Async"); got != "set" {
					t.Fatalf("X-Async %q", got)
				}
				if got := resp.Header.Get("X-Order"); got != "inner:,outer:set" {
					t.Fatalf("X-Order %q", got)
				}
				if string(body) != tt.body {
					t.Fatalf("body of %d bytes, want %d", len(body), len(tt.body))
				}
			})
		}
	}
}

// TestOnHeaderAsyncResumesInPlace checks that a hook that resumes from
// inside its call, as one that decides at once does, sends the response as
// an OnHeader hook would have.
func TestOnHeaderAsyncResumesInPlace(t *testing.T) {
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.OnHeaderAsync(func(status int, h stdhttp.Header, resume func(error)) {
			h.Set("X-Async", "now")
			resume(nil)
			resume(nil)
		})
		if c.Request.URL.Path == "/writer" {
			_, _ = io.WriteString(c, "writer")
			return
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("respond"))
	})))
	for _, client := range hookClients(t) {
		for _, path := range []string{"writer", "respond"} {
			resp, err := client.Get("http://" + addr + "/" + path)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.Header.Get("X-Async") != "now" || string(body) != path {
				t.Fatalf("%s /%s: %v %q", client.proto, path, resp.Header, body)
			}
		}
	}
}

// TestOnHeaderAsyncFails checks that a hook resuming with an error fails the
// response: the HTTP/1 connection closes, and the HTTP/2 stream is reset,
// with nothing sent.
func TestOnHeaderAsyncFails(t *testing.T) {
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.OnHeaderAsync(asyncHeaderLater(10*time.Millisecond, io.ErrUnexpectedEOF))
		if c.Request.URL.Path == "/writer" {
			_, _ = io.WriteString(c, "writer")
			return
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("respond"))
	})))
	for _, client := range hookClients(t) {
		for _, path := range []string{"writer", "respond"} {
			resp, err := client.Get("http://" + addr + "/" + path)
			if err == nil {
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				t.Fatalf("%s /%s answered %d %q", client.proto, path, resp.StatusCode, body)
			}
		}
	}
}

// TestOnHeaderAsyncNoWait checks that WriteNoWait reports above once more
// than is held is written while the header is held, and that the function
// OnWritable is given then runs once the resume has sent it.
func TestOnHeaderAsyncNoWait(t *testing.T) {
	var aboveSeen, writable atomic.Bool
	chunk := bytes.Repeat([]byte("z"), 16<<10)
	const chunks = 8
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.OnHeaderAsync(asyncHeaderLater(50*time.Millisecond, nil))
		c.Retain()
		var write func()
		sent := 0
		write = func() {
			for sent < chunks {
				above, err := c.WriteNoWait(chunk)
				sent++
				if err != nil {
					c.Release()
					return
				}
				if above {
					aboveSeen.Store(true)
					if c.OnWritable(func(err error) {
						writable.Store(true)
						if err != nil {
							c.Release()
							return
						}
						write()
					}) {
						return
					}
				}
			}
			c.Release()
		}
		write()
	})))
	for _, client := range hookClients(t) {
		aboveSeen.Store(false)
		writable.Store(false)
		resp, err := client.Get("http://" + addr + "/")
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(body) != chunks*len(chunk) || resp.Header.Get("X-Async") != "set" {
			t.Fatalf("%s: %d bytes, %v", client.proto, len(body), resp.Header)
		}
		if !aboveSeen.Load() || !writable.Load() {
			t.Fatalf("%s: above %v, writable %v", client.proto, aboveSeen.Load(), writable.Load())
		}
	}
}

// TestOnHeaderAsyncWholeHeldByOnResponse checks that a response an
// OnResponse hook holds whole, which is sent as the handler returns, is held
// at the OnHeaderAsync hook all the same.
func TestOnHeaderAsyncWholeHeldByOnResponse(t *testing.T) {
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.OnHeaderAsync(asyncHeaderLater(20*time.Millisecond, nil))
		c.OnResponse(func(r *Response) { r.Body = append(r.Body, "!"...) })
		_, _ = io.WriteString(c, "whole")
	})))
	for _, client := range hookClients(t) {
		resp, err := client.Get("http://" + addr + "/")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.Header.Get("X-Async") != "set" || string(body) != "whole!" {
			t.Fatalf("%s: %v %q", client.proto, resp.Header, body)
		}
	}
}

// TestOnHeaderAsyncWriteEndsOnCancel checks that a Write waiting for the
// resume of a hook that never resumes, past what is held, returns the
// request's error once the client goes, as it closes an HTTP/1 connection
// and resets an HTTP/2 stream, so that the handler finishes.
func TestOnHeaderAsyncWriteEndsOnCancel(t *testing.T) {
	type result struct {
		n   int
		err error
	}
	started := make(chan struct{}, 1)
	done := make(chan result, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.OnHeaderAsync(func(int, stdhttp.Header, func(error)) {})
		started <- struct{}{}
		chunk := bytes.Repeat([]byte("w"), 1000)
		n := 0
		var err error
		for n < 200*1000 && err == nil {
			var m int
			m, err = c.Write(chunk)
			n += m
		}
		done <- result{n, err}
	})))
	for _, client := range hookClients(t) {
		ctx, cancel := context.WithCancel(context.Background())
		req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, "http://"+addr+"/", nil)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			<-started
			cancel()
		}()
		if resp, err := client.Do(req); err == nil {
			resp.Body.Close()
			t.Fatalf("%s: answered %d", client.proto, resp.StatusCode)
		}
		select {
		case r := <-done:
			if r.err == nil || r.n > maxHeldBody {
				t.Fatalf("%s: wrote %d, %v", client.proto, r.n, r.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: Write still waits after the client went", client.proto)
		}
		cancel()
	}
}

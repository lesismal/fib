//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"context"
	"io"
	stdhttp "net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// upperLater is an AsyncBodyFilter that upper-cases each part of the body
// after d, on a goroutine of its own as a filter handing its work to a pool
// does, or in place when d is 0, and sets X-Upper in the trailer at the end.
// buffer holds the body back until the end; fail fails the call after the
// first. It checks that its calls come one at a time.
type upperLater struct {
	d      time.Duration
	buffer bool
	fail   bool
	held   []byte
	calls  atomic.Int32
	busy   atomic.Int32
	abort  chan error
}

func (u *upperLater) FilterAsync(p []byte, end bool, trailer stdhttp.Header, done func([]byte, error)) {
	if u.busy.Add(1) != 1 {
		panic("two calls at once")
	}
	n := u.calls.Add(1)
	if (trailer != nil) != end {
		panic("a trailer before the end, or none at it")
	}
	run := func() {
		out := bytes.ToUpper(p)
		if u.buffer {
			u.held = append(u.held, out...)
			out = nil
			if end {
				out = u.held
			}
		}
		if end {
			trailer.Set("X-Upper", "done")
		}
		u.busy.Add(-1)
		if u.fail && n > 1 {
			done(nil, io.ErrUnexpectedEOF)
			return
		}
		done(out, nil)
	}
	if u.d == 0 {
		run()
		return
	}
	go func() {
		time.Sleep(u.d)
		run()
	}()
}

func (u *upperLater) Abort(err error) {
	if u.abort != nil {
		u.abort <- err
	}
}

// replaceFilter is a BodyFilter that replaces one byte with another.
type replaceFilter struct{ from, to byte }

func (f replaceFilter) Filter(p []byte) ([]byte, error) {
	return bytes.ReplaceAll(p, []byte{f.from}, []byte{f.to}), nil
}
func (f replaceFilter) Close() ([]byte, error) { return nil, nil }

// asyncBodyWrites are the ways TestOnBodyFilterAsync writes a response, and
// the body each writes.
func asyncBodyWrites() map[string]struct {
	body string
	fn   func(c *Context)
} {
	long := strings.Repeat("y", 200*1000) // three times what is held
	chunk := strings.Repeat("n", 16<<10)
	return map[string]struct {
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
			// Past what is held, Write waits for the filter.
			c.WriteHeader(stdhttp.StatusAccepted)
			for i := 0; i < len(long); i += 1000 {
				_, _ = io.WriteString(c, long[i:i+1000])
			}
		}},
		"nowait": {strings.Repeat(chunk, 8), func(c *Context) {
			c.WriteHeader(stdhttp.StatusAccepted)
			c.Retain()
			sent := 0
			var write func()
			write = func() {
				for sent < 8 {
					above, err := c.WriteNoWait([]byte(chunk))
					sent++
					if err != nil {
						break
					}
					if above && c.OnWritable(func(err error) {
						if err != nil {
							c.Release()
							return
						}
						write()
					}) {
						return
					}
				}
				_ = c.Finish()
				c.Release()
			}
			write()
		}},
		"nothing": {"", func(c *Context) { c.WriteHeader(stdhttp.StatusAccepted) }},
	}
}

// TestOnBodyFilterAsync has a response written each way a handler writes
// one, through an asynchronous filter that resumes later, in place, or holds
// the body until its end, between two synchronous filters and after an
// OnHeaderAsync hook, and checks over HTTP/1 and HTTP/2 that the body is
// what the filters make of it and the trailer has what the filter set.
func TestOnBodyFilterAsync(t *testing.T) {
	write := asyncBodyWrites()
	for _, filter := range []struct {
		name string
		new  func() *upperLater
	}{
		{"later", func() *upperLater { return &upperLater{d: 2 * time.Millisecond} }},
		{"inplace", func() *upperLater { return &upperLater{} }},
		{"buffer", func() *upperLater { return &upperLater{d: time.Millisecond, buffer: true} }},
	} {
		addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
			// Registered first, its filter runs last, on the asynchronous
			// one's output.
			c.OnBodyFilter(func(int, stdhttp.Header) BodyFilter { return replaceFilter{'Y', 'Z'} })
			c.OnHeaderAsync(asyncHeaderLater(5*time.Millisecond, nil))
			c.OnBodyFilterAsync(func(status int, h stdhttp.Header) AsyncBodyFilter {
				h.Set("X-Filtered", "async")
				return filter.new()
			})
			c.OnBodyFilter(func(int, stdhttp.Header) BodyFilter { return replaceFilter{'n', 'm'} })
			write[strings.TrimPrefix(c.Request.URL.Path, "/")].fn(c)
		})))
		for _, client := range hookClients(t) {
			for name, tt := range write {
				t.Run(filter.name+"/"+client.proto+"/"+name, func(t *testing.T) {
					resp, err := client.Get("http://" + addr + "/" + name)
					if err != nil {
						t.Fatal(err)
					}
					body, err := io.ReadAll(resp.Body)
					resp.Body.Close()
					if err != nil {
						t.Fatal(err)
					}
					if resp.StatusCode != stdhttp.StatusAccepted || resp.Header.Get("X-Async") != "set" ||
						resp.Header.Get("X-Filtered") != "async" {
						t.Fatalf("%d %v", resp.StatusCode, resp.Header)
					}
					want := strings.ReplaceAll(strings.ToUpper(strings.ReplaceAll(tt.body, "n", "m")), "Y", "Z")
					if string(body) != want {
						t.Fatalf("body of %d bytes %.20q, want %d %.20q", len(body), body, len(want), want)
					}
					if got := resp.Trailer.Get("X-Upper"); got != "done" {
						t.Fatalf("trailer %v", resp.Trailer)
					}
				})
			}
		}
	}
}

// TestOnBodyFilterAsyncHead checks that a response to HEAD, which sends no
// body, aborts the filter rather than calling it.
func TestOnBodyFilterAsyncHead(t *testing.T) {
	aborted := make(chan error, 2)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.OnBodyFilterAsync(func(int, stdhttp.Header) AsyncBodyFilter {
			return &upperLater{d: time.Millisecond, abort: aborted}
		})
		_, _ = io.WriteString(c, "body")
	})))
	for _, client := range hookClients(t) {
		resp, err := client.Head("http://" + addr + "/")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		select {
		case <-aborted:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: the filter was not aborted", client.proto)
		}
	}
}

// TestOnBodyFilterAsyncFails checks that a call failing fails the
// response: the HTTP/1 connection closes and the HTTP/2 stream is reset, a
// response sent whole sending nothing.
func TestOnBodyFilterAsyncFails(t *testing.T) {
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.OnBodyFilterAsync(func(int, stdhttp.Header) AsyncBodyFilter {
			return &upperLater{d: time.Millisecond, fail: true}
		})
		if c.Request.URL.Path == "/respond" {
			// The first call, of a filter that fails from its second.
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("respond"))
			return
		}
		long := strings.Repeat("y", 200*1000)
		for i := 0; i < len(long); i += 1000 {
			if _, err := io.WriteString(c, long[i:i+1000]); err != nil {
				return
			}
		}
	})))
	for _, client := range hookClients(t) {
		resp, err := client.Get("http://" + addr + "/writer")
		if err == nil {
			_, err = io.ReadAll(resp.Body)
			resp.Body.Close()
		}
		if err == nil {
			t.Fatalf("%s: the failed body read whole", client.proto)
		}
	}
}

// TestOnBodyFilterAsyncCancel checks that a client going while a call is
// under way ends a Write waiting for room, and that the response ends, its
// filter aborted, once the call comes back.
func TestOnBodyFilterAsyncCancel(t *testing.T) {
	started := make(chan struct{}, 1)
	done := make(chan error, 1)
	aborted := make(chan error, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.OnBodyFilterAsync(func(int, stdhttp.Header) AsyncBodyFilter {
			return &upperLater{d: 300 * time.Millisecond, abort: aborted}
		})
		started <- struct{}{}
		chunk := bytes.Repeat([]byte("w"), 1000)
		var err error
		for n := 0; n < 1000*1000 && err == nil; n += 1000 {
			_, err = c.Write(chunk)
		}
		done <- err
	})))
	for _, client := range hookClients(t) {
		if runtime.GOOS == "windows" && client.proto == "HTTP/1.1" {
			t.Logf("%s skipped: Windows arms a connection's read only between its rounds; see fib.HangupHandler", client.proto)
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodGet, "http://"+addr+"/", nil)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			<-started
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()
		if resp, err := client.Do(req); err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		select {
		case err := <-done:
			if err == nil {
				t.Fatalf("%s: the handler wrote it all", client.proto)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: Write still waits after the client went", client.proto)
		}
		select {
		case err := <-aborted:
			if err == nil {
				t.Fatalf("%s: aborted without an error", client.proto)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: the filter was not aborted", client.proto)
		}
		cancel()
	}
}

// TestOnBodyFilterAsyncNil checks that a hook returning nil leaves the body
// as it is, sendfile and all.
func TestOnBodyFilterAsyncNil(t *testing.T) {
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.OnBodyFilterAsync(func(int, stdhttp.Header) AsyncBodyFilter { return nil })
		_, _ = io.WriteString(c, "plain")
	})))
	for _, client := range hookClients(t) {
		resp, err := client.Get("http://" + addr + "/")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(body) != "plain" {
			t.Fatalf("%s: %q", client.proto, body)
		}
	}
}

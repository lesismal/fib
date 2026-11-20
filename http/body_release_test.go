//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"io"
	stdhttp "net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// reuseFilter passes a body through a buffer of its own, which Close reuses
// for its tail, as a compressor does; Release scribbles over the buffer, as
// the next response handed it from a pool would. Anything sent from the
// buffer after it was reused or released arrives garbled.
type reuseFilter struct {
	buf      []byte
	released atomic.Int32
	count    *atomic.Int64
}

func (f *reuseFilter) Filter(p []byte) ([]byte, error) {
	if f.released.Load() != 0 {
		panic("Filter after Release")
	}
	f.buf = append(f.buf[:0], p...)
	return f.buf, nil
}

func (f *reuseFilter) Close() ([]byte, error) {
	f.buf = append(f.buf[:0], "<end>"...)
	return f.buf, nil
}

func (f *reuseFilter) Release() {
	if f.released.Add(1) != 1 {
		panic("Release twice")
	}
	buf := f.buf[:cap(f.buf)]
	for i := range buf {
		buf[i] = '#'
	}
	f.count.Add(1)
}

// TestBodyFilterRelease writes a response each way a handler writes one
// through a filter that reuses its buffer and scribbles over it on Release,
// with no asynchronous filter and after one, over HTTP/1 and HTTP/2, and
// checks that the body arrives whole and that each filter is released once,
// after what it returned was sent or copied.
func TestBodyFilterRelease(t *testing.T) {
	write := asyncBodyWrites()
	for _, async := range []bool{false, true} {
		var made, released atomic.Int64
		addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
			// Registered first, its filter runs last.
			c.OnBodyFilter(func(int, stdhttp.Header) BodyFilter {
				made.Add(1)
				return &reuseFilter{count: &released}
			})
			if async {
				c.OnBodyFilterAsync(func(int, stdhttp.Header) AsyncBodyFilter { return &upperLater{} })
			}
			write[strings.TrimPrefix(c.Request.URL.Path, "/")].fn(c)
		})))
		for _, client := range hookClients(t) {
			for name, tt := range write {
				resp, err := client.Get("http://" + addr + "/" + name)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				want := tt.body
				if async {
					want = strings.ToUpper(want)
				}
				want += "<end>"
				if string(body) != want {
					t.Fatalf("async %v %s/%s: body of %d bytes %.20q...%q, want %d", async, client.proto, name,
						len(body), body, body[max(0, len(body)-8):], len(want))
				}
			}
		}
		deadline := time.Now().Add(5 * time.Second)
		for released.Load() != made.Load() && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if released.Load() != made.Load() || made.Load() == 0 {
			t.Fatalf("async %v: %d filters released of %d", async, released.Load(), made.Load())
		}
	}
}

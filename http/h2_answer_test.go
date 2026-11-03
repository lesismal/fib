//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"fmt"
	stdhttp "net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestH2QueuedAnswersKeepHeaderOrder answers many streams of one connection
// while its mu is held, so that every response is queued by postAnswer rather
// than sent, and has each handler wreck what it answered with as soon as the
// call returns: its header map and the request's own body it echoed. The
// client's HPACK decoder fails on a header block decoded out of the order the
// encoder produced it, and each response must still carry what its handler
// gave.
func TestH2QueuedAnswersKeepHeaderOrder(t *testing.T) {
	const n = 64
	var (
		conn    atomic.Pointer[h2ServerConn]
		arrived sync.WaitGroup
		held    = make(chan struct{})
		gone    sync.WaitGroup
		errs    = make(chan error, n)
	)
	arrived.Add(n)
	gone.Add(n)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		defer gone.Done()
		conn.Store(c.stream.sc)
		arrived.Done()
		<-held
		id := c.Request.URL.Path[1:]
		i, _ := strconv.Atoi(id)
		header := stdhttp.Header{
			"X-Id":                         {id},
			"X-Group":                      {strconv.Itoa(i % 7)},
			"X-Churn-" + strconv.Itoa(i%5): {strings.Repeat(id, 40)},
			"Set-Cookie":                   {"s=" + id},
		}
		body := c.Body()
		if err := c.WriteResponse(Response{StatusCode: 200, Header: header, Body: body}); err != nil {
			errs <- fmt.Errorf("stream %s: %v", id, err)
		}
		clear(header)
		header["X-Id"] = []string{"wrecked"}
		for j := range body {
			body[j] = '!'
		}
	})))
	tc := dialH2(t, addr)
	for i := 0; i < n; i++ {
		id := uint32(2*i + 1)
		tc.headers(id, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", "/"+strconv.Itoa(i))
		tc.data(id, true, []byte("body-"+strconv.Itoa(i)))
	}
	waitGroup(t, &arrived)
	sc := conn.Load()
	sc.mu.Lock()
	close(held)
	waitGroup(t, &gone)
	queued := sc.answers.Load() != nil
	sc.unlock()
	if !queued {
		t.Fatal("no answer was queued while mu was held")
	}
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	for i := 0; i < n; i++ {
		header, body := tc.response(uint32(2*i + 1))
		id := strconv.Itoa(i)
		if header[":status"] != "200" || header["x-id"] != id || header["x-group"] != strconv.Itoa(i%7) ||
			header["x-churn-"+strconv.Itoa(i%5)] != strings.Repeat(id, 40) || header["set-cookie"] != "s="+id {
			t.Fatalf("stream %d: header %v", 2*i+1, header)
		}
		if string(body) != "body-"+id {
			t.Fatalf("stream %d: body %q", 2*i+1, body)
		}
	}
}

// waitGroup waits for wg, failing the test after a while instead of hanging.
func waitGroup(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the handlers")
	}
}

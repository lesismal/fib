//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"fmt"
	"io"
	stdhttp "net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	fibhttp "github.com/lesismal/fib/http"
)

// TestRequestStreamRecycleResets keeps requestStream.recycle in step with
// the stream: a field added to it has to be cleared there, and then listed
// here.
func TestRequestStreamRecycleResets(t *testing.T) {
	var fields []string
	typ := reflect.TypeFor[requestStream]()
	for i := 0; i < typ.NumField(); i++ {
		fields = append(fields, typ.Field(i).Name)
	}
	cleared := []string{"sc", "s", "parser", "req", "body", "declared", "trailers", "done", "streamed", "tunnel",
		"mu", "remoteDone", "responded", "closed", "upgraded", "streaming", "expected", "timedOut", "writeTimer", "collectTimer", "block", "values",
		"pooled", "refs", "nextDead", "headerFromPool"}
	sort.Strings(fields)
	sort.Strings(cleared)
	if !reflect.DeepEqual(fields, cleared) {
		t.Fatalf("requestStream has fields %v; recycle clears %v", fields, cleared)
	}
}

// TestRecycledStreamsKeepRequestsApart serves many concurrent requests on
// one HTTP/3 connection, half of them retained and answered from another
// goroutine after a pause, with the server recycling its request streams,
// and checks that every handler sees its own request throughout and every
// response is its own.
func TestRecycledStreamsKeepRequestsApart(t *testing.T) {
	var retained sync.WaitGroup
	t.Cleanup(retained.Wait)
	describe := func(r *stdhttp.Request, body []byte) string {
		return fmt.Sprintf("%s %s n=%s x=%s %s", r.Method, r.URL.Path, r.URL.Query().Get("n"), r.Header.Get("X-N"), body)
	}
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
		body, _ := io.ReadAll(c.Request.Body)
		n := strings.TrimPrefix(c.Request.URL.Path, "/r")
		if len(n)%2 == 0 {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(describe(c.Request, body)))
			return
		}
		c.Retain()
		retained.Add(1)
		go func() {
			defer retained.Done()
			time.Sleep(time.Millisecond)
			want := describe(c.Request, body)
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(want))
			// Until its last Release the request is still this handler's,
			// response written or not.
			time.Sleep(time.Millisecond)
			if got := describe(c.Request, body); got != want {
				t.Errorf("request changed under its handler before Release: %q, was %q", got, want)
			}
			c.Release()
		}()
	})
	client := newClient(t, nil)
	var wg sync.WaitGroup
	errs := make(chan error, 1000)
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			n := strconv.Itoa(i)
			method, body := stdhttp.MethodGet, ""
			if i%3 == 0 {
				method, body = stdhttp.MethodPost, strings.Repeat(n, 1+i%7)
			}
			req, _ := stdhttp.NewRequest(method, fmt.Sprintf("%s/r%s?n=%s", base, n, n), strings.NewReader(body))
			req.Header.Set("X-N", n)
			resp, err := client.Go(req).Wait()
			if err != nil {
				errs <- err
				return
			}
			got, _ := io.ReadAll(resp.Body)
			if want := fmt.Sprintf("%s /r%s n=%s x=%s %s", method, n, n, n, body); string(got) != want {
				errs <- fmt.Errorf("request %d: got %q, want %q", i, got, want)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestStreamRequestBodyRecyclesBodylessRequests serves a server that streams
// request bodies, as a proxy's listener does, and holds it to recycling a
// request whose body never streamed: with Config.StreamRequestBody set,
// every request used to be left to the collector, bodyless GETs included,
// which fib measured at 1.19M requests a second against the 1.56M the pool
// served. The address of each request is what the handler saw; a recycled
// requestStream serves the next request at the same one.
func TestStreamRequestBodyRecyclesBodylessRequests(t *testing.T) {
	var (
		mu   sync.Mutex
		seen = map[unsafe.Pointer]struct{}{}
	)
	base := startServer(t, Config{StreamRequestBody: true, StreamRequestBodyThreshold: 1}, func(c *fibhttp.Context) {
		_, _ = io.ReadAll(c.Request.Body)
		mu.Lock()
		seen[unsafe.Pointer(c.Request)] = struct{}{}
		mu.Unlock()
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	})
	client := newClient(t, nil)
	const requests = 64
	for i := 0; i < requests; i++ {
		req, err := stdhttp.NewRequest(stdhttp.MethodGet, base+"/g?n="+strconv.Itoa(i), nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Go(req).Wait()
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		_, _ = io.ReadAll(resp.Body)
	}
	mu.Lock()
	distinct := len(seen)
	mu.Unlock()
	if distinct == requests {
		t.Fatalf("%d bodyless requests went through %d request objects: none was recycled", requests, distinct)
	}
}

// TestStreamedBodiesKeepRequestsOutOfThePool holds a server that streams
// request bodies to leaving the requests whose bodies streamed out of the
// pool: their handlers may read the body past the response, so no later
// request may be given the same object.
func TestStreamedBodiesKeepRequestsOutOfThePool(t *testing.T) {
	var (
		mu   sync.Mutex
		seen = map[unsafe.Pointer]struct{}{}
	)
	base := startServer(t, Config{StreamRequestBody: true, StreamRequestBodyThreshold: 1}, func(c *fibhttp.Context) {
		// Read the body so that it streams; its size is not the subject here,
		// and a streamed body may arrive in pieces on a slow runner.
		_, _ = io.Copy(io.Discard, c.Request.Body)
		mu.Lock()
		seen[unsafe.Pointer(c.Request)] = struct{}{}
		mu.Unlock()
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	})
	client := newClient(t, nil)
	const uploads = 8
	for i := 0; i < uploads; i++ {
		req, err := stdhttp.NewRequest(stdhttp.MethodPost, base+"/u?n="+strconv.Itoa(i), strings.NewReader(strings.Repeat("x", 4096)))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Go(req).Wait()
		if err != nil {
			t.Fatalf("upload %d: %v", i, err)
		}
		_, _ = io.ReadAll(resp.Body)
	}
	mu.Lock()
	distinct := len(seen)
	mu.Unlock()
	if distinct != uploads {
		t.Fatalf("%d requests whose bodies streamed went through %d request objects: one was recycled", uploads, distinct)
	}
}

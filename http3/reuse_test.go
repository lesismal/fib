//go:build linux || darwin || windows

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
		"mu", "remoteDone", "responded", "closed", "upgraded", "streaming", "expected", "block", "values",
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

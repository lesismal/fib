//go:build linux || darwin || windows

package middleware_test

import (
	stdhttp "net/http"
	"strings"
	"sync"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware"
	"github.com/lesismal/fib/middleware/mwtest"
)

// TestChainOrder checks that the first middleware is outermost: it sees the
// request first and the response last.
func TestChainOrder(t *testing.T) {
	// The handlers run on the server's goroutines, which the response the
	// test reads orders nothing with, so seen is shared under a lock.
	var (
		mu   sync.Mutex
		seen []string
	)
	mark := func(name string) middleware.Middleware {
		return func(next fibhttp.Handler) fibhttp.Handler {
			return fibhttp.HandlerFunc(func(c *fibhttp.Context) {
				mu.Lock()
				seen = append(seen, name)
				mu.Unlock()
				c.OnHeader(func(_ int, h stdhttp.Header) { h.Add("X-Seen", name) })
				next.ServeHTTP(c)
			})
		}
	}
	handler := middleware.Chain(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	}), mark("a"), nil, mark("b"))
	url := mwtest.Serve(t, handler)
	c := mwtest.Clients(t)[0]
	resp, _ := c.Do(t, "GET", url, nil, "")
	mu.Lock()
	got := strings.Join(seen, ",")
	mu.Unlock()
	if got != "a,b" {
		t.Errorf("requests went through %s, want a,b", got)
	}
	if got := strings.Join(resp.Header["X-Seen"], ","); got != "b,a" {
		t.Errorf("responses went through %s, want b,a", got)
	}
}

func TestAddVary(t *testing.T) {
	h := stdhttp.Header{"Vary": {"Origin, accept-encoding"}}
	middleware.AddVary(h, "Accept-Encoding")
	middleware.AddVary(h, "Cookie")
	if got := strings.Join(h["Vary"], "|"); got != "Origin, accept-encoding|Cookie" {
		t.Errorf("Vary = %q", got)
	}
}

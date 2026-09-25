//go:build linux || darwin || windows

package middleware_test

import (
	stdhttp "net/http"
	"strings"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware"
	"github.com/lesismal/fib/middleware/internal/mwtest"
)

// TestChainOrder checks that the first middleware is outermost: it sees the
// request first and the response last.
func TestChainOrder(t *testing.T) {
	var seen []string
	mark := func(name string) middleware.Middleware {
		return func(next fibhttp.Handler) fibhttp.Handler {
			return fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
				seen = append(seen, name)
				c.OnHeader(func(_ int, h stdhttp.Header) { h.Add("X-Seen", name) })
				next.ServeHTTP(c, r)
			})
		}
	}
	handler := middleware.Chain(fibhttp.HandlerFunc(func(c *fibhttp.Context, _ *stdhttp.Request) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	}), mark("a"), nil, mark("b"))
	url := mwtest.Serve(t, handler)
	c := mwtest.Clients(t)[0]
	resp, _ := c.Do(t, "GET", url, nil, "")
	if got := strings.Join(seen, ","); got != "a,b" {
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

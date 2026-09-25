//go:build linux || darwin || windows

package recover_test

import (
	stdhttp "net/http"
	"strings"
	"sync"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/internal/mwtest"
	"github.com/lesismal/fib/middleware/recover"
)

func TestRecover(t *testing.T) {
	var mu sync.Mutex
	var logged []string
	handler := recover.New(recover.Config{
		Log: func(r *stdhttp.Request, recovered any, stack []byte) {
			mu.Lock()
			defer mu.Unlock()
			if len(stack) == 0 {
				t.Error("no stack trace")
			}
			logged = append(logged, r.URL.Path)
		},
	})(fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
		if r.URL.Path == "/panic" {
			panic("boom")
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	}))
	url := mwtest.Serve(t, handler)
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		resp, body := c.Do(t, "GET", url+"/panic", nil, "")
		if resp.StatusCode != stdhttp.StatusInternalServerError || !strings.Contains(body, "Internal Server Error") {
			t.Errorf("panic answered %d %q", resp.StatusCode, body)
		}
		// The connection lives on to serve the next request.
		resp, body = c.Do(t, "GET", url+"/ok", nil, "")
		if resp.StatusCode != stdhttp.StatusOK || body != "ok" {
			t.Errorf("after a panic: %d %q", resp.StatusCode, body)
		}
	})
	mu.Lock()
	defer mu.Unlock()
	if len(logged) != 2 {
		t.Errorf("logged %v, want a panic for each protocol", logged)
	}
}

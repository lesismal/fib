//go:build linux || darwin || windows

package pprof_test

import (
	stdhttp "net/http"
	"strings"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/internal/mwtest"
	"github.com/lesismal/fib/middleware/pprof"
)

func TestPprof(t *testing.T) {
	next := fibhttp.HandlerFunc(func(c *fibhttp.Context, _ *stdhttp.Request) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("app"))
	})
	for _, prefix := range []string{"", "/admin/pprof/"} {
		url := mwtest.Serve(t, pprof.New(pprof.Config{Prefix: prefix})(next))
		base := "/" + strings.Trim(prefix, "/")
		if prefix == "" {
			base = pprof.DefaultPrefix
		}
		mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
			resp, body := c.Do(t, "GET", url+base+"/", nil, "")
			if resp.StatusCode != stdhttp.StatusOK || !strings.Contains(body, "goroutine") {
				t.Errorf("index: %d %.80q", resp.StatusCode, body)
			}
			resp, body = c.Do(t, "GET", url+base+"/goroutine?debug=1", nil, "")
			if resp.StatusCode != stdhttp.StatusOK || !strings.Contains(body, "goroutine profile") {
				t.Errorf("goroutine: %d %.80q", resp.StatusCode, body)
			}
			resp, body = c.Do(t, "GET", url+base+"/cmdline", nil, "")
			if resp.StatusCode != stdhttp.StatusOK || body == "" {
				t.Errorf("cmdline: %d %q", resp.StatusCode, body)
			}
			resp, body = c.Do(t, "GET", url+base+"/profile?seconds=1", nil, "")
			if resp.StatusCode != stdhttp.StatusOK || len(body) == 0 {
				t.Errorf("profile: %d, %d bytes", resp.StatusCode, len(body))
			}
			resp, _ = c.Do(t, "GET", url+base, nil, "")
			if resp.StatusCode != stdhttp.StatusMovedPermanently || resp.Header.Get("Location") != base+"/" {
				t.Errorf("prefix without slash: %d to %q", resp.StatusCode, resp.Header.Get("Location"))
			}
			for _, path := range []string{"/", base + "ile"} {
				if _, body := c.Do(t, "GET", url+path, nil, ""); body != "app" {
					t.Errorf("%s did not reach the handler: %.40q", path, body)
				}
			}
		})
	}
}

//go:build linux || darwin || windows

package responsetime_test

import (
	"io"
	stdhttp "net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/internal/mwtest"
	"github.com/lesismal/fib/middleware/responsetime"
)

func TestResponseTime(t *testing.T) {
	url := mwtest.Serve(t, responsetime.New()(fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
		time.Sleep(5 * time.Millisecond)
		if r.URL.Path == "/writer" {
			_, _ = io.WriteString(c, "ok")
			return
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
	})))
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		for _, path := range []string{"/respond", "/writer"} {
			resp, _ := c.Do(t, "GET", url+path, nil, "")
			value := resp.Header.Get(responsetime.DefaultHeader)
			ms, err := strconv.ParseFloat(strings.TrimSuffix(value, "ms"), 64)
			if err != nil || !strings.HasSuffix(value, "ms") || ms < 5 {
				t.Errorf("%s: %s = %q", path, responsetime.DefaultHeader, value)
			}
		}
	})
}

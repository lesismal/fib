//go:build linux || darwin || windows

package requestid_test

import (
	stdhttp "net/http"
	"regexp"
	"strings"
	"testing"

	fibhttp "github.com/lesismal/fib/go/http"
	"github.com/lesismal/fib/go/middleware/internal/mwtest"
	"github.com/lesismal/fib/go/middleware/requestid"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestRequestID(t *testing.T) {
	url := mwtest.Serve(t, requestid.New()(fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(requestid.FromRequest(r)))
	})))
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		resp, body := c.Do(t, "GET", url, nil, "")
		id := resp.Header.Get(requestid.DefaultHeader)
		if !uuidPattern.MatchString(id) || body != id {
			t.Errorf("new id: header %q, handler saw %q", id, body)
		}
		resp2, _ := c.Do(t, "GET", url, nil, "")
		if resp2.Header.Get(requestid.DefaultHeader) == id {
			t.Error("two requests got the same id")
		}
		resp, body = c.Do(t, "GET", url, stdhttp.Header{"X-Request-Id": {"abc-123"}}, "")
		if got := resp.Header.Get(requestid.DefaultHeader); got != "abc-123" || body != "abc-123" {
			t.Errorf("client's id: header %q, handler saw %q", got, body)
		}
		bad := strings.Repeat("x", 200)
		resp, _ = c.Do(t, "GET", url, stdhttp.Header{"X-Request-Id": {bad}}, "")
		if got := resp.Header.Get(requestid.DefaultHeader); !uuidPattern.MatchString(got) {
			t.Errorf("an overlong id was kept: %q", got)
		}
	})
}

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package mwtest

import (
	stdhttp "net/http"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
)

// TestServeAndRun checks that Serve answers over both clients Run hands a
// test, that Do sends the request's headers and body, and that a redirect
// comes back to the test rather than being followed.
func TestServeAndRun(t *testing.T) {
	url := Serve(t, fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		if c.Request.URL.Path == "/redirect" {
			_ = c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusFound, Header: stdhttp.Header{"Location": {"/"}}})
			return
		}
		body := make([]byte, 64)
		n, _ := c.Request.Body.Read(body)
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(c.Request.Header.Get("X-Test")+":"+string(body[:n])))
	}))
	names := map[string]bool{}
	Run(t, func(t *testing.T, c Client) {
		names[c.Name] = true
		resp, body := c.Do(t, "POST", url+"/", stdhttp.Header{"X-Test": {"hi"}}, "body")
		if resp.StatusCode != stdhttp.StatusOK || body != "hi:body" {
			t.Errorf("got %d %q", resp.StatusCode, body)
		}
		if resp.ProtoMajor != map[string]int{"HTTP/1.1": 1, "HTTP/2": 2}[c.Name] {
			t.Errorf("%s spoke %s", c.Name, resp.Proto)
		}
		if resp, _ := c.Do(t, "GET", url+"/redirect", nil, ""); resp.StatusCode != stdhttp.StatusFound {
			t.Errorf("redirect: got %d", resp.StatusCode)
		}
	})
	if len(names) != 2 {
		t.Errorf("Run ran %v", names)
	}
}

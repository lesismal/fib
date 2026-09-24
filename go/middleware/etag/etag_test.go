//go:build linux || darwin || windows

package etag_test

import (
	stdhttp "net/http"
	"testing"

	fibhttp "github.com/lesismal/fib/go/http"
	"github.com/lesismal/fib/go/middleware/etag"
	"github.com/lesismal/fib/go/middleware/internal/mwtest"
)

func TestETag(t *testing.T) {
	url := mwtest.Serve(t, etag.New()(fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
		switch r.URL.Path {
		case "/own":
			c.Header().Set("ETag", `"v1"`)
			_, _ = c.Write([]byte("mine"))
		case "/missing":
			_ = c.Respond(stdhttp.StatusNotFound, "text/plain", []byte("missing"))
		default:
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("hello"))
		}
	})))
	want := etag.Generate([]byte("hello"), false)
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		resp, body := c.Do(t, "GET", url, nil, "")
		if got := resp.Header.Get("Etag"); got != want || body != "hello" {
			t.Fatalf("ETag %q, body %q; want %q", got, body, want)
		}
		for _, match := range []string{want, "W/" + want, `"other", ` + want, "*"} {
			resp, body = c.Do(t, "GET", url, stdhttp.Header{"If-None-Match": {match}}, "")
			if resp.StatusCode != stdhttp.StatusNotModified || body != "" || resp.Header.Get("Etag") != want {
				t.Errorf("If-None-Match %s: %d %q, ETag %q", match, resp.StatusCode, body, resp.Header.Get("Etag"))
			}
		}
		resp, _ = c.Do(t, "GET", url, stdhttp.Header{"If-None-Match": {`"other"`}}, "")
		if resp.StatusCode != stdhttp.StatusOK {
			t.Errorf("a different ETag answered %d", resp.StatusCode)
		}
		resp, _ = c.Do(t, "HEAD", url, nil, "")
		if got := resp.Header.Get("Etag"); got != want {
			t.Errorf("HEAD: ETag %q, want %q", got, want)
		}
		resp, _ = c.Do(t, "GET", url+"/own", stdhttp.Header{"If-None-Match": {`"v1"`}}, "")
		if resp.StatusCode != stdhttp.StatusNotModified {
			t.Errorf("the handler's own ETag answered %d", resp.StatusCode)
		}
		resp, _ = c.Do(t, "GET", url+"/missing", nil, "")
		if resp.Header.Get("Etag") != "" {
			t.Error("a 404 got an ETag")
		}
		resp, _ = c.Do(t, "POST", url, nil, "")
		if resp.Header.Get("Etag") != "" {
			t.Error("a POST got an ETag")
		}
	})
}

func TestMatches(t *testing.T) {
	for _, tc := range []struct {
		list, tag string
		want      bool
	}{
		{`"a"`, `"a"`, true},
		{`W/"a"`, `"a"`, true},
		{`"a"`, `W/"a"`, true},
		{`"b", "a"`, `"a"`, true},
		{`"a,b"`, `"a"`, false},
		{`"a,b"`, `"a,b"`, true},
		{`junk, "a"`, `"a"`, true},
		{`"b"`, `"a"`, false},
		{``, `"a"`, false},
		{`"unterminated`, `"a"`, false},
	} {
		if got := etag.Matches(tc.list, tc.tag); got != tc.want {
			t.Errorf("Matches(%q, %q) = %v", tc.list, tc.tag, got)
		}
	}
}

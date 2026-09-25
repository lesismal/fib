//go:build linux || darwin || windows

package csrf_test

import (
	stdhttp "net/http"
	"strings"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/csrf"
	"github.com/lesismal/fib/middleware/internal/mwtest"
)

func TestCSRF(t *testing.T) {
	url := mwtest.Serve(t, csrf.New()(fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(csrf.Token(r)))
	})))
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		resp, token := c.Do(t, "GET", url, nil, "")
		var cookie *stdhttp.Cookie
		for _, got := range resp.Cookies() {
			if got.Name == csrf.DefaultCookieName {
				cookie = got
			}
		}
		if cookie == nil || cookie.Value != token || token == "" {
			t.Fatalf("GET: token %q, cookies %v", token, resp.Cookies())
		}
		if cookie.SameSite != stdhttp.SameSiteLaxMode || cookie.MaxAge != 3600 || cookie.Path != "/" {
			t.Errorf("cookie %+v", cookie)
		}
		withCookie := func(header stdhttp.Header) stdhttp.Header {
			header.Set("Cookie", cookie.Name+"="+cookie.Value)
			return header
		}
		// A request that already has the cookie keeps it.
		resp, body := c.Do(t, "GET", url, withCookie(stdhttp.Header{}), "")
		if body != token || len(resp.Cookies()) != 0 {
			t.Errorf("GET with the cookie: token %q, cookies %v", body, resp.Cookies())
		}
		for _, tc := range []struct {
			name   string
			header stdhttp.Header
			want   int
		}{
			{"token", withCookie(stdhttp.Header{"X-Csrf-Token": {token}}), stdhttp.StatusOK},
			{"no token", withCookie(stdhttp.Header{}), stdhttp.StatusForbidden},
			{"no cookie", stdhttp.Header{"X-Csrf-Token": {token}}, stdhttp.StatusForbidden},
			{"wrong token", withCookie(stdhttp.Header{"X-Csrf-Token": {strings.Repeat("A", len(token))}}), stdhttp.StatusForbidden},
			{"cross-site", withCookie(stdhttp.Header{"X-Csrf-Token": {token}, "Sec-Fetch-Site": {"cross-site"}}), stdhttp.StatusForbidden},
			{"other origin", withCookie(stdhttp.Header{"X-Csrf-Token": {token}, "Origin": {"https://evil.example"}}), stdhttp.StatusForbidden},
		} {
			resp, _ := c.Do(t, "POST", url, tc.header, "")
			if resp.StatusCode != tc.want {
				t.Errorf("POST with %s: %d, want %d", tc.name, resp.StatusCode, tc.want)
			}
		}
	})
}

func TestCSRFForm(t *testing.T) {
	url := mwtest.Serve(t, csrf.New(csrf.Config{Lookup: "form:_csrf"})(fibhttp.HandlerFunc(
		func(c *fibhttp.Context, r *stdhttp.Request) {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(r.PostFormValue("name")))
		})))
	c := mwtest.Clients(t)[0]
	resp, _ := c.Do(t, "GET", url, nil, "")
	cookie := resp.Cookies()[0]
	header := stdhttp.Header{
		"Content-Type": {"application/x-www-form-urlencoded"},
		"Cookie":       {cookie.Name + "=" + cookie.Value},
	}
	resp, body := c.Do(t, "POST", url, header, "name=fib&_csrf="+cookie.Value)
	if resp.StatusCode != stdhttp.StatusOK || body != "fib" {
		t.Errorf("form with the token: %d %q", resp.StatusCode, body)
	}
	resp, _ = c.Do(t, "POST", url, header, "name=fib&_csrf=nope")
	if resp.StatusCode != stdhttp.StatusForbidden {
		t.Errorf("form with a wrong token: %d", resp.StatusCode)
	}
}

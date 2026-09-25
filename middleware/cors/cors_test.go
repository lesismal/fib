//go:build linux || darwin || windows

package cors_test

import (
	stdhttp "net/http"
	"testing"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/cors"
	"github.com/lesismal/fib/middleware/internal/mwtest"
)

var ok = fibhttp.HandlerFunc(func(c *fibhttp.Context, _ *stdhttp.Request) {
	_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("ok"))
})

func TestCORSAnyOrigin(t *testing.T) {
	url := mwtest.Serve(t, cors.New()(ok))
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		resp, body := c.Do(t, "GET", url, stdhttp.Header{"Origin": {"https://a.example"}}, "")
		if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" || body != "ok" {
			t.Errorf("Access-Control-Allow-Origin %q, body %q", got, body)
		}
		resp, body = c.Do(t, "OPTIONS", url, stdhttp.Header{
			"Origin":                         {"https://a.example"},
			"Access-Control-Request-Method":  {"PUT"},
			"Access-Control-Request-Headers": {"X-Custom"},
		}, "")
		if resp.StatusCode != stdhttp.StatusNoContent || body != "" {
			t.Errorf("preflight answered %d %q", resp.StatusCode, body)
		}
		for key, want := range map[string]string{
			"Access-Control-Allow-Origin":  "*",
			"Access-Control-Allow-Methods": "GET, POST, HEAD, PUT, DELETE, PATCH",
			"Access-Control-Allow-Headers": "X-Custom",
		} {
			if got := resp.Header.Get(key); got != want {
				t.Errorf("preflight %s = %q, want %q", key, got, want)
			}
		}
	})
}

func TestCORSOrigins(t *testing.T) {
	url := mwtest.Serve(t, cors.New(cors.Config{
		AllowOrigins:     []string{"https://app.example", "https://*.example.org"},
		AllowCredentials: true,
		ExposeHeaders:    []string{"X-Total"},
		MaxAge:           600,
	})(ok))
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		for origin, allowed := range map[string]bool{
			"https://app.example":       true,
			"https://APP.example":       true,
			"https://a.b.example.org":   true,
			"https://example.org":       false,
			"https://evil.example":      false,
			"http://app.example":        false,
			"https://x.example.org.bad": false,
		} {
			resp, _ := c.Do(t, "GET", url, stdhttp.Header{"Origin": {origin}}, "")
			got := resp.Header.Get("Access-Control-Allow-Origin")
			if allowed != (got == origin) || !allowed && got != "" {
				t.Errorf("origin %s: Access-Control-Allow-Origin %q", origin, got)
			}
			if allowed && (resp.Header.Get("Access-Control-Allow-Credentials") != "true" ||
				resp.Header.Get("Access-Control-Expose-Headers") != "X-Total") {
				t.Errorf("origin %s: header %v", origin, resp.Header)
			}
			if resp.Header.Get("Vary") != "Origin" {
				t.Errorf("origin %s: Vary %q", origin, resp.Header.Get("Vary"))
			}
		}
		resp, _ := c.Do(t, "GET", url, nil, "")
		if resp.Header.Get("Vary") != "Origin" || resp.Header.Get("Access-Control-Allow-Origin") != "" {
			t.Errorf("no origin: header %v", resp.Header)
		}
		resp, _ = c.Do(t, "OPTIONS", url, stdhttp.Header{
			"Origin": {"https://app.example"}, "Access-Control-Request-Method": {"POST"},
		}, "")
		if resp.Header.Get("Access-Control-Max-Age") != "600" || resp.Header.Get("Access-Control-Allow-Credentials") != "true" {
			t.Errorf("preflight: header %v", resp.Header)
		}
	})
}

func TestCORSCredentialsWithAnyOrigin(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("AllowCredentials with any origin did not panic")
		}
	}()
	cors.New(cors.Config{AllowCredentials: true})
}

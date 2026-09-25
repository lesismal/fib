//go:build linux || darwin || windows

package limiter_test

import (
	stdhttp "net/http"
	"strconv"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/internal/mwtest"
	"github.com/lesismal/fib/middleware/limiter"
)

func handler(c *fibhttp.Context, r *stdhttp.Request) {
	status := stdhttp.StatusOK
	if r.URL.Path == "/fail" {
		status = stdhttp.StatusBadRequest
	}
	_ = c.Respond(status, "text/plain", []byte("ok"))
}

// byHeader keys each test's requests apart, so the protocols do not share a
// count.
func byHeader(_ *fibhttp.Context, r *stdhttp.Request) string { return r.Header.Get("X-Key") }

func TestLimiter(t *testing.T) {
	url := mwtest.Serve(t, limiter.New(limiter.Config{Max: 3, Expiration: time.Hour, KeyGenerator: byHeader})(
		fibhttp.HandlerFunc(handler)))
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		key := stdhttp.Header{"X-Key": {c.Name}}
		for i := 1; i <= 5; i++ {
			resp, _ := c.Do(t, "GET", url, key, "")
			wantStatus, remaining := stdhttp.StatusOK, strconv.Itoa(3-i)
			if i > 3 {
				wantStatus, remaining = stdhttp.StatusTooManyRequests, "0"
			}
			if resp.StatusCode != wantStatus || resp.Header.Get("X-Ratelimit-Remaining") != remaining ||
				resp.Header.Get("X-Ratelimit-Limit") != "3" {
				t.Errorf("request %d: %d, header %v", i, resp.StatusCode, resp.Header)
			}
			if reset, _ := strconv.Atoi(resp.Header.Get("X-Ratelimit-Reset")); reset <= 0 || reset > 3600 {
				t.Errorf("request %d: X-RateLimit-Reset %q", i, resp.Header.Get("X-Ratelimit-Reset"))
			}
			if (i > 3) != (resp.Header.Get("Retry-After") != "") {
				t.Errorf("request %d: Retry-After %q", i, resp.Header.Get("Retry-After"))
			}
		}
		// Another client has a count of its own.
		resp, _ := c.Do(t, "GET", url, stdhttp.Header{"X-Key": {c.Name + " other"}}, "")
		if resp.StatusCode != stdhttp.StatusOK {
			t.Errorf("another key: %d", resp.StatusCode)
		}
	})
}

func TestLimiterWindowPasses(t *testing.T) {
	url := mwtest.Serve(t, limiter.New(limiter.Config{Max: 1, Expiration: 200 * time.Millisecond, KeyGenerator: byHeader})(
		fibhttp.HandlerFunc(handler)))
	c := mwtest.Clients(t)[0]
	key := stdhttp.Header{"X-Key": {"k"}}
	// Start at the beginning of a window, so the second request lands in it.
	time.Sleep(time.Until(time.Now().Truncate(200 * time.Millisecond).Add(200 * time.Millisecond)))
	c.Do(t, "GET", url, key, "")
	if resp, _ := c.Do(t, "GET", url, key, ""); resp.StatusCode != stdhttp.StatusTooManyRequests {
		t.Fatalf("second request: %d", resp.StatusCode)
	}
	time.Sleep(250 * time.Millisecond)
	if resp, _ := c.Do(t, "GET", url, key, ""); resp.StatusCode != stdhttp.StatusOK {
		t.Errorf("in the next window: %d", resp.StatusCode)
	}
}

func TestLimiterSkipFailed(t *testing.T) {
	url := mwtest.Serve(t, limiter.New(limiter.Config{
		Max: 2, Expiration: time.Hour, KeyGenerator: byHeader, SkipFailedRequests: true,
	})(fibhttp.HandlerFunc(handler)))
	mwtest.Run(t, func(t *testing.T, c mwtest.Client) {
		key := stdhttp.Header{"X-Key": {c.Name}}
		for i := 0; i < 5; i++ {
			if resp, _ := c.Do(t, "GET", url+"/fail", key, ""); resp.StatusCode != stdhttp.StatusBadRequest {
				t.Fatalf("failed request %d was limited: %d", i, resp.StatusCode)
			}
		}
		// The refund happens once each response is out; give the last one a
		// moment.
		time.Sleep(20 * time.Millisecond)
		for i, want := range []int{stdhttp.StatusOK, stdhttp.StatusOK, stdhttp.StatusTooManyRequests} {
			if resp, _ := c.Do(t, "GET", url, key, ""); resp.StatusCode != want {
				t.Errorf("request %d: %d, want %d", i, resp.StatusCode, want)
			}
		}
	})
}

func TestLimiterSliding(t *testing.T) {
	url := mwtest.Serve(t, limiter.New(limiter.Config{
		Max: 4, Expiration: 400 * time.Millisecond, KeyGenerator: byHeader, SlidingWindow: true,
	})(fibhttp.HandlerFunc(handler)))
	c := mwtest.Clients(t)[0]
	key := stdhttp.Header{"X-Key": {"k"}}
	window := 400 * time.Millisecond
	time.Sleep(time.Until(time.Now().Truncate(window).Add(window)))
	for i := 0; i < 4; i++ {
		c.Do(t, "GET", url, key, "")
	}
	// Early in the next window most of the last one still counts, which a
	// fixed window would have forgotten.
	time.Sleep(time.Until(time.Now().Truncate(window).Add(window + 20*time.Millisecond)))
	if resp, _ := c.Do(t, "GET", url, key, ""); resp.StatusCode != stdhttp.StatusTooManyRequests {
		t.Errorf("early in the next window: %d", resp.StatusCode)
	}
}

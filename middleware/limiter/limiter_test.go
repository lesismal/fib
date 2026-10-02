//go:build linux || darwin || windows

package limiter_test

import (
	stdhttp "net/http"
	"strconv"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware/limiter"
	"github.com/lesismal/fib/middleware/mwtest"
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
	const window = time.Second
	url := mwtest.Serve(t, limiter.New(limiter.Config{Max: 1, Expiration: window, KeyGenerator: byHeader})(
		fibhttp.HandlerFunc(handler)))
	c := mwtest.Clients(t)[0]
	key := stdhttp.Header{"X-Key": {"k"}}
	// Start at the beginning of a window, so that the second request lands in
	// it whatever the machine makes of the first.
	time.Sleep(time.Until(time.Now().Truncate(window).Add(window)))
	c.Do(t, "GET", url, key, "")
	if resp, _ := c.Do(t, "GET", url, key, ""); resp.StatusCode != stdhttp.StatusTooManyRequests {
		t.Fatalf("second request: %d", resp.StatusCode)
	}
	// The next window counts from nothing again, and every later one would
	// too, so waiting for it is not a race.
	time.Sleep(time.Until(time.Now().Truncate(window).Add(window)))
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

// The proportions a sliding window counts by are checked exactly, against a
// store given the time, in TestStoreSlidingWindow. This one is about a server
// answering by them, so it keeps well clear of the edges: a window filled at
// its beginning refuses the next request for the first window/Max of the
// window after it, which here is half a second.
func TestLimiterSliding(t *testing.T) {
	const window = time.Second
	const maxRequests = 2
	url := mwtest.Serve(t, limiter.New(limiter.Config{
		Max: maxRequests, Expiration: window, KeyGenerator: byHeader, SlidingWindow: true,
	})(fibhttp.HandlerFunc(handler)))
	c := mwtest.Clients(t)[0]
	// Fill a window from its beginning, so that all of the requests land in
	// it. A fill that ran into the next window would leave the count split
	// between the two, which is not what the check below is about, so it is
	// made again on a key of its own.
	var key stdhttp.Header
	for attempt := 0; ; attempt++ {
		if attempt == 3 {
			t.Fatal("the requests kept outlasting the window they were filling")
		}
		key = stdhttp.Header{"X-Key": {"k" + strconv.Itoa(attempt)}}
		time.Sleep(time.Until(time.Now().Truncate(window).Add(window)))
		filling := time.Now().Truncate(window)
		for i := 0; i < maxRequests; i++ {
			if resp, _ := c.Do(t, "GET", url, key, ""); resp.StatusCode != stdhttp.StatusOK {
				t.Fatalf("request %d filling the window: %d", i, resp.StatusCode)
			}
		}
		if time.Now().Truncate(window).Equal(filling) {
			break
		}
	}
	// Early in the next window the ones just past still count, which a fixed
	// window would have forgotten.
	time.Sleep(time.Until(time.Now().Truncate(window).Add(window + window/10)))
	if resp, _ := c.Do(t, "GET", url, key, ""); resp.StatusCode != stdhttp.StatusTooManyRequests {
		t.Errorf("early in the next window: %d", resp.StatusCode)
	}
}

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	stdhttp "net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestClientConnTimerBoundsEachRequest checks that the timer an HTTP/1
// connection keeps for its requests bounds each one it carries, not only
// the first: a request that is not answered in time on a connection that
// answered others before it, which set the timer for later, fails with its
// own deadline's error, in about the time that deadline gives it; and that a
// request waiting for the connection, which bounds nothing it does not carry,
// is bounded by a timer of its own.
func TestClientConnTimerBoundsEachRequest(t *testing.T) {
	const wait = 150 * time.Millisecond
	ts := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if r.URL.Path == "/slow" {
			time.Sleep(4 * wait)
		}
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(ts.Close)
	for _, c := range []struct {
		timeout, header time.Duration
		want            error
	}{
		{10 * time.Second, wait, ErrResponseHeaderTimeout},
		{wait, 10 * time.Second, errRequestTimeout},
		{wait, 0, errRequestTimeout},
	} {
		config := DefaultClientConfig()
		config.Timeout, config.ResponseHeaderTimeout = c.timeout, c.header
		config.MaxConnsPerHost = 1
		client := newTestClient(t, config)
		for i := range 3 {
			resp, err := client.Go(mustRequest(t, "GET", ts.URL+"/", nil)).Wait()
			if err != nil {
				t.Fatalf("%v/%v: request %d: %v", c.timeout, c.header, i, err)
			}
			readBody(t, resp)
			time.Sleep(wait / 3)
		}
		start := time.Now()
		slow := client.Go(mustRequest(t, "GET", ts.URL+"/slow", nil))
		// It waits for the one connection, which the slow request holds.
		queued := client.Go(mustRequest(t, "GET", ts.URL+"/", nil))
		if _, err := slow.Wait(); !errors.Is(err, c.want) {
			t.Errorf("%v/%v: slow request: err = %v, want %v", c.timeout, c.header, err, c.want)
		}
		if took := time.Since(start); took < wait*9/10 || took > 3*wait {
			t.Errorf("%v/%v: slow request failed after %v, want about %v", c.timeout, c.header, took, wait)
		}
		resp, err := queued.Wait()
		switch {
		case c.timeout == wait && !errors.Is(err, errRequestTimeout) && err != nil:
			t.Errorf("%v/%v: queued request: err = %v", c.timeout, c.header, err)
		case c.timeout != wait && err != nil:
			t.Errorf("%v/%v: queued request: err = %v", c.timeout, c.header, err)
		case err == nil:
			readBody(t, resp)
		}
		if resp, err := client.Go(mustRequest(t, "GET", ts.URL+"/", nil)).Wait(); err != nil {
			t.Errorf("%v/%v: request after the timeout: %v", c.timeout, c.header, err)
		} else {
			readBody(t, resp)
		}
	}
}

// TestClientConnTimerQueued checks that a request that waits for a
// connection past its Timeout fails then, though no connection carries it.
func TestClientConnTimerQueued(t *testing.T) {
	const wait = 150 * time.Millisecond
	ts := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if r.URL.Path == "/slow" {
			time.Sleep(4 * wait)
		}
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(ts.Close)
	config := DefaultClientConfig()
	config.MaxConnsPerHost = 1
	client := newTestClient(t, config)
	slow := client.Go(mustRequest(t, "GET", ts.URL+"/slow", nil))
	time.Sleep(wait / 3)
	config.Timeout = wait
	client.config.Timeout = wait
	start := time.Now()
	if _, err := client.Go(mustRequest(t, "GET", ts.URL+"/", nil)).Wait(); !errors.Is(err, errRequestTimeout) {
		t.Errorf("queued request: err = %v, want %v", err, errRequestTimeout)
	}
	if took := time.Since(start); took < wait*9/10 || took > 3*wait {
		t.Errorf("queued request failed after %v, want about %v", took, wait)
	}
	if resp, err := slow.Wait(); err != nil {
		t.Errorf("slow request: %v", err)
	} else {
		readBody(t, resp)
	}
}

// TestClientConnTimerParts checks the connection's timer piece by piece: it
// is set for the earliest deadline asked of it and left for a later one,
// stopped for good once the connection closes, and when it fires it fails a
// request past its deadline, sets itself again for one that is not, and does
// nothing for an idle connection.
func TestClientConnTimerParts(t *testing.T) {
	cc := &clientConn{}
	now := time.Now()
	cc.arm(now.Add(time.Hour))
	if !cc.due.Equal(now.Add(time.Hour)) {
		t.Fatalf("due %v after the first arm", cc.due)
	}
	cc.arm(now.Add(2 * time.Hour))
	if !cc.due.Equal(now.Add(time.Hour)) {
		t.Fatalf("a later deadline moved due to %v", cc.due)
	}
	cc.arm(now.Add(time.Minute))
	if !cc.due.Equal(now.Add(time.Minute)) {
		t.Fatalf("an earlier deadline left due at %v", cc.due)
	}

	// Idle: it fires, finds nothing in flight, and waits to be set again.
	cc.expire()
	if !cc.due.IsZero() {
		t.Fatalf("due %v after firing on an idle connection", cc.due)
	}

	// A request whose deadline is ahead: it is set again for it.
	ahead := &clientRequest{timeoutAt: now.Add(time.Hour)}
	cc.current = ahead
	cc.expire()
	if !cc.due.Equal(ahead.timeoutAt) {
		t.Fatalf("due %v, want the request's deadline %v", cc.due, ahead.timeoutAt)
	}

	// A request past its deadline fails with it.
	failed := make(chan error, 1)
	past := &clientRequest{headerAt: now.Add(-time.Second), timeoutAt: now.Add(time.Hour),
		callback: func(_ *ClientResponse, err error) { failed <- err }}
	cc.current = past
	cc.expire()
	if err := <-failed; !errors.Is(err, ErrResponseHeaderTimeout) {
		t.Fatalf("err = %v, want ErrResponseHeaderTimeout", err)
	}

	// A request already settled is left alone, and the timer with it.
	cc.expire()
	if !cc.due.IsZero() {
		t.Fatalf("due %v for a settled request", cc.due)
	}

	cc.mu.Lock()
	cc.closed = true
	cc.stopTimerLocked()
	cc.mu.Unlock()
	if cc.timer.Stop() {
		t.Fatal("the timer of a closed connection was left set")
	}
	cc.arm(now.Add(time.Minute))
	if !cc.due.IsZero() {
		t.Fatal("a closed connection's timer was set again")
	}
}

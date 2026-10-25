package reqbody

import (
	stdhttp "net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestExpectsContinue(t *testing.T) {
	req := func(proto int, expect ...string) *stdhttp.Request {
		r, _ := stdhttp.NewRequest("POST", "http://x/", nil)
		r.ProtoMinor = proto
		for _, v := range expect {
			r.Header.Add("Expect", v)
		}
		return r
	}
	cases := []struct {
		name    string
		req     *stdhttp.Request
		hasBody bool
		want    bool
	}{
		{"asks", req(1, "100-continue"), true, true},
		{"any case, in a list", req(1, "foo, 100-Continue "), true, true},
		{"no body", req(1, "100-continue"), false, false},
		{"other expectation", req(1, "200-ok"), true, false},
		{"none", req(1), true, false},
		{"HTTP/1.0", req(0, "100-continue"), true, false},
	}
	for _, tc := range cases {
		if got := ExpectsContinue(tc.req, tc.hasBody); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestGate(t *testing.T) {
	var opened atomic.Int32
	open := func() { opened.Add(1) }

	// Opened by the server's 100, once, and a later Shut finds it gone.
	g := NewGate(open)
	g.Start(time.Minute)
	g.Open()
	g.Open()
	if opened.Load() != 1 || g.Shut() {
		t.Fatalf("opened %d times; Shut after Open reported the body held", opened.Load())
	}

	// Opened by the wait.
	opened.Store(0)
	g = NewGate(open)
	g.Start(10 * time.Millisecond)
	deadline := time.Now().Add(5 * time.Second)
	for opened.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if opened.Load() != 1 {
		t.Fatal("the wait did not open the gate")
	}

	// Shut first: the body never goes, and Shut keeps saying it was held, the
	// second call too.
	opened.Store(0)
	g = NewGate(open)
	g.Start(10 * time.Millisecond)
	first, again := g.Shut(), g.Shut()
	if !first || !again {
		t.Fatal("Shut on a waiting gate did not report the body held")
	}
	g.Open()
	time.Sleep(50 * time.Millisecond)
	if opened.Load() != 0 {
		t.Fatal("a shut gate opened")
	}

	// Opened before the wait began: Start sets no timer.
	opened.Store(0)
	g = NewGate(open)
	g.Open()
	g.Start(time.Nanosecond)
	time.Sleep(20 * time.Millisecond)
	if opened.Load() != 1 {
		t.Fatalf("opened %d times", opened.Load())
	}

	// A nil gate, a request without the expectation, does nothing.
	var none *Gate
	none.Start(time.Second)
	none.Open()
	if none.Shut() {
		t.Fatal("a nil gate held a body")
	}
}

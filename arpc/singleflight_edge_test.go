//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package arpc

import (
	"context"
	"sync"
	"testing"
	"time"
)

// sfGates holds back the server's answer to each request of the
// singleflight tests until the test opens the request's gate.
type sfGates struct {
	mu    sync.Mutex
	gates map[string]chan struct{}
}

func (g *sfGates) gate(req string) chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.gates == nil {
		g.gates = map[string]chan struct{}{}
	}
	ch, ok := g.gates[req]
	if !ok {
		ch = make(chan struct{})
		g.gates[req] = ch
	}
	return ch
}

func (g *sfGates) open(req string) { close(g.gate(req)) }

// openAll lets every request through, for the test's end.
func (g *sfGates) openAll() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, ch := range g.gates {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
}

// sfServer serves /get, which waits for the request's gate before answering
// with an echoRsp, or, for the request "fail", an error, or, for "raw", a
// payload that is not JSON; entered hears of each request.
func sfServer(t *testing.T) (addr string, entered chan string, gates *sfGates) {
	t.Helper()
	s := NewServer()
	entered = make(chan string, 16)
	gates = &sfGates{}
	s.Handler.Handle("/get", func(ctx *Context) {
		req := string(ctx.Body())
		entered <- req
		<-gates.gate(req)
		switch req {
		case "fail":
			ctx.Error("shared failure")
		case "raw":
			ctx.Write("not json")
		default:
			ctx.Write(&echoRsp{Message: req})
		}
	})
	return startServer(t, s), entered, gates
}

// sfClient dials a Client that shares its calls of /get by request; keyed
// hears of each call as it looks for one in flight to join.
func sfClient(t *testing.T, addr string) (*Client, chan string) {
	t.Helper()
	keyed := make(chan string, 16)
	handler := NewHandler()
	handler.Singleflight("/get", func(req any) string {
		keyed <- req.(string)
		return req.(string)
	})
	return dial(t, addr, handler), keyed
}

// TestSingleflightBlockingOutcomes pins down what the followers of a shared
// blocking call get: the leader's error response; the payload decoded anew
// for a response of another type than the leader's, or when the leader kept
// none; nothing for no response; ErrClientTimeout when their own context
// ends first; and the leader's own failures, to decode or to wait.
func TestSingleflightBlockingOutcomes(t *testing.T) {
	addr, entered, gates := sfServer(t)
	defer gates.openAll()
	c, keyed := sfClient(t, addr)

	type outcome struct{ err error }
	// joined is how many callers joined the call in flight for req, or -1
	// when there is none.
	joined := func(req string) int {
		c.sfGroup.mu.Lock()
		defer c.sfGroup.mu.Unlock()
		if call := c.sfGroup.calls[sfKey{method: "/get", key: req}]; call != nil {
			return call.followers
		}
		return -1
	}
	call := func(req string, rsp any, timeout time.Duration) <-chan outcome {
		before := joined(req)
		ch := make(chan outcome, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			ch <- outcome{c.CallContext(ctx, "/get", req, rsp)}
		}()
		recvArpc(t, keyed, "the call to look for one in flight")
		// The key is taken before the call joins the one in flight; a gate
		// opened in between lets the leader finish first, and the follower
		// then goes to the server itself.
		if before >= 0 {
			waitArpc(t, "the follower to join", func() bool { return joined(req) > before })
		}
		return ch
	}
	lead := func(req string, rsp any, timeout time.Duration) <-chan outcome {
		ch := call(req, rsp, timeout)
		if got := recvArpc(t, entered, "the leader's request"); got != req {
			t.Fatalf("the server got %q, want %q", got, req)
		}
		return ch
	}

	// The leader's error is every follower's.
	leader := lead("fail", &echoRsp{}, arpcWait)
	follower := call("fail", nil, arpcWait)
	gates.open("fail")
	for _, ch := range []<-chan outcome{leader, follower} {
		if o := recvArpc(t, ch, "a call"); o.err == nil || o.err.Error() != "shared failure" {
			t.Fatalf("shared error call = %v", o.err)
		}
	}

	// A leader with no rsp keeps nothing decoded; a follower with a pointer
	// decodes the payload, and one with none takes nothing.
	leader = lead("a", nil, arpcWait)
	var asString string
	followers := []<-chan outcome{call("a", &asString, arpcWait), call("a", nil, arpcWait)}
	gates.open("a")
	for _, ch := range append(followers, leader) {
		if o := recvArpc(t, ch, "a call"); o.err != nil {
			t.Fatalf("call = %v", o.err)
		}
	}
	if asString != `{"Message":"a"}` {
		t.Fatalf("a follower of another type decoded %q", asString)
	}

	// A follower whose own context ends first gives up alone.
	var rsp echoRsp
	leader = lead("b", &rsp, arpcWait)
	if o := recvArpc(t, call("b", &echoRsp{}, 50*time.Millisecond), "the impatient follower"); o.err != ErrClientTimeout {
		t.Fatalf("a follower past its deadline = %v", o.err)
	}
	gates.open("b")
	if o := recvArpc(t, leader, "the leader"); o.err != nil || rsp.Message != "b" {
		t.Fatalf("leader = %+v, %v", rsp, o.err)
	}

	// A leader that cannot decode the response fails with the decoder's
	// error, and so does its follower.
	leader = lead("raw", &echoRsp{}, arpcWait)
	follower = call("raw", &echoRsp{}, arpcWait)
	gates.open("raw")
	for _, ch := range []<-chan outcome{leader, follower} {
		if o := recvArpc(t, ch, "a call"); o.err == nil {
			t.Fatal("a response that does not decode succeeded")
		}
	}

	// A leader whose context ends first fails with ErrClientTimeout.
	if o := recvArpc(t, lead("c", &echoRsp{}, 50*time.Millisecond), "the impatient leader"); o.err != ErrClientTimeout {
		t.Fatalf("a leader past its deadline = %v", o.err)
	}
}

// TestSingleflightFollowerOfStoppedClient pins down that a follower waiting
// on a shared call returns ErrClientStopped as soon as its Client stops.
func TestSingleflightFollowerOfStoppedClient(t *testing.T) {
	addr, entered, gates := sfServer(t)
	defer gates.openAll()
	c, keyed := sfClient(t, addr)
	results := make(chan error, 2)
	go func() { results <- c.Call("/get", "x", nil, arpcWait) }()
	recvArpc(t, entered, "the leader's request")
	go func() { results <- c.Call("/get", "x", nil, arpcWait) }()
	recvArpc(t, keyed, "the leader's key")
	recvArpc(t, keyed, "the follower's key")
	c.Stop()
	for range 2 {
		if err := recvArpc(t, results, "a call"); err != ErrClientStopped {
			t.Fatalf("a call of a stopped Client = %v", err)
		}
	}
}

// TestSingleflightAsyncEdges pins down shared asynchronous calls: a leader
// alone; a follower whose own timeout passes first; a follower that comes as
// the leader finishes, which makes a call of its own; and a leader that
// cannot encode its request, which fails at once.
func TestSingleflightAsyncEdges(t *testing.T) {
	addr, entered, gates := sfServer(t)
	defer gates.openAll()
	c, _ := sfClient(t, addr)
	type outcome struct {
		msg string
		err error
	}
	results := make(chan outcome, 8)
	handler := func(ctx *Context, err error) {
		var rsp echoRsp
		if err == nil {
			err = ctx.Bind(&rsp)
		}
		results <- outcome{rsp.Message, err}
	}

	if err := c.CallAsync("/get", "alone", handler, arpcWait); err != nil {
		t.Fatal(err)
	}
	recvArpc(t, entered, "the request")
	gates.open("alone")
	if o := recvArpc(t, results, "the leader alone"); o.err != nil || o.msg != "alone" {
		t.Fatalf("leader alone = %+v", o)
	}

	if err := c.CallAsync("/get", "slow", handler, arpcWait); err != nil {
		t.Fatal(err)
	}
	recvArpc(t, entered, "the request")
	if err := c.CallAsync("/get", "slow", handler, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if o := recvArpc(t, results, "the follower's timeout"); o.err != ErrTimeout {
		t.Fatalf("follower past its timeout = %+v", o)
	}
	gates.open("slow")
	if o := recvArpc(t, results, "the leader"); o.err != nil || o.msg != "slow" {
		t.Fatalf("leader = %+v", o)
	}

	// A follower that finds the call in flight but finished, as one does
	// that comes just as the leader's handler is done, calls on its own.
	c.sfGroup.mu.Lock()
	if c.sfGroup.calls == nil {
		c.sfGroup.calls = map[sfKey]*sfCall{}
	}
	c.sfGroup.calls[sfKey{method: "/get", key: "late", async: true}] = &sfCall{finished: true}
	c.sfGroup.mu.Unlock()
	if err := c.CallAsync("/get", "late", handler, arpcWait); err != nil {
		t.Fatal(err)
	}
	recvArpc(t, entered, "the late follower's own request")
	gates.open("late")
	if o := recvArpc(t, results, "the late follower"); o.err != nil || o.msg != "late" {
		t.Fatalf("late follower = %+v", o)
	}

	h := NewHandler()
	h.Singleflight("/get")
	c2 := dial(t, addr, h)
	if err := c2.CallAsync("/get", make(chan int), handler, arpcWait); err == nil {
		t.Fatal("a shared CallAsync with an unencodable request succeeded")
	}
	if err := c2.CallAsync("/get", "after", handler, arpcWait); err != nil {
		t.Fatal(err)
	}
	recvArpc(t, entered, "the call after the failed one")
	gates.open("after")
	if o := recvArpc(t, results, "the call after the failed one"); o.err != nil || o.msg != "after" {
		t.Fatalf("call after the failed one = %+v", o)
	}
}

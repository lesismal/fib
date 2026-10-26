//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"bytes"
	"io"
	stdhttp "net/http"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/lesismal/fib/bufferpool"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/quic"
)

// TestResponseCopyKeepsTheHandlersBody is the mirror of
// TestResponseSmallBodyIsNotCopied: a handler that changes its own buffer as
// soon as RespondCopy returns still has the body it gave on the wire, since
// that call copied it. The stream's window is small, so nearly all of the
// body goes out after the change.
func TestResponseCopyKeepsTheHandlersBody(t *testing.T) {
	const size = 4 << 10
	const window = 512
	body := bytes.Repeat([]byte("a"), size)
	want := append([]byte(nil), body...)
	changed := bytes.Repeat([]byte("b"), size)
	responded := make(chan struct{})
	config := Config{}
	config.StreamPool = fibhttp.StreamPoolConfig{Disable: true}
	base := startServer(t, config, func(c *fibhttp.Context) {
		_ = c.RespondCopy(stdhttp.StatusOK, "application/octet-stream", body)
		close(responded)
		copy(body, changed)
	})
	w := dialH3WireWith(t, nil, base, quic.Config{StreamReceiveWindow: window})
	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	s.HoldCredit()
	head, _ := h3Request{path: "/"}.frames()
	if err := s.Write(head, true); err != nil {
		t.Fatal(err)
	}
	id := s.ID()
	st := w.await(id, "the header", func(st *h3WireStream) bool { return st.status == stdhttp.StatusOK })
	select {
	case <-responded:
	case <-time.After(20 * time.Second):
		t.Fatal("the handler did not return from RespondCopy")
	}
	// The handler has changed its buffer; let the rest of the response out.
	credited := 0
	for {
		w.mu.Lock()
		n, done, next := len(st.body), st.done, st.changed
		w.mu.Unlock()
		if n > credited {
			s.Credit(n - credited)
			credited = n
		}
		if done {
			break
		}
		select {
		case <-next:
		case <-time.After(20 * time.Second):
			t.Fatalf("stalled at %d of %d bytes", n, size)
		}
	}
	if !bytes.Equal(st.body, want) {
		t.Fatalf("%d bytes on the wire, want the body as RespondCopy was given it", len(st.body))
	}
}

// TestWriteResponseCopyKeepsTheHandlersTrailer checks that the trailer of a
// response written with WriteResponseCopy is the map's content at the call:
// what the handler does to the map afterwards does not reach the peer.
func TestWriteResponseCopyKeepsTheHandlersTrailer(t *testing.T) {
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
		trailer := stdhttp.Header{"X-Sum": {"one"}}
		_ = c.WriteResponseCopy(fibhttp.Response{
			StatusCode: stdhttp.StatusOK,
			Body:       []byte("hello"),
			Trailer:    trailer,
		})
		trailer.Set("X-Sum", "two")
		trailer.Set("X-Late", "three")
	})
	w := dialH3Wire(t, nil, base)
	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	head, _ := h3Request{path: "/trailer"}.frames()
	if err := s.Write(head, true); err != nil {
		t.Fatal(err)
	}
	st := w.await(s.ID(), "the response", func(st *h3WireStream) bool { return st.done || st.err != nil })
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case st.err != nil:
		t.Fatal(st.err)
	case st.reset:
		t.Fatal("the stream was reset")
	}
	if got := st.trailer.Get("X-Sum"); got != "one" {
		t.Fatalf("trailer X-Sum %q, want one", got)
	}
	if got := st.trailer.Get("X-Late"); got != "" {
		t.Fatalf("trailer X-Late %q, want none", got)
	}
}

// TestResponseCopyWithoutABody checks the corner case of a *Copy call with no
// body: nothing is copied, no DATA frame goes out, and the response reads as
// any other empty one.
func TestResponseCopyWithoutABody(t *testing.T) {
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
		_ = c.RespondCopy(stdhttp.StatusOK, "text/plain", nil)
	})
	w := dialH3Wire(t, nil, base)
	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	head, _ := h3Request{path: "/empty"}.frames()
	if err := s.Write(head, true); err != nil {
		t.Fatal(err)
	}
	status, body := w.response(s.ID())
	if status != stdhttp.StatusOK || len(body) != 0 {
		t.Fatalf("response %d with %d bytes, want 200 with none", status, len(body))
	}
}

// TestResponseCopyFailsOnResetStream checks that a *Copy response to a stream
// the peer has reset is not written, and reports the failure; what happens to
// its copy then is TestWriteResponseOwnedReleasesOnClosedStream's.
func TestResponseCopyFailsOnResetStream(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 64<<10)
	results := make(chan error, 1)
	started := make(chan struct{})
	// The handler runs while the request is still arriving, so that the
	// stream can be reset with its sending side still open: a RESET_STREAM
	// after the request's own end has been acknowledged is a no-op.
	config := Config{StreamRequestBody: true}
	base := startServer(t, config, func(c *fibhttp.Context) {
		cancelled := make(chan struct{})
		c.OnCancel(func(error) { close(cancelled) })
		close(started)
		<-cancelled
		results <- c.RespondCopy(stdhttp.StatusOK, "", body)
	})
	w := dialH3Wire(t, nil, base)
	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	reqHead, reqBody := h3Request{path: "/cancel", framing: h3Length, body: make([]byte, 100)}.frames()
	if err := s.Write(append(reqHead, reqBody[:20]...), false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(20 * time.Second):
		t.Fatal("the handler did not run")
	}
	// The handler registered its callbacks before it closed started, so the
	// reset can land now: a sleep here was a guess at when the server had
	// caught up, and on a loaded runner the guess was sometimes wrong.
	s.Reset(uint64(ErrCodeRequestCancelled))
	select {
	case err = <-results:
	case <-time.After(20 * time.Second):
		t.Fatal("the handler did not finish")
	}
	if err == nil {
		t.Fatal("a response was written on a stream the peer reset")
	}
}

// TestWriteResponseOwnedReleasesOnClosedStream checks that the copy a *Copy
// call made goes back to the pool when the stream it was for has already
// failed: the stream takes the buffer over on every path, including the one
// that sends nothing.
func TestWriteResponseOwnedReleasesOnClosedStream(t *testing.T) {
	req := &stdhttp.Request{Method: stdhttp.MethodGet, ProtoMajor: 3, Header: make(stdhttp.Header)}
	restore := withOneProcessor()
	defer restore()
	// The race detector drops a share of what a pool is given, so a copy that
	// is given back can be lost; try again with another.
	released := false
	for range 10 {
		body := bufferpool.Get(64 << 10)
		for i := range body {
			body[i] = 'x'
		}
		rs := &requestStream{closed: true}
		if err := rs.writeResponse(req, fibhttp.Response{StatusCode: stdhttp.StatusOK, Body: body}, body); err == nil {
			t.Fatal("a response was written on a closed stream")
		}
		if released = poolReturns(unsafe.Pointer(unsafe.SliceData(body)), len(body)); released {
			break
		}
	}
	if !released {
		t.Fatal("the copy did not go back to the pool")
	}
}

// poolReturns reports whether the pool hands back the buffer at addr, of size
// bytes, among the next few it gives out: what a copy given back looks like.
// Buffers taken out are returned, so nothing is lost. The caller runs with one
// processor (see withOneProcessor), so the pool that took the buffer is the
// one asked for it: a sync.Pool keeps its fast path per processor, and a
// buffer in another processor's would not be handed out here. A caller that
// must see a return should try again with another, since the race detector
// drops a share of what a sync.Pool is given.
func poolReturns(addr unsafe.Pointer, size int) bool {
	// It stops as soon as the buffer looked for comes out.
	taken := make([][]byte, 0, 32)
	found := false
	for range cap(taken) {
		buf := bufferpool.Get(size)
		taken = append(taken, buf)
		if unsafe.Pointer(unsafe.SliceData(buf)) == addr {
			found = true
			break
		}
	}
	for _, buf := range taken {
		bufferpool.Put(buf)
	}
	return found
}

// withOneProcessor runs on one processor, as testing.AllocsPerRun does, and
// restores the count when the caller returns: it makes a buffer's return and
// the look for it use the same pool.
func withOneProcessor() func() {
	previous := runtime.GOMAXPROCS(1)
	return func() { runtime.GOMAXPROCS(previous) }
}

// copyReply is what the *Copy benchmarks answer with, made once so that the
// only thing a response allocates for its body is the copy itself.
var copyReply = []byte("hello")

// BenchmarkHTTP3RequestsRespondCopy is BenchmarkHTTP3Requests with
// RespondCopy, so that what the opt-in copy costs a request shows beside what
// the default path costs it.
func BenchmarkHTTP3RequestsRespondCopy(b *testing.B) {
	url := startServer(b, Config{}, func(c *fibhttp.Context) {
		_ = c.RespondCopy(stdhttp.StatusOK, "text/plain", copyReply)
	})
	client := newClient(b, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req, err := stdhttp.NewRequest(stdhttp.MethodGet, url+"/", nil)
		if err != nil {
			b.Fatal(err)
		}
		resp, err := client.Go(req).Wait()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			b.Fatal(err)
		}
		_ = resp.Body.Close()
	}
}

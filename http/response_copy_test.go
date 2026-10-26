//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"github.com/lesismal/fib/bufferpool"
)

// copyBodySize is large enough that a response cannot have written all of it
// before the handler returns, so that a test which changes the handler's
// buffer afterwards would change what is still to go out if the response
// held that buffer.
const copyBodySize = 4 << 20

// TestResponseCopyKeepsTheHandlersBody checks that a response written with
// RespondCopy carries the body as it was at the call, whatever the handler
// does to its own buffer as soon as the call returns: the mirror of HTTP/3's
// TestResponseSmallBodyIsNotCopied, which holds the default path to not
// copying.
func TestResponseCopyKeepsTheHandlersBody(t *testing.T) {
	t.Run("h1", func(t *testing.T) {
		body := bytes.Repeat([]byte("a"), copyBodySize)
		want := append([]byte(nil), body...)
		changed := bytes.Repeat([]byte("b"), len(body))
		responded, mutate, mutated := make(chan struct{}), make(chan struct{}), make(chan struct{})
		addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
			_ = c.RespondCopy(stdhttp.StatusOK, "application/octet-stream", body)
			close(responded)
			<-mutate
			copy(body, changed)
			close(mutated)
		})))
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
		if _, err := io.WriteString(conn, "GET /large HTTP/1.1\r\nHost: t\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		<-responded
		close(mutate)
		<-mutated
		resp, err := stdhttp.ReadResponse(bufio.NewReader(conn), &stdhttp.Request{Method: stdhttp.MethodGet})
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%d bytes on the wire, %d of them the handler's changed buffer", len(got), countChanged(got, changed))
		}
	})
	t.Run("h2", func(t *testing.T) {
		body := bytes.Repeat([]byte("a"), copyBodySize)
		want := append([]byte(nil), body...)
		changed := bytes.Repeat([]byte("b"), len(body))
		responded, mutate, mutated := make(chan struct{}), make(chan struct{}), make(chan struct{})
		addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
			_ = c.RespondCopy(stdhttp.StatusOK, "application/octet-stream", body)
			close(responded)
			<-mutate
			copy(body, changed)
			close(mutated)
		})))
		// A stream window of 512 bytes leaves nearly all of the body for
		// after the handler has changed its own buffer.
		tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 512})
		tc.headers(1, true, get("/large")...)
		<-responded
		close(mutate)
		<-mutated
		st := tc.stream(1)
		for !st.done && !st.resetSet {
			f := tc.read()
			if f.typ == h2FrameData && len(f.payload) > 0 {
				// Give the window back so that the rest of the body follows.
				tc.write(h2AppendWindowUpdate(nil, f.streamID, uint32(len(f.payload))))
				tc.write(h2AppendWindowUpdate(nil, 0, uint32(len(f.payload))))
			}
		}
		if st.resetSet {
			t.Fatalf("stream reset: %v", st.reset)
		}
		if !bytes.Equal(st.body, want) {
			t.Fatalf("%d bytes on the wire, %d of them the handler's changed buffer", len(st.body), countChanged(st.body, changed))
		}
	})
}

// countChanged counts the bytes of got that are the handler's changed buffer
// rather than the body it responded with, for a failure message.
func countChanged(got, changed []byte) int {
	n := 0
	for i := 0; i < len(got) && i < len(changed); i++ {
		if got[i] == changed[i] && changed[i] != 'a' {
			n++
		}
	}
	return n
}

// TestWriteResponseCopyKeepsTheHandlersTrailer checks that the trailer a
// response written with WriteResponseCopy carries is the one the handler gave
// at the call, not the map it changed afterwards. HTTP/2 keeps a trailer by
// reference until the body is out, so its tail must be the copy.
func TestWriteResponseCopyKeepsTheHandlersTrailer(t *testing.T) {
	t.Run("h1", func(t *testing.T) {
		addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
			trailer := stdhttp.Header{"X-Sum": {"one"}}
			_ = c.WriteResponseCopy(Response{StatusCode: stdhttp.StatusOK, Body: []byte("hello"), Trailer: trailer})
			trailer.Set("X-Sum", "two")
			trailer.Set("X-Late", "three")
		})))
		resp := h1Get(t, addr, "/trailer")
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != "hello" {
			t.Fatalf("body %q", body)
		}
		if got := resp.Trailer.Get("X-Sum"); got != "one" {
			t.Fatalf("trailer X-Sum %q, want one", got)
		}
		if got := resp.Trailer.Get("X-Late"); got != "" {
			t.Fatalf("trailer X-Late %q, want none", got)
		}
	})
	t.Run("h2", func(t *testing.T) {
		body := bytes.Repeat([]byte("a"), copyBodySize)
		responded, mutate, mutated := make(chan struct{}), make(chan struct{}), make(chan struct{})
		addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
			trailer := stdhttp.Header{"X-Sum": {"one"}}
			_ = c.WriteResponseCopy(Response{StatusCode: stdhttp.StatusOK, Body: body, Trailer: trailer})
			close(responded)
			<-mutate
			trailer.Set("X-Sum", "two")
			trailer.Set("X-Late", "three")
			close(mutated)
		})))
		tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 512})
		tc.headers(1, true, get("/trailer")...)
		<-responded
		close(mutate)
		<-mutated
		st := tc.stream(1)
		trailer := stdhttp.Header{}
		for !st.done && !st.resetSet {
			f := tc.read()
			switch {
			case f.typ == h2FrameData && len(f.payload) > 0:
				tc.write(h2AppendWindowUpdate(nil, f.streamID, uint32(len(f.payload))))
				tc.write(h2AppendWindowUpdate(nil, 0, uint32(len(f.payload))))
			case f.typ == h2FrameHeaders && f.streamID == 1 && st.header != nil:
				for key, value := range f.fields {
					trailer.Set(key, value)
				}
			}
		}
		if st.resetSet {
			t.Fatalf("stream reset: %v", st.reset)
		}
		if got := trailer.Get("x-sum"); got != "one" {
			t.Fatalf("trailer x-sum %q, want one", got)
		}
		if got := trailer.Get("x-late"); got != "" {
			t.Fatalf("trailer x-late %q, want none", got)
		}
	})
}

// h1Get sends one HTTP/1.1 GET to addr and returns the response, which the
// caller reads and closes.
func h1Get(t *testing.T, addr, path string) *stdhttp.Response {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := io.WriteString(conn, "GET "+path+" HTTP/1.1\r\nHost: t\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	resp, err := stdhttp.ReadResponse(bufio.NewReader(conn), &stdhttp.Request{Method: stdhttp.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// ownedCopyStream is a Stream that can take a pooled body over, and gives it
// back at once, as HTTP/3's does when the response is already sent: the
// simplest way to see what a *Copy call costs. What it saw of the body is
// kept beside the buffer, since the buffer itself is no longer its own.
type ownedCopyStream struct {
	n     int
	first byte
	err   error
}

func (s *ownedCopyStream) WriteResponse(*stdhttp.Request, Response) error {
	return errors.New("ownedCopyStream: WriteResponse where the body was owned")
}

func (s *ownedCopyStream) WriteResponseOwned(_ *stdhttp.Request, _ Response, body []byte) error {
	s.n = len(body)
	if len(body) > 0 {
		s.first = body[0]
	}
	bufferpool.Put(body)
	return s.err
}

func (s *ownedCopyStream) WriteInterim(int, stdhttp.Header) error { return nil }

func (s *ownedCopyStream) Push(*stdhttp.Request, string, *stdhttp.PushOptions) error { return nil }

// cloneCopyStream is a Stream that cannot take a pooled body over: it gets a
// plain copy.
type cloneCopyStream struct{ body []byte }

func (s *cloneCopyStream) WriteResponse(_ *stdhttp.Request, response Response) error {
	s.body = response.Body
	return nil
}

func (s *cloneCopyStream) WriteInterim(int, stdhttp.Header) error { return nil }

func (s *cloneCopyStream) Push(*stdhttp.Request, string, *stdhttp.PushOptions) error { return nil }

// plainCopyStream is a Stream that takes the body it is given and does
// nothing with it: the baseline a *Copy call is measured against.
type plainCopyStream struct{}

func (s *plainCopyStream) WriteResponse(*stdhttp.Request, Response) error { return nil }

func (s *plainCopyStream) WriteInterim(int, stdhttp.Header) error { return nil }

func (s *plainCopyStream) Push(*stdhttp.Request, string, *stdhttp.PushOptions) error { return nil }

func copyTestContext(stream Stream) *Context {
	req := &stdhttp.Request{Method: stdhttp.MethodGet, ProtoMajor: 1, ProtoMinor: 1}
	return NewStreamContext(nil, req, stream)
}

// TestWriteResponseCopyAllocations holds a *Copy call to the copy coming from
// the pool and going back to it: with a warm pool, the copy costs no
// allocation, so a *Copy response costs what the same response through
// WriteResponse does. A Stream that cannot take the body over is given a
// plain copy instead, which costs one.
func TestWriteResponseCopyAllocations(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 1000)
	owned := &ownedCopyStream{}
	c := copyTestContext(owned)
	round := func() {
		c.wrote = false
		if err := c.WriteResponseCopy(Response{StatusCode: stdhttp.StatusOK, Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	round()
	if owned.n != len(body) || owned.first != 'x' {
		t.Fatalf("the stream got %d bytes, first %q", owned.n, owned.first)
	}
	// The copy itself, on its own: Get, copy, Put, with nothing of the
	// response machinery around it.
	if allocs := testing.AllocsPerRun(100, func() {
		copyResponseBodyForTest(c, body)
	}); allocs != 0 {
		t.Fatalf("copying a 1000-byte body into a warm pool allocated %.1f times, want none", allocs)
	}
	// And the whole call against the same call through a Stream that takes
	// no body over, whose body is copied plainly: the difference is the
	// copy's own.
	plain := &plainCopyStream{}
	plainContext := copyTestContext(plain)
	plainRound := func() {
		plainContext.wrote = false
		if err := plainContext.WriteResponse(Response{StatusCode: stdhttp.StatusOK, Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	plainAllocs := testing.AllocsPerRun(100, plainRound)
	if allocs := testing.AllocsPerRun(100, round); allocs > plainAllocs {
		t.Fatalf("a *Copy response allocated %.1f times, plain WriteResponse %.1f", allocs, plainAllocs)
	}

	clone := &cloneCopyStream{}
	c = copyTestContext(clone)
	cloneRound := func() {
		c.wrote = false
		if err := c.WriteResponseCopy(Response{StatusCode: stdhttp.StatusOK, Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	if allocs := testing.AllocsPerRun(100, cloneRound); allocs != 1 {
		t.Fatalf("a body copied plainly allocated %.1f times, want once", allocs)
	}
	if len(clone.body) != len(body) || &clone.body[0] == &body[0] {
		t.Fatalf("the stream got the handler's own body")
	}
}

// copyResponseBodyForTest takes a copy of body and gives it back, as a *Copy
// call does when nothing sends it.
func copyResponseBodyForTest(c *Context, body []byte) {
	_, pooled := c.copyResponseBody(body)
	releaseBody(pooled)
}

// TestWriteResponseCopyEmptyBodyAllocatesNothing checks the corner case of a
// body with nothing in it: it is not copied, and neither is a nil trailer.
func TestWriteResponseCopyEmptyBodyAllocatesNothing(t *testing.T) {
	clone := &cloneCopyStream{}
	c := copyTestContext(clone)
	round := func() {
		c.wrote = false
		if err := c.WriteResponseCopy(Response{StatusCode: stdhttp.StatusNoContent, Body: nil, Trailer: nil}); err != nil {
			t.Fatal(err)
		}
	}
	if allocs := testing.AllocsPerRun(100, round); allocs != 0 {
		t.Fatalf("%.1f allocations for an empty body, want none", allocs)
	}
	if clone.body != nil {
		t.Fatalf("the stream got a %d-byte body, want none", len(clone.body))
	}
}

// errBodyFilter fails a whole response before it is sent, so that the copy a
// *Copy call made is never handed to a send path.
type errBodyFilter struct{ err error }

func (f errBodyFilter) Filter([]byte) ([]byte, error) { return nil, f.err }
func (f errBodyFilter) Close() ([]byte, error)        { return nil, f.err }

// TestWriteResponseCopyReleasesOnFailure checks that a copy whose response
// fails before it is sent goes back to the pool, rather than being lost with
// the Context: the next copy of that size is the same buffer.
func TestWriteResponseCopyReleasesOnFailure(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 1000)
	owned := &ownedCopyStream{}
	c := copyTestContext(owned)
	var addr unsafe.Pointer
	c.OnResponse(func(response *Response) { addr = unsafe.Pointer(unsafe.SliceData(response.Body)) })
	failure := errors.New("filter fails the response")
	c.OnBodyFilter(func(int, stdhttp.Header) BodyFilter { return errBodyFilter{err: failure} })
	restore := withOneProcessor()
	defer restore()
	// The race detector drops a share of what a pool is given, so a copy that
	// is given back can be lost; try again with another.
	released := false
	for range 10 {
		err := c.WriteResponseCopy(Response{StatusCode: stdhttp.StatusOK, Body: body})
		if !errors.Is(err, failure) {
			t.Fatalf("WriteResponseCopy = %v, want %v", err, failure)
		}
		if addr == nil {
			t.Fatal("the response hook saw no body")
		}
		if owned.n != 0 {
			t.Fatalf("the stream was sent %d bytes of a response that failed", owned.n)
		}
		if released = poolReturns(addr, len(body)); released {
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

// TestResponseCopyReleasesOnCancel checks that a copy made for a response
// whose request was cancelled under its handler goes back to the pool: the
// handler sees the write fail, and the copy is not left to the collector with
// the Context.
func TestResponseCopyReleasesOnCancel(t *testing.T) {
	// A body the HTTP/1 send copies, so that the copy comes from the pool and
	// this checks that it goes back, whatever its size.
	body := bytes.Repeat([]byte("x"), 64<<10)
	type result struct {
		released bool
		err      error
	}
	results := make(chan result, 1)
	started := make(chan struct{})
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		close(started)
		var copyAddr unsafe.Pointer
		c.OnResponse(func(response *Response) { copyAddr = unsafe.Pointer(unsafe.SliceData(response.Body)) })
		cancelled := make(chan struct{})
		c.OnCancel(func(error) { close(cancelled) })
		<-cancelled
		restore := withOneProcessor()
		// The race detector drops a share of what a pool is given, so a copy
		// that is given back can be lost; try again with another.
		released := false
		var err error
		for range 10 {
			if err = c.RespondCopy(stdhttp.StatusOK, "", body); err == nil {
				break
			}
			if released = poolReturns(copyAddr, len(body)); released {
				break
			}
		}
		restore()
		results <- result{released: released, err: err}
	})))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := io.WriteString(conn, "GET /cancel HTTP/1.1\r\nHost: t\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	// The connection goes once the handler has the request and has
	// registered its callbacks: a sleep long enough on one machine is a
	// guess on another, and a handler that never started never hears of the
	// close, which is what the macOS and Windows jobs saw.
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the handler never ran")
	}
	conn.Close()
	var got result
	select {
	case got = <-results:
	case <-time.After(30 * time.Second):
		t.Fatal("the handler did not finish")
	}
	if got.err == nil {
		t.Fatal("a response to a request whose connection went was written")
	}
	if !got.released {
		t.Fatal("the copy did not go back to the pool")
	}
}

// BenchmarkServerKeepAliveRespondCopy is BenchmarkServerKeepAlive with
// RespondCopy, so that what the opt-in copy costs a request shows beside what
// the default path costs it.
func BenchmarkServerKeepAliveRespondCopy(b *testing.B) {
	benchmarkServerHandler(b, DefaultConfig(), []byte("GET /hello HTTP/1.1\r\nHost: localhost\r\nUser-Agent: bench\r\nAccept: */*\r\n\r\n"),
		HandlerFunc(func(c *Context) {
			_ = c.RespondCopy(stdhttp.StatusOK, "text/plain", benchReply)
		}))
}

// BenchmarkH2ServerRespondCopy is BenchmarkH2ServerGET with RespondCopy, so
// that the two can be compared under one connection's streams.
func BenchmarkH2ServerRespondCopy(b *testing.B) {
	benchmarkH2Server(b, h2BenchOptions{handler: HandlerFunc(func(c *Context) {
		_ = c.RespondCopy(stdhttp.StatusOK, "text/plain", benchReply)
	})})
}

// TestResponseCopyOwnership pins where a *Copy call's copy comes from: the
// pool for a body the send path is done with when it returns — HTTP/1, which
// copies it on as it copies the head, and an external Stream that takes it
// over — and a plain allocation for one the stream keeps, as HTTP/2's does.
func TestResponseCopyOwnership(t *testing.T) {
	small := bytes.Repeat([]byte("x"), 100)
	large := bytes.Repeat([]byte("x"), 1<<20)
	for _, tc := range []struct {
		name   string
		ctx    *Context
		body   []byte
		pooled bool
	}{
		{"h1 small", &Context{}, small, true},
		{"h1 large", &Context{}, large, true},
		{"h2", &Context{stream: &h2ServerStream{}}, large, false},
		{"owned stream", copyTestContext(&ownedCopyStream{}), large, true},
		{"plain stream", copyTestContext(&cloneCopyStream{}), large, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.ctx.Request = &stdhttp.Request{Method: stdhttp.MethodGet}
			got, pooled := tc.ctx.copyResponseBody(tc.body)
			if (pooled != nil) != tc.pooled {
				t.Fatalf("%d-byte body pooled = %v, want %v", len(tc.body), pooled != nil, tc.pooled)
			}
			if &got[0] == &tc.body[0] {
				t.Fatal("the copy is the handler's own body")
			}
			releaseBody(pooled)
		})
	}
}

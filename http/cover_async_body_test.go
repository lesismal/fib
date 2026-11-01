//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"errors"
	"io"
	stdhttp "net/http"
	"testing"
	"time"
)

// cvAsyncCall is one call an asynchronous filter under test was made, for
// the test to answer when it chooses.
type cvAsyncCall struct {
	p    []byte
	end  bool
	done func([]byte, error)
}

// cvAsyncGate is an AsyncBodyFilter that hands each call to the test.
type cvAsyncGate struct{ calls chan cvAsyncCall }

func newCvAsyncGate() *cvAsyncGate { return &cvAsyncGate{calls: make(chan cvAsyncCall, 4)} }

func (g *cvAsyncGate) FilterAsync(p []byte, end bool, _ stdhttp.Header, done func([]byte, error)) {
	g.calls <- cvAsyncCall{bytes.Clone(p), end, done}
}

// next takes the filter's next call, failing the test if none comes.
func (g *cvAsyncGate) next(t *testing.T) cvAsyncCall {
	t.Helper()
	select {
	case call := <-g.calls:
		return call
	case <-time.After(10 * time.Second):
		t.Fatal("the filter was never called")
		return cvAsyncCall{}
	}
}

// cvAsyncFunc is an AsyncBodyFilter that answers each call in place with fn.
type cvAsyncFunc func(p []byte, end bool) ([]byte, error)

func (f cvAsyncFunc) FilterAsync(p []byte, end bool, _ stdhttp.Header, done func([]byte, error)) {
	done(f(p, end))
}

// cvSyncFilter is a BodyFilter whose Filter and Close do what its fields
// say, and which records an Abort.
type cvSyncFilter struct {
	filterErr error
	tail      string
	closeErr  error
	aborted   chan error
}

func (f *cvSyncFilter) Filter(p []byte) ([]byte, error) {
	if f.filterErr != nil {
		return nil, f.filterErr
	}
	return p, nil
}

func (f *cvSyncFilter) Close() ([]byte, error) {
	if f.closeErr != nil {
		return nil, f.closeErr
	}
	return []byte(f.tail), nil
}

func (f *cvSyncFilter) Abort(err error) {
	if f.aborted != nil {
		f.aborted <- err
	}
}

// An asynchronous filter called as a BodyFilter refuses; runStages closes
// the synchronous stages at the end, appending their tails, and a stage that
// fails at the end, synchronous or not, has those after it aborted.
func TestCoverAsyncBodyStages(t *testing.T) {
	a := asyncFilter{cvAsyncFunc(func(p []byte, _ bool) ([]byte, error) { return p, nil })}
	if _, err := a.Filter(nil); err != errAsyncFilter {
		t.Fatalf("Filter: %v", err)
	}
	if _, err := a.Close(); err != errAsyncFilter {
		t.Fatalf("Close: %v", err)
	}
	run := func(stages []BodyFilter, end bool) (string, error) {
		var out []byte
		var err error
		runStages(stages, []byte("body"), end, stdhttp.Header{}, func(p []byte, e error) { out, err = p, e })
		return string(out), err
	}
	if out, err := run([]BodyFilter{a, &cvSyncFilter{tail: "!"}}, true); err != nil || out != "body!" {
		t.Fatalf("tail: %q %v", out, err)
	}

	broken := errors.New("broken")
	failing := asyncFilter{cvAsyncFunc(func([]byte, bool) ([]byte, error) { return nil, broken })}
	after := &cvSyncFilter{aborted: make(chan error, 1)}
	if _, err := run([]BodyFilter{failing, after}, true); err != broken {
		t.Fatalf("failing asynchronous stage: %v", err)
	}
	if err := <-after.aborted; err != broken {
		t.Fatalf("the stage after it was aborted with %v", err)
	}

	after = &cvSyncFilter{aborted: make(chan error, 1)}
	if _, err := run([]BodyFilter{&cvSyncFilter{filterErr: broken}, after}, true); err != broken {
		t.Fatalf("failing synchronous stage: %v", err)
	}
	if err := <-after.aborted; err != broken {
		t.Fatalf("the stage after it was aborted with %v", err)
	}
	if _, err := run([]BodyFilter{&cvSyncFilter{filterErr: broken}, after}, false); err != broken {
		t.Fatalf("failing synchronous stage before the end: %v", err)
	}
	select {
	case err := <-after.aborted:
		t.Fatalf("a stage was aborted before the end: %v", err)
	default:
	}
}

// cvAsyncUpper is a filter that upper-cases in place.
func cvAsyncUpper(int, stdhttp.Header) AsyncBodyFilter {
	return cvAsyncFunc(func(p []byte, _ bool) ([]byte, error) { return bytes.ToUpper(p), nil })
}

// The ways a handler writes through an asynchronous filter that end without
// a call under way: a filter registered once the response has begun does
// nothing, a write after Finish and an empty write are refused or do
// nothing, Flush sends the header and then the filtered body, and OnWritable
// with nothing held hands over to the connection's own.
func TestCoverAsyncBodyWrites(t *testing.T) {
	results := make(chan cvWrResult, 1)
	addr := serveHTTP1(t, func(c *Context) {
		var errs []error
		switch c.Request.URL.Path {
		case "/late":
			c.WriteHeader(stdhttp.StatusOK)
			c.OnBodyFilterAsync(cvAsyncUpper)
			_, _ = c.WriteString("late")
		case "/finish":
			c.OnBodyFilterAsync(cvAsyncUpper)
			_, _ = c.WriteString("body")
			errs = append(errs, c.Finish())
			_, err := c.WriteString("after")
			errs = append(errs, err)
			n, err := c.Write(nil)
			if n != 0 {
				err = errors.New("an empty write wrote")
			}
			errs = append(errs, err)
		case "/flush":
			c.OnBodyFilterAsync(cvAsyncUpper)
			c.WriteHeader(stdhttp.StatusOK)
			errs = append(errs, c.FlushError())
			_, _ = c.WriteString("flushed")
			errs = append(errs, c.FlushError())
			if c.OnWritable(func(error) {}) {
				errs = append(errs, errors.New("OnWritable armed with nothing held"))
			}
			_, err := c.Write(nil)
			errs = append(errs, err)
		}
		results <- cvWrResult{c.Request.URL.Path, errs}
	})
	client := &stdhttp.Client{Timeout: 10 * time.Second}
	for path, want := range map[string]string{"/late": "late", "/finish": "BODY", "/flush": "FLUSHED"} {
		if _, body := do(t, client, mustRequest(t, "GET", "http://"+addr+path, nil)); body != want {
			t.Errorf("%s: %q, want %q", path, body, want)
		}
		r := cvWrWait(t, results)
		switch path {
		case "/finish":
			if len(r.errs) != 3 || r.errs[0] != nil || r.errs[1] != ErrResponseWritten {
				t.Errorf("%s: %v", path, r.errs)
			}
		default:
			for _, err := range r.errs {
				if err != nil {
					t.Errorf("%s: %v", path, err)
				}
			}
		}
	}
}

// While a call is under way, Flush does nothing and OnWritable keeps its
// function for when the call is back, which then hands it over: to run at
// once with nothing queued, or with the error a failed call left.
func TestCoverAsyncBodyWhileBusy(t *testing.T) {
	gate := newCvAsyncGate()
	writable := make(chan error, 1)
	results := make(chan cvWrResult, 1)
	addr := serveHTTP1(t, func(c *Context) {
		c.OnBodyFilterAsync(func(int, stdhttp.Header) AsyncBodyFilter { return gate })
		c.Retain()
		var errs []error
		_, err := c.WriteString("first")
		errs = append(errs, err, c.FlushError())
		if !c.OnWritable(func(err error) {
			writable <- err
			c.Release()
		}) {
			errs = append(errs, errors.New("OnWritable not kept while a call is under way"))
			c.Release()
		}
		results <- cvWrResult{c.Request.URL.Path, errs}
	})
	for _, fail := range []bool{false, true} {
		conn := dialRaw(t, addr)
		conn.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
		call := gate.next(t)
		r := cvWrWait(t, results)
		for _, err := range r.errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		if !fail {
			call.done(bytes.ToUpper(call.p), nil)
			select {
			case err := <-writable:
				if err != nil {
					t.Fatalf("OnWritable: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("OnWritable never ran")
			}
			// The handler's return is the end, which goes to the filter.
			end := gate.next(t)
			if !end.end {
				t.Fatal("the last call is not the end")
			}
			end.done(nil, nil)
			if _, body := conn.response("GET"); body != "FIRST" {
				t.Fatalf("body %q", body)
			}
			continue
		}
		call.done(nil, io.ErrUnexpectedEOF)
		select {
		case err := <-writable:
			if err == nil {
				t.Fatal("OnWritable ran without the call's error")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("OnWritable never ran")
		}
	}
}

// Finished while a call is under way, the body takes no more, its end
// going to the filter once the call is back.
func TestCoverAsyncBodyFinishWhileBusy(t *testing.T) {
	gate := newCvAsyncGate()
	results := make(chan cvWrResult, 1)
	addr := serveHTTP1(t, func(c *Context) {
		c.OnBodyFilterAsync(func(int, stdhttp.Header) AsyncBodyFilter { return gate })
		_, err := c.WriteString("first")
		errs := []error{err, c.Finish()}
		if _, err := c.WriteString("late"); err != ErrResponseWritten {
			errs = append(errs, errors.New("a write after Finish was taken"))
		}
		results <- cvWrResult{c.Request.URL.Path, errs}
	})
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	call := gate.next(t)
	for _, err := range cvWrWait(t, results).errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	call.done(bytes.ToUpper(call.p), nil)
	end := gate.next(t)
	if !end.end {
		t.Fatal("the last call is not the end")
	}
	end.done([]byte("!"), nil)
	if _, body := conn.response("GET"); body != "FIRST!" {
		t.Fatalf("body %q", body)
	}
}

// A synchronous filter, registered after the asynchronous one so that it
// runs before it, whose Close fails fails the end, and the connection
// closes with nothing sent; one whose Filter fails fails a response sent
// whole, which the handler leaves unanswered; and an asynchronous filter
// whose one call for a response sent whole fails sends nothing.
func TestCoverAsyncBodyFailures(t *testing.T) {
	broken := errors.New("broken")
	results := make(chan cvWrResult, 1)
	addr := serveHTTP1(t, func(c *Context) {
		var errs []error
		switch c.Request.URL.Path {
		case "/end":
			c.OnBodyFilterAsync(cvAsyncUpper)
			c.OnBodyFilter(func(int, stdhttp.Header) BodyFilter { return &cvSyncFilter{closeErr: broken} })
			_, err := c.WriteString("body")
			errs = append(errs, err)
			_ = c.Finish()
			_, err = c.WriteString("more")
			errs = append(errs, err)
		case "/whole":
			c.OnBodyFilterAsync(cvAsyncUpper)
			c.OnBodyFilter(func(int, stdhttp.Header) BodyFilter { return &cvSyncFilter{filterErr: broken} })
			errs = append(errs, c.Respond(stdhttp.StatusOK, "text/plain", []byte("body")))
		case "/whole-async":
			c.OnBodyFilterAsync(func(int, stdhttp.Header) AsyncBodyFilter {
				return cvAsyncFunc(func([]byte, bool) ([]byte, error) { return nil, broken })
			})
			errs = append(errs, c.WriteResponse(Response{StatusCode: stdhttp.StatusOK, Body: []byte("body")}))
		}
		results <- cvWrResult{c.Request.URL.Path, errs}
	})
	for _, path := range []string{"/end", "/whole", "/whole-async"} {
		conn := dialRaw(t, addr)
		conn.send("GET " + path + " HTTP/1.1\r\nHost: test\r\n\r\n")
		r := cvWrWait(t, results)
		switch path {
		case "/end":
			if len(r.errs) != 2 || r.errs[0] != nil || r.errs[1] == nil {
				t.Errorf("%s: %v", path, r.errs)
			}
		case "/whole":
			if len(r.errs) != 1 || r.errs[0] != broken {
				t.Errorf("%s: %v", path, r.errs)
			}
			if _, body := conn.response("GET"); body != "" {
				t.Errorf("%s: the failed body was sent: %q", path, body)
			}
			continue
		case "/whole-async":
			if len(r.errs) != 1 || r.errs[0] != nil {
				t.Errorf("%s: %v", path, r.errs)
			}
		}
		_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		if got, _ := io.ReadAll(conn.r); len(got) != 0 {
			t.Errorf("%s: sent %q", path, got)
		}
	}
}

// A hook that returns no filter, on a response the handler never wrote,
// gives the response back at once as it ends.
func TestCoverAsyncBodyNilUnwritten(t *testing.T) {
	addr := serveHTTP1(t, func(c *Context) {
		c.OnBodyFilterAsync(func(int, stdhttp.Header) AsyncBodyFilter { return nil })
	})
	client := &stdhttp.Client{Timeout: 10 * time.Second}
	if resp, body := do(t, client, mustRequest(t, "GET", "http://"+addr+"/", nil)); resp.StatusCode != stdhttp.StatusOK || body != "" {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
}

// cvAsyncAborted is a cvAsyncFunc that reports its Abort.
type cvAsyncAborted struct {
	cvAsyncFunc
	aborted chan error
}

func (a cvAsyncAborted) Abort(err error) { a.aborted <- err }

// A filter whose output is more than the connection takes at once pauses
// until the client has taken it, and fails the response when the client
// goes first.
func TestCoverAsyncBodyOutputPaced(t *testing.T) {
	big := bytes.Repeat([]byte("z"), 16<<20)
	aborted := make(chan error, 3)
	addr := serveHTTP1(t, func(c *Context) {
		c.OnBodyFilterAsync(func(int, stdhttp.Header) AsyncBodyFilter {
			return cvAsyncAborted{cvAsyncFunc(func(p []byte, end bool) ([]byte, error) {
				if end {
					return p, nil
				}
				return big, nil
			}), aborted}
		})
		_, _ = c.WriteString("a")
		c.Flush()
	})
	// Read whole, the body arrives once the client has made room for it.
	conn := dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	if _, body := conn.response("GET"); len(body) != len(big) {
		t.Fatalf("body of %d bytes, want %d", len(body), len(big))
	}
	// Gone before it reads, the client's connection is closed under it.
	conn = dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	conn.head()
	conn.Close()
	// The response ends without its body's end, so the filter is aborted,
	// whichever the server hears first: the close, which cancels the
	// request, or a send failing on the closed socket, which fails the
	// response and ends the request with it, OnCancel never running.
	select {
	case err := <-aborted:
		if err == nil {
			t.Fatal("the filter was aborted without a reason")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the response did not end with its client gone")
	}
	// The server carries on serving others.
	conn = dialRaw(t, addr)
	conn.send("GET / HTTP/1.1\r\nHost: test\r\n\r\n")
	if _, body := conn.response("GET"); len(body) != len(big) {
		t.Fatalf("body of %d bytes after a client went, want %d", len(body), len(big))
	}
}

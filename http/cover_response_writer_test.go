//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"errors"
	"fmt"
	stdhttp "net/http"
	"strings"
	"testing"
)

// cvRwNotifier is a stream served outside this package that can be written
// to without waiting: above says what WriteBodyNoWait reports, err fails
// it, and notify is what NotifyWritable reports.
type cvRwNotifier struct {
	cvWrStreamer
	above  bool
	err    error
	notify bool
}

func (s *cvRwNotifier) WriteBodyNoWait(p []byte) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	s.body = append(s.body, p...)
	return s.above, nil
}

func (s *cvRwNotifier) NotifyWritable(func(error)) bool { return s.notify }

// A stream served outside this package that can be written to without
// waiting is: WriteNoWait hands it the body and reports what it holds back,
// OnWritable is its NotifyWritable, and once its WriteBodyNoWait fails, the
// response has failed.
func TestCoverWriterExternalNotifier(t *testing.T) {
	req, _ := stdhttp.NewRequest("GET", "/", nil)
	stream := &cvRwNotifier{above: true, notify: true}
	c := NewStreamContext(nil, req, stream)
	c.WriteHeader(stdhttp.StatusOK)
	big := bytes.Repeat([]byte("n"), writerBufferSize+1)
	if above, err := c.WriteNoWait(big); err != nil || !above {
		t.Fatalf("WriteNoWait = %v, %v", above, err)
	}
	if !c.OnWritable(func(error) {}) {
		t.Fatal("OnWritable did not hand over to the stream's NotifyWritable")
	}
	broken := errors.New("stream gone")
	stream.err = broken
	if _, err := c.WriteNoWait([]byte("more")); err != broken {
		t.Fatalf("WriteNoWait of a failing stream = %v", err)
	}
	if _, err := c.WriteNoWait([]byte("more")); err != broken {
		t.Fatalf("WriteNoWait after the stream failed = %v", err)
	}
	if !bytes.Equal(stream.body, big) {
		t.Fatalf("the stream had %d bytes, want %d", len(stream.body), len(big))
	}
}

// A stream served outside this package that streams but cannot be written
// to without waiting has WriteNoWait write as Write does, and report it
// below its limit; so does what an asynchronous filter sends on it. Such a
// stream has no OnWritable.
func TestCoverWriterExternalWithoutNotifier(t *testing.T) {
	req, _ := stdhttp.NewRequest("GET", "/", nil)
	stream := new(cvWrStreamer)
	c := NewStreamContext(nil, req, stream)
	big := strings.Repeat("w", writerBufferSize+1)
	if above, err := c.WriteNoWait([]byte(big)); err != nil || above {
		t.Fatalf("WriteNoWait = %v, %v", above, err)
	}
	if c.OnWritable(func(error) {}) {
		t.Fatal("OnWritable armed on a stream that cannot notify")
	}
	if err := c.Finish(); err != nil || string(stream.body) != big {
		t.Fatalf("Finish = %v, body of %d bytes", err, len(stream.body))
	}

	stream = new(cvWrStreamer)
	c = NewStreamContext(nil, req, stream)
	c.OnBodyFilterAsync(cvAsyncUpper)
	if _, err := c.WriteString(big); err != nil {
		t.Fatal(err)
	}
	if err := c.Finish(); err != nil || string(stream.body) != strings.ToUpper(big) {
		t.Fatalf("Finish = %v, body of %d bytes", err, len(stream.body))
	}
}

// cvRwFlushFail is a filter whose Flush fails.
type cvRwFlushFail struct{ replaceFilter }

func (cvRwFlushFail) Flush() ([]byte, error) { return nil, errors.New("flush failed") }

// WriteNoWait refuses what Write refuses: a body after the response, one a
// status without a body cannot have, one past the length declared, and one
// a filter fails; and on a header an OnHeaderAsync hook failed, it and Write
// report the hook's error, as Flush does when a filter fails to flush.
func TestCoverWriterNoWaitRefusals(t *testing.T) {
	broken := errors.New("broken")
	results := make(chan cvWrResult, 1)
	addr := serveHTTP1(t, func(c *Context) {
		var errs []error
		noWait := func(p string) {
			_, err := c.WriteNoWait([]byte(p))
			errs = append(errs, err)
		}
		switch c.Request.URL.Path {
		case "/finished":
			_, _ = c.WriteString("done")
			_ = c.Finish()
			noWait("late")
		case "/nobody":
			c.WriteHeader(stdhttp.StatusNoContent)
			noWait("x")
		case "/declared":
			c.Header().Set("Content-Length", "2")
			noWait("too long")
		case "/filter":
			c.OnBodyFilter(func(int, stdhttp.Header) BodyFilter { return errBodyFilter{broken} })
			noWait("x")
		case "/hook":
			c.OnHeaderAsync(func(_ int, _ stdhttp.Header, resume func(error)) { resume(broken) })
			c.WriteHeader(stdhttp.StatusOK)
			_, err := c.WriteString("x")
			errs = append(errs, err)
			noWait("x")
		case "/flush":
			c.OnBodyFilter(func(int, stdhttp.Header) BodyFilter { return cvRwFlushFail{} })
			_, _ = c.WriteString("x")
			errs = append(errs, c.FlushError())
		}
		results <- cvWrResult{c.Request.URL.Path, errs}
	})
	for path, want := range map[string]string{
		"/finished": fmt.Sprint([]error{ErrResponseWritten}),
		"/nobody":   fmt.Sprint([]error{stdhttp.ErrBodyNotAllowed}),
		"/declared": fmt.Sprint([]error{stdhttp.ErrContentLength}),
		"/filter":   fmt.Sprint([]error{broken}),
		"/hook":     fmt.Sprint([]error{broken, broken}),
		"/flush":    "[flush failed]",
	} {
		conn := dialRaw(t, addr)
		conn.send("GET " + path + " HTTP/1.1\r\nHost: test\r\n\r\n")
		if got := fmt.Sprint(cvWrWait(t, results).errs); got != want {
			t.Errorf("%s: %s, want %s", path, got, want)
		}
	}
}

// RespondCopy and WriteResponseCopy refuse a response already begun, and
// LimitBody does nothing with no limit or no request.
func TestCoverWriterCopiesAfterBegun(t *testing.T) {
	req, _ := stdhttp.NewRequest("GET", "/", nil)
	c := NewStreamContext(nil, req, new(cvWrStreamer))
	c.WriteHeader(stdhttp.StatusOK)
	if err := c.RespondCopy(stdhttp.StatusOK, "text/plain", []byte("x")); err != ErrResponseWritten {
		t.Fatalf("RespondCopy = %v", err)
	}
	if err := c.WriteResponseCopy(Response{StatusCode: stdhttp.StatusOK}); err != ErrResponseWritten {
		t.Fatalf("WriteResponseCopy = %v", err)
	}
	if err := c.LimitBody(-1); err != nil {
		t.Fatalf("LimitBody(-1) = %v", err)
	}
	if err := (&Context{}).LimitBody(10); err != nil {
		t.Fatalf("LimitBody without a request = %v", err)
	}
}

// A response sent whole on a stream served outside this package, which an
// OnHeaderAsync hook held, goes to the stream's WriteResponse once resumed.
func TestCoverWriterExternalHeldWhole(t *testing.T) {
	req, _ := stdhttp.NewRequest("GET", "/", nil)
	stream := new(cvWrStream)
	c := NewStreamContext(nil, req, stream)
	var resume func(error)
	c.OnHeaderAsync(func(_ int, h stdhttp.Header, r func(error)) {
		h.Set("X-Held", "1")
		resume = r
	})
	if err := c.Respond(stdhttp.StatusOK, "text/plain", []byte("held")); err != nil {
		t.Fatal(err)
	}
	if stream.whole != nil || resume == nil {
		t.Fatal("the response was not held")
	}
	resume(nil)
	if stream.whole == nil || string(stream.whole.Body) != "held" || stream.whole.Header.Get("X-Held") != "1" {
		t.Fatalf("sent %+v", stream.whole)
	}
}

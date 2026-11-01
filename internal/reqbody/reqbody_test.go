package reqbody

import (
	"errors"
	"io"
	stdhttp "net/http"
	"strings"
	"testing"
)

// closeCounter is a body that counts how often it is closed.
type closeCounter struct {
	io.Reader
	closes int
}

func (c *closeCounter) Close() error { c.closes++; return nil }

// A second attempt at a streamed request abandons the first, closing what it
// was reading, and reads the body afresh from GetBody; without GetBody, or
// once the request is over, there is nothing to send again.
func TestSourceTakeReplaysThroughGetBody(t *testing.T) {
	first := &closeCounter{Reader: strings.NewReader("one")}
	again := &closeCounter{Reader: strings.NewReader("two")}
	req := &stdhttp.Request{Body: first, GetBody: func() (io.ReadCloser, error) { return again, nil }}
	s := NewSource(req)
	if _, err := s.Take(); err != nil {
		t.Fatal(err)
	}
	rc, err := s.Take()
	if err != nil {
		t.Fatal(err)
	}
	if first.closes != 1 {
		t.Errorf("the abandoned attempt's body was closed %d times, want 1", first.closes)
	}
	if b, _ := io.ReadAll(rc); string(b) != "two" {
		t.Errorf("the second attempt read %q, want the body GetBody returned", b)
	}
	s.Close()
	s.Close()
	if again.closes != 1 {
		t.Errorf("the second attempt's body was closed %d times, want 1", again.closes)
	}
	if _, err := s.Take(); err != ErrNotReplayable {
		t.Errorf("Take after Close = %v, want ErrNotReplayable", err)
	}

	broken := errors.New("cannot reopen")
	s = NewSource(&stdhttp.Request{Body: io.NopCloser(strings.NewReader("x")),
		GetBody: func() (io.ReadCloser, error) { return nil, broken }})
	_, _ = s.Take()
	if _, err := s.Take(); err != broken {
		t.Errorf("Take with a failing GetBody = %v, want its error", err)
	}

	s = NewSource(&stdhttp.Request{Body: io.NopCloser(strings.NewReader("x"))})
	_, _ = s.Take()
	if _, err := s.Take(); err != ErrNotReplayable {
		t.Errorf("Take again without GetBody = %v, want ErrNotReplayable", err)
	}
}

// A body longer than the length it declared fails as the body's fault, and
// the error says what it was.
func TestPumpRefusesABodyLongerThanDeclared(t *testing.T) {
	err := Pump(io.NopCloser(strings.NewReader("too long")), 3, func([]byte) error { return nil })
	var bodyErr *BodyError
	if !errors.As(err, &bodyErr) || !strings.Contains(err.Error(), "ContentLength=3") || errors.Unwrap(err) == nil {
		t.Fatalf("Pump = %v, want a BodyError naming the declared length", err)
	}
}

// Which bodies stream: none without a body, none on HTTP/1.0, a known one
// only past BufferLimit, and an unknown one unless GetBody marks it as an
// empty in-memory body. Declared is the known length, or -1.
func TestStreamsAndDeclared(t *testing.T) {
	body := io.NopCloser(strings.NewReader("x"))
	for _, tc := range []struct {
		name    string
		req     *stdhttp.Request
		streams bool
		length  int64
	}{
		{"none", &stdhttp.Request{ProtoMajor: 1, ProtoMinor: 1}, false, -1},
		{"nobody", &stdhttp.Request{ProtoMajor: 1, ProtoMinor: 1, Body: stdhttp.NoBody}, false, -1},
		{"http/1.0", &stdhttp.Request{ProtoMajor: 1, ProtoMinor: 0, Body: body}, false, -1},
		{"small", &stdhttp.Request{ProtoMajor: 1, ProtoMinor: 1, Body: body, ContentLength: 10}, false, 10},
		{"large", &stdhttp.Request{ProtoMajor: 2, Body: body, ContentLength: BufferLimit + 1}, true, BufferLimit + 1},
		{"unknown", &stdhttp.Request{ProtoMajor: 1, ProtoMinor: 1, Body: body}, true, -1},
		{"empty", &stdhttp.Request{ProtoMajor: 1, ProtoMinor: 1, Body: body,
			GetBody: func() (io.ReadCloser, error) { return stdhttp.NoBody, nil }}, false, -1},
	} {
		if got := Streams(tc.req); got != tc.streams {
			t.Errorf("%s: Streams = %v, want %v", tc.name, got, tc.streams)
		}
		if got := Declared(tc.req); got != tc.length {
			t.Errorf("%s: Declared = %d, want %d", tc.name, got, tc.length)
		}
	}
}

// A Source closed before any attempt took its body closes the body, as a
// transport closes a request's body whatever becomes of the request.
func TestSourceCloseUntaken(t *testing.T) {
	body := &closeCounter{Reader: strings.NewReader("x")}
	s := NewSource(&stdhttp.Request{Body: body})
	s.Close()
	if body.closes != 1 {
		t.Fatalf("the untaken body was closed %d times, want 1", body.closes)
	}
}

// failingReader fails once it has given what it holds.
type failingReader struct {
	data []byte
	err  error
}

func (r *failingReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

// Pump hands every piece to send and ends cleanly at the declared length;
// a body shorter than declared, or one whose read fails, fails as the
// body's fault, and an error from send is returned as it is.
func TestPumpEnds(t *testing.T) {
	var got []byte
	send := func(p []byte) error { got = append(got, p...); return nil }
	if err := Pump(io.NopCloser(strings.NewReader("whole")), 5, send); err != nil || string(got) != "whole" {
		t.Fatalf("Pump = %v, sent %q", err, got)
	}
	var bodyErr *BodyError
	if err := Pump(io.NopCloser(strings.NewReader("short")), 9, send); !errors.As(err, &bodyErr) ||
		!strings.Contains(err.Error(), "ContentLength=9 with Body length 5") {
		t.Fatalf("short body: %v", err)
	}
	broken := errors.New("read failed")
	if err := Pump(io.NopCloser(&failingReader{data: []byte("ab"), err: broken}), -1, send); !errors.As(err, &bodyErr) ||
		!errors.Is(err, broken) {
		t.Fatalf("failing body: %v", err)
	}
	refused := errors.New("transport gone")
	if err := Pump(io.NopCloser(strings.NewReader("x")), -1, func([]byte) error { return refused }); err != refused {
		t.Fatalf("send failing: %v", err)
	}
}

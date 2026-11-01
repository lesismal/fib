//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"errors"
	"io"
	stdhttp "net/http"
	"testing"
)

// cvDuplexFailing is a stream served outside this package that fails to
// send anything.
type cvDuplexFailing struct {
	cvWrStreamer
	err error
}

func (s *cvDuplexFailing) BeginResponse(*stdhttp.Request, int, stdhttp.Header) error { return s.err }
func (s *cvDuplexFailing) WriteBody([]byte, bool) error                              { return s.err }

// A duplex on a request with no body reads none, and one whose stream
// fails to send has Write and Flush report the failure.
func TestCoverDuplexStreamFails(t *testing.T) {
	broken := errors.New("stream gone")
	req, _ := stdhttp.NewRequest("GET", "/", nil)
	req.Body = nil
	c := NewStreamContext(nil, req, &cvDuplexFailing{err: broken})
	done := make(chan []error, 1)
	err := c.ServeDuplex(func(d *Duplex) {
		var errs []error
		if n, err := d.Read(make([]byte, 4)); n != 0 || err != io.EOF {
			errs = append(errs, errors.New("read a body that is not there"))
		}
		_, err := d.Write(bytes.Repeat([]byte("d"), writerBufferSize+1))
		errs = append(errs, err, d.Flush())
		done <- errs
	})
	if err != nil {
		t.Fatal(err)
	}
	errs := cvCliWait(t, done, "the duplex handler")
	if len(errs) != 2 || !errors.Is(errs[0], broken) || !errors.Is(errs[1], broken) {
		t.Fatalf("errors %v", errs)
	}
}

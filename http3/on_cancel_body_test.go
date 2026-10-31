//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"strings"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/qpack"
)

// TestOnCancelHearsBodyFailure checks that a middleware's OnCancel runs for an
// HTTP/3 request whose body, which the handler takes through OnBody, fails
// part way, as OnBody's callback hears: the client resetting the stream, its
// connection closing, and a body past its content-length. OnBody's last call
// was the last hold on the request, and let go with it the request could end
// unanswered before the cancellation that would have run OnCancel's
// callbacks.
func TestOnCancelHearsBodyFailure(t *testing.T) {
	bodyErr, cancelled := make(chan error, 4), make(chan error, 4)
	handler := func(c *fibhttp.Context) {
		c.OnBody(func(data []byte, fin bool, err error) {
			if err != nil {
				bodyErr <- err
			}
		})
	}
	base := startServer(t, Config{StreamRequestBody: true}, func(c *fibhttp.Context) {
		c.OnCancel(func(err error) { cancelled <- err })
		handler(c)
	})
	heard := func(ch <-chan error, what, how string) {
		t.Helper()
		select {
		case err := <-ch:
			if err == nil {
				t.Fatalf("%s: %s was given no reason", how, what)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: %s never heard of it", how, what)
		}
	}
	data := func(s string) []byte { return append(appendFrameHeader(nil, frameData, len(s)), s...) }
	head := headersFrame(qpack.HeaderField{Name: ":method", Value: "POST"}, qpack.HeaderField{Name: ":scheme", Value: "https"},
		qpack.HeaderField{Name: ":authority", Value: "localhost"}, qpack.HeaderField{Name: ":path", Value: "/"},
		qpack.HeaderField{Name: "content-length", Value: "100000"})
	for _, how := range []string{"reset", "closed", "malformed"} {
		w := dialH3Wire(t, nil, base)
		s, err := w.qc.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Write(append(head, data("abc")...), false); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
		switch how {
		case "reset":
			s.Reset(uint64(ErrCodeRequestCancelled))
		case "closed":
			w.qc.Close(uint64(ErrCodeNoError), "")
		case "malformed":
			// More than the content-length said, which aborts the stream.
			_ = s.Write(data(strings.Repeat("x", 100000)), true)
		}
		heard(bodyErr, "OnBody", how)
		heard(cancelled, "OnCancel", how)
	}
}

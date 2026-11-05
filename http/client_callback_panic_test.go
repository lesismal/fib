//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	stdhttp "net/http"
	"testing"
	"time"
)

// A detached stream's OnBody callback that panics, on the goroutine the
// stream started for it, ends its exchange with ErrResponseAbandoned rather
// than the process, and hears nothing more.
func TestResponseStreamDetachedCallbackPanic(t *testing.T) {
	req, _ := stdhttp.NewRequest(stdhttp.MethodGet, "http://a/", nil)
	head := &stdhttp.Response{StatusCode: 200, Header: stdhttp.Header{}, ContentLength: -1}
	aborted := make(chan error, 1)
	calls := 0
	stream := NewResponseStream(head, req, StreamOptions{Detached: true}, StreamHooks{
		Begin: func(*ResponseStream) bool { return true },
		Callback: func(cr *ClientResponse) {
			cr.OnBody(func(data []byte, fin bool, err error) {
				calls++
				panic("callback panic")
			})
		},
		Whole:  func(*stdhttp.Response) {},
		Done:   func() bool { return true },
		Abort:  func(err error) { aborted <- err },
		Credit: func(int64) {},
	})
	stream.Start()
	stream.Data([]byte("body"))
	select {
	case err := <-aborted:
		if !errors.Is(err, ErrResponseAbandoned) {
			t.Fatalf("aborted with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the exchange was not aborted")
	}
	stream.Data([]byte("more"))
	stream.End()
	time.Sleep(50 * time.Millisecond)
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if calls != 1 || stream.delivering || stream.buffered != 0 {
		t.Fatalf("calls %d, delivering %v, buffered %d", calls, stream.delivering, stream.buffered)
	}
}

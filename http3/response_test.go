//go:build linux || darwin || windows

package http3

import (
	"bytes"
	stdhttp "net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/quic"
)

// A flushed response reaches the client while its handler is still at work:
// the header, then each piece as it is written, then the end.
func TestResponseStreamsAsItIsWritten(t *testing.T) {
	next := make(chan struct{})
	base := startServer(t, Config{}, func(c *fibhttp.Context, r *stdhttp.Request) {
		_, _ = c.WriteString("first")
		c.Flush()
		<-next
		// Past what is held back, so it goes out without a Flush.
		_, _ = c.WriteString(strings.Repeat("s", 10000))
		<-next
		_, _ = c.WriteString("last")
	})
	w := dialH3Wire(t, nil, base)
	id := w.send(t, h3Request{path: "/"}, false)
	w.await(id, "the first piece", func(st *h3WireStream) bool {
		return st.status == 200 && string(st.body) == "first" && !st.done
	})
	next <- struct{}{}
	w.await(id, "the second piece", func(st *h3WireStream) bool { return len(st.body) == 10005 && !st.done })
	next <- struct{}{}
	if status, body := w.response(id); status != 200 || string(body) != "first"+strings.Repeat("s", 10000)+"last" {
		t.Fatalf("status %d, %d bytes", status, len(body))
	}
}

// A streamed response carries its trailers, and its declared length.
func TestResponseStreamsTrailersAndLength(t *testing.T) {
	base := startServer(t, Config{}, func(c *fibhttp.Context, r *stdhttp.Request) {
		if r.URL.Path == "/declared" {
			c.Header().Set("Content-Length", "100000")
			for range 10 {
				_, _ = c.Write(bytes.Repeat([]byte("d"), 10000))
			}
			return
		}
		c.Header().Set("Trailer", "X-Sum")
		for range 10 {
			_, _ = c.WriteString(strings.Repeat("t", 1000))
			c.Flush()
		}
		c.Header().Set("X-Sum", "streamed")
		c.Header().Set(stdhttp.TrailerPrefix+"X-Late", "late")
	})
	client := newClient(t, nil)
	resp, err := client.Go(mustRequest(t, "GET", base+"/trailers", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if body := readBody(t, resp); body != strings.Repeat("t", 10000) || resp.Trailer.Get("X-Sum") != "streamed" ||
		resp.Trailer.Get("X-Late") != "late" {
		t.Fatalf("trailers: %d bytes, trailer %v", len(body), resp.Trailer)
	}
	resp, err = client.Go(mustRequest(t, "GET", base+"/declared", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if body := readBody(t, resp); len(body) != 100000 || resp.ContentLength != 100000 {
		t.Fatalf("declared: %d bytes, length %d", len(body), resp.ContentLength)
	}
}

// A handler that writes faster than the client gives the stream room waits
// for it, rather than having the stream hold its whole body; it is let go as
// the client reads.
func TestResponseWaitsForTheClient(t *testing.T) {
	const total = 1 << 20
	const window = 16 << 10
	var written atomic.Int64
	done := make(chan error, 1)
	base := startServer(t, Config{}, func(c *fibhttp.Context, r *stdhttp.Request) {
		chunk := bytes.Repeat([]byte("w"), 16<<10)
		for written.Load() < total {
			if _, err := c.Write(chunk); err != nil {
				done <- err
				return
			}
			written.Add(int64(len(chunk)))
		}
		done <- nil
	})
	w := dialH3WireWith(t, nil, base, quic.Config{StreamReceiveWindow: window})
	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	// The client gives the stream room only as the test says it has read.
	s.HoldCredit()
	head, _ := h3Request{path: "/"}.frames()
	if err := s.Write(head, true); err != nil {
		t.Fatal(err)
	}
	st := w.await(s.ID(), "the window's worth", func(st *h3WireStream) bool { return len(st.body) >= window-64 })
	time.Sleep(100 * time.Millisecond)
	// What the window let go, what may wait in the stream before a write
	// waits, the piece written behind it, and the chunk the handler is in.
	if n := written.Load(); n > window+2*maxUnsentOutput+16<<10 {
		t.Fatalf("the handler wrote %d bytes into a window of %d", n, window)
	}
	credited := 0
	for {
		w.mu.Lock()
		n, finished, changed := len(st.body), st.done, st.changed
		w.mu.Unlock()
		if n > credited {
			s.Credit(n - credited)
			credited = n
		}
		if finished {
			break
		}
		select {
		case <-changed:
		case <-time.After(10 * time.Second):
			t.Fatalf("stalled at %d bytes", n)
		}
	}
	if err := <-done; err != nil || len(st.body) != total || st.reset {
		t.Fatalf("handler %v, body %d bytes, reset %v", err, len(st.body), st.reset)
	}
}

// A handler waiting for the client is let go when the client stops the
// stream, its Write failing.
func TestResponseWaitEndsWithTheStream(t *testing.T) {
	results := make(chan error, 1)
	base := startServer(t, Config{}, func(c *fibhttp.Context, r *stdhttp.Request) {
		chunk := make([]byte, 16<<10)
		for {
			if _, err := c.Write(chunk); err != nil {
				results <- err
				return
			}
		}
	})
	w := dialH3WireWith(t, nil, base, quic.Config{StreamReceiveWindow: 16 << 10})
	s, err := w.qc.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	s.HoldCredit()
	head, _ := h3Request{path: "/"}.frames()
	if err := s.Write(head, true); err != nil {
		t.Fatal(err)
	}
	w.await(s.ID(), "the header", func(st *h3WireStream) bool { return st.status == 200 })
	select {
	case err := <-results:
		t.Fatalf("the handler did not wait: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	s.StopSending(uint64(ErrCodeRequestCancelled))
	select {
	case err := <-results:
		if err == nil {
			t.Fatal("a nil error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler was not let go")
	}
}

// A handler the connection runs on the goroutine its datagrams arrive on
// cannot wait for the client, which only that goroutine would hear from:
// what it writes waits in the stream until flow control lets it go.
func TestResponseOnTheReaderDoesNotWait(t *testing.T) {
	config := Config{}
	config.StreamPool = fibhttp.StreamPoolConfig{Disable: true}
	const total = 3 << 20
	base := startServer(t, config, func(c *fibhttp.Context, r *stdhttp.Request) {
		chunk := bytes.Repeat([]byte("r"), 64<<10)
		for range total / len(chunk) {
			_, _ = c.Write(chunk)
		}
	})
	w := dialH3Wire(t, nil, base)
	id := w.send(t, h3Request{path: "/"}, false)
	if status, body := w.response(id); status != 200 || len(body) != total {
		t.Fatalf("status %d, %d bytes", status, len(body))
	}
}

// A handler still at work on a request whose stream fails under it hears of
// it through OnCancel, whether it has not answered or is streaming its
// answer: the client resetting the request, asking the server to stop
// sending, or closing the connection. One that answered whole is not told.
func TestStreamFailureCancelsTheContext(t *testing.T) {
	config := Config{StreamRequestBody: true}
	cancelled := make(chan error, 8)
	entered := make(chan string, 8)
	base := startServer(t, config, func(c *fibhttp.Context, r *stdhttp.Request) {
		if r.URL.Path == "/quick" {
			c.OnCancel(func(err error) { cancelled <- err })
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("done"))
			return
		}
		done := make(chan struct{})
		c.OnCancel(func(err error) {
			cancelled <- err
			close(done)
		})
		if r.URL.Path == "/streaming" {
			c.Flush()
		}
		entered <- r.URL.Path
		select {
		case <-done:
			if c.Err() == nil {
				t.Error("Err is nil in a cancelled Context")
			}
		case <-time.After(5 * time.Second):
			t.Errorf("%s: the handler was never told", r.URL.Path)
		}
	})
	expectCancel := func(how string) {
		t.Helper()
		select {
		case err := <-cancelled:
			if err == nil {
				t.Fatalf("%s: cancelled with a nil error", how)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: no cancellation", how)
		}
	}
	w := dialH3Wire(t, nil, base)
	if status, body := w.response(w.send(t, h3Request{path: "/quick"}, false)); status != 200 || string(body) != "done" {
		t.Fatalf("quick: %d %q", status, body)
	}
	for _, path := range []string{"/slow", "/streaming"} {
		// The client resets a request whose body is still arriving, which
		// the handler runs before it has.
		s, err := w.qc.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		head, body := h3Request{path: path, framing: h3Length, body: make([]byte, 100)}.frames()
		if err := s.Write(append(head, body[:20]...), false); err != nil {
			t.Fatal(err)
		}
		<-entered
		s.Reset(uint64(ErrCodeRequestCancelled))
		expectCancel(path + " reset")

		// The client asks the server to stop sending.
		if s, err = w.qc.OpenStream(); err != nil {
			t.Fatal(err)
		}
		head, _ = h3Request{path: path}.frames()
		if err := s.Write(head, true); err != nil {
			t.Fatal(err)
		}
		<-entered
		s.StopSending(uint64(ErrCodeRequestCancelled))
		expectCancel(path + " stop sending")
	}
	// The connection closes under a request.
	w.send(t, h3Request{path: "/streaming"}, false)
	<-entered
	w.qc.Close(0, "")
	expectCancel("connection closed")
	select {
	case err := <-cancelled:
		t.Fatalf("an answered request was cancelled too: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
}

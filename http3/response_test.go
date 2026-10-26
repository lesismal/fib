//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"bytes"
	stdhttp "net/http"
	"strconv"
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
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
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
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
		if c.Request.URL.Path == "/declared" {
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
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
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
	base := startServer(t, Config{}, func(c *fibhttp.Context) {
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
	base := startServer(t, config, func(c *fibhttp.Context) {
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
	base := startServer(t, config, func(c *fibhttp.Context) {
		if c.Request.URL.Path == "/quick" {
			c.OnCancel(func(err error) { cancelled <- err })
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("done"))
			return
		}
		done := make(chan struct{})
		c.OnCancel(func(err error) {
			cancelled <- err
			close(done)
		})
		if c.Request.URL.Path == "/streaming" {
			c.Flush()
		}
		entered <- c.Request.URL.Path
		select {
		case <-done:
			if c.Err() == nil {
				t.Error("Err is nil in a cancelled Context")
			}
		case <-time.After(5 * time.Second):
			t.Errorf("%s: the handler was never told", c.Request.URL.Path)
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

// A whole response is kept the same way whatever its body's size, the one of
// 4KiB or less that used to be copied into the buffer its frames were built
// in as much as a larger one: a reader that stalls it with its window and then
// reads gets every byte, and the trailer, back.
func TestResponseWholeBodyStalled(t *testing.T) {
	// The window is small enough that the frames alone cannot carry a body of
	// the sizes below through it, so each response waits in its stream until
	// the reader takes what it has.
	const window = 256
	for _, size := range []int{1 << 10, 4 << 10, 4<<10 + 1, 64 << 10} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			body := make([]byte, size)
			for i := range body {
				body[i] = byte('a' + i%26)
			}
			trailer := "t" + strconv.Itoa(size)
			base := startServer(t, Config{}, func(c *fibhttp.Context) {
				_ = c.WriteResponse(fibhttp.Response{StatusCode: stdhttp.StatusOK, Body: body,
					Trailer: stdhttp.Header{"X-Sum": {trailer}}})
			})
			w := dialH3WireWith(t, nil, base, quic.Config{StreamReceiveWindow: window})
			s, err := w.qc.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			// The reader takes nothing of what arrives until the test says so.
			s.HoldCredit()
			head, _ := h3Request{path: "/"}.frames()
			if err := s.Write(head, true); err != nil {
				t.Fatal(err)
			}
			id := s.ID()
			st := w.await(id, "the header", func(st *h3WireStream) bool { return st.status == stdhttp.StatusOK })
			time.Sleep(50 * time.Millisecond)
			w.mu.Lock()
			got, gotTrailer := len(st.body), st.trailer.Get("X-Sum")
			w.mu.Unlock()
			if got >= size || gotTrailer != "" {
				t.Fatalf("a window of %d took %d of %d bytes and trailer %q", window, got, size, gotTrailer)
			}
			// Read it, giving the stream room as the reader takes it.
			credited := 0
			for {
				w.mu.Lock()
				n, done, changed := len(st.body), st.done, st.changed
				w.mu.Unlock()
				if n > credited {
					s.Credit(n - credited)
					credited = n
				}
				if done {
					break
				}
				select {
				case <-changed:
				case <-time.After(20 * time.Second):
					t.Fatalf("stalled at %d of %d bytes", n, size)
				}
			}
			w.mu.Lock()
			read, readTrailer, reset, readErr := bytes.Clone(st.body), st.trailer.Get("X-Sum"), st.reset, st.err
			w.mu.Unlock()
			if readErr != nil || reset {
				t.Fatalf("the response ended with %v, reset %v", readErr, reset)
			}
			if !bytes.Equal(read, body) || readTrailer != trailer {
				t.Fatalf("read %d bytes and trailer %q, want %d and %q", len(read), readTrailer, size, trailer)
			}
		})
	}
}

// A body no larger than the size that used to be copied with its frames is
// not copied now either: the stream reads the handler's slice as the reader's
// window lets it, so what the handler writes into that slice after Respond
// returns, before the reader has taken it, is what the reader gets.
//
// The handler runs on the connection's reader here (StreamPool.Disable), so
// writing the body after Respond returns and the stream's later read of it are
// that goroutine's alone, and the test is not a data race.
func TestResponseSmallBodyIsNotCopied(t *testing.T) {
	const size = 4 << 10
	const window = 512
	body := bytes.Repeat([]byte("a"), size)
	// The handler changes the tail of its body, which is past the window the
	// reader has, once Respond has returned.
	changed := bytes.Repeat([]byte("b"), window)
	responded, mutate, mutated := make(chan struct{}), make(chan struct{}), make(chan struct{})
	config := Config{}
	config.StreamPool = fibhttp.StreamPoolConfig{Disable: true}
	base := startServer(t, config, func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "application/octet-stream", body)
		close(responded)
		<-mutate
		copy(body[size-window:], changed)
		close(mutated)
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
		t.Fatal("the handler did not return from Respond")
	}
	// Nothing of the tail has gone out: the window carries the frames and the
	// front of the body at most.
	w.mu.Lock()
	taken := len(st.body)
	w.mu.Unlock()
	if taken > size-window {
		t.Fatalf("%d bytes read before the handler changed any, want at most %d", taken, size-window)
	}
	close(mutate)
	<-mutated
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
	want := bytes.Repeat([]byte("a"), size)
	copy(want[size-window:], changed)
	w.mu.Lock()
	read, readErr := bytes.Clone(st.body), st.err
	w.mu.Unlock()
	if readErr != nil {
		t.Fatalf("the response ended with %v", readErr)
	}
	if !bytes.Equal(read, want) {
		tail := read
		if len(tail) > size-window {
			tail = tail[size-window:]
		}
		t.Fatalf("read %d bytes, %d of the tail changed, want %d bytes and %d changed",
			len(read), bytes.Count(tail, []byte("b")), size, window)
	}
}

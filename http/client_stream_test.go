//go:build linux || darwin || windows

package http

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
)

// streamRun records what a callback and its OnBody function were told.
type streamRun struct {
	mu        sync.Mutex
	callbacks int
	status    int
	complete  bool
	callErr   error
	data      []byte
	calls     int
	fins      int
	err       error
	trailer   string
	done      chan struct{}
	// onCall, if set, runs inside the callback instead of taking the body.
	onCall func(*ClientResponse)
}

// run sends req and waits for the callback, and the end of the body when
// OnBody took it.
func (s *streamRun) run(t *testing.T, client *Client, req *stdhttp.Request) {
	t.Helper()
	s.done = make(chan struct{})
	client.Do(req, func(cr *ClientResponse, err error) {
		s.mu.Lock()
		s.callbacks++
		s.mu.Unlock()
		if err != nil {
			s.callErr = err
			close(s.done)
			return
		}
		s.status = cr.StatusCode
		s.complete = cr.BodyComplete()
		if s.onCall != nil {
			s.onCall(cr)
			return
		}
		cr.OnBody(func(data []byte, fin bool, err error) {
			s.mu.Lock()
			s.calls++
			s.data = append(s.data, data...)
			if fin {
				s.fins++
				s.err = err
				s.trailer = cr.Trailer().Get("X-Sum")
			}
			s.mu.Unlock()
			if fin {
				close(s.done)
			}
		})
	})
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the response")
	}
}

func streamConfig(threshold int64) ClientConfig {
	config := DefaultClientConfig()
	config.StreamResponseBody = true
	config.StreamResponseBodyThreshold = threshold
	return config
}

func pattern(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte('a' + i%26)
	}
	return out
}

func TestClientStreamContentLength(t *testing.T) {
	body := pattern(1 << 20)
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		for off := 0; off < len(body); off += 64 << 10 {
			_, _ = w.Write(body[off : off+64<<10])
			w.(stdhttp.Flusher).Flush()
		}
	}))
	defer server.Close()
	config := streamConfig(0)
	config.MaxResponseBodyBytes = 1 << 10 // far below the body: streaming is not bound by it
	client := newTestClient(t, config)

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", server.URL, nil))
	if run.callErr != nil || run.status != 200 {
		t.Fatalf("status %d, error %v", run.status, run.callErr)
	}
	if run.callbacks != 1 || run.fins != 1 || run.err != nil {
		t.Fatalf("callbacks %d, fins %d, err %v", run.callbacks, run.fins, run.err)
	}
	if !bytes.Equal(run.data, body) {
		t.Fatalf("got %d bytes, want %d, or they differ", len(run.data), len(body))
	}
	if run.calls < 2 {
		t.Fatalf("body arrived in %d calls, want it in pieces", run.calls)
	}
}

func TestClientStreamChunkedWithTrailer(t *testing.T) {
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Trailer", "X-Sum")
		for i := 0; i < 5; i++ {
			fmt.Fprintf(w, "piece-%d;", i)
			w.(stdhttp.Flusher).Flush()
			time.Sleep(5 * time.Millisecond)
		}
		w.Header().Set("X-Sum", "5")
	}))
	defer server.Close()
	client := newTestClient(t, streamConfig(0))

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", server.URL, nil))
	if run.complete {
		t.Fatal("a chunked body streaming has not ended when the callback runs")
	}
	if got := string(run.data); got != "piece-0;piece-1;piece-2;piece-3;piece-4;" {
		t.Fatalf("body %q", got)
	}
	if run.trailer != "5" || run.err != nil || run.fins != 1 {
		t.Fatalf("trailer %q, err %v, fins %d", run.trailer, run.err, run.fins)
	}
}

func TestClientStreamUntilClose(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 4096)
		_, _ = conn.Read(buf)
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n")
		for i := 0; i < 4; i++ {
			_, _ = fmt.Fprintf(conn, "part%d.", i)
			time.Sleep(10 * time.Millisecond)
		}
	}()
	client := newTestClient(t, streamConfig(0))

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", "http://"+listener.Addr().String(), nil))
	if got := string(run.data); got != "part0.part1.part2.part3." || run.err != nil || run.fins != 1 {
		t.Fatalf("body %q, err %v, fins %d", got, run.err, run.fins)
	}
}

func TestClientDefaultDeliversWholeBodyToOnBody(t *testing.T) {
	body := pattern(300 << 10)
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		_, _ = w.Write(body)
	}))
	defer server.Close()
	client := newTestClient(t, DefaultClientConfig())

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", server.URL, nil))
	if !run.complete {
		t.Fatal("the response did not arrive whole")
	}
	if run.calls != 1 || run.fins != 1 || !bytes.Equal(run.data, body) {
		t.Fatalf("%d calls, %d fins, %d bytes", run.calls, run.fins, len(run.data))
	}
}

func TestClientStreamThreshold(t *testing.T) {
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		size := 0
		fmt.Sscan(r.URL.Query().Get("size"), &size)
		if r.URL.Query().Get("chunked") == "" {
			w.Header().Set("Content-Length", fmt.Sprint(size))
		}
		for sent := 0; sent < size; sent += 1000 {
			_, _ = w.Write(pattern(min(1000, size-sent)))
			w.(stdhttp.Flusher).Flush()
			time.Sleep(2 * time.Millisecond)
		}
	}))
	defer server.Close()
	client := newTestClient(t, streamConfig(5000))

	for _, tc := range []struct {
		query    string
		size     int
		complete bool
	}{
		{"size=5000", 5000, true},           // not larger than the threshold: whole
		{"size=5001", 5001, false},          // larger by Content-Length: streams
		{"size=3000&chunked=1", 3000, true}, // chunked and over before the threshold
		{"size=12000&chunked=1", 12000, false},
		{"size=0", 0, true},
	} {
		var run streamRun
		run.run(t, client, mustRequest(t, "GET", server.URL+"/?"+tc.query, nil))
		if run.complete != tc.complete {
			t.Errorf("%s: complete at callback = %v, want %v", tc.query, run.complete, tc.complete)
		}
		if len(run.data) != tc.size || run.fins != 1 || run.err != nil {
			t.Errorf("%s: %d bytes, %d fins, err %v", tc.query, len(run.data), run.fins, run.err)
		}
	}
}

func TestClientStreamMaxStreamedBodyBytes(t *testing.T) {
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		for i := 0; i < 20; i++ {
			_, _ = w.Write(pattern(1000))
			w.(stdhttp.Flusher).Flush()
		}
	}))
	defer server.Close()
	config := streamConfig(0)
	config.MaxStreamedBodyBytes = 5000
	client := newTestClient(t, config)

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", server.URL, nil))
	if !errors.Is(run.err, ErrResponseBodyTooLarge) {
		t.Fatalf("err %v, want %v", run.err, ErrResponseBodyTooLarge)
	}
}

func TestClientStreamTimeoutMidBody(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		_, _ = w.Write([]byte("start"))
		w.(stdhttp.Flusher).Flush()
		<-release
	}))
	defer server.Close()
	defer close(release)
	config := streamConfig(0)
	config.Timeout = 200 * time.Millisecond
	client := newTestClient(t, config)

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", server.URL, nil))
	if !errors.Is(run.err, os.ErrDeadlineExceeded) || string(run.data) != "start" || run.fins != 1 {
		t.Fatalf("err %v, data %q, fins %d", run.err, run.data, run.fins)
	}
}

func TestClientStreamCloseGivesUp(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Length", "100000")
		_, _ = w.Write([]byte("start"))
		w.(stdhttp.Flusher).Flush()
		<-release
	}))
	defer server.Close()
	defer close(release)
	client := newTestClient(t, streamConfig(0))

	var run streamRun
	run.onCall = func(cr *ClientResponse) {
		cr.OnBody(func(data []byte, fin bool, err error) {
			run.mu.Lock()
			run.data = append(run.data, data...)
			if fin {
				run.fins++
				run.err = err
			}
			run.mu.Unlock()
			if len(run.data) >= 5 && !fin {
				cr.Close()
			}
			if fin {
				close(run.done)
			}
		})
	}
	run.run(t, client, mustRequest(t, "GET", server.URL, nil))
	if !errors.Is(run.err, ErrResponseAbandoned) || run.fins != 1 {
		t.Fatalf("err %v, fins %d", run.err, run.fins)
	}
}

func TestClientStreamUntakenBody(t *testing.T) {
	var conns connCounter
	server := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		size := 0
		fmt.Sscan(r.URL.Query().Get("size"), &size)
		w.Header().Set("Content-Length", fmt.Sprint(size))
		_, _ = w.Write(pattern(size))
	}))
	server.Config.ConnState = conns.hook
	server.Start()
	defer server.Close()
	client := newTestClient(t, streamConfig(0))

	ignore := func(size int) {
		t.Helper()
		got := make(chan error, 1)
		client.Do(mustRequest(t, "GET", fmt.Sprintf("%s/?size=%d", server.URL, size), nil), func(cr *ClientResponse, err error) {
			got <- err
		})
		if err := <-got; err != nil {
			t.Fatal(err)
		}
		// Nothing says when the rest of an ignored body has been read through.
		time.Sleep(200 * time.Millisecond)
	}
	// A small body the callback ignores is read through, so the connection is
	// kept.
	ignore(1000)
	ignore(1000)
	if n := conns.n.Load(); n != 1 {
		t.Fatalf("two ignored small bodies used %d connections, want 1", n)
	}
	// A large one is not worth reading, and the connection goes.
	ignore(1 << 20)
	var run streamRun
	run.run(t, client, mustRequest(t, "GET", server.URL+"/?size=10", nil))
	if len(run.data) != 10 || run.err != nil {
		t.Fatalf("%d bytes, err %v", len(run.data), run.err)
	}
	if n := conns.n.Load(); n != 2 {
		t.Fatalf("after an ignored large body %d connections, want 2", n)
	}
}

func TestClientStreamKeepsConnectionAlive(t *testing.T) {
	var conns connCounter
	server := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Length", "200000")
		_, _ = w.Write(pattern(200000))
	}))
	server.Config.ConnState = conns.hook
	server.Start()
	defer server.Close()
	client := newTestClient(t, streamConfig(0))

	for i := 0; i < 5; i++ {
		var run streamRun
		run.run(t, client, mustRequest(t, "GET", server.URL, nil))
		if len(run.data) != 200000 || run.err != nil {
			t.Fatalf("request %d: %d bytes, err %v", i, len(run.data), run.err)
		}
	}
	if n := conns.n.Load(); n != 1 {
		t.Fatalf("five streamed responses used %d connections, want 1", n)
	}
}

func TestClientStreamBodyReadWouldBlock(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Length", "10")
		w.WriteHeader(200)
		w.(stdhttp.Flusher).Flush()
		<-release
		_, _ = w.Write([]byte("helloworld"))
	}))
	defer server.Close()
	client := newTestClient(t, streamConfig(0))

	var run streamRun
	var first error
	var body []byte
	run.onCall = func(cr *ClientResponse) {
		_, first = cr.Body.Read(make([]byte, 16)) // the header is all that has arrived
		close(release)
		go func() {
			buf := make([]byte, 16)
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				n, err := cr.Body.Read(buf)
				body = append(body, buf[:n]...)
				if errors.Is(err, ErrWouldBlock) {
					time.Sleep(time.Millisecond)
					continue
				}
				break
			}
			close(run.done)
		}()
	}
	run.run(t, client, mustRequest(t, "GET", server.URL, nil))
	if !errors.Is(first, ErrWouldBlock) {
		t.Fatalf("read before any body: %v, want %v", first, ErrWouldBlock)
	}
	if string(body) != "helloworld" {
		t.Fatalf("body %q", body)
	}
}

func TestClientStreamHoldsReadsForSlowReader(t *testing.T) {
	const size = 8 << 20
	body := pattern(size)
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(size))
		_, _ = w.Write(body)
	}))
	defer server.Close()
	config := streamConfig(0)
	config.StreamResponseBodyBuffer = 64 << 10
	client := newTestClient(t, config)

	var run streamRun
	ready := make(chan *ClientResponse, 1)
	run.onCall = func(cr *ClientResponse) {
		// Mark the body as being read without taking anything yet.
		_, _ = cr.Body.Read(nil)
		ready <- cr
	}
	client.Do(mustRequest(t, "GET", server.URL, nil), func(cr *ClientResponse, err error) {
		if err != nil {
			t.Error(err)
			return
		}
		run.onCall(cr)
	})
	cr := <-ready
	time.Sleep(300 * time.Millisecond)
	cr.s.mu.Lock()
	buffered, held := len(cr.s.buf), cr.s.held
	cr.s.mu.Unlock()
	if !held || buffered > 1<<20 {
		t.Fatalf("buffered %d bytes, held %v: reads were not held back", buffered, held)
	}
	var total int
	var data []byte
	buf := make([]byte, 32<<10)
	deadline := time.Now().Add(10 * time.Second)
	for total < size && time.Now().Before(deadline) {
		n, err := cr.Body.Read(buf)
		data = append(data, buf[:n]...)
		total += n
		if errors.Is(err, ErrWouldBlock) {
			time.Sleep(time.Millisecond)
			continue
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(data, body) {
		t.Fatalf("read %d of %d bytes, or they differ", total, size)
	}
}

func TestClientGoStaysWholeWhenStreaming(t *testing.T) {
	body := pattern(500 << 10)
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		_, _ = w.Write(body)
	}))
	defer server.Close()
	client := newTestClient(t, streamConfig(0))

	resp, err := client.Go(mustRequest(t, "GET", server.URL, nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != string(body) {
		t.Fatalf("got %d bytes", len(got))
	}
}

func TestClientStreamHeadAndNoContent(t *testing.T) {
	server := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if r.URL.Path == "/empty" {
			w.WriteHeader(stdhttp.StatusNoContent)
			return
		}
		w.Header().Set("Content-Length", "1000")
	}))
	defer server.Close()
	client := newTestClient(t, streamConfig(0))

	for _, tc := range []struct{ method, path string }{{"HEAD", "/"}, {"GET", "/empty"}} {
		var run streamRun
		run.run(t, client, mustRequest(t, tc.method, server.URL+tc.path, nil))
		if !run.complete || len(run.data) != 0 || run.fins != 1 || run.err != nil {
			t.Errorf("%s %s: complete %v, %d bytes, fins %d, err %v", tc.method, tc.path, run.complete, len(run.data), run.fins, run.err)
		}
	}
}

func TestClientStreamOneByteWrites(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	response := "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nTrailer: X-Sum\r\n\r\n" +
		"5\r\nhello\r\n6\r\n world\r\n0\r\nX-Sum: 11\r\n\r\n"
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 4096)
		_, _ = conn.Read(buf)
		for i := 0; i < len(response); i++ {
			_, _ = conn.Write([]byte{response[i]})
			time.Sleep(time.Millisecond)
		}
		time.Sleep(200 * time.Millisecond)
	}()
	client := newTestClient(t, streamConfig(0))

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", "http://"+listener.Addr().String(), nil))
	if got := string(run.data); got != "hello world" || run.trailer != "11" || run.err != nil || run.fins != 1 {
		t.Fatalf("body %q, trailer %q, err %v, fins %d", got, run.trailer, run.err, run.fins)
	}
}

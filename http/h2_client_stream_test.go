//go:build linux || darwin || windows

package http

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestClientHTTP2StreamLargeBody(t *testing.T) {
	const size = 8 << 20
	body := pattern(size)
	ts, counter, tlsConfig := newH2TestServer(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(size))
		_, _ = w.Write(body)
	})
	config := streamConfig(0)
	config.TLSConfig = tlsConfig
	config.MaxResponseBodyBytes = 1 << 10
	client := newTestClient(t, config)

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", ts.URL, nil))
	if run.callErr != nil || run.callbacks != 1 || run.fins != 1 || run.err != nil {
		t.Fatalf("callback error %v, callbacks %d, fins %d, err %v", run.callErr, run.callbacks, run.fins, run.err)
	}
	if run.complete {
		t.Fatal("the body had all arrived when the callback ran")
	}
	if !bytes.Equal(run.data, body) {
		t.Fatalf("got %d bytes, want %d, or they differ", len(run.data), size)
	}
	if run.calls < 2 {
		t.Fatalf("body arrived in %d calls, want it in pieces", run.calls)
	}
	if n := counter.n.Load(); n != 1 {
		t.Fatalf("%d connections", n)
	}
}

func TestClientHTTP2StreamTrailer(t *testing.T) {
	ts, _, tlsConfig := newH2TestServer(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Trailer", "X-Sum")
		for i := 0; i < 5; i++ {
			fmt.Fprintf(w, "piece-%d;", i)
			w.(stdhttp.Flusher).Flush()
			time.Sleep(5 * time.Millisecond)
		}
		w.Header().Set("X-Sum", "5")
	})
	config := streamConfig(0)
	config.TLSConfig = tlsConfig
	client := newTestClient(t, config)

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", ts.URL, nil))
	if got := string(run.data); got != "piece-0;piece-1;piece-2;piece-3;piece-4;" {
		t.Fatalf("body %q", got)
	}
	if run.trailer != "5" || run.err != nil || run.fins != 1 {
		t.Fatalf("trailer %q, err %v, fins %d", run.trailer, run.err, run.fins)
	}
}

func TestClientHTTP2StreamThreshold(t *testing.T) {
	ts, _, tlsConfig := newH2TestServer(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		size := 0
		fmt.Sscan(r.URL.Query().Get("size"), &size)
		if r.URL.Query().Get("length") != "" {
			w.Header().Set("Content-Length", strconv.Itoa(size))
		}
		for sent := 0; sent < size; sent += 1000 {
			_, _ = w.Write(pattern(min(1000, size-sent)))
			w.(stdhttp.Flusher).Flush()
			time.Sleep(2 * time.Millisecond)
		}
	})
	config := streamConfig(5000)
	config.TLSConfig = tlsConfig
	client := newTestClient(t, config)

	for _, tc := range []struct {
		query    string
		size     int
		complete bool
	}{
		{"size=5000&length=1", 5000, true},
		{"size=5001&length=1", 5001, false},
		{"size=3000", 3000, true}, // no length, over before the threshold
		{"size=12000", 12000, false},
	} {
		var run streamRun
		run.run(t, client, mustRequest(t, "GET", ts.URL+"/?"+tc.query, nil))
		if run.complete != tc.complete {
			t.Errorf("%s: complete at callback = %v, want %v", tc.query, run.complete, tc.complete)
		}
		if len(run.data) != tc.size || run.fins != 1 || run.err != nil {
			t.Errorf("%s: %d bytes, %d fins, err %v", tc.query, len(run.data), run.fins, run.err)
		}
	}
}

// A threshold beyond the stream window must not stall the stream: what is
// collected before the callback runs is given window at once.
func TestClientHTTP2StreamThresholdBeyondWindow(t *testing.T) {
	const size = 5 << 20
	body := pattern(size)
	ts, _, tlsConfig := newH2TestServer(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(size))
		_, _ = w.Write(body)
	})
	config := streamConfig(2 << 20)
	config.TLSConfig = tlsConfig
	client := newTestClient(t, config)

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", ts.URL, nil))
	if !bytes.Equal(run.data, body) || run.err != nil {
		t.Fatalf("got %d bytes, err %v", len(run.data), run.err)
	}
	if run.complete {
		t.Fatal("the callback waited for the whole body")
	}
}

func TestClientHTTP2StreamWindowPacesSlowReader(t *testing.T) {
	const size = 8 << 20
	body := pattern(size)
	ts, _, tlsConfig := newH2TestServer(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(size))
		_, _ = w.Write(body)
	})
	config := streamConfig(0)
	config.TLSConfig = tlsConfig
	client := newTestClient(t, config)

	ready := make(chan *ClientResponse, 1)
	client.Do(mustRequest(t, "GET", ts.URL, nil), func(cr *ClientResponse, err error) {
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = cr.Body.Read(nil) // reading, but nothing taken yet
		ready <- cr
	})
	cr := <-ready
	time.Sleep(300 * time.Millisecond)
	cr.s.mu.Lock()
	buffered := len(cr.s.buf)
	cr.s.mu.Unlock()
	if buffered == 0 || buffered > h2StreamWindow {
		t.Fatalf("buffered %d bytes, want 1 to %d: the sender was not held to the window", buffered, h2StreamWindow)
	}
	var data []byte
	buf := make([]byte, 32<<10)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		n, err := cr.Body.Read(buf)
		data = append(data, buf[:n]...)
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
		t.Fatalf("read %d of %d bytes, or they differ", len(data), size)
	}
}

// One stream whose reader has stopped must not hold up the others on the
// connection.
func TestClientHTTP2StreamStalledReaderDoesNotBlockOthers(t *testing.T) {
	const size = 4 << 20
	body := pattern(size)
	ts, counter, tlsConfig := newH2TestServer(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(size))
		_, _ = w.Write(body)
	})
	config := streamConfig(0)
	config.TLSConfig = tlsConfig
	client := newTestClient(t, config)

	stalled := make(chan *ClientResponse, 1)
	client.Do(mustRequest(t, "GET", ts.URL, nil), func(cr *ClientResponse, err error) {
		if err == nil {
			_, _ = cr.Body.Read(nil)
			stalled <- cr
		}
	})
	slow := <-stalled

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var run streamRun
			run.run(t, client, mustRequest(t, "GET", ts.URL, nil))
			if !bytes.Equal(run.data, body) || run.err != nil {
				t.Errorf("got %d bytes, err %v", len(run.data), run.err)
			}
		}()
	}
	wg.Wait()
	slow.Close()
	if n := counter.n.Load(); n != 1 {
		t.Fatalf("%d connections, want 1", n)
	}
}

func TestClientHTTP2StreamCloseKeepsConnection(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	ts, counter, tlsConfig := newH2TestServer(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if r.URL.Path == "/big" {
			w.Header().Set("Content-Length", "1000000")
			_, _ = w.Write([]byte("start"))
			w.(stdhttp.Flusher).Flush()
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		_, _ = io.WriteString(w, "after")
	})
	config := streamConfig(0)
	config.TLSConfig = tlsConfig
	client := newTestClient(t, config)

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
	run.run(t, client, mustRequest(t, "GET", ts.URL+"/big", nil))
	if !errors.Is(run.err, ErrResponseAbandoned) || run.fins != 1 {
		t.Fatalf("err %v, fins %d", run.err, run.fins)
	}
	resp, err := client.Go(mustRequest(t, "GET", ts.URL+"/after", nil)).Wait()
	if err != nil || readBody(t, resp) != "after" {
		t.Fatalf("request after Close: %v", err)
	}
	if n := counter.n.Load(); n != 1 {
		t.Fatalf("%d connections, want 1: giving up one stream closed the connection", n)
	}
}

func TestClientHTTP2StreamTimeoutMidBody(t *testing.T) {
	release := make(chan struct{})
	ts, _, tlsConfig := newH2TestServer(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		_, _ = w.Write([]byte("start"))
		w.(stdhttp.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	defer close(release)
	config := streamConfig(0)
	config.TLSConfig = tlsConfig
	config.Timeout = 300 * time.Millisecond
	client := newTestClient(t, config)

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", ts.URL, nil))
	if !errors.Is(run.err, os.ErrDeadlineExceeded) || string(run.data) != "start" || run.fins != 1 {
		t.Fatalf("err %v, data %q, fins %d", run.err, run.data, run.fins)
	}
}

func TestClientHTTP2StreamMaxStreamedBodyBytes(t *testing.T) {
	ts, _, tlsConfig := newH2TestServer(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		for i := 0; i < 20; i++ {
			_, _ = w.Write(pattern(1000))
			w.(stdhttp.Flusher).Flush()
		}
	})
	config := streamConfig(0)
	config.TLSConfig = tlsConfig
	config.MaxStreamedBodyBytes = 5000
	client := newTestClient(t, config)

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", ts.URL, nil))
	if !errors.Is(run.err, ErrResponseBodyTooLarge) {
		t.Fatalf("err %v, want %v", run.err, ErrResponseBodyTooLarge)
	}
}

func TestClientHTTP2StreamUntakenBody(t *testing.T) {
	const size = 2 << 20
	ts, counter, tlsConfig := newH2TestServer(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(size))
		_, _ = w.Write(pattern(size))
	})
	config := streamConfig(0)
	config.TLSConfig = tlsConfig
	client := newTestClient(t, config)

	got := make(chan error, 1)
	client.Do(mustRequest(t, "GET", ts.URL, nil), func(cr *ClientResponse, err error) { got <- err })
	if err := <-got; err != nil {
		t.Fatal(err)
	}
	var run streamRun
	run.run(t, client, mustRequest(t, "GET", ts.URL, nil))
	if len(run.data) != size || run.err != nil {
		t.Fatalf("%d bytes, err %v", len(run.data), run.err)
	}
	if n := counter.n.Load(); n != 1 {
		t.Fatalf("%d connections, want 1", n)
	}
}

func TestClientHTTP2GoStaysWholeWhenStreaming(t *testing.T) {
	body := pattern(1 << 20)
	ts, _, tlsConfig := newH2TestServer(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		_, _ = w.Write(body)
	})
	config := streamConfig(0)
	config.TLSConfig = tlsConfig
	client := newTestClient(t, config)

	resp, err := client.Go(mustRequest(t, "GET", ts.URL, nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != string(body) {
		t.Fatalf("got %d bytes", len(got))
	}
}

func TestClientHTTP2DefaultDeliversWholeBodyToOnBody(t *testing.T) {
	body := pattern(300 << 10)
	ts, _, tlsConfig := newH2TestServer(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		_, _ = w.Write(body)
	})
	config := DefaultClientConfig()
	config.TLSConfig = tlsConfig
	client := newTestClient(t, config)

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", ts.URL, nil))
	if !run.complete || run.calls != 1 || run.fins != 1 || !bytes.Equal(run.data, body) {
		t.Fatalf("complete %v, %d calls, %d fins, %d bytes", run.complete, run.calls, run.fins, len(run.data))
	}
}

func TestClientHTTP2StreamHeadAndNoContent(t *testing.T) {
	ts, _, tlsConfig := newH2TestServer(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if r.URL.Path == "/empty" {
			w.WriteHeader(stdhttp.StatusNoContent)
			return
		}
		w.Header().Set("Content-Length", "1000")
	})
	config := streamConfig(0)
	config.TLSConfig = tlsConfig
	client := newTestClient(t, config)

	for _, tc := range []struct{ method, path string }{{"HEAD", "/"}, {"GET", "/empty"}} {
		var run streamRun
		run.run(t, client, mustRequest(t, tc.method, ts.URL+tc.path, nil))
		if !run.complete || len(run.data) != 0 || run.fins != 1 || run.err != nil {
			t.Errorf("%s %s: complete %v, %d bytes, fins %d, err %v", tc.method, tc.path, run.complete, len(run.data), run.fins, run.err)
		}
	}
}

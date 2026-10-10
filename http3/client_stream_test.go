package http3

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

	fibhttp "github.com/lesismal/fib/http"
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
	onCall func(*fibhttp.ClientResponse)
}

func (s *streamRun) run(t *testing.T, client *Client, req *stdhttp.Request) {
	t.Helper()
	s.done = make(chan struct{})
	client.Do(req, func(cr *fibhttp.ClientResponse, err error) {
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
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for the response")
	}
}

func streaming(threshold int64) func(*ClientConfig) {
	return func(config *ClientConfig) {
		config.StreamResponseBody = true
		config.StreamResponseBodyThreshold = threshold
	}
}

func pattern(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte('a' + i%26)
	}
	return out
}

func TestStreamLargeBodyWithLength(t *testing.T) {
	const size = 8 << 20
	body := pattern(size)
	url := startServer(t, Config{}, func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "application/octet-stream", body)
	})
	client := newClient(t, func(config *ClientConfig) {
		streaming(0)(config)
		config.MaxResponseBodyBytes = 1 << 10 // streaming is not bound by it
	})

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", url+"/", nil))
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
}

func TestStreamWithoutLengthAndTrailer(t *testing.T) {
	url := startServer(t, Config{}, func(c *fibhttp.Context) {
		c.Header().Set("Trailer", "X-Sum")
		for i := 0; i < 5; i++ {
			_, _ = fmt.Fprintf(c, "piece-%d;", i)
			c.Flush()
			time.Sleep(5 * time.Millisecond)
		}
		c.Header().Set("X-Sum", "5")
	})
	client := newClient(t, streaming(0))

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", url+"/", nil))
	if got := string(run.data); got != "piece-0;piece-1;piece-2;piece-3;piece-4;" {
		t.Fatalf("body %q", got)
	}
	if run.trailer != "5" || run.err != nil || run.fins != 1 {
		t.Fatalf("trailer %q, err %v, fins %d", run.trailer, run.err, run.fins)
	}
}

func TestDefaultDeliversWholeBodyToOnBody(t *testing.T) {
	body := pattern(300 << 10)
	url := startServer(t, Config{}, func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "application/octet-stream", body)
	})
	client := newClient(t, nil)

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", url+"/", nil))
	if !run.complete || run.calls != 1 || run.fins != 1 || !bytes.Equal(run.data, body) {
		t.Fatalf("complete %v, %d calls, %d fins, %d bytes", run.complete, run.calls, run.fins, len(run.data))
	}
}

func TestStreamThreshold(t *testing.T) {
	url := startServer(t, Config{}, func(c *fibhttp.Context) {
		size := 0
		_, _ = fmt.Sscan(c.Request.URL.Query().Get("size"), &size)
		if c.Request.URL.Query().Get("length") != "" {
			c.Header().Set("Content-Length", strconv.Itoa(size))
		}
		for sent := 0; sent < size; sent += 1000 {
			_, _ = c.Write(pattern(min(1000, size-sent)))
			c.Flush()
			time.Sleep(2 * time.Millisecond)
		}
	})
	client := newClient(t, streaming(5000))

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
		run.run(t, client, mustRequest(t, "GET", url+"/?"+tc.query, nil))
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
func TestStreamThresholdBeyondWindow(t *testing.T) {
	const size = 6 << 20
	body := pattern(size)
	url := startServer(t, Config{}, func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "application/octet-stream", body)
	})
	client := newClient(t, streaming(3<<20))

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", url+"/", nil))
	if !bytes.Equal(run.data, body) || run.err != nil {
		t.Fatalf("got %d bytes, err %v", len(run.data), run.err)
	}
	if run.complete {
		t.Fatal("the callback waited for the whole body")
	}
}

func TestStreamSlowReaderGetsEveryByte(t *testing.T) {
	const size = 8 << 20
	body := pattern(size)
	url := startServer(t, Config{}, func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "application/octet-stream", body)
	})
	client := newClient(t, streaming(0))

	ready := make(chan *fibhttp.ClientResponse, 1)
	client.Do(mustRequest(t, "GET", url+"/", nil), func(cr *fibhttp.ClientResponse, err error) {
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = cr.Body.Read(nil) // reading, but nothing taken yet
		ready <- cr
	})
	cr := <-ready
	time.Sleep(300 * time.Millisecond)
	var data []byte
	buf := make([]byte, 32<<10)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		n, err := cr.Body.Read(buf)
		data = append(data, buf[:n]...)
		if errors.Is(err, fibhttp.ErrWouldBlock) {
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
func TestStreamStalledReaderDoesNotBlockOthers(t *testing.T) {
	const size = 4 << 20
	body := pattern(size)
	url := startServer(t, Config{}, func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "application/octet-stream", body)
	})
	client := newClient(t, streaming(0))

	stalled := make(chan *fibhttp.ClientResponse, 1)
	client.Do(mustRequest(t, "GET", url+"/", nil), func(cr *fibhttp.ClientResponse, err error) {
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
			run.run(t, client, mustRequest(t, "GET", url+"/", nil))
			if !bytes.Equal(run.data, body) || run.err != nil {
				t.Errorf("got %d bytes, err %v", len(run.data), run.err)
			}
		}()
	}
	wg.Wait()
	slow.Close()
}

func TestStreamCloseKeepsConnection(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	url := startServer(t, Config{}, func(c *fibhttp.Context) {
		if c.Request.URL.Path == "/big" {
			c.Header().Set("Content-Length", "1000000")
			_, _ = c.WriteString("start")
			c.Flush()
			c.Retain()
			go func() {
				<-release
				c.Release()
			}()
			return
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("after"))
	})
	client := newClient(t, streaming(0))

	var run streamRun
	run.onCall = func(cr *fibhttp.ClientResponse) {
		cr.OnBody(func(data []byte, fin bool, err error) {
			run.mu.Lock()
			run.data = append(run.data, data...)
			if fin {
				run.fins++
				run.err = err
			}
			n := len(run.data)
			run.mu.Unlock()
			if n >= 5 && !fin {
				cr.Close()
			}
			if fin {
				close(run.done)
			}
		})
	}
	run.run(t, client, mustRequest(t, "GET", url+"/big", nil))
	if !errors.Is(run.err, fibhttp.ErrResponseAbandoned) || run.fins != 1 {
		t.Fatalf("err %v, fins %d", run.err, run.fins)
	}
	resp, err := client.Go(mustRequest(t, "GET", url+"/after", nil)).Wait()
	if err != nil || readBody(t, resp) != "after" {
		t.Fatalf("request after Close: %v", err)
	}
}

func TestStreamTimeoutMidBody(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	url := startServer(t, Config{}, func(c *fibhttp.Context) {
		_, _ = c.WriteString("start")
		c.Flush()
		c.Retain()
		go func() {
			<-release
			c.Release()
		}()
	})
	client := newClient(t, func(config *ClientConfig) {
		streaming(0)(config)
		config.Timeout = 400 * time.Millisecond
	})

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", url+"/", nil))
	if !errors.Is(run.err, os.ErrDeadlineExceeded) || string(run.data) != "start" || run.fins != 1 {
		t.Fatalf("err %v, data %q, fins %d", run.err, run.data, run.fins)
	}
}

func TestStreamMaxStreamedBodyBytes(t *testing.T) {
	url := startServer(t, Config{}, func(c *fibhttp.Context) {
		for i := 0; i < 20; i++ {
			_, _ = c.Write(pattern(1000))
			c.Flush()
		}
	})
	client := newClient(t, func(config *ClientConfig) {
		streaming(0)(config)
		config.MaxStreamedBodyBytes = 5000
	})

	var run streamRun
	run.run(t, client, mustRequest(t, "GET", url+"/", nil))
	if !errors.Is(run.err, errBodyTooLarge) {
		t.Fatalf("err %v, want %v", run.err, errBodyTooLarge)
	}
}

func TestStreamUntakenBody(t *testing.T) {
	const size = 2 << 20
	url := startServer(t, Config{}, func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "application/octet-stream", pattern(size))
	})
	client := newClient(t, streaming(0))

	got := make(chan error, 1)
	client.Do(mustRequest(t, "GET", url+"/", nil), func(cr *fibhttp.ClientResponse, err error) { got <- err })
	if err := <-got; err != nil {
		t.Fatal(err)
	}
	var run streamRun
	run.run(t, client, mustRequest(t, "GET", url+"/", nil))
	if len(run.data) != size || run.err != nil {
		t.Fatalf("%d bytes, err %v", len(run.data), run.err)
	}
}

func TestStreamGoStaysWhole(t *testing.T) {
	body := pattern(1 << 20)
	url := startServer(t, Config{}, func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "application/octet-stream", body)
	})
	client := newClient(t, streaming(0))

	resp, err := client.Go(mustRequest(t, "GET", url+"/", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != string(body) {
		t.Fatalf("got %d bytes", len(got))
	}
}

func TestStreamHeadAndNoContent(t *testing.T) {
	url := startServer(t, Config{}, func(c *fibhttp.Context) {
		if c.Request.URL.Path == "/empty" {
			c.WriteHeader(stdhttp.StatusNoContent)
			return
		}
		c.Header().Set("Content-Length", "1000")
	})
	client := newClient(t, streaming(0))

	for _, tc := range []struct{ method, path string }{{"HEAD", "/"}, {"GET", "/empty"}} {
		var run streamRun
		run.run(t, client, mustRequest(t, tc.method, url+tc.path, nil))
		if !run.complete || len(run.data) != 0 || run.fins != 1 || run.err != nil {
			t.Errorf("%s %s: complete %v, %d bytes, fins %d, err %v", tc.method, tc.path, run.complete, len(run.data), run.fins, run.err)
		}
	}
}

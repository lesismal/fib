//go:build linux || darwin || windows

package http

import (
	"errors"
	"fmt"
	stdhttp "net/http"
	"os"
	"sync"
	"testing"
	"time"
)

// concurrencyProbe holds every request it is given until the test releases
// them, and records how many it held at once.
type concurrencyProbe struct {
	release chan struct{}
	mu      sync.Mutex
	running int
	peak    int
}

func newConcurrencyProbe(t *testing.T) *concurrencyProbe {
	p := &concurrencyProbe{release: make(chan struct{})}
	// A test that fails before releasing must not leave the handlers held.
	t.Cleanup(p.free)
	return p
}

func (p *concurrencyProbe) ServeHTTP(c *Context, r *stdhttp.Request) {
	p.mu.Lock()
	p.running++
	if p.running > p.peak {
		p.peak = p.running
	}
	p.mu.Unlock()
	<-p.release
	p.mu.Lock()
	p.running--
	p.mu.Unlock()
	_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(r.URL.Path))
}

func (p *concurrencyProbe) free() {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.release:
	default:
		close(p.release)
	}
}

// held is how many requests are held now, and peakRunning how many were
// held at once at the busiest moment.
func (p *concurrencyProbe) held() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

func (p *concurrencyProbe) peakRunning() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak
}

// waitFor waits for n requests to be held at once, and reports whether they
// were.
func (p *concurrencyProbe) waitFor(n int) bool {
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if p.held() >= n {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// TestH2StreamPoolConcurrency checks what one HTTP/2 connection serves at
// once: every request it has framed by default, and no more than the
// configured number when one is set, with the connection reading nothing
// further while it is at that number.
func TestH2StreamPoolConcurrency(t *testing.T) {
	const requests = 6
	for _, tt := range []struct {
		name   string
		pool   StreamPoolConfig
		atOnce int
	}{
		{"unlimited", StreamPoolConfig{}, requests},
		{"limited", StreamPoolConfig{MaxConcurrentHandlers: 2}, 2},
		{"oneAtATime", StreamPoolConfig{MaxConcurrentHandlers: 1}, 1},
		{"disabled", StreamPoolConfig{Disable: true}, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			probe := newConcurrencyProbe(t)
			config := DefaultConfig()
			config.StreamPool = tt.pool
			tc := dialH2(t, serve(t, NewHandlerWithConfig(config, probe)))
			for i := 0; i < requests; i++ {
				tc.headers(uint32(2*i+1), true, get(fmt.Sprintf("/%d", i))...)
			}
			if !probe.waitFor(tt.atOnce) {
				t.Fatalf("%d of %d requests ran at once", probe.peakRunning(), tt.atOnce)
			}
			// Nothing over the limit is served while the handlers are held:
			// the connection has stopped being read.
			time.Sleep(100 * time.Millisecond)
			if peak := probe.peakRunning(); peak != tt.atOnce {
				t.Fatalf("%d requests ran at once, want %d", peak, tt.atOnce)
			}
			probe.free()
			for i := 0; i < requests; i++ {
				id := uint32(2*i + 1)
				header, body := tc.response(id)
				if header[":status"] != "200" || string(body) != fmt.Sprintf("/%d", i) {
					t.Fatalf("stream %d: %v %q", id, header, body)
				}
			}
		})
	}
}

// TestH2StreamPoolHandlerPanic checks that a handler that panics on the pool
// ends its connection, the way one that panics on the connection's own worker
// does: the engine is not there to do it, and the stream would otherwise wait
// for a response that is never written.
func TestH2StreamPoolHandlerPanic(t *testing.T) {
	addr := serve(t, NewHandler(HandlerFunc(func(*Context, *stdhttp.Request) {
		panic("handler gave up")
	})))
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/panic")...)
	_ = tc.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 4096)
	for {
		if _, err := tc.c.Read(buf); err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatal("the connection stayed open after the handler panicked")
			}
			return
		}
	}
}

// TestStreamPoolShared checks that every server gets the one pool, since a
// pool is never stopped and one per server would leave every server's
// workers behind it, and that HTTP/2 and HTTP/3 are served by the same one.
func TestStreamPoolShared(t *testing.T) {
	first := NewStreamPool(StreamPoolConfig{})
	second := NewStreamPool(StreamPoolConfig{MaxConcurrentHandlers: 8})
	if first == nil || second == nil || first.pool != second.pool {
		t.Fatalf("two servers' pools: %v, %v", first, second)
	}
	if pool := NewStreamPool(StreamPoolConfig{Disable: true}); pool != nil {
		t.Fatal("a disabled pool was built")
	}
	if pool := NewStreamPool(StreamPoolConfig{MaxConcurrentHandlers: 1}); pool != nil {
		t.Fatal("a pool was built for handlers that run on the reader")
	}
}

// recordingStream is a Stream that records the responses written to it.
type recordingStream struct {
	mu    sync.Mutex
	paths []string
	done  chan struct{}
}

func (s *recordingStream) WriteResponse(req *stdhttp.Request, _ Response) error {
	s.mu.Lock()
	s.paths = append(s.paths, req.URL.Path)
	s.mu.Unlock()
	s.done <- struct{}{}
	return nil
}
func (s *recordingStream) WriteInterim(int, stdhttp.Header) error { return nil }
func (s *recordingStream) Push(*stdhttp.Request, string, *stdhttp.PushOptions) error {
	return stdhttp.ErrNotSupported
}

// TestStreamPoolQueue checks that requests queued on a batch wait for Submit,
// and that one the connection's limit keeps off the pool is served where it
// is queued, after what was queued ahead of it has gone to the pool.
func TestStreamPoolQueue(t *testing.T) {
	pool := NewStreamPool(StreamPoolConfig{MaxConcurrentHandlers: 3})
	stream := &recordingStream{done: make(chan struct{}, 8)}
	release := make(chan struct{})
	handler := HandlerFunc(func(c *Context, r *stdhttp.Request) {
		if r.URL.Path != "/inline" {
			<-release
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", nil)
	})
	request := func(path string) *StreamRequest {
		r := new(StreamRequest)
		r.URL.Path = path
		r.Request.URL = &r.URL
		r.Request.Method = stdhttp.MethodGet
		r.Request.Header = stdhttp.Header{}
		r.Context(nil, stream, nil)
		return r
	}
	var gate StreamGate
	var batch StreamBatch
	pool.Queue(&batch, &gate, nil, handler, request("/a"))
	pool.Queue(&batch, &gate, nil, handler, request("/b"))
	if gate.Running() != 2 || len(batch.tasks) != 2 {
		t.Fatalf("%d running and %d queued, want both queued and counted", gate.Running(), len(batch.tasks))
	}
	select {
	case <-stream.done:
		t.Fatal("a queued request was served before Submit")
	case <-time.After(20 * time.Millisecond):
	}
	// The third is at the limit: it is served here, and only once the two
	// ahead of it are on the pool.
	pool.Queue(&batch, &gate, nil, handler, request("/inline"))
	if len(batch.tasks) != 0 {
		t.Fatalf("%d requests left queued behind one served in place", len(batch.tasks))
	}
	<-stream.done
	close(release)
	<-stream.done
	<-stream.done
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.paths) != 3 || stream.paths[0] != "/inline" {
		t.Fatalf("served %v", stream.paths)
	}
}

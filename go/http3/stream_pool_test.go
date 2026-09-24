//go:build linux || darwin || windows

package http3

import (
	stdhttp "net/http"
	"sync"
	"testing"
	"time"
	"unsafe"

	fibhttp "github.com/lesismal/fib/go/http"
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

func (p *concurrencyProbe) serve(c *fibhttp.Context, r *stdhttp.Request) {
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

// TestStreamPoolConcurrency checks what one connection serves at once: every
// request it has framed by default, and no more than the configured number
// when one is set.
func TestStreamPoolConcurrency(t *testing.T) {
	const requests = 4
	for _, tt := range []struct {
		name   string
		pool   fibhttp.StreamPoolConfig
		atOnce int
	}{
		{"unlimited", fibhttp.StreamPoolConfig{}, requests},
		{"limited", fibhttp.StreamPoolConfig{MaxConcurrentHandlers: 2}, 2},
		{"oneAtATime", fibhttp.StreamPoolConfig{MaxConcurrentHandlers: 1}, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			probe := newConcurrencyProbe(t)
			url := startServer(t, Config{StreamPool: tt.pool}, probe.serve)
			client := newClient(t, nil)
			futures := make([]*Future, requests)
			for i := range futures {
				futures[i] = client.Go(mustRequest(t, stdhttp.MethodGet, url+"/held", nil))
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
			for i, f := range futures {
				resp, err := f.Wait()
				if err != nil {
					t.Fatalf("request %d: %v", i, err)
				}
				if resp.StatusCode != stdhttp.StatusOK || readBody(t, resp) != "/held" {
					t.Fatalf("request %d: %d %q", i, resp.StatusCode, readBody(t, resp))
				}
			}
		})
	}
}

// A request's stream is allocated with everything serving it takes, and kept
// until the client has acknowledged its response, so its size is memory per
// request in flight: it stays within the allocator's 896-byte size class,
// which one field more would take it out of into the class of 1024.
func TestRequestStreamSize(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("sized for 64-bit platforms")
	}
	if size := unsafe.Sizeof(requestStream{}); size > 896 {
		t.Fatalf("a requestStream takes %d bytes, past the 896-byte size class", size)
	}
}

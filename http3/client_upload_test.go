//go:build linux || darwin || windows

package http3

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	stdhttp "net/http"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
)

// uploadSize is the body the large upload test sends: big enough that
// buffering it would show, smaller under the race detector.
func uploadSize() int64 {
	if raceEnabled {
		return 64 << 20
	}
	return 256 << 20
}

// genBody is a request body made up as it is read, of size bytes, or without
// end for a size below zero. It counts and checksums what it produced, and
// records being closed.
type genBody struct {
	size     int64
	produced atomic.Int64
	crc      uint32
	closedCh chan struct{}
	closed   atomic.Bool
	once     sync.Once
	atEOF    func(crc uint32)
}

var genBlock = func() []byte {
	b := make([]byte, 64<<10)
	for i := range b {
		b[i] = byte(i*7 + i>>8)
	}
	return b
}()

func newGenBody(size int64) *genBody { return &genBody{size: size, closedCh: make(chan struct{})} }

func (g *genBody) Read(p []byte) (int, error) {
	if g.closed.Load() {
		return 0, errors.New("genBody: read after close")
	}
	off := g.produced.Load()
	if g.size >= 0 && off >= g.size {
		if g.atEOF != nil {
			g.atEOF(g.crc)
			g.atEOF = nil
		}
		return 0, io.EOF
	}
	n := len(p)
	if g.size >= 0 {
		n = int(min(int64(n), g.size-off))
	}
	start := int(off % int64(len(genBlock)))
	for i := 0; i < n; {
		i += copy(p[i:n], genBlock[(start+i)%len(genBlock):])
	}
	g.crc = crc32.Update(g.crc, crc32.IEEETable, p[:n])
	g.produced.Add(int64(n))
	return n, nil
}

func (g *genBody) Close() error {
	g.once.Do(func() {
		g.closed.Store(true)
		close(g.closedCh)
	})
	return nil
}

func (g *genBody) waitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-g.closedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("the request body was never closed")
	}
}

// heapPeak samples the live heap, collecting garbage before each sample,
// until stop is called, which returns how far above where it started it went.
// The live heap is what the collection found reachable. MemStats.HeapAlloc
// would also count what was allocated while the collection ran and since: an
// upload makes about twice its body in short-lived buffers, and on a loaded
// machine, where a collection takes long, that counted 150MB of garbage as
// growth.
func heapPeak() (stop func() uint64) {
	live := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	runtime.GC()
	metrics.Read(live)
	base := live[0].Value.Uint64()
	var peak atomic.Uint64
	done, finished := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(finished)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
			}
			runtime.GC()
			metrics.Read(sample)
			if v := sample[0].Value.Uint64(); v > base && v-base > peak.Load() {
				peak.Store(v - base)
			}
		}
	}()
	return func() uint64 {
		close(done)
		<-finished
		return peak.Load()
	}
}

// uploadSink takes a streamed body as it arrives and answers with its length,
// checksum and X-Crc trailer.
func uploadSink(c *fibhttp.Context) {
	h := crc32.NewIEEE()
	var n int64
	c.OnBody(func(data []byte, fin bool, err error) {
		h.Write(data)
		n += int64(len(data))
		if err != nil {
			_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", []byte(err.Error()))
			return
		}
		if fin {
			_ = c.Respond(stdhttp.StatusOK, "text/plain",
				fmt.Appendf(nil, "%d %d %s", n, h.Sum32(), c.Request.Trailer.Get("X-Crc")))
		}
	})
}

func uploadClient(t *testing.T) *Client {
	return newClient(t, func(config *ClientConfig) { config.Timeout = 5 * time.Minute })
}

func TestClientUploadStreamsLargeBody(t *testing.T) {
	url := startServer(t, Config{StreamRequestBody: true}, uploadSink)
	client := uploadClient(t)
	for _, declared := range []bool{true, false} {
		t.Run(fmt.Sprintf("declared=%v", declared), func(t *testing.T) {
			size := uploadSize()
			// The heap is the whole process's: a run is measured again when
			// it is past the bound, and the least kept, as package http's
			// upload tests do, since a large buffer something else holds for
			// a moment is not there every time and buffering the body would be.
			least := uint64(math.MaxUint64)
			for try := 0; try < 3 && least > 48<<20; try++ {
				least = min(least, func() uint64 {
					body := newGenBody(size)
					req := mustRequest(t, stdhttp.MethodPost, url+"/upload", nil)
					req.Body = body
					if declared {
						req.ContentLength = size
					} else {
						req.Trailer = stdhttp.Header{"X-Crc": nil}
						body.atEOF = func(crc uint32) { req.Trailer.Set("X-Crc", strconv.FormatUint(uint64(crc), 10)) }
					}
					stop := heapPeak()
					start := time.Now()
					resp, err := client.Go(req).Wait()
					peak := stop()
					if err != nil {
						t.Fatal(err)
					}
					got := readBody(t, resp)
					body.waitClosed(t)
					want := fmt.Sprintf("%d %d ", size, body.crc)
					if !declared {
						want += strconv.FormatUint(uint64(body.crc), 10)
					}
					if got != want {
						t.Fatalf("server saw %q, want %q", got, want)
					}
					t.Logf("%dMB in %v, client and server live heap peak +%dKB", size>>20, time.Since(start).Round(time.Millisecond), peak>>10)
					return peak
				}())
			}
			peak := least
			if peak > 48<<20 {
				t.Fatalf("heap grew by %dMB sending a %dMB body", peak>>20, size>>20)
			}
		})
	}
}

// A server that stops taking the body holds it back: the client stops
// reading it too, paced by the stream's flow control.
func TestClientUploadWaitsForSlowServer(t *testing.T) {
	const size = 64 << 20
	var consumed atomic.Int64
	release := make(chan struct{})
	url := startServer(t, Config{StreamRequestBody: true}, func(c *fibhttp.Context) {
		h := crc32.NewIEEE()
		stalled := false
		c.OnBody(func(data []byte, fin bool, err error) {
			h.Write(data)
			if consumed.Add(int64(len(data))) >= 1<<20 && !stalled {
				stalled = true
				<-release
			}
			if err != nil {
				_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", nil)
			} else if fin {
				_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%d %d", consumed.Load(), h.Sum32()))
			}
		})
	})
	client := uploadClient(t)
	body := newGenBody(size)
	req := mustRequest(t, stdhttp.MethodPut, url, nil)
	req.Body = body
	f := client.Go(req)
	deadline := time.Now().Add(10 * time.Second)
	for consumed.Load() < 1<<20 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	ahead := body.produced.Load() - consumed.Load()
	close(release)
	resp, err := f.Wait()
	if err != nil {
		t.Fatal(err)
	}
	body.waitClosed(t)
	if got, want := readBody(t, resp), fmt.Sprintf("%d %d", size, body.crc); got != want {
		t.Fatalf("server saw %q, want %q", got, want)
	}
	t.Logf("while the server stalled, the client had read %dKB more than it took", ahead>>10)
	if ahead > 16<<20 {
		t.Fatalf("client read %dMB of the body ahead of a stalled server", ahead>>20)
	}
}

// Cancelling a request mid-upload fails it at once and closes its body, which
// ends a Read on the body that waits.
func TestClientUploadCancel(t *testing.T) {
	var consumed atomic.Int64
	gotErr := make(chan error, 1)
	url := startServer(t, Config{StreamRequestBody: true}, func(c *fibhttp.Context) {
		c.OnBody(func(data []byte, fin bool, err error) {
			consumed.Add(int64(len(data)))
			if fin {
				gotErr <- err
			}
		})
	})
	client := uploadClient(t)
	pr, pw := io.Pipe()
	go func() {
		chunk := make([]byte, 64<<10)
		for i := 0; i < 32; i++ {
			if _, err := pw.Write(chunk); err != nil {
				return
			}
		}
	}()
	body := newReadWatch(pr)
	ctx, cancel := context.WithCancel(context.Background())
	req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodPost, url, body)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	client.Do(req, func(_ *fibhttp.ClientResponse, err error) { result <- err })
	deadline := time.Now().Add(10 * time.Second)
	for consumed.Load() < 2<<20 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("callback got %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not end the request")
	}
	// The body is closed, which ends a Read waiting on it. Whether a Read
	// is waiting when it is depends on where the cancel finds the client's
	// sender — on Windows it can still be waiting for its last write — so
	// only a Read that did end is checked for why.
	select {
	case <-body.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the client left the body open, and a Read waiting on it")
	}
	select {
	case err := <-body.readErr:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("the waiting Read ended with %v, want io.ErrClosedPipe", err)
		}
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case err := <-gotErr:
		if err == nil {
			t.Fatal("the server saw the cancelled body end cleanly")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the server never heard the body was given up")
	}
}

// A server that answers before taking the body gets its answer delivered,
// and the client stops sending.
func TestClientUploadEarlyResponse(t *testing.T) {
	url := startServer(t, Config{StreamRequestBody: true}, func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusRequestEntityTooLarge, "text/plain", []byte("too large"))
	})
	client := uploadClient(t)
	body := newGenBody(-1)
	req := mustRequest(t, stdhttp.MethodPost, url, nil)
	req.Body = body
	resp, err := client.Go(req).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != stdhttp.StatusRequestEntityTooLarge || readBody(t, resp) != "too large" {
		t.Fatalf("status %d", resp.StatusCode)
	}
	body.waitClosed(t)
	resp, err = client.Go(mustRequest(t, stdhttp.MethodPost, url, strings.NewReader("x"))).Wait()
	if err != nil || resp.StatusCode != stdhttp.StatusRequestEntityTooLarge {
		t.Fatalf("next request: %v", err)
	}
}

// A body that fails to read fails the request with its error, and one that
// is not as long as its ContentLength fails it too.
func TestClientUploadBodyErrors(t *testing.T) {
	url := startServer(t, Config{StreamRequestBody: true}, uploadSink)
	client := uploadClient(t)
	boom := errors.New("boom")
	req := mustRequest(t, stdhttp.MethodPost, url, nil)
	req.Body = io.NopCloser(io.MultiReader(io.LimitReader(newGenBody(-1), 3<<20), errReader{boom}))
	if _, err := client.Go(req).Wait(); !errors.Is(err, boom) {
		t.Fatalf("read failure: got %v, want %v", err, boom)
	}
	req = mustRequest(t, stdhttp.MethodPost, url, nil)
	req.Body = newGenBody(2 << 20)
	req.ContentLength = 3 << 20
	if _, err := client.Go(req).Wait(); err == nil || !strings.Contains(err.Error(), "ContentLength") {
		t.Fatalf("short body: got %v", err)
	}
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// readWatch reports the first error a Read on r returns.
type readWatch struct {
	r       io.ReadCloser
	readErr chan error
	closed  chan struct{}
	once    sync.Once
}

func newReadWatch(r io.ReadCloser) *readWatch {
	return &readWatch{r: r, readErr: make(chan error, 1), closed: make(chan struct{})}
}

func (w *readWatch) Read(p []byte) (int, error) {
	n, err := w.r.Read(p)
	if err != nil {
		select {
		case w.readErr <- err:
		default:
		}
	}
	return n, err
}

func (w *readWatch) Close() error {
	w.once.Do(func() { close(w.closed) })
	return w.r.Close()
}

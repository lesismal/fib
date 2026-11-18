//go:build linux || darwin || windows

package http

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// uploadSize is the body the large upload tests send: big enough that
// buffering it would show, smaller under the race detector, which slows the
// copy several times over.
func uploadSize() int64 {
	if raceEnabled {
		return 64 << 20
	}
	return 256 << 20
}

// genBody is a request body made up as it is read, of size bytes, or without
// end for a size below zero, so that sending it buffers nothing of the test's
// own. It counts and checksums what it produced, and records being closed.
type genBody struct {
	size     int64
	produced atomic.Int64
	crc      uint32
	closed   atomic.Bool
	closedCh chan struct{}
	once     sync.Once
	// atEOF, if set, runs as the body ends, as a producer filling in its
	// request's trailer would.
	atEOF func(crc uint32)
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
		c := copy(p[i:n], genBlock[(start+i)%len(genBlock):])
		i += c
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

// uploadSink checksums the bodies it is sent, and answers with the length,
// the checksum and the X-Crc trailer.
func uploadSink(w stdhttp.ResponseWriter, r *stdhttp.Request) {
	h := crc32.NewIEEE()
	n, err := io.Copy(h, r.Body)
	if err != nil {
		w.WriteHeader(stdhttp.StatusBadRequest)
		return
	}
	fmt.Fprintf(w, "%d %d %d %s %s", r.ProtoMajor, n, h.Sum32(), r.Trailer.Get("X-Crc"), r.TransferEncoding)
}

func uploadClientConfig() ClientConfig {
	config := DefaultClientConfig()
	config.Timeout = 5 * time.Minute
	return config
}

func TestClientUploadStreamsLargeBody(t *testing.T) {
	h1 := httptest.NewServer(stdhttp.HandlerFunc(uploadSink))
	t.Cleanup(h1.Close)
	h2, _, tlsConfig := newH2TestServer(t, uploadSink)
	config := uploadClientConfig()
	config.TLSConfig = tlsConfig
	client := newTestClient(t, config)

	for _, tc := range []struct {
		name     string
		url      string
		proto    int
		declared bool
	}{
		{"http1-length", h1.URL, 1, true},
		{"http1-chunked", h1.URL, 1, false},
		{"http2-length", h2.URL, 2, true},
		{"http2-unknown", h2.URL, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			size := uploadSize()
			body := newGenBody(size)
			req := mustRequest(t, stdhttp.MethodPost, tc.url+"/upload", nil)
			req.Body = body
			if tc.declared {
				req.ContentLength = size
			} else {
				// The trailer is filled in as the body ends, as net/http
				// has it.
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
			body.waitClosed(t) // and the producer is done with crc
			want := fmt.Sprintf("%d %d %d", tc.proto, size, body.crc)
			if !tc.declared {
				want += fmt.Sprintf(" %d", body.crc)
				if tc.proto == 1 {
					want += " [chunked]"
				}
			}
			if !strings.HasPrefix(got, want) {
				t.Fatalf("server saw %q, want %q", got, want)
			}
			t.Logf("%dMB in %v, client and server live heap peak +%dKB", size>>20, time.Since(start).Round(time.Millisecond), peak>>10)
			// The server runs in this process too, so the bound covers both;
			// buffering the body anywhere would take all of it.
			if peak > 48<<20 {
				t.Fatalf("heap grew by %dMB sending a %dMB body", peak>>20, size>>20)
			}
		})
	}
}

// A server that stops reading holds the body back: the client stops reading
// it too, rather than queueing what the server does not take.
func TestClientUploadWaitsForSlowServer(t *testing.T) {
	const size = 64 << 20
	var consumed atomic.Int64
	var releaseMu sync.Mutex
	var release chan struct{}
	released := func() chan struct{} {
		releaseMu.Lock()
		defer releaseMu.Unlock()
		return release
	}
	handler := func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		h := crc32.NewIEEE()
		buf := make([]byte, 32<<10)
		stalled := false
		for {
			n, err := r.Body.Read(buf)
			h.Write(buf[:n])
			if consumed.Add(int64(n)) >= 1<<20 && !stalled {
				stalled = true
				<-released()
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				w.WriteHeader(stdhttp.StatusBadRequest)
				return
			}
		}
		fmt.Fprintf(w, "%d %d", consumed.Load(), h.Sum32())
	}
	h1 := httptest.NewServer(stdhttp.HandlerFunc(handler))
	t.Cleanup(h1.Close)
	h2, _, tlsConfig := newH2TestServer(t, handler)
	config := uploadClientConfig()
	config.TLSConfig = tlsConfig
	client := newTestClient(t, config)

	for _, tc := range []struct{ name, url string }{{"http1", h1.URL}, {"http2", h2.URL}} {
		t.Run(tc.name, func(t *testing.T) {
			consumed.Store(0)
			releaseMu.Lock()
			release = make(chan struct{})
			releaseMu.Unlock()
			body := newGenBody(size)
			req := mustRequest(t, stdhttp.MethodPut, tc.url, nil)
			req.Body = body
			f := client.Go(req)
			deadline := time.Now().Add(10 * time.Second)
			for consumed.Load() < 1<<20 && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			time.Sleep(500 * time.Millisecond)
			ahead := body.produced.Load() - consumed.Load()
			close(released())
			resp, err := f.Wait()
			if err != nil {
				t.Fatal(err)
			}
			body.waitClosed(t)
			if got, want := readBody(t, resp), fmt.Sprintf("%d %d", size, body.crc); got != want {
				t.Fatalf("server saw %q, want %q", got, want)
			}
			t.Logf("while the server stalled, the client had read %dKB more than it took", ahead>>10)
			// What is ahead sits in the socket buffers, the server's own
			// buffers and the HTTP/2 windows, and at most one piece and
			// reqbody.MaxQueued in the client.
			if ahead > 16<<20 {
				t.Fatalf("client read %dMB of the body ahead of a stalled server", ahead>>20)
			}
		})
	}
}

// Cancelling a request mid-upload fails it at once and closes its body, which
// ends a Read on the body that waits.
func TestClientUploadCancel(t *testing.T) {
	var consumed atomic.Int64
	gotErr := make(chan error, 1)
	handler := func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		buf := make([]byte, 32<<10)
		for {
			n, err := r.Body.Read(buf)
			consumed.Add(int64(n))
			if err != nil {
				gotErr <- err
				return
			}
		}
	}
	h1 := httptest.NewServer(stdhttp.HandlerFunc(handler))
	t.Cleanup(h1.Close)
	h2, _, tlsConfig := newH2TestServer(t, handler)
	config := uploadClientConfig()
	config.TLSConfig = tlsConfig
	client := newTestClient(t, config)

	for _, tc := range []struct{ name, url string }{{"http1", h1.URL}, {"http2", h2.URL}} {
		t.Run(tc.name, func(t *testing.T) {
			consumed.Store(0)
			// A producer that sends 2MB and then nothing: the client's next
			// Read waits on the pipe until closing the body ends it.
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
			req, err := stdhttp.NewRequestWithContext(ctx, stdhttp.MethodPost, tc.url, body)
			if err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			client.Do(req, func(_ *ClientResponse, err error) { result <- err })
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
				if err == io.EOF {
					t.Fatal("the server saw the cancelled body end cleanly")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the server never heard the body was given up")
			}
		})
	}
}

// A server that answers before reading the body gets its answer delivered,
// and the client stops sending.
func TestClientUploadEarlyResponse(t *testing.T) {
	handler := func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(stdhttp.StatusRequestEntityTooLarge)
		_, _ = io.WriteString(w, "too large")
	}
	h1 := httptest.NewServer(stdhttp.HandlerFunc(handler))
	t.Cleanup(h1.Close)
	h2, _, tlsConfig := newH2TestServer(t, handler)
	config := uploadClientConfig()
	config.TLSConfig = tlsConfig
	client := newTestClient(t, config)

	for _, tc := range []struct{ name, url string }{{"http1", h1.URL}, {"http2", h2.URL}} {
		t.Run(tc.name, func(t *testing.T) {
			body := newGenBody(-1) // never ends
			req := mustRequest(t, stdhttp.MethodPost, tc.url, nil)
			req.Body = body
			resp, err := client.Go(req).Wait()
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != stdhttp.StatusRequestEntityTooLarge || readBody(t, resp) != "too large" {
				t.Fatalf("status %d", resp.StatusCode)
			}
			body.waitClosed(t)
			// The next request works, on whatever connection.
			resp, err = client.Go(mustRequest(t, stdhttp.MethodPost, tc.url, strings.NewReader("x"))).Wait()
			if err != nil || resp.StatusCode != stdhttp.StatusRequestEntityTooLarge {
				t.Fatalf("next request: %v", err)
			}
		})
	}
}

// A body that fails to read fails the request with its error, and one that
// is not as long as its ContentLength fails it too.
func TestClientUploadBodyErrors(t *testing.T) {
	h1 := httptest.NewServer(stdhttp.HandlerFunc(uploadSink))
	t.Cleanup(h1.Close)
	h2, _, tlsConfig := newH2TestServer(t, uploadSink)
	config := uploadClientConfig()
	config.TLSConfig = tlsConfig
	client := newTestClient(t, config)
	boom := errors.New("boom")

	for _, tc := range []struct{ name, url string }{{"http1", h1.URL}, {"http2", h2.URL}} {
		t.Run(tc.name, func(t *testing.T) {
			req := mustRequest(t, stdhttp.MethodPost, tc.url, nil)
			req.Body = io.NopCloser(io.MultiReader(io.LimitReader(newGenBody(-1), 3<<20), iotestErrReader{boom}))
			if _, err := client.Go(req).Wait(); !errors.Is(err, boom) {
				t.Fatalf("read failure: got %v, want %v", err, boom)
			}
			req = mustRequest(t, stdhttp.MethodPost, tc.url, nil)
			req.Body = newGenBody(2 << 20)
			req.ContentLength = 3 << 20
			if _, err := client.Go(req).Wait(); err == nil || !strings.Contains(err.Error(), "ContentLength") {
				t.Fatalf("short body: got %v", err)
			}
		})
	}
}

type iotestErrReader struct{ err error }

func (r iotestErrReader) Read([]byte) (int, error) { return 0, r.err }

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

// fibUploadSink is uploadSink on fib's own server, which hands the body over
// as it arrives.
func fibUploadSink(c *Context) {
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
				fmt.Appendf(nil, "%d %d %d %s", c.Request.ProtoMajor, n, h.Sum32(), c.Request.Trailer.Get("X-Crc")))
		}
	})
}

// The same uploads against fib's own server, over HTTP/1 and h2c, and its
// early answer, which it gives without reading the body; see also
// TestServerEarlyAnswerLingers.
func TestClientUploadToFibServer(t *testing.T) {
	config := DefaultConfig()
	config.StreamRequestBody = true
	sink := "http://" + serve(t, NewHandlerWithConfig(config, HandlerFunc(fibUploadSink)))
	early := "http://" + serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		_ = c.Respond(stdhttp.StatusRequestEntityTooLarge, "text/plain", []byte("too large"))
	})))
	h1 := newTestClient(t, uploadClientConfig())
	h2config := uploadClientConfig()
	h2config.UnencryptedHTTP2 = true
	h2c := newTestClient(t, h2config)

	for _, tc := range []struct {
		name   string
		client *Client
		proto  int
	}{{"http1", h1, 1}, {"h2c", h2c, 2}} {
		for _, declared := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/declared=%v", tc.name, declared), func(t *testing.T) {
				size := uploadSize()
				body := newGenBody(size)
				req := mustRequest(t, stdhttp.MethodPost, sink, nil)
				req.Body = body
				if declared {
					req.ContentLength = size
				} else {
					req.Trailer = stdhttp.Header{"X-Crc": nil}
					body.atEOF = func(crc uint32) { req.Trailer.Set("X-Crc", strconv.FormatUint(uint64(crc), 10)) }
				}
				stop := heapPeak()
				resp, err := tc.client.Go(req).Wait()
				peak := stop()
				if err != nil {
					t.Fatal(err)
				}
				got := readBody(t, resp)
				body.waitClosed(t)
				want := fmt.Sprintf("%d %d %d ", tc.proto, size, body.crc)
				if !declared {
					want += strconv.FormatUint(uint64(body.crc), 10)
				}
				if got != want {
					t.Fatalf("server saw %q, want %q", got, want)
				}
				if peak > 48<<20 {
					t.Fatalf("heap grew by %dMB sending a %dMB body", peak>>20, size>>20)
				}
			})
		}
		t.Run(tc.name+"/early", func(t *testing.T) {
			body := newGenBody(-1)
			req := mustRequest(t, stdhttp.MethodPost, early, nil)
			req.Body = body
			resp, err := tc.client.Go(req).Wait()
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != stdhttp.StatusRequestEntityTooLarge || readBody(t, resp) != "too large" {
				t.Fatalf("status %d", resp.StatusCode)
			}
			body.waitClosed(t)
		})
	}
}

// A fib server that answers an upload before reading it, and closes the
// connection on the rest, closes it lingering: it shuts its write side and
// drops what the client still sends, rather than closing with the body unread
// and resetting a connection whose client, busy writing, may not have read the
// answer yet. Every early answer reaches net/http's client and this one, for
// a body of a declared length and for a chunked one, whether the handler gave
// it to a streamed body or the server refused a body past MaxBodyBytes.
func TestServerEarlyAnswerLingers(t *testing.T) {
	streamed := DefaultConfig()
	streamed.StreamRequestBody = true
	refusing := DefaultConfig()
	refusing.MaxBodyBytes = 1 << 20
	tooLarge := HandlerFunc(func(c *Context) {
		_ = c.Respond(stdhttp.StatusRequestEntityTooLarge, "text/plain", []byte("too large"))
	})
	servers := []struct{ name, url, body string }{
		{"handler", "http://" + serve(t, NewHandlerWithConfig(streamed, tooLarge)), "too large"},
		{"refused", "http://" + serve(t, NewHandlerWithConfig(refusing, tooLarge)),
			stdhttp.StatusText(stdhttp.StatusRequestEntityTooLarge) + "\n"},
	}
	fibClient := newTestClient(t, uploadClientConfig())
	transport := &stdhttp.Transport{}
	t.Cleanup(transport.CloseIdleConnections)
	stdClient := &stdhttp.Client{Transport: transport, Timeout: time.Minute}
	clients := []struct {
		name string
		do   func(*stdhttp.Request) (*stdhttp.Response, error)
	}{
		{"net-http", stdClient.Do},
		{"fib", func(req *stdhttp.Request) (*stdhttp.Response, error) { return fibClient.Go(req).Wait() }},
	}

	runs := 50
	if testing.Short() {
		runs = 10
	}
	for _, server := range servers {
		for _, client := range clients {
			for _, declared := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%s/declared=%v", server.name, client.name, declared), func(t *testing.T) {
					for i := range runs {
						body := newGenBody(-1)
						req := mustRequest(t, stdhttp.MethodPost, server.url, nil)
						req.Body = body
						if declared {
							req.ContentLength = 1 << 40
						}
						resp, err := client.do(req)
						if err != nil {
							t.Fatalf("run %d: %v", i, err)
						}
						if got := readBody(t, resp); resp.StatusCode != stdhttp.StatusRequestEntityTooLarge || got != server.body {
							t.Fatalf("run %d: status %d, body %q", i, resp.StatusCode, got)
						}
						body.waitClosed(t)
					}
				})
			}
		}
	}
}

// A request that fails before it is sent still has its body closed, as a
// transport closes it whatever becomes of the request: one Do turns away
// outright, for its scheme, whether its body streams or would have been read
// whole.
func TestClientRejectedRequestClosesBody(t *testing.T) {
	client := newTestClient(t, uploadClientConfig())
	for _, size := range []int64{-1, 16} {
		body := newGenBody(size)
		req := mustRequest(t, stdhttp.MethodPost, "ftp://127.0.0.1:1/", nil)
		req.Body = body
		if size >= 0 {
			req.ContentLength = size
		}
		if _, err := client.Go(req).Wait(); !errors.Is(err, ErrUnsupportedScheme) {
			t.Fatalf("size %d: %v", size, err)
		}
		body.waitClosed(t)
	}
}

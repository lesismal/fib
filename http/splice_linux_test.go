//go:build linux

package http

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/internal/debughook"
)

// spliceFront is a proxy in front of backend, as fit's is: a body that
// streams goes upstream through a BodyWriter, offered for splicing unless
// noSplice says not to, and pushed through OnBody otherwise, or when the
// client declines the offer.
type spliceFront struct {
	client   *Client
	backend  string
	noSplice bool
	// unsized sends the upstream request with no length, which the client
	// frames in chunks, and so cannot splice.
	unsized bool
	// offered counts the bodies SpliceBody offered, fallbacks the offers
	// declined, and failures the requests the client failed.
	offered, fallbacks, failures atomic.Int64
	// conns are the connections bodies were offered from, for a test that
	// finds one stuck to describe.
	conns sync.Map
}

// state describes the connections bodies were offered from.
func (f *spliceFront) state() string {
	var b strings.Builder
	f.conns.Range(func(k, _ any) bool {
		fmt.Fprintf(&b, "\n%p: %s", k, debughook.ConnState(k))
		return true
	})
	return b.String()
}

func (f *spliceFront) serve(c *Context) {
	req, err := stdhttp.NewRequest(c.Request.Method, f.backend+c.Request.URL.RequestURI(), nil)
	if err != nil {
		_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", []byte(err.Error()))
		return
	}
	c.Retain()
	respond := func(resp *ClientResponse, err error) {
		defer c.Release()
		if err != nil {
			f.failures.Add(1)
			_ = c.Respond(stdhttp.StatusBadGateway, "text/plain", []byte(err.Error()))
			return
		}
		body, _ := io.ReadAll(resp.Body)
		_ = c.Respond(resp.StatusCode, "text/plain", body)
	}
	if c.RequestBody() == nil {
		req.Body, req.ContentLength = NewBufferedBody(bytes.Clone(c.Body())), int64(len(c.Body()))
		f.client.Do(req, respond)
		return
	}
	body := NewBodyWriter(64 << 10)
	req.Body, req.ContentLength = body, c.Request.ContentLength
	if f.unsized {
		req.ContentLength = -1
	}
	push := func() {
		c.OnBody(func(data []byte, fin bool, err error) {
			switch {
			case err != nil:
				body.CloseWithError(err)
				return
			case len(data) > 0:
				if _, werr := body.Write(data); werr != nil {
					return
				}
			}
			if fin {
				_ = body.Close()
				return
			}
			c.HoldBody(true)
			if !body.OnDrain(func() { c.HoldBody(false) }) {
				c.HoldBody(false)
			}
		})
	}
	if !f.noSplice && c.SpliceBody(body, func() { f.fallbacks.Add(1); push() }) {
		f.offered.Add(1)
		f.conns.Store(c.Conn, struct{}{})
		f.client.Do(req, respond)
		return
	}
	f.client.Do(req, respond)
	push()
}

// spliceServerConfig is a server's config that streams bodies past 64 KB,
// buffering as much of them, as fit's does.
func spliceServerConfig() Config {
	config := DefaultConfig()
	config.StreamRequestBody = true
	config.StreamRequestBodyThreshold = 64 << 10
	config.StreamRequestBodyBuffer = 64 << 10
	return config
}

// newSpliceFront serves a spliceFront in front of backend with client.
func newSpliceFront(t *testing.T, client *Client, backend string) (*spliceFront, string) {
	t.Helper()
	f := &spliceFront{client: client, backend: backend}
	return f, serve(t, NewHandlerWithConfig(spliceServerConfig(), HandlerFunc(f.serve)))
}

func postHead(size int, extra string) string {
	return "POST /upload HTTP/1.1\r\nHost: splice\r\nContent-Length: " + strconv.Itoa(size) + "\r\n" + extra + "\r\n"
}

// uploadAnswer is what fibUploadSink answers an HTTP/1 upload of body with.
func uploadAnswer(body []byte) string {
	return fmt.Sprintf("1 %d %d ", len(body), crc32.ChecksumIEEE(body))
}

func genBytes(n int) []byte {
	b := make([]byte, n)
	for off := 0; off < n; off += len(genBlock) {
		copy(b[off:], genBlock)
	}
	return b
}

// An upload through the proxy is spliced to the backend whole, of any size
// past the threshold, and the request pipelined behind it is served after it
// on the same connection; the client's connection to the backend carries the
// next upload too.
func TestSpliceBodyProxiesUploads(t *testing.T) {
	backend := "http://" + serve(t, NewHandlerWithConfig(spliceServerConfig(), HandlerFunc(fibUploadSink)))
	f, front := newSpliceFront(t, newTestClient(t, uploadClientConfig()), backend)
	raw := dialRaw(t, front)
	_ = raw.SetDeadline(time.Now().Add(60 * time.Second))
	sizes := []int{100 << 10, 1<<20 + 3, 8 << 20}
	for _, size := range sizes {
		body := genBytes(size)
		next := "GET /next HTTP/1.1\r\nHost: splice\r\n\r\n"
		go func() {
			_, _ = raw.Write([]byte(postHead(size, "")))
			_, _ = raw.Write(append(bytes.Clone(body), next...))
		}()
		resp, got := raw.response(stdhttp.MethodPost)
		if resp.StatusCode != stdhttp.StatusOK || got != uploadAnswer(body) {
			t.Fatalf("size %d: %d %q, want %q", size, resp.StatusCode, got, uploadAnswer(body))
		}
		if _, got := raw.response(stdhttp.MethodGet); got != uploadAnswer(nil) {
			t.Fatalf("the pipelined request got %q", got)
		}
	}
	if n := f.offered.Load(); n != int64(len(sizes)) {
		t.Fatalf("%d bodies offered, want %d", n, len(sizes))
	}
	if n := f.fallbacks.Load(); n != 0 {
		t.Fatalf("%d offers declined", n)
	}
}

// The backend's connection is reused from one spliced upload to the next: the
// response, which can be read before the splice's last write has been
// reported, finds the body gone.
func TestSpliceBodyReusesBackendConnection(t *testing.T) {
	var conns connCounter
	server := httptest.NewUnstartedServer(stdhttp.HandlerFunc(uploadSink))
	server.Config.ConnState = conns.hook
	server.Start()
	t.Cleanup(server.Close)
	f, front := newSpliceFront(t, newTestClient(t, uploadClientConfig()), server.URL)
	raw := dialRaw(t, front)
	const uploads = 20
	for i := range uploads {
		body := genBytes(256<<10 + i)
		go func() { _, _ = raw.Write(append([]byte(postHead(len(body), "")), body...)) }()
		resp, got := raw.response(stdhttp.MethodPost)
		want := fmt.Sprintf("1 %d %d  []", len(body), crc32.ChecksumIEEE(body))
		if resp.StatusCode != stdhttp.StatusOK || got != want {
			t.Fatalf("upload %d: %d %q, want %q", i, resp.StatusCode, got, want)
		}
	}
	if n := f.offered.Load(); n != uploads || f.fallbacks.Load() != 0 {
		t.Fatalf("%d offered, %d declined", n, f.fallbacks.Load())
	}
	if n := conns.n.Load(); n != 1 {
		t.Fatalf("the backend saw %d connections", n)
	}
}

// Bodies that cannot be spliced go the ordinary way: one that arrived whole,
// one sent in chunks, which SpliceBody does not offer, and one the client
// sends over h2c, which declines the offer, and the body is pushed instead.
func TestSpliceBodyFallsBack(t *testing.T) {
	backend := "http://" + serve(t, NewHandlerWithConfig(spliceServerConfig(), HandlerFunc(fibUploadSink)))
	t.Run("whole and chunked", func(t *testing.T) {
		f, front := newSpliceFront(t, newTestClient(t, uploadClientConfig()), backend)
		raw := dialRaw(t, front)
		small := genBytes(1000)
		raw.send(postHead(len(small), "") + string(small))
		if _, got := raw.response(stdhttp.MethodPost); got != uploadAnswer(small) {
			t.Fatalf("whole: %q", got)
		}
		big := genBytes(300 << 10)
		var chunked bytes.Buffer
		chunked.WriteString("POST /upload HTTP/1.1\r\nHost: splice\r\nTransfer-Encoding: chunked\r\n\r\n")
		for off := 0; off < len(big); off += 50 << 10 {
			piece := big[off:min(off+50<<10, len(big))]
			fmt.Fprintf(&chunked, "%x\r\n", len(piece))
			chunked.Write(piece)
			chunked.WriteString("\r\n")
		}
		chunked.WriteString("0\r\n\r\n")
		go func() { _, _ = raw.Write(chunked.Bytes()) }()
		if _, got := raw.response(stdhttp.MethodPost); got != fmt.Sprintf("1 %d %d ", len(big), crc32.ChecksumIEEE(big)) {
			t.Fatalf("chunked: %q", got)
		}
		if f.offered.Load() != 0 {
			t.Fatal("a body that cannot be spliced was offered")
		}
	})
	t.Run("h2c upstream", func(t *testing.T) {
		config := uploadClientConfig()
		config.UnencryptedHTTP2 = true
		f, front := newSpliceFront(t, newTestClient(t, config), backend)
		raw := dialRaw(t, front)
		for _, size := range []int{200 << 10, 3 << 20} {
			body := genBytes(size)
			go func() { _, _ = raw.Write(append([]byte(postHead(size, "")), body...)) }()
			if _, got := raw.response(stdhttp.MethodPost); got != fmt.Sprintf("2 %d %d ", size, crc32.ChecksumIEEE(body)) {
				t.Fatalf("size %d: %q", size, got)
			}
		}
		if f.offered.Load() != 2 || f.fallbacks.Load() != 2 {
			t.Fatalf("%d offered, %d declined", f.offered.Load(), f.fallbacks.Load())
		}
	})
	t.Run("chunked upstream", func(t *testing.T) {
		f, front := newSpliceFront(t, newTestClient(t, uploadClientConfig()), backend)
		f.unsized = true
		raw := dialRaw(t, front)
		body := genBytes(1 << 20)
		go func() { _, _ = raw.Write(append([]byte(postHead(len(body), "")), body...)) }()
		if _, got := raw.response(stdhttp.MethodPost); got != uploadAnswer(body) {
			t.Fatalf("got %q", got)
		}
		if f.offered.Load() != 1 || f.fallbacks.Load() != 1 {
			t.Fatalf("%d offered, %d declined", f.offered.Load(), f.fallbacks.Load())
		}
	})
}

// A client waiting for 100 Continue is sent it as the splice begins, and
// then its body is spliced.
func TestSpliceBodyExpectContinue(t *testing.T) {
	backend := "http://" + serve(t, NewHandlerWithConfig(spliceServerConfig(), HandlerFunc(fibUploadSink)))
	config := spliceServerConfig()
	// The body streams from its header, as it does with Expect only once
	// it is asked for.
	config.StreamRequestBodyAtHeader = func(*stdhttp.Request) bool { return true }
	f := &spliceFront{client: newTestClient(t, uploadClientConfig()), backend: backend}
	front := serve(t, NewHandlerWithConfig(config, HandlerFunc(f.serve)))
	raw := dialRaw(t, front)
	body := genBytes(2 << 20)
	raw.send(postHead(len(body), "Expect: 100-continue\r\n"))
	if line, err := raw.r.ReadString('\n'); err != nil || !strings.Contains(line, "100") {
		t.Fatalf("got %q, %v, want 100 Continue", line, err)
	}
	if line, _ := raw.r.ReadString('\n'); line != "\r\n" {
		t.Fatalf("100 Continue ended with %q", line)
	}
	go func() { _, _ = raw.Write(body) }()
	if _, got := raw.response(stdhttp.MethodPost); got != uploadAnswer(body) {
		t.Fatalf("got %q", got)
	}
	if f.offered.Load() != 1 || f.fallbacks.Load() != 0 {
		t.Fatalf("%d offered, %d declined", f.offered.Load(), f.fallbacks.Load())
	}
}

// outcomeSink is fibUploadSink that also reports how each upload ended.
func outcomeSink(outcomes chan<- error) HandlerFunc {
	return func(c *Context) {
		var n int64
		c.OnBody(func(data []byte, fin bool, err error) {
			n += int64(len(data))
			if err != nil {
				outcomes <- err
				return
			}
			if fin {
				outcomes <- nil
				_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(strconv.FormatInt(n, 10)))
			}
		})
	}
}

// A client that goes away in the middle of a spliced body cuts the backend's
// request short, which the backend hears as a body that failed.
func TestSpliceBodyClientGone(t *testing.T) {
	outcomes := make(chan error, 4)
	backend := "http://" + serve(t, NewHandlerWithConfig(spliceServerConfig(), outcomeSink(outcomes)))
	f, front := newSpliceFront(t, newTestClient(t, uploadClientConfig()), backend)
	raw := dialRaw(t, front)
	raw.send(postHead(8<<20, ""))
	_, _ = raw.Write(genBytes(1 << 20))
	spliceWait(t, "the splice", func() bool { return f.offered.Load() == 1 })
	time.Sleep(50 * time.Millisecond)
	raw.Close()
	select {
	case err := <-outcomes:
		if err == nil {
			t.Fatal("the backend took a cut body for a whole one")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the backend never heard the upload end")
	}
	if f.fallbacks.Load() != 0 {
		t.Fatal("the offer was declined")
	}
}

// A backend that goes away in the middle of a spliced body has its request
// fail, and the proxy answers 502, then closes the client's connection,
// whose body it will not read to its end; one that answers before it has
// read the body has its answer relayed.
func TestSpliceBodyBackendGone(t *testing.T) {
	t.Run("closed", func(t *testing.T) {
		backend := "http://" + serve(t, NewHandlerWithConfig(spliceServerConfig(), HandlerFunc(func(c *Context) {
			var n int
			c.OnBody(func(data []byte, fin bool, err error) {
				if n += len(data); n > 512<<10 {
					_ = c.Conn.Close()
				}
			})
		})))
		f, front := newSpliceFront(t, newTestClient(t, uploadClientConfig()), backend)
		raw := dialRaw(t, front)
		go func() {
			_, _ = raw.Write([]byte(postHead(16<<20, "")))
			_, _ = raw.Write(genBytes(16 << 20))
		}()
		resp, _ := raw.response(stdhttp.MethodPost)
		if resp.StatusCode != stdhttp.StatusBadGateway {
			t.Fatalf("status %d", resp.StatusCode)
		}
		if !raw.closed() {
			t.Fatal("the client's connection stayed open")
		}
		if f.offered.Load() != 1 || f.failures.Load() != 1 {
			t.Fatalf("%d offered, %d failed", f.offered.Load(), f.failures.Load())
		}
	})
	t.Run("early answer", func(t *testing.T) {
		backend := "http://" + serve(t, NewHandlerWithConfig(spliceServerConfig(), HandlerFunc(func(c *Context) {
			_ = c.Respond(stdhttp.StatusRequestEntityTooLarge, "text/plain", []byte("too large"))
		})))
		_, front := newSpliceFront(t, newTestClient(t, uploadClientConfig()), backend)
		raw := dialRaw(t, front)
		go func() {
			_, _ = raw.Write([]byte(postHead(16<<20, "")))
			_, _ = raw.Write(genBytes(16 << 20))
		}()
		resp, got := raw.response(stdhttp.MethodPost)
		if resp.StatusCode != stdhttp.StatusRequestEntityTooLarge || got != "too large" {
			t.Fatalf("%d %q", resp.StatusCode, got)
		}
	})
}

// Many uploads spliced at once each arrive whole.
func TestSpliceBodyConcurrent(t *testing.T) {
	backend := "http://" + serve(t, NewHandlerWithConfig(spliceServerConfig(), HandlerFunc(fibUploadSink)))
	f, front := newSpliceFront(t, newTestClient(t, uploadClientConfig()), backend)
	const clients, rounds = 32, 3
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for i := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", front)
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			raw := &rawConn{t: t, Conn: conn, r: bufio.NewReader(conn)}
			_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
			for round := range rounds {
				body := genBytes(512<<10 + i*1000 + round)
				go func() { _, _ = conn.Write(append([]byte(postHead(len(body), "")), body...)) }()
				resp, err := stdhttp.ReadResponse(raw.r, &stdhttp.Request{Method: stdhttp.MethodPost})
				if err != nil {
					errs <- err
					return
				}
				got, _ := io.ReadAll(resp.Body)
				if string(got) != uploadAnswer(body) {
					errs <- fmt.Errorf("client %d round %d: %q", i, round, got)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		t.Log(f.state())
	}
	if f.fallbacks.Load() != 0 || f.offered.Load() != clients*rounds {
		t.Fatalf("%d offered, %d declined", f.offered.Load(), f.fallbacks.Load())
	}
}

// SpliceBody offers nothing it cannot splice, and a body offered and then
// read finds it taken.
func TestSpliceBodyRefuses(t *testing.T) {
	var c Context
	if c.SpliceBody(NewBodyWriter(0), func() {}) {
		t.Fatal("a Context with no request offered a body")
	}
	results := make(chan string, 4)
	front := serve(t, NewHandlerWithConfig(spliceServerConfig(), HandlerFunc(func(c *Context) {
		var verdicts []string
		verdicts = append(verdicts, strconv.FormatBool(c.SpliceBody(nil, func() {})))
		verdicts = append(verdicts, strconv.FormatBool(c.SpliceBody(NewBodyWriter(0), nil)))
		w := NewBodyWriter(0)
		offered := c.SpliceBody(w, func() {})
		verdicts = append(verdicts, strconv.FormatBool(offered))
		if offered {
			s, ok := w.TakeSplice().(*bodySplice)
			if ok {
				// One that does not match the request's length is refused.
				verdicts = append(verdicts, strconv.FormatBool(s.SpliceTo(c.Conn, 1, nil)))
				// Nor can a body go to the connection it arrives on.
				verdicts = append(verdicts, strconv.FormatBool(s.SpliceTo(c.Conn, c.Request.ContentLength, nil)))
			}
		}
		results <- strings.Join(verdicts, " ")
		_ = c.Respond(stdhttp.StatusOK, "text/plain", nil)
	})))
	raw := dialRaw(t, front)
	go func() { _, _ = raw.Write(append([]byte(postHead(256<<10, "")), genBytes(256<<10)...)) }()
	if got := <-results; got != "false false true false false" {
		t.Fatalf("got %q", got)
	}
}

// A body read while it is being spliced is not the reader's.
func TestSpliceBodyReadWhileSplicing(t *testing.T) {
	s := &BodyStream{splicing: true}
	if _, err := s.Read(make([]byte, 1)); !errors.Is(err, ErrBodyAbandoned) {
		t.Fatalf("got %v", err)
	}
}

func spliceWait(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

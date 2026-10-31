package http

import (
	"bytes"
	"io"
	stdhttp "net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/lesismal/fib/bufferpool"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// TestClientTakeBody checks that a response that arrived whole hands its body
// over: a small one over HTTP/1 as a buffer from the pool, a larger one or
// one over HTTP/2 as bytes of its own, none for a body that streams, and that
// the response finds its body empty once it has been taken.
func TestClientTakeBody(t *testing.T) {
	handler := func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		switch r.URL.Path {
		case "/large":
			_, _ = w.Write(pattern(1 << 20))
		case "/medium":
			_, _ = w.Write(pattern(8 << 10))
		default:
			_, _ = w.Write([]byte("body of " + r.URL.Path))
		}
	}
	h1 := httptest.NewServer(stdhttp.HandlerFunc(handler))
	t.Cleanup(h1.Close)
	h2, _, tlsConfig := newH2TestServer(t, handler)
	type result struct {
		body          []byte
		pooled, ok    bool
		after         []byte
		afterOK, done bool
	}
	for _, proto := range []string{"HTTP/1.1", "HTTP/2.0"} {
		config := streamConfig(64 << 10)
		url := h1.URL
		if proto == "HTTP/2.0" {
			config.TLSConfig, url = tlsConfig, h2.URL
		}
		client := newTestClient(t, config)
		get := func(path string) result {
			got := make(chan result, 1)
			client.Do(mustRequest(t, "GET", url+path, nil), func(cr *ClientResponse, err error) {
				if err != nil {
					t.Error(err)
					got <- result{}
					return
				}
				var r result
				r.body, r.pooled, r.ok = cr.TakeBody()
				r.after, r.afterOK = cr.BodyBytes()
				r.done = true
				got <- r
				cr.Close()
			})
			return <-got
		}
		small := get("/a")
		if !small.ok || string(small.body) != "body of /a" {
			t.Errorf("%s: took %q, %v", proto, small.body, small.ok)
		}
		if want := proto == "HTTP/1.1"; small.pooled != want {
			t.Errorf("%s: a small body taken reports pooled %v, want %v", proto, small.pooled, want)
		}
		if !small.afterOK || len(small.after) != 0 {
			t.Errorf("%s: after TakeBody, BodyBytes gave %q, %v", proto, small.after, small.afterOK)
		}
		if small.pooled {
			bufferpool.Put(small.body)
		}
		medium := get("/medium")
		if !medium.ok || !bytes.Equal(medium.body, pattern(8<<10)) || medium.pooled {
			t.Errorf("%s: took %d bytes, pooled %v, ok %v", proto, len(medium.body), medium.pooled, medium.ok)
		}
		if large := get("/large"); !large.done || large.ok {
			t.Errorf("%s: a streamed body handed over %d bytes", proto, len(large.body))
		}
	}
}

// TestClientBufferedRequestBody checks that a request whose Body is a
// NewBufferedBody goes with its bytes as they are, over HTTP/1 and HTTP/2,
// without the client reading the body into a copy of its own.
func TestClientBufferedRequestBody(t *testing.T) {
	handler := func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		body, _ := io.ReadAll(r.Body)
		_, _ = w.Write(body)
	}
	h1 := httptest.NewServer(stdhttp.HandlerFunc(handler))
	t.Cleanup(h1.Close)
	h2, _, tlsConfig := newH2TestServer(t, handler)
	data := pattern(2 << 10)
	for _, proto := range []string{"HTTP/1.1", "HTTP/2.0"} {
		config := DefaultClientConfig()
		url := h1.URL
		if proto == "HTTP/2.0" {
			config.TLSConfig, url = tlsConfig, h2.URL
		}
		client := newTestClient(t, config)
		body := NewBufferedBody(data)
		req := mustRequest(t, "POST", url+"/echo", nil)
		req.Body, req.ContentLength = body, int64(len(data))
		got := make(chan []byte, 1)
		client.Do(req, func(cr *ClientResponse, err error) {
			if err != nil {
				t.Error(err)
				got <- nil
				return
			}
			echo, _ := cr.BodyBytes()
			got <- bytes.Clone(echo)
		})
		if echo := <-got; !bytes.Equal(echo, data) {
			t.Errorf("%s: the server read %d bytes, want %d", proto, len(echo), len(data))
		}
		// The client took the bytes without reading them, so the body still
		// has all of them to give.
		if rest, _ := io.ReadAll(body); !bytes.Equal(rest, data) {
			t.Errorf("%s: the client read the body, which has %d bytes left", proto, len(rest))
		}
	}
}

// TestWriteResponseOwned checks that a response sent with a body from the
// pool that it takes over arrives as it was, small and large, over HTTP/1 and
// HTTP/2.
func TestWriteResponseOwned(t *testing.T) {
	handler := HandlerFunc(func(c *Context) {
		n := 100
		if c.Request.URL.Path == "/large" {
			n = 64 << 10
		}
		body := bufferpool.Get(n)
		copy(body, pattern(n))
		if err := c.WriteResponseOwned(Response{StatusCode: stdhttp.StatusOK, Body: body}); err != nil {
			t.Error(err)
		}
	})
	addr := serveHTTP1(t, handler)
	serverConfig, clientConfig, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	tlsAddr := serve(t, fibtls.NewServer(ConfigureTLS(serverConfig), NewHandler(handler)))
	clients := map[string]*stdhttp.Client{
		"http://" + addr: {Timeout: 10 * time.Second},
		"https://" + tlsAddr: {Timeout: 10 * time.Second, Transport: &stdhttp.Transport{
			TLSClientConfig: clientConfig.Clone(), ForceAttemptHTTP2: true}},
	}
	for base, client := range clients {
		for _, path := range []string{"/small", "/large", "/small"} {
			n := 100
			if path == "/large" {
				n = 64 << 10
			}
			resp, body := do(t, client, mustRequest(t, "GET", base+path, nil))
			if base == "https://"+tlsAddr && resp.ProtoMajor != 2 {
				t.Errorf("%s: %s, want HTTP/2", base, resp.Proto)
			}
			if body != string(pattern(n)) {
				t.Errorf("%s%s over %s: %d bytes", base, path, resp.Proto, len(body))
			}
		}
	}
}

// TestWriteResponseOwnedGivesTheBodyBack checks that a small body sent with
// WriteResponseOwned goes back to the pool once sent, over HTTP/1 and over
// HTTP/2, whose framing copies it: a buffer the handler sent turns up again
// from the pool, at a handler's Get or at the test's own once the requests
// are done. The test keeps every buffer it sent alive, so that one turning up
// cannot be a new allocation at its address. A buffer given back may be
// held by something else on the server when the requests end, so the test's
// own Get looks for a while rather than once.
func TestWriteResponseOwnedGivesTheBodyBack(t *testing.T) {
	const size, requests = 1000, 50
	var mu sync.Mutex
	sent := map[*byte]bool{}
	reused := 0
	handler := HandlerFunc(func(c *Context) {
		body := bufferpool.Get(size)
		copy(body, pattern(size))
		mu.Lock()
		if sent[unsafe.SliceData(body)] {
			reused++
		}
		sent[unsafe.SliceData(body)] = true
		mu.Unlock()
		if err := c.WriteResponseOwned(Response{StatusCode: stdhttp.StatusOK, Body: body}); err != nil {
			t.Error(err)
		}
	})
	addr := serveHTTP1(t, handler)
	serverConfig, clientConfig, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	tlsAddr := serve(t, fibtls.NewServer(ConfigureTLS(serverConfig), NewHandler(handler)))
	// found reports whether a Get finds a buffer the handler sent, looking
	// for up to two seconds.
	found := func() bool {
		for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			var taken [][]byte
			hit := false
			mu.Lock()
			for range 4 * requests {
				b := bufferpool.Get(size)
				taken = append(taken, b)
				if hit = sent[unsafe.SliceData(b)]; hit {
					break
				}
			}
			mu.Unlock()
			for _, b := range taken {
				bufferpool.Put(b)
			}
			if hit {
				return true
			}
		}
		return false
	}
	for base, client := range map[string]*stdhttp.Client{
		"http://" + addr: {Timeout: 10 * time.Second},
		"https://" + tlsAddr: {Timeout: 10 * time.Second, Transport: &stdhttp.Transport{
			TLSClientConfig: clientConfig.Clone(), ForceAttemptHTTP2: true}},
	} {
		mu.Lock()
		clear(sent)
		reused = 0
		mu.Unlock()
		proto := ""
		for range requests {
			resp, body := do(t, client, mustRequest(t, "GET", base+"/", nil))
			proto = resp.Proto
			if body != string(pattern(size)) {
				t.Fatalf("%s: %d bytes", proto, len(body))
			}
		}
		mu.Lock()
		n := reused
		mu.Unlock()
		if n == 0 && !found() {
			t.Errorf("%s: none of the %d bodies sent went back to the pool", proto, requests)
		}
	}
}

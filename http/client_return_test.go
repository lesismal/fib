package http

import (
	"bytes"
	"io"
	stdhttp "net/http"
	"runtime"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/lesismal/fib/bufferpool"
)

// siteAllocator is the package's own pool, keeping the buffers taken at a
// call site whose function name contains site that have not come back, so
// that a test can see a path give back what it took: an allocator such as
// bufferpool.Arena gets nothing back from the collector.
type siteAllocator struct {
	site string
	mu   sync.Mutex
	out  map[*byte]bool
}

func trackSite(t *testing.T, site string) *siteAllocator {
	a := &siteAllocator{site: site, out: map[*byte]bool{}}
	bufferpool.SetAllocator(a)
	t.Cleanup(func() { bufferpool.SetAllocator(nil) })
	return a
}

func (a *siteAllocator) Get(size int) []byte {
	b := bufferpool.Builtin.Get(size)
	pcs := make([]uintptr, 8)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		f, more := frames.Next()
		if strings.Contains(f.Function, a.site) {
			a.mu.Lock()
			a.out[unsafe.SliceData(b)] = true
			a.mu.Unlock()
			break
		}
		if !more {
			break
		}
	}
	return b
}

func (a *siteAllocator) Put(b []byte) {
	if cap(b) > 0 {
		a.mu.Lock()
		delete(a.out, unsafe.SliceData(b[:1]))
		a.mu.Unlock()
	}
	bufferpool.Builtin.Put(b)
}

func (a *siteAllocator) outstanding() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.out)
}

// A response that arrived whole has its body copied into a pooled buffer,
// which Close gives back. One whose bytes BodyBytes handed out is left to
// the collector, since the caller may keep them.
func TestClientResponseBodyCloseGivesBufferBack(t *testing.T) {
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", bytes.Repeat([]byte("b"), 3000))
	})))
	client := newTestClient(t, DefaultClientConfig())
	a := trackSite(t, "responseParser).complete")
	for _, keep := range []bool{false, true} {
		type answer struct {
			resp *ClientResponse
			kept []byte
			err  error
		}
		answers := make(chan answer, 1)
		client.Do(mustRequest(t, stdhttp.MethodGet, "http://"+addr+"/", nil), func(resp *ClientResponse, err error) {
			var kept []byte
			if err == nil && keep {
				kept, _ = resp.BodyBytes()
			}
			answers <- answer{resp, kept, err}
		})
		ans := <-answers
		if ans.err != nil {
			t.Fatal(ans.err)
		}
		resp, kept := ans.resp, ans.kept
		got, _ := io.ReadAll(resp.Body)
		if len(got) != 3000 {
			t.Fatalf("read %d bytes", len(got))
		}
		_ = resp.Body.Close()
		if n := a.outstanding(); keep != (n == 1) {
			t.Fatalf("BodyBytes taken %v: %d of the body's buffers still out", keep, n)
		}
		if keep && (len(kept) != 3000 || kept[0] != 'b') {
			t.Fatal("the bytes BodyBytes handed out changed")
		}
		if n, _ := resp.Body.Read(make([]byte, 1)); n != 0 {
			t.Fatal("Read found a body after Close")
		}
	}
}

// An HTTP/2 request is framed for HTTP/1 before the connection is known,
// and goes out as HTTP/2 frames instead: the framing goes back with the
// answer, as it does over HTTP/1.
func TestClientH2RequestFramingGoesBack(t *testing.T) {
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		body, _ := io.ReadAll(c.Request.Body)
		_ = c.Respond(stdhttp.StatusOK, "text/plain", body)
	})))
	config := DefaultClientConfig()
	config.UnencryptedHTTP2 = true
	client := newTestClient(t, config)
	a := trackSite(t, "fastFramedRequest")
	for i := range 20 {
		body := bytes.Repeat([]byte("r"), 1000+i*100)
		resp, err := client.Go(mustRequest(t, stdhttp.MethodPost, "http://"+addr+"/", bytes.NewReader(body))).Wait()
		if err != nil {
			t.Fatal(err)
		}
		if resp.ProtoMajor != 2 {
			t.Fatalf("proto %s", resp.Proto)
		}
		got, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if !bytes.Equal(got, body) {
			t.Fatalf("round %d: %d bytes back", i, len(got))
		}
	}
	if n := a.outstanding(); n != 0 {
		t.Fatalf("%d request framings never went back", n)
	}
}

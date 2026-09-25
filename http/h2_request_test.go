//go:build linux || darwin || windows

package http

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	stdhttp "net/http"
	"net/textproto"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/internal/hpack"
)

// TestH2HeaderKeyTables holds the lookup tables to what they stand in for:
// strings.ToLower of a key, and textproto.CanonicalMIMEHeaderKey of a name.
func TestH2HeaderKeyTables(t *testing.T) {
	for key, name := range h2LowerKeys {
		if want := strings.ToLower(key); name != want {
			t.Errorf("h2LowerKeys[%q] = %q, want %q", key, name, want)
		}
		if want := textproto.CanonicalMIMEHeaderKey(name); key != want {
			t.Errorf("%q is not canonical: CanonicalMIMEHeaderKey makes it %q", key, want)
		}
		if got := h2CanonicalKey(name); got != key {
			t.Errorf("h2CanonicalKey(%q) = %q, want %q", name, got, key)
		}
	}
	for _, name := range []string{"x-custom", "accept-patch", "a", "x_under", "x.dot"} {
		if got, want := h2CanonicalKey(name), textproto.CanonicalMIMEHeaderKey(name); got != want {
			t.Errorf("h2CanonicalKey(%q) = %q, want %q", name, got, want)
		}
		if got := h2LowerKey(textproto.CanonicalMIMEHeaderKey(name)); got != name {
			t.Errorf("h2LowerKey(%q) = %q, want %q", textproto.CanonicalMIMEHeaderKey(name), got, name)
		}
	}
}

func testH2Fields(path string, regular ...hpack.HeaderField) []hpack.HeaderField {
	return append([]hpack.HeaderField{
		{Name: ":method", Value: "POST"}, {Name: ":scheme", Value: "http"},
		{Name: ":authority", Value: "example.com"}, {Name: ":path", Value: path},
	}, regular...)
}

// TestH2NewRequest checks the request a stream's fields become: its URL the
// one url.ParseRequestURI makes of the path, and its header's values, taken
// from the stream while they last, each its own.
func TestH2NewRequest(t *testing.T) {
	sc := &h2ServerConn{remoteAddr: "192.0.2.1:1234"}
	for _, path := range []string{"/", "/echo", "/a/b;c=d", "/q?x=1&y=2", "/q?", "/q?a?b", "/%41", "/sp%20ace",
		"/!x", "/a#frag", "*"} {
		st := &h2ServerStream{sc: sc}
		req, err := sc.newRequest(testH2Fields(path), st)
		if err != nil {
			t.Fatalf("%q: %v", path, err)
		}
		want := &url.URL{Path: "*"}
		if path != "*" {
			if want, err = url.ParseRequestURI(path); err != nil {
				t.Fatal(err)
			}
		}
		if !reflect.DeepEqual(req.URL, want) {
			t.Errorf("%q: URL %#v, want %#v", path, req.URL, want)
		}
		if req.RequestURI != path || req.Host != "example.com" || req.RemoteAddr != sc.remoteAddr {
			t.Errorf("%q: RequestURI %q, Host %q, RemoteAddr %q", path, req.RequestURI, req.Host, req.RemoteAddr)
		}
		if req != &st.block.Request || req.URL != &st.block.URL {
			t.Errorf("%q: the request is not the stream's own", path)
		}
	}

	// More values than the stream has room for, and a key that repeats.
	var regular []hpack.HeaderField
	for i := 0; i < h2RequestValues+2; i++ {
		regular = append(regular, hpack.HeaderField{Name: "x-field-" + string(rune('a'+i)), Value: "v"})
	}
	regular = append(regular, hpack.HeaderField{Name: "x-field-a", Value: "again"},
		hpack.HeaderField{Name: "content-type", Value: "text/plain"})
	st := &h2ServerStream{sc: sc}
	req, err := sc.newRequest(testH2Fields("/", regular...), st)
	if err != nil {
		t.Fatal(err)
	}
	if got := req.Header["X-Field-A"]; !reflect.DeepEqual(got, []string{"v", "again"}) {
		t.Errorf("X-Field-A = %q", got)
	}
	for i := 1; i < h2RequestValues+2; i++ {
		key := "X-Field-" + string(rune('A'+i))
		if got := req.Header[key]; !reflect.DeepEqual(got, []string{"v"}) {
			t.Errorf("%s = %q", key, got)
		}
	}
	if got := req.Header.Get("Content-Type"); got != "text/plain" {
		t.Errorf("Content-Type = %q", got)
	}
	// A handler appending to one key's values does not write over the next.
	req.Header["X-Field-B"] = append(req.Header["X-Field-B"], "appended")
	if got := req.Header["X-Field-C"]; !reflect.DeepEqual(got, []string{"v"}) {
		t.Errorf("appending to X-Field-B changed X-Field-C to %q", got)
	}
}

// h2HeaderSink moves the map the baseline makes to the heap, as the request's
// header is.
var h2HeaderSink map[string][]string

// TestH2NewRequestAllocations guards what building an ordinary request
// allocates once its stream exists: the header map and nothing else.
func TestH2NewRequestAllocations(t *testing.T) {
	sc := &h2ServerConn{remoteAddr: "192.0.2.1:1234"}
	fields := testH2Fields("/echo?x=1",
		hpack.HeaderField{Name: "content-length", Value: "1024"},
		hpack.HeaderField{Name: "content-type", Value: "application/octet-stream"},
		hpack.HeaderField{Name: "user-agent", Value: "test"},
		hpack.HeaderField{Name: "accept-encoding", Value: "gzip"})
	st := &h2ServerStream{sc: sc}
	baseline := testing.AllocsPerRun(100, func() {
		h2HeaderSink = make(map[string][]string, len(fields))
		h2HeaderSink["Content-Length"] = nil
	})
	allocs := testing.AllocsPerRun(100, func() {
		if _, err := sc.newRequest(fields, st); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > baseline {
		t.Fatalf("newRequest took %v allocations, the header map alone %v", allocs, baseline)
	}
}

// TestH2RetainedBodyOutlivesHandler checks that a request's body, whose
// buffer is the pool's, stays the request's until its last Release: read
// after the handler has returned, while other streams take buffers from the
// pool and give them back, it is still what the client sent; read after the
// Release, it reports ErrBodyReleased.
func TestH2RetainedBodyOutlivesHandler(t *testing.T) {
	released := make(chan error, 64)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
		c.Retain()
		go func() {
			time.Sleep(10 * time.Millisecond)
			body, err := io.ReadAll(r.Body)
			if err != nil {
				body = []byte(err.Error())
			}
			_ = c.Respond(stdhttp.StatusOK, "application/octet-stream", body)
			c.Release()
			_, err = r.Body.Read(make([]byte, 1))
			released <- err
		}()
	})))
	var protocols stdhttp.Protocols
	protocols.SetUnencryptedHTTP2(true)
	client := &stdhttp.Client{Timeout: 10 * time.Second, Transport: &stdhttp.Transport{Protocols: &protocols}}
	defer client.CloseIdleConnections()
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Go(func() {
			body := bytes.Repeat([]byte{byte('a' + i%26)}, 512+i*97)
			resp, err := client.Post(fmt.Sprintf("http://%s/%d", addr, i), "application/octet-stream", bytes.NewReader(body))
			if err != nil {
				t.Error(err)
				return
			}
			defer resp.Body.Close()
			got, _ := io.ReadAll(resp.Body)
			if resp.ProtoMajor != 2 || !bytes.Equal(got, body) {
				t.Errorf("request %d: %s, %d bytes back of %d", i, resp.Proto, len(got), len(body))
			}
		})
	}
	wg.Wait()
	for i := 0; i < 64; i++ {
		if err := <-released; !errors.Is(err, ErrBodyReleased) {
			t.Fatalf("a read after the last Release returned %v, want ErrBodyReleased", err)
		}
	}
}

//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	stdtls "crypto/tls"
	"crypto/x509"
	"errors"
	"math"
	stdhttp "net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"

	"github.com/lesismal/fib/hpack"
)

// TestH2ServerHonoursTheClientsHeaderListSize checks that a response whose
// header, or trailer, is larger than the client's
// SETTINGS_MAX_HEADER_LIST_SIZE is not sent: its stream is reset with
// INTERNAL_ERROR and the handler told, while the connection and the
// responses that fit carry on. An interim response that does not fit is
// left out, and the final one still goes.
func TestH2ServerHonoursTheClientsHeaderListSize(t *testing.T) {
	big := strings.Repeat("v", 600)
	errs := make(chan error, 8)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		var err error
		switch c.Request.URL.Path {
		case "/header":
			err = c.WriteResponse(Response{StatusCode: 200, Header: stdhttp.Header{"X-Big": {big}}, Body: []byte("x")})
		case "/trailer":
			err = c.WriteResponse(Response{StatusCode: 200, Body: []byte("x"), Trailer: stdhttp.Header{"X-Big": {big}}})
		case "/stream-trailer":
			c.Header().Set("Trailer", "X-Big")
			_, _ = c.Write([]byte("partial"))
			c.Flush()
			c.Header().Set("X-Big", big)
			err = c.Finish()
		case "/interim":
			if err = c.WriteInterim(stdhttp.StatusEarlyHints, stdhttp.Header{"Link": {big}}); err == nil {
				err = errors.New("no error")
			}
			errs <- err
			err = c.Respond(200, "text/plain", []byte("final"))
		default:
			err = c.Respond(200, "text/plain", []byte("ok"))
		}
		errs <- err
	})))
	tc := dialH2(t, addr, [2]uint32{uint32(h2SettingMaxHeaderListSize), 400})
	expectRefused := func(id uint32, path string) {
		t.Helper()
		tc.headers(id, true, get(path)...)
		if code := h2covReset(t, tc, id); code != H2InternalError {
			t.Fatalf("%s: stream reset with %v, want INTERNAL_ERROR", path, code)
		}
		if err := <-errs; !errors.Is(err, errH2HeaderListTooLarge) {
			t.Fatalf("%s: the handler was told %v", path, err)
		}
	}
	expectRefused(1, "/header")
	expectRefused(3, "/trailer")
	expectRefused(5, "/stream-trailer")

	tc.headers(7, true, get("/interim")...)
	if err := <-errs; !errors.Is(err, errH2HeaderListTooLarge) {
		t.Fatalf("interim: the handler was told %v", err)
	}
	if _, body := tc.response(7); string(body) != "final" {
		t.Fatalf("interim: body %q", body)
	}
	if st := tc.stream(7); len(st.interim) != 0 {
		t.Fatalf("interim responses %v sent", st.interim)
	}
	<-errs

	tc.headers(9, true, get("/")...)
	if _, body := tc.response(9); string(body) != "ok" {
		t.Fatalf("body %q", body)
	}
}

// TestH2ServerHeaderListUnboundedByDefault checks that a client that sets
// no SETTINGS_MAX_HEADER_LIST_SIZE gets a header of any size.
func TestH2ServerHeaderListUnboundedByDefault(t *testing.T) {
	big := strings.Repeat("v", 64<<10)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		_ = c.WriteResponse(Response{StatusCode: 200, Header: stdhttp.Header{"X-Big": {big}}, Body: []byte("x")})
	})))
	tc := dialH2(t, addr)
	if header, _ := tc.response1(1); header["x-big"] != big {
		t.Fatal("the large header did not arrive")
	}
}

// response1 sends a GET for / on stream id and returns its response.
func (tc *h2TestConn) response1(id uint32) (map[string]string, []byte) {
	tc.t.Helper()
	tc.headers(id, true, get("/")...)
	return tc.response(id)
}

// TestH2ClientHonoursTheServersHeaderListSize checks that the client fails
// a request whose header is larger than the server's
// SETTINGS_MAX_HEADER_LIST_SIZE rather than sending it, and goes on serving
// the requests that fit on the same connection.
func TestH2ClientHonoursTheServersHeaderListSize(t *testing.T) {
	counter := &connCounter{}
	ts := httptest.NewUnstartedServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	ts.EnableHTTP2 = true
	ts.Config.MaxHeaderBytes = 4 << 10
	ts.Config.ConnState = counter.hook
	ts.StartTLS()
	t.Cleanup(ts.Close)
	pool := x509.NewCertPool()
	pool.AddCert(ts.Certificate())
	config := DefaultClientConfig()
	config.TLSConfig = &stdtls.Config{RootCAs: pool}
	client := newTestClient(t, config)

	// The first request learns the server's settings.
	resp, err := client.Go(mustRequest(t, stdhttp.MethodGet, ts.URL+"/", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "ok" || resp.ProtoMajor != 2 {
		t.Fatalf("%s %q", resp.Proto, got)
	}
	req := mustRequest(t, stdhttp.MethodGet, ts.URL+"/big", nil)
	req.Header.Set("X-Big", strings.Repeat("v", 16<<10))
	if _, err := client.Go(req).Wait(); !errors.Is(err, errH2HeaderListTooLarge) {
		t.Fatalf("a header past the server's limit: %v", err)
	}
	resp, err = client.Go(mustRequest(t, stdhttp.MethodGet, ts.URL+"/", nil)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "ok" {
		t.Fatalf("%q", got)
	}
	if n := counter.n.Load(); n != 1 {
		t.Fatalf("%d connections, want 1", n)
	}
}

// TestH2ClientHeaderBlockBomb checks that the client keeps no more of a
// header block's fields than its MaxResponseHeaderBytes allows: a 1 MiB block
// of one-byte references to the static table decodes to a million fields,
// which it used to collect, some 40 MB of them, before looking at the list's
// size, even for a stream it never opened. The block is still decoded whole,
// which keeps the dynamic table in step: an indexed entry the block adds is
// there for the next one.
func TestH2ClientHeaderBlockBomb(t *testing.T) {
	config := DefaultClientConfig()
	config.MaxResponseHeaderBytes = 16 << 10
	// A literal with incremental indexing, then the bomb: index 2 of the
	// static table, ":method: GET", a million times.
	block := []byte{0x40, 3, 'x', '-', 'a', 1, 'b'}
	block = append(block, bytes.Repeat([]byte{0x82}, 1<<20)...)
	// TotalAlloc counts the whole process, so what the package's earlier
	// tests left running, a server winding down or a client's sidepool
	// callback, is counted too: the coverage job saw 9 MB in one try. The
	// decode's own allocation is the same every time and the rest only adds
	// to it, so the least of a few tries is the decode's.
	var hc *h2ClientConn
	least := uint64(math.MaxUint64)
	for try := 0; try < 5 && least > 1<<20; try++ {
		hc = &h2ClientConn{
			client:  &Client{config: config},
			dec:     hpack.NewDecoder(hpack.DefaultTableSize),
			streams: make(map[uint32]*h2ClientStream),
		}
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		if err := hc.handleHeaderBlock(99, block, true); err != nil {
			t.Fatal(err)
		}
		runtime.ReadMemStats(&after)
		least = min(least, after.TotalAlloc-before.TotalAlloc)
	}
	if least > 1<<20 {
		t.Fatalf("decoding the block allocated %d bytes at least", least)
	}
	// The literal went into the table: index 62 is it.
	var got []hpack.HeaderField
	if err := hc.dec.Decode([]byte{0xbe}, func(f hpack.HeaderField) error {
		got = append(got, f)
		return nil
	}); err != nil || len(got) != 1 || got[0].Name != "x-a" || got[0].Value != "b" {
		t.Fatalf("the dynamic table after the block: %v, %v", got, err)
	}
}

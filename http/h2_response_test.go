//go:build linux || darwin || windows

package http

import (
	"bytes"
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// readAcking reads until stream id has ended, opening the windows again for
// each DATA frame as it arrives, as a client reading the body does.
func (tc *h2TestConn) readAcking(id uint32) *h2TestStream {
	tc.t.Helper()
	st := tc.stream(id)
	for !st.done {
		if f := tc.read(); f.typ == h2FrameData && len(f.payload) > 0 {
			tc.write(h2AppendWindowUpdate(nil, 0, uint32(len(f.payload))))
			tc.write(h2AppendWindowUpdate(nil, f.streamID, uint32(len(f.payload))))
		}
	}
	return st
}

// A flushed HTTP/2 response reaches the client while its handler is still
// at work: HEADERS that leave the stream open and carry no content-length,
// then each piece as it is flushed, then the trailers that end it.
func TestH2ResponseStreamsAsItIsWritten(t *testing.T) {
	next := make(chan struct{})
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		c.Header().Set("Trailer", "X-Sum")
		_, _ = c.WriteString("first")
		c.Flush()
		<-next
		// Past what is held back, so it goes out without a Flush.
		_, _ = c.WriteString(strings.Repeat("s", 10000))
		<-next
		_, _ = c.WriteString("last")
		c.Header().Set("X-Sum", "10009")
		c.Header().Set(stdhttp.TrailerPrefix+"X-Late", "late")
	})))
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/")...)
	st := tc.stream(1)
	head := tc.readUntil(h2FrameHeaders)
	if head.has(h2FlagEndStream) || head.fields[":status"] != "200" || head.fields["content-length"] != "" {
		t.Fatalf("header: flags %x, %v", head.flags, head.fields)
	}
	for string(st.body) != "first" {
		if f := tc.read(); f.typ != h2FrameData || f.has(h2FlagEndStream) {
			t.Fatalf("waiting for the first piece: %v, %q", f.typ, st.body)
		}
	}
	next <- struct{}{}
	for len(st.body) < 5+10000 {
		if f := tc.read(); f.typ != h2FrameData || f.has(h2FlagEndStream) {
			t.Fatalf("waiting for the second piece: %v, %d bytes", f.typ, len(st.body))
		}
	}
	next <- struct{}{}
	var trailer h2TestFrame
	for {
		f := tc.read()
		if f.typ == h2FrameHeaders {
			trailer = f
			break
		}
		if f.typ != h2FrameData || f.has(h2FlagEndStream) {
			t.Fatalf("waiting for the trailer: %v, flags %x", f.typ, f.flags)
		}
	}
	if !trailer.has(h2FlagEndStream) || trailer.fields["x-sum"] != "10009" || trailer.fields["x-late"] != "late" {
		t.Fatalf("trailer: flags %x, %v", trailer.flags, trailer.fields)
	}
	if want := "first" + strings.Repeat("s", 10000) + "last"; string(st.body) != want {
		t.Fatalf("body: %d bytes, want %d", len(st.body), len(want))
	}
}

// A response that is neither flushed nor long is still sent whole, with its
// content-length, and so is a declared one up to maxHeldBody; a longer
// declared one streams with its content-length, and one that falls short of
// it is reset rather than ended.
func TestH2ResponseHeldOrDeclared(t *testing.T) {
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		switch c.Request.URL.Path {
		case "/short":
			_, _ = c.WriteString("short")
		case "/declared":
			c.Header().Set("Content-Length", "100000")
			for range 10 {
				_, _ = c.Write(bytes.Repeat([]byte("d"), 10000))
			}
		case "/truncated":
			c.Header().Set("Content-Length", "100000")
			_, _ = c.Write(bytes.Repeat([]byte("t"), 10000))
		case "/empty":
			c.Flush()
		}
	})))
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/short")...)
	header, body := tc.response(1)
	if string(body) != "short" || header["content-length"] != "5" {
		t.Fatalf("short: %q, %v", body, header)
	}
	tc.headers(3, true, get("/declared")...)
	if st := tc.readAcking(3); len(st.body) != 100000 || st.header["content-length"] != "100000" || st.resetSet {
		t.Fatalf("declared: %d bytes, %v, reset %v", len(st.body), st.header, st.reset)
	}
	tc.headers(5, true, get("/truncated")...)
	st := tc.stream(5)
	for !st.done {
		tc.read()
	}
	if !st.resetSet || st.reset != H2InternalError || st.header["content-length"] != "100000" {
		t.Fatalf("truncated: reset %v %v, header %v", st.resetSet, st.reset, st.header)
	}
	tc.headers(7, true, get("/empty")...)
	header, body = tc.response(7)
	if len(body) != 0 || header[":status"] != "200" {
		t.Fatalf("empty: %q, %v", body, header)
	}
}

// A handler that writes faster than the client opens its window waits for
// it, rather than having the server hold its whole body; it is let go as the
// client reads.
func TestH2ResponseWaitsForTheClientsWindow(t *testing.T) {
	const total = 1 << 20
	var written atomic.Int64
	done := make(chan error, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		chunk := bytes.Repeat([]byte("w"), 16<<10)
		for written.Load() < total {
			if _, err := c.Write(chunk); err != nil {
				done <- err
				return
			}
			written.Add(int64(len(chunk)))
		}
		done <- nil
	})))
	tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 1000})
	tc.headers(1, true, get("/")...)
	tc.readUntil(h2FrameHeaders)
	st := tc.stream(1)
	for len(st.body) < 1000 {
		tc.read()
	}
	time.Sleep(100 * time.Millisecond)
	// What the window let go, what the stream holds, and the chunk the
	// handler is waiting to write.
	if n := written.Load(); n > 1000+h2MaxPendingOutput+16<<10 {
		t.Fatalf("the handler wrote %d bytes into a window of 1000", n)
	}
	// The client takes the rest, opening the window as it reads.
	tc.write(h2AppendWindowUpdate(nil, 1, 1000))
	tc.readAcking(1)
	if err := <-done; err != nil || len(st.body) != total || st.resetSet {
		t.Fatalf("handler %v, body %d bytes, reset %v", err, len(st.body), st.reset)
	}
}

// A handler waiting for the window is let go when the client resets the
// stream, its Write failing, and so is one whose connection closes. One that
// has begun its response and is waiting on something else hears of it
// through OnCancel, as one that has not begun does.
func TestH2ResponseWaitEndsWithTheStream(t *testing.T) {
	results := make(chan error, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		if c.Request.URL.Path == "/idle" {
			cancelled := make(chan error, 1)
			c.OnCancel(func(err error) { cancelled <- err })
			c.Flush()
			select {
			case err := <-cancelled:
				results <- err
			case <-time.After(5 * time.Second):
				results <- nil
			}
			return
		}
		chunk := make([]byte, 16<<10)
		for {
			if _, err := c.Write(chunk); err != nil {
				results <- err
				return
			}
		}
	})))
	for _, path := range []string{"/write", "/idle"} {
		for _, closeConn := range []bool{false, true} {
			tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 0})
			tc.headers(1, true, get(path)...)
			tc.readUntil(h2FrameHeaders)
			select {
			case err := <-results:
				t.Fatalf("%s: the handler did not wait: %v", path, err)
			case <-time.After(50 * time.Millisecond):
			}
			if closeConn {
				tc.c.Close()
			} else {
				tc.write(h2AppendRSTStream(nil, 1, H2Cancel))
			}
			if err := <-results; err == nil {
				t.Fatalf("%s, close %v: the handler was not let go", path, closeConn)
			}
		}
	}
}

// A handler the connection runs on its own reader cannot wait for the
// window, which only that reader would open: what it writes past the window
// is held, and goes once the handler returns and the client's updates are
// read.
func TestH2ResponseOnTheReaderDoesNotWait(t *testing.T) {
	config := DefaultConfig()
	config.StreamPool = StreamPoolConfig{Disable: true}
	const total = 300 << 10
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		chunk := bytes.Repeat([]byte("r"), 10<<10)
		for range total / len(chunk) {
			_, _ = c.Write(chunk)
		}
	})))
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/")...)
	if st := tc.readAcking(1); len(st.body) != total || st.resetSet {
		t.Fatalf("body %d bytes, reset %v", len(st.body), st.reset)
	}
}

// Many streaming responses at once on recycled streams, through Go's own
// client, each with its own body and trailer.
func TestH2ResponseStreamsConcurrently(t *testing.T) {
	serverConfig, clientConfig, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	addr := serve(t, fibtls.NewServer(ConfigureTLS(serverConfig), NewHandler(HandlerFunc(func(c *Context) {
		n := c.Request.URL.Query().Get("n")
		c.Header().Set("Trailer", "X-N")
		for i := range 20 {
			_, _ = fmt.Fprintf(c, "%s:%d:%s|", n, i, strings.Repeat("p", 3000))
			if i%3 == 0 {
				c.Flush()
			}
		}
		c.Header().Set("X-N", n)
	}))))
	client := &stdhttp.Client{Timeout: 20 * time.Second,
		Transport: &stdhttp.Transport{TLSClientConfig: clientConfig, ForceAttemptHTTP2: true}}
	defer client.CloseIdleConnections()
	var wg sync.WaitGroup
	for n := range 64 {
		wg.Go(func() {
			resp, err := client.Get(fmt.Sprintf("https://%s/?n=%d", addr, n))
			if err != nil {
				t.Error(err)
				return
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			var want strings.Builder
			for i := range 20 {
				fmt.Fprintf(&want, "%d:%d:%s|", n, i, strings.Repeat("p", 3000))
			}
			if err != nil || resp.ProtoMajor != 2 || string(body) != want.String() || resp.Trailer.Get("X-N") != fmt.Sprint(n) {
				t.Errorf("n=%d: %v, %s, %d bytes, trailer %v", n, err, resp.Proto, len(body), resp.Trailer)
			}
		})
	}
	wg.Wait()
}

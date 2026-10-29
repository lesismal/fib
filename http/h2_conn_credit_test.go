//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"encoding/binary"
	"io"
	stdhttp "net/http"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestH2StreamedBodyHoldsTheConnectionWindow checks that the data of a body
// streaming to its handler holds the connection's window, not only its
// stream's, until the handler consumes it, and that a stream that ends with
// its body unconsumed gives its share back: a client cannot pile up more of
// the connection's bodies than its window, and a reset one does not shrink
// the window for good.
func TestH2StreamedBodyHoldsTheConnectionWindow(t *testing.T) {
	const (
		streamWindow = 64 << 10
		connWindow   = 2 * streamWindow
		declared     = 100000
	)
	config := DefaultConfig()
	config.StreamRequestBody = true
	config.HTTP2.StreamWindow = streamWindow
	config.HTTP2.ConnWindow = connWindow
	gates := map[string]chan struct{}{}
	for _, path := range []string{"/1", "/3", "/5", "/7"} {
		gates[path] = make(chan struct{})
	}
	var closeOnce sync.Map
	open := func(path string) {
		if _, done := closeOnce.LoadOrStore(path, true); !done {
			close(gates[path])
		}
	}
	t.Cleanup(func() {
		for path := range gates {
			open(path)
		}
	})
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		gate := gates[c.Request.URL.Path]
		c.Retain()
		go func() {
			defer c.Release()
			<-gate
			total := 0
			c.OnBody(func(data []byte, fin bool, err error) {
				total += len(data)
				if fin || err != nil {
					_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(strconv.Itoa(total)))
				}
			})
		}()
	})))
	tc := dialH2(t, addr)
	// remaining is what the client may still send on the connection, as it
	// sees it; take counts what a frame gives back, and ping returns what
	// the server gave back before answering a PING.
	remaining := h2DefaultWindow
	take := func(f h2TestFrame) int {
		if f.typ != h2FrameWindowUpdate || f.streamID != 0 {
			return 0
		}
		n := int(binary.BigEndian.Uint32(f.payload) & 0x7fffffff)
		remaining += n
		return n
	}
	ping := func() (credit int) {
		t.Helper()
		tc.write(append(h2AppendFrameHeader(nil, h2FramePing, 0, 0, 8), make([]byte, 8)...))
		for {
			f := tc.read()
			credit += take(f)
			if f.typ == h2FramePing && f.has(h2FlagAck) {
				return credit
			}
		}
	}
	upload := func(id uint32, path string, length, n int) {
		tc.headers(id, false, ":method", "POST", ":scheme", "http", ":authority", "test", ":path", path,
			"content-length", strconv.Itoa(length))
		for left := n; left > 0; left -= h2DefaultMaxFrameSize {
			chunk := min(left, h2DefaultMaxFrameSize)
			tc.data(id, n == length && left == chunk, make([]byte, chunk))
		}
		remaining -= n
	}
	if ping(); remaining != connWindow {
		t.Fatalf("the preface opened the connection's window to %d, want %d", remaining, connWindow)
	}
	// Two streams take the whole connection window, which their bodies
	// hold while no handler consumes them.
	upload(1, "/1", streamWindow, streamWindow)
	upload(3, "/3", declared, streamWindow)
	if got := ping(); got != 0 {
		t.Fatalf("%d of the connection's window given back for bodies no handler has consumed", got)
	}
	// Reset, stream 3 gives back what its body held, though its handler
	// never consumes it.
	tc.write(h2AppendRSTStream(nil, 3, H2Cancel))
	if got := ping(); got != streamWindow {
		t.Fatalf("reset stream 3 gave back %d of the connection's window, want %d", got, streamWindow)
	}
	// Consumed, stream 1's body gives its share back too.
	open("/1")
	for st := tc.stream(1); !st.done; {
		take(tc.read())
	}
	if _, body := tc.response(1); string(body) != strconv.Itoa(streamWindow) {
		t.Fatalf("stream 1 answered %q, want %d", body, streamWindow)
	}
	// Whatever the client has left, two more held bodies take it all, which
	// has the server give back everything not held.
	held := 0
	for _, id := range []uint32{5, 7} {
		n := min(remaining, streamWindow)
		upload(id, "/"+strconv.Itoa(int(id)), declared, n)
		held += n
	}
	for remaining < connWindow-held {
		take(tc.read())
	}
	if ping(); remaining != connWindow-held {
		t.Fatalf("the client may send %d with %d of the bodies held, want %d", remaining, held, connWindow-held)
	}
}

// TestH2StreamedBodiesShareTheConnectionWindow uploads several bodies at once
// over one connection to handlers that are slow to take them: no more of
// them all arrives before they do than the connection's window, though each
// stream's window would let more, and every upload then completes. net/http's
// client is the peer, since it keeps to the windows it is given.
func TestH2StreamedBodiesShareTheConnectionWindow(t *testing.T) {
	const (
		window  = 1 << 20
		uploads = 4
		size    = 3 << 20
	)
	config := DefaultConfig()
	config.StreamRequestBody = true
	config.HTTP2.StreamWindow = window
	config.HTTP2.ConnWindow = window
	var (
		arrived  sync.WaitGroup
		recorded sync.WaitGroup
		mu       sync.Mutex
		before   int64
	)
	arrived.Add(uploads)
	recorded.Add(uploads)
	addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
		if c.Request.Method == stdhttp.MethodGet {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", nil)
			return
		}
		c.Retain()
		arrived.Done()
		go func() {
			defer c.Release()
			arrived.Wait()
			// Long enough for a client that ignored the window to send
			// everything it could.
			time.Sleep(300 * time.Millisecond)
			mu.Lock()
			before += c.RequestBody().Consumed()
			mu.Unlock()
			recorded.Done()
			recorded.Wait()
			total := 0
			c.OnBody(func(data []byte, fin bool, err error) {
				total += len(data)
				if fin || err != nil {
					_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(strconv.Itoa(total)))
				}
			})
		}()
	})))
	client := netHTTPClient(t, false)
	// One connection carries all the uploads.
	if resp, err := client.Get("http://" + addr + "/warm"); err != nil {
		t.Fatal(err)
	} else {
		resp.Body.Close()
	}
	errs := make(chan error, uploads)
	for range uploads {
		go func() {
			resp, err := client.Post("http://"+addr+"/up", "application/octet-stream", bytes.NewReader(make([]byte, size)))
			if err != nil {
				errs <- err
				return
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.ProtoMajor != 2 || string(body) != strconv.Itoa(size) {
				errs <- &stdhttp.ProtocolError{ErrorString: resp.Proto + " answered " + string(body)}
				return
			}
			errs <- nil
		}()
	}
	for range uploads {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if before > window || before == 0 {
		t.Fatalf("%d bytes of %d uploads arrived before their handlers took any; want some, and no more than the %d byte connection window",
			before, uploads, window)
	}
}

//go:build linux || darwin || windows

package http

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/taskpool"
)

// streamingConfig is a server that hands over any body past 1KB.
func streamingConfig() Config {
	config := DefaultConfig()
	config.StreamRequestBodyThreshold = 1 << 10
	return config
}

// serveStreaming runs a server with config and returns its host:port.
func serveStreamingServer(t *testing.T, config Config, handler HandlerFunc) string {
	t.Helper()
	return serve(t, NewHandlerWithConfig(config, handler))
}

// serveOnPinnedPool runs a server whose rounds go to a pool of a fixed size,
// for a test that counts goroutines: the built-in pool grows with how many
// connections happen to be busy at once, which is not a cost of the requests
// themselves and differs from one machine to the next. ModeCond starts all of
// its workers with the pool, so they are already there when the count is
// taken and none is added later.
func serveOnPinnedPool(t *testing.T, config Config, handler HandlerFunc) string {
	t.Helper()
	pool := taskpool.NewWithMode("test", taskpool.ModeCond, 8, 1024)
	engine := fib.DefaultConfig()
	engine.Addr = "127.0.0.1:0"
	// Without pollers every round runs on the pinned pool; with them HTTP/1
	// would build a pool of workers of its own.
	engine.IOPollers = false
	engine.SetTaskPool(pool)
	server, err := fib.Bind(engine, NewHandlerWithConfig(config, handler))
	if err != nil {
		t.Fatal(err)
	}
	addr, err := server.LocalAddr()
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run() }()
	t.Cleanup(func() {
		server.Stop()
		<-runDone
		_ = server.Close()
		pool.Stop()
	})
	return addr.String()
}

// bodyOf is the body a streamed request carries, or nil when the request was
// buffered whole instead.
func bodyOf(r *stdhttp.Request) *BodyStream {
	stream, _ := r.Body.(*BodyStream)
	return stream
}

// collectBody is the shape a handler takes under a body that never waits: it
// reads what has arrived, and when Read reports ErrWouldBlock it retains the
// request and takes the rest through OnBody. answer runs once, with what the
// body came to and the error that ended it, if any.
func collectBody(answer func(c *Context, total int64, digest []byte, err error)) HandlerFunc {
	return func(c *Context, r *stdhttp.Request) {
		digest := sha256.New()
		var total int64
		buf := make([]byte, 4096)
		for {
			n, err := r.Body.Read(buf)
			if n > 0 {
				digest.Write(buf[:n])
				total += int64(n)
			}
			switch {
			case err == nil:
				continue
			case errors.Is(err, io.EOF):
				answer(c, total, digest.Sum(nil), nil)
				return
			case errors.Is(err, ErrWouldBlock):
				c.Retain()
				c.OnBody(func(data []byte, fin bool, err error) {
					digest.Write(data)
					total += int64(len(data))
					if err != nil || fin {
						answer(c, total, digest.Sum(nil), err)
						c.Release()
					}
				})
				return
			default:
				answer(c, total, digest.Sum(nil), err)
				return
			}
		}
	}
}

// sizeAndDigest answers with what collectBody gathered.
func sizeAndDigest(c *Context, total int64, digest []byte, err error) {
	if err != nil {
		_ = c.Respond(stdhttp.StatusBadRequest, "text/plain", []byte(err.Error()))
		return
	}
	_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%d %x", total, digest))
}

func wantSizeAndDigest(payload []byte) string {
	return fmt.Sprintf("%d %x", len(payload), sha256.Sum256(payload))
}

// TestStreamRequestBodyReachesHandlerBeforeTheBodyEnds also checks the
// handover: what the handler read before ErrWouldBlock and what OnBody
// delivered after it have to add up to the whole body, with nothing dropped
// between the two.
func TestStreamRequestBodyReachesHandlerBeforeTheBodyEnds(t *testing.T) {
	const size = 1 << 20
	started := make(chan int64, 1)
	addr := serveStreamingServer(t, streamingConfig(), func(c *Context, r *stdhttp.Request) {
		stream := bodyOf(r)
		if stream == nil {
			t.Errorf("body is %T, want a *BodyStream", r.Body)
			_ = c.Respond(stdhttp.StatusInternalServerError, "text/plain", nil)
			return
		}
		started <- stream.Consumed()
		collectBody(sizeAndDigest)(c, r)
	})

	payload := bytes.Repeat([]byte("fib streaming body "), size/19)
	conn := dialRaw(t, addr)
	conn.send(fmt.Sprintf("POST /upload HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", len(payload)))

	// The handler has to run before the body is sent, not after.
	var consumed int64
	select {
	case consumed = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler did not run before the body arrived")
	}
	if consumed > int64(len(payload)) {
		t.Fatalf("the handler started with %d bytes of a %d byte body", consumed, len(payload))
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	if _, body := conn.response(stdhttp.MethodPost); body != wantSizeAndDigest(payload) {
		t.Fatalf("body = %q, want %q", body, wantSizeAndDigest(payload))
	}
}

// TestStreamRequestBodyReadReportsWouldBlockAndEOF checks the answers a
// handler steers by: nothing here yet, and the body already taken over.
func TestStreamRequestBodyReadReportsWouldBlock(t *testing.T) {
	errs := make(chan error, 2)
	addr := serveStreamingServer(t, streamingConfig(), func(c *Context, r *stdhttp.Request) {
		buf := make([]byte, 64<<10)
		_, err := r.Body.Read(buf)
		errs <- err
		c.Retain()
		c.OnBody(func(data []byte, fin bool, err error) {
			if !fin && err == nil {
				return
			}
			// Once a callback has the body, reading it reports as much.
			_, readErr := r.Body.Read(buf)
			errs <- readErr
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("done"))
			c.Release()
		})
	})
	conn := dialRaw(t, addr)
	conn.send("POST /x HTTP/1.1\r\nHost: test\r\nContent-Length: 4096\r\n\r\n")
	// The body goes out only once the handler has read, so that the read
	// finds nothing there however long the handler takes to run.
	select {
	case err := <-errs:
		if !errors.Is(err, ErrWouldBlock) {
			t.Fatalf("first read = %v, want ErrWouldBlock", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the handler never ran")
	}
	if _, err := conn.Write(bytes.Repeat([]byte("q"), 4096)); err != nil {
		t.Fatal(err)
	}
	if _, body := conn.response(stdhttp.MethodPost); body != "done" {
		t.Fatalf("body = %q", body)
	}
	if err := <-errs; !errors.Is(err, ErrBodyAbandoned) {
		t.Fatalf("read after OnBody took the body = %v, want ErrBodyAbandoned", err)
	}
}

// TestStreamRequestBodyReadsToEOFWithoutOnBody covers the body that is all
// there by the time the handler runs: it reads to io.EOF and answers, with no
// callback and nothing retained.
func TestStreamRequestBodyReadsToEOFWithoutOnBody(t *testing.T) {
	addr := serveStreamingServer(t, streamingConfig(), func(c *Context, r *stdhttp.Request) {
		digest := sha256.New()
		var total int64
		buf := make([]byte, 4096)
		for {
			n, err := r.Body.Read(buf)
			digest.Write(buf[:n])
			total += int64(n)
			if errors.Is(err, io.EOF) {
				sizeAndDigest(c, total, digest.Sum(nil), nil)
				return
			}
			if err != nil {
				t.Errorf("read = %v, want the whole body to be here", err)
				collectBody(sizeAndDigest)(c, r)
				return
			}
		}
	})
	payload := bytes.Repeat([]byte("p"), 8<<10)
	conn := dialRaw(t, addr)
	// Header and body in one write, so the body is there when the handler is.
	conn.send(fmt.Sprintf("POST /x HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n%s", len(payload), payload))
	if _, body := conn.response(stdhttp.MethodPost); body != wantSizeAndDigest(payload) {
		t.Fatalf("body = %q, want %q", body, wantSizeAndDigest(payload))
	}
}
func TestStreamRequestBodyKeepsBodiesUnderTheThresholdBuffered(t *testing.T) {
	kinds := make(chan string, 1)
	addr := serveStreamingServer(t, streamingConfig(), func(c *Context, r *stdhttp.Request) {
		if bodyOf(r) != nil {
			kinds <- "stream"
		} else {
			kinds <- "buffered"
		}
		body, _ := io.ReadAll(r.Body)
		_ = c.Respond(stdhttp.StatusOK, "text/plain", body)
	})
	conn := dialRaw(t, addr)
	conn.send("POST /small HTTP/1.1\r\nHost: test\r\nContent-Length: 5\r\n\r\nhello")
	if _, body := conn.response(stdhttp.MethodPost); body != "hello" {
		t.Fatalf("body = %q, want %q", body, "hello")
	}
	if kind := <-kinds; kind != "buffered" {
		t.Fatalf("a 5 byte body was %s, want buffered", kind)
	}
}

func TestStreamRequestBodyChunkedWithTrailer(t *testing.T) {
	addr := serveStreamingServer(t, streamingConfig(), func(c *Context, r *stdhttp.Request) {
		if bodyOf(r) == nil {
			t.Errorf("body is %T, want a *BodyStream", r.Body)
		}
		collectBody(func(c *Context, total int64, _ []byte, err error) {
			if err != nil {
				t.Errorf("reading the body: %v", err)
			}
			_ = c.Respond(stdhttp.StatusOK, "text/plain",
				fmt.Appendf(nil, "%d %s", total, r.Trailer.Get("X-Checksum")))
		})(c, r)
	})

	conn := dialRaw(t, addr)
	conn.send("POST /chunked HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\nTrailer: X-Checksum\r\n\r\n")
	total := 0
	// Enough chunks to cross the threshold, sent one write at a time so the
	// decoder has to carry its state across reads.
	for i := 0; i < 64; i++ {
		chunk := bytes.Repeat([]byte{byte('a' + i%26)}, 64)
		total += len(chunk)
		conn.send(fmt.Sprintf("%x;name=value\r\n%s\r\n", len(chunk), chunk))
		time.Sleep(time.Millisecond)
	}
	conn.send("0\r\nX-Checksum: abc123\r\n\r\n")
	if _, body := conn.response(stdhttp.MethodPost); body != fmt.Sprintf("%d abc123", total) {
		t.Fatalf("body = %q, want %q", body, fmt.Sprintf("%d abc123", total))
	}
}

// TestStreamRequestBodyHoldsReadsWhileTheHandlerIsBehind checks the
// backpressure a handler that retains the body without taking it still gets:
// what has arrived is buffered up to the watermark, and past it the
// connection stops reading.
func TestStreamRequestBodyHoldsReadsWhileTheHandlerIsBehind(t *testing.T) {
	const size = 1 << 20
	config := streamingConfig()
	config.StreamRequestBodyBuffer = 16 << 10
	held := make(chan bool, 1)
	addr := serveStreamingServer(t, config, func(c *Context, r *stdhttp.Request) {
		c.Retain()
		go func() {
			// Let the buffer fill before reading a byte.
			deadline := time.Now().Add(5 * time.Second)
			for !c.Conn.ReadsHeld() && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			held <- c.Conn.ReadsHeld()
			var total int64
			buf := make([]byte, 4096)
			for {
				n, err := r.Body.Read(buf)
				total += int64(n)
				if errors.Is(err, io.EOF) {
					break
				}
				if errors.Is(err, ErrWouldBlock) {
					time.Sleep(time.Millisecond)
					continue
				}
				if err != nil {
					t.Errorf("reading the body: %v", err)
					break
				}
			}
			if c.Conn.ReadsHeld() {
				t.Error("reads still held once the body had been read")
			}
			_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%d", total))
			c.Release()
		}()
	})

	payload := bytes.Repeat([]byte("x"), size)
	conn := dialRaw(t, addr)
	conn.send(fmt.Sprintf("POST /slow HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", len(payload)))
	writeDone := make(chan error, 1)
	go func() { _, err := conn.Write(payload); writeDone <- err }()
	if !<-held {
		t.Fatal("the connection never held its reads while the body piled up")
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if _, body := conn.response(stdhttp.MethodPost); body != fmt.Sprintf("%d", size) {
		t.Fatalf("body = %q, want %d", body, size)
	}
}
func TestStreamRequestBodyKeepsAliveAfterAShortDiscard(t *testing.T) {
	addr := serveStreamingServer(t, streamingConfig(), func(c *Context, r *stdhttp.Request) {
		// Answer without reading the body at all.
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(r.URL.Path))
	})
	conn := dialRaw(t, addr)
	payload := bytes.Repeat([]byte("y"), 8<<10)
	conn.send(fmt.Sprintf("POST /first HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", len(payload)))
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	if _, body := conn.response(stdhttp.MethodPost); body != "/first" {
		t.Fatalf("body = %q, want %q", body, "/first")
	}
	// The unread body was discarded, so the connection still carries requests.
	conn.send("GET /second HTTP/1.1\r\nHost: test\r\n\r\n")
	if _, body := conn.response(stdhttp.MethodGet); body != "/second" {
		t.Fatalf("body = %q, want %q", body, "/second")
	}
}

func TestStreamRequestBodyClosesAfterALongDiscard(t *testing.T) {
	addr := serveStreamingServer(t, streamingConfig(), func(c *Context, r *stdhttp.Request) {
		_ = c.Respond(stdhttp.StatusRequestEntityTooLarge, "text/plain", []byte("too large"))
	})
	conn := dialRaw(t, addr)
	conn.send(fmt.Sprintf("POST /huge HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", 64<<20))
	if _, err := conn.Write(bytes.Repeat([]byte("z"), 32<<10)); err != nil {
		t.Fatal(err)
	}
	resp, body := conn.response(stdhttp.MethodPost)
	if resp.StatusCode != stdhttp.StatusRequestEntityTooLarge || body != "too large" {
		t.Fatalf("response = %d %q", resp.StatusCode, body)
	}
	if !conn.closed() {
		t.Fatal("the connection stayed open with a 64MB body still to come")
	}
}

func TestStreamRequestBodyExpectContinueWaitsForTheFirstRead(t *testing.T) {
	read := make(chan struct{})
	addr := serveStreamingServer(t, streamingConfig(), func(c *Context, r *stdhttp.Request) {
		c.Retain()
		go func() {
			<-read
			collectBody(func(c *Context, total int64, _ []byte, err error) {
				if err != nil {
					t.Errorf("reading the body: %v", err)
				}
				_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%d", total))
			})(c, r)
			c.Release()
		}()
	})
	payload := bytes.Repeat([]byte("q"), 4<<10)
	conn := dialRaw(t, addr)
	conn.send(fmt.Sprintf("POST /expect HTTP/1.1\r\nHost: test\r\nExpect: 100-continue\r\nContent-Length: %d\r\n\r\n", len(payload)))

	// Nothing comes back until the handler asks for the body.
	_ = conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
	if _, err := conn.r.Peek(1); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("peek before the first read = %v, want a timeout", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	close(read)
	if head := conn.head(); !strings.HasPrefix(head, "HTTP/1.1 100 Continue") {
		t.Fatalf("interim response = %q", head)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	if _, body := conn.response(stdhttp.MethodPost); body != fmt.Sprintf("%d", len(payload)) {
		t.Fatalf("body = %q, want %d", body, len(payload))
	}
}

// TestStreamRequestBodyExpectContinueGrantedByOnBody checks that asking for
// the body as a callback is asking for it too.
func TestStreamRequestBodyExpectContinueGrantedByOnBody(t *testing.T) {
	addr := serveStreamingServer(t, streamingConfig(), func(c *Context, r *stdhttp.Request) {
		var total int64
		c.Retain()
		c.OnBody(func(data []byte, fin bool, err error) {
			total += int64(len(data))
			if err != nil || fin {
				_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%d", total))
				c.Release()
			}
		})
	})
	payload := bytes.Repeat([]byte("e"), 4<<10)
	conn := dialRaw(t, addr)
	conn.send(fmt.Sprintf("POST /expect HTTP/1.1\r\nHost: test\r\nExpect: 100-continue\r\nContent-Length: %d\r\n\r\n", len(payload)))
	if head := conn.head(); !strings.HasPrefix(head, "HTTP/1.1 100 Continue") {
		t.Fatalf("interim response = %q", head)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	if _, body := conn.response(stdhttp.MethodPost); body != fmt.Sprintf("%d", len(payload)) {
		t.Fatalf("body = %q, want %d", body, len(payload))
	}
}
func TestStreamRequestBodyRefusedExpectationSendsNoContinue(t *testing.T) {
	addr := serveStreamingServer(t, streamingConfig(), func(c *Context, r *stdhttp.Request) {
		_ = c.Respond(stdhttp.StatusForbidden, "text/plain", []byte("no"))
	})
	conn := dialRaw(t, addr)
	conn.send(fmt.Sprintf("POST /expect HTTP/1.1\r\nHost: test\r\nExpect: 100-continue\r\nContent-Length: %d\r\n\r\n", 4<<10))
	resp, body := conn.response(stdhttp.MethodPost)
	if resp.StatusCode != stdhttp.StatusForbidden || body != "no" {
		t.Fatalf("response = %d %q", resp.StatusCode, body)
	}
	if !conn.closed() {
		t.Fatal("the connection stayed open after refusing a body the client still owes")
	}
}

func TestStreamRequestBodyLimitFailsTheRead(t *testing.T) {
	config := streamingConfig()
	config.MaxStreamedBodyBytes = 64 << 10
	failed := make(chan error, 1)
	addr := serveStreamingServer(t, config, collectBody(func(c *Context, _ int64, _ []byte, err error) {
		failed <- err
	}))
	conn := dialRaw(t, addr)
	conn.send("POST /big HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\n\r\n")
	chunk := bytes.Repeat([]byte("w"), 8<<10)
	for i := 0; i < 32; i++ {
		if _, err := fmt.Fprintf(conn, "%x\r\n%s\r\n", len(chunk), chunk); err != nil {
			break
		}
	}
	select {
	case err := <-failed:
		if !errors.Is(err, ErrBodyTooLarge) {
			t.Fatalf("read error = %v, want ErrBodyTooLarge", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the body grew past MaxStreamedBodyBytes without failing")
	}
}
func TestStreamRequestBodyTruncatedUploadFailsTheRead(t *testing.T) {
	failed := make(chan error, 1)
	addr := serveStreamingServer(t, streamingConfig(), collectBody(func(c *Context, _ int64, _ []byte, err error) {
		failed <- err
	}))
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.WriteString(conn, "POST /cut HTTP/1.1\r\nHost: test\r\nContent-Length: 1048576\r\n\r\npartial"); err != nil {
		t.Fatal(err)
	}
	conn.Close()
	select {
	case err := <-failed:
		if err == nil {
			t.Fatal("a truncated upload read as a complete one")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the read never failed after the client went away")
	}
}
func TestStreamRequestBodyPipelinesBehindTheStreamedRequest(t *testing.T) {
	var order atomic.Int64
	addr := serveStreamingServer(t, streamingConfig(), func(c *Context, r *stdhttp.Request) {
		path := r.URL.Path
		collectBody(func(c *Context, total int64, _ []byte, err error) {
			if err != nil {
				t.Errorf("reading the body: %v", err)
			}
			_ = c.Respond(stdhttp.StatusOK, "text/plain",
				fmt.Appendf(nil, "%d %s %d", order.Add(1), path, total))
		})(c, r)
	})
	payload := bytes.Repeat([]byte("p"), 4<<10)
	conn := dialRaw(t, addr)
	conn.send(fmt.Sprintf("POST /first HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", len(payload)))
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	// The second request arrives in the same breath as the end of the first
	// body, so it may only be served once the first handler has answered.
	conn.send("GET /second HTTP/1.1\r\nHost: test\r\n\r\n")
	for _, want := range []string{
		fmt.Sprintf("1 /first %d", len(payload)),
		"2 /second 0",
	} {
		if _, body := conn.response(stdhttp.MethodPost); body != want {
			t.Fatalf("body = %q, want %q", body, want)
		}
	}
}

// TestStreamRequestBodyAcceptsMoreThanMaxBodyBytes is the point of the whole
// thing: an upload larger than the server would ever hold in memory, taken a
// buffer at a time.
func TestStreamRequestBodyAcceptsMoreThanMaxBodyBytes(t *testing.T) {
	config := streamingConfig()
	config.MaxBodyBytes = 1 << 20
	config.StreamRequestBodyBuffer = 64 << 10
	const size = 32 << 20
	addr := serveStreamingServer(t, config, collectBody(func(c *Context, total int64, _ []byte, err error) {
		if err != nil {
			t.Errorf("reading the body: %v", err)
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%d", total))
	}))
	conn := dialRaw(t, addr)
	conn.send(fmt.Sprintf("POST /huge HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", size))
	chunk := bytes.Repeat([]byte("m"), 64<<10)
	for sent := 0; sent < size; sent += len(chunk) {
		if _, err := conn.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if _, body := conn.response(stdhttp.MethodPost); body != fmt.Sprintf("%d", size) {
		t.Fatalf("body = %q, want %d", body, size)
	}
}
func TestStreamRequestBodyHandlerPanicEndsTheConnection(t *testing.T) {
	addr := serveStreamingServer(t, streamingConfig(), func(c *Context, r *stdhttp.Request) {
		panic("handler gave up")
	})
	conn := dialRaw(t, addr)
	conn.send(fmt.Sprintf("POST /panic HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", 8<<10))
	if _, err := conn.Write(bytes.Repeat([]byte("k"), 1<<10)); err != nil {
		t.Fatal(err)
	}
	if !conn.closed() {
		t.Fatal("the connection stayed open after the handler panicked")
	}
}

func TestChunkedDecoderAcrossReads(t *testing.T) {
	const raw = "4\r\nWiki\r\n5;ext=1\r\npedia\r\n0\r\nX-A: 1\r\nX-B: 2\r\n\r\n"
	for _, step := range []int{1, 2, 3, 7, len(raw)} {
		t.Run(fmt.Sprintf("step%d", step), func(t *testing.T) {
			decoder := &chunkedDecoder{maxTrailer: 1 << 20}
			var decoded, pending []byte
			var trailer []byte
			done := false
			for i := 0; i < len(raw) && !done; i += step {
				end := min(i+step, len(raw))
				pending = append(pending, raw[i:end]...)
				var used int
				var err error
				decoded, used, trailer, done, err = decoder.decode(decoded, pending)
				if err != nil {
					t.Fatalf("decode: %v", err)
				}
				pending = append(pending[:0], pending[used:]...)
			}
			if !done {
				t.Fatal("the decoder never reached the end of the body")
			}
			if string(decoded) != "Wikipedia" {
				t.Fatalf("decoded = %q, want %q", decoded, "Wikipedia")
			}
			fields, err := parseTrailer(trailer)
			if err != nil {
				t.Fatal(err)
			}
			if fields.Get("X-A") != "1" || fields.Get("X-B") != "2" {
				t.Fatalf("trailer = %v", fields)
			}
		})
	}
}

func TestChunkedDecoderRejectsBadFraming(t *testing.T) {
	for name, raw := range map[string]string{
		"size":       "zz\r\nab\r\n",
		"terminator": "2\r\nabXX",
	} {
		t.Run(name, func(t *testing.T) {
			decoder := &chunkedDecoder{maxTrailer: 1 << 20}
			if _, _, _, _, err := decoder.decode(nil, []byte(raw)); !errors.Is(err, ErrMalformed) {
				t.Fatalf("decode(%q) error = %v, want ErrMalformed", raw, err)
			}
		})
	}
}

// TestStreamRequestBodyWithNetHTTPClient uploads through Go's own client, so
// the framing is not this package's on both ends.
func TestStreamRequestBodyWithNetHTTPClient(t *testing.T) {
	addr := serveStreamingServer(t, streamingConfig(), collectBody(sizeAndDigest))
	client, _ := stdClient(t)
	payload := bytes.Repeat([]byte("net/http streams too "), 50000)
	for _, chunked := range []bool{false, true} {
		t.Run(fmt.Sprintf("chunked=%v", chunked), func(t *testing.T) {
			var body io.Reader = bytes.NewReader(payload)
			if chunked {
				// A reader of unknown length makes net/http send chunked.
				body = bufio.NewReader(io.LimitReader(bytes.NewReader(payload), int64(len(payload))))
			}
			req, err := stdhttp.NewRequest(stdhttp.MethodPost, "http://"+addr+"/upload", body)
			if err != nil {
				t.Fatal(err)
			}
			if chunked {
				req.ContentLength = -1
			}
			resp, got := do(t, client, req)
			if resp.StatusCode != stdhttp.StatusOK {
				t.Fatalf("status = %d", resp.StatusCode)
			}
			if got != wantSizeAndDigest(payload) {
				t.Fatalf("body = %q, want %q", got, wantSizeAndDigest(payload))
			}
		})
	}
}

// TestStreamRequestBodyRunsOnTheConnectionWorker checks the invariant that
// reading without waiting buys: many uploads held open at once, each with its
// handler waiting on the rest of its body, and not a goroutine between them.
// A body that had to be waited on would need one apiece.
func TestStreamRequestBodyRunsOnTheConnectionWorker(t *testing.T) {
	const uploads = 64
	config := streamingConfig()
	config.StreamRequestBodyBuffer = 16 << 10
	waiting := make(chan struct{}, uploads)
	// The pool is pinned: what this test counts is goroutines the uploads
	// themselves cost, not the workers the engine's own pool adds under load.
	addr := serveOnPinnedPool(t, config, func(c *Context, r *stdhttp.Request) {
		var total int64
		buf := make([]byte, 4096)
		for {
			n, err := r.Body.Read(buf)
			total += int64(n)
			if !errors.Is(err, ErrWouldBlock) {
				if err != nil && !errors.Is(err, io.EOF) {
					t.Errorf("reading the body: %v", err)
				}
				continue
			}
			// The rest has not arrived; wait for it without a goroutine.
			c.Retain()
			waiting <- struct{}{}
			c.OnBody(func(data []byte, fin bool, err error) {
				total += int64(len(data))
				if err != nil || fin {
					_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%d", total))
					c.Release()
				}
			})
			return
		}
	})

	const size = 256 << 10
	before := runtime.NumGoroutine()
	conns := make([]net.Conn, 0, uploads)
	for i := 0; i < uploads; i++ {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
		if _, err = fmt.Fprintf(conn, "POST /u HTTP/1.1\r\nHost: t\r\nContent-Length: %d\r\n\r\n", size); err != nil {
			t.Fatal(err)
		}
		// Enough to reach the handler, not enough to finish the body.
		if _, err = conn.Write(bytes.Repeat([]byte("s"), 2<<10)); err != nil {
			t.Fatal(err)
		}
		conns = append(conns, conn)
	}
	for i := 0; i < uploads; i++ {
		select {
		case <-waiting:
		case <-time.After(10 * time.Second):
			t.Fatalf("only %d of %d uploads reached their handler", i, uploads)
		}
	}
	// Every upload is now held open with its body unfinished. A goroutine per
	// upload would show here; the few the runtime and the sockets come with
	// are what the allowance is for.
	const allowance = 8
	grew := runtime.NumGoroutine() - before
	if grew > allowance {
		t.Fatalf("%d uploads in flight grew the process by %d goroutines, allowing %d",
			uploads, grew, allowance)
	}

	rest := bytes.Repeat([]byte("s"), size-(2<<10))
	for _, conn := range conns {
		if _, err := conn.Write(rest); err != nil {
			t.Fatal(err)
		}
	}
	for _, conn := range conns {
		resp, err := stdhttp.ReadResponse(bufio.NewReader(conn), &stdhttp.Request{Method: stdhttp.MethodPost})
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if string(body) != fmt.Sprintf("%d", size) {
			t.Fatalf("body = %q, want %d", body, size)
		}
	}
}

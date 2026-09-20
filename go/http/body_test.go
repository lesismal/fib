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
	"strings"
	"sync/atomic"
	"testing"
	"time"
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

// bodyOf is the body a streamed request carries, or nil when the request was
// buffered whole instead.
func bodyOf(r *stdhttp.Request) *BodyStream {
	stream, _ := r.Body.(*BodyStream)
	return stream
}

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
		digest := sha256.New()
		n, err := io.Copy(digest, r.Body)
		if err != nil {
			t.Errorf("reading the body: %v", err)
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%d %x", n, digest.Sum(nil)))
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
	digest := sha256.Sum256(payload)
	_, body := conn.response(stdhttp.MethodPost)
	if want := fmt.Sprintf("%d %x", len(payload), digest); body != want {
		t.Fatalf("body = %q, want %q", body, want)
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
		stream := bodyOf(r)
		if stream == nil {
			t.Errorf("body is %T, want a *BodyStream", r.Body)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading the body: %v", err)
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain",
			fmt.Appendf(nil, "%d %s", len(body), r.Trailer.Get("X-Checksum")))
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

func TestStreamRequestBodyHoldsReadsWhileTheHandlerIsBehind(t *testing.T) {
	const size = 1 << 20
	config := streamingConfig()
	config.StreamRequestBodyBuffer = 16 << 10
	held := make(chan bool, 1)
	addr := serveStreamingServer(t, config, func(c *Context, r *stdhttp.Request) {
		// Let the buffer fill before reading a byte.
		deadline := time.Now().Add(5 * time.Second)
		for !c.Conn.ReadsHeld() && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		held <- c.Conn.ReadsHeld()
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			t.Errorf("reading the body: %v", err)
		}
		if c.Conn.ReadsHeld() {
			t.Error("reads still held once the body had been read")
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%d", n))
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
		<-read
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			t.Errorf("reading the body: %v", err)
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%d", n))
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
	addr := serveStreamingServer(t, config, func(c *Context, r *stdhttp.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		failed <- err
	})
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
	addr := serveStreamingServer(t, streamingConfig(), func(c *Context, r *stdhttp.Request) {
		_, err := io.Copy(io.Discard, r.Body)
		failed <- err
	})
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
		n, _ := io.Copy(io.Discard, r.Body)
		_ = c.Respond(stdhttp.StatusOK, "text/plain",
			fmt.Appendf(nil, "%d %s %d", order.Add(1), r.URL.Path, n))
	})
	payload := bytes.Repeat([]byte("p"), 4<<10)
	conn := dialRaw(t, addr)
	conn.send(fmt.Sprintf("POST /first HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", len(payload)))
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	// The second request arrives in the same breath as the end of the first
	// body, so it may only be served once the first handler has returned.
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
	addr := serveStreamingServer(t, config, func(c *Context, r *stdhttp.Request) {
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			t.Errorf("reading the body: %v", err)
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%d", n))
	})
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

// TestStreamRequestBodyHandlerPanicEndsTheConnection checks that a panic on
// the goroutine serving a streamed request is contained the way the task pool
// contains one on a worker, rather than taking the process with it.
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
	config := streamingConfig()
	addr := serveStreamingServer(t, config, func(c *Context, r *stdhttp.Request) {
		digest := sha256.New()
		n, err := io.Copy(digest, r.Body)
		if err != nil {
			t.Errorf("reading the body: %v", err)
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%d %x", n, digest.Sum(nil)))
	})
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
			want := fmt.Sprintf("%d %x", len(payload), sha256.Sum256(payload))
			if got != want {
				t.Fatalf("body = %q, want %q", got, want)
			}
		})
	}
}

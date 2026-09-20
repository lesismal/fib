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
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/fib/go/internal/tlstest"
	fibtls "github.com/lesismal/fib/go/tls"
)

// TestRetainAnswersFromAnotherGoroutine is the plain case: the handler keeps
// the response open, returns, and something else answers later.
func TestRetainAnswersFromAnotherGoroutine(t *testing.T) {
	addr := serveStreamingServer(t, DefaultConfig(), func(c *Context, r *stdhttp.Request) {
		c.Retain()
		go func() {
			time.Sleep(50 * time.Millisecond)
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("late "+r.URL.Path))
			c.Release()
		}()
	})
	conn := dialRaw(t, addr)
	conn.send("GET /one HTTP/1.1\r\nHost: test\r\n\r\n")
	if _, body := conn.response(stdhttp.MethodGet); body != "late /one" {
		t.Fatalf("body = %q, want %q", body, "late /one")
	}
}

// TestRetainHoldsPipelinedRequests checks that a retained request keeps the
// ones behind it waiting, so responses stay in request order.
func TestRetainHoldsPipelinedRequests(t *testing.T) {
	var order atomic.Int64
	addr := serveStreamingServer(t, DefaultConfig(), func(c *Context, r *stdhttp.Request) {
		if r.URL.Path == "/slow" {
			c.Retain()
			go func() {
				time.Sleep(150 * time.Millisecond)
				_ = c.Respond(stdhttp.StatusOK, "text/plain",
					fmt.Appendf(nil, "%d %s", order.Add(1), r.URL.Path))
				c.Release()
			}()
			return
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%d %s", order.Add(1), r.URL.Path))
	})
	conn := dialRaw(t, addr)
	conn.send("GET /slow HTTP/1.1\r\nHost: test\r\n\r\nGET /fast HTTP/1.1\r\nHost: test\r\n\r\n")
	for _, want := range []string{"1 /slow", "2 /fast"} {
		if _, body := conn.response(stdhttp.MethodGet); body != want {
			t.Fatalf("body = %q, want %q", body, want)
		}
	}
}

// TestRetainNestedHolds checks the count rather than a flag.
func TestRetainNestedHolds(t *testing.T) {
	addr := serveStreamingServer(t, DefaultConfig(), func(c *Context, r *stdhttp.Request) {
		c.Retain()
		c.Retain()
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("two holds"))
		go func() {
			c.Release()
			time.Sleep(30 * time.Millisecond)
			c.Release()
		}()
	})
	conn := dialRaw(t, addr)
	conn.send("GET /two HTTP/1.1\r\nHost: test\r\n\r\n")
	if _, body := conn.response(stdhttp.MethodGet); body != "two holds" {
		t.Fatalf("body = %q", body)
	}
}

func TestOnBodyDeliversABufferedBodyWhole(t *testing.T) {
	type call struct {
		data string
		fin  bool
	}
	calls := make(chan call, 4)
	addr := serveStreamingServer(t, DefaultConfig(), func(c *Context, r *stdhttp.Request) {
		c.Retain()
		c.OnBody(func(data []byte, fin bool, err error) {
			calls <- call{string(data), fin}
			if err != nil || fin {
				_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("got it"))
				c.Release()
			}
		})
	})
	conn := dialRaw(t, addr)
	conn.send("POST /small HTTP/1.1\r\nHost: test\r\nContent-Length: 5\r\n\r\nhello")
	if _, body := conn.response(stdhttp.MethodPost); body != "got it" {
		t.Fatalf("body = %q", body)
	}
	got := <-calls
	if got.data != "hello" || !got.fin {
		t.Fatalf("callback got %q fin=%v, want %q fin=true", got.data, got.fin, "hello")
	}
	select {
	case extra := <-calls:
		t.Fatalf("a second callback with %q fin=%v", extra.data, extra.fin)
	default:
	}
}

// TestOnBodyDeliversAStreamedBodyInPieces is the point of OnBody: a large
// upload handled without a goroutine of the handler's own and without the
// body ever being buffered whole.
func TestOnBodyDeliversAStreamedBodyInPieces(t *testing.T) {
	const size = 4 << 20
	config := streamingConfig()
	config.StreamRequestBodyBuffer = 32 << 10
	pieces := make(chan int, 1)
	addr := serveStreamingServer(t, config, func(c *Context, r *stdhttp.Request) {
		digest := sha256.New()
		var total int64
		var parts int
		c.Retain()
		c.OnBody(func(data []byte, fin bool, err error) {
			if err != nil {
				c.Release()
				return
			}
			digest.Write(data)
			total += int64(len(data))
			if len(data) > 0 {
				parts++
			}
			if fin {
				pieces <- parts
				_ = c.Respond(stdhttp.StatusOK, "text/plain",
					fmt.Appendf(nil, "%d %x", total, digest.Sum(nil)))
				c.Release()
			}
		})
	})
	payload := bytes.Repeat([]byte("callback bodies "), size/16)
	conn := dialRaw(t, addr)
	conn.send(fmt.Sprintf("POST /upload HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", len(payload)))
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("%d %x", len(payload), sha256.Sum256(payload))
	if _, body := conn.response(stdhttp.MethodPost); body != want {
		t.Fatalf("body = %q, want %q", body, want)
	}
	if parts := <-pieces; parts < 2 {
		t.Fatalf("a %d byte body arrived in %d piece(s); it was not delivered as it came", len(payload), parts)
	}
}

// TestOnBodyReleasesFromInsideTheCallback exercises the handover a release
// inside a body callback needs: the callback runs on the connection's worker,
// which holds the parser, so carrying the connection on falls to the worker
// once the callback returns. A pipelined request behind it proves it happened.
func TestOnBodyReleasesFromInsideTheCallback(t *testing.T) {
	var order atomic.Int64
	addr := serveStreamingServer(t, streamingConfig(), func(c *Context, r *stdhttp.Request) {
		if r.URL.Path != "/upload" {
			_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%d %s", order.Add(1), r.URL.Path))
			return
		}
		var total int64
		c.Retain()
		c.OnBody(func(data []byte, fin bool, err error) {
			if err != nil {
				c.Release()
				return
			}
			total += int64(len(data))
			if fin {
				_ = c.Respond(stdhttp.StatusOK, "text/plain",
					fmt.Appendf(nil, "%d /upload %d", order.Add(1), total))
				c.Release()
			}
		})
	})
	payload := bytes.Repeat([]byte("u"), 64<<10)
	conn := dialRaw(t, addr)
	conn.send(fmt.Sprintf("POST /upload HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", len(payload)))
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	conn.send("GET /after HTTP/1.1\r\nHost: test\r\n\r\n")
	for _, want := range []string{fmt.Sprintf("1 /upload %d", len(payload)), "2 /after"} {
		if _, body := conn.response(stdhttp.MethodPost); body != want {
			t.Fatalf("body = %q, want %q", body, want)
		}
	}
}

func TestOnBodyChunkedWithTrailer(t *testing.T) {
	addr := serveStreamingServer(t, streamingConfig(), func(c *Context, r *stdhttp.Request) {
		var got []byte
		c.Retain()
		c.OnBody(func(data []byte, fin bool, err error) {
			if err != nil {
				c.Release()
				return
			}
			got = append(got, data...)
			if fin {
				_ = c.Respond(stdhttp.StatusOK, "text/plain",
					fmt.Appendf(nil, "%d %s", len(got), r.Trailer.Get("X-Checksum")))
				c.Release()
			}
		})
	})
	conn := dialRaw(t, addr)
	conn.send("POST /chunked HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: chunked\r\nTrailer: X-Checksum\r\n\r\n")
	total := 0
	for i := 0; i < 48; i++ {
		chunk := bytes.Repeat([]byte{byte('a' + i%26)}, 64)
		total += len(chunk)
		conn.send(fmt.Sprintf("%x\r\n%s\r\n", len(chunk), chunk))
		time.Sleep(time.Millisecond)
	}
	conn.send("0\r\nX-Checksum: deadbeef\r\n\r\n")
	if _, body := conn.response(stdhttp.MethodPost); body != fmt.Sprintf("%d deadbeef", total) {
		t.Fatalf("body = %q, want %q", body, fmt.Sprintf("%d deadbeef", total))
	}
}

// TestOnBodyWithoutRetainAnswersAndDropsTheRest is the early-refusal case: the
// handler answers straight away and the rest of the upload is discarded.
func TestOnBodyWithoutRetainAnswersAndDropsTheRest(t *testing.T) {
	addr := serveStreamingServer(t, streamingConfig(), func(c *Context, r *stdhttp.Request) {
		c.OnBody(func(data []byte, fin bool, err error) {})
		_ = c.Respond(stdhttp.StatusRequestEntityTooLarge, "text/plain", []byte("no thanks"))
	})
	conn := dialRaw(t, addr)
	conn.send(fmt.Sprintf("POST /big HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", 64<<20))
	if _, err := conn.Write(bytes.Repeat([]byte("z"), 8<<10)); err != nil {
		t.Fatal(err)
	}
	resp, body := conn.response(stdhttp.MethodPost)
	if resp.StatusCode != stdhttp.StatusRequestEntityTooLarge || body != "no thanks" {
		t.Fatalf("response = %d %q", resp.StatusCode, body)
	}
}

// TestOnBodyReportsAClosedConnection is the consistency requirement: a client
// that goes away mid-upload must reach the callback as an error, exactly once,
// and the held request must not be left behind.
func TestOnBodyReportsAClosedConnection(t *testing.T) {
	type outcome struct {
		err   error
		calls int
	}
	done := make(chan outcome, 1)
	addr := serveStreamingServer(t, streamingConfig(), func(c *Context, r *stdhttp.Request) {
		calls := 0
		c.Retain()
		c.OnBody(func(data []byte, fin bool, err error) {
			if err == nil && !fin {
				return
			}
			calls++
			done <- outcome{err, calls}
			c.Release()
			// A second release must be harmless.
			c.Release()
		})
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
	case got := <-done:
		if got.err == nil {
			t.Fatal("the callback was told the body had ended normally")
		}
		if got.calls != 1 {
			t.Fatalf("the callback was ended %d times", got.calls)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the callback was never told the connection had gone")
	}
	select {
	case got := <-done:
		t.Fatalf("a second ending call with %v", got.err)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestOnCancelReportsAClosedConnection covers a handler that retained without
// reading the body at all.
func TestOnCancelReportsAClosedConnection(t *testing.T) {
	cancelled := make(chan error, 2)
	addr := serveStreamingServer(t, DefaultConfig(), func(c *Context, r *stdhttp.Request) {
		c.Retain()
		c.OnCancel(func(err error) {
			cancelled <- err
			c.Release()
		})
	})
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.WriteString(conn, "GET /held HTTP/1.1\r\nHost: test\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	conn.Close()
	select {
	case err := <-cancelled:
		if err == nil {
			t.Fatal("OnCancel was given no reason")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OnCancel never ran")
	}
	select {
	case <-cancelled:
		t.Fatal("OnCancel ran twice")
	case <-time.After(200 * time.Millisecond):
	}
}

// TestOnCancelReportsAReadTimeout checks the other way a retained request can
// end without an answer.
func TestOnCancelReportsAReadTimeout(t *testing.T) {
	cancelled := make(chan error, 1)
	config := streamingConfig()
	config.ReadTimeout = 250 * time.Millisecond
	addr := serveStreamingServer(t, config, func(c *Context, r *stdhttp.Request) {
		c.Retain()
		c.OnCancel(func(err error) { cancelled <- err; c.Release() })
		c.OnBody(func(data []byte, fin bool, err error) {})
	})
	conn := dialRaw(t, addr)
	conn.send(fmt.Sprintf("POST /drip HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n\r\n", 1<<20))
	if _, err := conn.Write(bytes.Repeat([]byte("d"), 4<<10)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-cancelled:
		if err == nil {
			t.Fatal("OnCancel was given no reason")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a retained request outlived the read timeout")
	}
}

// TestOnBodyReportsAnOversizedBody checks that a body the server refuses
// mid-flight reaches the callback rather than only the connection.
func TestOnBodyReportsAnOversizedBody(t *testing.T) {
	config := streamingConfig()
	config.MaxStreamedBodyBytes = 64 << 10
	failed := make(chan error, 1)
	addr := serveStreamingServer(t, config, func(c *Context, r *stdhttp.Request) {
		c.Retain()
		c.OnBody(func(data []byte, fin bool, err error) {
			if err != nil || fin {
				failed <- err
				c.Release()
			}
		})
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
			t.Fatalf("callback error = %v, want ErrBodyTooLarge", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the oversized body never reached the callback")
	}
}

// TestRetainAfterTheResponseIsWrittenDoesNothing checks the idempotency of the
// count against a handler that gets its own bookkeeping wrong.
func TestRetainAfterTheResponseIsWrittenDoesNothing(t *testing.T) {
	addr := serveStreamingServer(t, DefaultConfig(), func(c *Context, r *stdhttp.Request) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(r.URL.Path))
		go func() {
			time.Sleep(20 * time.Millisecond)
			// The response has been written by the handler's return by now.
			c.Retain()
			c.Release()
			c.Release()
		}()
	})
	conn := dialRaw(t, addr)
	for _, path := range []string{"/one", "/two"} {
		conn.send("GET " + path + " HTTP/1.1\r\nHost: test\r\n\r\n")
		if _, body := conn.response(stdhttp.MethodGet); body != path {
			t.Fatalf("body = %q, want %q", body, path)
		}
	}
	time.Sleep(100 * time.Millisecond)
	conn.send("GET /three HTTP/1.1\r\nHost: test\r\n\r\n")
	if _, body := conn.response(stdhttp.MethodGet); body != "/three" {
		t.Fatalf("body = %q, want %q", body, "/three")
	}
}

// TestRetainStress hammers the paths a retained request can take at once:
// answered inline, answered from another goroutine, answered from a body
// callback, and cut short by a client that walks away mid-upload. It is here
// for the consistency the four of them have to keep between them — every
// request ends exactly once, no connection is left stalled, and nothing is
// left running afterwards.
func TestRetainStress(t *testing.T) {
	var (
		answered atomic.Int64
		ended    atomic.Int64
		twice    atomic.Int64
	)
	config := streamingConfig()
	config.StreamRequestBodyBuffer = 16 << 10
	addr := serveStreamingServer(t, config, func(c *Context, r *stdhttp.Request) {
		// ended counts every request that reaches an end, and twice any that
		// reaches one more than once.
		var over atomic.Bool
		finish := func() {
			if over.Swap(true) {
				twice.Add(1)
				return
			}
			ended.Add(1)
		}
		switch r.URL.Path {
		case "/inline":
			finish()
			_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("inline"))
		case "/async":
			c.Retain()
			c.OnCancel(func(error) { finish(); c.Release() })
			go func() {
				time.Sleep(time.Duration(len(r.Host)%5) * time.Millisecond)
				if c.Err() == nil {
					finish()
					_ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("async"))
				}
				c.Release()
			}()
		default:
			var total int64
			c.Retain()
			c.OnBody(func(data []byte, fin bool, err error) {
				total += int64(len(data))
				if err != nil {
					finish()
					c.Release()
					return
				}
				if fin {
					finish()
					_ = c.Respond(stdhttp.StatusOK, "text/plain", fmt.Appendf(nil, "%d", total))
					c.Release()
				}
			})
		}
	})

	const clients = 24
	before := runtime.NumGoroutine()
	payload := bytes.Repeat([]byte("s"), 128<<10)
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
			const pipelined = 8
			expect := pipelined
			switch i % 4 {
			case 0, 1:
				path := "/inline"
				if i%4 == 1 {
					path = "/async"
				}
				for j := 0; j < pipelined; j++ {
					if _, err := io.WriteString(conn, "GET "+path+" HTTP/1.1\r\nHost: t\r\n\r\n"); err != nil {
						return
					}
				}
			case 2:
				expect = 1
				_, _ = fmt.Fprintf(conn, "POST /upload HTTP/1.1\r\nHost: t\r\nContent-Length: %d\r\n\r\n", len(payload))
				if _, err := conn.Write(payload); err != nil {
					return
				}
			default:
				// Walk away in the middle of the upload.
				_, _ = fmt.Fprintf(conn, "POST /upload HTTP/1.1\r\nHost: t\r\nContent-Length: %d\r\n\r\n", 64<<20)
				_, _ = conn.Write(payload)
				conn.Close()
				return
			}
			reader := bufio.NewReader(conn)
			for j := 0; j < expect; j++ {
				_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
				resp, err := stdhttp.ReadResponse(reader, &stdhttp.Request{Method: stdhttp.MethodGet})
				if err != nil {
					t.Errorf("client %d response %d: %v", i, j, err)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				answered.Add(1)
			}
		}(i)
	}
	wg.Wait()

	// Give the cancellations of the abandoned uploads time to run.
	deadline := time.Now().Add(5 * time.Second)
	for ended.Load() < answered.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := twice.Load(); got != 0 {
		t.Fatalf("%d requests ended more than once", got)
	}
	if answered.Load() == 0 {
		t.Fatal("nothing was answered")
	}
	if ended.Load() < answered.Load() {
		t.Fatalf("%d requests answered but only %d reached an end", answered.Load(), ended.Load())
	}
	// Nothing should still be running on the requests' behalf.
	for i := 0; i < 100; i++ {
		if runtime.NumGoroutine() <= before+clients {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutines went from %d to %d", before, runtime.NumGoroutine())
}

// TestRetainOverHTTP2 checks that the same handler code works on a
// multiplexed connection, where a retained response holds up nothing else.
func TestRetainOverHTTP2(t *testing.T) {
	serverConfig, clientConfig, err := tlstest.Configs()
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(HandlerFunc(func(c *Context, r *stdhttp.Request) {
		var got []byte
		c.Retain()
		c.OnBody(func(data []byte, fin bool, err error) {
			if err != nil {
				c.Release()
				return
			}
			got = append(got, data...)
			if fin {
				go func() {
					time.Sleep(20 * time.Millisecond)
					_ = c.Respond(stdhttp.StatusOK, "text/plain",
						fmt.Appendf(nil, "%s %d", r.URL.Path, len(got)))
					c.Release()
				}()
			}
		})
	}))
	addr := serve(t, fibtls.NewServer(ConfigureTLS(serverConfig), handler))
	client := &stdhttp.Client{
		Timeout:   10 * time.Second,
		Transport: &stdhttp.Transport{TLSClientConfig: clientConfig, ForceAttemptHTTP2: true},
	}
	defer client.CloseIdleConnections()

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := strings.Repeat("h", i*500)
			resp, err := client.Post(fmt.Sprintf("https://%s/r%d", addr, i), "text/plain", strings.NewReader(body))
			if err != nil {
				errs <- err
				return
			}
			defer resp.Body.Close()
			out, _ := io.ReadAll(resp.Body)
			if resp.ProtoMajor != 2 {
				errs <- fmt.Errorf("proto %s", resp.Proto)
				return
			}
			if want := fmt.Sprintf("/r%d %d", i, len(body)); string(out) != want {
				errs <- fmt.Errorf("body = %q, want %q", out, want)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

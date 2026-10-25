//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"errors"
	"io"
	"net"
	stdhttp "net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// expectRequest is a POST of body that expects 100-continue. A body of
// unknown length (size < 0) streams, through an io.Pipe.
func expectRequest(t *testing.T, url, body string, streams bool) *stdhttp.Request {
	t.Helper()
	var reader io.Reader = strings.NewReader(body)
	if streams {
		pr, pw := io.Pipe()
		go func() {
			_, _ = io.WriteString(pw, body)
			_ = pw.Close()
		}()
		reader = pr
	}
	req := mustRequest(t, "POST", url, reader)
	req.Header.Set("Expect", "100-continue")
	return req
}

// quiet reports whether nothing more arrives on conn, read through br, for
// d: the client is holding the body back.
func quiet(conn net.Conn, br *bufio.Reader, d time.Duration) bool {
	_ = conn.SetReadDeadline(time.Now().Add(d))
	_, err := br.Peek(1)
	_ = conn.SetReadDeadline(time.Time{})
	return errors.Is(err, os.ErrDeadlineExceeded)
}

// An HTTP/1 request that expects 100-continue sends its header alone, and its
// body once the server answers 100, whether the body was read whole or
// streams.
func TestClientExpectContinueHTTP1(t *testing.T) {
	for _, streams := range []bool{false, true} {
		t.Run("streams="+strconv.FormatBool(streams), func(t *testing.T) {
			held := make(chan bool, 1)
			url := rawServer(t, func(conn net.Conn, _ int) {
				br := bufio.NewReader(conn)
				req, err := stdhttp.ReadRequest(br)
				if err != nil {
					return
				}
				held <- quiet(conn, br, 300*time.Millisecond)
				_, _ = io.WriteString(conn, "HTTP/1.1 100 Continue\r\n\r\n")
				body, err := io.ReadAll(req.Body)
				if err != nil {
					return
				}
				_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: "+strconv.Itoa(len(body))+"\r\n\r\n"+string(body))
			})
			client := newTestClient(t, ClientConfig{Timeout: 10 * time.Second, ExpectContinueTimeout: time.Minute})
			start := time.Now()
			resp, err := client.Go(expectRequest(t, url, "the body", streams)).Wait()
			if err != nil {
				t.Fatal(err)
			}
			if !<-held {
				t.Fatal("the body came with the header, before the server's 100")
			}
			if got := readBody(t, resp); got != "the body" {
				t.Fatalf("server got %q", got)
			}
			if time.Since(start) > 5*time.Second {
				t.Fatal("the body waited for ExpectContinueTimeout despite the 100")
			}
		})
	}
}

// A server that never answers the expectation gets the body once
// ExpectContinueTimeout has run out.
func TestClientExpectContinueTimeoutHTTP1(t *testing.T) {
	const wait = 300 * time.Millisecond
	waited := make(chan time.Duration, 1)
	url := rawServer(t, func(conn net.Conn, _ int) {
		br := bufio.NewReader(conn)
		req, err := stdhttp.ReadRequest(br)
		if err != nil {
			return
		}
		start := time.Now()
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return
		}
		waited <- time.Since(start)
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: "+strconv.Itoa(len(body))+"\r\n\r\n"+string(body))
	})
	client := newTestClient(t, ClientConfig{Timeout: 10 * time.Second, ExpectContinueTimeout: wait})
	resp, err := client.Go(expectRequest(t, url, "late body", false)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "late body" {
		t.Fatalf("server got %q", got)
	}
	if d := <-waited; d < wait*2/3 {
		t.Fatalf("the body came %v after the header, before the %v wait", d, wait)
	}
}

// A final response before the 100 is the answer: the body is never sent, and
// the connection, which announced a body it did not send, is not used again.
func TestClientExpectContinueRefusedHTTP1(t *testing.T) {
	for _, streams := range []bool{false, true} {
		t.Run("streams="+strconv.FormatBool(streams), func(t *testing.T) {
			after := make(chan int, 2)
			url := rawServer(t, func(conn net.Conn, _ int) {
				br := bufio.NewReader(conn)
				if _, err := stdhttp.ReadRequest(br); err != nil {
					return
				}
				_, _ = io.WriteString(conn, "HTTP/1.1 417 Expectation Failed\r\nContent-Length: 2\r\n\r\nno")
				_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				rest, _ := io.ReadAll(br)
				after <- len(rest)
			})
			client := newTestClient(t, ClientConfig{Timeout: 10 * time.Second, ExpectContinueTimeout: time.Minute,
				MaxIdleConnsPerHost: 4})
			resp, err := client.Go(expectRequest(t, url, "unwanted", streams)).Wait()
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != stdhttp.StatusExpectationFailed || readBody(t, resp) != "no" {
				t.Fatalf("got %d", resp.StatusCode)
			}
			// The client closes the connection rather than keep it.
			select {
			case n := <-after:
				if n != 0 {
					t.Fatalf("the server got %d bytes after the header", n)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("the connection was kept")
			}
		})
	}
}

// With ExpectContinueTimeout zero the body goes with the header, as before.
func TestClientExpectContinueOffHTTP1(t *testing.T) {
	url := rawServer(t, func(conn net.Conn, _ int) {
		br := bufio.NewReader(conn)
		req, err := stdhttp.ReadRequest(br)
		if err != nil {
			return
		}
		// No 100: a body that waited for one would never come.
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: "+strconv.Itoa(len(body))+"\r\n\r\n"+string(body))
	})
	client := newTestClient(t, ClientConfig{Timeout: 10 * time.Second})
	resp, err := client.Go(expectRequest(t, url, "eager", false)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "eager" {
		t.Fatalf("server got %q", got)
	}
}

// h2NoData reports whether the client sends no DATA on stream id for d.
func h2NoData(p *cvH2cPeer, id uint32, d time.Duration) bool {
	_ = p.conn.SetReadDeadline(time.Now().Add(d))
	defer p.conn.SetReadDeadline(time.Now().Add(20 * time.Second))
	for {
		f, ok := p.next()
		if !ok {
			return true
		}
		if f.typ == h2FrameData && f.streamID == id {
			return false
		}
	}
}

// On HTTP/2 the HEADERS of a request that expects 100-continue go alone, and
// its DATA once the server answers 100.
func TestClientExpectContinueHTTP2(t *testing.T) {
	for _, streams := range []bool{false, true} {
		t.Run("streams="+strconv.FormatBool(streams), func(t *testing.T) {
			held := make(chan bool, 1)
			url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
				p.settings()
				// The client's SETTINGS ack and WINDOW_UPDATE come first.
				id, _, ok := p.request()
				if !ok {
					return
				}
				held <- h2NoData(p, id, 300*time.Millisecond)
				p.headers(id, false, ":status", "100")
				var body []byte
				for {
					f, _, ok := p.await(h2FrameData)
					if !ok {
						return
					}
					body = append(body, f.payload...)
					if f.has(h2FlagEndStream) {
						break
					}
				}
				p.headers(id, false, ":status", "200")
				p.frame(h2FrameData, h2FlagEndStream, id, body...)
				p.drain()
			})
			client := cvH2cClient(t, func(c *ClientConfig) { c.ExpectContinueTimeout = time.Minute })
			resp, err := client.Go(expectRequest(t, url, "h2 body", streams)).Wait()
			if err != nil {
				t.Fatal(err)
			}
			if !<-held {
				t.Fatal("DATA came with HEADERS, before the server's 100")
			}
			if got := readBody(t, resp); got != "h2 body" {
				t.Fatalf("server got %q", got)
			}
		})
	}
}

// A final HTTP/2 response before the 100 keeps the body from being sent, and
// the stream is reset once the response is complete.
func TestClientExpectContinueRefusedHTTP2(t *testing.T) {
	got := make(chan string, 1)
	url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
		p.settings()
		id, _, ok := p.request()
		if !ok {
			return
		}
		p.headers(id, true, ":status", "403")
		for {
			f, ok := p.next()
			if !ok {
				got <- "closed"
				return
			}
			if f.streamID != id {
				continue
			}
			switch f.typ {
			case h2FrameData:
				got <- "DATA"
				return
			case h2FrameRSTStream:
				got <- "RST_STREAM"
				p.drain()
				return
			}
		}
	})
	client := cvH2cClient(t, func(c *ClientConfig) { c.ExpectContinueTimeout = 300 * time.Millisecond })
	resp, err := client.Go(expectRequest(t, url, "unwanted", false)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != stdhttp.StatusForbidden {
		t.Fatalf("got %d", resp.StatusCode)
	}
	if g := <-got; g != "RST_STREAM" {
		t.Fatalf("after the response the client sent %s, want RST_STREAM and no DATA", g)
	}
	// Past the wait, the timer finds the stream gone and sends nothing.
	time.Sleep(500 * time.Millisecond)
}

// An HTTP/2 server that never answers the expectation gets the body once the
// wait has run out.
func TestClientExpectContinueTimeoutHTTP2(t *testing.T) {
	const wait = 300 * time.Millisecond
	waited := make(chan time.Duration, 1)
	url := cvH2cServe(t, func(p *cvH2cPeer, _ int) {
		p.settings()
		id, _, ok := p.request()
		if !ok {
			return
		}
		start := time.Now()
		f, _, ok := p.await(h2FrameData)
		if !ok {
			return
		}
		waited <- time.Since(start)
		p.headers(id, false, ":status", "200")
		p.frame(h2FrameData, h2FlagEndStream, id, f.payload...)
		p.drain()
	})
	client := cvH2cClient(t, func(c *ClientConfig) { c.ExpectContinueTimeout = wait })
	resp, err := client.Go(expectRequest(t, url, "late", false)).Wait()
	if err != nil {
		t.Fatal(err)
	}
	if got := readBody(t, resp); got != "late" {
		t.Fatalf("server got %q", got)
	}
	if d := <-waited; d < wait*2/3 {
		t.Fatalf("DATA came %v after HEADERS, before the %v wait", d, wait)
	}
}

// fib's own server answers the expectation, over HTTP/1 and HTTP/2, so a
// client that waits for the 100 gets its body there without the wait.
func TestClientExpectContinueToFibServer(t *testing.T) {
	for _, h2c := range []bool{false, true} {
		t.Run("h2c="+strconv.FormatBool(h2c), func(t *testing.T) {
			addr := serve(t, NewHandler(echoHandler()))
			client := newTestClient(t, ClientConfig{Timeout: 10 * time.Second, ExpectContinueTimeout: time.Minute,
				UnencryptedHTTP2: h2c})
			start := time.Now()
			resp, err := client.Go(expectRequest(t, "http://"+addr, "to fib", false)).Wait()
			if err != nil {
				t.Fatal(err)
			}
			if got := readBody(t, resp); got != "POST / to fib" {
				t.Fatalf("server got %q", got)
			}
			if resp.ProtoMajor != map[bool]int{false: 1, true: 2}[h2c] {
				t.Fatalf("spoke %s", resp.Proto)
			}
			if time.Since(start) > 5*time.Second {
				t.Fatal("the body waited for ExpectContinueTimeout")
			}
		})
	}
}

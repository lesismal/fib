//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bufio"
	"bytes"
	"io"
	"net"
	stdhttp "net/http"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/internal/outputwait"
)

// A whole response above the copy threshold is kept as the handler gave it: a
// reader that stops after the head leaves the body in the connection's queue,
// counted as queued bytes, and the peer that reads on gets exactly the bytes
// given, so nothing has copied it or written into it while it waited.
func TestRespondToStalledReaderKeepsTheBody(t *testing.T) {
	if runtime.GOOS == "windows" {
		// An overlapped send hands the whole body to the kernel at once, so
		// none of it waits in the queue this looks at.
		t.Skip("Windows hands a queued send to the kernel whole")
	}
	const size = 8 << 20
	body := make([]byte, size)
	for i := range body {
		body[i] = byte(i*13 + 7)
	}
	want := bytes.Clone(body)
	conns := make(chan *fib.Connection, 1)
	addr := serve(t, NewHandlerWithConfig(DefaultConfig(), HandlerFunc(func(c *Context) {
		// A small send buffer keeps the kernel from swallowing the body, so
		// that the rest of it waits in the queue the test looks at.
		if err := c.Conn.SetWriteBuffer(64 << 10); err != nil {
			t.Error(err)
		}
		conns <- c.Conn
		if err := c.Respond(stdhttp.StatusOK, "application/octet-stream", body); err != nil {
			t.Error(err)
		}
	})))
	peer, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if _, err := io.WriteString(peer, "GET /big HTTP/1.1\r\nHost: bench\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(peer)
	resp, err := stdhttp.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ContentLength != size {
		t.Fatalf("Content-Length = %d, want %d", resp.ContentLength, size)
	}
	c := <-conns
	// The round flushes when the handler returns; whatever the socket did not
	// take has to stay queued, and it is the body, not a copy of it.
	deadline := time.Now().Add(10 * time.Second)
	pending := outputwait.Unsent(c)
	for pending < size-1<<20 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
		pending = outputwait.Unsent(c)
	}
	if pending < size-1<<20 {
		t.Fatalf("only %d of the %d body bytes are queued", pending, size)
	}
	got := make([]byte, size)
	if _, err := io.ReadFull(resp.Body, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("what the peer read differs from the body it was given")
	}
	if !bytes.Equal(body, want) {
		t.Fatal("the handler's body was written into")
	}
}

// Context.Body is the server's buffer for the request, which goes back to the
// pool once the response is written, while a whole response may still be
// waiting in the queue. A response built from it is copied for that reason,
// even though the same body above the threshold is otherwise kept: the peer
// gets what the request carried, however much of the next request has been
// read into that buffer meanwhile.
func TestRespondWithContextBodyIsCopied(t *testing.T) {
	const size = 2 << 20
	first := bytes.Repeat([]byte{'a'}, size)
	second := bytes.Repeat([]byte{'b'}, size)
	var requests atomic.Int32
	addr := serve(t, NewHandlerWithConfig(DefaultConfig(), HandlerFunc(func(c *Context) {
		if requests.Add(1) == 1 {
			// Let the connection go on reading with an answer still queued,
			// so that the buffer the first request's body was in is free for
			// the next request's body while the first answer waits.
			c.Conn.SetWriteBackpressure(false)
			if err := c.Conn.SetWriteBuffer(64 << 10); err != nil {
				t.Error(err)
			}
		}
		if err := c.Respond(stdhttp.StatusOK, "application/octet-stream", c.Body()); err != nil {
			t.Error(err)
		}
	})))
	peer, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	request := func(body []byte) {
		t.Helper()
		head := "POST /echo HTTP/1.1\r\nHost: bench\r\nContent-Length: " +
			strconv.Itoa(len(body)) + "\r\n\r\n"
		if _, err := peer.Write(append([]byte(head), body...)); err != nil {
			t.Fatal(err)
		}
	}
	request(first)
	reader := bufio.NewReader(peer)
	resp, err := stdhttp.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ContentLength != size {
		t.Fatalf("first Content-Length = %d, want %d", resp.ContentLength, size)
	}
	// The first answer is queued and the reader is not reading it; the second
	// request goes on the wire, and its body into a buffer like the first's.
	request(second)
	time.Sleep(50 * time.Millisecond)
	got := make([]byte, size)
	if _, err := io.ReadFull(resp.Body, got); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !bytes.Equal(got, first) {
		t.Fatal("the echo of the first request's body differs from what it carried")
	}
	resp, err = stdhttp.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got = make([]byte, size)
	if _, err := io.ReadFull(resp.Body, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, second) {
		t.Fatal("the echo of the second request's body differs from what it carried")
	}
}

// within is what tells a response built from the request's own buffer, which
// must be copied, from one the handler owns, which the connection keeps.
func TestWithin(t *testing.T) {
	b := make([]byte, 16)
	other := make([]byte, 16)
	cases := []struct {
		name string
		p, b []byte
		want bool
	}{
		{"whole", b, b, true},
		{"prefix", b[:4], b, true},
		{"middle", b[4:8], b, true},
		{"suffix", b[8:], b, true},
		{"empty", b[:0], b, false},
		{"nil body", b, nil, false},
		{"other", other[:4], b, false},
		{"longer", append(b, 0), b, false},
	}
	for _, tc := range cases {
		if got := within(tc.p, tc.b); got != tc.want {
			t.Errorf("%s: within = %v, want %v", tc.name, got, tc.want)
		}
	}
}

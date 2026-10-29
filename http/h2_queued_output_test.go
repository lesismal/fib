//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	stdhttp "net/http"
	"sync/atomic"
	"testing"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/internal/outputwait"
)

// A client whose windows let every response go at once, as h2load's 1 GiB
// ones do, but which reads slower than the server writes, has no more than
// h2MaxQueuedOutput of the connection's whole responses framed and queued for
// its socket at a time: the rest of each body waits in its stream as the
// handler's own array, rather than all of them being copied into the
// connection's queue at once. Each arrives whole once the client reads.
func TestH2QueuedOutputIsBoundedByTheSocket(t *testing.T) {
	if outputwait.Queued == nil {
		t.Skip("no queue to measure on this backend")
	}
	const size = 8 << 20
	whole := bytes.Repeat([]byte("0123456789abcdef"), size/16)
	var conn atomic.Pointer[fib.Connection]
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		conn.Store(c.Conn)
		_ = c.WriteResponse(Response{StatusCode: stdhttp.StatusOK, Body: whole})
	})))
	tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), h2MaxWindow})
	tc.write(h2AppendWindowUpdate(nil, 0, h2MaxWindow-h2DefaultWindow))
	const streams = 4
	for i := range streams {
		tc.headers(uint32(1+2*i), true, get("/")...)
	}

	// Nothing is read for a while, so the socket fills and the rest of what
	// the server would send waits on its side.
	var most int64
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(2 * time.Millisecond) {
		if c := conn.Load(); c != nil {
			most = max(most, outputwait.Queued(c))
		}
	}
	if most == 0 {
		t.Fatal("nothing waited for the socket; the test measures nothing")
	}
	if most > h2MaxQueuedOutput {
		t.Fatalf("%d bytes waited for the socket at most, want no more than %d", most, h2MaxQueuedOutput)
	}

	for i := range streams {
		if _, body := tc.response(uint32(1 + 2*i)); !bytes.Equal(body, whole) {
			t.Fatalf("stream %d: got %d bytes, want the %d sent", 1+2*i, len(body), len(whole))
		}
	}
}

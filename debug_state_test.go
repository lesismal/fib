//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/lesismal/fib/internal/debughook"
)

// With tracing on, a connection records its steps, deadlines among them with
// the callers that set them, and keeps the last of them once its ring is
// full; the debug hooks read them back, and the connection's state, and
// answer plainly for anything else.
func TestDebugHooksTraceAConnection(t *testing.T) {
	c := newOfflineConnection(newOfflineServer(t))
	if got := debughook.ConnTrace(c); got != "no trace" {
		t.Errorf("ConnTrace before tracing = %q", got)
	}
	debughook.SetTrace(true)
	t.Cleanup(func() {
		debughook.SetTrace(false)
		traces.Delete(c)
	})

	if err := c.SetReadDeadline(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := c.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	trace := debughook.ConnTrace(c)
	if !strings.Contains(trace, "deadline write=false") || !strings.Contains(trace, "in=zero") ||
		!strings.Contains(trace, " from ") {
		t.Errorf("trace of two deadlines:\n%s", trace)
	}

	const steps = 300
	for i := range steps {
		c.traceStep("step %d", i)
	}
	lines := strings.Split(debughook.ConnTrace(c), "\n")
	if len(lines) != len(traceRing{}.ev) || !strings.Contains(lines[len(lines)-1], "step 299 |") {
		t.Errorf("a full ring kept %d lines ending %q", len(lines), lines[len(lines)-1])
	}

	if state := debughook.ConnState(c); !strings.HasPrefix(state, "queue=0/0") || !strings.Contains(state, "fileBytes=0") {
		t.Errorf("ConnState = %q", state)
	}
	var none *Connection
	for _, v := range []any{"x", none} {
		if got := debughook.ConnTrace(v); got != "not a *fib.Connection" {
			t.Errorf("ConnTrace(%#v) = %q", v, got)
		}
		if got := debughook.ConnState(v); got != "not a *fib.Connection" {
			t.Errorf("ConnState(%#v) = %q", v, got)
		}
	}
}

// A deadline set on a connection that is closing is refused.
func TestDeadlineOnAClosedConnection(t *testing.T) {
	c := newOfflineConnection(newOfflineServer(t))
	c.mu.Lock()
	c.closing = true
	c.mu.Unlock()
	if err := c.SetDeadline(time.Now().Add(time.Second)); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("SetDeadline on a closing connection = %v, want net.ErrClosed", err)
	}
	if err := c.SetReadDeadline(time.Now().Add(time.Second)); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("SetReadDeadline on a closing connection = %v, want net.ErrClosed", err)
	}
}

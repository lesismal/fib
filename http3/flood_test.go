//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http3

import (
	"errors"
	"testing"
	"time"

	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3/qpack"
	"github.com/lesismal/fib/http3/quic"
)

// floodBurst is the burst of resets QUIC allows a peer; see quic's flood.go.
const floodBurst = 1000

// floodServer serves 200 to every request, and lets a client have more
// requests open at once than the tests open, so that only the flood limits
// stand in their way.
func floodServer(t *testing.T) string {
	config := DefaultConfig()
	config.MaxConcurrentStreams = 4 * floodBurst
	return startServer(t, config, func(c *fibhttp.Context) {
		_ = c.Respond(200, "text/plain", []byte("ok"))
	})
}

// awaitExcessiveLoad waits for the server to close w's connection with
// H3_EXCESSIVE_LOAD.
func (w *h3Wire) awaitExcessiveLoad() {
	w.t.Helper()
	select {
	case err := <-w.closed:
		var appErr *quic.ApplicationError
		if !errors.As(err, &appErr) || !appErr.Remote || appErr.Code != uint64(ErrCodeExcessiveLoad) {
			w.t.Fatalf("closed with %v, want H3_EXCESSIVE_LOAD", err)
		}
	case <-time.After(20 * time.Second):
		w.t.Fatal("the connection was not closed")
	}
}

// openAndReset opens n request streams, each with a request header that
// waits for its body, and abandons each at once, as a client cancelling its
// requests does. It stops at the first stream the connection no longer
// lets it open.
func (w *h3Wire) openAndReset(n int) {
	head := headersFrame(qpack.HeaderField{Name: ":method", Value: "POST"}, qpack.HeaderField{Name: ":scheme", Value: "https"},
		qpack.HeaderField{Name: ":authority", Value: "localhost"}, qpack.HeaderField{Name: ":path", Value: "/"})
	for i := 0; i < n; i++ {
		s, err := w.qc.OpenStream()
		if err != nil {
			return
		}
		_ = s.Write(head, false)
		s.StopSending(uint64(ErrCodeRequestCancelled))
		s.Reset(uint64(ErrCodeRequestCancelled))
	}
}

// TestH3ServerEndsRapidReset checks that a client opening requests and
// cancelling them at once, past the burst the server allows, has its
// connection closed with H3_EXCESSIVE_LOAD (CVE-2023-44487's HTTP/3 form).
func TestH3ServerEndsRapidReset(t *testing.T) {
	w := dialH3Wire(t, nil, floodServer(t))
	w.openAndReset(floodBurst + 100)
	w.awaitExcessiveLoad()
}

// TestH3ServerAllowsResetsUnderTheBurst checks that cancellations within the
// burst leave the connection serving.
func TestH3ServerAllowsResetsUnderTheBurst(t *testing.T) {
	w := dialH3Wire(t, nil, floodServer(t))
	w.openAndReset(floodBurst / 2)
	id := w.send(t, h3Request{path: "/after"}, false)
	if status, _ := w.response(id); status != 200 {
		t.Fatalf("status %d after %d resets", status, floodBurst/2)
	}
}

// TestH3ServerEndsMalformedRequestFlood checks that requests the server
// resets for being malformed count as the client's own resets do: a client
// sending one after another is closed with H3_EXCESSIVE_LOAD.
func TestH3ServerEndsMalformedRequestFlood(t *testing.T) {
	w := dialH3Wire(t, nil, floodServer(t))
	// No :path, which a request must have.
	head := headersFrame(qpack.HeaderField{Name: ":method", Value: "GET"}, qpack.HeaderField{Name: ":scheme", Value: "https"},
		qpack.HeaderField{Name: ":authority", Value: "localhost"})
	for i := 0; i < floodBurst+100; i++ {
		s, err := w.qc.OpenStream()
		if err != nil {
			break
		}
		_ = s.Write(head, true)
	}
	w.awaitExcessiveLoad()
}

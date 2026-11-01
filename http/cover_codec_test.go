//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	"errors"
	stdhttp "net/http"
	"strings"
	"testing"
)

// The client's fast framing and parsing leave to net/http what they do not
// take: an empty field name, a request declaring a length with no body, a
// chunk size in upper-case hex (taken), one that is not hex, and status
// lines and fields net/http would read differently.
func TestCoverCodecFastPathRefusals(t *testing.T) {
	if validToken("") {
		t.Fatal("an empty token is valid")
	}
	req, _ := stdhttp.NewRequest("GET", "http://example.com/", nil)
	req.ContentLength = 5
	if _, ok := fastRequest(req, nil, false); ok {
		t.Fatal("a length with no body was framed")
	}
	if got, ok := decodeChunks(nil, []byte("A\r\n0123456789\r\n0\r\n\r\n")); !ok || string(got) != "0123456789" {
		t.Fatalf("upper-case chunk size: %q %v", got, ok)
	}
	for _, chunks := range []string{
		"g\r\nx\r\n0\r\n\r\n",
		"1\r\nxy\r\n0\r\n\r\n",
		// More framing than body, in extensions, than net/http allows.
		strings.Repeat("1;"+strings.Repeat("e", 40)+"\r\nx\r\n", 1000) + "0\r\n\r\n",
	} {
		if _, ok := decodeChunks(nil, []byte(chunks)); ok {
			t.Errorf("%.20q decoded", chunks)
		}
	}
	get := &stdhttp.Request{Method: "GET"}
	for _, head := range []string{
		"HTTP/1.1 200 OK\nX: y\r\n\r\n",
		"HTTP/1.1 20 OK\r\n\r\n",
		"HTTP/1.1 2x0 OK\r\n\r\n",
		"HTTP/1.1 200 O\x01K\r\n\r\n",
		"HTTP/1.1 200 OK\r\nno colon\r\n\r\n",
		"HTTP/1.1 200 OK\r\n: empty name\r\n\r\n",
		// Heads cut short of their last line's end.
		"HTTP/1.1 200 OK\r\n",
		"HTTP/1.1 200 OK\r\nX: y\r\n",
	} {
		if resp, ok := parseResponseHead([]byte(head), get); ok {
			t.Errorf("%q parsed: %v", head, resp)
		}
	}
}

// splitTransferCodings: a head with no Transfer-Encoding has nothing to
// rewrite, one with an empty one is refused, a request's must end in
// chunked, and a response in a coding alone loses its Content-Length.
func TestCoverCodecSplitTransferCodings(t *testing.T) {
	if rewritten, _, list, err := splitTransferCodings([]byte("HTTP/1.1 200 OK\r\n\r\n"), false); rewritten != nil || list != nil || err != nil {
		t.Fatalf("no field: %q %v %v", rewritten, list, err)
	}
	if _, _, _, err := splitTransferCodings([]byte("HTTP/1.1 200 OK\r\nTransfer-Encoding: ,\r\n\r\n"), false); err == nil {
		t.Fatal("an empty Transfer-Encoding was taken")
	}
	if _, _, _, err := splitTransferCodings([]byte("POST / HTTP/1.1\r\nTransfer-Encoding: gzip\r\n\r\n"), true); err == nil {
		t.Fatal("a request not ending in chunked was taken")
	}
	rewritten, coding, _, err := splitTransferCodings(
		[]byte("HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\nContent-Length: 5\r\nX: y\r\n\r\n"), false)
	if err != nil || coding != "gzip" || bytes.Contains(rewritten, []byte("Content-Length")) ||
		bytes.Contains(rewritten, []byte("Transfer-Encoding")) || !bytes.Contains(rewritten, []byte("X: y\r\n")) {
		t.Fatalf("%q %q %v", rewritten, coding, err)
	}
	// A whole body decoded with no limit.
	payload := bytes.Repeat([]byte("unbounded "), 1000)
	if out, err := decodeWhole("gzip", gzipBody(t, payload), 0, ErrBodyTooLarge, ErrMalformed); err != nil || !bytes.Equal(out, payload) {
		t.Fatalf("decodeWhole: %d bytes, %v", len(out), err)
	}
}

// cvCodecTail is a filter whose Close returns tail.
type cvCodecTail struct{ tail string }

func (f cvCodecTail) Filter(p []byte) ([]byte, error) { return p, nil }
func (f cvCodecTail) Close() ([]byte, error)          { return []byte(f.tail), nil }

// endFilters runs a filter's tail through the filters after it, passing
// over them once there is none, and a filter that fails on a tail fails the
// end, aborting those after it unless it only flushed.
func TestCoverCodecEndFilters(t *testing.T) {
	h := &responseHooks{filters: []BodyFilter{cvCodecTail{}, cvCodecTail{"!"}}}
	if out, err := h.endFilters(false); err != nil || string(out) != "!" {
		t.Fatalf("%q %v", out, err)
	}
	broken := errors.New("broken")
	after := &cvSyncFilter{aborted: make(chan error, 1)}
	h = &responseHooks{filters: []BodyFilter{cvCodecTail{"x"}, &cvSyncFilter{filterErr: broken}, after}}
	if _, err := h.endFilters(false); err != broken || h.filters != nil {
		t.Fatalf("%v, filters %v", err, h.filters)
	}
	select {
	case <-after.aborted:
	default:
		t.Fatal("the filters after the failing one were not aborted")
	}
	h = &responseHooks{filters: []BodyFilter{cvRwFlushTail{}, &cvSyncFilter{filterErr: broken}}}
	if _, err := h.endFilters(true); err != broken || h.filters == nil {
		t.Fatalf("flush: %v, filters %v", err, h.filters)
	}
}

// cvRwFlushTail is a filter whose Flush hands over something.
type cvRwFlushTail struct{ cvCodecTail }

func (cvRwFlushTail) Flush() ([]byte, error) { return []byte("held"), nil }

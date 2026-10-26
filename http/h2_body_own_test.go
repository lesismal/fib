//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"bytes"
	stdhttp "net/http"
	"strconv"
	"testing"
	"unsafe"
)

// Context.Body is the server's buffer for the request, which goes back to the
// pool once the handler returns, while an HTTP/2 response built from it may
// still be held by the client's windows. A whole response made of it is copied
// for that reason, as HTTP/1's send copies one: the peer gets what the request
// carried, however much of the next request has been read into that buffer
// meanwhile. The client's window is shut, so that each body waits in its
// stream, and the next request is read over the first body's buffer before
// either window opens.
func TestH2RespondWithContextBodyCopiesIt(t *testing.T) {
	const size = 64 << 10
	first := bytes.Repeat([]byte{'a'}, size)
	second := bytes.Repeat([]byte{'b'}, size)
	forms := []struct {
		name string
		send func(c *Context, body []byte)
	}{
		{"Respond", func(c *Context, body []byte) {
			_ = c.Respond(stdhttp.StatusOK, "application/octet-stream", body)
		}},
		{"WriteResponse", func(c *Context, body []byte) {
			_ = c.WriteResponse(Response{
				StatusCode: stdhttp.StatusOK,
				Header:     stdhttp.Header{"Content-Type": {"application/octet-stream"}},
				Body:       body,
			})
		}},
	}
	for _, form := range forms {
		t.Run(form.name, func(t *testing.T) {
			// One processor, and the handler on the reader, so that the buffer
			// the first request's body goes back to is the one the next
			// request's body is collected in, as it is on HTTP/1: the recycle
			// this checks is the server's own.
			defer withOneProcessor()()
			config := DefaultConfig()
			config.StreamPool = StreamPoolConfig{Disable: true}
			addr := serve(t, NewHandlerWithConfig(config, HandlerFunc(func(c *Context) {
				form.send(c, c.Body())
			})))
			tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 0})
			request := func(id uint32, body []byte) {
				tc.headers(id, false, ":method", "POST", ":scheme", "http", ":authority", "test",
					":path", "/echo", "content-length", strconv.Itoa(len(body)))
				for rest := body; len(rest) > 0; {
					chunk := rest[:min(len(rest), h2DefaultMaxFrameSize)]
					rest = rest[len(chunk):]
					tc.data(id, len(rest) == 0, chunk)
				}
			}
			request(1, first)
			request(3, second)
			// Both handlers have answered once their heads have arrived, and
			// both bodies wait in their streams: the second request's body has
			// been collected over the first's buffer by then.
			for _, id := range []uint32{1, 3} {
				if f := tc.readUntil(h2FrameHeaders); f.streamID != id {
					t.Fatalf("the head of stream %d arrived on stream %d", id, f.streamID)
				}
			}
			// Let both bodies go, opening the connection's window too.
			for _, id := range []uint32{1, 3} {
				tc.write(h2AppendWindowUpdate(nil, 0, size))
				tc.write(h2AppendWindowUpdate(nil, id, size))
			}
			for _, id := range []uint32{1, 3} {
				tc.readAcking(id)
			}
			if got := tc.stream(1).body; !bytes.Equal(got, first) {
				t.Fatalf("the echo of the first request's body differs from what it carried: %d bytes, starting %q",
					len(got), got[:min(len(got), 8)])
			}
			if got := tc.stream(3).body; !bytes.Equal(got, second) {
				t.Fatalf("the echo of the second request's body differs from what it carried: %d bytes, starting %q",
					len(got), got[:min(len(got), 8)])
			}
		})
	}
}

// A body the handler built is the response's own: its stream keeps that slice
// rather than copying it, as a whole HTTP/1 response does, and only a body
// that is the request's own buffer is copied. The client's window is shut, so
// that the body waits in the stream, which the handler looks at directly.
func TestH2RespondKeepsAHandlerBody(t *testing.T) {
	body := bytes.Repeat([]byte{'x'}, 64<<10)
	type held struct {
		body, pending unsafe.Pointer
		n             int
	}
	done := make(chan held, 1)
	addr := serve(t, NewHandler(HandlerFunc(func(c *Context) {
		var h held
		if err := c.Respond(stdhttp.StatusOK, "application/octet-stream", body); err != nil {
			t.Error(err)
		} else {
			st := c.stream
			sc := st.sc
			sc.mu.Lock()
			pending := st.pending
			sc.mu.Unlock()
			h = held{unsafe.Pointer(unsafe.SliceData(body)), unsafe.Pointer(unsafe.SliceData(pending)), len(pending)}
		}
		done <- h
	})))
	tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 0})
	tc.headers(1, true, get("/")...)
	h := <-done
	if h.n != len(body) {
		t.Fatalf("the stream holds %d body bytes, want %d", h.n, len(body))
	}
	if h.pending != h.body {
		t.Fatal("the stream copied a body the handler built, which it should have kept")
	}
}

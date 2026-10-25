//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"encoding/binary"
	"io"
	"testing"
	"time"
)

// h2ExpectLinger reads until the server's GOAWAY, checks its code, and
// that the server then ends its side of the connection while it goes on
// taking what the client sends, as a client in the middle of an upload
// would: the client's writes succeed for a while, rather than meeting the
// reset a socket closed with bytes unread sends, and then the server closes
// the connection within the linger's bound.
func h2ExpectLinger(t *testing.T, tc *h2TestConn, code H2ErrorCode) {
	t.Helper()
	f := tc.readUntil(h2FrameGoAway)
	if got := H2ErrorCode(binary.BigEndian.Uint32(f.payload[4:8])); got != code {
		t.Fatalf("GOAWAY %v, want %v", got, code)
	}
	buf := make([]byte, 1024)
	for {
		_, err := tc.c.Read(buf)
		if err == nil {
			continue
		}
		if err != io.EOF {
			t.Fatalf("read after GOAWAY: %v, want EOF", err)
		}
		break
	}
	frame := append(h2AppendFrameHeader(nil, h2FrameData, 0, 1, 1000), make([]byte, 1000)...)
	start := time.Now()
	for i := 0; i < 5; i++ {
		if _, err := tc.c.Write(frame); err != nil {
			t.Fatalf("write %d, %v after EOF: %v", i, time.Since(start), err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Then the connection closes: writes fail once the reset the close
	// draws has come back.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := tc.c.Write(frame); err != nil {
			if waited := time.Since(start); waited > 2*time.Second {
				t.Fatalf("lingered %v", waited)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the connection stayed open after its GOAWAY")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestH2ServerLingersAfterAConnectionError checks that a connection ended
// with GOAWAY for an error lingers rather than resetting a client still
// sending.
func TestH2ServerLingersAfterAConnectionError(t *testing.T) {
	addr := serve(t, NewHandler(echoHandler()))
	tc := dialH2(t, addr)
	tc.headers(1, false, ":method", "POST", ":scheme", "http", ":authority", "t", ":path", "/up")
	tc.data(1, false, make([]byte, 1000))
	// PING on a stream is a connection error.
	tc.write(append(h2AppendFrameHeader(nil, h2FramePing, 0, 1, 8), "12345678"...))
	h2ExpectLinger(t, tc, H2ProtocolError)
}

// TestH2ServerLingersAfterRetiring checks the same of a connection retired
// gracefully, here for being idle, which the client may not have heard of
// before it sent more.
func TestH2ServerLingersAfterRetiring(t *testing.T) {
	config := DefaultConfig()
	config.IdleTimeout = 200 * time.Millisecond
	addr := serve(t, NewHandlerWithConfig(config, echoHandler()))
	tc := dialH2(t, addr)
	tc.headers(1, true, get("/")...)
	tc.response(1)
	h2ExpectLinger(t, tc, H2NoError)
}

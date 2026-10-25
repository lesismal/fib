//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"testing"
)

// h2ReadData reads until n bytes of DATA have arrived, across all streams,
// and returns how many came on each, in frames of how many bytes each, in
// the order they came.
func h2ReadData(tc *h2TestConn, n int) (map[uint32]int, []uint32) {
	tc.t.Helper()
	got := map[uint32]int{}
	var order []uint32
	for total := 0; total < n; {
		f := tc.read()
		if f.typ != h2FrameData {
			continue
		}
		got[f.streamID] += len(f.payload)
		order = append(order, f.streamID)
		total += len(f.payload)
	}
	return got, order
}

// TestH2ServerSharesTheConnectionWindowFairly checks that when the client's
// connection window lets less go than several responses have waiting, the
// streams take turns at it a frame at a time rather than the first of them
// taking all of it.
func TestH2ServerSharesTheConnectionWindowFairly(t *testing.T) {
	addr := serve(t, NewHandler(echoHandler()))
	// Large stream windows, so that only the connection's, which stays at
	// its default, holds the responses back.
	tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 1 << 20})
	ids := []uint32{1, 3, 5}
	for _, id := range ids {
		tc.headers(id, true, get("/?size=200000")...)
	}
	// All three answer: their headers go, and the connection's window to
	// whichever answered first.
	for headers := 0; headers < len(ids); {
		if f := tc.read(); f.typ == h2FrameHeaders {
			headers++
		}
	}
	sent := func() int {
		n := 0
		for _, id := range ids {
			n += len(tc.stream(id).body)
		}
		return n
	}
	for sent() < h2DefaultWindow {
		tc.read()
	}
	h2covSync(t, tc)
	if n := sent(); n != h2DefaultWindow {
		t.Fatalf("sent %d bytes into a connection window of %d", n, h2DefaultWindow)
	}

	// Three frames' worth of window: one frame for each stream.
	for round := 0; round < 3; round++ {
		tc.write(h2AppendWindowUpdate(nil, 0, 3*h2DefaultMaxFrameSize))
		got, order := h2ReadData(tc, 3*h2DefaultMaxFrameSize)
		for _, id := range ids {
			if got[id] != h2DefaultMaxFrameSize {
				t.Fatalf("round %d: streams got %v in frames from %v, want %d each", round, got, order, h2DefaultMaxFrameSize)
			}
		}
	}

	// Then the rest goes, and each stream's body is whole.
	tc.write(h2AppendWindowUpdate(nil, 0, 1<<20))
	for _, id := range ids {
		if _, body := tc.response(id); len(body) != 200000 {
			t.Fatalf("stream %d: body of %d bytes", id, len(body))
		}
	}
}

// TestH2ServerSkipsStreamsHeldByTheirOwnWindow checks that a stream its own
// window holds back does not keep the connection's window from the others,
// and goes on once its window opens.
func TestH2ServerSkipsStreamsHeldByTheirOwnWindow(t *testing.T) {
	addr := serve(t, NewHandler(echoHandler()))
	// Stream windows of 100 bytes; the connection's is larger than all the
	// responses together.
	tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 100})
	tc.write(h2AppendWindowUpdate(nil, 0, 1<<20))
	tc.headers(1, true, get("/?size=5000")...)
	tc.headers(3, true, get("/?size=5000")...)
	got, _ := h2ReadData(tc, 200)
	if got[1] != 100 || got[3] != 100 {
		t.Fatalf("first windows: %v", got)
	}
	// Stream 3's window opens; stream 1's stays shut.
	tc.write(h2AppendWindowUpdate(nil, 3, 10000))
	if _, body := tc.response(3); len(body) != 5000 {
		t.Fatalf("stream 3: body of %d bytes", len(body))
	}
	if n := len(tc.stream(1).body); n != 100 {
		t.Fatalf("stream 1 sent %d bytes into a window of 100", n)
	}
	tc.write(h2AppendWindowUpdate(nil, 1, 10000))
	if _, body := tc.response(1); len(body) != 5000 {
		t.Fatalf("stream 1: body of %d bytes", len(body))
	}
}

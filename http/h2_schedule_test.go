//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"testing"

	"github.com/lesismal/fib/hpack"
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

// TestH2PriorityWeight checks what send weight RFC 9218's priority header
// asks for: its urgency, 0 the most urgent, and the round-robin weight of 1
// for a request that sends no priority, or the default urgency, or anything
// this does not understand.
func TestH2PriorityWeight(t *testing.T) {
	cases := []struct {
		name   string
		values []string
		want   uint8
	}{
		{"none", nil, 1},
		{"urgent", []string{"u=0"}, 8},
		{"more urgent than default", []string{"u=1"}, 4},
		{"urgent still", []string{"u=2"}, 2},
		{"default urgency", []string{"u=3"}, 1},
		{"least urgent", []string{"u=7"}, 1},
		{"incremental alone", []string{"i"}, 1},
		{"incremental and urgent", []string{"u=0, i"}, 8},
		{"dictionary order", []string{"i=?1, u=1"}, 4},
		{"unknown member ignored", []string{"u=2, x=9"}, 2},
		{"invalid urgency ignored", []string{"u=8"}, 1},
		{"invalid urgency beside a good one", []string{"u=9, u=0"}, 8},
		{"last urgency wins", []string{"u=0", "u=7"}, 1},
		{"uppercase urgency ignored", []string{"U=0"}, 1},
		{"space around the equals sign ignored", []string{"u = 0"}, 1},
		{"empty", []string{""}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var fields []hpack.HeaderField
			for _, value := range tc.values {
				fields = append(fields, hpack.HeaderField{Name: "priority", Value: value})
			}
			if got := h2PriorityWeight(fields); got != tc.want {
				t.Fatalf("h2PriorityWeight(%q) = %d, want %d", tc.values, got, tc.want)
			}
		})
	}
	if got := h2PriorityWeight([]hpack.HeaderField{{Name: "x-priority", Value: "u=0"}}); got != 1 {
		t.Fatalf("a field other than priority gave weight %d", got)
	}
}

// TestH2ServerWeighsStreamsByThePriorityHeader checks that a stream whose
// request asks for a more urgent priority gets more of the connection's
// window than the others, while a stream with the default weight still gets
// served, and that every body arrives whole once the windows allow it.
func TestH2ServerWeighsStreamsByThePriorityHeader(t *testing.T) {
	addr := serve(t, NewHandler(echoHandler()))
	// Stream windows of 0 hold every response back until the test opens
	// them, so that all of them wait in the send queue at once.
	tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 0})
	ids := []uint32{1, 3, 5}
	priorities := []string{"u=0", "u=1", ""}
	for i, id := range ids {
		fields := get("/?size=2000000")
		if priorities[i] != "" {
			fields = append(fields, "priority", priorities[i])
		}
		tc.headers(id, true, fields...)
	}
	for headers := 0; headers < len(ids); {
		if f := tc.read(); f.typ == h2FrameHeaders {
			headers++
		}
	}
	// Stream 1's window opens first, so it spends the window the connection
	// opens with; then the other streams' windows open, and every response
	// waits for the connection's window.
	for _, id := range ids {
		tc.write(h2AppendWindowUpdate(nil, id, 1<<20))
	}
	h2ReadData(tc, h2DefaultWindow)

	// A window of 64 frames, enough for several turns each: urgency 0 sends
	// eight frames a turn, urgency 1 four, and the default one.
	const grant = 64 * h2DefaultMaxFrameSize
	tc.write(h2AppendWindowUpdate(nil, 0, grant))
	got, order := h2ReadData(tc, grant)
	if got[5] == 0 {
		t.Fatalf("stream 5 got nothing while the urgent streams sent: %v", order)
	}
	if !(got[1] > got[3] && got[3] > got[5]) {
		t.Fatalf("streams got %v bytes, want stream 1 > 3 > 5", got)
	}
	if got[1] < 4*got[5] || got[3] < 2*got[5] {
		t.Fatalf("streams got %v bytes, want the urgent streams the larger share", got)
	}

	// The rest of every body goes once its windows allow it.
	for _, id := range ids {
		tc.write(h2AppendWindowUpdate(nil, id, 4<<20))
	}
	tc.write(h2AppendWindowUpdate(nil, 0, 16<<20))
	for _, id := range ids {
		if _, body := tc.response(id); len(body) != 2000000 {
			t.Fatalf("stream %d: body of %d bytes", id, len(body))
		}
	}
}

// TestH2ServerWeighsStreamsAcrossSmallWindowUpdates checks that a weighted
// stream's turn survives the connection's window running out, so that a peer
// that grants the window a frame at a time still gives it its larger share:
// urgency 0 sends eight frames before the default weight's one, however
// little of the window comes at once.
func TestH2ServerWeighsStreamsAcrossSmallWindowUpdates(t *testing.T) {
	addr := serve(t, NewHandler(echoHandler()))
	// Stream windows of 0 hold both responses until the test opens them, so
	// that both wait in the send queue.
	tc := dialH2(t, addr, [2]uint32{uint32(h2SettingInitialWindowSize), 0})
	tc.headers(1, true, append(get("/?size=2000000"), "priority", "u=0")...)
	tc.headers(3, true, get("/?size=2000000")...)
	for headers := 0; headers < 2; {
		if f := tc.read(); f.typ == h2FrameHeaders {
			headers++
		}
	}
	// Stream 1's window opens first, so it spends the window the connection
	// opens with, partway through its turn of eight frames; then stream 3's
	// window opens.
	tc.write(h2AppendWindowUpdate(nil, 1, 4<<20))
	tc.write(h2AppendWindowUpdate(nil, 3, 4<<20))
	h2ReadData(tc, h2DefaultWindow)

	// The window comes a frame at a time, 90 frames of it: stream 1's turn
	// survives each time the window runs out, so it sends eight frames for
	// every one of stream 3's, the frames left of its first turn included,
	// which is 80 frames to 10.
	const rounds, frames = 10, 9
	var out []byte
	for i := 0; i < rounds*frames; i++ {
		out = h2AppendWindowUpdate(out, 0, h2DefaultMaxFrameSize)
	}
	tc.write(out)
	got, order := h2ReadData(tc, rounds*frames*h2DefaultMaxFrameSize)
	if got[1] != (rounds*frames-rounds)*h2DefaultMaxFrameSize || got[3] != rounds*h2DefaultMaxFrameSize {
		t.Fatalf("streams got %v bytes over %d frames from %v, want 8 frames of stream 1 to 1 of stream 3",
			got, rounds*frames, order)
	}

	// The rest of every body goes once its windows allow it.
	tc.write(h2AppendWindowUpdate(nil, 1, 4<<20))
	tc.write(h2AppendWindowUpdate(nil, 3, 4<<20))
	tc.write(h2AppendWindowUpdate(nil, 0, 16<<20))
	for _, id := range []uint32{1, 3} {
		if _, body := tc.response(id); len(body) != 2000000 {
			t.Fatalf("stream %d: body of %d bytes", id, len(body))
		}
	}
}

package srt

import (
	"testing"
	"time"
)

// What a peer sent before its SHUTDOWN is delivered, not dropped with the
// receive buffer for not having reached its TSBPD time: ffmpeg's encoder
// flushes its last 1.6 s of frames in a burst just before it closes.
func TestShutdownDeliversReceived(t *testing.T) {
	s := newSim(t, Config{}, Config{})
	s.connect()
	var first uint32
	firstSet := false
	s.drop = func(toServer bool, b []byte) bool {
		seq, ok := isData(b)
		if !ok || !toServer {
			return false
		}
		if !firstSet {
			first, firstSet = seq, true
		}
		return seq == first+3 // lost, and no time to ask for it again
	}
	for i := range 10 {
		if err := s.caller.conn.Write(payload(i, 1316), s.now); err != nil {
			t.Fatal(err)
		}
	}
	s.run(10 * time.Millisecond)
	if len(s.server.data) != 0 {
		t.Fatal("delivered before its time")
	}
	s.caller.conn.Close(s.now)
	s.run(10 * time.Millisecond)
	if s.server.err != ErrPeerClosed {
		t.Fatal(s.server.err)
	}
	checkData(t, s.server.data, 0, 10, map[int]bool{3: true})
}

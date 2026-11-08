package utp

import (
	"bytes"
	"io"
	"net/netip"
	"sync"
	"testing"
	"time"
)

// TestBurstLossKeepsRTT loses a long run of consecutive data packets, as
// Windows' loopback drops a burst its socket has no room for. They come
// back one per tail loss probe, and each that fills the hole slides the
// selective acknowledgement's 32 packets by one, acknowledging a packet
// that arrived long before. Taken as round-trip samples, those ages grew
// the RTT, and with it the probe's and the timeout's waits, until a
// transfer waited tens of seconds for each packet. An acknowledgement a
// retransmission brought about times nothing (Karn).
func TestBurstLossKeepsRTT(t *testing.T) {
	n := newSimNet(t, 14)
	n.delay = 2 * time.Millisecond
	a, b := n.node(t, 1, Config{}), n.node(t, 2, Config{})
	ca, cb := dialPair(t, a, b)
	var mu sync.Mutex
	seen := map[uint16]bool{}
	firsts := 0
	n.mu.Lock()
	n.drop = func(from, to netip.AddrPort, d []byte) bool {
		if from != a.addr || d[0]>>4 != stData || len(d) <= HeaderSize {
			return false
		}
		seq := uint16(d[16])<<8 | uint16(d[17])
		mu.Lock()
		defer mu.Unlock()
		if seen[seq] {
			return false // a retransmission
		}
		seen[seq] = true
		firsts++
		// Packets 300 to 419 are lost the first time they go.
		return firsts >= 300 && firsts < 420
	}
	n.mu.Unlock()
	data := randBytes(54, 4<<20)
	go func() {
		_, _ = ca.Write(data)
		ca.CloseWrite()
	}()
	start := time.Now()
	stop := make(chan struct{})
	var maxRTT, maxRTO int
	var watched sync.WaitGroup
	watched.Add(1)
	go func() {
		defer watched.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			ca.mu.Lock()
			maxRTT, maxRTO = max(maxRTT, ca.rtt), max(maxRTO, ca.rto)
			ca.mu.Unlock()
		}
	}()
	got, err := io.ReadAll(cb)
	close(stop)
	watched.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data differ")
	}
	rtt, rto := maxRTT, maxRTO
	took := time.Since(start)
	t.Logf("took %v, peak rtt %dms rto %dms %+v", took, rtt, rto, ca.Stats())
	// The path's round trip is 4ms; the minimum RTO is half a second.
	if rtt > 100 || rto > 1000 {
		t.Fatalf("rtt %dms, rto %dms after a burst loss", rtt, rto)
	}
	if took > 20*time.Second {
		t.Fatalf("took %v", took)
	}
}

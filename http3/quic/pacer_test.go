package quic

import (
	"bytes"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The pacing budget fills at 1.25 congestion windows a smoothed round trip,
// up to the larger of the initial window and two minPacingDelay of it, and a
// round that has spent it waits for a datagram's worth, minPacingDelay at
// least (RFC 9002 section 7.7).
func TestPacingRate(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if !c.paceAllows(now) || c.paceBudget != 0 {
		t.Fatalf("paced before a round-trip sample, budget %d", c.paceBudget)
	}
	// A second's round trip: 150KB a second, and a burst of the initial
	// window, which is more than two minPacingDelay of it.
	c.rtt.hasSample, c.rtt.smoothed = true, time.Second
	c.cc.cwnd = 100 * maxDatagram
	if got, want := c.pacingRate(), 1.25*100*maxDatagram; got != want {
		t.Fatalf("rate %v bytes a second, want %v", got, want)
	}
	if !c.paceAllows(now) || c.paceBudget != maxBurstPackets*maxDatagram {
		t.Fatalf("first budget %d, want the initial window", c.paceBudget)
	}
	c.paceBudget = 0
	if c.paceAllows(now) {
		t.Fatal("a spent budget allowed a datagram")
	}
	// 1200 bytes at 150KB a second take 8ms.
	if d, want := c.paceNext(now).Sub(now), max(8*time.Millisecond, minPacingDelay); d != want {
		t.Fatalf("next send in %v, want %v", d, want)
	}
	if c.paceAllows(now.Add(4 * time.Millisecond)) {
		t.Fatalf("a budget of %d allowed a datagram", c.paceBudget)
	}
	if !c.paceAllows(now.Add(10*time.Millisecond)) || c.paceBudget < 1499 || c.paceBudget > 1500 {
		t.Fatalf("budget %d 10ms on, want 1500", c.paceBudget)
	}
	if !c.paceAllows(now.Add(time.Hour)) || c.paceBudget != maxBurstPackets*maxDatagram {
		t.Fatalf("budget %d after an idle hour, want the burst", c.paceBudget)
	}
	// A round trip shorter than minPacingDelay paces as minPacingDelay, a
	// burst of 2.5 windows that holds nothing back; one of zero, which a
	// coarse clock measures on a fast path, too, rather than at an infinite
	// rate whose budget would wrap around and never send again.
	for _, rtt := range []time.Duration{minPacingDelay / 2, 0} {
		c.rtt.smoothed = rtt
		c.paceLast = time.Time{}
		if !c.paceAllows(now) || c.paceBudget != 250*maxDatagram {
			t.Fatalf("burst %d with a round trip of %v, want %d", c.paceBudget, rtt, 250*maxDatagram)
		}
		c.paceBudget = 0
		if !c.paceAllows(now.Add(time.Second)) || c.paceBudget != 250*maxDatagram {
			t.Fatalf("budget %d a second on with a round trip of %v", c.paceBudget, rtt)
		}
	}
}

// recordPC keeps what it is sent, refusing it with EAGAIN while refuse is
// above zero, and takes batches when counted is set.
type recordPC struct {
	mu        sync.Mutex
	datagrams [][]byte
	times     []time.Time
	refuse    int
	// batchRefuseAt, when not zero, has the next batch take that many
	// datagrams and refuse the rest.
	batchRefuseAt int
	batches       int
}

func (p *recordPC) Send(d []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.refuse > 0 {
		p.refuse--
		return syscall.EAGAIN
	}
	p.datagrams = append(p.datagrams, bytes.Clone(d))
	p.times = append(p.times, time.Now())
	return nil
}

func (p *recordPC) Close() error { return nil }

func (p *recordPC) sent() (n, size int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, d := range p.datagrams {
		size += len(d)
	}
	return len(p.datagrams), size
}

// countingPC is a recordPC that takes batches and says how much of each it
// took.
type countingPC struct{ recordPC }

func (p *countingPC) SendDatagrams(ds [][]byte) (int, error) {
	p.mu.Lock()
	p.batches++
	cut := p.batchRefuseAt
	p.batchRefuseAt = 0
	p.mu.Unlock()
	for i, d := range ds {
		if cut > 0 && i == cut {
			return i, syscall.EAGAIN
		}
		if err := p.Send(d); err != nil {
			return i, err
		}
	}
	return len(ds), nil
}

// A round of sending that spends the pacing budget stops there and leaves
// the rest to the timer, which sends it at the pacing rate.
func TestPacingSpreadsRound(t *testing.T) {
	c := frameConn(t)
	pc := &recordPC{}
	c.mu.Lock()
	c.pc = pc
	c.rtt.hasSample, c.rtt.smoothed, c.rtt.min = true, 200*time.Millisecond, 200*time.Millisecond
	c.cc.cwnd = 100_000
	c.mu.Unlock()
	s, err := c.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 60<<10)
	if err := s.Write(payload, true); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	burst := int(max(float64(maxBurstPackets*maxDatagram), c.pacingRate()*2*minPacingDelay.Seconds()))
	sendAt, timerAt := c.sendAt, c.timerAt
	c.mu.Unlock()
	n, size := pc.sent()
	if size > burst+maxDatagram || n == 0 {
		t.Fatalf("the first round sent %d bytes in %d datagrams, past a burst of %d", size, n, burst)
	}
	if sendAt.IsZero() || timerAt.IsZero() || timerAt.After(sendAt) {
		t.Fatalf("paced round left sendAt %v, timer %v", sendAt, timerAt)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, size := pc.sent(); size >= len(payload) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d bytes sent", size, len(payload))
		}
		time.Sleep(time.Millisecond)
	}
	pc.mu.Lock()
	spread := pc.times[len(pc.times)-1].Sub(pc.times[0])
	pc.mu.Unlock()
	// At 625KB a second, the 40KB or more past a burst of 12KB (20KB on
	// Windows) take 65ms or more.
	if spread < minPacingDelay {
		t.Fatalf("60KiB left within %v", spread)
	}
}

// Datagrams a full socket refuses, with EAGAIN, are sent again once the
// timer says to, in order and none twice, rather than lost: one at a time
// and through a path that takes batches and says how many it took.
func TestBlockedSendResumes(t *testing.T) {
	for _, batched := range []bool{false, true} {
		c := frameConn(t)
		var pc *recordPC
		c.mu.Lock()
		if batched {
			cpc := &countingPC{}
			cpc.batchRefuseAt = 3
			c.pc, pc = cpc, &cpc.recordPC
		} else {
			pc = &recordPC{refuse: 2}
			c.pc = pc
		}
		c.mu.Unlock()
		s, err := c.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Write(make([]byte, 8000), true); err != nil {
			t.Fatal(err)
		}
		c.mu.Lock()
		blocked, sendAt := c.blocked != nil, c.sendAt
		built := c.spaces[spaceApp].nextPN
		c.mu.Unlock()
		if !blocked || sendAt.IsZero() {
			t.Fatalf("batched %v: a refused round kept nothing (sendAt %v)", batched, sendAt)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			if n, _ := pc.sent(); uint64(n) >= built {
				break
			}
			if time.Now().After(deadline) {
				n, _ := pc.sent()
				t.Fatalf("batched %v: %d datagrams arrived of %d built", batched, n, built)
			}
			time.Sleep(time.Millisecond)
		}
		pc.mu.Lock()
		got := pc.datagrams
		pc.mu.Unlock()
		seen := make(map[string]bool)
		for i, d := range got {
			if seen[string(d)] {
				t.Fatalf("batched %v: datagram %d sent twice", batched, i)
			}
			seen[string(d)] = true
		}
		if batched {
			pc.mu.Lock()
			batches := pc.batches
			pc.mu.Unlock()
			if batches < 2 {
				t.Fatalf("the refused part of a batch went %d times", batches)
			}
		}
	}
}

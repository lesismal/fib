package quic

import (
	"bytes"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The pacing budget fills at 1.25 congestion windows a smoothed round trip,
// up to the larger of the initial window and two milliseconds of it, and a
// round that has spent it waits for a datagram's worth, a millisecond at
// least (RFC 9002 section 7.7).
func TestPacingRate(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if !c.paceAllows(now) || c.paceBudget != 0 {
		t.Fatalf("paced before a round-trip sample, budget %d", c.paceBudget)
	}
	c.rtt.hasSample, c.rtt.smoothed = true, 100*time.Millisecond
	c.cc.cwnd = 100 * maxDatagram
	if got, want := c.pacingRate(), 1.25*100*maxDatagram/0.1; got != want {
		t.Fatalf("rate %v bytes a second, want %v", got, want)
	}
	if !c.paceAllows(now) || c.paceBudget != maxBurstPackets*maxDatagram {
		t.Fatalf("first budget %d, want the initial window", c.paceBudget)
	}
	c.paceBudget = 0
	if c.paceAllows(now) {
		t.Fatal("a spent budget allowed a datagram")
	}
	// 1200 bytes at 1.5 MB a second take 800µs, which waits a millisecond.
	if d := c.paceNext(now).Sub(now); d != minPacingDelay {
		t.Fatalf("next send in %v", d)
	}
	if c.paceAllows(now.Add(400 * time.Microsecond)) {
		t.Fatalf("a budget of %d allowed a datagram", c.paceBudget)
	}
	if !c.paceAllows(now.Add(time.Millisecond)) || c.paceBudget != 1500 {
		t.Fatalf("budget %d a millisecond later, want 1500", c.paceBudget)
	}
	if !c.paceAllows(now.Add(time.Hour)) || c.paceBudget != maxBurstPackets*maxDatagram {
		t.Fatalf("budget %d after an idle hour, want the burst", c.paceBudget)
	}
	// A fast path's burst is two milliseconds of its rate.
	c.rtt.smoothed = time.Millisecond
	c.paceLast = time.Time{}
	if !c.paceAllows(now) || c.paceBudget != int(2*c.pacingRate()/1000) {
		t.Fatalf("burst %d on a fast path, want %v", c.paceBudget, 2*c.pacingRate()/1000)
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
	c.rtt.hasSample, c.rtt.smoothed, c.rtt.min = true, 20*time.Millisecond, 20*time.Millisecond
	c.cc.cwnd = 200_000
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
	// At 12.5MB a second, the 35KB past a burst of 25KB take almost three
	// milliseconds, and the timer waits a millisecond at least.
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

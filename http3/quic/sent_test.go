package quic

import (
	"testing"
	"time"
)

// The queue keeps packets by number across wrapping and growing, with holes
// for those taken and for numbers never pushed, and drops the slots before
// the first packet still waiting.
func TestSentQueue(t *testing.T) {
	var q sentQueue
	pn := uint64(100)
	push := func(n int) {
		for ; n > 0; n-- {
			q.push(&sentPacket{pn: pn})
			pn++
		}
	}
	push(10)
	for i := uint64(100); i < 105; i++ {
		if p := q.take(i); p == nil || p.pn != i {
			t.Fatalf("took %v for %d", p, i)
		}
	}
	q.trim()
	if q.base != 105 || q.len() != 5 || q.n != 5 {
		t.Fatalf("base %d, %d packets in %d slots", q.base, q.len(), q.n)
	}
	// Past the end of the ring and into a bigger one, with a hole in the
	// middle that keeps the slots after it.
	push(40)
	if q.take(107) == nil || q.take(107) != nil || q.take(99) != nil || q.take(pn) != nil {
		t.Fatal("took a packet twice, or one the queue never had")
	}
	q.trim()
	if q.base != 105 || q.len() != 44 {
		t.Fatalf("base %d with %d packets", q.base, q.len())
	}
	var seen []uint64
	q.each(110, func(p *sentPacket) bool { seen = append(seen, p.pn); return true })
	if len(seen) != 4 || seen[0] != 105 || seen[1] != 106 || seen[2] != 108 || seen[3] != 109 {
		t.Fatalf("packets below 110: %v", seen)
	}
	// A number skipped leaves a hole.
	pn += 3
	push(1)
	if q.end() != pn || q.take(pn-2) != nil || q.take(pn-1) == nil {
		t.Fatalf("end %d after a gap", q.end())
	}
	for i := q.base; i < q.end(); i++ {
		q.take(i)
	}
	q.trim()
	if q.len() != 0 || q.n != 0 {
		t.Fatalf("%d packets in %d slots once all are taken", q.len(), q.n)
	}
	// An empty queue starts again at whatever number comes next.
	q.push(&sentPacket{pn: 5000})
	if q.base != 5000 || q.take(5000) == nil {
		t.Fatalf("restarted at %d", q.base)
	}
}

// An acknowledgement looks only at the slots of packets still waiting, so
// a frame that acknowledges again what it did before, and a new packet
// beside it, finds the new one at once however many came before; loss
// detection looks only up to the largest acknowledged.
func TestAckProportional(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	s := &c.spaces[spaceApp]
	now := time.Now()
	for i := 0; i < 1000; i++ {
		c.onPacketSent(spaceApp, &sentPacket{pn: s.nextPN, time: now, size: 100, inFlight: true, ackEliciting: true})
		s.nextPN++
	}
	if err := c.onAckReceived(spaceApp, []pnRange{{lo: 0, hi: 899}}, 0, now); err != nil {
		t.Fatal(err)
	}
	if s.sent.base != 900 || s.sent.len() != 100 || s.ackElicitingInFlight != 100 {
		t.Fatalf("base %d, %d waiting, %d ack-eliciting", s.sent.base, s.sent.len(), s.ackElicitingInFlight)
	}
	// The whole history again, and one more.
	if err := c.onAckReceived(spaceApp, []pnRange{{lo: 950, hi: 950}, {lo: 0, hi: 899}}, 0, now); err != nil {
		t.Fatal(err)
	}
	// 950 acknowledged leaves 900 to 947 lost by count; 948 and 949 wait
	// on time.
	if s.sent.len() != 51 || s.sent.base != 948 || s.lossTime.IsZero() {
		t.Fatalf("base %d, %d waiting, loss time %v", s.sent.base, s.sent.len(), s.lossTime)
	}
}

// The timer is set again only for a deadline sooner than the one it is set
// for, or once it has fired; one that fires early finds nothing due and is
// set for what is.
func TestTimerArmsOnlySooner(t *testing.T) {
	c := frameConn(t)
	c.mu.Lock()
	now := time.Now()
	// The idle timeout, 30 seconds on, is what the timer waits for when
	// nothing else is due sooner.
	idle := c.idleDeadline()
	c.lossDeadline = now.Add(time.Hour)
	c.armTimerLocked(now)
	if !c.timerAt.Equal(idle) {
		t.Fatalf("the timer is set for %v, not the idle timeout", c.timerAt)
	}
	c.lossDeadline = now.Add(20 * time.Second)
	c.armTimerLocked(now)
	at := c.lossDeadline
	if !c.timerAt.Equal(at) {
		t.Fatalf("a sooner deadline left the timer at %v", c.timerAt)
	}
	c.lossDeadline = now.Add(25 * time.Second)
	c.armTimerLocked(now)
	if !c.timerAt.Equal(at) {
		t.Fatalf("a later deadline moved the timer to %v", c.timerAt)
	}
	// Moved later once it is set for 10ms: it fires then, early, and is
	// set for the deadline as it is.
	c.lossDeadline = now.Add(10 * time.Millisecond)
	c.armTimerLocked(now)
	c.lossDeadline = time.Time{}
	c.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		at := c.timerAt
		c.mu.Unlock()
		if at.Equal(idle) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the timer is set for %v", at)
		}
		time.Sleep(time.Millisecond)
	}
}

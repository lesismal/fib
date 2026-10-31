package sctp

import (
	"math/rand/v2"
	"testing"
	"time"
)

// testLink wires two associations together in memory under a fake clock.
// Packets can be lost, duplicated, delayed and so reordered, all from a
// seeded generator so a failure repeats.
type testLink struct {
	t       testing.TB
	now     time.Time
	rng     *rand.Rand
	loss    float64
	dup     float64
	jitter  time.Duration
	delay   time.Duration
	q       pktHeap
	seq     int
	ends    [2]*Association
	sent    [2]int
	drop    func(from int, p []byte) bool
	closeEr [2]error
	closed  [2]bool
	free    [][]byte
}

type pkt struct {
	at  time.Time
	seq int
	to  int
	b   []byte
}

// pktHeap is a binary min-heap by arrival time, then send order, kept
// by hand so pushing a packet does not box it.
type pktHeap []pkt

func (h pktHeap) less(i, j int) bool {
	if !h[i].at.Equal(h[j].at) {
		return h[i].at.Before(h[j].at)
	}
	return h[i].seq < h[j].seq
}

func (h *pktHeap) push(p pkt) {
	*h = append(*h, p)
	q := *h
	for i := len(q) - 1; i > 0; {
		parent := (i - 1) / 2
		if !q.less(i, parent) {
			break
		}
		q[i], q[parent] = q[parent], q[i]
		i = parent
	}
}

func (h *pktHeap) pop() pkt {
	q := *h
	p := q[0]
	n := len(q) - 1
	q[0] = q[n]
	q = q[:n]
	for i := 0; ; {
		m, l, r := i, 2*i+1, 2*i+2
		if l < n && q.less(l, m) {
			m = l
		}
		if r < n && q.less(r, m) {
			m = r
		}
		if m == i {
			break
		}
		q[i], q[m] = q[m], q[i]
		i = m
	}
	*h = q
	return p
}

// newLink makes a client (end 0, DTLS client) and server (end 1).
func newLink(t testing.TB, seed uint64, configure func(i int, c *Config)) *testLink {
	l := &testLink{
		t:     t,
		now:   time.Unix(1_700_000_000, 0),
		rng:   rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		delay: 10 * time.Millisecond,
	}
	for i := range 2 {
		i := i
		c := Config{
			Client: i == 0,
			Now:    func() time.Time { return l.now },
			Output: func(p []byte) { l.output(i, p) },
			OnClose: func(err error) {
				l.closed[i] = true
				l.closeEr[i] = err
			},
		}
		if configure != nil {
			configure(i, &c)
		}
		l.ends[i] = NewAssociation(c)
	}
	return l
}

func (l *testLink) output(from int, p []byte) {
	l.sent[from]++
	if l.drop != nil && l.drop(from, p) {
		return
	}
	if l.loss > 0 && l.rng.Float64() < l.loss {
		return
	}
	n := 1
	if l.dup > 0 && l.rng.Float64() < l.dup {
		n = 2
	}
	for range n {
		at := l.now.Add(l.delay)
		if l.jitter > 0 {
			at = at.Add(time.Duration(l.rng.Int64N(int64(l.jitter))))
		}
		l.seq++
		var b []byte
		if n := len(l.free); n > 0 {
			b = l.free[n-1][:0]
			l.free = l.free[:n-1]
		}
		l.q.push(pkt{at: at, seq: l.seq, to: 1 - from, b: append(b, p...)})
	}
}

// run moves time on, delivering packets and running timers, until done
// reports true or limit passes; it reports whether done did.
func (l *testLink) run(limit time.Duration, done func() bool) bool {
	end := l.now.Add(limit)
	for {
		if done != nil && done() {
			return true
		}
		next := end
		if len(l.q) > 0 && l.q[0].at.Before(next) {
			next = l.q[0].at
		}
		for _, a := range l.ends {
			if t, ok := a.NextTimeout(); ok && t.Before(next) {
				next = t
			}
		}
		if next.After(l.now) {
			l.now = next
		}
		if !l.now.Before(end) && (len(l.q) == 0 || l.q[0].at.After(l.now)) {
			return done != nil && done()
		}
		for len(l.q) > 0 && !l.q[0].at.After(l.now) {
			p := l.q.pop()
			_ = l.ends[p.to].HandlePacket(p.b, l.now)
			l.free = append(l.free, p.b)
		}
		for _, a := range l.ends {
			if t, ok := a.NextTimeout(); ok && !t.After(l.now) {
				a.HandleTimeout(l.now)
			}
		}
	}
}

func (l *testLink) established() bool {
	return l.ends[0].State() == StateEstablished && l.ends[1].State() == StateEstablished
}

// connect brings the pair up, the client connecting, or both when both is
// set.
func (l *testLink) connect(both bool) {
	l.t.Helper()
	l.ends[0].Connect(l.now)
	if both {
		l.ends[1].Connect(l.now)
	}
	if !l.run(time.Minute, l.established) {
		l.t.Fatalf("not established: %v %v", l.ends[0].State(), l.ends[1].State())
	}
}

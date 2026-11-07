package utp

import (
	"container/heap"
	"math/rand/v2"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"
)

// simNet is an in-memory network between Muxes that loses, duplicates,
// delays and so reorders datagrams. Each node reads what is due in a
// burst, then flushes, as a fib Socket does.
type simNet struct {
	mu      sync.Mutex
	rng     *rand.Rand
	loss    float64
	dup     float64
	delay   time.Duration
	jitter  time.Duration
	drop    func(from, to netip.AddrPort, b []byte) bool
	nodes   map[netip.AddrPort]*simNode
	blocked bool
}

type simPkt struct {
	at   time.Time
	n    int
	from netip.AddrPort
	b    []byte
}

type pktHeap []simPkt

func (h pktHeap) Len() int { return len(h) }
func (h pktHeap) Less(i, j int) bool {
	if h[i].at.Equal(h[j].at) {
		return h[i].n < h[j].n
	}
	return h[i].at.Before(h[j].at)
}
func (h pktHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *pktHeap) Push(x any)   { *h = append(*h, x.(simPkt)) }
func (h *pktHeap) Pop() any {
	old := *h
	p := old[len(old)-1]
	*h = old[:len(old)-1]
	return p
}

type simNode struct {
	net   *simNet
	addr  netip.AddrPort
	mux   *Mux
	mu    sync.Mutex
	queue pktHeap
	seq   int
	wake  chan struct{}
	done  chan struct{}
}

func newSimNet(t *testing.T, seed uint64) *simNet {
	n := &simNet{rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)), nodes: map[netip.AddrPort]*simNode{}}
	return n
}

// node adds a Mux at 10.0.0.<i>:6881.
func (n *simNet) node(t *testing.T, i int, cfg Config) *simNode {
	t.Helper()
	addr := netip.AddrPortFrom(netip.AddrFrom4([4]byte{10, 0, 0, byte(i)}), 6881)
	nd := &simNode{net: n, addr: addr, wake: make(chan struct{}, 1), done: make(chan struct{})}
	nd.mux = NewMux(cfg, net.UDPAddrFromAddrPort(addr), nd.send)
	n.mu.Lock()
	n.nodes[addr] = nd
	n.mu.Unlock()
	go nd.run()
	t.Cleanup(func() {
		_ = nd.mux.Close()
		close(nd.done)
	})
	return nd
}

func (nd *simNode) send(to netip.AddrPort, dgrams [][]byte) {
	n := nd.net
	n.mu.Lock()
	defer n.mu.Unlock()
	dst := n.nodes[to]
	if dst == nil || n.blocked {
		return
	}
	for _, d := range dgrams {
		if n.drop != nil && n.drop(nd.addr, to, d) {
			continue
		}
		if n.loss > 0 && n.rng.Float64() < n.loss {
			continue
		}
		copies := 1
		if n.dup > 0 && n.rng.Float64() < n.dup {
			copies = 2
		}
		for range copies {
			at := time.Now().Add(n.delay)
			if n.jitter > 0 {
				at = at.Add(time.Duration(n.rng.Int64N(int64(n.jitter))))
			}
			dst.push(simPkt{at: at, from: nd.addr, b: append([]byte(nil), d...)})
		}
	}
}

func (nd *simNode) push(p simPkt) {
	nd.mu.Lock()
	nd.seq++
	p.n = nd.seq
	heap.Push(&nd.queue, p)
	nd.mu.Unlock()
	select {
	case nd.wake <- struct{}{}:
	default:
	}
}

func (nd *simNode) run() {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		now := time.Now()
		var due []simPkt
		wait := time.Hour
		nd.mu.Lock()
		for nd.queue.Len() > 0 && !nd.queue[0].at.After(now) && len(due) < 64 {
			due = append(due, heap.Pop(&nd.queue).(simPkt))
		}
		if nd.queue.Len() > 0 {
			wait = nd.queue[0].at.Sub(now)
		}
		nd.mu.Unlock()
		if len(due) > 0 {
			for _, p := range due {
				nd.mux.HandleDatagram(p.from, p.b)
			}
			nd.mux.Flush()
			continue
		}
		timer.Reset(wait)
		select {
		case <-nd.wake:
		case <-timer.C:
		case <-nd.done:
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}

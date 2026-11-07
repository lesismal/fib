package utp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func randBytes(seed uint64, n int) []byte {
	r := rand.New(rand.NewPCG(seed, 7))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

// dialPair connects a to b and returns both ends.
func dialPair(t *testing.T, a, b *simNode) (*Conn, *Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	type res struct {
		c   *Conn
		err error
	}
	acc := make(chan res, 1)
	go func() {
		c, err := b.mux.AcceptUTP()
		acc <- res{c, err}
	}()
	ca, err := a.mux.Dial(ctx, b.addr)
	if err != nil {
		t.Fatal(err)
	}
	r := <-acc
	if r.err != nil {
		t.Fatal(r.err)
	}
	return ca, r.c
}

// exchange sends n bytes each way at once, half-closing after, and checks
// what each end reads to EOF.
func exchange(t *testing.T, ca, cb *Conn, n int, seed uint64) {
	t.Helper()
	da, db := randBytes(seed, n), randBytes(seed+1, n)
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	send := func(c *Conn, d []byte) {
		defer wg.Done()
		for off := 0; off < len(d); {
			k := min(len(d)-off, 1+int(rand.Uint32N(40000)))
			if _, err := c.Write(d[off : off+k]); err != nil {
				errs <- err
				return
			}
			off += k
		}
		if err := c.CloseWrite(); err != nil {
			errs <- err
		}
	}
	recv := func(c *Conn, want []byte) {
		defer wg.Done()
		got, err := io.ReadAll(c)
		if err != nil {
			errs <- err
			return
		}
		if sha256.Sum256(got) != sha256.Sum256(want) {
			errs <- errors.New("data differ")
		}
	}
	wg.Add(4)
	go send(ca, da)
	go send(cb, db)
	go recv(ca, db)
	go recv(cb, da)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(90 * time.Second):
		t.Fatalf("exchange stuck: a %+v b %+v", ca.Stats(), cb.Stats())
	}
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestBulkBothDirections(t *testing.T) {
	for _, tc := range []struct {
		name          string
		loss, dup     float64
		delay, jitter time.Duration
		size          int
	}{
		{"clean", 0, 0, 0, 0, 4 << 20},
		{"delay", 0, 0, 5 * time.Millisecond, 0, 2 << 20},
		{"lossy", 0.05, 0.02, time.Millisecond, 4 * time.Millisecond, 2 << 20},
		{"reorder", 0, 0.05, 0, 3 * time.Millisecond, 2 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := newSimNet(t, 1)
			n.loss, n.dup, n.delay, n.jitter = tc.loss, tc.dup, tc.delay, tc.jitter
			a, b := n.node(t, 1, Config{}), n.node(t, 2, Config{})
			ca, cb := dialPair(t, a, b)
			start := time.Now()
			exchange(t, ca, cb, tc.size, 10)
			t.Logf("%v: a %+v b %+v rtt %v", time.Since(start), ca.Stats(), cb.Stats(), ca.RTT())
			ca.Close()
			cb.Close()
			waitGone(t, a.mux, b.mux)
		})
	}
}

// waitGone waits for the Muxes to have no connections.
func waitGone(t *testing.T, ms ...*Mux) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for _, m := range ms {
		for m.Len() > 0 {
			if time.Now().After(deadline) {
				t.Fatalf("%d connections left", m.Len())
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestConcurrentConns(t *testing.T) {
	n := newSimNet(t, 2)
	n.loss, n.jitter = 0.02, 2*time.Millisecond
	a, b := n.node(t, 1, Config{}), n.node(t, 2, Config{})
	var wg sync.WaitGroup
	for i := range 8 {
		// Dialed one at a time, so that each accepted end is the dialed
		// one's; they then all run at once.
		ca, cb := dialPair(t, a, b)
		wg.Add(1)
		go func() {
			defer wg.Done()
			exchange(t, ca, cb, 256<<10, uint64(100+2*i))
			ca.Close()
			cb.Close()
		}()
	}
	wg.Wait()
	waitGone(t, a.mux, b.mux)
}

func TestSeqWrap(t *testing.T) {
	n := newSimNet(t, 3)
	n.loss, n.jitter = 0.03, 2*time.Millisecond
	a, b := n.node(t, 1, Config{}), n.node(t, 2, Config{})
	a.mux.randSeq = func() uint16 { return 65500 }
	b.mux.randSeq = func() uint16 { return 65530 }
	ca, cb := dialPair(t, a, b)
	exchange(t, ca, cb, 1<<20, 20)
}

func TestCloseDeliversThenEOF(t *testing.T) {
	n := newSimNet(t, 4)
	n.loss, n.jitter = 0.05, 2*time.Millisecond
	a, b := n.node(t, 1, Config{}), n.node(t, 2, Config{})
	ca, cb := dialPair(t, a, b)
	data := randBytes(30, 1<<20)
	go func() {
		_, _ = ca.Write(data)
		ca.Close()
	}()
	got, err := io.ReadAll(cb)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read %d bytes, differ", len(got))
	}
	if _, err := ca.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("write after close: %v", err)
	}
	cb.Close()
	waitGone(t, a.mux, b.mux)
}

func TestResetOnMuxClose(t *testing.T) {
	n := newSimNet(t, 5)
	a, b := n.node(t, 1, Config{}), n.node(t, 2, Config{})
	ca, cb := dialPair(t, a, b)
	if _, err := ca.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(cb, buf); err != nil {
		t.Fatal(err)
	}
	_ = a.mux.Close()
	if _, err := cb.Read(buf); !errors.Is(err, ErrReset) {
		t.Fatalf("read after the peer's reset: %v", err)
	}
	if _, err := ca.Read(buf); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("read after close: %v", err)
	}
}

func TestResetForUnknownConn(t *testing.T) {
	n := newSimNet(t, 6)
	a, b := n.node(t, 1, Config{}), n.node(t, 2, Config{})
	ca, cb := dialPair(t, a, b)
	// b forgets the connection without a word; a's next packet gets a
	// RESET.
	cb.mu.Lock()
	cb.destroy(nil, micros())
	cb.unlock()
	waitGone(t, b.mux)
	if _, err := ca.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	_ = ca.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := ca.Read(make([]byte, 1)); !errors.Is(err, ErrReset) {
		t.Fatalf("read: %v", err)
	}
}

func TestDialTimeout(t *testing.T) {
	n := newSimNet(t, 7)
	a := n.node(t, 1, Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := a.mux.Dial(ctx, netip.MustParseAddrPort("10.0.0.9:6881"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("dial: %v", err)
	}
	waitGone(t, a.mux)
}

func TestWindowStallAndRecovery(t *testing.T) {
	n := newSimNet(t, 8)
	n.delay = time.Millisecond
	a := n.node(t, 1, Config{SendBuffer: 32 << 10})
	b := n.node(t, 2, Config{RecvBuffer: 16 << 10})
	ca, cb := dialPair(t, a, b)
	data := randBytes(40, 1<<20)
	var written atomic.Int64
	go func() {
		for off := 0; off < len(data); off += 4096 {
			if _, err := ca.Write(data[off : off+4096]); err != nil {
				return
			}
			written.Store(int64(off + 4096))
		}
		ca.CloseWrite()
	}()
	time.Sleep(500 * time.Millisecond)
	w := written.Load()
	cb.mu.Lock()
	buffered := cb.rbufLen() + cb.reorderBytes
	cb.mu.Unlock()
	if buffered > 16<<10+cb.slack() {
		t.Fatalf("receiver buffered %d past its window", buffered)
	}
	if w > 16<<10+32<<10+64<<10 {
		t.Fatalf("writer got %d bytes out to a reader that reads nothing", w)
	}
	got, err := io.ReadAll(cb)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data differ")
	}
	t.Logf("stalled at %d written; a %+v", w, ca.Stats())
}

func TestFastRetransmitOnSACK(t *testing.T) {
	n := newSimNet(t, 9)
	n.delay = 2 * time.Millisecond
	a, b := n.node(t, 1, Config{}), n.node(t, 2, Config{})
	ca, cb := dialPair(t, a, b)
	var dataSeen atomic.Int32
	n.mu.Lock()
	n.drop = func(from, to netip.AddrPort, d []byte) bool {
		// Drop the 20th data packet from a, once.
		if from == a.addr && d[0]>>4 == stData && len(d) > HeaderSize {
			return dataSeen.Add(1) == 20
		}
		return false
	}
	n.mu.Unlock()
	data := randBytes(50, 512<<10)
	go func() {
		_, _ = ca.Write(data)
		ca.CloseWrite()
	}()
	got, err := io.ReadAll(cb)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data differ")
	}
	st := ca.Stats()
	if st.FastRetransmits == 0 {
		t.Fatalf("no fast retransmit: %+v", st)
	}
	t.Logf("%+v", st)
}

func TestKeepAliveAndIdle(t *testing.T) {
	n := newSimNet(t, 10)
	cfg := Config{KeepAlive: 50 * time.Millisecond, IdleTimeout: 300 * time.Millisecond}
	a, b := n.node(t, 1, cfg), n.node(t, 2, cfg)
	ca, cb := dialPair(t, a, b)
	time.Sleep(time.Second)
	for _, c := range []*Conn{ca, cb} {
		c.mu.Lock()
		st := c.state
		c.mu.Unlock()
		if st != stateConnected {
			t.Fatal("keepalives did not keep the connection")
		}
	}
	n.mu.Lock()
	n.blocked = true
	n.mu.Unlock()
	_ = ca.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := ca.Read(make([]byte, 1)); !errors.Is(err, ErrIdle) {
		t.Fatalf("read: %v", err)
	}
}

func TestDeadlines(t *testing.T) {
	n := newSimNet(t, 11)
	a, b := n.node(t, 1, Config{SendBuffer: 4096}), n.node(t, 2, Config{RecvBuffer: 4096})
	ca, _ := dialPair(t, a, b)
	_ = ca.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	if _, err := ca.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read: %v", err)
	}
	_ = ca.SetWriteDeadline(time.Now().Add(200 * time.Millisecond))
	_, err := ca.Write(make([]byte, 1<<20))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("write: %v", err)
	}
}

func TestLostHandshake(t *testing.T) {
	n := newSimNet(t, 12)
	a, b := n.node(t, 1, Config{}), n.node(t, 2, Config{})
	var states atomic.Int32
	n.mu.Lock()
	n.drop = func(from, to netip.AddrPort, d []byte) bool {
		// The first answer to the SYN is lost; the acceptor's data,
		// sent before it hears from the initiator, must not complete
		// the handshake by itself either.
		return from == b.addr && d[0]>>4 == stState && states.Add(1) == 1
	}
	n.mu.Unlock()
	acc := make(chan *Conn, 1)
	go func() {
		c, err := b.mux.AcceptUTP()
		if err == nil {
			_, _ = c.Write([]byte("server first"))
		}
		acc <- c
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ca, err := a.mux.Dial(ctx, b.addr)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 12)
	if _, err := io.ReadFull(ca, buf); err != nil || string(buf) != "server first" {
		t.Fatalf("read %q: %v", buf, err)
	}
	<-acc
}

// TestSynAnswersLost loses the answers to the first two SYNs. libutp
// answers no SYN again whose connection it has, so the third goes under
// another ID, and the half-open connection of the first is reset.
func TestSynAnswersLost(t *testing.T) {
	n := newSimNet(t, 13)
	a, b := n.node(t, 1, Config{}), n.node(t, 2, Config{})
	var answers atomic.Int32
	n.mu.Lock()
	n.drop = func(from, to netip.AddrPort, d []byte) bool {
		return from == b.addr && d[0]>>4 == stState && answers.Add(1) <= 2
	}
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ca, err := a.mux.Dial(ctx, b.addr)
	if err != nil {
		t.Fatal(err)
	}
	orphan, err := b.mux.AcceptUTP()
	if err != nil {
		t.Fatal(err)
	}
	cb, err := b.mux.AcceptUTP()
	if err != nil {
		t.Fatal(err)
	}
	_ = orphan.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := orphan.Read(make([]byte, 1)); !errors.Is(err, ErrReset) {
		t.Fatalf("the half-open connection read %v", err)
	}
	exchange(t, ca, cb, 64<<10, 80)
}

package kcp

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
)

// simLink carries the packets of two KCPs on a virtual clock, losing,
// delaying and reordering them.
type simLink struct {
	rnd    *rand.Rand
	loss   float64
	delay  uint32
	jitter uint32
	now    uint32
	queue  []simPacket
	sent   int
}

type simPacket struct {
	at   uint32
	to   int
	data []byte
}

func (l *simLink) output(to int) func([]byte) {
	return func(p []byte) {
		l.sent++
		if l.rnd.Float64() < l.loss {
			return
		}
		at := l.now + l.delay
		if l.jitter > 0 {
			at += uint32(l.rnd.IntN(int(l.jitter)))
		}
		l.queue = append(l.queue, simPacket{at: at, to: to, data: append([]byte(nil), p...)})
	}
}

// step advances the clock by 1 ms, delivering what is due and flushing
// what the KCPs have due.
func (l *simLink) step(t *testing.T, ks [2]*KCP) {
	l.now++
	rest := l.queue[:0]
	var due []simPacket
	for _, p := range l.queue {
		if timediff(l.now, p.at) >= 0 {
			due = append(due, p)
		} else {
			rest = append(rest, p)
		}
	}
	l.queue = rest
	for _, p := range due {
		if err := ks[p.to].Input(p.data, l.now); err != nil {
			t.Fatalf("input: %v", err)
		}
	}
	for _, k := range ks {
		// Acknowledge at once, as a session does after each burst.
		if at, ok := k.NextFlush(l.now); ok && timediff(at, l.now) <= 0 {
			k.Flush(l.now)
		}
	}
}

func message(i int, rnd *rand.Rand, max int) []byte {
	n := 1 + rnd.IntN(max)
	b := make([]byte, n)
	for j := range b {
		b[j] = byte(i*31 + j)
	}
	return b
}

func transfer(t *testing.T, loss float64, jitter uint32, tune func(k *KCP), msgs, maxSize int) (*simLink, [2]*KCP) {
	t.Helper()
	l := &simLink{rnd: rand.New(rand.NewPCG(1, uint64(loss*1000)+uint64(jitter))), loss: loss, delay: 20, jitter: jitter, now: 0xffffff00}
	ks := [2]*KCP{New(7, l.output(1)), New(7, l.output(0))}
	for _, k := range ks {
		if tune != nil {
			tune(k)
		}
	}
	stream := ks[0].stream
	rnd := rand.New(rand.NewPCG(9, 9))
	var want [2][][]byte
	var all [2][]byte
	for i := 0; i < msgs; i++ {
		for side := range 2 {
			m := message(i+7*side, rnd, maxSize)
			want[side] = append(want[side], m)
			all[side] = append(all[side], m...)
		}
	}
	next := [2]int{}
	got := [2]int{}
	var stream2 [2][]byte
	buf := make([]byte, 255*DefaultMTU)
	done := func() bool {
		if stream {
			return len(stream2[0]) >= len(all[1]) && len(stream2[1]) >= len(all[0])
		}
		return got[0] >= msgs && got[1] >= msgs
	}
	for step := 0; !done(); step++ {
		if step > 3_000_000 {
			t.Fatalf("stuck after %d ms: got %v of %d", step, got, msgs)
		}
		for side, k := range ks {
			// Keep a window's worth queued.
			for next[side] < msgs && k.WaitSend() < 2*k.SendWindow() {
				if err := k.Send(want[side][next[side]]); err != nil {
					t.Fatal(err)
				}
				next[side]++
			}
		}
		l.step(t, ks)
		for side, k := range ks {
			for {
				n, err := k.Recv(buf)
				if err == ErrNoMessage {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if stream {
					stream2[side] = append(stream2[side], buf[:n]...)
					continue
				}
				if exp := want[1-side][got[side]]; !bytes.Equal(buf[:n], exp) {
					t.Fatalf("side %d message %d: %d bytes, want %d", side, got[side], n, len(exp))
				}
				got[side]++
			}
		}
	}
	if stream {
		for side := range 2 {
			if !bytes.Equal(stream2[side], all[1-side]) {
				t.Fatalf("side %d: stream differs", side)
			}
		}
	}
	return l, ks
}

func TestTransfer(t *testing.T) {
	for _, tc := range []struct {
		name   string
		loss   float64
		jitter uint32
		tune   func(k *KCP)
	}{
		{"clean", 0, 0, nil},
		{"loss10", 0.10, 0, nil},
		{"loss30-reorder", 0.30, 40, nil},
		{"fast3-loss20-reorder", 0.20, 30, func(k *KCP) { k.NoDelay(1, 10, 2, true) }},
		{"nodelay2-smallwindow", 0.15, 10, func(k *KCP) { k.NoDelay(2, 10, 1, false); k.SetWindow(8, 8) }},
		{"mtu200", 0.05, 5, func(k *KCP) { k.SetMTU(200) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, ks := transfer(t, tc.loss, tc.jitter, tc.tune, 300, 5000)
			if tc.loss > 0 && ks[0].Stats().Retransmitted == 0 {
				t.Fatal("nothing retransmitted on a lossy link")
			}
			t.Logf("%d packets, stats %+v, rto %d", l.sent, ks[0].Stats(), ks[0].RTO())
		})
	}
}

func TestStreamMode(t *testing.T) {
	transfer(t, 0.1, 10, func(k *KCP) { k.SetStreamMode(true) }, 200, 3000)
}

func TestMessageTooBig(t *testing.T) {
	k := New(1, func([]byte) {})
	if err := k.Send(make([]byte, 255*k.MSS())); err != nil {
		t.Fatal(err)
	}
	if err := k.Send(make([]byte, 255*k.MSS()+1)); err != ErrMessageTooBig {
		t.Fatalf("got %v", err)
	}
	if err := k.Send(nil); err != ErrEmpty {
		t.Fatalf("got %v", err)
	}
}

func TestIdleNeedsNoTimer(t *testing.T) {
	l := &simLink{rnd: rand.New(rand.NewPCG(1, 1)), delay: 5}
	ks := [2]*KCP{New(1, l.output(1)), New(1, l.output(0))}
	if _, ok := ks[0].NextFlush(0); ok {
		t.Fatal("a new KCP has something due")
	}
	_ = ks[0].Send([]byte("hello"))
	for range 100 {
		l.step(t, ks)
	}
	buf := make([]byte, 100)
	if n, err := ks[1].Recv(buf); err != nil || string(buf[:n]) != "hello" {
		t.Fatalf("got %q %v", buf[:n], err)
	}
	for range 100 {
		l.step(t, ks)
	}
	for i, k := range ks {
		if at, ok := k.NextFlush(l.now); ok {
			t.Fatalf("side %d: idle, yet a flush is due at %d (now %d)", i, at, l.now)
		}
	}
}

func TestZeroWindowProbe(t *testing.T) {
	l := &simLink{rnd: rand.New(rand.NewPCG(1, 1)), delay: 5}
	ks := [2]*KCP{New(1, l.output(1)), New(1, l.output(0))}
	ks[1].SetWindow(0, 4)
	for i := range 20 {
		_ = ks[0].Send([]byte{byte(i)})
	}
	// The receiver reads nothing: its window closes.
	for range 2000 {
		l.step(t, ks)
	}
	if ks[0].rmtWnd != 0 {
		t.Fatalf("remote window %d, want 0", ks[0].rmtWnd)
	}
	if _, ok := ks[0].NextFlush(l.now); !ok {
		t.Fatal("no probe due with the peer's window closed")
	}
	buf := make([]byte, 10)
	for got := 0; got < 20; {
		if n, err := ks[1].Recv(buf); err == nil {
			if n != 1 || buf[0] != byte(got) {
				t.Fatalf("message %d: %v", got, buf[:n])
			}
			got++
			continue
		}
		l.step(t, ks)
		if l.now > 200000 {
			t.Fatal("stuck behind a closed window")
		}
	}
}

func TestDeadLink(t *testing.T) {
	l := &simLink{rnd: rand.New(rand.NewPCG(1, 1)), loss: 1, delay: 5}
	ks := [2]*KCP{New(1, l.output(1)), New(1, l.output(0))}
	ks[0].SetDeadLink(5)
	ks[0].NoDelay(1, 10, 0, true)
	_ = ks[0].Send([]byte("x"))
	for !ks[0].Dead() {
		l.step(t, ks)
		if l.now > 100000 {
			t.Fatal("never dead")
		}
	}
}

func TestInputRejects(t *testing.T) {
	k := New(1, func([]byte) {})
	seg := appendHeader(nil, 2, cmdPush, 0, 32, 0, 0, 0, 0)
	if err := k.Input(seg, 0); err != ErrConv {
		t.Fatalf("other conv: %v", err)
	}
	seg = appendHeader(nil, 1, 99, 0, 32, 0, 0, 0, 0)
	if err := k.Input(seg, 0); err != ErrMalformed {
		t.Fatalf("bad cmd: %v", err)
	}
	seg = appendHeader(nil, 1, cmdPush, 0, 32, 0, 0, 0, 10)
	if err := k.Input(seg, 0); err != ErrMalformed {
		t.Fatalf("short data: %v", err)
	}
	if err := k.Input(seg[:10], 0); err != ErrMalformed {
		t.Fatalf("short header: %v", err)
	}
}

func FuzzInput(f *testing.F) {
	f.Add(appendHeader(nil, 1, cmdPush, 0, 32, 0, 0, 0, 0))
	f.Add(append(appendHeader(nil, 1, cmdPush, 2, 32, 5, 3, 0, 3), 1, 2, 3))
	f.Fuzz(func(t *testing.T, b []byte) {
		k := New(1, func([]byte) {})
		_ = k.Send([]byte("data"))
		k.Flush(0)
		_ = k.Input(b, 10)
		k.Flush(20)
		_ = k.PeekSize()
		_, _ = k.Recv(make([]byte, 1<<20))
	})
}

func ExampleKCP() {
	var a, b *KCP
	a = New(1, func(p []byte) { _ = b.Input(p, 0) })
	b = New(1, func(p []byte) { _ = a.Input(p, 0) })
	_ = a.Send([]byte("hello"))
	a.Flush(0)
	buf := make([]byte, 16)
	n, _ := b.Recv(buf)
	fmt.Println(string(buf[:n]))
	// Output: hello
}

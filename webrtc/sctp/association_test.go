package sctp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

type gotMsg struct {
	stream uint16
	ppid   uint32
	data   []byte
}

// collect records end i's raw messages.
func collect(msgs *[]gotMsg) func(i int, c *Config) {
	return func(i int, c *Config) {
		if i == 1 {
			c.OnMessage = func(s uint16, p uint32, d []byte) {
				*msgs = append(*msgs, gotMsg{s, p, append([]byte(nil), d...)})
			}
		}
	}
}

func payload(rng *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rng.Uint32())
	}
	return b
}

func TestHandshake(t *testing.T) {
	for _, both := range []bool{false, true} {
		t.Run(fmt.Sprintf("both=%v", both), func(t *testing.T) {
			l := newLink(t, 1, nil)
			l.connect(both)
			a, b := l.ends[0], l.ends[1]
			if a.peerTag != b.localTag || b.peerTag != a.localTag {
				t.Fatalf("tags: %x/%x %x/%x", a.peerTag, b.localTag, b.peerTag, a.localTag)
			}
			if !a.peerForwardTSN || !a.peerReconfig || !b.peerForwardTSN || !b.peerReconfig {
				t.Fatal("extensions not negotiated")
			}
		})
	}
}

func TestHandshakeLossy(t *testing.T) {
	for seed := uint64(0); seed < 20; seed++ {
		l := newLink(t, seed, nil)
		l.loss = 0.3
		l.dup = 0.2
		l.jitter = 50 * time.Millisecond
		l.ends[0].Connect(l.now)
		l.ends[1].Connect(l.now)
		if !l.run(5*time.Minute, l.established) {
			t.Fatalf("seed %d: %v %v", seed, l.ends[0].State(), l.ends[1].State())
		}
	}
}

func TestHandshakeTimeout(t *testing.T) {
	l := newLink(t, 1, nil)
	l.loss = 1
	l.ends[0].Connect(l.now)
	l.run(time.Hour, func() bool { return l.closed[0] })
	if !errors.Is(l.closeEr[0], ErrTimeout) {
		t.Fatalf("close error %v", l.closeEr[0])
	}
}

// TestOrderedLossy sends large ordered messages over a link that loses,
// duplicates and reorders packets: every message arrives whole and in
// order.
func TestOrderedLossy(t *testing.T) {
	for seed := uint64(1); seed <= 4; seed++ {
		var got []gotMsg
		l := newLink(t, seed, collect(&got))
		l.connect(false)
		l.loss, l.dup, l.jitter = 0.05, 0.02, 30*time.Millisecond
		rng := rand.New(rand.NewPCG(seed, 7))
		var want [][]byte
		for i := range 40 {
			n := 1 + rng.IntN(64<<10)
			if i%10 == 0 {
				n = 64 << 10
			}
			p := payload(rng, n)
			want = append(want, p)
			if err := l.ends[0].Send(uint16(i%3), PPIDBinary, p, SendOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		if !l.run(10*time.Minute, func() bool { return len(got) == len(want) }) {
			t.Fatalf("seed %d: %d of %d messages", seed, len(got), len(want))
		}
		next := map[uint16]int{}
		for _, m := range got {
			s := m.stream
			// Messages on one stream keep their order.
			i := next[s]*3 + int(s)
			for i < len(want) && !bytes.Equal(want[i], m.data) {
				t.Fatalf("seed %d: stream %d message out of order or corrupt (len %d)", seed, s, len(m.data))
			}
			next[s]++
		}
		l.run(time.Minute, func() bool { return !l.ends[0].outstanding() })
		if l.ends[0].outstanding() || l.ends[0].BufferedAmount(0) != 0 {
			t.Fatalf("still outstanding: buffered %d", l.ends[0].BufferedAmount(0))
		}
		if l.ends[1].held != 0 {
			t.Fatalf("receiver holds %d bytes", l.ends[1].held)
		}
	}
}

func TestUnorderedLossy(t *testing.T) {
	var got []gotMsg
	l := newLink(t, 3, collect(&got))
	l.connect(false)
	l.loss, l.dup, l.jitter = 0.05, 0.05, 40*time.Millisecond
	rng := rand.New(rand.NewPCG(3, 3))
	want := map[string]bool{}
	for range 100 {
		p := payload(rng, 1+rng.IntN(8000))
		want[string(p)] = true
		if err := l.ends[0].Send(5, 99, p, SendOptions{Unordered: true}); err != nil {
			t.Fatal(err)
		}
	}
	if !l.run(10*time.Minute, func() bool { return len(got) == len(want) }) {
		t.Fatalf("%d of %d", len(got), len(want))
	}
	for _, m := range got {
		if !want[string(m.data)] || m.ppid != 99 || m.stream != 5 {
			t.Fatal("unexpected message")
		}
		delete(want, string(m.data))
	}
}

// TestPartialReliability gives messages up under heavy loss: those that
// arrive are whole and in order, and reliable messages after them on the
// same stream all get through.
func TestPartialReliability(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts SendOptions
	}{
		{"rexmit0", SendOptions{PR: PRRetransmits, Limit: 0}},
		{"rexmit1-unordered", SendOptions{PR: PRRetransmits, Limit: 1, Unordered: true}},
		{"lifetime", SendOptions{PR: PRLifetime, Limit: 50}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []gotMsg
			l := newLink(t, 9, collect(&got))
			l.connect(false)
			l.loss = 0.2
			const n = 200
			for i := range n {
				p := make([]byte, 100+i*37%3000)
				binary.BigEndian.PutUint32(p, uint32(i))
				if err := l.ends[0].Send(1, 0, p, tc.opts); err != nil {
					t.Fatal(err)
				}
			}
			for i := range 20 {
				p := make([]byte, 500)
				binary.BigEndian.PutUint32(p, uint32(n+i))
				if err := l.ends[0].Send(1, 1, p, SendOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			reliable := func() int {
				c := 0
				for _, m := range got {
					if m.ppid == 1 {
						c++
					}
				}
				return c
			}
			if !l.run(30*time.Minute, func() bool { return reliable() == 20 }) {
				t.Fatalf("%d of 20 reliable messages", reliable())
			}
			last := -1
			for _, m := range got {
				i := int(binary.BigEndian.Uint32(m.data))
				if m.ppid == 0 && len(m.data) != 100+i*37%3000 {
					t.Fatalf("message %d has %d bytes", i, len(m.data))
				}
				if !tc.opts.Unordered && i <= last {
					t.Fatalf("message %d after %d", i, last)
				}
				last = max(last, i)
			}
			if len(got) == n+20 {
				t.Fatal("nothing was given up")
			}
			t.Logf("%d of %d partially reliable messages delivered", len(got)-20, n)
			l.loss = 0
			l.run(time.Minute, func() bool { return !l.ends[0].outstanding() })
			if b := l.ends[0].BufferedAmount(1); b != 0 {
				t.Fatalf("buffered %d", b)
			}
			if l.ends[1].held != 0 {
				t.Fatalf("receiver holds %d", l.ends[1].held)
			}
			if l.ends[0].prCount != 0 {
				t.Fatalf("prCount %d", l.ends[0].prCount)
			}
		})
	}
}

func TestMessageSizes(t *testing.T) {
	var got []gotMsg
	l := newLink(t, 1, func(i int, c *Config) {
		collect(&got)(i, c)
		c.RemoteMaxMessageSize = 1 << 20
	})
	l.connect(false)
	big := payload(rand.New(rand.NewPCG(1, 1)), DefaultMaxMessageSize)
	if err := l.ends[0].Send(0, 0, big, SendOptions{}); err != nil {
		t.Fatal(err)
	}
	if !l.run(time.Minute, func() bool { return len(got) == 1 }) || !bytes.Equal(got[0].data, big) {
		t.Fatal("256 KiB message lost")
	}
	if err := l.ends[0].Send(0, 0, nil, SendOptions{}); !errors.Is(err, ErrEmptyMessage) {
		t.Fatalf("empty: %v", err)
	}
	// Past the receiver's limit the peer aborts.
	if err := l.ends[0].Send(0, 0, make([]byte, DefaultMaxMessageSize+1), SendOptions{}); err != nil {
		t.Fatal(err)
	}
	l.run(time.Minute, func() bool { return l.closed[0] && l.closed[1] })
	if !errors.Is(l.closeEr[1], ErrMessageTooLarge) || !errors.Is(l.closeEr[0], ErrAborted) {
		t.Fatalf("close errors %v / %v", l.closeEr[0], l.closeEr[1])
	}

	l = newLink(t, 1, nil)
	l.connect(false)
	if err := l.ends[0].Send(0, 0, make([]byte, DefaultRemoteMaxMessage+1), SendOptions{}); !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("over the remote's limit: %v", err)
	}
}

func TestShutdown(t *testing.T) {
	var got []gotMsg
	l := newLink(t, 1, collect(&got))
	l.connect(false)
	l.loss = 0.1
	for range 20 {
		if err := l.ends[0].Send(0, 0, make([]byte, 5000), SendOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	l.ends[0].Shutdown()
	if err := l.ends[0].Send(0, 0, []byte{1}, SendOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("send after shutdown: %v", err)
	}
	if !l.run(10*time.Minute, func() bool { return l.closed[0] && l.closed[1] }) {
		t.Fatalf("states %v %v", l.ends[0].State(), l.ends[1].State())
	}
	if l.closeEr[0] != nil || l.closeEr[1] != nil || len(got) != 20 {
		t.Fatalf("errors %v %v, %d messages", l.closeEr[0], l.closeEr[1], len(got))
	}
}

func TestAbort(t *testing.T) {
	l := newLink(t, 1, nil)
	l.connect(false)
	l.ends[1].Abort()
	l.run(time.Second, func() bool { return l.closed[0] })
	if !errors.Is(l.closeEr[0], ErrAborted) || l.closeEr[1] != nil {
		t.Fatalf("%v %v", l.closeEr[0], l.closeEr[1])
	}
	if err := l.ends[0].Send(0, 0, []byte{1}, SendOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestHeartbeat(t *testing.T) {
	l := newLink(t, 1, func(i int, c *Config) { c.HeartbeatInterval = time.Second })
	l.connect(false)
	l.run(time.Minute, nil)
	if l.closed[0] || l.closed[1] {
		t.Fatal("closed while idle")
	}
	if !l.ends[0].rttValid {
		t.Fatal("heartbeats took no RTT sample")
	}
	if rto := l.ends[0].rto; rto != DefaultRTOMin {
		t.Logf("rto %v", rto)
	}
	// A peer gone silent is given up on.
	l.loss = 1
	l.run(time.Hour, func() bool { return l.closed[0] })
	if !errors.Is(l.closeEr[0], ErrTimeout) {
		t.Fatalf("close error %v", l.closeEr[0])
	}
}

// TestRetransmitTimeout loses one packet's worth of a burst: T3-rtx or fast
// retransmit recovers it, and the congestion window reacts.
func TestCongestionControl(t *testing.T) {
	var got []gotMsg
	l := newLink(t, 1, collect(&got))
	l.connect(false)
	a := l.ends[0]
	cwnd0 := a.cwnd
	for range 100 {
		_ = a.Send(0, 0, make([]byte, 1000), SendOptions{})
	}
	l.run(time.Minute, func() bool { return len(got) == 100 })
	if a.cwnd <= cwnd0 {
		t.Fatalf("slow start did not grow cwnd: %d", a.cwnd)
	}
	grown := a.cwnd
	// Drop the next DATA packet once: three SACKs reporting it missing
	// trigger fast retransmit.
	dropped := false
	l.drop = func(from int, p []byte) bool {
		if from == 0 && !dropped && p[headerSize] == chunkData {
			dropped = true
			return true
		}
		return false
	}
	for range 50 {
		_ = a.Send(0, 0, make([]byte, 1000), SendOptions{})
	}
	l.run(time.Minute, func() bool { return len(got) == 150 })
	if len(got) != 150 || a.cwnd >= grown+50*a.mtu {
		t.Fatalf("%d messages, cwnd %d (was %d)", len(got), a.cwnd, grown)
	}
	if a.ssthresh >= 1<<20 {
		t.Fatalf("ssthresh never cut: %d", a.ssthresh)
	}
	// Lose everything for a while: T3 collapses cwnd to one MTU.
	l.loss = 1
	_ = a.Send(0, 0, make([]byte, 1000), SendOptions{})
	l.run(3*time.Second, nil)
	if a.cwnd != a.mtu || a.rto <= DefaultRTOMin {
		t.Fatalf("after timeouts cwnd %d rto %v", a.cwnd, a.rto)
	}
	l.loss = 0
	l.run(5*time.Minute, func() bool { return len(got) == 151 })
	if len(got) != 151 {
		t.Fatal("not recovered")
	}
}

// TestZeroWindow fills a receiver whose messages cannot complete: the
// sender stops at the advertised window and probes.
func TestReceiveWindow(t *testing.T) {
	var got []gotMsg
	l := newLink(t, 1, func(i int, c *Config) {
		collect(&got)(i, c)
		c.ReceiveWindow = 16 << 10
		c.RemoteMaxMessageSize = -1
	})
	l.connect(false)
	rng := rand.New(rand.NewPCG(1, 2))
	want := payload(rng, 200<<10)
	_ = l.ends[0].Send(0, 0, want, SendOptions{})
	l.jitter = 20 * time.Millisecond
	if !l.run(10*time.Minute, func() bool { return len(got) == 1 }) {
		t.Fatal("not delivered")
	}
	if !bytes.Equal(got[0].data, want) {
		t.Fatal("corrupt")
	}
}

func TestSACKDelay(t *testing.T) {
	l := newLink(t, 1, nil)
	l.connect(false)
	base := l.sent[1]
	_ = l.ends[0].Send(0, 0, []byte("x"), SendOptions{})
	l.run(150*time.Millisecond, nil)
	if l.sent[1] != base {
		t.Fatal("SACK sent before the delay")
	}
	l.run(200*time.Millisecond, nil)
	if l.sent[1] != base+1 {
		t.Fatalf("%d packets", l.sent[1]-base)
	}
	// Two packets of data are acknowledged at once.
	base = l.sent[1]
	_ = l.ends[0].Send(0, 0, make([]byte, 1100), SendOptions{})
	_ = l.ends[0].Send(0, 0, make([]byte, 1100), SendOptions{})
	l.run(20*time.Millisecond, nil)
	if l.sent[1] != base+1 {
		t.Fatalf("%d packets", l.sent[1]-base)
	}
}

func TestStreamResetRaw(t *testing.T) {
	var got []gotMsg
	l := newLink(t, 1, collect(&got))
	l.connect(false)
	a, b := l.ends[0], l.ends[1]
	_ = a.Send(4, 0, []byte("one"), SendOptions{})
	_ = a.ResetStream(4)
	if err := a.Send(4, 0, []byte("x"), SendOptions{}); !errors.Is(err, ErrStreamClosing) {
		t.Fatal(err)
	}
	l.run(time.Second, func() bool { return !a.stream(4).resetting })
	if a.stream(4).resetting || b.stream(4).nextSSN != 0 || a.stream(4).ssn != 0 {
		t.Fatal("not reset")
	}
	_ = a.Send(4, 0, []byte("two"), SendOptions{})
	l.run(time.Second, func() bool { return len(got) == 2 })
	if len(got) != 2 || string(got[1].data) != "two" {
		t.Fatal("stream unusable after reset")
	}
}

// TestMalformed feeds an established association broken packets: each is
// rejected with an error and the association carries on.
func TestMalformed(t *testing.T) {
	var got []gotMsg
	l := newLink(t, 1, collect(&got))
	l.connect(false)
	b := l.ends[1]
	tag := b.localTag
	build := func(vtag uint32, chunks ...[]byte) []byte {
		p := make([]byte, headerSize)
		binary.BigEndian.PutUint16(p, 5000)
		binary.BigEndian.PutUint16(p[2:], 5000)
		binary.BigEndian.PutUint32(p[4:], vtag)
		for _, c := range chunks {
			p = append(p, c...)
		}
		binary.LittleEndian.PutUint32(p[8:], checksum(p))
		return p
	}
	chunk := func(typ, flags byte, v ...byte) []byte {
		c := []byte{typ, flags, 0, 0}
		binary.BigEndian.PutUint16(c[2:], uint16(4+len(v)))
		c = append(c, v...)
		for len(c)%4 != 0 {
			c = append(c, 0)
		}
		return c
	}
	bad := build(tag, chunk(chunkHeartbeat, 0, 0, 1, 0, 4))
	bad[8] ^= 1
	tests := []struct {
		name string
		p    []byte
		err  error
	}{
		{"short", []byte{1, 2, 3}, ErrMalformed},
		{"checksum", bad, ErrChecksum},
		{"tag", build(tag+1, chunk(chunkHeartbeat, 0, 0, 1, 0, 4)), ErrVerificationTag},
		{"chunk length", build(tag, []byte{chunkSack, 0, 0, 99}), ErrMalformed},
		{"chunk length short", build(tag, []byte{chunkSack, 0, 0, 2}), ErrMalformed},
		{"short sack", build(tag, chunk(chunkSack, 0, 1, 2, 3)), ErrMalformed},
		{"sack gaps past end", build(tag, chunk(chunkSack, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0)), ErrMalformed},
		{"short data", build(tag, chunk(chunkData, 3, 1, 2, 3)), ErrMalformed},
		{"init with data", build(0, chunk(chunkInit, 0, make([]byte, 16)...), chunk(chunkData, 3, make([]byte, 13)...)), ErrMalformed},
		{"init zero tag", build(0, chunk(chunkInit, 0, make([]byte, 16)...)), ErrMalformed},
		{"forged cookie", build(tag, chunk(chunkCookieEcho, 0, make([]byte, cookieSize)...)), ErrBadCookie},
		{"short cookie", build(tag, chunk(chunkCookieEcho, 0, 1, 2)), ErrBadCookie},
		{"forward tsn odd", build(tag, chunk(chunkForwardTSN, 0, 1, 2, 3, 4, 5, 6)), ErrMalformed},
		{"reconfig param", build(tag, chunk(chunkReconfig, 0, 0, 13, 0, 99)), ErrMalformed},
		{"short reset request", build(tag, chunk(chunkReconfig, 0, 0, 13, 0, 8, 0, 0, 0, 0)), ErrMalformed},
		{"unknown stop", build(tag, chunk(0x3f, 0)), nil},
		{"unknown skip", build(tag, chunk(0xbf, 0)), nil},
		{"unknown report", build(tag, chunk(0x7f, 0, 1, 2)), nil},
		{"unknown skip report", build(tag, chunk(0xff, 0)), nil},
		{"incoming reset denied", build(tag, chunk(chunkReconfig, 0, 0, 14, 0, 10, 0, 0, 0, 0, 0, 7, 0, 0)), nil},
	}
	for _, tc := range tests {
		err := b.HandlePacket(tc.p, l.now)
		if !errors.Is(err, tc.err) || (tc.err == nil) != (err == nil) {
			t.Errorf("%s: got %v, want %v", tc.name, err, tc.err)
		}
	}
	// SACK of a TSN never sent is a protocol violation.
	l2 := newLink(t, 2, nil)
	l2.connect(false)
	a2 := l2.ends[0]
	v := make([]byte, 12)
	binary.BigEndian.PutUint32(v, a2.nextTSN+5)
	_ = a2.HandlePacket(build(a2.localTag, chunk(chunkSack, 0, v...)), l2.now)
	if !l2.closed[0] || !errors.Is(l2.closeEr[0], ErrProtocol) {
		t.Fatalf("bogus SACK: %v", l2.closeEr[0])
	}
	// DATA with no user data aborts.
	if !l.established() {
		t.Fatal("malformed packets broke the association")
	}
	_ = l.ends[0].Send(0, 0, []byte("still"), SendOptions{})
	l.run(time.Second, func() bool { return len(got) == 1 })
	if len(got) != 1 {
		t.Fatal("association stopped working")
	}
	d := make([]byte, 12)
	binary.BigEndian.PutUint32(d, b.peerCum+1)
	_ = b.HandlePacket(build(tag, chunk(chunkData, 3, d...)), l.now)
	if !l.closed[1] || !errors.Is(l.closeEr[1], ErrProtocol) {
		t.Fatalf("empty DATA: %v", l.closeEr[1])
	}
}

// TestTSNWrap runs traffic across the 32-bit TSN wrap.
func TestTSNWrap(t *testing.T) {
	var got []gotMsg
	l := newLink(t, 5, collect(&got))
	a := l.ends[0]
	a.localTSN = 0xffffffff - 20
	a.sender.init(a.localTSN, a.mtu)
	a.reqSeq = a.localTSN
	l.connect(false)
	l.loss, l.jitter = 0.05, 20*time.Millisecond
	for i := range 100 {
		_ = a.Send(0, 0, bytes.Repeat([]byte{byte(i)}, 700), SendOptions{})
	}
	if !l.run(10*time.Minute, func() bool { return len(got) == 100 }) {
		t.Fatalf("%d", len(got))
	}
	for i, m := range got {
		if m.data[0] != byte(i) {
			t.Fatal("order")
		}
	}
}

func TestShutdownSimultaneous(t *testing.T) {
	l := newLink(t, 1, nil)
	l.connect(false)
	l.loss = 0.2
	_ = l.ends[0].Send(0, 0, make([]byte, 3000), SendOptions{})
	_ = l.ends[1].Send(0, 0, make([]byte, 3000), SendOptions{})
	l.ends[0].Shutdown()
	l.ends[1].Shutdown()
	if !l.run(10*time.Minute, func() bool { return l.closed[0] && l.closed[1] }) {
		t.Fatalf("states %v %v", l.ends[0].State(), l.ends[1].State())
	}
	if l.closeEr[0] != nil || l.closeEr[1] != nil {
		t.Fatalf("%v %v", l.closeEr[0], l.closeEr[1])
	}
}

// TestResetInProgress loses the data sent just before a stream reset: the
// peer answers the request "in progress" until the data arrives, and
// delivers it under the old sequence numbers before the reset.
func TestResetInProgress(t *testing.T) {
	var got []gotMsg
	l := newLink(t, 1, collect(&got))
	l.connect(false)
	a, b := l.ends[0], l.ends[1]
	dropped := 0
	l.drop = func(from int, p []byte) bool {
		if from == 0 && p[headerSize] == chunkData && dropped < 2 {
			dropped++
			return true
		}
		return false
	}
	_ = a.Send(2, 0, []byte("before"), SendOptions{})
	_ = a.ResetStream(2)
	l.run(time.Minute, func() bool { return !a.stream(2).resetting })
	if a.stream(2).resetting || len(got) != 1 || string(got[0].data) != "before" {
		t.Fatalf("resetting %v, %d messages", a.stream(2).resetting, len(got))
	}
	if b.peerReqSeq != a.reqSeq {
		t.Fatalf("request sequence %d, peer expects %d", a.reqSeq, b.peerReqSeq)
	}
	_ = a.Send(2, 0, []byte("after"), SendOptions{})
	l.run(time.Minute, func() bool { return len(got) == 2 })
	if len(got) != 2 {
		t.Fatal("stream unusable after reset")
	}
}

func TestStateString(t *testing.T) {
	if StateEstablished.String() != "established" || State(99).String() != "State(99)" {
		t.Fatal(StateEstablished.String())
	}
}

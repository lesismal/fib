//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"
)

// readBufferProbe records the capacity of the buffer each OnData is handed
// and answers every newline it reads with one byte, so a client can tell when
// what it sent has been read.
type readBufferProbe struct {
	mu   sync.Mutex
	caps []int
}

func (p *readBufferProbe) onData(c *Connection, b []byte) {
	p.mu.Lock()
	p.caps = append(p.caps, cap(b))
	p.mu.Unlock()
	if n := bytes.Count(b, []byte{'\n'}); n > 0 {
		_, _ = c.Write(bytes.Repeat([]byte{'k'}, n))
	}
}

func (p *readBufferProbe) snapshot() []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int(nil), p.caps...)
}

// startReadBufferEngine serves probe on a fresh engine with the read buffer
// sizes given and returns the engine and a client connected to it.
func startReadBufferEngine(t *testing.T, start, full int, probe *readBufferProbe) (*Engine, net.Conn) {
	t.Helper()
	config := DefaultConfig()
	config.LogSettings = false
	config.ReadBufferSize = full
	config.ReadBufferStartSize = start
	engine, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{Data: probe.onData}})
	if err != nil {
		t.Fatal(err)
	}
	addr, err := engine.LocalAddr()
	if err != nil {
		_ = engine.Close()
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- engine.Run() }()
	t.Cleanup(func() {
		engine.Stop()
		if err := <-runDone; err != nil {
			t.Error(err)
		}
		if err := engine.Close(); err != nil {
			t.Error(err)
		}
	})
	conn, err := net.DialTimeout("tcp4", addr.String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	t.Cleanup(func() { _ = conn.Close() })
	return engine, conn
}

// readExchange sends msg, which ends in a newline, and waits for the reply that
// says the server has read it.
func readExchange(t *testing.T, conn net.Conn, msg []byte) {
	t.Helper()
	if _, err := conn.Write(msg); err != nil {
		t.Fatal(err)
	}
	var reply [1]byte
	if _, err := conn.Read(reply[:]); err != nil {
		t.Fatal(err)
	}
}

// TestReadBufferStartSmallRequestsStaySmall checks a connection whose
// requests fit the starting buffer never takes the full one: the buffer a
// served request holds is what the starting size saves.
func TestReadBufferStartSmallRequestsStaySmall(t *testing.T) {
	const start, full = 4 << 10, 64 << 10
	probe := &readBufferProbe{}
	_, conn := startReadBufferEngine(t, start, full, probe)
	for i := 0; i < 2*readShrinkRounds; i++ {
		readExchange(t, conn, []byte("ping\n"))
	}
	caps := probe.snapshot()
	if len(caps) != 2*readShrinkRounds {
		t.Fatalf("rounds handed %d buffers, want %d: %v", len(caps), 2*readShrinkRounds, caps)
	}
	for _, c := range caps {
		if c != start {
			t.Fatalf("a small request's round used a %d-byte buffer, want the %d-byte start: %v", c, start, caps)
		}
	}
}

// TestReadBufferStartGrowthPersists checks a large body moves the connection
// to the full buffer, that its next rounds start there although they are
// small, and that readShrinkRounds small rounds in a row move it back.
func TestReadBufferStartGrowthPersists(t *testing.T) {
	const start, full = 4 << 10, 32 << 10
	const size = 1 << 20
	probe := &readBufferProbe{}
	_, conn := startReadBufferEngine(t, start, full, probe)

	body := bytes.Repeat([]byte{'a'}, size)
	body[size-1] = '\n'
	readExchange(t, conn, body)
	caps := probe.snapshot()
	if len(caps) == 0 || caps[0] != start {
		t.Fatalf("the first read used %v, want the %d-byte start", caps, start)
	}
	grown := false
	for _, c := range caps {
		if c != start && c != full {
			t.Fatalf("a round used a %d-byte buffer, want %d or %d: %v", c, start, full, caps)
		}
		grown = grown || c == full
	}
	if !grown {
		t.Fatalf("the rounds never grew to %d: %v", full, caps)
	}

	// The tail of the body may have come in small rounds; one at least the
	// starting size leaves the connection grown with no small round counted.
	mid := bytes.Repeat([]byte{'b'}, 2*start)
	mid[len(mid)-1] = '\n'
	readExchange(t, conn, mid)

	before := len(probe.snapshot())
	for i := 0; i <= readShrinkRounds; i++ {
		readExchange(t, conn, []byte("ping\n"))
	}
	pings := probe.snapshot()[before:]
	want := make([]int, readShrinkRounds+1)
	for i := range want {
		want[i] = full
	}
	want[readShrinkRounds] = start
	if len(pings) != len(want) {
		t.Fatalf("the pings were handed %d buffers, want %d: %v", len(pings), len(want), pings)
	}
	for i := range want {
		if pings[i] != want[i] {
			t.Fatalf("the pings' rounds used %v, want %v: a grown connection stays grown for %d small rounds", pings, want, readShrinkRounds)
		}
	}
}

// TestReadBufferStartSize checks the starting size an engine settles on: zero
// is fib's default read buffer or ReadBufferSize, whichever is smaller, and
// below zero or above ReadBufferSize is ReadBufferSize.
func TestReadBufferStartSize(t *testing.T) {
	for _, tc := range []struct{ start, full, want int }{
		{0, 64 << 10, defaultReadBufferSize},
		{0, defaultReadBufferSize, defaultReadBufferSize},
		{0, 8 << 10, 8 << 10},
		{4 << 10, 64 << 10, 4 << 10},
		{-1, 64 << 10, 64 << 10},
		{128 << 10, 64 << 10, 64 << 10},
		{64 << 10, 64 << 10, 64 << 10},
	} {
		if got := readBufferStart(tc.start, tc.full); got != tc.want {
			t.Errorf("readBufferStart(%d, %d) = %d, want %d", tc.start, tc.full, got, tc.want)
		}
	}
	config := DefaultConfig()
	config.LogSettings = false
	config.ReadBufferSize = 64 << 10
	engine, err := New(config, Binding{Addr: "127.0.0.1:0", Handler: HandlerFuncs{}})
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	if got := engine.Settings().ReadBufferStartSize; got != defaultReadBufferSize {
		t.Fatalf("Settings().ReadBufferStartSize = %d, want %d", got, defaultReadBufferSize)
	}
}

// TestReadBufferStartFull checks a starting size of ReadBufferSize leaves
// every round at the full buffer, as before there was a starting size.
func TestReadBufferStartFull(t *testing.T) {
	const full = 16 << 10
	probe := &readBufferProbe{}
	_, conn := startReadBufferEngine(t, -1, full, probe)
	body := bytes.Repeat([]byte{'a'}, 256<<10)
	body[len(body)-1] = '\n'
	readExchange(t, conn, body)
	readExchange(t, conn, []byte("ping\n"))
	for _, c := range probe.snapshot() {
		if c != full {
			t.Fatalf("a round used a %d-byte buffer, want the full %d", c, full)
		}
	}
}

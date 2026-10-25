package quic

import (
	"bytes"
	"testing"
	"time"
)

// The flood budgets are the config's: a small one closes the connection
// sooner than the default would, and one below zero never does.
func TestFloodBudgetsConfigurable(t *testing.T) {
	for _, tc := range []struct {
		name         string
		burst, rate  int
		sent         int
		wantFlooding bool
	}{
		{name: "small", burst: 5, rate: 1, sent: 10, wantFlooding: true},
		{name: "default", sent: DefaultControlFrameBurst - 10},
		{name: "unlimited", burst: -1, sent: 3 * DefaultControlFrameBurst},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := frameConn(t)
			c.glitches = newFloodBudget(tc.burst, tc.rate, DefaultControlFrameBurst, DefaultControlFrameRate)
			c.mu.Lock()
			defer c.mu.Unlock()
			var err error
			for i := 0; i < tc.sent && err == nil; i++ {
				_, err = c.handleFrames(spaceApp, frame(framePing), time.Now())
			}
			if (err != nil) != tc.wantFlooding {
				t.Fatalf("after %d PINGs: %v", tc.sent, err)
			}
		})
	}
	// The reset budget too.
	c := frameConn(t)
	c.resets = newFloodBudget(2, 1, DefaultStreamResetBurst, DefaultStreamResetRate)
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	for i := 0; i < 4 && err == nil; i++ {
		_, err = c.handleFrames(spaceApp, frame(frameResetStream, 4*i, 0, 0), time.Now())
	}
	if err == nil {
		t.Fatal("a fourth reset with a burst of two was let through")
	}
}

// MaxAckDelay goes to the peer in max_ack_delay, which the peer then allows
// for, and holds acknowledgements back no longer.
func TestMaxAckDelay(t *testing.T) {
	p := newTestPair(t, 0, func(_, server *Config) { server.MaxAckDelay = 7 * time.Millisecond })
	p.client.mu.Lock()
	got := p.client.peerParams.maxAckDelay
	p.client.mu.Unlock()
	if got != 7*time.Millisecond {
		t.Fatalf("client sees max_ack_delay %v", got)
	}
	server := p.serverConn(t)
	if server.local.maxAckDelay != 7*time.Millisecond {
		t.Fatalf("server holds acknowledgements for %v", server.local.maxAckDelay)
	}
	// The default is not sent, which is what saying nothing means.
	c := Config{}.withDefaults()
	if c.MaxAckDelay != DefaultMaxAckDelay {
		t.Fatalf("default %v", c.MaxAckDelay)
	}
	params := defaultParams()
	if bytes.Contains(params.encode(), []byte{paramMaxAckDelay}) {
		parsed, err := parseParams(params.encode(), false)
		if err != nil || parsed.maxAckDelay != DefaultMaxAckDelay {
			t.Fatalf("default max_ack_delay round trip: %v %v", parsed.maxAckDelay, err)
		}
	}
	if d := (Config{MaxAckDelay: time.Hour}).withDefaults().MaxAckDelay; d != maxAckDelayLimit {
		t.Fatalf("an hour is held to %v", d)
	}
}

// KeyUpdateInterval updates the keys by time, long before the cipher's
// packet limit, and the data keeps flowing across the updates.
func TestKeyUpdateInterval(t *testing.T) {
	p := newTestPair(t, 0, func(client, _ *Config) { client.KeyUpdateInterval = 20 * time.Millisecond })
	p.serverH.onData = func(s *Stream, data []byte, fin bool) { _ = s.Write(data, fin) }
	deadline := time.Now().Add(10 * time.Second)
	for i := 0; ; i++ {
		s, err := p.client.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Write([]byte("tick"), true); err != nil {
			t.Fatal(err)
		}
		waitFinished(t, p.clientH, s.ID())
		if got := p.clientH.received(s.ID()); string(got) != "tick" {
			t.Fatalf("echo %q", got)
		}
		p.client.mu.Lock()
		gen := p.client.txGen
		p.client.mu.Unlock()
		if gen >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d key updates after %d round trips", gen, i)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The server, which has no interval of its own, followed.
	server := p.serverConn(t)
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.rxGen < 2 {
		t.Fatalf("server received %d updates", server.rxGen)
	}
}

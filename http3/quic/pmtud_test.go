package quic

import (
	"bytes"
	"slices"
	"testing"
	"time"
)

// newMTUPair connects a client and a server over a pipe that stops
// fragmenting, each direction carrying datagrams of at most mtu bytes, or
// any when it is zero.
func newMTUPair(t *testing.T, mtu int, loss float64, configure func(client, server *Config)) *testPair {
	t.Helper()
	return newTestPairOpts(t, loss, configure, pairOpts{pipes: func(client, server *pipeEnd) {
		for _, end := range []*pipeEnd{client, server} {
			end.df, end.mtu = true, mtu
		}
	}})
}

// waitDatagramSize waits for c to settle on a datagram size within [lo, hi],
// with its search over, and returns it.
func waitDatagramSize(t *testing.T, c *Conn, lo, hi int) int {
	t.Helper()
	size := 0
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		c.mu.Lock()
		size = c.maxDatagram
		searching := c.mtu.on && c.mtu.probe > 0 && c.mtu.at.IsZero()
		c.mu.Unlock()
		if size >= lo && size <= hi && !searching {
			return size
		}
	}
	t.Fatalf("datagram size %d, want %d to %d", size, lo, hi)
	return 0
}

// echoLarge has the client send n bytes on a stream of its own and waits for
// the server's echo of them.
func echoLarge(t *testing.T, p *testPair, n int) {
	t.Helper()
	p.serverH.onData = func(s *Stream, data []byte, fin bool) { _ = s.Write(data, fin) }
	payload := bytes.Repeat([]byte("pmtud"), n/5)
	s, err := p.client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(payload, true); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, p.clientH, s.ID())
	if got := p.clientH.received(s.ID()); !bytes.Equal(got, payload) {
		t.Fatalf("echo of %d bytes came back as %d", len(payload), len(got))
	}
}

// On a path that carries what the limit allows, the first probe finds it,
// and the connections send datagrams that large.
func TestMTUDiscoveryFindsLimit(t *testing.T) {
	p := newMTUPair(t, 0, 0, nil)
	serverSizes := watchSends(p.serverEnd)
	waitDatagramSize(t, p.client, maxDatagramLimit, maxDatagramLimit)
	waitDatagramSize(t, p.server, maxDatagramLimit, maxDatagramLimit)
	echoLarge(t, p, 256<<10)
	if got := slices.Max(serverSizes()); got != maxDatagramLimit {
		t.Fatalf("server's largest datagram is %d bytes, want %d", got, maxDatagramLimit)
	}
}

// On a path that carries less than the limit, the search settles within
// mtuSearchStep of what it carries, the probes lost on the way leave the
// congestion window alone, and what the connections send then gets through.
func TestMTUDiscoverySearches(t *testing.T) {
	const mtu = 1350
	p := newMTUPair(t, mtu, 0, nil)
	for _, c := range []*Conn{p.client, p.server} {
		waitDatagramSize(t, c, mtu-mtuSearchStep+1, mtu)
		c.mu.Lock()
		recovery := c.cc.recoveryStart
		c.mu.Unlock()
		if !recovery.IsZero() {
			t.Fatal("lost probes were taken for congestion")
		}
	}
	echoLarge(t, p, 256<<10)
	for _, c := range []*Conn{p.client, p.server} {
		c.mu.Lock()
		at, probe := c.mtu.at, c.mtu.probe
		c.mu.Unlock()
		// The search looks again from the top once the raise timer is up.
		if probe != maxDatagramLimit || time.Until(at) < mtuRaiseInterval-time.Minute {
			t.Fatalf("next probe of %d bytes at %v, want %d in %v", probe, time.Until(at), maxDatagramLimit, mtuRaiseInterval)
		}
	}
}

// A connection whose path cannot stop fragmenting, or whose config turns it
// off, does no discovery, and neither does one whose peer allows no more.
func TestMTUDiscoveryOff(t *testing.T) {
	cases := []struct {
		name      string
		df        bool
		configure func(client, server *Config)
	}{
		{"fragmenting path", false, nil},
		{"disabled", true, func(client, server *Config) {
			client.DisablePathMTUDiscovery, server.DisablePathMTUDiscovery = true, true
		}},
		{"limit at the size sent", true, func(client, server *Config) {
			client.MaxProbeDatagramSize, server.MaxProbeDatagramSize = maxDatagram, maxDatagram
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestPairOpts(t, 0, tc.configure, pairOpts{pipes: func(client, server *pipeEnd) {
				client.df, server.df = tc.df, tc.df
			}})
			sizes := watchSends(p.serverEnd)
			echoLarge(t, p, 64<<10)
			if got := slices.Max(sizes()); got != maxDatagram {
				t.Fatalf("largest datagram %d bytes, want %d", got, maxDatagram)
			}
			for _, c := range []*Conn{p.client, p.server} {
				c.mu.Lock()
				on, size := c.mtu.on, c.maxDatagram
				c.mu.Unlock()
				if on || size != maxDatagram {
					t.Fatalf("discovery on: %v, size %d", on, size)
				}
			}
		})
	}
}

// The search goes no higher than the configured limit.
func TestMTUDiscoveryLimit(t *testing.T) {
	p := newMTUPair(t, 0, 0, func(client, server *Config) {
		client.MaxProbeDatagramSize, server.MaxProbeDatagramSize = 1300, 1300
	})
	sizes := watchSends(p.serverEnd)
	waitDatagramSize(t, p.server, 1300, 1300)
	echoLarge(t, p, 64<<10)
	if got := slices.Max(sizes()); got != 1300 {
		t.Fatalf("largest datagram %d bytes, want 1300", got)
	}
}

// Discovery keeps transfers whole on a path that loses datagrams, where
// probes are lost for no fault of their size.
func TestMTUDiscoveryWithLoss(t *testing.T) {
	p := newMTUPair(t, 1400, 0.05, nil)
	echoLarge(t, p, 256<<10)
	for _, c := range []*Conn{p.client, p.server} {
		if size := c.DatagramSize(); size > 1400 {
			t.Fatalf("datagram size %d, more than the path carries", size)
		}
	}
}

// A path that stops carrying the size a connection found is a black hole:
// the connection falls back to the size every path carries, what it sends
// gets through, and the search starts again later.
func TestMTUBlackHole(t *testing.T) {
	p := newMTUPair(t, 0, 0, nil)
	waitDatagramSize(t, p.server, maxDatagramLimit, maxDatagramLimit)
	p.serverEnd.mu.Lock()
	p.serverEnd.mtu = 1280
	p.serverEnd.mu.Unlock()
	start := time.Now()
	echoLarge(t, p, 256<<10)
	if size := p.server.DatagramSize(); size != maxDatagram {
		t.Fatalf("server's datagram size %d after a black hole, want %d", size, maxDatagram)
	}
	p.server.mu.Lock()
	at, probe := p.server.mtu.at, p.server.mtu.probe
	p.server.mu.Unlock()
	if probe != maxDatagramLimit || at.Before(start.Add(mtuBlackHoleWait)) {
		t.Fatalf("next probe of %d bytes in %v, want %d in about %v", probe, time.Until(at), maxDatagramLimit, mtuBlackHoleWait)
	}
	// The client's path still carries its datagrams.
	if size := p.client.DatagramSize(); size != maxDatagramLimit {
		t.Fatalf("client's datagram size %d, want %d", size, maxDatagramLimit)
	}
}

// A large datagram set with Config.MaxDatagramSize on a path that does not
// carry it is a black hole too, discovery or not.
func TestMTUBlackHoleOfConfiguredSize(t *testing.T) {
	p := newTestPairOpts(t, 0, func(client, server *Config) {
		server.MaxDatagramSize = 1400
	}, pairOpts{pipes: func(client, server *pipeEnd) { server.mtu = 1300 }})
	echoLarge(t, p, 256<<10)
	if size := p.server.DatagramSize(); size != maxDatagram {
		t.Fatalf("server's datagram size %d, want %d", size, maxDatagram)
	}
}

// Lost large packets make a black hole only when no large packet sent after
// them has been acknowledged: those lost to congestion are followed by some
// that were not.
func TestMTUBlackHoleByLoss(t *testing.T) {
	c := newConn(nil, nil, Config{}, nil, false)
	now := time.Now()
	c.setMaxDatagramLocked(1400)
	c.mtu.largeAcked = now
	large := func(pn uint64, sent time.Time) *sentPacket {
		return &sentPacket{pn: pn, size: 1400, time: sent, ackEliciting: true, inFlight: true}
	}
	for i := range mtuBlackHoleLosses {
		c.mtuLostLocked(large(uint64(i), now.Add(time.Duration(i+1)*time.Millisecond)), now)
	}
	// A later one acknowledged says the path carries them.
	c.mtuAckedLocked(large(10, now.Add(10*time.Millisecond)), now)
	c.mtuCheckBlackHoleLocked(now)
	if c.maxDatagram != 1400 {
		t.Fatal("losses before an acknowledged large packet made a black hole")
	}
	// Losses after it do not leave it at that, nor do those of packets the
	// path carries anyway.
	c.mtuLostLocked(&sentPacket{pn: 11, size: maxDatagram, time: now.Add(11 * time.Millisecond), ackEliciting: true}, now)
	for i := range mtuBlackHoleLosses {
		c.mtuCheckBlackHoleLocked(now)
		if c.maxDatagram != 1400 {
			t.Fatalf("black hole after %d losses", i)
		}
		c.mtuLostLocked(large(uint64(12+i), now.Add(time.Duration(12+i)*time.Millisecond)), now)
	}
	c.mtuCheckBlackHoleLocked(now)
	if c.maxDatagram != maxDatagram {
		t.Fatalf("datagram size %d after %d large packets lost, want %d", c.maxDatagram, mtuBlackHoleLosses, maxDatagram)
	}
}

// The search finds what a path carries, to within mtuSearchStep, with few
// probes, losing each size it rejects mtuProbeAttempts times.
func TestMTUSearch(t *testing.T) {
	for _, pathMTU := range []int{1200, 1250, 1280, 1350, 1400, 1440, 1451, 1452, 1500} {
		c := newConn(nil, nil, Config{}, nil, false)
		now := time.Now()
		c.mtu = mtuState{on: true, limit: maxDatagramLimit, lo: maxDatagram, hi: maxDatagramLimit + 1, probe: maxDatagramLimit}
		probes := 0
		for pn := uint64(0); c.mtu.probe > 0 && c.mtu.at.IsZero(); pn++ {
			if probes++; probes > 40 {
				t.Fatalf("path of %d: search does not end", pathMTU)
			}
			c.mtu.inFlight, c.mtu.probePN = true, pn
			p := &sentPacket{pn: pn, size: c.mtu.probe, mtuProbe: true, ackEliciting: true, inFlight: true}
			if p.size <= pathMTU {
				c.mtuAckedLocked(p, now)
			} else {
				c.mtuLostLocked(p, now)
			}
		}
		want := min(pathMTU, maxDatagramLimit)
		if c.maxDatagram > want || c.maxDatagram <= want-mtuSearchStep || c.mtu.lo != c.maxDatagram {
			t.Errorf("path of %d: settled on %d", pathMTU, c.maxDatagram)
		}
		if probes > 25 {
			t.Errorf("path of %d: %d probes", pathMTU, probes)
		}
		if want == maxDatagramLimit && c.mtu.probe != 0 {
			t.Errorf("path of %d: searching again for %d", pathMTU, c.mtu.probe)
		}
	}
}

// A server that follows its client to a new address knows nothing of what
// the new path carries: it sends what every path carries until the path is
// validated, then searches it afresh.
func TestMTUDiscoveryAfterRebinding(t *testing.T) {
	p := newRebindPairWith(t, func(p *rebindPair) {
		p.a.df.Store(true)
		p.b.df.Store(true)
		p.b.mtu.Store(1300)
	})
	waitDatagramSize(t, p.server, maxDatagramLimit, maxDatagramLimit)
	p.echo(t, "before")
	p.n.current.Store(p.b)
	p.echo(t, "across")
	p.waitPath(t, p.b)
	waitDatagramSize(t, p.server, 1300-mtuSearchStep+1, 1300)
	p.echo(t, string(bytes.Repeat([]byte("after"), 20<<10)))
}

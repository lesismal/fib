package quic

import (
	"testing"
	"time"
)

// speedupConn is a server whose Initial space has a packet of handshake data
// in flight, and has received the first 100 bytes of the client's.
func speedupConn(t *testing.T) *Conn {
	t.Helper()
	c := frameConn(t)
	s := &c.spaces[spaceInitial]
	s.tx, s.rx = initialKeys(c.scid, false)
	s.cryptoRecv.offset = 100
	p := &sentPacket{pn: 0, time: time.Now(), size: 1200, ackEliciting: true, inFlight: true}
	p.frames = append(p.inline[:0], sentFrame{kind: sfCrypto, n: 0})
	c.onPacketSent(spaceInitial, p)
	s.nextPN = 1
	return c
}

// TestServerSpeedsUpHandshake checks that a client Initial that asks for an
// acknowledgement and brings no new handshake data, a PING or the
// ClientHello again, has the server send its Initial data again at once,
// a few times a connection.
func TestServerSpeedsUpHandshake(t *testing.T) {
	for name, payload := range map[string][]byte{
		"PING":              frame(framePing),
		"PING padded":       append(frame(framePing), 0, 0, 0),
		"ClientHello again": frame(frameCrypto, 0, 10, "0123456789"),
	} {
		t.Run(name, func(t *testing.T) {
			c := speedupConn(t)
			c.mu.Lock()
			defer c.mu.Unlock()
			for i := 1; i <= maxHandshakeSpeedups+2; i++ {
				c.spaces[spaceInitial].probes = 0
				if _, err := c.handleFrames(spaceInitial, payload, time.Now()); err != nil {
					t.Fatal(err)
				}
				want := 0
				if i <= maxHandshakeSpeedups {
					want = maxProbes
				}
				if got := c.spaces[spaceInitial].probes; got != want {
					t.Fatalf("Initial %d: %d probes, want %d", i, got, want)
				}
			}
			if c.handshakeSpeedups != maxHandshakeSpeedups {
				t.Fatalf("%d speedups, want %d", c.handshakeSpeedups, maxHandshakeSpeedups)
			}
		})
	}
}

// TestServerHandshakeNotSpedUp checks the Initials that leave the server to
// its probe timeout: one with handshake data it lacked, one that elicits no
// acknowledgement, one when nothing it sent is in flight, or no longer is
// once it is handled, and anything a client receives.
func TestServerHandshakeNotSpedUp(t *testing.T) {
	cases := map[string]struct {
		setup   func(c *Conn)
		payload []byte
	}{
		"new handshake data": {payload: frame(frameCrypto, 200, 4, "more")},
		"acknowledgement":    {payload: frame(frameAck, 0, 0, 0, 0)},
		"PING with an acknowledgement of everything": {
			payload: append(frame(frameAck, 0, 0, 0, 0), framePing),
		},
		"nothing in flight": {
			setup:   func(c *Conn) { c.spaces[spaceInitial].ackElicitingInFlight = 0 },
			payload: frame(framePing),
		},
		"client": {
			setup:   func(c *Conn) { c.isClient = true },
			payload: frame(framePing),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := speedupConn(t)
			c.mu.Lock()
			defer c.mu.Unlock()
			if tc.setup != nil {
				tc.setup(c)
			}
			if _, err := c.handleFrames(spaceInitial, tc.payload, time.Now()); err != nil {
				t.Fatal(err)
			}
			if c.handshakeSpeedups != 0 || c.spaces[spaceInitial].probes != 0 {
				t.Fatalf("sped up: %d speedups, %d probes", c.handshakeSpeedups, c.spaces[spaceInitial].probes)
			}
		})
	}
}

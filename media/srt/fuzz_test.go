package srt

import (
	"encoding/binary"
	"net/netip"
	"testing"
	"time"
)

// seedPackets returns the datagrams of a handshake and some data, both
// ways, encrypted, as fuzzing seeds.
func seedPackets(t testing.TB) [][]byte {
	var out [][]byte
	s := &sim{now: time.Unix(1_000_000, 0), delay: time.Millisecond, addr: netip.MustParseAddrPort("192.0.2.1:5000")}
	tt, _ := t.(*testing.T)
	s.t = tt
	s.lis, _ = NewListener(ListenerConfig{Accept: s.accept, Rand: newRand(1)})
	s.serverCfg = Config{Passphrase: "0123456789abc"}
	s.drop = func(_ bool, b []byte) bool {
		out = append(out, append([]byte(nil), b...))
		return false
	}
	s.caller.conn, _ = NewCaller(Config{Passphrase: "0123456789abc", StreamID: "#!::r=a,m=publish", Rand: newRand(2),
		Output: func(b []byte) { s.send(true, b) }}, s.now)
	for range 100 {
		s.step()
	}
	if s.caller.conn.State() == StateConnected {
		for i := range 5 {
			_ = s.caller.conn.Write(payload(i, 50), s.now)
		}
		for range 100 {
			s.step()
		}
	}
	return out
}

// step delivers what is in flight and fires due timeouts once, for seeding
// without a *testing.T.
func (s *sim) step() {
	s.now = s.now.Add(5 * time.Millisecond)
	due := s.inflight
	s.inflight = nil
	for _, g := range due {
		s.deliver(g)
	}
	for _, c := range []*Conn{s.caller.conn, s.server.conn} {
		if c != nil {
			c.HandleTimeout(s.now)
		}
	}
}

// FuzzListener feeds arbitrary datagrams to a Listener: it never panics,
// and answers at most once per datagram.
func FuzzListener(f *testing.F) {
	for _, p := range seedPackets(f) {
		f.Add(p)
	}
	l, _ := NewListener(ListenerConfig{Accept: func(*Request) (Config, error) { return Config{Passphrase: "0123456789abc"}, nil }, Rand: newRand(1)})
	addr := netip.MustParseAddrPort("192.0.2.1:5000")
	f.Fuzz(func(t *testing.T, b []byte) {
		now := time.Unix(1_000_000, 0)
		// A valid cookie, so conclusions get past it.
		if len(b) >= headerSize+32 {
			binary.BigEndian.PutUint32(b[headerSize+28:], l.cookie(addr, now))
		}
		n := 0
		l.Handle(b, addr, now, func([]byte) { n++ })
		if n > 1 {
			t.Fatalf("%d answers", n)
		}
		IsHandshake(b)
	})
}

// FuzzConn feeds arbitrary datagrams, addressed to it, to a connected Conn
// that has data in flight both ways: it never panics, and its timers
// always move forward.
func FuzzConn(f *testing.F) {
	for _, p := range seedPackets(f) {
		f.Add(p)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		s := newSim(t, Config{}, Config{})
		s.connect()
		for i := range 3 {
			_ = s.caller.conn.Write(payload(i, 30), s.now)
			_ = s.server.conn.Write(payload(i, 30), s.now)
		}
		s.run(2 * time.Millisecond)
		for _, c := range []*Conn{s.server.conn, s.caller.conn} {
			if len(b) >= headerSize {
				binary.BigEndian.PutUint32(b[12:], c.SocketID())
			}
			c.Handle(b, s.now)
		}
		s.run(2 * time.Second)
	})
}

// FuzzKM parses arbitrary keying material.
func FuzzKM(f *testing.F) {
	km, _ := newKeyMaterial(newRand(1), 16)
	b, _ := km.marshal("0123456789abc")
	f.Add(b)
	f.Fuzz(func(t *testing.T, b []byte) {
		if km, err := unmarshalKM(b, "0123456789abc"); err == nil && km.even == nil && km.odd == nil {
			t.Fatal("no key")
		}
	})
}

// FuzzHandshakeCodec parses arbitrary handshakes; what parses marshals and
// parses again the same.
func FuzzHandshakeCodec(f *testing.F) {
	for _, p := range seedPackets(f) {
		if len(p) > headerSize {
			f.Add(p[headerSize:])
		}
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		var hs handshake
		if hs.unmarshal(b) != nil {
			return
		}
		var again handshake
		if err := again.unmarshal(hs.append(nil)); err != nil {
			t.Fatal(err)
		}
		if again.streamID != hs.streamID || again.typ != hs.typ || (again.hsReq == nil) != (hs.hsReq == nil) {
			t.Fatal("round trip")
		}
	})
}

// FuzzStreamID parses arbitrary stream IDs.
func FuzzStreamID(f *testing.F) {
	f.Add("#!::r=live/s,m=publish,u=a")
	f.Add("live/s?x=1")
	f.Fuzz(func(t *testing.T, s string) {
		if id, err := ParseStreamID(s); err == nil && id.Resource == "" {
			t.Fatal("empty resource")
		}
	})
}

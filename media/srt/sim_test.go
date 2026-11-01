package srt

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"
	"net/netip"
	"testing"
	"time"
)

// seededRand is a deterministic io.Reader for socket IDs and keys.
type seededRand struct{ r *rand.ChaCha8 }

func newRand(seed byte) *seededRand {
	var s [32]byte
	s[0] = seed
	return &seededRand{rand.NewChaCha8(s)}
}

func (r *seededRand) Read(b []byte) (int, error) { return r.r.Read(b) }

// endpoint records what one side of a simulated connection heard.
type endpoint struct {
	conn      *Conn
	data      [][]byte
	dataAt    []time.Time
	connected int
	closed    int
	err       error
}

type dgram struct {
	toServer bool
	at       time.Time
	b        []byte
}

// sim connects a caller and a Listener's Conn over a simulated network with
// a fixed one-way delay, a clock of its own and optional drops.
type sim struct {
	t        *testing.T
	now      time.Time
	delay    time.Duration
	lis      *Listener
	addr     netip.AddrPort
	caller   endpoint
	server   endpoint
	inflight []dgram
	// drop decides the fate of each datagram; nil drops none.
	drop func(toServer bool, b []byte) bool
	// serverCfg is what Accept returns, with the callbacks filled in;
	// acceptErr, when set, rejects instead.
	serverCfg Config
	acceptErr error
	requests  []*Request
	sent      [2]int // datagrams sent to the caller, to the server
}

func newSim(t *testing.T, callerCfg, serverCfg Config) *sim {
	t.Helper()
	s := &sim{t: t, now: time.Now(), delay: 5 * time.Millisecond,
		addr: netip.MustParseAddrPort("192.0.2.1:5000"), serverCfg: serverCfg}
	var err error
	s.lis, err = NewListener(ListenerConfig{Accept: s.accept, Rand: newRand(1)})
	if err != nil {
		t.Fatal(err)
	}
	callerCfg.Output = func(b []byte) { s.send(true, b) }
	callerCfg.OnConnect = func() { s.caller.connected++ }
	callerCfg.OnData = func(b []byte) {
		s.caller.data = append(s.caller.data, append([]byte(nil), b...))
		s.caller.dataAt = append(s.caller.dataAt, s.now)
	}
	callerCfg.OnClose = func(err error) { s.caller.closed++; s.caller.err = err }
	if callerCfg.Rand == nil {
		callerCfg.Rand = newRand(2)
	}
	s.caller.conn, err = NewCaller(callerCfg, s.now)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (s *sim) accept(req *Request) (Config, error) {
	s.requests = append(s.requests, req)
	if s.acceptErr != nil {
		return Config{}, s.acceptErr
	}
	cfg := s.serverCfg
	cfg.Output = func(b []byte) { s.send(false, b) }
	cfg.OnConnect = func() { s.server.connected++ }
	cfg.OnData = func(b []byte) {
		s.server.data = append(s.server.data, append([]byte(nil), b...))
		s.server.dataAt = append(s.server.dataAt, s.now)
	}
	cfg.OnClose = func(err error) { s.server.closed++; s.server.err = err }
	if cfg.Rand == nil {
		cfg.Rand = newRand(3)
	}
	return cfg, nil
}

func (s *sim) send(toServer bool, b []byte) {
	if toServer {
		s.sent[1]++
	} else {
		s.sent[0]++
	}
	if s.drop != nil && s.drop(toServer, b) {
		return
	}
	s.inflight = append(s.inflight, dgram{toServer, s.now.Add(s.delay), append([]byte(nil), b...)})
}

// run advances the clock by d, delivering datagrams and timeouts in order.
func (s *sim) run(d time.Duration) {
	s.t.Helper()
	end := s.now.Add(d)
	for range 1_000_000 {
		next := end
		for _, g := range s.inflight {
			if g.at.Before(next) {
				next = g.at
			}
		}
		for _, c := range []*Conn{s.caller.conn, s.server.conn} {
			if c == nil {
				continue
			}
			if t, ok := c.NextTimeout(); ok && t.Before(next) {
				next = t
			}
		}
		if next.Before(s.now) {
			next = s.now
		}
		s.now = next
		var due []dgram
		rest := s.inflight[:0]
		for _, g := range s.inflight {
			if !g.at.After(s.now) {
				due = append(due, g)
			} else {
				rest = append(rest, g)
			}
		}
		s.inflight = rest
		for _, g := range due {
			s.deliver(g)
		}
		for _, c := range []*Conn{s.caller.conn, s.server.conn} {
			if c == nil {
				continue
			}
			if t, ok := c.NextTimeout(); ok && !t.After(s.now) {
				c.HandleTimeout(s.now)
			}
		}
		if !s.now.Before(end) && len(due) == 0 {
			return
		}
	}
	s.t.Fatal("simulation does not advance")
}

func (s *sim) deliver(g dgram) {
	if !g.toServer {
		s.caller.conn.Handle(g.b, s.now)
		return
	}
	if s.server.conn != nil {
		if id, _, ok := IsHandshake(g.b); !ok || id == s.server.conn.PeerSocketID() {
			s.server.conn.Handle(g.b, s.now)
			return
		}
	}
	if c := s.lis.Handle(g.b, s.addr, s.now, func(b []byte) { s.send(false, b) }); c != nil {
		s.server.conn = c
	}
}

// connect runs the handshake and checks both sides are connected.
func (s *sim) connect() {
	s.t.Helper()
	s.run(200 * time.Millisecond)
	if s.caller.connected != 1 || s.server.connected != 1 {
		s.t.Fatalf("connected caller %d server %d (caller err %v)", s.caller.connected, s.server.connected, s.caller.err)
	}
}

// payload returns a recognisable payload of n bytes for message i.
func payload(i, n int) []byte {
	b := bytes.Repeat([]byte{byte(i)}, n)
	binary.BigEndian.PutUint32(b, uint32(i))
	return b
}

// checkData checks that got holds the payloads from to to-1, in order,
// with the ones in missing left out.
func checkData(t *testing.T, got [][]byte, from, to int, missing map[int]bool) {
	t.Helper()
	j := 0
	for i := from; i < to; i++ {
		if missing[i] {
			continue
		}
		if j >= len(got) {
			t.Fatalf("got %d payloads, payload %d missing", len(got), i)
		}
		if binary.BigEndian.Uint32(got[j]) != uint32(i) {
			t.Fatalf("payload %d is %d", j, binary.BigEndian.Uint32(got[j]))
		}
		j++
	}
	if j != len(got) {
		t.Fatalf("got %d payloads, want %d", len(got), j)
	}
}

// isData reports whether b is a data packet, and its sequence number.
func isData(b []byte) (uint32, bool) {
	var h header
	if h.unmarshal(b) != nil || h.ctrl {
		return 0, false
	}
	return h.seq, true
}

// ctrlType returns the control type of b, or -1 for a data packet.
func ctrlType(b []byte) int {
	var h header
	if h.unmarshal(b) != nil || !h.ctrl {
		return -1
	}
	return int(h.typ)
}

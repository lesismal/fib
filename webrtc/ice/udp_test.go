package ice

import (
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/fib/webrtc/stun"
)

// udpAgent drives an Agent over real UDP sockets, the way its caller would:
// one goroutine reads each socket, another fires its timers, and a mutex
// serializes them.
type udpAgent struct {
	mu        sync.Mutex
	a         *Agent
	conns     map[LocalID]*net.UDPConn
	wake      chan struct{}
	done      chan struct{}
	connected chan struct{}
	once      sync.Once
}

func newUDPAgent(t *testing.T, cfg Config, addrs ...string) (*udpAgent, []Socket) {
	u := &udpAgent{conns: map[LocalID]*net.UDPConn{}, wake: make(chan struct{}, 1), done: make(chan struct{}), connected: make(chan struct{})}
	cfg.Send = func(local LocalID, to netip.AddrPort, b []byte) {
		if c := u.conns[local]; c != nil {
			_, _ = c.WriteToUDPAddrPort(b, to)
		}
	}
	cfg.OnStateChange = func(s State) {
		if s == StateConnected {
			u.once.Do(func() { close(u.connected) })
		}
	}
	a, err := NewAgent(cfg)
	if err != nil {
		t.Fatal(err)
	}
	u.a = a
	var socks []Socket
	for i, s := range addrs {
		c, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort(s)))
		if err != nil {
			t.Logf("skipping %s: %v", s, err)
			continue
		}
		id := LocalID(i + 1)
		u.conns[id] = c
		socks = append(socks, Socket{ID: id, Addrs: []netip.AddrPort{c.LocalAddr().(*net.UDPAddr).AddrPort()}})
	}
	if len(socks) == 0 {
		t.Skip("no loopback UDP")
	}
	t.Cleanup(u.close)
	return u, socks
}

func (u *udpAgent) start() {
	for id, c := range u.conns {
		go func() {
			buf := make([]byte, 1500)
			for {
				n, from, err := c.ReadFromUDPAddrPort(buf)
				if err != nil {
					return
				}
				if !stun.IsMessage(buf[:n]) {
					continue
				}
				u.mu.Lock()
				_ = u.a.HandlePacket(id, from, buf[:n], time.Now())
				u.mu.Unlock()
				select {
				case u.wake <- struct{}{}:
				default:
				}
			}
		}()
	}
	go func() {
		timer := time.NewTimer(time.Hour)
		for {
			u.mu.Lock()
			now := time.Now()
			if t, ok := u.a.NextTimeout(); ok && !t.After(now) {
				u.a.HandleTimeout(now)
			}
			t, ok := u.a.NextTimeout()
			u.mu.Unlock()
			d := time.Hour
			if ok {
				d = max(time.Until(t), 0)
			}
			timer.Reset(d)
			select {
			case <-timer.C:
			case <-u.wake:
			case <-u.done:
				return
			}
		}
	}()
}

func (u *udpAgent) close() {
	select {
	case <-u.done:
		return
	default:
	}
	close(u.done)
	u.mu.Lock()
	u.a.Close()
	u.mu.Unlock()
	for _, c := range u.conns {
		c.Close()
	}
}

func (u *udpAgent) do(f func(a *Agent)) {
	u.mu.Lock()
	f(u.a)
	u.mu.Unlock()
	select {
	case u.wake <- struct{}{}:
	default:
	}
}

func TestLoopbackUDP(t *testing.T) {
	for _, controlling := range []bool{true, false} {
		x, sx := newUDPAgent(t, Config{Controlling: controlling}, "127.0.0.1:0", "[::1]:0")
		y, sy := newUDPAgent(t, Config{Controlling: !controlling}, "127.0.0.1:0", "[::1]:0")
		x.start()
		y.start()
		var cx, cy []Candidate
		var ux, px, uy, py string
		x.do(func(a *Agent) {
			_ = a.Gather(sx, time.Now())
			cx = a.LocalCandidates()
			ux, px = a.LocalCredentials()
		})
		y.do(func(a *Agent) {
			_ = a.Gather(sy, time.Now())
			cy = a.LocalCandidates()
			uy, py = a.LocalCredentials()
		})
		x.do(func(a *Agent) {
			_ = a.SetRemoteCredentials(uy, py, time.Now())
			for _, c := range cy {
				// Through the wire format, as signaling carries them.
				pc, err := ParseCandidate(c.String())
				if err != nil {
					t.Error(err)
				}
				_ = a.AddRemoteCandidate(pc, time.Now())
			}
			a.EndOfRemoteCandidates()
		})
		y.do(func(a *Agent) {
			_ = a.SetRemoteCredentials(ux, px, time.Now())
			for _, c := range cx {
				pc, _ := ParseCandidate(c.String())
				_ = a.AddRemoteCandidate(pc, time.Now())
			}
			a.EndOfRemoteCandidates()
		})
		for _, u := range []*udpAgent{x, y} {
			select {
			case <-u.connected:
			case <-time.After(10 * time.Second):
				t.Fatal("not connected over loopback UDP")
			}
		}
		var p, q Pair
		x.do(func(a *Agent) { p, _ = a.SelectedPair() })
		y.do(func(a *Agent) { q, _ = a.SelectedPair() })
		if p.Remote.AddrPort() != q.Local.AddrPort() || q.Remote.AddrPort() != p.Local.AddrPort() {
			t.Fatalf("pairs %v->%v and %v->%v", p.Local.AddrPort(), p.Remote.AddrPort(), q.Local.AddrPort(), q.Remote.AddrPort())
		}
		t.Logf("controlling=%v selected %v <-> %v", controlling, p.Local.AddrPort(), p.Remote.AddrPort())
		x.close()
		y.close()
	}
}

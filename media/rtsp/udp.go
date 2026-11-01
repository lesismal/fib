package rtsp

import (
	"net/netip"
	"sync"
	"time"

	fib "github.com/lesismal/fib"
)

// udpSender sends datagrams to one address from a UDP binding of an
// engine. Engine.OpenUDPPeer, which gives the connection to send on, waits
// for the socket's event loop, so it runs on a goroutine of its own, and
// what is sent meanwhile waits, up to a bound, for it.
type udpSender struct {
	mu      sync.Mutex
	c       *fib.Connection
	queue   [][]byte
	queued  int
	limit   int
	opening bool
	closed  bool
	failed  bool
}

func newUDPSender(e *fib.Engine, binding int, addr netip.AddrPort, limit int) *udpSender {
	u := &udpSender{limit: limit, opening: true}
	go u.open(e, binding, addr)
	return u
}

func (u *udpSender) open(e *fib.Engine, binding int, addr netip.AddrPort) {
	c, err := e.OpenUDPPeer(binding, addr)
	u.mu.Lock()
	u.opening = false
	if err != nil || u.closed {
		u.failed = err != nil
		u.queue, u.queued = nil, 0
		u.mu.Unlock()
		return
	}
	// The peer belongs to the session, which closes it; fib's idle timeout
	// would close it under a player that only listens.
	c.SetUDPIdleTimeout(-1)
	u.c = c
	q := u.queue
	u.queue, u.queued = nil, 0
	u.mu.Unlock()
	for _, b := range q {
		_ = c.SendOwned(b)
	}
}

// send sends b, which it takes ownership of, and reports false when b is
// lost: the socket was full, or the queue while the peer opens was.
func (u *udpSender) send(b []byte) bool {
	u.mu.Lock()
	c := u.c
	if c == nil {
		ok := u.opening && !u.closed && u.queued+len(b) <= u.limit
		if ok {
			u.queue = append(u.queue, b)
			u.queued += len(b)
		}
		u.mu.Unlock()
		return ok
	}
	u.mu.Unlock()
	return c.SendOwned(b) == nil
}

// close closes the connection once open, or as soon as it opens.
func (u *udpSender) close() {
	u.mu.Lock()
	c := u.c
	u.closed, u.c, u.queue = true, nil, nil
	u.mu.Unlock()
	if c != nil {
		_ = c.Close()
	}
}

// unmapAddr returns ap with an IPv4-mapped IPv6 address as IPv4, the form
// routes are keyed by.
func unmapAddr(ap netip.AddrPort) netip.AddrPort {
	return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
}

// udpRoute is where the datagrams from one client address go: a stream of
// a session, its RTP or its RTCP.
type udpRoute struct {
	sc   *ServerConn
	sess *session
	st   *stream
	rtcp bool
}

// udpInfo returns the server's UDP bindings, nil before ListenUDP.
func (s *Server) udpInfo() *serverUDP {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.udp
}

func (s *Server) addRoute(addr netip.AddrPort, r *udpRoute) {
	s.mu.Lock()
	s.routes[unmapAddr(addr)] = r
	s.mu.Unlock()
}

func (s *Server) addSSRCRoute(ssrc uint32, r *udpRoute) {
	s.mu.Lock()
	if s.ssrcRoutes == nil {
		s.ssrcRoutes = make(map[uint32]*udpRoute)
	}
	s.ssrcRoutes[ssrc] = r
	s.mu.Unlock()
}

// removeRoutes removes the routes of a session.
func (s *Server) removeRoutes(sess *session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for addr, r := range s.routes {
		if r.sess == sess {
			delete(s.routes, addr)
		}
	}
	for ssrc, r := range s.ssrcRoutes {
		if r.sess == sess {
			delete(s.ssrcRoutes, ssrc)
		}
	}
}

// route finds the route of a datagram from addr: by the address, or for
// RTP from an address a NAT has changed, by the SSRC the client declared
// in its Transport, when the datagram comes from the client's IP. The
// address found by SSRC is remembered.
func (s *Server) route(addr netip.AddrPort, rtcp bool, b []byte) *udpRoute {
	addr = unmapAddr(addr)
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.routes[addr]; r != nil {
		return r
	}
	if rtcp || len(b) < 12 || len(s.ssrcRoutes) == 0 {
		return nil
	}
	ssrc := uint32(b[8])<<24 | uint32(b[9])<<16 | uint32(b[10])<<8 | uint32(b[11])
	r := s.ssrcRoutes[ssrc]
	if r == nil || r.st.clientRTP.Addr() != addr.Addr() {
		return nil
	}
	s.routes[addr] = r
	return r
}

// udpHandler serves the server's RTP or RTCP binding.
type udpHandler struct {
	s    *Server
	rtcp bool
}

func (h *udpHandler) OnOpen(*fib.Connection)                 {}
func (h *udpHandler) OnPriorityData(*fib.Connection, []byte) {}
func (h *udpHandler) OnClose(*fib.Connection, error)         {}

// OnData takes a datagram of a client: RTP of a stream it records, or RTCP,
// which shows it is there and, from a recorder, lines its streams up.
// Datagrams from addresses no session set up are dropped.
func (h *udpHandler) OnData(c *fib.Connection, data []byte) {
	r := h.s.route(c.RemoteAddrPort(), h.rtcp, data)
	if r == nil {
		return
	}
	sc := r.sc
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.closed || sc.sess != r.sess {
		return
	}
	now := time.Now()
	sc.touch(now)
	if r.rtcp {
		r.sess.receiveRTCP(r.st, now, data)
	} else {
		r.sess.receiveRTP(r.st, now, data)
	}
}

package socks

import (
	"errors"
	"net"
	"net/netip"
	"strconv"
	"sync"

	fib "github.com/lesismal/fib"
)

// maxWaitingDatagrams bounds the datagrams for one destination that wait
// for its socket to open; more are dropped, as a full socket would drop
// them.
const maxWaitingDatagrams = 16

// associate grants a UDP ASSOCIATE: the client is told the engine's UDP
// address, and the first datagrams to reach it from the client's IP are
// the client's.
func (s *Server) associate(sc *serverConn, req *Request) {
	relay := relayAddr(sc.c)
	if relay == nil {
		sc.refuse(ReplyCommandNotSupported)
		return
	}
	client := sc.c.RemoteAddrPort()
	a := &association{s: s, control: sc.c, ip: client.Addr().Unmap(), username: sc.username,
		targets: make(map[string]*udpTarget)}
	// The client may name the port it will send from. Its IP may be one a
	// NAT rewrites, and only datagrams from the IP its connection comes from
	// are taken anyway.
	if host, port, err := splitHostPort(req.Addr); err == nil {
		if ip, err := netip.ParseAddr(host); err == nil && (ip.IsUnspecified() || ip.Unmap() == a.ip) {
			a.port = port
		}
	}
	s.mu.Lock()
	s.associations[a.ip] = append(s.associations[a.ip], a)
	s.mu.Unlock()
	sc.mu.Lock()
	sc.state = stateAssociated
	sc.association = a
	// The control connection carries nothing after the request.
	sc.early = nil
	sc.mu.Unlock()
	sc.reply(ReplySucceeded, relay)
}

// relayAddr is the engine's UDP address that the client of c is to send its
// datagrams to: one of the family of the address c reached the server at,
// named by that address when the socket is bound to every address, or nil
// when the engine has none.
func relayAddr(c *fib.Connection) *net.UDPAddr {
	local, ok := c.LocalAddr().(*net.TCPAddr)
	if !ok {
		return nil
	}
	// An engine whose sockets cannot report their addresses has none to
	// offer.
	addrs, _ := c.Engine().LocalUDPAddrs()
	want4 := local.IP.To4() != nil
	for _, a := range addrs {
		if a.IP.IsUnspecified() {
			// An IPv6 socket bound to every address takes IPv4 too, unless
			// it is bound to "0.0.0.0", which takes nothing but IPv4.
			if a.IP.To4() != nil && !want4 {
				continue
			}
			return &net.UDPAddr{IP: local.IP, Port: a.Port}
		}
		if (a.IP.To4() != nil) == want4 {
			return a
		}
	}
	return nil
}

// association is a client's UDP association: the client's datagrams arrive
// at the engine's UDP socket, as the datagrams of a peer of it, and go on
// to their destinations over a UDP socket the server dials for each, whose
// answers go back to the client wrapped in the header that names them.
type association struct {
	s        *Server
	control  *fib.Connection
	ip       netip.Addr
	port     uint16
	username string

	mu sync.Mutex
	// src is where the client's datagrams come from, once the first has,
	// and peer the connection the engine keeps for it, while it does.
	src     netip.AddrPort
	peer    *fib.Connection
	targets map[string]*udpTarget
	opened  uint64
	closed  bool
}

// udpTarget is a destination of an association's datagrams, keyed by its
// address as the client named it.
type udpTarget struct {
	a   *association
	key string
	// header is what goes before a datagram from the destination: the
	// header the client's datagrams to it came with.
	header []byte
	seq    uint64
	// denied says Allow or AllowResolved refused the destination, whose
	// datagrams are dropped; it changes under the association's lock.
	denied bool
	// conn is the socket to it, once open, and waiting what arrived for it
	// before.
	conn    *fib.Connection
	waiting [][]byte
}

// claim finds the association that a datagram from c's peer belongs to,
// and gives it c, or reports that there is none.
func (s *Server) claim(c *fib.Connection) *association {
	src := c.RemoteAddrPort()
	src = netip.AddrPortFrom(src.Addr().Unmap(), src.Port())
	s.mu.Lock()
	defer s.mu.Unlock()
	// The association already fed from src, or else the first unclaimed one
	// that named src's port, that named none, or that named another, which
	// a NAT may have changed.
	var picks [4]*association
	for _, a := range s.associations[src.Addr()] {
		a.mu.Lock()
		claimed, port := a.src, a.port
		a.mu.Unlock()
		rank := 3
		switch {
		case claimed == src:
			rank = 0
		case claimed.IsValid():
			continue
		case port == src.Port():
			rank = 1
		case port == 0:
			rank = 2
		}
		if picks[rank] == nil {
			picks[rank] = a
		}
	}
	var a *association
	for _, a = range picks {
		if a != nil {
			break
		}
	}
	if a == nil {
		return nil
	}
	a.mu.Lock()
	a.src, a.peer = src, c
	a.mu.Unlock()
	return a
}

// unregister forgets a closed association.
func (s *Server) unregister(a *association) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.associations[a.ip]
	for i, x := range list {
		if x == a {
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(list) == 0 {
		delete(s.associations, a.ip)
	} else {
		s.associations[a.ip] = list
	}
}

// onDatagram takes a datagram a client sent to the engine's UDP socket.
func (s *Server) onDatagram(c *fib.Connection, data []byte) {
	a, _ := c.Attachment().(*association)
	if a == nil {
		if a = s.claim(c); a == nil {
			// No association expects it.
			c.Close()
			return
		}
		c.SetAttachment(a)
	}
	a.fromClient(c, data)
}

// fromClient sends a client's datagram on to the destination its header
// names. One with a fragment number is dropped, as RFC 1928 allows a server
// that does not reassemble fragments to.
func (a *association) fromClient(c *fib.Connection, data []byte) {
	if len(data) < 4 || data[0] != 0 || data[1] != 0 || data[2] != 0 {
		return
	}
	host, port, n, err := parseAddr(data[3:])
	if err != nil || n == 0 {
		return
	}
	header, payload := data[:3+n], data[3+n:]
	key := header[3:]
	a.mu.Lock()
	t := a.targets[string(key)]
	a.mu.Unlock()
	var evicted *fib.Connection
	if t == nil {
		req := &Request{Version: Version5, Command: CommandUDPAssociate,
			Addr: net.JoinHostPort(host, strconv.Itoa(int(port))), Username: a.username, Conn: a.control}
		denied := false
		if allow := a.s.config.Allow; allow != nil {
			denied = allow(req) != nil
		}
		// Only this peer's rounds add targets, one at a time, so the target
		// is still missing.
		t = &udpTarget{a: a, key: string(key), header: append([]byte(nil), header...), denied: denied}
		a.mu.Lock()
		if a.closed {
			a.mu.Unlock()
			return
		}
		evicted = a.addLocked(t)
		if !denied {
			t.waiting = append(t.waiting, payload)
		}
		a.mu.Unlock()
		if evicted != nil {
			evicted.Close()
		}
		if !denied {
			a.dial(t, req)
		}
		return
	}
	a.mu.Lock()
	if t.denied {
		a.mu.Unlock()
		return
	}
	conn := t.conn
	if conn == nil {
		if len(t.waiting) < maxWaitingDatagrams {
			t.waiting = append(t.waiting, payload)
		}
		a.mu.Unlock()
		return
	}
	a.mu.Unlock()
	_ = conn.Send(payload)
}

// addLocked adds t, making room for it by dropping the target opened
// longest ago when the association has as many as it may, and returns that
// target's socket for the caller to close.
func (a *association) addLocked(t *udpTarget) *fib.Connection {
	var evicted *fib.Connection
	if len(a.targets) >= a.s.config.MaxUDPTargets {
		var oldest *udpTarget
		for _, x := range a.targets {
			if oldest == nil || x.seq < oldest.seq {
				oldest = x
			}
		}
		delete(a.targets, oldest.key)
		evicted = oldest.conn
	}
	a.opened++
	t.seq = a.opened
	a.targets[t.key] = t
	return evicted
}

// dial opens the socket to a target.
func (a *association) dial(t *udpTarget, req *Request) {
	err := a.s.dial(a.control.Engine(), "udp", req, (*udpTargetHandler)(t),
		func(_ *fib.Connection, err error) {
			if err != nil {
				a.failed(t, err)
			}
		})
	if err != nil {
		a.failed(t, err)
	}
}

// failed forgets a target whose socket failed to open, or, when
// AllowResolved refused it, keeps it denied, so that its datagrams are
// dropped rather than resolved again.
func (a *association) failed(t *udpTarget, err error) {
	var refused *ReplyError
	if !errors.As(err, &refused) {
		a.drop(t)
		return
	}
	a.mu.Lock()
	t.denied = true
	t.waiting = nil
	a.mu.Unlock()
}

// drop forgets a target whose socket failed to open or has closed, so that
// the next datagram for it opens another.
func (a *association) drop(t *udpTarget) {
	a.mu.Lock()
	if a.targets[t.key] == t {
		delete(a.targets, t.key)
	}
	t.waiting = nil
	a.mu.Unlock()
}

// peerClosed lets go of the peer connection the engine kept for the client,
// which it closes once the client has been silent for its UDP idle timeout.
// The association lasts as long as its control connection, and the next
// datagram from the client opens another.
func (a *association) peerClosed(c *fib.Connection) {
	a.mu.Lock()
	if a.peer == c {
		a.peer = nil
	}
	a.mu.Unlock()
}

// close ends the association, when its control connection has closed.
func (a *association) close() {
	a.mu.Lock()
	a.closed = true
	peer := a.peer
	a.peer = nil
	targets := a.targets
	a.targets = nil
	a.mu.Unlock()
	for _, t := range targets {
		if t.conn != nil {
			t.conn.Close()
		}
	}
	if peer != nil {
		peer.Close()
	}
	a.s.unregister(a)
}

// udpTargetHandler serves the socket to a target.
type udpTargetHandler udpTarget

// OnOpen sends what waited for the socket, and closes one the association
// no longer wants.
func (h *udpTargetHandler) OnOpen(c *fib.Connection) {
	t := (*udpTarget)(h)
	a := t.a
	a.mu.Lock()
	if a.closed || a.targets[t.key] != t {
		a.mu.Unlock()
		c.Close()
		return
	}
	t.conn = c
	// Under the lock, so that what waited goes before what arrives next.
	for _, d := range t.waiting {
		_ = c.Send(d)
	}
	t.waiting = nil
	a.mu.Unlock()
}

// OnData sends a datagram from the target back to the client.
func (h *udpTargetHandler) OnData(_ *fib.Connection, data []byte) {
	t := (*udpTarget)(h)
	t.a.mu.Lock()
	peer := t.a.peer
	t.a.mu.Unlock()
	if peer != nil {
		_ = peer.SendParts(t.header, data)
	}
}

func (h *udpTargetHandler) OnPriorityData(*fib.Connection, []byte) {}

func (h *udpTargetHandler) OnClose(*fib.Connection, error) {
	t := (*udpTarget)(h)
	t.a.drop(t)
}

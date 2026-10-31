package webrtc

import (
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"sync"
	"syscall"
	"time"

	"github.com/lesismal/fib"
	"github.com/lesismal/fib/webrtc/ice"
)

// DefaultMuxIdleTimeout is how long a Mux keeps the fib connection of a
// remote address that has gone quiet, when MuxConfig.IdleTimeout is zero:
// longer than the 30 seconds ICE consent may go unanswered, so a path that
// is alive is never cut while its peer still consents, and a consent check
// every 5 seconds keeps it open however long media or data pause.
const DefaultMuxIdleTimeout = 45 * time.Second

// maxOpenQueue bounds the datagrams held for an address while its
// connection opens; ICE and DTLS send theirs again if these are lost.
const maxOpenQueue = 8

// errMuxClosed is what a PeerConnection reports for a Mux that has closed.
var errMuxClosed = errors.New("webrtc: mux closed")

// MuxConfig configures a Mux.
type MuxConfig struct {
	// Network is "udp", "udp4" or "udp6"; "udp" when empty.
	Network string
	// Addr is the address to listen on, as fib.Binding takes it: ":0" when
	// empty. A wildcard address advertises the address of every interface
	// that is up (ice.HostAddrs) as a host candidate; a specific one
	// advertises itself.
	Addr string
	// PublicAddrs, when set, are advertised as host candidates in place of
	// the interface addresses, with the port the Mux listens on: the
	// addresses of a server behind a one-to-one NAT, or of a load balancer
	// that forwards the port unchanged.
	PublicAddrs []netip.Addr
	// IdleTimeout is how long the connection of a remote address may go
	// without a datagram either way before fib closes it;
	// DefaultMuxIdleTimeout when zero.
	IdleTimeout time.Duration
}

// Mux is a UDP port of a fib engine that many PeerConnections share, as an
// ICE-UDP mux does for a server: every peer reaches every PeerConnection on
// it at the one address, and the Mux sorts what arrives by its sender.
//
// A datagram from an address a PeerConnection talks to goes to that
// PeerConnection, whatever it carries: STUN, DTLS, SRTP or SRTCP, told
// apart there by their first byte as RFC 7983 has it. A STUN Binding request
// from an address no PeerConnection talks to yet is how a peer first
// reaches a server, from an address of its own the server may never have
// been told of (a peer-reflexive candidate): its USERNAME begins with the
// ICE ufrag of the PeerConnection it is for, which the Mux looks up, and the
// ICE agent there checks its MESSAGE-INTEGRITY before the address is
// trusted with anything more. Anything else from an unknown address is
// dropped.
//
// Everything is keyed by remote address, never by fib connection: with
// fib's ReusePort a binding has a socket on every poller, and replies from
// one address may arrive on any of them. A Mux sends to an address through
// whichever connection it last saw for it, or opens one with
// Engine.OpenUDPPeer.
type Mux struct {
	engine  *fib.Engine
	binding int
	addrs   []netip.AddrPort
	idle    time.Duration

	mu      sync.RWMutex
	closed  bool
	ufrags  map[string]*PeerConnection
	peers   map[netip.AddrPort]*PeerConnection
	conns   map[netip.AddrPort]*fib.Connection
	opening map[netip.AddrPort][][]byte
}

// NewMux listens on a UDP port of engine, adding a binding for it, and
// returns the Mux that serves it. The engine may be running or not yet;
// the Mux serves once it runs.
func NewMux(engine *fib.Engine, config MuxConfig) (*Mux, error) {
	m := &Mux{
		engine:  engine,
		idle:    config.IdleTimeout,
		ufrags:  make(map[string]*PeerConnection),
		peers:   make(map[netip.AddrPort]*PeerConnection),
		conns:   make(map[netip.AddrPort]*fib.Connection),
		opening: make(map[netip.AddrPort][][]byte),
	}
	if m.idle <= 0 {
		m.idle = DefaultMuxIdleTimeout
	}
	network := config.Network
	if network == "" {
		network = "udp"
	}
	addr := config.Addr
	if addr == "" {
		addr = ":0"
	}
	i, err := engine.AddBinding(fib.Binding{Network: network, Addr: addr, Handler: muxHandler{m}})
	if err != nil {
		return nil, err
	}
	m.binding = i
	if m.addrs, err = hostAddrs(engine, i, config.PublicAddrs); err != nil {
		_ = engine.RemoveBinding(i)
		return nil, err
	}
	return m, nil
}

// hostAddrs lists the addresses a binding is reached at.
func hostAddrs(engine *fib.Engine, binding int, public []netip.Addr) ([]netip.AddrPort, error) {
	bound, err := engine.BindingAddrs(binding)
	if err != nil {
		return nil, err
	}
	var out []netip.AddrPort
	add := func(a netip.AddrPort) {
		for _, o := range out {
			if o == a {
				return
			}
		}
		out = append(out, a)
	}
	for _, b := range bound {
		ua, ok := b.(*net.UDPAddr)
		if !ok {
			continue
		}
		ap := ua.AddrPort()
		ip := ap.Addr().Unmap()
		switch {
		case len(public) > 0:
			for _, p := range public {
				add(netip.AddrPortFrom(p.Unmap(), ap.Port()))
			}
		case ip.IsUnspecified():
			hosts, err := ice.HostAddrs()
			if err != nil {
				return nil, err
			}
			for _, h := range hosts {
				// A socket bound to 0.0.0.0 cannot reach IPv6; one bound to
				// ::, as "udp" binds, takes both families.
				if ip.Is4() && !h.Is4() {
					continue
				}
				add(netip.AddrPortFrom(h, ap.Port()))
			}
		default:
			add(netip.AddrPortFrom(ip, ap.Port()))
		}
	}
	if len(out) == 0 {
		return nil, errors.New("webrtc: mux has no address to advertise")
	}
	return out, nil
}

// Addrs returns the addresses the Mux advertises as host candidates.
func (m *Mux) Addrs() []netip.AddrPort { return append([]netip.AddrPort(nil), m.addrs...) }

// Close stops listening: the binding is removed and its connections closed.
// PeerConnections still on the Mux can no longer send, and their ICE fails
// unless they are closed first.
func (m *Mux) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	clear(m.conns)
	m.mu.Unlock()
	return m.engine.RemoveBinding(m.binding)
}

// Len reports how many PeerConnections the Mux serves.
func (m *Mux) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	seen := make(map[*PeerConnection]struct{}, len(m.ufrags))
	for _, pc := range m.ufrags {
		seen[pc] = struct{}{}
	}
	return len(seen)
}

// register routes STUN requests for the local ufrag to pc.
func (m *Mux) register(ufrag string, pc *PeerConnection) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errMuxClosed
	}
	m.ufrags[ufrag] = pc
	return nil
}

// unregister forgets a ufrag of pc.
func (m *Mux) unregister(ufrag string, pc *PeerConnection) {
	m.mu.Lock()
	if m.ufrags[ufrag] == pc {
		delete(m.ufrags, ufrag)
	}
	m.mu.Unlock()
}

// bind routes everything from addr to pc. A PeerConnection binds the
// addresses it sends to and those its ICE agent has heard a valid request
// from. The latest PeerConnection to bind an address has it.
func (m *Mux) bind(addr netip.AddrPort, pc *PeerConnection) {
	m.mu.Lock()
	if !m.closed {
		m.peers[addr] = pc
	}
	m.mu.Unlock()
}

// release forgets pc's addresses and ufrags and closes the connections of
// its addresses, which no other PeerConnection uses.
func (m *Mux) release(pc *PeerConnection, addrs []netip.AddrPort) {
	var conns []*fib.Connection
	m.mu.Lock()
	for _, a := range addrs {
		if m.peers[a] != pc {
			continue
		}
		delete(m.peers, a)
		if c := m.conns[a]; c != nil {
			conns = append(conns, c)
			delete(m.conns, a)
		}
	}
	for u, p := range m.ufrags {
		if p == pc {
			delete(m.ufrags, u)
		}
	}
	m.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

// send sends b to addr. Without a connection for addr yet it opens one on
// a goroutine of its own, since Engine.OpenUDPPeer waits for the socket's
// event loop, and sends b, copied, once it is open.
func (m *Mux) send(addr netip.AddrPort, b []byte) {
	m.mu.RLock()
	c := m.conns[addr]
	closed := m.closed
	m.mu.RUnlock()
	if closed {
		return
	}
	if c != nil {
		if err := c.Send(b); err == nil || !errors.Is(err, net.ErrClosed) && !errors.Is(err, syscall.EPIPE) {
			return
		}
		m.mu.Lock()
		if m.conns[addr] == c {
			delete(m.conns, addr)
		}
		m.mu.Unlock()
	}
	m.mu.Lock()
	if q, ok := m.opening[addr]; ok {
		if len(q) < maxOpenQueue {
			m.opening[addr] = append(q, append([]byte(nil), b...))
		}
		m.mu.Unlock()
		return
	}
	m.opening[addr] = [][]byte{append([]byte(nil), b...)}
	m.mu.Unlock()
	go m.open(addr)
}

func (m *Mux) open(addr netip.AddrPort) {
	c, err := m.engine.OpenUDPPeer(m.binding, addr)
	m.mu.Lock()
	q := m.opening[addr]
	delete(m.opening, addr)
	if err == nil && !m.closed {
		if old := m.conns[addr]; old != nil {
			c = old
		} else {
			m.conns[addr] = c
			c.SetUDPIdleTimeout(m.idle)
		}
	}
	m.mu.Unlock()
	if err != nil {
		return
	}
	for _, b := range q {
		_ = c.Send(b)
	}
}

// muxHandler is the fib handler of a Mux's binding.
type muxHandler struct{ m *Mux }

func (h muxHandler) OnOpen(*fib.Connection)                 {}
func (h muxHandler) OnPriorityData(*fib.Connection, []byte) {}

func (h muxHandler) OnData(c *fib.Connection, b []byte) {
	if len(b) == 0 {
		return
	}
	m := h.m
	from := c.RemoteAddrPort()
	from = netip.AddrPortFrom(from.Addr().Unmap(), from.Port())
	m.mu.RLock()
	pc := m.peers[from]
	known := m.conns[from] == c
	if b[0] < 4 {
		// A request names the PeerConnection it is for, which may have
		// restarted ICE, or be another one checking the same peer.
		if u := requestUfrag(b); u != "" {
			if p := m.ufrags[u]; p != nil {
				pc = p
			}
		}
	}
	m.mu.RUnlock()
	if pc == nil {
		return
	}
	if !known {
		m.mu.Lock()
		if m.conns[from] == nil && !m.closed {
			m.conns[from] = c
			c.SetUDPIdleTimeout(m.idle)
		}
		m.mu.Unlock()
	}
	pc.handleDatagram(from, b)
}

func (h muxHandler) OnClose(c *fib.Connection, _ error) {
	m := h.m
	from := c.RemoteAddrPort()
	from = netip.AddrPortFrom(from.Addr().Unmap(), from.Port())
	m.mu.Lock()
	if m.conns[from] == c {
		delete(m.conns, from)
	}
	m.mu.Unlock()
}

// requestUfrag returns the local ufrag a STUN Binding request's USERNAME
// names, "<local ufrag>:<remote ufrag>", or "" for any other message. It
// reads the attributes in place, allocating only the ufrag it returns.
func requestUfrag(b []byte) string {
	const header = 20
	if len(b) < header || binary.BigEndian.Uint32(b[4:]) != 0x2112A442 {
		return ""
	}
	// A Binding request: method 0x001, class bits zero.
	if binary.BigEndian.Uint16(b)&0x3eef != 0x0001 || binary.BigEndian.Uint16(b)&0x0110 != 0 {
		return ""
	}
	n := header + int(binary.BigEndian.Uint16(b[2:]))
	if n > len(b) {
		return ""
	}
	for i := header; i+4 <= n; {
		t := binary.BigEndian.Uint16(b[i:])
		l := int(binary.BigEndian.Uint16(b[i+2:]))
		v := i + 4
		if v+l > n {
			return ""
		}
		if t == 0x0006 {
			user := b[v : v+l]
			for j, c := range user {
				if c == ':' {
					return string(user[:j])
				}
			}
			return ""
		}
		i = v + (l+3)&^3
	}
	return ""
}

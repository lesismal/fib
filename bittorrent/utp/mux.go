package utp

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"
)

// Defaults of a Config.
const (
	// DefaultPacketSize is the largest datagram a connection sends,
	// header included: below the 1500-byte Ethernet MTU with the IPv6 and
	// UDP headers and room to spare for a tunnel, as libutp's packets are.
	DefaultPacketSize = 1400
	// DefaultRecvBuffer is how many bytes a connection buffers for its
	// reader, the window it advertises when empty, as libutp's default.
	DefaultRecvBuffer = 1 << 20
	// DefaultSendBuffer is how many bytes Write queues before it blocks.
	DefaultSendBuffer = 1 << 20
	// DefaultTargetDelay is the queuing delay LEDBAT keeps to, BEP 29's
	// and libutp's 100ms.
	DefaultTargetDelay = 100 * time.Millisecond
	// DefaultMinRTO is the least retransmission timeout, BEP 29's 500ms
	// (libutp's is 1s).
	DefaultMinRTO = 500 * time.Millisecond
	// DefaultKeepAlive is how long a connection may send nothing before
	// it sends a keepalive, libutp's 29 seconds, short of the 30 a NAT
	// commonly keeps a UDP mapping for.
	DefaultKeepAlive = 29 * time.Second
	// DefaultIdleTimeout is how long a connection may hear nothing before
	// it is reset: several of libutp's keepalives, and more than the two
	// minutes BitTorrent peers may keep quiet.
	DefaultIdleTimeout = 3 * time.Minute
	// DefaultLinger is how long a closed connection keeps sending what it
	// had queued before it gives up and resets.
	DefaultLinger = 30 * time.Second
	// DefaultAcceptBacklog is how many connections may wait for Accept.
	DefaultAcceptBacklog = 128
)

// Errors of a connection.
var (
	// ErrReset is what a connection the peer reset reports.
	ErrReset = errors.New("utp: connection reset by peer")
	// ErrRefused is what Dial reports when the peer resets the connection
	// it asked for.
	ErrRefused = errors.New("utp: connection refused")
	// ErrTimeout ends a connection whose packets went unacknowledged
	// through their retransmissions, or whose handshake was never
	// answered.
	ErrTimeout = errors.New("utp: peer stopped acknowledging")
	// ErrIdle ends a connection that heard nothing for
	// Config.IdleTimeout.
	ErrIdle = errors.New("utp: peer idle")
)

// Config configures the connections of a Mux.
type Config struct {
	// PacketSize is the largest datagram sent, header included;
	// DefaultPacketSize when zero. There is no path MTU discovery.
	PacketSize int
	// RecvBuffer is how many bytes a connection buffers for its reader:
	// the window it advertises, closed while the reader keeps up with
	// nothing; DefaultRecvBuffer when zero.
	RecvBuffer int
	// SendBuffer is how many bytes Write queues beyond those in flight
	// before it blocks; DefaultSendBuffer when zero.
	SendBuffer int
	// TargetDelay is the queuing delay LEDBAT congestion control keeps
	// to; DefaultTargetDelay when zero.
	TargetDelay time.Duration
	// MinRTO is the least retransmission timeout; DefaultMinRTO when zero.
	MinRTO time.Duration
	// NoDelay sends a packet that is not full while others are in flight,
	// rather than holding the bytes until they fill one or the others are
	// acknowledged (libutp's Nagle-like coalescing, the default).
	NoDelay bool
	// KeepAlive is how long a connection may send nothing before it sends
	// a keepalive; DefaultKeepAlive when zero, never when negative.
	KeepAlive time.Duration
	// IdleTimeout ends a connection that hears nothing for that long, with
	// ErrIdle; DefaultIdleTimeout when zero, never when negative.
	IdleTimeout time.Duration
	// Linger is how long a connection Close closed keeps sending what was
	// queued and its FIN before it resets; DefaultLinger when zero.
	Linger time.Duration
	// AcceptBacklog bounds the connections waiting for Accept; a SYN past
	// it is dropped, for the peer to send again. DefaultAcceptBacklog when
	// zero; a negative value refuses every SYN, for a Mux that only dials.
	AcceptBacklog int
	// MaxConns bounds the connections of the Mux; 0 is unlimited.
	MaxConns int
	// Accept, when set, decides whether a peer's SYN gets a connection. It
	// runs on the goroutine that handed the Mux the datagram and must not
	// block.
	Accept func(remote netip.AddrPort) bool
}

func (cfg *Config) setDefaults() {
	if cfg.PacketSize <= 0 {
		cfg.PacketSize = DefaultPacketSize
	}
	cfg.PacketSize = max(cfg.PacketSize, HeaderSize+2+sackBytes+64)
	if cfg.RecvBuffer <= 0 {
		cfg.RecvBuffer = DefaultRecvBuffer
	}
	if cfg.SendBuffer <= 0 {
		cfg.SendBuffer = DefaultSendBuffer
	}
	if cfg.TargetDelay <= 0 {
		cfg.TargetDelay = DefaultTargetDelay
	}
	if cfg.MinRTO <= 0 {
		cfg.MinRTO = DefaultMinRTO
	}
	if cfg.KeepAlive == 0 {
		cfg.KeepAlive = DefaultKeepAlive
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = DefaultIdleTimeout
	}
	if cfg.Linger <= 0 {
		cfg.Linger = DefaultLinger
	}
	if cfg.AcceptBacklog == 0 {
		cfg.AcceptBacklog = DefaultAcceptBacklog
	}
}

// connKey is how the Mux finds a connection: the peer's address and the
// connection ID the peer sends to it with.
type connKey struct {
	addr netip.AddrPort
	id   uint16
}

// rstKey names a RESET already sent, so that a peer that keeps sending to
// a connection that is gone is not answered every time, as libutp's
// rst_info.
type rstKey struct {
	addr netip.AddrPort
	id   uint16
	seq  uint16
}

const (
	rstInfoTimeout = 10 * time.Second
	rstInfoLimit   = 1000
)

// Mux is uTP without a socket (sans-I/O): the connections of one UDP
// port, told apart by peer address and connection ID. Inbound datagrams
// come in through HandleDatagram, from whatever reads the socket, and
// what the connections send goes out through the caller's send function,
// so that the one UDP port can also carry the DHT and UDP tracker
// traffic, sorted by IsUTP. Socket is a Mux on a fib UDP binding.
//
// HandleDatagram defers the acknowledgements and the packets that a
// datagram causes until Flush, which the caller calls once the burst of
// datagrams it read is handled, as libutp's utp_issue_deferred_acks: a
// burst is then answered with one acknowledgement per connection and one
// batch of datagrams. The connections' own timers, and their readers and
// writers, send at once.
//
// A Mux is a net.Listener of the connections peers start.
type Mux struct {
	cfg   Config
	send  func(to netip.AddrPort, dgrams [][]byte)
	local atomic.Pointer[net.Addr]

	mu      sync.Mutex
	conns   map[connKey]*Conn
	rsts    map[rstKey]int64
	closed  bool
	accepts chan *Conn
	die     chan struct{}

	dirtyMu sync.Mutex
	dirty   []*Conn
	spare   []*Conn

	// randSeq picks a connection's first sequence number; tests set it to
	// start near the wrap.
	randSeq func() uint16
}

// NewMux returns a Mux whose connections send through send, which is
// given the datagrams for one peer at a time, valid only until it
// returns. send is called with a connection's mutex held: it must not
// block, nor call back into the Mux; a datagram it cannot send is lost,
// as on the network. local is what LocalAddr and Addr report.
func NewMux(cfg Config, local net.Addr, send func(to netip.AddrPort, dgrams [][]byte)) *Mux {
	cfg.setDefaults()
	m := &Mux{
		cfg:     cfg,
		send:    send,
		conns:   make(map[connKey]*Conn),
		rsts:    make(map[rstKey]int64),
		accepts: make(chan *Conn, max(cfg.AcceptBacklog, 1)),
		die:     make(chan struct{}),
		randSeq: rand16,
	}
	if local != nil {
		m.local.Store(&local)
	}
	return m
}

func rand16() uint16 {
	var b [2]byte
	_, _ = rand.Read(b[:])
	return binary.BigEndian.Uint16(b[:])
}

func unmap(a netip.AddrPort) netip.AddrPort {
	return netip.AddrPortFrom(a.Addr().Unmap(), a.Port())
}

// HandleDatagram takes a datagram from the peer at from, and reports
// whether it was uTP; one that was not is left to the caller. b is not
// kept. What it causes to be sent waits for Flush.
func (m *Mux) HandleDatagram(from netip.AddrPort, b []byte) bool {
	h, sack, payload, ok := parse(b)
	if !ok {
		return false
	}
	from = unmap(from)
	now := micros()
	switch h.typ {
	case stReset:
		// The ID is the one we receive with, or the one we send with,
		// either side of it as we did or did not start the connection.
		m.mu.Lock()
		c := m.conns[connKey{from, h.connID}]
		if c == nil {
			if o := m.conns[connKey{from, h.connID + 1}]; o != nil && o.sendID == h.connID {
				c = o
			} else if o := m.conns[connKey{from, h.connID - 1}]; o != nil && o.sendID == h.connID {
				c = o
			}
		}
		m.mu.Unlock()
		if c != nil {
			c.mu.Lock()
			c.reset()
			c.unlockQuiet()
		}
	case stSyn:
		// A SYN carries the ID the initiator receives with; the
		// connection it starts receives with the next.
		m.mu.Lock()
		c := m.conns[connKey{from, h.connID + 1}]
		m.mu.Unlock()
		if c != nil {
			if !c.initiator && c.sendID == h.connID {
				c.mu.Lock()
				c.dupSyn()
				c.unlockQuiet()
			}
			return true
		}
		m.accept(from, h, now)
	default:
		m.mu.Lock()
		c := m.conns[connKey{from, h.connID}]
		m.mu.Unlock()
		if c == nil {
			m.sendReset(from, h, now)
			return true
		}
		c.mu.Lock()
		c.process(h, sack, payload, now)
		c.unlockQuiet()
	}
	return true
}

// Flush sends what the datagrams HandleDatagram took since the last
// Flush caused: acknowledgements, window updates, retransmissions and the
// data acknowledgements made room for.
func (m *Mux) Flush() {
	m.dirtyMu.Lock()
	list := m.dirty
	m.dirty, m.spare = m.spare[:0], nil
	m.dirtyMu.Unlock()
	for _, c := range list {
		c.mu.Lock()
		c.inDirty = false
		c.unlock()
	}
	clear(list)
	m.dirtyMu.Lock()
	if m.spare == nil {
		m.spare = list[:0]
	}
	m.dirtyMu.Unlock()
}

// markDirty queues c for the next Flush; c's mutex is held.
func (m *Mux) markDirty(c *Conn) {
	if c.inDirty {
		return
	}
	c.inDirty = true
	m.dirtyMu.Lock()
	m.dirty = append(m.dirty, c)
	m.dirtyMu.Unlock()
}

// accept starts the connection of a SYN.
func (m *Mux) accept(from netip.AddrPort, h header, now int64) {
	if m.cfg.AcceptBacklog < 0 || (m.cfg.Accept != nil && !m.cfg.Accept(from)) {
		return
	}
	m.mu.Lock()
	key := connKey{from, h.connID + 1}
	if m.closed || m.conns[key] != nil || len(m.accepts) >= cap(m.accepts) ||
		(m.cfg.MaxConns > 0 && len(m.conns) >= m.cfg.MaxConns) {
		m.mu.Unlock()
		return
	}
	c := m.newConn(from, h.connID+1, h.connID, false)
	c.mu.Lock()
	c.state = stateSynRecv
	c.ackNr = h.seq
	c.seqNr = m.randSeq()
	c.fastResendSeq = c.seqNr
	c.peerWnd = int(h.wnd)
	c.lastHeard = now
	c.handshakeAt = now + int64(handshakeTimeout/time.Microsecond)
	if h.ts != 0 {
		c.replyMicro = uint32(now) - h.ts
	}
	c.ackDue = true
	m.markDirty(c)
	m.conns[key] = c
	m.accepts <- c
	m.mu.Unlock()
	c.unlockQuiet()
}

// sendReset answers a packet for a connection the Mux does not have.
func (m *Mux) sendReset(to netip.AddrPort, h header, now int64) {
	key := rstKey{to, h.connID, h.seq}
	m.mu.Lock()
	if at, ok := m.rsts[key]; ok && now-at < int64(rstInfoTimeout/time.Microsecond) {
		m.rsts[key] = now
		m.mu.Unlock()
		return
	}
	if len(m.rsts) >= rstInfoLimit {
		for k, at := range m.rsts {
			if now-at >= int64(rstInfoTimeout/time.Microsecond) {
				delete(m.rsts, k)
			}
		}
		if len(m.rsts) >= rstInfoLimit {
			m.mu.Unlock()
			return
		}
	}
	m.rsts[key] = now
	m.mu.Unlock()
	var b [HeaderSize]byte
	putHeader(b[:], header{typ: stReset, connID: h.connID, ts: uint32(now), seq: m.randSeq(), ack: h.seq})
	m.send(to, [][]byte{b[:]})
}

func (m *Mux) newConn(remote netip.AddrPort, recvID, sendID uint16, initiator bool) *Conn {
	c := &Conn{
		m:         m,
		cfg:       &m.cfg,
		remote:    remote,
		recvID:    recvID,
		sendID:    sendID,
		initiator: initiator,
		connected: make(chan struct{}),
		die:       make(chan struct{}),
		readable:  make(chan struct{}, 1),
		writable:  make(chan struct{}, 1),
	}
	c.init()
	return c
}

// forget removes a connection that has ended.
func (m *Mux) forget(c *Conn) {
	m.mu.Lock()
	if k := (connKey{c.remote, c.recvID}); m.conns[k] == c {
		delete(m.conns, k)
	}
	m.mu.Unlock()
	if fn := c.onGone; fn != nil {
		fn()
	}
}

// Dial starts a connection to the peer at to, and waits for its answer,
// for the context, or for the handshake to time out.
func (m *Mux) Dial(ctx context.Context, to netip.AddrPort) (*Conn, error) {
	to = unmap(to)
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, net.ErrClosed
	}
	id := m.freeID(to)
	c := m.newConn(to, id, id+1, true)
	m.conns[connKey{to, id}] = c
	m.mu.Unlock()

	c.mu.Lock()
	now := micros()
	c.state = stateSynSent
	c.seqNr = m.randSeq()
	c.fastResendSeq = c.seqNr
	c.lastHeard = now
	syn := &outPacket{seq: c.seqNr, typ: stSyn}
	c.out.put(syn.seq, syn, syn.seq)
	c.seqNr++
	c.inflightPkts = 1
	c.retransmitTimeout = initialRTO
	c.rtoAt = now + int64(initialRTO)*1000
	c.transmit(syn, now)
	c.unlock()

	select {
	case <-c.connected:
	case <-ctx.Done():
		c.mu.Lock()
		c.abort(ctx.Err())
		c.unlock()
		return nil, ctx.Err()
	}
	c.mu.Lock()
	err := c.err
	ok := c.state != stateDone
	c.mu.Unlock()
	if !ok {
		if err == nil {
			err = net.ErrClosed
		}
		return nil, err
	}
	return c, nil
}

// rekey gives a connection being dialed another pair of IDs, under which
// its SYN goes again. c's mutex is held: taking the Mux's after it is safe,
// for the Mux holds its own before a connection's only for one that is
// new, which nothing else knows yet. IDs are read under either mutex.
func (m *Mux) rekey(c *Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := m.freeID(c.remote)
	if k := (connKey{c.remote, c.recvID}); m.conns[k] == c {
		delete(m.conns, k)
	}
	c.recvID, c.sendID = id, id+1
	if !m.closed {
		m.conns[connKey{c.remote, id}] = c
	}
}

// freeID picks a receive ID for a connection to to, neither it nor the
// next in use; m.mu is held.
func (m *Mux) freeID(to netip.AddrPort) uint16 {
	for {
		id := rand16()
		if m.conns[connKey{to, id}] == nil && m.conns[connKey{to, id + 1}] == nil {
			return id
		}
	}
}

// Accept waits for the next connection a peer starts, as net.Listener.
func (m *Mux) Accept() (net.Conn, error) { return m.AcceptUTP() }

// AcceptUTP is Accept returning the *Conn.
func (m *Mux) AcceptUTP() (*Conn, error) {
	select {
	case c := <-m.accepts:
		return c, nil
	case <-m.die:
		return nil, net.ErrClosed
	}
}

// Addr returns the local address the Mux was made with.
func (m *Mux) Addr() net.Addr {
	if a := m.local.Load(); a != nil {
		return *a
	}
	return &net.UDPAddr{}
}

// Len reports how many connections the Mux has.
func (m *Mux) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.conns)
}

// Close resets every connection and refuses new ones.
func (m *Mux) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	close(m.die)
	all := make([]*Conn, 0, len(m.conns))
	for _, c := range m.conns {
		all = append(all, c)
	}
	m.mu.Unlock()
	for _, c := range all {
		c.mu.Lock()
		c.abort(net.ErrClosed)
		c.unlock()
	}
	return nil
}

// abortAddr resets the connections of the peer at addr.
func (m *Mux) abortAddr(addr netip.AddrPort, err error) {
	var all []*Conn
	m.mu.Lock()
	for k, c := range m.conns {
		if k.addr == addr {
			all = append(all, c)
		}
	}
	m.mu.Unlock()
	for _, c := range all {
		c.mu.Lock()
		c.abort(err)
		c.unlock()
	}
}

var _ net.Listener = (*Mux)(nil)

//go:build linux || darwin || windows

package fib

import (
	"errors"
	"net"
	"net/netip"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/lesismal/fib/bufferpool"
)

// UDP rides on the same connections, handlers and workers as TCP. What is
// different is who reads the socket and what a connection is.
//
// A listening UDP socket has no connections of its own, so the engine makes
// one per peer address: the first datagram from an address opens a
// connection for it, OnOpen runs, and every datagram from that address is
// then that connection's input. The connection closes when it is closed, or
// when the peer has been silent for Config.UDPIdleTimeout. A dialed UDP
// connection is a socket connected to one peer and is simply that peer's.
//
// The event loop reads UDP sockets itself and queues each datagram on its
// connection, and a worker hands the queue to OnData one datagram per call.
// Reading on the loop is what lets many peers share one socket without any of
// them reading another's datagrams, and it keeps datagram boundaries intact:
// OnData receives exactly one datagram, and each Send sends exactly one.
//
// Sends go straight to the socket and are never queued. A datagram the socket
// has no room for is dropped and Send reports the error, which is what UDP
// does anyway when a router has no room for it, so there is no backpressure
// and no write watermark to pause reads on.

// udpState is what a UDP connection keeps on top of an ordinary one.
type udpState struct {
	udpPlatform
	// listener is the socket a peer's datagrams arrive on and its replies
	// leave from, or nil for a dialed connection, which has its own socket.
	listener *udpListener
	// sa and key are a peer's address, as the socket calls take it and as the
	// listener's peer table is keyed. raddr is the same for RemoteAddr.
	sa    syscall.Sockaddr
	key   netip.AddrPort
	raddr *net.UDPAddr
	// queue holds datagrams the loop has read and the handler has not seen
	// yet, from head on. Guarded by the connection's mu. spare is the array
	// the queue last handed to a DatagramsHandler, kept for the next swap.
	queue [][]byte
	head  int
	spare [][]byte
	// lastActive is when the peer last sent or was sent a datagram, in
	// nanoseconds, for the idle timeout.
	lastActive atomic.Int64
}

// udpListener is a bound UDP socket and the peers it has seen.
type udpListener struct {
	udpListenerPlatform
	// peers maps an address to its open connection. Event-loop ownership.
	peers map[netip.AddrPort]*Connection
}

// rawSockaddrKey reads the table key of the peer a receive left its address
// in, and the scope of an IPv6 one, which the key leaves out. It reads the
// raw address where the kernel wrote it rather than through a
// syscall.Sockaddr, which would be an allocation for every datagram when only
// a new peer needs one.
func rawSockaddrKey(rsa *syscall.RawSockaddrAny) (key netip.AddrPort, zone uint32, ok bool) {
	switch rsa.Addr.Family {
	case syscall.AF_INET:
		pp := (*syscall.RawSockaddrInet4)(unsafe.Pointer(rsa))
		port := (*[2]byte)(unsafe.Pointer(&pp.Port))
		return netip.AddrPortFrom(netip.AddrFrom4(pp.Addr), uint16(port[0])<<8|uint16(port[1])), 0, true
	case syscall.AF_INET6:
		pp := (*syscall.RawSockaddrInet6)(unsafe.Pointer(rsa))
		port := (*[2]byte)(unsafe.Pointer(&pp.Port))
		return netip.AddrPortFrom(netip.AddrFrom16(pp.Addr), uint16(port[0])<<8|uint16(port[1])), pp.Scope_id, true
	}
	return netip.AddrPort{}, 0, false
}

// keySockaddr is the socket address a peer's replies are sent to: the
// address its key was read from.
func keySockaddr(key netip.AddrPort, zone uint32) syscall.Sockaddr {
	if addr := key.Addr(); addr.Is4() {
		return &syscall.SockaddrInet4{Port: int(key.Port()), Addr: addr.As4()}
	}
	return &syscall.SockaddrInet6{Port: int(key.Port()), ZoneId: zone, Addr: key.Addr().As16()}
}

func sockaddrToUDPAddr(sa syscall.Sockaddr) *net.UDPAddr {
	addr, err := sockaddrToTCPAddr(sa)
	if err != nil {
		return nil
	}
	return &net.UDPAddr{IP: addr.IP, Port: addr.Port, Zone: addr.Zone}
}

// udpPeer returns the connection for the peer whose address a receive left
// in from, opening one if this is the first datagram from it. It returns nil
// once the engine is stopping. Callers run on the event loop.
func (e *Engine) udpPeer(l *udpListener, from *syscall.RawSockaddrAny) *Connection {
	key, zone, ok := rawSockaddrKey(from)
	if !ok {
		return nil
	}
	if c := l.peers[key]; c != nil {
		return c
	}
	if e.stopping.Load() {
		return nil
	}
	sa := keySockaddr(key, zone)
	c := &Connection{engine: e, handler: e.handler,
		udp: &udpState{listener: l, sa: sa, key: key, raddr: sockaddrToUDPAddr(sa)}}
	c.initUDPPeer()
	c.udp.lastActive.Store(time.Now().UnixNano())
	l.peers[key] = c
	c.handler.OnOpen(c)
	return c
}

// deliverDatagram queues a copy of one datagram on its connection and reports
// the connection if that made it runnable. The copy is a buffer from package
// bufferpool, which a handler done with it may give back. now, in Unix
// nanoseconds, is when the datagram was read, which a read shares between
// the datagrams it took rather than ask the clock for each. Callers run on
// the event loop.
func (e *Engine) deliverDatagram(c *Connection, data []byte, now int64) *Connection {
	u := c.udp
	c.mu.Lock()
	if c.closing || c.closed || len(u.queue)-u.head >= maxQueuedDatagrams {
		c.mu.Unlock()
		return nil
	}
	if u.head == len(u.queue) {
		u.queue = u.queue[:0]
		u.head = 0
	}
	datagram := bufferpool.Get(len(data))
	copy(datagram, data)
	u.queue = append(u.queue, datagram)
	c.mu.Unlock()
	u.lastActive.Store(now)
	return e.noteEvent(c, evIn)
}

// drainDatagrams hands every queued datagram to the handler, one per OnData,
// or all of them at once to a DatagramsHandler.
func (c *Connection) drainDatagrams() {
	u := c.udp
	if h, ok := c.handler.(DatagramsHandler); ok {
		for {
			c.mu.Lock()
			if u.head == len(u.queue) || c.closing || c.closed {
				c.mu.Unlock()
				return
			}
			queue := u.queue
			batch := queue[u.head:]
			// The loop queues what arrives meanwhile on the spare array, so
			// the batch is the handler's alone while it runs.
			u.queue, u.head, u.spare = u.spare[:0], 0, nil
			c.mu.Unlock()
			h.OnDatagrams(c, batch)
			clear(queue)
			c.mu.Lock()
			if u.spare == nil {
				u.spare = queue[:0]
			}
			c.mu.Unlock()
		}
	}
	for {
		c.mu.Lock()
		if u.head == len(u.queue) || c.closing || c.closed {
			c.mu.Unlock()
			return
		}
		data := u.queue[u.head]
		u.queue[u.head] = nil
		u.head++
		c.mu.Unlock()
		c.handler.OnData(c, data)
	}
}

// sendDatagram sends data as one datagram, or drops it and reports why.
func (c *Connection) sendDatagram(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.closed || c.closeAfterSend {
		return syscall.EPIPE
	}
	err := c.sysSendDatagram(data)
	if err == nil {
		c.udp.lastActive.Store(time.Now().UnixNano())
	}
	return err
}

// SendBatch sends each of datagrams as a datagram of its own, in order, as
// that many Sends would, but on Linux and macOS with one system call for as
// many of them as it can: sendmmsg, or sendmsg_x. A datagram the socket has
// no room for is dropped, with those after it, and SendBatch reports why;
// those before it were sent. On a connection that is not UDP it Sends each
// in turn.
func (c *Connection) SendBatch(datagrams [][]byte) error {
	batch := c.udp != nil
	for _, d := range datagrams {
		// Send sends nothing for an empty datagram, where a batch would
		// send an empty one.
		batch = batch && len(d) > 0
	}
	if !batch {
		for _, d := range datagrams {
			if err := c.Send(d); err != nil {
				return err
			}
		}
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.closed || c.closeAfterSend {
		return syscall.EPIPE
	}
	err := c.sysSendDatagrams(datagrams)
	if err == nil {
		c.udp.lastActive.Store(time.Now().UnixNano())
	}
	return err
}

// sendDatagramParts sends two parts as one datagram.
func (c *Connection) sendDatagramParts(first, second []byte) error {
	if len(second) == 0 {
		return c.sendDatagram(first)
	}
	if len(first) == 0 {
		return c.sendDatagram(second)
	}
	// sendDatagram hands the bytes to the socket before it returns, so the
	// joined copy goes straight back to the pool.
	data := bufferpool.Join(nil, first, second)
	defer bufferpool.Put(data)
	return c.sendDatagram(data)
}

// detachPeer drops a closed peer from its listener's table. The socket is the
// listener's, so there is nothing to close. Callers run on the event loop.
func (e *Engine) detachPeer(c *Connection) {
	u := c.udp
	if u.listener.peers[u.key] == c {
		delete(u.listener.peers, u.key)
	}
}

// startUDPSweeper starts the ticker that has the loop look for idle peers.
func (e *Engine) startUDPSweeper() {
	if len(e.udpListeners) == 0 || e.udpIdleTimeout <= 0 {
		return
	}
	e.udpSweepDone = make(chan struct{})
	go func(done <-chan struct{}) {
		ticker := time.NewTicker(udpSweepInterval(e.udpIdleTimeout))
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if !e.stopping.Load() && !e.request(command{kind: commandUDPSweep}) {
					return
				}
			}
		}
	}(e.udpSweepDone)
}

func (e *Engine) stopUDPSweeper() {
	if e.udpSweepDone != nil {
		close(e.udpSweepDone)
		e.udpSweepDone = nil
	}
}

// sweepUDP closes the peers that have been silent for the idle timeout.
// Callers run on the event loop.
func (e *Engine) sweepUDP() {
	limit := int64(e.udpIdleTimeout)
	if limit <= 0 {
		return
	}
	now := time.Now().UnixNano()
	for _, l := range e.udpListeners {
		for _, c := range l.peers {
			if now-c.udp.lastActive.Load() >= limit {
				e.closeConnection(c, ErrUDPIdleTimeout, true)
			}
		}
	}
}

// closeUDPPeers closes every peer without a callback, as Close does for TCP
// connections.
func (e *Engine) closeUDPPeers() {
	for _, l := range e.udpListeners {
		for _, c := range l.peers {
			e.closeConnection(c, nil, false)
		}
	}
}

// LocalUDPAddrs returns one address per UDP listener, in configured order.
// Ports left at zero report the port the kernel chose.
func (e *Engine) LocalUDPAddrs() ([]*net.UDPAddr, error) {
	addrs := make([]*net.UDPAddr, 0, len(e.udpListeners))
	for _, l := range e.udpListeners {
		sa, err := l.sockname()
		if err != nil {
			return nil, err
		}
		addr := sockaddrToUDPAddr(sa)
		if addr == nil {
			return nil, syscall.EAFNOSUPPORT
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

// LocalUDPAddr returns the address of the engine's first UDP listener.
func (e *Engine) LocalUDPAddr() (*net.UDPAddr, error) {
	addrs, err := e.LocalUDPAddrs()
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, errNoListener
	}
	return addrs[0], nil
}

// errUDPRead is what Read reports on a UDP connection, whose datagrams the
// event loop reads and delivers whole.
var errUDPRead = errors.New("fib: a UDP connection is read through OnData")

// Protocol returns the transport the connection runs over, which it keeps
// for its whole life, after it closes too.
func (c *Connection) Protocol() Protocol {
	switch {
	case c.udp != nil:
		return ProtocolUDP
	case c.unix:
		return ProtocolUnix
	}
	return ProtocolTCP
}

// RemoteAddr returns the peer's address: a *net.UDPAddr for a UDP
// connection, a *net.UnixAddr for a Unix socket, whose name is empty when the
// peer never bound one, and a *net.TCPAddr otherwise. It returns nil once the
// socket is gone.
func (c *Connection) RemoteAddr() net.Addr {
	if c.udp != nil {
		return c.udp.raddr
	}
	sa, err := c.peerSockaddr()
	if err != nil {
		return nil
	}
	return sockaddrToAddr(sa)
}

//go:build linux || darwin

package fib

import (
	"net"
	"net/netip"
	"sync/atomic"
	"syscall"
	"time"
)

// pollersSupported says Config.IOPollers applies: the readiness backends
// can run one loop per poller.
const pollersSupported = true

const (
	// A token's low half is always a descriptor and its high half says what
	// that descriptor is: one of the server's listeners, the wake-up eventfd,
	// or a connection. For a connection the high half doubles as a generation,
	// so an event left over from a descriptor's previous owner resolves to
	// nothing. Tagging rather than reserving whole token values is what lets a
	// server carry any number of listeners.
	listenerKind = uint64(0)
	wakeKind     = uint64(1)
	udpKind      = uint64(2)
	// firstGeneration seeds the generation half of a connection token. Starting
	// above the reserved kinds keeps every connection token distinct from a
	// listener or wake token whatever descriptor it lands on.
	firstGeneration = uint64(3)
	// connPageShift sizes one page of the descriptor table. 4096 entries is
	// 32KB per page, small enough that a server holding few connections in a
	// wide descriptor range wastes little, large enough that the page
	// directory stays tiny.
	connPageShift = 12
	connPageSize  = 1 << connPageShift
	connPageMask  = connPageSize - 1
)

// enginePlatform is the state a readiness backend keeps on top of the shared
// engine: the listening descriptors and the table that maps a descriptor back
// to its connection. backend is the epoll or kqueue instance itself.
type enginePlatform struct {
	backend
	// udpBatch is what the loop reads UDP datagrams into.
	udpBatch  *udpBatch
	listenFDs []int
	// pollersListen says the engine's pollers accept its connections on
	// sockets of their own, and its listeners only hold their addresses;
	// see Config.ReusePort.
	pollersListen bool
	// tcpListeners says the listeners accept TCP connections, which get
	// TCP_NODELAY as they are accepted.
	tcpListeners   bool
	nextGeneration atomic.Uint64
	// connections is a paged table indexed by file descriptor: a descriptor is
	// a small dense integer the kernel already allocates, so the lookup on
	// every event is two bounds checks rather than a hash.
	//
	// It is paged rather than flat because descriptors are handed out per
	// process while this table is per server. A process running one server per
	// listening port sees every server's descriptors drawn from one
	// interleaved range, so a flat table would grow to the highest descriptor
	// in the process no matter how few connections this server holds, and
	// doubling its way there copied 102MB across a 100k-connection dial.
	// Pages are allocated once, on demand, and never copied.
	// Event-loop ownership.
	connections [][]*Connection
}

type connPlatform struct {
	fd    atomic.Int32
	token uint64
}

// udpPlatform is empty here: a dialed UDP connection reads its own descriptor
// and a peer sends through its listener's.
type udpPlatform struct{}

type udpListenerPlatform struct{ fd int }

// FD returns the connection's descriptor, or -1 once it is closed. A UDP peer
// has no descriptor of its own and always reports -1.
func (c *Connection) FD() int { return int(c.fd.Load()) }

func (c *Connection) initUDPPeer() { c.fd.Store(-1) }

func (l *udpListener) sockname() (syscall.Sockaddr, error) { return syscall.Getsockname(l.fd) }

func (c *Connection) peerSockaddr() (syscall.Sockaddr, error) { return syscall.Getpeername(c.FD()) }

func (c *Connection) sockname() (syscall.Sockaddr, error) { return syscall.Getsockname(c.FD()) }

// sysSendDatagram sends one datagram. Callers hold c.mu.
func (c *Connection) sysSendDatagram(data []byte) error {
	for {
		var err error
		if l := c.udp.listener; l != nil {
			err = syscall.Sendto(l.fd, data, 0, c.udp.sa)
		} else {
			_, err = syscall.Write(c.FD(), data)
		}
		if err != syscall.EINTR {
			return err
		}
	}
}

// sysSendDatagrams sends datagrams, none of them empty, in batches. Callers
// hold c.mu.
func (c *Connection) sysSendDatagrams(datagrams [][]byte) error {
	fd, to := c.FD(), syscall.Sockaddr(nil)
	if l := c.udp.listener; l != nil {
		fd, to = l.fd, c.udp.sa
	}
	for len(datagrams) > 0 {
		n, err := sendBatch(fd, to, datagrams)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return err
		}
		datagrams = datagrams[n:]
	}
	return nil
}

func (e *Engine) open(config Config, addrs []string) error {
	e.nextGeneration.Store(firstGeneration)
	e.tcpListeners = !isUnixNetwork(config.Network) && !isUDPNetwork(config.Network)
	if p := e.parent; p != nil && p.pollersListen {
		// A poller that accepts for itself listens beside its parent's
		// other pollers, on every address the parent is bound to.
		for _, like := range p.listenFDs {
			fd, err := createListenerLike(config, like)
			if err != nil {
				e.closeListeners()
				return err
			}
			e.listenFDs = append(e.listenFDs, fd)
		}
	}
	udp := isUDPNetwork(config.Network)
	e.pollersListen = e.parent == nil && pollersListen(config)
	for _, addr := range addrs {
		if udp {
			fd, err := createUDPListener(config, addr)
			if err != nil {
				e.closeListeners()
				return err
			}
			e.udpListeners = append(e.udpListeners, &udpListener{udpListenerPlatform: udpListenerPlatform{fd: fd},
				peers: make(map[netip.AddrPort]*Connection)})
			continue
		}
		// When the pollers listen, the engine's own socket only holds the
		// address, and the port the kernel chose for it: were it to listen
		// too, it would be handed a share of the connections that nothing
		// accepts.
		fd, err := createListener(config, addr, !e.pollersListen)
		if err != nil {
			e.closeListeners()
			return err
		}
		e.listenFDs = append(e.listenFDs, fd)
		e.noteUnixPath(config.Network, addr)
	}
	if err := e.openBackend(); err != nil {
		e.closeListeners()
		return err
	}
	return nil
}

func (e *Engine) closeListeners() {
	for _, fd := range e.listenFDs {
		syscall.Close(fd)
	}
	e.listenFDs = nil
	for _, l := range e.udpListeners {
		syscall.Close(l.fd)
	}
	e.udpListeners = nil
	e.removeUnixPaths()
}

func listenerToken(fd int) uint64 { return uint64(uint32(fd)) | listenerKind<<32 }
func wakeToken(fd int) uint64     { return uint64(uint32(fd)) | wakeKind<<32 }
func udpToken(fd int) uint64      { return uint64(uint32(fd)) | udpKind<<32 }

// udpListenerAt returns the UDP listener on a descriptor, or nil.
func (e *Engine) udpListenerAt(fd int) *udpListener {
	for _, l := range e.udpListeners {
		if l.fd == fd {
			return l
		}
	}
	return nil
}

// readUDPListener reads the datagrams waiting on a UDP listener and queues
// each on its peer's connection, opening connections for new peers. The socket
// is level-triggered, so it reads at most one round's share and leaves the
// rest to the next round.
//
// It reads in batches, and stops at a batch that comes back short, which
// says the socket is empty, rather than at a read that finds nothing. avail
// is how many bytes of datagrams the poller reported waiting, or -1 where it
// does not say. Where it says, the round reads the first datagram with an
// ordinary receive, since most rounds find just the one and a batch of one
// costs more, batches only when the count says several are left after it
// (see nextRead), and stops once it has had that many bytes. What arrived
// since is the next round's.
// Callers run on the event loop.
func (e *Engine) readUDPListener(l *udpListener, avail int, ready []*Connection) []*Connection {
	b := e.datagramBatch()
	counted := avail >= 0
	last := 0
	for read := 0; read < maxDatagramsPerRound; {
		want, one := nextRead(read, avail, last)
		n, empty, err := b.recv(l.fd, want, one)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return ready
		}
		now := time.Now().UnixNano()
		for i := 0; i < n; i++ {
			data := b.datagram(i)
			avail -= len(data)
			if c := e.udpPeer(l, &b.names[i]); c != nil {
				if r := e.deliverDatagram(c, data, now); r != nil {
					ready = append(ready, r)
				}
			}
		}
		read += n
		last = len(b.datagram(n - 1))
		if empty || counted && avail <= 0 {
			return ready
		}
	}
	return ready
}

// minCountedBatch is the fewest datagrams a round the poller counts reads
// with a batch: a batched receive costs more than an ordinary one, about a
// quarter more on macOS, and pays for itself only when it takes several.
const minCountedBatch = 4

// nextRead is how many datagrams a round's next receive asks for, and
// whether it is an ordinary receive of one rather than a batch: read is how
// many the round has had, left how many bytes of datagrams the poller counted
// as still waiting, or -1 where it does not count, and last the size of the
// last one read, from which left gives how many there are.
func nextRead(read, left, last int) (int, bool) {
	want := min(udpBatchSize, maxDatagramsPerRound-read)
	switch {
	case left < 0:
		return want, false
	case read == 0 || last <= 0:
		return 1, true
	}
	if more := (left + last - 1) / last; more >= minCountedBatch {
		return min(want, more), false
	}
	return 1, true
}

// readUDPConnection reads a dialed UDP connection's datagrams, as
// readUDPListener does for a listener's. An error, such as the refusal a
// connected socket reports when nothing listens at the peer's port, closes the
// connection. Callers run on the event loop.
func (e *Engine) readUDPConnection(c *Connection, avail int, ready []*Connection) []*Connection {
	b := e.datagramBatch()
	counted := avail >= 0
	last := 0
	for read := 0; read < maxDatagramsPerRound; {
		want, one := nextRead(read, avail, last)
		n, empty, err := b.recv(c.FD(), want, one)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			if !isWouldBlock(err) {
				c.closeWithError(err)
			}
			return ready
		}
		now := time.Now().UnixNano()
		for i := 0; i < n; i++ {
			data := b.datagram(i)
			avail -= len(data)
			if r := e.deliverDatagram(c, data, now); r != nil {
				ready = append(ready, r)
			}
		}
		read += n
		last = len(b.datagram(n - 1))
		if empty || counted && avail <= 0 {
			return ready
		}
	}
	return ready
}

// datagramBatch returns the loop's batch to read datagrams into.
func (e *Engine) datagramBatch() *udpBatch {
	if e.udpBatch == nil {
		e.udpBatch = newUDPBatch()
	}
	return e.udpBatch
}

// connectionAt returns the connection currently holding a descriptor.
func (e *Engine) connectionAt(fd int) *Connection {
	page := fd >> connPageShift
	if page < 0 || page >= len(e.connections) {
		return nil
	}
	entries := e.connections[page]
	if entries == nil {
		return nil
	}
	return entries[fd&connPageMask]
}

// connectionFor returns the connection a token refers to, or nil if the token
// is stale. The low half is the descriptor and the high half a generation, so a
// reused descriptor never resolves to the connection that previously held it.
func (e *Engine) connectionFor(token uint64) *Connection {
	c := e.connectionAt(int(uint32(token)))
	if c == nil || c.token != token {
		return nil
	}
	return c
}

// trackConnection records a newly accepted connection, growing the descriptor
// table to cover it. Callers run on the event loop.
func (e *Engine) trackConnection(fd int, c *Connection) {
	page := fd >> connPageShift
	if page >= len(e.connections) {
		// Only the page directory is ever copied, and it holds one pointer per
		// 4096 descriptors, so growing it stays cheap however high descriptors
		// climb.
		directory := make([][]*Connection, max(page+1, 2*len(e.connections)))
		copy(directory, e.connections)
		e.connections = directory
	}
	if e.connections[page] == nil {
		e.connections[page] = make([]*Connection, connPageSize)
	}
	e.connections[page][fd&connPageMask] = c
}

func (e *Engine) isListener(fd int) bool {
	for _, listenFD := range e.listenFDs {
		if listenFD == fd {
			return true
		}
	}
	return false
}

// ListenAddrs returns the address of every listener, in configured order,
// whatever its network: a *net.TCPAddr, *net.UnixAddr or *net.UDPAddr. Ports
// left at zero report the port the kernel chose.
func (e *Engine) ListenAddrs() ([]net.Addr, error) {
	addrs := make([]net.Addr, 0, len(e.listenFDs)+len(e.udpListeners))
	for _, fd := range e.listenFDs {
		sa, err := syscall.Getsockname(fd)
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, sockaddrToAddr(sa))
	}
	udpAddrs, err := e.LocalUDPAddrs()
	if err != nil {
		return nil, err
	}
	for _, addr := range udpAddrs {
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

// LocalAddrs returns one address per TCP listener, in configured order. Ports
// left at zero report the port the kernel chose. It fails for an engine
// listening on Unix sockets; ListenAddrs reports those.
func (e *Engine) LocalAddrs() ([]*net.TCPAddr, error) {
	addrs := make([]*net.TCPAddr, 0, len(e.listenFDs))
	for _, fd := range e.listenFDs {
		sa, err := syscall.Getsockname(fd)
		if err != nil {
			return nil, err
		}
		addr, err := sockaddrToTCPAddr(sa)
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

// Run serves the engine until Stop. With pollers, it runs each of them on a
// goroutine of its own while this one accepts, and returns once they have all
// returned; a poller that fails stops the rest.
func (e *Engine) Run() error {
	e.logRun()
	if len(e.pollers) == 0 {
		return e.runLoop()
	}
	errs := make(chan error, len(e.pollers))
	for _, p := range e.pollers {
		go func(p *Engine) {
			err := p.runLoop()
			if err != nil {
				e.Stop()
			}
			errs <- err
		}(p)
	}
	err := e.runLoop()
	e.Stop()
	for range e.pollers {
		if pollerErr := <-errs; err == nil {
			err = pollerErr
		}
	}
	return err
}

func (e *Engine) acceptConnections(listenFD int) {
	for {
		fd, err := acceptSocket(listenFD)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return
		}
		if e.tcpListeners {
			// Replies are written whole, so Nagle would only hold a small
			// one back until the peer's delayed ACK.
			_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
		}
		if len(e.pollers) == 0 {
			e.admit(&Connection{engine: e, handler: e.handler, unix: !e.tcpListeners}, fd)
			continue
		}
		// The poller registers the descriptor itself, since its table and
		// its backend are its loop's alone.
		p := e.pollers[fd%len(e.pollers)]
		c := &Connection{engine: p, handler: e.handler, unix: !e.tcpListeners}
		c.fd.Store(int32(fd))
		if !p.request(command{kind: commandAccept, connection: c}) {
			syscall.Close(fd)
		}
	}
}

// admitAccepted admits a connection the engine's own loop accepted and
// handed to this poller. One that arrives after the poller stopped is closed
// instead, since nothing would ever serve it. Callers run on the event loop.
func (e *Engine) admitAccepted(c *Connection) {
	fd := c.FD()
	if e.stopping.Load() {
		c.fd.Store(-1)
		syscall.Close(fd)
		return
	}
	e.admit(c, fd)
}

// admit registers an accepted descriptor with the loop and opens its
// connection. Callers run on the event loop.
func (e *Engine) admit(c *Connection, fd int) {
	// The token pairs the descriptor with a generation: the descriptor
	// indexes the table, and the generation makes an event left over from a
	// previous owner of the same descriptor resolve to nothing.
	token := uint64(uint32(fd)) | e.nextGeneration.Add(1)<<32
	c.token = token
	c.fd.Store(int32(fd))
	// Write interest is registered up front and never modified again. The
	// descriptor is edge-triggered, so an always-armed write interest only
	// fires when the socket goes from full back to writable, which spares
	// the loop a registration change per backpressured message.
	if err := e.registerConnection(fd, token); err != nil {
		c.fd.Store(-1)
		syscall.Close(fd)
		return
	}
	e.trackConnection(fd, c)
	c.handler.OnOpen(c)
}

// detach releases a closed connection's descriptor and its table slot.
func (e *Engine) detach(c *Connection) {
	if c.udp != nil && c.udp.listener != nil {
		e.detachPeer(c)
		return
	}
	fd := int(c.fd.Swap(-1))
	if fd < 0 {
		return
	}
	e.unregister(fd)
	_ = syscall.Close(fd)
	if page := fd >> connPageShift; page < len(e.connections) {
		if entries := e.connections[page]; entries != nil && entries[fd&connPageMask] == c {
			entries[fd&connPageMask] = nil
		}
	}
}

// Close releases all resources. Run must have returned before Close is called.
func (e *Engine) Close() error {
	var closeErr error
	e.closeOnce.Do(func() {
		e.Stop()
		e.stopUDPSweeper()
		// The pollers' connections are the engine's, so they close first,
		// while the task pool they run on is still there.
		for _, p := range e.pollers {
			if err := p.Close(); err != nil && closeErr == nil {
				closeErr = err
			}
		}
		e.taskWG.Wait()
		e.releaseTaskPool()
		e.releaseWorkerPool()
		e.closeCommands()
		for _, entries := range e.connections {
			for _, c := range entries {
				if c != nil {
					e.closeConnection(c, nil, false)
				}
			}
		}
		e.closeUDPPeers()
		e.budgetPaused = nil
		for _, fd := range e.listenFDs {
			if err := syscall.Close(fd); err != nil && closeErr == nil {
				closeErr = err
			}
		}
		for _, l := range e.udpListeners {
			if err := syscall.Close(l.fd); err != nil && closeErr == nil {
				closeErr = err
			}
		}
		e.removeUnixPaths()
		if err := e.closeBackend(); err != nil && closeErr == nil {
			closeErr = err
		}
	})
	return closeErr
}

// pollersListen reports whether config has each poller listen on a socket
// of its own and accept its connections itself; see Config.ReusePort.
func pollersListen(config Config) bool {
	return config.ReusePort && reusePortSpreads && pollerCount(config) > 0 &&
		!isUnixNetwork(config.Network) && !isUDPNetwork(config.Network)
}

// createListener opens a socket bound to addr, and listening on it unless
// listen is false.
func createListener(config Config, addr string, listen bool) (int, error) {
	family, bound, err := resolveListenAddr(config.Network, addr)
	if err != nil {
		return -1, err
	}
	return listenSocket(config, family, bound, listen)
}

// createListenerLike opens a socket listening where like is bound, which
// SO_REUSEPORT lets it share with like; see Config.ReusePort.
func createListenerLike(config Config, like int) (int, error) {
	bound, err := syscall.Getsockname(like)
	if err != nil {
		return -1, err
	}
	family := syscall.AF_INET
	if _, ok := bound.(*syscall.SockaddrInet6); ok {
		family = syscall.AF_INET6
	}
	return listenSocket(config, family, bound, true)
}

func listenSocket(config Config, family int, bound syscall.Sockaddr, listen bool) (int, error) {
	fd, err := newSocket(family)
	if err != nil {
		return -1, err
	}
	if family != syscall.AF_UNIX {
		_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
		if config.ReusePort {
			if err = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, soReusePort, 1); err != nil {
				syscall.Close(fd)
				return -1, err
			}
		}
	}
	if family == syscall.AF_INET6 {
		// "tcp" accepts both families on one socket; "tcp6" is IPv6 only. This
		// is the distinction net.Listen draws between the two networks.
		v6only := 0
		if config.Network == "tcp6" {
			v6only = 1
		}
		_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_IPV6, syscall.IPV6_V6ONLY, v6only)
	}
	if err = syscall.Bind(fd, bound); err == nil && listen {
		err = syscall.Listen(fd, config.Backlog)
	}
	if err != nil {
		syscall.Close(fd)
		return -1, err
	}
	return fd, nil
}

func createUDPListener(config Config, addr string) (int, error) {
	family, bound, err := resolveListenAddr(config.Network, addr)
	if err != nil {
		return -1, err
	}
	fd, err := newDatagramSocket(family)
	if err != nil {
		return -1, err
	}
	_ = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	if family == syscall.AF_INET6 {
		v6only := 0
		if config.Network == "udp6" {
			v6only = 1
		}
		_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_IPV6, syscall.IPV6_V6ONLY, v6only)
	}
	if err = syscall.Bind(fd, bound); err != nil {
		syscall.Close(fd)
		return -1, err
	}
	return fd, nil
}

// The readiness backends never have a write outstanding in the kernel: a
// blocked write simply waits for the next write edge, which is always armed.
func (c *Connection) writeBusyLocked() bool      { return false }
func (c *Connection) awaitWritableLocked() error { return nil }
func (c *Connection) rearmRead()                 {}

func isWouldBlock(err error) bool { return err == syscall.EAGAIN || err == syscall.EWOULDBLOCK }

func (c *Connection) sysRead(buf []byte) (int, error)  { return syscall.Read(c.FD(), buf) }
func (c *Connection) sysWrite(buf []byte) (int, error) { return syscall.Write(c.FD(), buf) }

func (c *Connection) sysRecvOOB(buf []byte) (int, error) {
	n, _, err := syscall.Recvfrom(c.FD(), buf, syscall.MSG_OOB)
	return n, err
}

func (c *Connection) sysWrite2(first, second []byte) (int, error) {
	if len(first) == 0 {
		return syscall.Write(c.FD(), second)
	}
	if len(second) == 0 {
		return syscall.Write(c.FD(), first)
	}
	var iov [2]syscall.Iovec
	iov[0].Base = &first[0]
	iov[0].SetLen(len(first))
	iov[1].Base = &second[0]
	iov[1].SetLen(len(second))
	return writevRaw(c.FD(), iov[:])
}

func (c *Connection) sysWritev(buffers [][]byte) (int, error) {
	var iov [maxWritevItems]syscall.Iovec
	count := 0
	for _, b := range buffers {
		if len(b) > 0 {
			if count == len(iov) {
				break
			}
			iov[count].Base = &b[0]
			iov[count].SetLen(len(b))
			count++
		}
	}
	if count == 0 {
		return 0, nil
	}
	return writevRaw(c.FD(), iov[:count])
}

func (c *Connection) socketError() error {
	errno, err := syscall.GetsockoptInt(c.FD(), syscall.SOL_SOCKET, syscall.SO_ERROR)
	if err != nil {
		return err
	}
	if errno != 0 {
		return syscall.Errno(errno)
	}
	return syscall.ECONNRESET
}

//go:build linux || darwin

package fib

import (
	"io"
	"net"
	"net/netip"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/internal/listener"
	"github.com/lesismal/fib/internal/netaddr"
	"github.com/lesismal/fib/internal/sys"
	"github.com/lesismal/fib/internal/udpbatch"
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
	udpBatch  *udpbatch.Batch
	listenFDs []int
	// pollersListen says the engine's pollers accept its connections, or
	// read its datagrams, on sockets of their own, and its TCP listeners only
	// hold their addresses; see Config.ReusePort.
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
		n, err := udpbatch.Send(fd, to, datagrams)
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
	e.tcpListeners = !netaddr.IsUnix(config.Network) && !netaddr.IsUDP(config.Network)
	if p := e.parent; p != nil && p.pollersListen {
		// A poller that accepts for itself listens beside its parent's
		// other pollers, on every address the parent is bound to.
		for _, like := range p.listenFDs {
			fd, err := listener.CreateLike(listenOptions(config), like)
			if err != nil {
				e.closeListeners()
				return err
			}
			e.listenFDs = append(e.listenFDs, fd)
		}
		if err := e.openUDPBeside(config, p); err != nil {
			e.closeListeners()
			return err
		}
	}
	udp := netaddr.IsUDP(config.Network)
	e.pollersListen = e.parent == nil && pollersListen(config)
	for _, addr := range addrs {
		if udp {
			fd, err := listener.CreateUDP(listenOptions(config), addr)
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
		fd, err := listener.Create(listenOptions(config), addr, !e.pollersListen, e.pollersListen)
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

// openUDPBeside gives a poller of an engine whose pollers read its UDP
// addresses themselves (see Config.ReusePort) a socket on each of them. The
// first poller takes the engine's own sockets, and every other one binds one
// of its own beside them. The engine cannot keep a socket only to hold the
// address, as it does a TCP listener it leaves unlistened: a bound UDP socket
// is in its address's SO_REUSEPORT group whatever else it does, and would be
// handed a share of the datagrams that nothing reads.
func (e *Engine) openUDPBeside(config Config, parent *Engine) error {
	if len(parent.udpListeners) > 0 {
		e.udpListeners, parent.udpListeners = parent.udpListeners, nil
		return nil
	}
	if len(parent.pollers) == 0 {
		return nil
	}
	for _, like := range parent.pollers[0].udpListeners {
		fd, err := listener.CreateUDPLike(listenOptions(config), like.fd)
		if err != nil {
			return err
		}
		e.udpListeners = append(e.udpListeners, &udpListener{udpListenerPlatform: udpListenerPlatform{fd: fd},
			peers: make(map[netip.AddrPort]*Connection)})
	}
	return nil
}

// stopListening stops the engine's TCP listeners taking connections without
// giving up their descriptors; see stopListeningFD.
func (e *Engine) stopListening() {
	if !e.tcpListeners {
		return
	}
	for _, fd := range e.listenFDs {
		stopListeningFD(fd)
	}
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

// newToken pairs fd with the next generation. The engine and its pollers
// draw on one counter, the engine's, so that a descriptor reused on another
// poller, which accepting on sockets of their own lets happen, still comes
// back with a token of its own.
func (e *Engine) newToken(fd int) uint64 {
	return uint64(uint32(fd)) | e.root().nextGeneration.Add(1)<<32
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
		n, empty, err := b.Recv(l.fd, want, one)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return ready
		}
		now := time.Now().UnixNano()
		for i := 0; i < n; i++ {
			data := b.Datagram(i)
			avail -= len(data)
			if c := e.udpPeer(l, b.Addr(i)); c != nil {
				if r := e.deliverDatagram(c, data, now); r != nil {
					ready = append(ready, r)
				}
			}
		}
		read += n
		last = len(b.Datagram(n - 1))
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
	want := min(udpbatch.Size, maxDatagramsPerRound-read)
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
		n, empty, err := b.Recv(c.FD(), want, one)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			if !sys.IsWouldBlock(err) {
				c.closeWithError(err)
			}
			return ready
		}
		now := time.Now().UnixNano()
		for i := 0; i < n; i++ {
			data := b.Datagram(i)
			avail -= len(data)
			if r := e.deliverDatagram(c, data, now); r != nil {
				ready = append(ready, r)
			}
		}
		read += n
		last = len(b.Datagram(n - 1))
		if empty || counted && avail <= 0 {
			return ready
		}
	}
	return ready
}

// datagramBatch returns the loop's batch to read datagrams into.
func (e *Engine) datagramBatch() *udpbatch.Batch {
	if e.udpBatch == nil {
		e.udpBatch = udpbatch.New()
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
		addrs = append(addrs, netaddr.ToAddr(sa))
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
		addr, err := netaddr.ToTCPAddr(sa)
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
		fd, peer, err := sys.AcceptSocket(listenFD)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return
		}
		if e.tcpListeners && !sys.AcceptedInheritNoDelay {
			// Replies are written whole, so Nagle would only hold a small
			// one back until the peer's delayed ACK.
			_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
		}
		if len(e.pollers) == 0 {
			e.admit(&Connection{engine: e, handler: e.handler, unix: !e.tcpListeners, peer: peer}, fd)
			continue
		}
		// The poller registers the descriptor itself, since its table and
		// its backend are its loop's alone.
		p := e.pollers[fd%len(e.pollers)]
		c := &Connection{engine: p, handler: e.handler, unix: !e.tcpListeners, peer: peer}
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
	token := e.newToken(fd)
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
	// Closing the last descriptor of a socket takes it out of the event
	// loop's interest set, so only one that may have been duplicated needs
	// taking out first, a system call every connection would otherwise make.
	if c.rawExposed.Load() {
		e.unregister(fd)
	}
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
		// No connection is taken from here on. Closing the ones there are
		// tells their peers, which may connect again at once, and with a
		// listener for each of many pollers some would still be listening:
		// on 128 CPUs, 64 pollers' listeners, a client reconnecting 10ms
		// after the server closed its connection was mostly let in. A
		// listening socket that is shut down stops listening, and keeps its
		// descriptor until it is closed below, so no loop that has yet to
		// see Stop can be left waiting on a descriptor reused elsewhere.
		e.stopListening()
		for _, p := range e.pollers {
			p.stopListening()
		}
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
// of its own, accepting its TCP connections or reading its UDP datagrams
// itself. Under IOPollers it does for TCP whatever ReusePort says, where
// SO_REUSEPORT spreads connections; see Config.ReusePort. UDP needs
// ReusePort, since a peer's datagrams move between sockets as sockets join.
func pollersListen(config Config) bool {
	return (config.ReusePort || !netaddr.IsUDP(config.Network)) && sys.ReusePortSpreads &&
		pollerCount(config) > 0 && !netaddr.IsUnix(config.Network)
}

// listenOptions is what of config the listening sockets are opened by.
func listenOptions(config Config) listener.Options {
	return listener.Options{Network: config.Network, Backlog: config.Backlog, ReusePort: config.ReusePort}
}

// The readiness backends never have a write outstanding in the kernel: a
// blocked write simply waits for the next write edge, which is always armed.
func (c *Connection) writeBusyLocked() bool      { return false }
func (c *Connection) awaitWritableLocked() error { return nil }
func (c *Connection) rearmRead()                 {}

func (c *Connection) sysRead(buf []byte) (int, error) {
	return sys.Read(c.FD(), buf, c.engine.socketSyscalls)
}

func (c *Connection) sysWrite(buf []byte) (int, error) {
	return sys.Write(c.FD(), buf, c.engine.socketSyscalls)
}

func (c *Connection) sysRecvOOB(buf []byte) (int, error) {
	n, _, err := syscall.Recvfrom(c.FD(), buf, syscall.MSG_OOB)
	return n, err
}

func (c *Connection) sysWrite2(first, second []byte) (int, error) {
	if len(first) == 0 {
		return sys.Write(c.FD(), second, c.engine.socketSyscalls)
	}
	if len(second) == 0 {
		return sys.Write(c.FD(), first, c.engine.socketSyscalls)
	}
	var iov [2]syscall.Iovec
	iov[0].Base = &first[0]
	iov[0].SetLen(len(first))
	iov[1].Base = &second[0]
	iov[1].SetLen(len(second))
	return sys.Writev(c.FD(), iov[:], c.engine.socketSyscalls)
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
	return sys.Writev(c.FD(), iov[:count], c.engine.socketSyscalls)
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

// dialLoop picks the loop a dial is to run on. With pollers, that is the one
// its descriptor picks, as for an accepted connection, so the socket is
// opened here, on whatever goroutine asked for the dial, rather than on the
// loop. A dial that cannot open one runs on the engine's own loop, which
// only has to report why.
func (e *Engine) dialLoop(d *dialRequest) *Engine {
	if len(e.pollers) == 0 || d.err != nil || d.hasSocket {
		return e
	}
	fd, err := d.openSocket()
	if err != nil {
		d.err = err
		return e
	}
	d.socket, d.hasSocket = fd, true
	return e.pollers[fd%len(e.pollers)]
}

// openSocket returns the socket opened for d ahead of time, if there is one,
// and otherwise opens one of the kind d's network needs. The caller owns it.
func (d *dialRequest) openSocket() (int, error) {
	if d.hasSocket {
		d.hasSocket = false
		return d.socket, nil
	}
	if netaddr.IsUDP(d.network) {
		return sys.NewDatagramSocket(d.family)
	}
	return sys.NewSocket(d.family)
}

// closeDialSocket closes a socket opened for a dial that never connected it.
func closeDialSocket(fd int) { _ = syscall.Close(fd) }

// connectSocket opens a non-blocking socket, starts connecting it and
// registers it with the backend, where the connect's outcome arrives as the
// socket's first events. connected reports a connect the kernel finished on the
// spot, as it may over loopback. Callers run on the event loop.
func (e *Engine) connectSocket(d *dialRequest) (c *Connection, connected bool, err error) {
	if netaddr.IsUDP(d.network) {
		return e.connectDatagram(d)
	}
	fd, err := d.openSocket()
	if err != nil {
		return nil, false, err
	}
	if !netaddr.IsUnix(d.network) {
		// As for an accepted connection; see acceptConnections.
		_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
	}
	for {
		err = syscall.Connect(fd, d.sa)
		if err != syscall.EINTR {
			break
		}
	}
	switch err {
	case nil:
		connected = true
	case syscall.EINPROGRESS, syscall.EALREADY:
	default:
		syscall.Close(fd)
		return nil, false, err
	}
	token := e.newToken(fd)
	c = &Connection{engine: e, handler: d.handler, dialing: d, dialed: true, unix: netaddr.IsUnix(d.network)}
	c.token = token
	c.fd.Store(int32(fd))
	// Registering an unconnected socket is what makes the connect
	// asynchronous: its first write edge says the connect has finished, one
	// way or the other. A connect that finished before the registration still
	// raises that edge, since adding a descriptor reports its current state.
	if err = e.registerConnection(fd, token); err != nil {
		syscall.Close(fd)
		return nil, false, err
	}
	e.trackConnection(fd, c)
	return c, connected, nil
}

// connectDatagram opens a UDP socket connected to the dialed peer. Connecting
// a UDP socket only records the peer, so it is done on the spot.
func (e *Engine) connectDatagram(d *dialRequest) (c *Connection, connected bool, err error) {
	fd, err := d.openSocket()
	if err != nil {
		return nil, false, err
	}
	for {
		err = syscall.Connect(fd, d.sa)
		if err != syscall.EINTR {
			break
		}
	}
	if err != nil {
		syscall.Close(fd)
		return nil, false, err
	}
	token := e.newToken(fd)
	c = &Connection{engine: e, handler: d.handler, dialing: d, dialed: true,
		udp: &udpState{raddr: &net.UDPAddr{IP: d.raddr.IP, Port: d.raddr.Port, Zone: d.raddr.Zone}}}
	c.token = token
	c.fd.Store(int32(fd))
	if err = e.registerDatagram(fd, token); err != nil {
		syscall.Close(fd)
		return nil, false, err
	}
	e.trackConnection(fd, c)
	return c, true, nil
}

// finishDial settles a connect from the events its socket raised and reports
// the connection if those events also need a worker. reported is the error the
// backend delivered with the events, if it carries one; the socket's own error
// takes precedence. Callers run on the event loop.
func (e *Engine) finishDial(c *Connection, events uint32, reported error) *Connection {
	fd := c.FD()
	var err error
	errno, sockErr := syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_ERROR)
	switch {
	case sockErr != nil:
		err = sockErr
	case errno == int(syscall.EINPROGRESS), errno == int(syscall.EALREADY), errno == int(syscall.EINTR):
		// Still connecting: an event can arrive before the connect settles.
		return nil
	case errno != 0 && errno != int(syscall.EISCONN):
		err = syscall.Errno(errno)
	case reported != nil:
		err = reported
	}
	if err == nil {
		// A clean socket error does not prove the connect finished, since
		// events can arrive spuriously. Having a peer does.
		if _, peerErr := syscall.Getpeername(fd); peerErr != nil {
			if events&(evErr|evHup) == 0 {
				return nil
			}
			// The kernel says the socket is finished but names no error.
			// Waiting would never end: no further edge is coming.
			err = syscall.ECONNREFUSED
		}
	}
	if err != nil {
		e.failDial(c, err)
		return nil
	}
	e.completeDial(c)
	// Whatever else arrived with the connect, bytes the peer sent straight
	// away above all, is now the connection's to handle.
	return e.noteEvent(c, events)
}

// maxSendFileCall bounds one sendfile call, below the 2GB Linux takes at most.
const maxSendFileCall = 1 << 30

// fileSegment is the part of a file SendFile has yet to send, read through
// the connection's own duplicate of the caller's descriptor.
type fileSegment struct {
	fd        int
	offset    int64
	remaining int64
	// copyBuf is set once sendfile has refused this pair of descriptors, as
	// macOS does for anything but a TCP socket, after which the segment is
	// copied through it instead.
	copyBuf []byte
}

func newFileSegment(f File, offset, count int64) (*fileSegment, error) {
	rc, err := f.SyscallConn()
	if err != nil {
		return nil, err
	}
	dup := -1
	var dupErr error
	err = rc.Control(func(fd uintptr) {
		syscall.ForkLock.RLock()
		dup, dupErr = syscall.Dup(int(fd))
		if dupErr == nil {
			syscall.CloseOnExec(dup)
		}
		syscall.ForkLock.RUnlock()
	})
	if err == nil {
		err = dupErr
	}
	if err != nil {
		return nil, err
	}
	return &fileSegment{fd: dup, offset: offset, remaining: count}, nil
}

func (s *fileSegment) advance(n int64) {
	s.offset += n
	s.remaining -= n
}

func (s *fileSegment) close() {
	if s.fd >= 0 {
		_ = syscall.Close(s.fd)
		s.fd = -1
	}
	// A copy through this segment is a synchronous write, so nothing is
	// reading the staging buffer by the time the segment ends.
	bufferpool.Put(s.copyBuf)
	s.copyBuf = nil
}

// sysSendFileLocked sends what it can of the file item at the head of the
// queue. fromFile reports that the bytes went by sendfile, from the file
// rather than from memory. Callers hold c.mu.
func (c *Connection) sysSendFileLocked(item *sendItem) (n, attempted int, fromFile bool, err error) {
	s := item.file
	attempted = int(min(s.remaining, maxSendFileCall))
	if s.copyBuf == nil {
		for {
			offset := s.offset
			n, err = syscall.Sendfile(c.FD(), s.fd, &offset, attempted)
			n = max(n, 0)
			if err == syscall.EINTR && n == 0 {
				continue
			}
			if n > 0 && err != nil {
				// macOS reports what it sent along with EAGAIN or EINTR. A
				// partial count stands, and a full socket raises the write
				// edge that resumes the rest; an interrupted call is short
				// only because it was interrupted, so it must not wait for an
				// edge that may never come.
				if err == syscall.EINTR {
					attempted = n
				}
				err = nil
			}
			break
		}
		switch {
		case err == nil && n == 0:
			return 0, attempted, true, io.ErrUnexpectedEOF
		case err == syscall.EINVAL || err == syscall.ENOTSUP || err == syscall.EOPNOTSUPP || err == syscall.ENOTSOCK:
			// This kind of socket or file cannot be spliced; copy it.
			s.copyBuf = bufferpool.Get(sendFileChunk)
		default:
			return n, attempted, true, err
		}
	}
	chunk := s.copyBuf[:min(s.remaining, int64(len(s.copyBuf)))]
	read, err := syscall.Pread(s.fd, chunk, s.offset)
	if err != nil {
		return 0, len(chunk), true, err
	}
	if read == 0 {
		return 0, len(chunk), true, io.ErrUnexpectedEOF
	}
	n, err = c.sysWrite(chunk[:read])
	// The bytes were read for this write alone; what it did not take is read
	// again next time, so none of it is ever counted as pending.
	return max(n, 0), read, true, err
}

// rawSocket is what the kernel names a socket by: a file descriptor here.
type rawSocket = int

func (c *Connection) rawSocket() rawSocket { return c.FD() }

// shutdownReadLocked shuts the socket's reading side. Callers hold c.mu.
func (c *Connection) shutdownReadLocked() error {
	return syscall.Shutdown(c.FD(), syscall.SHUT_RD)
}

// setKeepAliveTimes sets the idle time before the first keep-alive probe and
// the interval between probes; see Connection.SetKeepAliveConfig.
func setKeepAliveTimes(s int, idle, interval time.Duration) error {
	if idle >= 0 {
		if err := syscall.SetsockoptInt(s, syscall.IPPROTO_TCP, tcpKeepIdle, keepAliveSeconds(idle)); err != nil {
			return err
		}
	}
	if interval >= 0 {
		return syscall.SetsockoptInt(s, syscall.IPPROTO_TCP, tcpKeepInterval, keepAliveSeconds(interval))
	}
	return nil
}

// setKeepAliveCount sets how many unanswered probes end the connection.
func setKeepAliveCount(s int, count int) error {
	if count < 0 {
		return nil
	}
	if count == 0 {
		count = defaultKeepAliveCount
	}
	return syscall.SetsockoptInt(s, syscall.IPPROTO_TCP, tcpKeepCount, count)
}

// File returns a duplicate of the connection's socket, as net.TCPConn.File
// does. The two share the socket, but closing the file does not close the
// connection, nor the other way round. A UDP listener's peer has no socket of
// its own and returns an error.
func (c *Connection) File() (*os.File, error) {
	if c.udp != nil && c.udp.listener != nil {
		return nil, errNoSocket
	}
	name := fileName(c.LocalAddr(), c.RemoteAddr())
	var f *os.File
	err := c.control("dup", func(s int) error {
		c.rawExposed.Store(true)
		syscall.ForkLock.RLock()
		dup, err := syscall.Dup(s)
		if err == nil {
			syscall.CloseOnExec(dup)
		}
		syscall.ForkLock.RUnlock()
		if err != nil {
			return err
		}
		f = os.NewFile(uintptr(dup), name)
		return nil
	})
	return f, err
}

// fileName names a connection's file as the net package does.
func fileName(local, remote net.Addr) string {
	name := ""
	if local != nil {
		name = local.Network() + ":" + local.String()
	}
	if remote != nil {
		name += "->" + remote.String()
	}
	return name
}

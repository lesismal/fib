//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package fib

import (
	"io"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lesismal/fib/bufferpool"
	"github.com/lesismal/fib/internal/listener"
	"github.com/lesismal/fib/internal/netaddr"
	"github.com/lesismal/fib/internal/sys"
	"github.com/lesismal/fib/internal/udpbatch"
	"github.com/lesismal/fib/sidepool"
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
	udpBatch *udpbatch.Batch
	// listeners are the engine's TCP and Unix listening sockets, in
	// configured order.
	listeners []streamListener
	// pollersListen says the engine's pollers accept some of its
	// connections, or read its datagrams, themselves; see Config.EngineLoop
	// and Config.ReusePort. Which listeners they accept for each says
	// itself. udpSpread says they read every UDP address on sockets of their
	// own, and udpHanded that the first poller reads the engine's own.
	pollersListen  bool
	udpSpread      bool
	udpHanded      bool
	nextGeneration atomic.Uint64
	// listenMu guards stopping the listeners, which Stop does from whatever
	// goroutine calls it, and listenAddrs, the addresses they were bound to
	// when they stopped: on Darwin and the BSDs a stopped listener's
	// descriptor holds an unbound socket, so its address is kept from before.
	// It also guards the list of listeners where a goroutine other than the
	// loop changes it, and listenReleased, which says Close has taken the
	// listeners to close: closeBinding on an engine with no loop of its own
	// leaves them to Close from then on.
	listenMu       sync.Mutex
	listenStopped  bool
	listenReleased bool
	listenAddrs    []syscall.Sockaddr
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

// streamListener is a TCP or Unix listening socket and what serves the
// connections it takes.
type streamListener struct {
	fd      int
	network string
	// unix says the socket is a Unix one, whose connections get no
	// TCP_NODELAY.
	unix bool
	// spread says the engine's pollers accept the address's connections on
	// sockets of their own, bound beside this one, which only holds the
	// address; see Config.ReusePort.
	spread bool
	// shared says, of an engine's socket, that every one of its pollers
	// waits on this socket itself and accepts onto its own loop, and, of a
	// poller's, that the socket is the engine's, which stops and closes it;
	// see Config.EngineLoop.
	shared  bool
	handler Handler
	// binding is the index of the Binding the socket was opened for.
	binding int
}

type connPlatform struct {
	fd atomic.Int32
	// gen is the generation half of the connection's token; see newToken.
	// The descriptor half is fd, so the pair fits the word a whole token took.
	gen uint32
}

// token returns the token the connection is registered with.
func (c *Connection) token() uint64 { return uint64(uint32(c.fd.Load())) | uint64(c.gen)<<32 }

// udpPlatform is, for a dialed UDP connection, which reads and sends on its
// own descriptor, the senders waiting for that descriptor to have room; a
// peer sends through its listener's, and waits with the listener's.
type udpPlatform struct{ writable *udpWaiters }

type udpListenerPlatform struct {
	fd       int
	writable udpWaiters
}

// udpWaiters are the senders waiting in NotifyWritable for a UDP socket that
// was full to have room. While any of them waits, the socket's loop watches
// it for room as well as for datagrams; the first time it finds room it
// stops watching for it and posts every waiter. Watching all the time would
// wake the loop on every wait, since a socket is writable for as long as it
// has room, which it nearly always has.
type udpWaiters struct {
	mu sync.Mutex
	// armed says the loop is watching the socket for room.
	armed bool
	// closed says the socket is closed, and nothing waits on it again.
	closed bool
	fns    []func()
	// woken counts the times the loop found room. For tests.
	woken int
}

// wait adds fn to the waiters and has the loop watch fd, whose token is
// token, for room unless it does already, and reports whether it does.
func (w *udpWaiters) wait(e *Engine, fd int, token uint64, dialed bool, fn func()) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || fd < 0 || e.stopping.Load() {
		return false
	}
	if !w.armed {
		if e.watchWritable(fd, token, dialed, true) != nil {
			return false
		}
		w.armed = true
	}
	w.fns = append(w.fns, fn)
	return true
}

// ready stops the loop watching fd for room, which it has, and posts the
// waiters. Callers run on the event loop.
func (w *udpWaiters) ready(e *Engine, fd int, token uint64, dialed bool) {
	w.mu.Lock()
	w.woken++
	if w.armed {
		w.armed = false
		_ = e.watchWritable(fd, token, dialed, false)
	}
	fns := w.fns
	w.fns = nil
	w.mu.Unlock()
	for _, fn := range fns {
		sidepool.Post(fn)
	}
}

// closeFD closes fd, the socket the waiters wait on, and posts them: a
// sender has nothing to wait for on a closed socket, and finds it closed
// when it sends again. Holding the lock keeps a sender from watching the
// descriptor once it is closed, when it may already belong to another
// socket.
func (w *udpWaiters) closeFD(fd int) error {
	w.mu.Lock()
	w.closed = true
	fns := w.fns
	w.fns = nil
	err := syscall.Close(fd)
	w.mu.Unlock()
	for _, fn := range fns {
		sidepool.Post(fn)
	}
	return err
}

// closeSocket closes a UDP listener's socket; see udpWaiters.closeFD.
func (l *udpListener) closeSocket() error { return l.writable.closeFD(l.fd) }

// notifyWritable is NotifyWritable on a backend that tells when a socket
// has room.
func (c *Connection) notifyWritable(fn func()) bool {
	if l := c.udp.listener; l != nil {
		return l.writable.wait(c.engine, l.fd, udpToken(l.fd), false, fn)
	}
	w := c.udp.writable
	if w == nil {
		return false
	}
	// A descriptor read before detach closes it is one wait finds closed.
	return w.wait(c.engine, c.FD(), c.token(), true, fn)
}

// udpWritable has the waiters of a UDP socket the poller found room on go
// on: a listener's, or a dialed connection's. Callers run on the event loop.
func (e *Engine) udpWritable(l *udpListener, c *Connection) {
	if l != nil {
		l.writable.ready(e, l.fd, udpToken(l.fd), false)
	} else if w := c.udp.writable; w != nil {
		w.ready(e, c.FD(), c.token(), true)
	}
}

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

// sysSendDatagrams sends datagrams, none of them empty, in batches, and
// reports how many it sent. Callers hold c.mu.
func (c *Connection) sysSendDatagrams(datagrams [][]byte) (int, error) {
	fd, to := c.FD(), syscall.Sockaddr(nil)
	if l := c.udp.listener; l != nil {
		fd, to = l.fd, c.udp.sa
	}
	sent := 0
	for sent < len(datagrams) {
		n, err := udpbatch.Send(fd, to, datagrams[sent:])
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return sent, err
		}
		sent += n
	}
	return sent, nil
}

func (e *Engine) open(config Config, bindings []Binding) error {
	e.nextGeneration.Store(firstGeneration)
	if p := e.parent; p != nil && p.pollersListen {
		// A poller that accepts for itself listens beside its parent's
		// other pollers, on every TCP address the parent is bound to.
		for _, like := range p.listeners {
			switch {
			case like.shared:
				e.listeners = append(e.listeners, like)
			case like.spread:
				fd, err := listener.CreateLike(listenOptions(config, like.network), like.fd)
				if err != nil {
					e.closeListeners()
					return err
				}
				like.fd, like.spread = fd, false
				e.listeners = append(e.listeners, like)
			}
		}
		if err := e.openUDPBeside(config, p); err != nil {
			e.closeListeners()
			return err
		}
	}
	for i, b := range bindings {
		udp := netaddr.IsUDP(b.Network)
		for _, addr := range b.Addrs {
			if udp {
				fd, err := listener.CreateUDP(listenOptions(config, b.Network), addr)
				if err != nil {
					e.closeListeners()
					return err
				}
				e.udpListeners = append(e.udpListeners, &udpListener{udpListenerPlatform: udpListenerPlatform{fd: fd},
					network: b.Network, handler: b.Handler, binding: i, peers: make(map[netip.AddrPort]*Connection)})
				e.udpSpread = e.udpSpread || pollersListen(config, b.Network)
				continue
			}
			// When the pollers listen each on a socket of its own, the
			// engine's socket only holds the address, and the port the kernel
			// chose for it: were it to listen too, it would be handed a share
			// of the connections that nothing accepts. An engine without a
			// loop of its own has its pollers all wait on the one socket
			// where they cannot each have their own.
			spread := pollersListen(config, b.Network)
			shared := e.noLoop && !spread
			fd, err := listener.Create(listenOptions(config, b.Network), addr, !spread, spread)
			if err != nil {
				e.closeListeners()
				return err
			}
			e.listeners = append(e.listeners, streamListener{fd: fd, network: b.Network,
				unix: netaddr.IsUnix(b.Network), spread: spread, shared: shared, handler: b.Handler, binding: i})
			e.pollersListen = e.pollersListen || spread || shared
			e.noteUnixPath(b.Network, addr, i)
		}
	}
	// Without a loop of its own, the engine has the first poller read the
	// UDP sockets it cannot give every poller one of.
	e.udpHanded = e.noLoop && !e.udpSpread && len(e.udpListeners) > 0
	e.pollersListen = e.pollersListen || e.udpSpread || e.udpHanded
	if e.noLoop {
		// Nothing waits on a backend, and nothing need wake one: every
		// command goes to a poller. Marking the engine awake for good makes
		// notify, which Stop calls, do nothing.
		e.wakePending.Store(true)
		return nil
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
// of its own beside them, unless the engine has no loop of its own and no
// ReusePort, when the first poller reads them alone. The engine cannot keep a socket only to hold the
// address, as it does a TCP listener it leaves unlistened: a bound UDP socket
// is in its address's SO_REUSEPORT group whatever else it does, and would be
// handed a share of the datagrams that nothing reads.
func (e *Engine) openUDPBeside(config Config, parent *Engine) error {
	if !parent.udpSpread && !parent.udpHanded {
		return nil
	}
	if len(parent.udpListeners) > 0 {
		e.udpListeners, parent.udpListeners = parent.udpListeners, nil
		return nil
	}
	if !parent.udpSpread || len(parent.pollers) == 0 {
		// The engine's sockets went to the first poller, which reads them
		// alone; see Config.EngineLoop.
		return nil
	}
	for _, like := range parent.pollers[0].udpListeners {
		fd, err := listener.CreateUDPLike(listenOptions(config, like.network), like.fd)
		if err != nil {
			return err
		}
		e.udpListeners = append(e.udpListeners, &udpListener{udpListenerPlatform: udpListenerPlatform{fd: fd},
			network: like.network, handler: like.handler, binding: like.binding, peers: make(map[netip.AddrPort]*Connection)})
	}
	return nil
}

// stopListening stops the engine's listeners taking connections without
// giving up their descriptors; see stopListeningFD. Stop calls it, so that a
// client connecting to an engine that is stopping is refused rather than left
// in a backlog nothing accepts from, and it is safe to call again or from any
// goroutine.
func (e *Engine) stopListening() {
	e.listenMu.Lock()
	defer e.listenMu.Unlock()
	if e.listenStopped {
		return
	}
	e.listenStopped = true
	e.listenAddrs = make([]syscall.Sockaddr, len(e.listeners))
	for i, l := range e.listeners {
		if l.shared && e.parent != nil {
			// The engine's own, which it stops itself.
			continue
		}
		e.listenAddrs[i], _ = syscall.Getsockname(l.fd)
		stopListeningFD(l.fd)
	}
}

// beginShutdown starts Shutdown: the listeners stop taking connections, as
// Stop has them do, while the loops go on serving the connections there are.
// They stop before ShuttingDown reports true, so that a client connecting
// once it does is refused.
func (e *Engine) beginShutdown() {
	e.stopListening()
	for _, p := range e.pollers {
		p.stopListening()
	}
	for _, p := range e.pollers {
		p.draining.Store(true)
	}
	e.draining.Store(true)
}

// shutdownConnections runs one round of Shutdown on the loop: each open
// connection is told the engine is shutting down, and the answer is how many
// of those Shutdown waits for remain. With a binding of zero or more, the
// round is ShutdownBinding's, over the connections that binding took.
// Callers run on the event loop.
func (e *Engine) shutdownConnections(binding int) int {
	n := 0
	for _, entries := range e.connections {
		for _, c := range entries {
			if c != nil && c.inBinding(binding) && shutdownConnection(c) {
				n++
			}
		}
	}
	return n + e.shutdownUDPPeers(binding)
}

// listenSocknameLocked returns the address of the i-th listener, fd, which
// is the one it had before it stopped listening if it has. Callers hold
// listenMu.
func (e *Engine) listenSocknameLocked(i, fd int) (syscall.Sockaddr, error) {
	if e.listenStopped && i < len(e.listenAddrs) {
		if sa := e.listenAddrs[i]; sa != nil {
			return sa, nil
		}
	}
	return syscall.Getsockname(fd)
}

func (e *Engine) closeListeners() {
	for _, l := range e.listeners {
		if !l.shared || e.parent == nil {
			syscall.Close(l.fd)
		}
	}
	e.listeners = nil
	for _, l := range e.udpListeners {
		l.closeSocket()
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

// listenerAt returns the TCP or Unix listener on a descriptor, or nil.
func (e *Engine) listenerAt(fd int) *streamListener {
	for i := range e.listeners {
		if e.listeners[i].fd == fd {
			return &e.listeners[i]
		}
	}
	return nil
}

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
// says the socket is empty, rather than at a read that finds nothing. A read
// the kernel coalesced with GRO holds several datagrams, which are queued one
// at a time. avail is how many bytes of datagrams the poller reported
// waiting, or -1 where it does not say. Where it says, the round reads the first datagram with an
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
			segs, addr := b.Segments(i), b.Addr(i)
			for j := 0; j < segs; j++ {
				data := b.Segment(i, j)
				avail -= len(data)
				if c := e.udpPeer(l, addr); c != nil {
					if r := e.deliverDatagram(c, data, now); r != nil {
						ready = append(ready, r)
					}
				}
				read++
			}
		}
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
			for j, segs := 0, b.Segments(i); j < segs; j++ {
				data := b.Segment(i, j)
				avail -= len(data)
				if r := e.deliverDatagram(c, data, now); r != nil {
					ready = append(ready, r)
				}
				read++
			}
		}
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
	if c == nil || c.gen != uint32(token>>32) {
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

// ListenAddrs returns the address of every listener, whatever its network: a
// *net.TCPAddr, *net.UnixAddr or *net.UDPAddr. The TCP and Unix listeners
// come first, in configured order, and the UDP ones after them, in theirs.
// Ports left at zero report the port the kernel chose.
func (e *Engine) ListenAddrs() ([]net.Addr, error) {
	return e.listenAddrsOf(-1)
}

// BindingAddrs returns the addresses of the i-th binding, as New and
// AddBinding number them, in the order the binding lists them, as ListenAddrs reports them.
func (e *Engine) BindingAddrs(i int) ([]net.Addr, error) {
	if i < 0 {
		return nil, errNoBinding
	}
	addrs, err := e.listenAddrsOf(i)
	if err == nil && len(addrs) == 0 {
		err = errNoBinding
	}
	return addrs, err
}

// listenAddrsOf returns the addresses of binding's listeners, or of every
// listener for a binding below zero.
func (e *Engine) listenAddrsOf(binding int) ([]net.Addr, error) {
	e.listenMu.Lock()
	addrs := make([]net.Addr, 0, len(e.listeners)+len(e.udpListeners))
	for i, l := range e.listeners {
		if binding >= 0 && l.binding != binding {
			continue
		}
		sa, err := e.listenSocknameLocked(i, l.fd)
		if err != nil {
			e.listenMu.Unlock()
			return nil, err
		}
		addrs = append(addrs, netaddr.ToAddr(sa))
	}
	e.listenMu.Unlock()
	for _, l := range e.udpSockets() {
		if binding >= 0 && l.binding != binding {
			continue
		}
		addr, err := udpListenerAddr(l)
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

// LocalAddrs returns one address per TCP listener, in configured order,
// leaving out the Unix ones, which ListenAddrs reports. Ports left at zero
// report the port the kernel chose.
func (e *Engine) LocalAddrs() ([]*net.TCPAddr, error) {
	e.listenMu.Lock()
	defer e.listenMu.Unlock()
	addrs := make([]*net.TCPAddr, 0, len(e.listeners))
	for i, l := range e.listeners {
		if l.unix {
			continue
		}
		sa, err := e.listenSocknameLocked(i, l.fd)
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
// goroutine of its own while this one runs the engine's own loop, if it has
// one (see Config.EngineLoop), and returns once they have all returned; a
// poller that fails stops the rest.
func (e *Engine) Run() error {
	e.logRun()
	if len(e.pollers) == 0 {
		e.claimLoop()
		return e.runLoop()
	}
	errs := make(chan error, len(e.pollers))
	for _, p := range e.pollers {
		p.claimLoop()
		go func(p *Engine) {
			err := p.runLoop()
			if err != nil {
				e.Stop()
			}
			errs <- err
		}(p)
	}
	var err error
	if !e.noLoop {
		e.claimLoop()
		err = e.runLoop()
		e.Stop()
	}
	for range e.pollers {
		if pollerErr := <-errs; err == nil {
			err = pollerErr
		}
	}
	return err
}

func (e *Engine) acceptConnections(listenFD int) {
	l := e.listenerAt(listenFD)
	if l == nil {
		return
	}
	for {
		fd, peer, err := sys.AcceptSocket(listenFD)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return
		}
		if !e.accepted.take() {
			syscall.Close(fd)
			continue
		}
		if !l.unix && !sys.AcceptedInheritNoDelay {
			// Replies are written whole, so Nagle would only hold a small
			// one back until the peer's delayed ACK.
			_ = syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
		}
		// A connection goes to the poller its descriptor picks, unless a
		// poller accepted it on a socket of its own under AcceptKernelHash:
		// the kernel already spread connections over the pollers' sockets by
		// a hash of their addresses, and the poller keeps it, sparing a
		// wake-up and a registration on another loop. One accepted on a
		// socket every poller waits on, or by the engine's own loop, always
		// goes by descriptor, since the kernel wakes the pollers sharing a
		// socket unevenly.
		p := e
		root := e.root()
		if len(root.pollers) > 0 && (e.parent == nil || l.shared || root.acceptPlacement != AcceptKernelHash) {
			p = root.pollers[fd%len(root.pollers)]
		}
		if p == e {
			e.admit(newAccepted(e, l, peer), fd)
			continue
		}
		// The poller registers the descriptor itself, since its table and
		// its backend are its loop's alone.
		c := newAccepted(p, l, peer)
		c.fd.Store(int32(fd))
		if !p.request(command{kind: commandAccept, connection: c}) {
			syscall.Close(fd)
			e.accepted.leave()
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
		e.accepted.leave()
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
	c.gen = uint32(token >> 32)
	c.fd.Store(int32(fd))
	// Write interest is registered up front and never modified again. The
	// descriptor is edge-triggered, so an always-armed write interest only
	// fires when the socket goes from full back to writable, which spares
	// the loop a registration change per backpressured message.
	if err := e.registerConnection(fd, token); err != nil {
		c.fd.Store(-1)
		syscall.Close(fd)
		e.accepted.leave()
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
	if c.flags.has(flagRawExposed) {
		e.unregister(fd)
	}
	if c.udp != nil && c.udp.writable != nil {
		_ = c.udp.writable.closeFD(fd)
	} else {
		_ = syscall.Close(fd)
	}
	if c.bindingID != 0 {
		e.accepted.leave()
	}
	if page := fd >> connPageShift; page < len(e.connections) {
		if entries := e.connections[page]; entries != nil && entries[fd&connPageMask] == c {
			entries[fd&connPageMask] = nil
		}
	}
}

// Close stops the engine and closes every connection it still has, at once:
// a peer sees its connection end, and a handler still running, which nothing
// can interrupt, finds whatever it sends refused, as net/http.Server.Close
// leaves its handlers. Run must have returned before Close is called.
//
// Close releases all resources once no handler is running any more: before
// it returns when the rounds still running end within a moment of their
// connections being cut, which is all of them unless a handler is held up by
// something else, and otherwise when the last of them does, after Close has
// returned. Closing descriptors it releases then reports no error.
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
		loops := append([]*Engine{e}, e.pollers...)
		for _, l := range loops {
			// Whatever a binding opens or closes from here on is Close's
			// to release, through the loop's queue.
			l.claimLoop()
			l.cutConnections()
		}
		closeErr = afterRounds(func() {
			for _, l := range loops {
				l.taskWG.Wait()
			}
		}, e.release)
	})
	return closeErr
}

// cutConnections ends every connection of the loop at once; see
// cutConnection.
func (e *Engine) cutConnections() {
	for _, entries := range e.connections {
		for _, c := range entries {
			if c != nil {
				e.cutConnection(c)
			}
		}
	}
	e.cutUDPPeers()
}

// shutdownSocket ends the connection's socket both ways, for a connection
// cut while a round of it runs, keeping its descriptor; see cutConnection.
func (c *Connection) shutdownSocket() {
	if fd := c.FD(); fd >= 0 {
		_ = syscall.Shutdown(fd, syscall.SHUT_RDWR)
	}
}

// release is what Close leaves until no round runs: the pollers, the task
// pool, and the descriptors of the engine, its listeners and the connections
// left. The pollers' connections are the engine's, so they go first, while
// the task pool they run on is still there.
func (e *Engine) release() error {
	var closeErr error
	e.closeBalancers(-1)
	e.closeCut()
	for _, p := range e.pollers {
		p.closeOnce.Do(func() {
			p.Stop()
			p.stopUDPSweeper()
			if err := p.release(); err != nil && closeErr == nil {
				closeErr = err
			}
		})
	}
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
	// An engine with no loop of its own has its listeners taken off by
	// closeBinding on the goroutine that calls it, under listenMu.
	e.listenMu.Lock()
	listeners := e.listeners
	e.listenReleased = true
	e.listenMu.Unlock()
	for _, l := range listeners {
		if l.shared && e.parent != nil {
			// The engine's own, closed below once every poller is done
			// with it.
			continue
		}
		if err := syscall.Close(l.fd); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	for _, l := range e.udpListeners {
		if err := l.closeSocket(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	e.removeUnixPaths()
	if e.noLoop {
		return closeErr
	}
	if err := e.closeBackend(); err != nil && closeErr == nil {
		closeErr = err
	}
	return closeErr
}

// acceptPlacements describes, for each TCP and Unix listener in order, the
// loop that accepts its connections and the poller that then serves them;
// see Config.AcceptPlacement. It reports whether any listener's placement
// is the one Config.AcceptPlacement chose, which only a poller accepting on
// a socket of its own takes.
func (e *Engine) acceptPlacements() (string, bool) {
	e.listenMu.Lock()
	defer e.listenMu.Unlock()
	parts := make([]string, 0, len(e.listeners))
	applies := false
	for i, l := range e.listeners {
		var how string
		switch {
		case len(e.pollers) == 0:
			how = "engine-loop"
		case l.spread:
			how = "pollers," + e.acceptPlacement.String()
			applies = true
		case l.shared:
			how = "pollers-shared-socket," + AcceptFDModulo.String()
		default:
			how = "engine-loop," + AcceptFDModulo.String()
		}
		addr := l.network
		if sa, err := e.listenSocknameLocked(i, l.fd); err == nil {
			addr = l.network + "://" + netaddr.ToAddr(sa).String()
		}
		parts = append(parts, addr+"="+how)
	}
	return strings.Join(parts, " "), applies
}

// pollersListen reports whether config has each poller listen on a socket
// of its own for an address on network, accepting its TCP connections or
// reading its UDP datagrams itself. Under IOPollers without EngineLoop it
// does for TCP whatever ReusePort says, where SO_REUSEPORT spreads
// connections; see Config.ReusePort. UDP needs ReusePort, since a peer's datagrams move
// between sockets as sockets join.
func pollersListen(config Config, network string) bool {
	return (config.ReusePort || !netaddr.IsUDP(network)) && sys.ReusePortSpreads &&
		!engineLoop(config) && !netaddr.IsUnix(network)
}

// listenOptions is what of config the listening sockets on network are
// opened by.
func listenOptions(config Config, network string) listener.Options {
	return listener.Options{Network: network, Backlog: config.Backlog, ReusePort: config.ReusePort}
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
// only has to report why, or on the first poller where the engine has none.
func (e *Engine) dialLoop(d *dialRequest) *Engine {
	if len(e.pollers) == 0 || d.hasSocket {
		return e
	}
	if d.err == nil {
		fd, err := d.openSocket()
		if err == nil {
			d.socket, d.hasSocket = fd, true
			return e.pollers[fd%len(e.pollers)]
		}
		d.err = err
	}
	if e.noLoop {
		return e.pollers[0]
	}
	return e
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
	c = &Connection{engine: e, handler: d.handler, dialing: d, dialed: true}
	c.flags.set(flagUnix, netaddr.IsUnix(d.network))
	c.gen = uint32(token >> 32)
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
	// See listener.bindUDPSocket: a dialed socket has its runs of datagrams
	// coalesced too, which the loop splits again.
	udpbatch.EnableGRO(fd)
	token := e.newToken(fd)
	c = &Connection{engine: e, handler: d.handler, dialing: d, dialed: true,
		udp: &udpState{udpPlatform: udpPlatform{writable: &udpWaiters{}},
			raddr: &net.UDPAddr{IP: d.raddr.IP, Port: d.raddr.Port, Zone: d.raddr.Zone}}}
	c.gen = uint32(token >> 32)
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
	// sealer, when set, transforms the file on its way, as TLS does, so that
	// it is read and sealed a chunk at a time into sealed rather than sent by
	// sendfile; see stageSealedLocked.
	sealer FileSealer
	sealed []byte
}

// readAt reads the file at off, below its own position, which the caller's
// descriptor shares.
func (s *fileSegment) readAt(p []byte, off int64) (int, error) {
	for {
		n, err := syscall.Pread(s.fd, p, off)
		if err == syscall.EINTR {
			continue
		}
		return max(n, 0), err
	}
}

// newSealBuffer is the buffer a sealed segment seals its chunks into: one
// from the pool, which the segment gives back as it ends, since every write
// here is done with its bytes by the time it returns.
func newSealBuffer(n int) []byte { return bufferpool.Get(n) }

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
	bufferpool.Put(s.sealed)
	s.copyBuf, s.sealed = nil, nil
}

// sysSendFileLocked sends what it can of the file item at the head of the
// queue. fromFile reports that the bytes went by sendfile, from the file
// rather than from memory. Callers hold c.mu.
func (c *Connection) sysSendFileLocked(item *sendItem) (n, attempted int, fromFile bool, err error) {
	s := item.file
	if s.sealer != nil {
		if err = c.stageSealedLocked(item); err != nil {
			return 0, 0, false, err
		}
		data := item.data[item.offset:]
		// While more of the file follows at once, TCP holds what a chunk
		// leaves of a segment unfilled for the next one, rather than pushing
		// each chunk on its own as the end of what there is to send.
		n, err = writeSealed(c, data, s.remaining > 0)
		return max(n, 0), len(data), false, err
	}
	attempted = int(min(s.remaining, maxSendFileCall))
	if s.copyBuf == nil {
		for {
			offset := s.offset
			corked := c.corkKernelTLS(true)
			n, err = sendfile(c.FD(), s.fd, &offset, attempted)
			if corked {
				c.corkKernelTLS(false)
			}
			n = max(n, 0)
			if err == syscall.EINTR && n == 0 {
				continue
			}
			if n > 0 && err != nil {
				// macOS and FreeBSD report what they sent along with
				// EAGAIN or EINTR. A
				// partial count stands, and a full socket raises the write
				// edge that resumes the rest; an interrupted call is short
				// only because it was interrupted, so it must not wait for an
				// edge that may never come.
				if err == syscall.EINTR {
					attempted = n
				}
				err = nil
			} else if err == nil && n > 0 && n < attempted {
				// Linux reports a short count without an error, and not only
				// when the socket filled: the call also ends when its read of
				// the file comes up short, as it can with the page cache under
				// pressure. The socket then still has room, and no write edge
				// follows, since the kernel raises one only for a socket that
				// ran out of room: a flush waiting for it waited forever, with
				// the rest of the file queued and the socket empty. The count
				// is taken as all that was attempted, so the flush calls again
				// at once; a socket that is full answers EAGAIN, which is what
				// the edge then follows.
				attempted = n
			}
			break
		}
		switch {
		case err == nil && n == 0:
			return 0, attempted, true, io.ErrUnexpectedEOF
		case err == syscall.EINVAL || err == syscall.ENOTSUP || err == syscall.EOPNOTSUPP || err == syscall.ENOTSOCK ||
			err == syscall.ENOSYS:
			// This kind of socket or file cannot be spliced, or, on NetBSD
			// and OpenBSD, there is no sendfile at all; copy it.
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

// sendfile is syscall.Sendfile, which a test replaces to have it come up
// short.
var sendfile = syscall.Sendfile

// writeSealed writes a sealed chunk of a file, telling the kernel more
// follows it when more says so; a test replaces it to see which do.
var writeSealed = func(c *Connection, data []byte, more bool) (int, error) {
	if more {
		return c.sysWriteMore([][]byte{data})
	}
	return c.sysWrite(data)
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
	if !keepAliveTunable {
		// The system's own times stand in for the defaults; only one asked
		// for in so many words is refused, as package net refuses it.
		if idle > 0 || interval > 0 {
			return syscall.ENOPROTOOPT
		}
		return nil
	}
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
	if count < 0 || count == 0 && !keepAliveTunable {
		return nil
	}
	if !keepAliveTunable {
		return syscall.ENOPROTOOPT
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
		c.flags.set(flagRawExposed, true)
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

// newAccepted returns a connection for a socket a listener accepted from peer.
func newAccepted(e *Engine, l *streamListener, peer netip.AddrPort) *Connection {
	c := &Connection{engine: e, handler: l.handler, peer: packAddrPort(peer), bindingID: int32(l.binding + 1)}
	c.flags.set(flagUnix, l.unix)
	return c
}

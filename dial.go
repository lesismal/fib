//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"fmt"
	"math/rand/v2"
	"net"
	"net/netip"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/lesismal/fib/internal/netaddr"
	"github.com/lesismal/fib/internal/sys"
	"github.com/lesismal/fib/sidepool"
)

// dialRequest is one outbound connect, from Dial until the event loop reports
// its outcome.
type dialRequest struct {
	network string
	addr    string
	timeout time.Duration
	done    func(*Connection, error)
	// handler serves the connection once it is established.
	handler Handler
	// local is what DialWith asked the socket to be bound to, when hasLocal
	// says it asked at all; see bindLocal.
	local    DialOptions
	hasLocal bool
	// family and sa are the resolved address to connect to, and raddr the same
	// address as the net package reports it. err is set instead when resolving
	// failed, and the loop only has to report it.
	family int
	sa     syscall.Sockaddr
	raddr  *net.TCPAddr
	err    error
	// socket is a descriptor opened for the dial before it reached a loop,
	// when hasSocket says there is one: an engine with pollers opens it to
	// learn which poller the dial belongs to. The loop connects it in place
	// of opening one of its own.
	socket    int
	hasSocket bool
	// timer fires the dial timeout. Event-loop ownership.
	timer *time.Timer
}

// Dial connects to addr without blocking and hands the connection to this
// engine's event loop, which then serves it exactly as it serves an accepted
// one: the same handler, the same workers and the same backpressure.
//
// network and addr are what net.Dial takes: network is "tcp", "tcp4", "tcp6",
// "udp", "udp4", "udp6" or "unix" (empty means "tcp"), and addr is
// "host:port", or for "unix" the socket's path. A UDP
// dial connects its socket to addr, so the connection exchanges datagrams with
// that peer alone: each Send is one datagram and each OnData one received. A host that is not an IP
// literal is resolved on a pool of workers kept for clients, never on the
// caller's goroutine, so Dial never waits on DNS.
// A timeout of zero leaves the connect to the operating system's own timeout.
//
// When the connect succeeds, the handler's OnOpen runs first and then done
// with the connection. When it fails or times out, done runs with a nil
// connection and a *net.OpError, and the handler hears nothing. Either way done
// runs exactly once, on the event loop, so like OnOpen it must not block. It
// may be nil.
//
// Dial returns an error, and never calls done, only when it can tell at once
// that the dial cannot start: an unknown network, a malformed or unresolvable
// literal address, or an engine that has been stopped.
func (e *Engine) Dial(network, addr string, timeout time.Duration, done func(*Connection, error)) error {
	return e.DialWithHandler(network, addr, timeout, nil, done)
}

// DialWithHandler is Dial with a handler of the connection's own: OnOpen,
// OnData, OnPriorityData and OnClose for this connection go to handler rather
// than to the engine's. This is what lets one engine carry a server and the
// clients it talks to, each with its own protocol. A nil handler means the
// engine's.
func (e *Engine) DialWithHandler(network, addr string, timeout time.Duration, handler Handler, done func(*Connection, error)) error {
	return e.DialWith(DialOptions{Network: network, Addr: addr, Timeout: timeout, Handler: handler}, done)
}

// DialWith is DialWithHandler with the options of opts, the local address
// and port range the socket is bound to among them; see DialOptions. The
// bind, and the search of the range, happen before the connect, off the
// caller's goroutine where Dial resolves names off it too, and a failed one
// reaches done like a failed connect. An option that cannot work, as a
// range that is empty or a local address of the other family than a literal
// Addr, fails DialWith itself.
func (e *Engine) DialWith(opts DialOptions, done func(*Connection, error)) error {
	handler := opts.Handler
	if handler == nil {
		handler = e.handler
	}
	network := opts.Network
	if network == "" {
		network = "tcp"
	}
	d := &dialRequest{network: network, addr: opts.Addr, timeout: opts.Timeout, done: done, handler: handler,
		local: opts, hasLocal: opts.hasLocal()}
	if e.stopping.Load() {
		return d.opError(net.ErrClosed)
	}
	if !netaddr.IsDialNetwork(network) {
		return d.opError(net.UnknownNetworkError(network))
	}
	if err := opts.check(network); err != nil {
		return d.opError(err)
	}
	if !isLiteralAddr(opts.Addr) {
		resolveOn := opts.resolveNetwork(network)
		sidepool.Go(func() {
			d.family, d.sa, d.raddr, d.err = netaddr.ResolveDial(resolveOn, opts.Addr)
			if d.err == nil && d.raddr != nil {
				d.err = opts.familyMismatch(d.raddr.IP)
			}
			e.requestDial(d)
		})
		return nil
	}
	var err error
	if d.family, d.sa, d.raddr, err = netaddr.ResolveDial(network, opts.Addr); err != nil {
		return d.opError(err)
	}
	if d.raddr != nil {
		if err = opts.familyMismatch(d.raddr.IP); err != nil {
			return d.opError(err)
		}
	}
	e.requestDial(d)
	return nil
}

// isLiteralAddr reports whether addr names its host by IP, or not at all, so
// that resolving it needs no DNS.
func isLiteralAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		// A malformed address fails in the resolver without a lookup.
		return true
	}
	if zone := strings.IndexByte(host, '%'); zone >= 0 {
		host = host[:zone]
	}
	return net.ParseIP(host) != nil
}

// requestDial hands a dial to the event loop that is to serve it: one of the
// engine's pollers, if it has them, or its own. An engine that has already
// closed will never run it, so the dial fails here instead.
func (e *Engine) requestDial(d *dialRequest) {
	if !e.dialLoop(d).request(command{kind: commandDial, dial: d}) {
		d.fail(net.ErrClosed)
	}
}

// fail reports a dial that never produced a connection, closing the socket
// opened for it if no loop took it over.
func (d *dialRequest) fail(err error) {
	if d.hasSocket {
		d.hasSocket = false
		closeDialSocket(d.socket)
	}
	if d.done != nil {
		d.done(nil, d.opError(err))
	}
}

// opError wraps a dial failure the way net.Dial does.
func (d *dialRequest) opError(err error) error {
	if errno, ok := err.(syscall.Errno); ok {
		err = os.NewSyscallError("connect", sys.PortableErrno(errno))
	}
	opErr := &net.OpError{Op: "dial", Net: d.network, Err: err}
	if d.hasLocal {
		opErr.Source = d.sourceAddr()
	}
	if netaddr.IsUnix(d.network) {
		opErr.Addr = &net.UnixAddr{Name: d.addr, Net: "unix"}
	} else if d.raddr != nil {
		opErr.Addr = d.raddr
		if netaddr.IsUDP(d.network) {
			opErr.Addr = &net.UDPAddr{IP: d.raddr.IP, Port: d.raddr.Port, Zone: d.raddr.Zone}
		}
	}
	return opErr
}

// sourceAddr is the local end d asked for, as net reports it.
func (d *dialRequest) sourceAddr() net.Addr {
	ip := d.local.localIP()
	var ipb net.IP
	if ip.IsValid() {
		ipb = ip.AsSlice()
	}
	port := int(d.local.LocalAddr.Port())
	if netaddr.IsUDP(d.network) {
		return &net.UDPAddr{IP: ipb, Port: port, Zone: ip.Zone()}
	}
	return &net.TCPAddr{IP: ipb, Port: port, Zone: ip.Zone()}
}

// bindLocal binds the socket opened for d, through bind, to the local end
// DialWith asked for, if it asked for one: the port it names, or the first
// free one of its range from a random start. It runs before the connect, on
// whichever goroutine opens the socket.
func (d *dialRequest) bindLocal(bind func(syscall.Sockaddr) error) error {
	if !d.hasLocal {
		return nil
	}
	ip := d.local.localIP()
	if !ip.IsValid() {
		ip = netip.IPv4Unspecified()
		if d.family == syscall.AF_INET6 {
			ip = netip.IPv6Unspecified()
		}
	}
	sockaddr := func(port int) (syscall.Sockaddr, error) {
		if d.family == syscall.AF_INET {
			return &syscall.SockaddrInet4{Addr: ip.As4(), Port: port}, nil
		}
		sa := &syscall.SockaddrInet6{Addr: ip.As16(), Port: port}
		if zone := ip.Zone(); zone != "" {
			ifi, err := net.InterfaceByName(zone)
			if err != nil {
				return nil, err
			}
			sa.ZoneId = uint32(ifi.Index)
		}
		return sa, nil
	}
	try := func(port int) error {
		sa, err := sockaddr(port)
		if err != nil {
			return err
		}
		if err = bind(sa); err != nil {
			if errno, ok := err.(syscall.Errno); ok {
				err = os.NewSyscallError("bind", sys.PortableErrno(errno))
			}
		}
		return err
	}
	lo, hi := int(d.local.PortMin), int(d.local.PortMax)
	if hi == 0 {
		return try(int(d.local.LocalAddr.Port()))
	}
	n := hi - lo + 1
	start := rand.IntN(n)
	var err error
	for i := range n {
		if err = try(lo + (start+i)%n); err == nil || !sys.IsAddrInUse(err) {
			return err
		}
	}
	return fmt.Errorf("%w %d-%d: %w", ErrNoLocalPort, lo, hi, err)
}

// startDial runs on the event loop: it opens the socket, starts the connect
// and registers the descriptor, or reports why it could not.
func (e *Engine) startDial(d *dialRequest) {
	if d.err != nil {
		d.fail(d.err)
		return
	}
	if e.stopping.Load() {
		d.fail(net.ErrClosed)
		return
	}
	c, connected, err := e.connectSocket(d)
	if err != nil {
		d.fail(err)
		return
	}
	if connected {
		e.completeDial(c)
		if c.udp == nil {
			// A connect that finished on the spot never raised the
			// completion that arms the first read where that is needed.
			c.rearmRead()
		}
		return
	}
	if d.timeout > 0 {
		d.timer = time.AfterFunc(d.timeout, func() {
			e.request(command{kind: commandDialTimeout, connection: c, dial: d})
		})
	}
}

// expireDial fails a dial whose timeout fired while it was still connecting.
// The timer may fire just as the connect completes, so a dial that has already
// been settled, or a connection now dialing for a different request, is left
// alone.
func (e *Engine) expireDial(c *Connection, d *dialRequest) {
	if c.dialing == d {
		e.failDial(c, os.ErrDeadlineExceeded)
	}
}

// completeDial admits a connected socket to the engine. From here on it is an
// ordinary connection.
func (e *Engine) completeDial(c *Connection) {
	d := c.dialing
	c.dialing = nil
	if d.timer != nil {
		d.timer.Stop()
	}
	c.handler.OnOpen(c)
	if d.done != nil {
		d.done(c, nil)
	}
}

// failDial abandons a connect that failed, timed out or was overtaken by the
// engine closing, and tells the dialer why.
func (e *Engine) failDial(c *Connection, err error) {
	d := c.dialing
	c.dialing = nil
	if d.timer != nil {
		d.timer.Stop()
	}
	c.mu.Lock()
	c.closing = true
	c.closed = true
	c.mu.Unlock()
	e.detach(c)
	d.fail(err)
}

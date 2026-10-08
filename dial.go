//go:build linux || darwin || windows

package fib

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"syscall"
	"time"

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
	if handler == nil {
		handler = e.handler
	}
	d := &dialRequest{network: network, addr: addr, timeout: timeout, done: done, handler: handler}
	if network == "" {
		d.network = "tcp"
	}
	if e.stopping.Load() {
		return d.opError(net.ErrClosed)
	}
	if !isLiteralAddr(addr) {
		if !isDialNetwork(network) {
			return d.opError(net.UnknownNetworkError(network))
		}
		sidepool.Go(func() {
			d.family, d.sa, d.raddr, d.err = resolveDialAddr(network, addr)
			e.requestDial(d)
		})
		return nil
	}
	var err error
	if d.family, d.sa, d.raddr, err = resolveDialAddr(network, addr); err != nil {
		return d.opError(err)
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
		err = os.NewSyscallError("connect", errno)
	}
	opErr := &net.OpError{Op: "dial", Net: d.network, Err: err}
	if isUnixNetwork(d.network) {
		opErr.Addr = &net.UnixAddr{Name: d.addr, Net: "unix"}
	} else if d.raddr != nil {
		opErr.Addr = d.raddr
		if isUDPNetwork(d.network) {
			opErr.Addr = &net.UDPAddr{IP: d.raddr.IP, Port: d.raddr.Port, Zone: d.raddr.Zone}
		}
	}
	return opErr
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

// resolveListenAddr turns a net.Listen network and address into the socket
// address to bind, using the net package so that a host, a service name and an
// empty address all mean here what they mean there.
//
// The "udp" networks resolve the same way, as net.ListenPacket reads them, and
// "unix" takes addr as the socket's path, as net.Listen does.
func resolveListenAddr(network, addr string) (family int, sa syscall.Sockaddr, err error) {
	if isUnixNetwork(network) {
		return resolveUnixAddr(addr)
	}
	switch network {
	case "":
		network = "tcp"
	case "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6":
	default:
		return 0, nil, net.UnknownNetworkError(network)
	}
	if addr == "" {
		// net.Listen reads an empty address as every interface on a port of
		// the kernel's choosing.
		addr = ":0"
	}
	resolved, err := resolveAddr(network, addr)
	if err != nil {
		return 0, nil, err
	}
	return tcpAddrToSockaddr(network, addr, resolved)
}

// resolveAddr resolves a TCP or UDP address. A UDP address comes back in a
// TCPAddr too: only its IP, port and zone matter from here on.
func resolveAddr(network, addr string) (*net.TCPAddr, error) {
	if !isUDPNetwork(network) {
		return net.ResolveTCPAddr(network, addr)
	}
	resolved, err := net.ResolveUDPAddr(network, addr)
	if err != nil {
		return nil, err
	}
	return &net.TCPAddr{IP: resolved.IP, Port: resolved.Port, Zone: resolved.Zone}, nil
}

// resolveDialAddr turns a net.Dial network and address into the socket address
// to connect to. A missing host means the local system, as it does to
// net.Dial.
//
// A "unix" address is a path and needs no resolving; raddr stays nil.
func resolveDialAddr(network, addr string) (family int, sa syscall.Sockaddr, raddr *net.TCPAddr, err error) {
	if !isDialNetwork(network) {
		return 0, nil, nil, net.UnknownNetworkError(network)
	}
	if isUnixNetwork(network) {
		family, sa, err = resolveUnixAddr(addr)
		return family, sa, nil, err
	}
	if network == "" {
		network = "tcp"
	}
	raddr, err = resolveAddr(network, addr)
	if err != nil {
		return 0, nil, nil, err
	}
	if raddr.IP == nil || raddr.IP.IsUnspecified() {
		loopback := net.IPv4(127, 0, 0, 1)
		if network == "tcp6" || network == "udp6" || (raddr.IP != nil && raddr.IP.To4() == nil) {
			loopback = net.IPv6loopback
		}
		raddr = &net.TCPAddr{IP: loopback, Port: raddr.Port}
	}
	family, sa, err = tcpAddrToSockaddr(network, addr, raddr)
	return family, sa, raddr, err
}

// isDialNetwork reports whether network is one this package listens and
// dials on. Empty means "tcp".
func isDialNetwork(network string) bool {
	switch network {
	case "", "tcp", "tcp4", "tcp6":
		return true
	}
	return isUDPNetwork(network) || isUnixNetwork(network)
}

// isUnixNetwork reports whether network names a Unix domain stream socket.
func isUnixNetwork(network string) bool { return network == "unix" }

// resolveUnixAddr turns a socket path into the address to bind or connect to.
// On Linux a leading '@' names an abstract socket, as it does to the net
// package; the syscall package makes that translation.
func resolveUnixAddr(path string) (family int, sa syscall.Sockaddr, err error) {
	if path == "" {
		return 0, nil, &net.AddrError{Err: "missing unix socket path", Addr: path}
	}
	return syscall.AF_UNIX, &syscall.SockaddrUnix{Name: path}, nil
}

// isAbstractUnixPath reports whether path names a Linux abstract socket, which
// has no file to remove.
func isAbstractUnixPath(path string) bool {
	return runtime.GOOS == "linux" && len(path) > 0 && path[0] == '@'
}

// sockaddrToAddr reports a socket address the way net reports one: a
// *net.TCPAddr for an IP socket and a *net.UnixAddr for a Unix one. It
// reports nil for anything else.
func sockaddrToAddr(sa syscall.Sockaddr) net.Addr {
	if unix, ok := sa.(*syscall.SockaddrUnix); ok {
		return &net.UnixAddr{Name: unix.Name, Net: "unix"}
	}
	addr, err := sockaddrToTCPAddr(sa)
	if err != nil {
		return nil
	}
	return addr
}

// tcpAddrToSockaddr picks the socket family and address for a resolved TCP
// address. An IPv4 address under "tcp" gets an IPv4 socket, as it does from the
// net package.
func tcpAddrToSockaddr(network, addr string, resolved *net.TCPAddr) (family int, sa syscall.Sockaddr, err error) {
	ip4 := resolved.IP.To4()
	switch {
	case network == "tcp4" || network == "udp4":
		if resolved.IP != nil && ip4 == nil {
			return 0, nil, fmt.Errorf("address %q is not IPv4", addr)
		}
		bound := &syscall.SockaddrInet4{Port: resolved.Port}
		copy(bound.Addr[:], ip4)
		return syscall.AF_INET, bound, nil
	case ip4 != nil:
		bound := &syscall.SockaddrInet4{Port: resolved.Port}
		copy(bound.Addr[:], ip4)
		return syscall.AF_INET, bound, nil
	default:
		bound := &syscall.SockaddrInet6{Port: resolved.Port}
		copy(bound.Addr[:], resolved.IP.To16())
		if resolved.Zone != "" {
			zone, zoneErr := net.InterfaceByName(resolved.Zone)
			if zoneErr != nil {
				return 0, nil, zoneErr
			}
			bound.ZoneId = uint32(zone.Index)
		}
		return syscall.AF_INET6, bound, nil
	}
}

// sockaddrToTCPAddr reports a bound socket address the way net reports one.
func sockaddrToTCPAddr(sa syscall.Sockaddr) (*net.TCPAddr, error) {
	switch bound := sa.(type) {
	case *syscall.SockaddrInet4:
		return &net.TCPAddr{IP: net.IP(bound.Addr[:]), Port: bound.Port}, nil
	case *syscall.SockaddrInet6:
		addr := &net.TCPAddr{IP: net.IP(bound.Addr[:]), Port: bound.Port}
		if bound.ZoneId != 0 {
			if zone, zoneErr := net.InterfaceByIndex(int(bound.ZoneId)); zoneErr == nil {
				addr.Zone = zone.Name
			}
		}
		return addr, nil
	default:
		return nil, fmt.Errorf("listener is not TCP: %T", sa)
	}
}

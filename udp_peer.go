//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package fib

import (
	"net/netip"
	"syscall"
)

// OpenUDPPeer returns the connection of the peer at addr on a socket of the
// UDP binding at index binding, opening one if the socket has none: the
// connection the peer's first datagram would have opened, with the binding's
// handler and its OnOpen, so that what the peer sends back arrives there.
// It is for a protocol that speaks first, from the port it listens on, to an
// address that has sent it nothing: ICE, which checks every address a peer
// may be reached at from the one socket, and asks a STUN or TURN server from
// it what the socket looks like from outside.
//
// With Config.ReusePort a binding's address has a socket on every poller, and
// the kernel picks which of them a peer's datagrams arrive on. The peer
// OpenUDPPeer opens is on the first of them, and its replies may arrive on
// another, which then opens a connection of its own for the same address:
// a protocol that opens peers keys what it knows of a peer by its address,
// not by its connection, and may send on either.
//
// OpenUDPPeer waits for the socket's loop, so it must not be called from a
// callback that loop runs: the OnOpen of a UDP peer, OnHangup, or a function
// the engine runs on its loop. It reports net.ErrClosed once the engine is
// stopping or the binding is draining, errNoBinding for an index that names
// no UDP binding, and errUDPPeerAddr for an address the socket cannot reach,
// an IPv6 one on an IPv4 socket.
func (e *Engine) OpenUDPPeer(binding int, addr netip.AddrPort) (*Connection, error) {
	if !addr.IsValid() {
		return nil, errUDPPeerAddr
	}
	zone, err := zoneIndex(addr.Addr().Zone())
	if err != nil {
		return nil, err
	}
	root := e.root()
	for _, o := range append([]*Engine{root}, root.pollers...) {
		var (
			c     *Connection
			found bool
			err   error
		)
		loopErr := o.onLoop(func() {
			for _, l := range o.udpListeners {
				if l.binding != binding || l.udpClosed() {
					continue
				}
				found = true
				key, ok := udpPeerKey(l, addr)
				if !ok {
					err = errUDPPeerAddr
					return
				}
				if c = o.udpPeerAt(l, key, zone); c == nil {
					err = errPeerClosed
				}
				return
			}
		})
		if loopErr != nil {
			return nil, loopErr
		}
		if found {
			return c, err
		}
	}
	return nil, errNoBinding
}

// udpPeerKey is the peer table key of addr on l's socket: an IPv4 address
// mapped into IPv6 on an IPv6 socket, which is how such a socket reports an
// IPv4 peer, and the IPv4 address on an IPv4 socket, which cannot reach an
// IPv6 one.
func udpPeerKey(l *udpListener, addr netip.AddrPort) (netip.AddrPort, bool) {
	sa, err := l.sockname()
	if err != nil {
		return netip.AddrPort{}, false
	}
	ip := addr.Addr().WithZone("")
	if _, ok := sa.(*syscall.SockaddrInet4); ok {
		if ip = ip.Unmap(); !ip.Is4() {
			return netip.AddrPort{}, false
		}
		return netip.AddrPortFrom(ip, addr.Port()), true
	}
	return netip.AddrPortFrom(netip.AddrFrom16(ip.As16()), addr.Port()), true
}

//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd && !windows

package fib

import "net/netip"

// OpenUDPPeer returns the connection of the peer at addr on the first socket
// of the UDP binding at index binding, opening one if the socket has none;
// see the epoll and kqueue backends' for what it is for. This backend reads
// each socket on a goroutine of its own, so it does not wait for one.
func (e *Engine) OpenUDPPeer(binding int, addr netip.AddrPort) (*Connection, error) {
	if !addr.IsValid() {
		return nil, errUDPPeerAddr
	}
	e.mu.Lock()
	var l *udpListener
	for _, u := range e.udpListeners {
		if u.binding == binding {
			l = u
			break
		}
	}
	e.mu.Unlock()
	if l == nil {
		return nil, errNoBinding
	}
	if c := e.udpPeer(l, addr); c != nil {
		return c, nil
	}
	return nil, errPeerClosed
}

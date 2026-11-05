//go:build windows

package core

import (
	"net"
	"syscall"
)

// openBinding opens the sockets of b, the i-th binding, and has the loop
// take connections and datagrams on them, as open does for the bindings New
// was given. It opens every socket before the loop posts an operation on
// one, and on failure closes whatever it opened.
func (e *Engine) openBinding(b Binding, i int) error {
	var listeners []*winListener
	var udpListeners []*udpListener
	closeAll := func() {
		for _, l := range listeners {
			syscall.Closesocket(l.fd)
		}
		for _, l := range udpListeners {
			syscall.Closesocket(l.fd)
		}
		e.removeUnixPathsOf(i)
	}
	for _, addr := range b.Addrs {
		l, u, err := e.openListener(e.bindConfig, b, i, addr)
		if err != nil {
			closeAll()
			return err
		}
		if u != nil {
			udpListeners = append(udpListeners, u)
		} else {
			listeners = append(listeners, l)
		}
	}
	var attachErr error
	err := e.onLoop(func() { attachErr = e.attachSockets(listeners, udpListeners) })
	if err == errLoopClosed {
		closeAll()
		return err
	}
	if err == nil {
		err = attachErr
	}
	if err != nil {
		// The loop holds the sockets, or will once its queue drains, and
		// gives them up as the binding is closed, or as the engine is.
		_, _ = e.closeBinding(i, false)
	}
	return err
}

// attachSockets puts listeners and UDP sockets AddBinding opened on the
// loop's lists and posts their first operations. A loop that has stopped
// takes none, and closes them. A socket whose operation failed to post stays
// on its list, for closeBinding to take off. Callers own the loop's state.
func (e *Engine) attachSockets(listeners []*winListener, udpListeners []*udpListener) error {
	if e.stopping.Load() {
		for _, l := range listeners {
			syscall.Closesocket(l.fd)
		}
		for _, l := range udpListeners {
			syscall.Closesocket(l.fd)
		}
		return net.ErrClosed
	}
	e.listenMu.Lock()
	e.listeners = append(e.listeners, listeners...)
	e.udpListeners = append(e.udpListeners, udpListeners...)
	e.listenMu.Unlock()
	if err := e.postListening(listeners, udpListeners); err != nil {
		return err
	}
	e.startUDPSweeper()
	return nil
}

// closeBinding takes the i-th binding's sockets off the loop and closes
// them. With keepUDP, its UDP sockets stay, open no connection for a peer
// they do not know, and go on serving the ones they do; without it they
// close, and so do those peers. It reports whether the engine had any socket
// of the binding left.
func (e *Engine) closeBinding(i int, keepUDP bool) (bool, error) {
	var found bool
	if err := e.onLoop(func() { found = e.detachBinding(i, keepUDP) }); err != nil {
		return false, err
	}
	e.removeUnixPathsOf(i)
	return found, nil
}

// detachBinding takes the i-th binding's sockets off the loop's lists and
// closes them, as closeBinding describes, keeping each on a retired list
// until the operations its closing cancelled have completed. Callers own the
// loop's state.
func (e *Engine) detachBinding(i int, keepUDP bool) bool {
	e.listenMu.Lock()
	var listeners []*winListener
	kept := e.listeners[:0]
	for _, l := range e.listeners {
		if l.binding == i {
			listeners = append(listeners, l)
			continue
		}
		kept = append(kept, l)
	}
	clear(e.listeners[len(kept):])
	e.listeners = kept
	found := len(listeners) > 0
	var udpListeners []*udpListener
	keptUDP := e.udpListeners[:0]
	for _, l := range e.udpListeners {
		if l.binding == i {
			found = true
			if !keepUDP {
				udpListeners = append(udpListeners, l)
				continue
			}
			l.draining = true
		}
		keptUDP = append(keptUDP, l)
	}
	clear(e.udpListeners[len(keptUDP):])
	e.udpListeners = keptUDP
	e.listenMu.Unlock()
	for _, l := range listeners {
		if !l.closed {
			l.closed = true
			syscall.Closesocket(l.fd)
		}
		if l.pending > 0 {
			e.retired = append(e.retired, l)
		}
	}
	for _, l := range udpListeners {
		// The peers send from the socket, so none outlives it.
		for _, c := range l.peers {
			e.closeConnection(c, net.ErrClosed, true)
		}
		l.closed = true
		syscall.Closesocket(l.fd)
		if l.posted {
			e.retiredUDP = append(e.retiredUDP, l)
		}
	}
	return found
}

// closeBindingConnections closes what is left of the i-th binding's
// connections, and its UDP sockets' peers, as ShutdownBinding does once its
// ctx is done.
func (e *Engine) closeBindingConnections(i int) {
	_ = e.onLoop(func() {
		for c := range e.conns {
			if !c.dialed && c.inBinding(i) {
				e.closeConnection(c, net.ErrClosed, true)
			}
		}
		for _, l := range e.udpListeners {
			if l.binding != i {
				continue
			}
			for _, c := range l.peers {
				e.closeConnection(c, net.ErrClosed, true)
			}
		}
	})
}

// forget returns list without x.
func forget[T comparable](list []T, x T) []T {
	for k, v := range list {
		if v == x {
			last := len(list) - 1
			list[k] = list[last]
			var zero T
			list[last] = zero
			return list[:last]
		}
	}
	return list
}

// balanceInitialGroups is a no-op: this backend listens through the net
// package and has no reuseport groups to balance.
func (e *Engine) balanceInitialGroups() {}

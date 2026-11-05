//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd && !windows

package core

import "net"

// inBinding reports whether the binding-th binding took c, or, for a binding
// below zero, whether any did.
func (c *Connection) inBinding(binding int) bool {
	return binding < 0 || int(c.bindingID)-1 == binding
}

// openBinding opens the listeners and UDP sockets of b, the i-th binding,
// and serves them at once if Run has started, or once it does. Either every
// address listens or none does.
func (e *Engine) openBinding(b Binding, i int) error {
	listeners, udpListeners, err := listenBindings([]Binding{b}, i)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopping.Load() || e.draining.Load() {
		for _, l := range listeners {
			_ = l.Close()
		}
		for _, l := range udpListeners {
			_ = l.pc.Close()
		}
		return net.ErrClosed
	}
	e.listeners = append(e.listeners, listeners...)
	e.udpListeners = append(e.udpListeners, udpListeners...)
	if e.running {
		for _, l := range listeners {
			e.startServing(l)
		}
		for _, l := range udpListeners {
			e.startServingUDP(l)
		}
	}
	e.startUDPSweeperLocked()
	return nil
}

// closeBinding closes the i-th binding's listeners, which ends the
// goroutines accepting on them. With keepUDP, its UDP sockets stay, open no
// connection for a peer they do not know, and go on serving the ones they
// do; without it they close, and so do those peers. It reports whether the
// engine had any socket of the binding left.
func (e *Engine) closeBinding(i int, keepUDP bool) (bool, error) {
	e.mu.Lock()
	var listeners []boundListener
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
			l.draining.Store(true)
		}
		keptUDP = append(keptUDP, l)
	}
	clear(e.udpListeners[len(keptUDP):])
	e.udpListeners = keptUDP
	e.mu.Unlock()
	for _, l := range listeners {
		// A Unix listener removes its socket file as it closes.
		_ = l.Close()
	}
	for _, l := range udpListeners {
		_ = l.pc.Close()
		// The peers send from the socket, so none outlives it.
		for _, c := range peersOf(l) {
			c.closeWithError(net.ErrClosed)
		}
	}
	return found, nil
}

// peersOf returns the connections of l's peers. A peer's close takes l.mu,
// so they are closed from the copy.
func peersOf(l *udpListener) []*Connection {
	l.mu.Lock()
	defer l.mu.Unlock()
	peers := make([]*Connection, 0, len(l.peers))
	for _, c := range l.peers {
		peers = append(peers, c)
	}
	return peers
}

// closeBindingConnections closes what is left of the i-th binding's
// connections, its UDP sockets' peers among them, as ShutdownBinding does
// once its ctx is done.
func (e *Engine) closeBindingConnections(i int) {
	e.mu.Lock()
	var closing []*Connection
	for c := range e.connections {
		if !c.dialed && c.inBinding(i) {
			closing = append(closing, c)
		}
	}
	e.mu.Unlock()
	for _, c := range closing {
		c.closeWithError(net.ErrClosed)
	}
}

// balanceInitialGroups is a no-op: this backend has no pollers and no
// reuseport groups to balance.
func (e *Engine) balanceInitialGroups() {}

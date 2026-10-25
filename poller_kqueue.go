//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package fib

import (
	"syscall"

	"github.com/lesismal/fib/taskpool"
)

// backend is a kqueue instance. Every descriptor is registered with EV_CLEAR,
// which gives kqueue the same edge-triggered behaviour the epoll backend gets
// from EPOLLET.
type backend struct {
	kq int
	wakeBackend
	// acceptable collects the listeners that became readable during one wait.
	// Event-loop ownership.
	acceptable []int
}

func (e *Engine) openBackend() error {
	syscall.ForkLock.RLock()
	kq, err := syscall.Kqueue()
	if err == nil {
		syscall.CloseOnExec(kq)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return err
	}
	changes := make([]syscall.Kevent_t, 0, len(e.listeners)+len(e.udpListeners)+1)
	for _, l := range e.listeners {
		if l.spread {
			// Its pollers accept for it; see Config.ReusePort.
			continue
		}
		changes = append(changes, newKevent(l.fd, syscall.EVFILT_READ, syscall.EV_ADD|syscall.EV_CLEAR))
	}
	// UDP sockets are level-triggered, so the loop can stop reading one after
	// its share of a round and still hear about the rest. Those of an engine
	// whose pollers read for it go to the first poller; see openUDPBeside.
	for _, l := range e.udpListeners {
		if e.udpSpread {
			break
		}
		changes = append(changes, newKevent(l.fd, syscall.EVFILT_READ, syscall.EV_ADD))
	}
	if changes, err = e.openWake(changes); err != nil {
		syscall.Close(kq)
		return err
	}
	if _, err = syscall.Kevent(kq, changes, nil, nil); err != nil {
		e.closeWake()
		syscall.Close(kq)
		return err
	}
	e.kq = kq
	return nil
}

// newKevent describes a change to fd's filter. The fields' types differ
// between systems and word sizes, which SetKevent converts to.
func newKevent(fd, filter, flags int) syscall.Kevent_t {
	var ev syscall.Kevent_t
	syscall.SetKevent(&ev, fd, filter, flags)
	return ev
}

func (e *Engine) closeBackend() error {
	e.closeWake()
	return syscall.Close(e.kq)
}

// runLoop runs the engine's event loop until Stop.
func (e *Engine) runLoop() error {
	batch := e.pollBatchSize
	events := make([]syscall.Kevent_t, batch)
	var ready []*Connection
	var tasks []taskpool.Task
	for !e.stopping.Load() {
		n, err := syscall.Kevent(e.kq, nil, events, nil)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return err
		}
		// Awake until settle says otherwise: nothing queued from here on
		// needs to wake the loop.
		e.wakePending.Store(true)
		woken := false
		for i := 0; i < n; i++ {
			ev := &events[i]
			if e.isWake(ev) {
				woken = true
				continue
			}
			fd := int(ev.Ident)
			c := e.connectionAt(fd)
			if c == nil {
				if ev.Filter == syscall.EVFILT_WRITE {
					if l := e.udpListenerAt(fd); l != nil {
						e.udpWritable(l, nil)
					}
				} else if ev.Filter == syscall.EVFILT_READ {
					if l := e.udpListenerAt(fd); l != nil {
						// Reading opens and closes no descriptor, so it
						// need not wait for the batch to end.
						// A socket's read event counts the bytes of
						// datagrams waiting on it.
						ready = e.readUDPListener(l, int(ev.Data), ready)
					} else if e.isListener(fd) {
						e.acceptable = append(e.acceptable, fd)
					}
				}
				continue
			}
			if c.udp != nil {
				if ev.Filter == syscall.EVFILT_WRITE {
					e.udpWritable(nil, c)
				} else {
					ready = e.readUDPConnection(c, int(ev.Data), ready)
				}
				continue
			}
			if c.dialing != nil {
				// A connect that failed reports its error in fflags, next to
				// the EOF that ends it.
				var reported error
				if ev.Flags&syscall.EV_EOF != 0 && ev.Fflags != 0 {
					reported = syscall.Errno(ev.Fflags)
				}
				c = e.finishDial(c, kqueueEvents(ev), reported)
			} else {
				c = e.noteEvent(c, kqueueEvents(ev))
			}
			if c != nil {
				ready = append(ready, c)
			}
		}
		// A kevent names only a descriptor, with no generation to tell one
		// owner of it from the next, so nothing may close or accept a
		// descriptor while events that name it are still being read. A failed
		// dial is the one exception, and a safe one: it clears its table slot
		// as it closes, and nothing can take the descriptor back until the
		// accepts and commands below, so its remaining events resolve to
		// nothing. Closes
		// and accepts therefore wait until the whole batch has been folded
		// into connections. Closing a descriptor removes its pending events
		// from the kqueue, so the next wait cannot see a stale one either.
		if woken {
			e.ackWake()
			e.drainCommands()
		}
		for _, fd := range e.acceptable {
			e.acceptConnections(fd)
		}
		e.acceptable = e.acceptable[:0]
		ready, tasks = e.runReady(ready, tasks)
		ready, tasks = e.settle(ready, tasks)
	}
	e.drainCommands()
	return nil
}

// kqueueEvents translates one kevent into the readiness bits a connection
// accumulates.
func kqueueEvents(ev *syscall.Kevent_t) uint32 {
	var events uint32
	switch ev.Filter {
	case syscall.EVFILT_READ:
		events = evIn
		if ev.Flags&syscall.EV_EOF != 0 {
			// The peer has finished sending. Whatever it sent first is still
			// readable, which is what the read-side half-close means under
			// epoll too.
			events |= evRdHup
		}
	case syscall.EVFILT_WRITE:
		events = evOut
	default:
		if !hasExceptFilter || int(ev.Filter) != evfiltExcept {
			return 0
		}
		// XNU evaluates the except filter with the read filter's code, which
		// falls through to the ordinary readable test when no urgent data is
		// marked, so it fires with every read event. Only NOTE_OOB in fflags
		// says there is urgent data; without the check each read round would
		// also pay for an MSG_OOB receive that fails. On EOF, fflags is the
		// socket error instead, which the read filter reports as well.
		if ev.Flags&syscall.EV_EOF == 0 && ev.Fflags&noteOOB != 0 {
			events = evPri
		}
		return events
	}
	if ev.Flags&syscall.EV_EOF != 0 && ev.Fflags != 0 {
		// On EOF, fflags carries the socket error, if there was one.
		events |= evErr
	}
	return events
}

func (e *Engine) registerConnection(fd int, _ uint64) error {
	changes := [3]syscall.Kevent_t{
		newKevent(fd, syscall.EVFILT_READ, syscall.EV_ADD|syscall.EV_CLEAR),
		newKevent(fd, syscall.EVFILT_WRITE, syscall.EV_ADD|syscall.EV_CLEAR),
		newKevent(fd, evfiltExcept, syscall.EV_ADD|syscall.EV_CLEAR),
	}
	changes[2].Fflags = noteOOB
	n := len(changes)
	if !hasExceptFilter {
		n--
	} else if sa, err := syscall.Getsockname(fd); err == nil {
		if _, unix := sa.(*syscall.SockaddrUnix); unix {
			// A Unix socket has no urgent data, yet EVFILT_EXCEPT still fires
			// on it, which would only add an event to every read.
			n--
		}
	}
	_, err := syscall.Kevent(e.kq, changes[:n], nil, nil)
	return err
}

// registerDatagram registers a dialed UDP socket for reads alone, since sends
// never wait, and level-triggered like a UDP listener.
func (e *Engine) registerDatagram(fd int, _ uint64) error {
	change := [1]syscall.Kevent_t{newKevent(fd, syscall.EVFILT_READ, syscall.EV_ADD)}
	_, err := syscall.Kevent(e.kq, change[:], nil, nil)
	return err
}

// unregister does nothing: closing a descriptor removes its kevents.
func (e *Engine) unregister(int) {}

// watchListener has the loop wait on a TCP or Unix listener for the
// connections it takes.
func (e *Engine) watchListener(l streamListener) error {
	change := [1]syscall.Kevent_t{newKevent(l.fd, syscall.EVFILT_READ, syscall.EV_ADD|syscall.EV_CLEAR)}
	_, err := syscall.Kevent(e.kq, change[:], nil, nil)
	return err
}

// watchUDP has the loop read a UDP socket's datagrams, level-triggered; see
// openBackend.
func (e *Engine) watchUDP(fd int) error {
	change := [1]syscall.Kevent_t{newKevent(fd, syscall.EVFILT_READ, syscall.EV_ADD)}
	_, err := syscall.Kevent(e.kq, change[:], nil, nil)
	return err
}

// watchWritable has the loop hear once that a UDP socket has room, with a
// one-shot write filter, which a socket that has room already fires at once;
// see udpWaiters. The filter deletes itself as it fires, so there is nothing
// to undo. Callers hold the socket's udpWaiters lock.
func (e *Engine) watchWritable(fd int, _ uint64, _, on bool) error {
	if !on {
		return nil
	}
	change := [1]syscall.Kevent_t{newKevent(fd, syscall.EVFILT_WRITE, syscall.EV_ADD|syscall.EV_ONESHOT)}
	_, err := syscall.Kevent(e.kq, change[:], nil, nil)
	return err
}

// unwatch has the loop stop waiting on a listener or a UDP socket. Closing
// it would remove its kevent too, but the engine's socket its pollers share
// stays open while the others wait on it.
func (e *Engine) unwatch(fd int) {
	change := [1]syscall.Kevent_t{newKevent(fd, syscall.EVFILT_READ, syscall.EV_DELETE)}
	_, _ = syscall.Kevent(e.kq, change[:], nil, nil)
}

// pausesInPlace says reads pause by the connection's readPaused flag alone,
// as with epoll: the read filter, which clears on delivery, stays added.
const pausesInPlace = true

// setReadPaused does nothing; see pausesInPlace.
func (e *Engine) setReadPaused(*Connection, bool) error { return nil }

func (e *Engine) isListener(fd int) bool { return e.listenerAt(fd) != nil }

//go:build linux

package fib

import (
	"encoding/binary"
	"os"
	"runtime"
	"syscall"

	"github.com/lesismal/fib/internal/sys"
	"github.com/lesismal/fib/taskpool"
)

const (
	epollET    = uint32(1 << 31)
	baseEvents = uint32(syscall.EPOLLIN|syscall.EPOLLPRI|syscall.EPOLLERR|
		syscall.EPOLLHUP|syscall.EPOLLRDHUP) | epollET
	allEvents = baseEvents | syscall.EPOLLOUT
)

// The readiness bits are epoll's own, so events pass through untranslated. A
// mismatch fails to compile: the array length is zero only when they agree.
var _ [0]struct{} = [evIn ^ syscall.EPOLLIN | evPri ^ syscall.EPOLLPRI | evOut ^ syscall.EPOLLOUT |
	evErr ^ syscall.EPOLLERR | evHup ^ syscall.EPOLLHUP | evRdHup ^ syscall.EPOLLRDHUP]struct{}{}

// backend is an epoll instance plus the eventfd that other goroutines use to
// wake it.
type backend struct {
	epollFD, wakeFD int
}

func (e *Engine) openBackend() error {
	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return err
	}
	wakeFD, err := sys.EventFD()
	if err != nil {
		syscall.Close(epfd)
		return err
	}
	e.epollFD, e.wakeFD = epfd, wakeFD
	for _, fd := range e.listenFDs {
		if e.pollersListen {
			// Its pollers accept for it; see Config.ReusePort.
			break
		}
		if err = e.addFD(fd, listenerToken(fd), uint32(syscall.EPOLLIN)|epollET); err != nil {
			break
		}
	}
	// UDP sockets are level-triggered, so the loop can stop reading one after
	// its share of a round and still hear about the rest. Those of an engine
	// whose pollers read for it go to the first poller; see openUDPBeside.
	for _, l := range e.udpListeners {
		if err == nil && !e.pollersListen {
			err = e.addFD(l.fd, udpToken(l.fd), uint32(syscall.EPOLLIN))
		}
	}
	if err == nil {
		err = e.addFD(wakeFD, wakeToken(wakeFD), uint32(syscall.EPOLLIN)|epollET)
	}
	if err != nil {
		syscall.Close(wakeFD)
		syscall.Close(epfd)
		return err
	}
	return nil
}

func (e *Engine) closeBackend() error {
	err := syscall.Close(e.wakeFD)
	if closeErr := syscall.Close(e.epollFD); err == nil {
		err = closeErr
	}
	return err
}

// runLoop runs the engine's event loop until Stop.
func (e *Engine) runLoop() error {
	batch := e.maxEvents
	if batch > maxWaitBatch {
		batch = maxWaitBatch
	}
	events := make([]syscall.EpollEvent, batch)
	var ready []*Connection
	var tasks []taskpool.Task
	var waiter *epollWaiter
	if len(e.root().pollers) > 0 || runtime.GOMAXPROCS(0) == 1 {
		waiter = newEpollWaiter(e.epollFD)
		defer waiter.close()
	}
	for !e.stopping.Load() {
		n, err := waiter.wait(e.epollFD, events)
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			return err
		}
		// Awake until settle says otherwise: nothing queued from here on
		// needs to wake the loop.
		e.wakePending.Store(true)
		for i := 0; i < n; i++ {
			token := uint64(uint32(events[i].Fd)) | uint64(uint32(events[i].Pad))<<32
			switch token >> 32 {
			case listenerKind:
				e.acceptConnections(int(uint32(token)))
			case wakeKind:
				e.ackWake()
				e.drainCommands()
			case udpKind:
				if l := e.udpListenerAt(int(uint32(token))); l != nil {
					ready = e.readUDPListener(l, -1, ready)
				}
			default:
				if c := e.connectionFor(token); c != nil {
					if c.udp != nil {
						ready = e.readUDPConnection(c, -1, ready)
						continue
					}
					if c.dialing != nil {
						c = e.finishDial(c, events[i].Events, nil)
					} else {
						c = e.noteEvent(c, events[i].Events)
					}
					if c != nil {
						ready = append(ready, c)
					}
				}
			}
		}
		ready, tasks = e.runReady(ready, tasks)
		ready, tasks = e.settle(ready, tasks)
	}
	e.drainCommands()
	return nil
}

// epollWaiter has a loop wait for events parked in the runtime's network
// poller rather than blocked in epoll_wait.
//
// A goroutine blocked in a system call keeps its P until the runtime takes
// the P back, which it does only once the call has lasted one of its
// monitor's ticks, and the goroutines queued on that P wait all the while.
// A loop that is woken for every accepted connection, or every command
// another loop sends it, waits over and over for short spells, and each of
// them left a P idle with work queued behind it: a burst of TLS handshakes to
// 60k connections on three CPUs, their connections accepted by the engine's
// loop and handed to three pollers, measured those CPUs 10 to 15% idle while
// hundreds of goroutines were runnable. Parked, the loop gives its P up at
// once, as a goroutine blocked in a read on a net.Conn does, and the runtime
// wakes it when the epoll descriptor becomes readable. The same burst
// measured the CPUs 0 to 2% idle, and HTTP/1 over 10k connections accepted
// them 19% faster, with echoes unchanged.
//
// Only the loops of an engine with pollers wait this way, and a lone loop
// where GOMAXPROCS is 1. The runtime looks for a parked goroutine's events
// only when a P runs out of work, or every 10ms under load, and a lone loop,
// which every connection waits on, waited longer for that than for its P
// back: the HTTP/1 benchmark accepted connections 15% slower and echoed 2%
// slower with it parked. With one P, though, a loop blocked in epoll_wait
// holds the only P there is, and nothing else runs until events arrive or
// the monitor takes it back, which an idle monitor checks for every 10ms:
// not the timers of handlers sleeping on the workers, nor the goroutines a
// response from elsewhere wakes. In a child of package prefork with one P,
// HttpArena's async profile, 32000 connections each waiting 10ms in its
// handler, served 1.27M requests a second with the loop blocking, with 26.6ms
// for the median request, and with two Ps 2.02M.
//
// The runtime's poller watches a duplicate of the descriptor, so that the
// engine keeps closing its own as before. Where that cannot be set up, wait
// blocks in epoll_wait as the loop always did.
type epollWaiter struct {
	file *os.File
	conn syscall.RawConn
	// poll is what conn.Read calls, made once with the waiter rather than as
	// a closure on every wait, which would escape to the heap each time: a
	// loop waits once per round. epfd and events are what it polls, and n and
	// err what it found.
	poll   func(uintptr) bool
	epfd   int
	events []syscall.EpollEvent
	n      int
	err    error
}

func newEpollWaiter(epfd int) *epollWaiter {
	fd, err := syscall.Dup(epfd)
	if err != nil {
		return nil
	}
	syscall.CloseOnExec(fd)
	// The runtime only polls a descriptor that is non-blocking. The flag is
	// shared with the engine's own descriptor, which epoll_wait ignores.
	if err := syscall.SetNonblock(fd, true); err != nil {
		syscall.Close(fd)
		return nil
	}
	file := os.NewFile(uintptr(fd), "fib-epoll")
	conn, err := file.SyscallConn()
	if err != nil {
		file.Close()
		return nil
	}
	w := &epollWaiter{file: file, conn: conn, epfd: epfd}
	w.poll = func(uintptr) bool {
		w.n, w.err = syscall.EpollWait(w.epfd, w.events, 0)
		return w.n != 0 || (w.err != nil && w.err != syscall.EINTR)
	}
	return w
}

// wait returns the events that are ready, waiting for some if there are none.
// Read looks for them before it parks, and again each time the runtime
// reports the descriptor readable, so a loop with events waiting makes one
// call to epoll_wait, as it did blocking.
func (w *epollWaiter) wait(epfd int, events []syscall.EpollEvent) (int, error) {
	if w != nil {
		w.events = events
		readErr := w.conn.Read(w.poll)
		w.events = nil
		if readErr == nil {
			return w.n, w.err
		}
		// The runtime cannot poll the descriptor after all.
	}
	return syscall.EpollWait(epfd, events, -1)
}

func (w *epollWaiter) close() {
	if w != nil {
		w.file.Close()
	}
}

func (e *Engine) wake() {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	_, _ = syscall.Write(e.wakeFD, b[:])
}

// ackWake drains the eventfd. A single read is enough: reading returns the
// whole counter and resets it to zero, so looping until EAGAIN only adds a
// wasted syscall.
func (e *Engine) ackWake() {
	var b [8]byte
	for {
		if _, err := syscall.Read(e.wakeFD, b[:]); err != syscall.EINTR {
			break
		}
	}
}

func (e *Engine) addFD(fd int, token uint64, events uint32) error {
	ev := syscall.EpollEvent{Events: events, Fd: int32(token), Pad: int32(token >> 32)}
	return syscall.EpollCtl(e.epollFD, syscall.EPOLL_CTL_ADD, fd, &ev)
}

func (e *Engine) registerConnection(fd int, token uint64) error {
	return e.addFD(fd, token, allEvents)
}

// registerDatagram registers a dialed UDP socket. Only reads matter, since
// sends never wait, and it is level-triggered like a UDP listener.
func (e *Engine) registerDatagram(fd int, token uint64) error {
	return e.addFD(fd, token, uint32(syscall.EPOLLIN|syscall.EPOLLERR))
}

func (e *Engine) unregister(fd int) {
	_ = syscall.EpollCtl(e.epollFD, syscall.EPOLL_CTL_DEL, fd, nil)
}

func (e *Engine) setReadPaused(c *Connection, paused bool) error {
	events := allEvents
	if paused {
		events &^= syscall.EPOLLIN
	}
	ev := syscall.EpollEvent{Events: events, Fd: int32(c.token), Pad: int32(c.token >> 32)}
	return syscall.EpollCtl(e.epollFD, syscall.EPOLL_CTL_MOD, c.FD(), &ev)
}

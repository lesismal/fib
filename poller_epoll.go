//go:build linux

package fib

import (
	"encoding/binary"
	"os"
	"syscall"
	"unsafe"

	"github.com/lesismal/fib/internal/sys"
	"github.com/lesismal/fib/taskpool"
)

const (
	epollET = uint32(1 << 31)
	// epollExclusive is EPOLLEXCLUSIVE, which package syscall leaves out.
	epollExclusive = uint32(1 << 28)
	baseEvents     = uint32(syscall.EPOLLIN|syscall.EPOLLPRI|syscall.EPOLLERR|
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
	for _, l := range e.listeners {
		if l.spread {
			// Its pollers accept for it; see Config.ReusePort.
			continue
		}
		if err = e.watchListener(l); err != nil {
			break
		}
	}
	// Those of an engine whose pollers read for it go to the first poller;
	// see openUDPBeside.
	for _, l := range e.udpListeners {
		if err == nil && !e.udpSpread {
			err = e.watchUDP(l.fd)
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
	batch := e.pollBatchSize
	events := make([]syscall.EpollEvent, batch)
	var ready []*Connection
	var tasks []taskpool.Task
	waiter := newEpollWaiter(e.epollFD)
	defer waiter.close()
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
					if events[i].Events&syscall.EPOLLOUT != 0 {
						e.udpWritable(l, nil)
					}
					if events[i].Events&udpReadEvents != 0 {
						ready = e.readUDPListener(l, -1, ready)
					}
				}
			default:
				if c := e.connectionFor(token); c != nil {
					if c.udp != nil {
						if events[i].Events&syscall.EPOLLOUT != 0 {
							e.udpWritable(nil, c)
						}
						if events[i].Events&udpReadEvents != 0 {
							ready = e.readUDPConnection(c, -1, ready)
						}
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
// Every loop waits this way, a lone one too. The runtime looks for a parked
// goroutine's events only when a P runs out of work, or every 10ms under
// load, and a lone loop, which every connection waits on, waits for that
// rather than for its P back. On 8 CPUs of a Ryzen 7800X3D, a lone loop
// parked served HTTP/1 2.2% more pipelined requests, 256 connections 16 deep,
// but 1.4% fewer where 4096 connections each made ten requests and
// reconnected, and over 64 connections the same requests at 4% less CPU with
// their 99th percentile latency 164us to 188us. With one P, a loop blocked in
// epoll_wait holds the only P there is, and nothing else runs until events
// arrive or the monitor takes it back, which an idle monitor checks for every
// 10ms: not the timers of handlers sleeping on the workers, nor the
// goroutines a response from elsewhere wakes. In a child of package prefork
// with one P, HttpArena's async profile, 32000 connections each waiting 10ms
// in its handler, served 1.27M requests a second with the loop blocking, with
// 26.6ms for the median request, and with two Ps 2.02M.
//
// The runtime's poller does not watch the loop's epoll instance itself but a
// relay: a second epoll instance holding the first, which reports it only
// while the loop is parked. Watched directly, the loop's instance was looked
// at by every P that ran out of work, since the runtime's poller looks at
// every descriptor that reported itself since its last look, and a busy
// loop's instance reports itself with every event its connections raise.
// Looking at it takes the instance's lock, which the loop holds while it
// copies out a batch of events, and the Ps that came for it spun on that lock
// rather than looking for work: in go-websocket-benchmark's echo over 20k
// connections, with one poller on 8 CPUs, they spun for 4.3% of the server's
// CPU time, and one poller served 4.9% more echoes with the relay. Disarmed,
// the relay hears the loop's instance and lets it go, and the runtime's
// poller never hears of it.
//
// Those looks did earn a lightly loaded loop something: each of its events
// woke a thread parked in the runtime's poller, which took an idle P and went
// looking for the workers the loop had just woken. One poller serving 100
// connections echoed 5.5% less without them; two, as 8 CPUs get by default,
// echoed the same at every count from 100 connections to 50k. Keeping the
// relay armed while batches are small won that back, but cost two pollers
// 1.6% at 20k and 50k connections, where their batches are small too: every
// event then passes through one more epoll instance. Switching the duplicate
// itself in and out is no cheaper: adding an epoll instance to another makes
// the kernel walk every descriptor it holds.
//
// Where the relay cannot be set up, wait blocks in epoll_wait as the loop
// always did.
type epollWaiter struct {
	file *os.File
	conn syscall.RawConn
	// poll is what conn.Read calls, made once with the waiter rather than as
	// a closure on every wait, which would escape to the heap each time: a
	// loop waits once per round. epfd and events are what it polls, and n and
	// err what it found.
	poll   func(uintptr) bool
	relay  int
	epfd   int
	events []syscall.EpollEvent
	n      int
	err    error
}

func newEpollWaiter(epfd int) *epollWaiter {
	relay, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil
	}
	// Added disarmed; see arm. The runtime only polls a descriptor that is
	// non-blocking.
	ev := syscall.EpollEvent{}
	if err := syscall.EpollCtl(relay, syscall.EPOLL_CTL_ADD, epfd, &ev); err != nil {
		syscall.Close(relay)
		return nil
	}
	if err := syscall.SetNonblock(relay, true); err != nil {
		syscall.Close(relay)
		return nil
	}
	file := os.NewFile(uintptr(relay), "fib-epoll")
	conn, err := file.SyscallConn()
	if err != nil {
		file.Close()
		return nil
	}
	w := &epollWaiter{file: file, conn: conn, relay: relay, epfd: epfd}
	w.poll = func(uintptr) bool {
		w.n, w.err = pollEvents(w.epfd, w.events)
		return w.n != 0 || (w.err != nil && w.err != syscall.EINTR)
	}
	return w
}

// arm has the relay report the loop's instance readable, or with no events,
// only the error and hang-up epoll always adds, not report it at all: the
// instance announces its events to the relay as readable, which those do not
// include.
func (w *epollWaiter) arm(events uint32) error {
	ev := syscall.EpollEvent{Events: events}
	return syscall.EpollCtl(w.relay, syscall.EPOLL_CTL_MOD, w.epfd, &ev)
}

// wait returns the events that are ready, waiting for some if there are none.
// A loop with events waiting, as a busy one has, takes them without parking
// and without touching the relay. Read looks for events again before it
// parks, and each time the runtime reports the relay readable, so events
// that arrive while the relay is armed are never missed. The look after
// arming is what catches one that came before it: arming reports an instance
// already readable only if the relay has not kept it on its ready list since
// an earlier wake-up, which, read by no one, it may. Arming without that
// look, a single connection's ping-pong hung within a thousand rounds.
func (w *epollWaiter) wait(epfd int, events []syscall.EpollEvent) (int, error) {
	if w != nil && w.file != nil {
		if n, err := pollEvents(epfd, events); n != 0 || (err != nil && err != syscall.EINTR) {
			return n, err
		}
		if w.arm(syscall.EPOLLIN) == nil {
			w.events = events
			readErr := w.conn.Read(w.poll)
			w.events = nil
			if w.arm(0) == nil && readErr == nil {
				return w.n, w.err
			}
			if readErr == nil {
				// The events are taken; the relay is given up from the
				// next wait on.
				w.close()
				w.file = nil
				return w.n, w.err
			}
		}
		// The runtime cannot poll the relay after all.
		w.close()
		w.file = nil
	}
	return syscall.EpollWait(epfd, events, -1)
}

// pollEvents returns the events that are ready without waiting. It never
// blocks, so it makes the call raw, as the sockets' reads and writes are.
func pollEvents(epfd int, events []syscall.EpollEvent) (int, error) {
	r, _, errno := syscall.RawSyscall6(syscall.SYS_EPOLL_PWAIT, uintptr(epfd),
		uintptr(unsafe.Pointer(unsafe.SliceData(events))), uintptr(len(events)), 0, 0, 0)
	if errno != 0 {
		return 0, errno
	}
	return int(r), nil
}

func (w *epollWaiter) close() {
	if w != nil && w.file != nil {
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

// watchListener has the loop wait on a TCP or Unix listener for the
// connections it takes.
func (e *Engine) watchListener(l streamListener) error {
	events := uint32(syscall.EPOLLIN) | epollET
	if l.shared {
		// Every poller waits on the engine's socket; see
		// Config.EngineLoop. Exclusive, a connection arriving wakes one
		// of them that is waiting rather than all.
		events |= epollExclusive
	}
	err := e.addFD(l.fd, listenerToken(l.fd), events)
	if err == syscall.EINVAL && l.shared {
		// Kernels before 4.5 know no EPOLLEXCLUSIVE, and wake them all.
		err = e.addFD(l.fd, listenerToken(l.fd), events&^epollExclusive)
	}
	return err
}

// udpReadEvents are what make the loop read a UDP socket: the datagrams it
// holds, and an error or hangup it reports, which a read passes on. A socket
// watched for room as well reports EPOLLOUT on every round for as long as it
// has any, and one whose event is only that holds no datagram to read.
const udpReadEvents = syscall.EPOLLIN | syscall.EPOLLERR | syscall.EPOLLHUP

// watchUDP has the loop read a UDP socket's datagrams. UDP sockets are
// level-triggered, so the loop can stop reading one after its share of a
// round and still hear about the rest.
func (e *Engine) watchUDP(fd int) error {
	return e.addFD(fd, udpToken(fd), uint32(syscall.EPOLLIN))
}

// watchWritable has the loop watch a UDP socket for room as well as for
// datagrams, or, with on false, for datagrams alone again, as watchUDP, or
// registerDatagram for a dialed one, has it; see udpWaiters. The socket is
// level-triggered, so one that has room already reports it at once. Callers
// hold the socket's udpWaiters lock.
func (e *Engine) watchWritable(fd int, token uint64, dialed, on bool) error {
	events := uint32(syscall.EPOLLIN)
	if dialed {
		events |= syscall.EPOLLERR
	}
	if on {
		events |= syscall.EPOLLOUT
	}
	ev := syscall.EpollEvent{Events: events, Fd: int32(token), Pad: int32(token >> 32)}
	return syscall.EpollCtl(e.epollFD, syscall.EPOLL_CTL_MOD, fd, &ev)
}

// unwatch has the loop stop waiting on a listener or a UDP socket, which
// another loop may go on waiting on.
func (e *Engine) unwatch(fd int) { e.unregister(fd) }

// pausesInPlace says reads pause by the connection's readPaused flag alone:
// the socket stays registered for input, an edge it raises while paused is
// noted as a read owed, and resuming hands that read back; see
// settlePauseLocked. Taking input interest out and putting it back cost an
// epoll_ctl each, and a round trip through the event loop to make them.
const pausesInPlace = true

// setReadPaused does nothing; see pausesInPlace.
func (e *Engine) setReadPaused(*Connection, bool) error { return nil }

const (
	keepAliveTunable = true
	tcpKeepIdle      = syscall.TCP_KEEPIDLE
	tcpKeepInterval  = syscall.TCP_KEEPINTVL
	tcpKeepCount     = syscall.TCP_KEEPCNT
)

const (
	ipprotoMPTCP = 262
	solMPTCP     = 284
	mptcpInfo    = 1
)

// usingMultipathTCP reports a socket opened for Multipath TCP that has not
// fallen back to plain TCP, which is how the net package tells.
func usingMultipathTCP(s int) bool {
	proto, err := syscall.GetsockoptInt(s, syscall.SOL_SOCKET, syscall.SO_PROTOCOL)
	if err != nil || proto != ipprotoMPTCP {
		return false
	}
	_, err = syscall.GetsockoptInt(s, solMPTCP, mptcpInfo)
	return err != syscall.EOPNOTSUPP && err != syscall.ENOPROTOOPT
}

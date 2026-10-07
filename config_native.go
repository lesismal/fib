//go:build linux || darwin || windows

package fib

import (
	"time"

	"github.com/lesismal/fib/taskpool"
)

// Config controls listener and worker-pool sizing.
type Config struct {
	// Name labels the engine in what it logs, and names the task pools its
	// connections run on: "<Name>-workers" for the engine's own and
	// "<Name>-streams" for the HTTP/2 and HTTP/3 handlers. Engines of the
	// same name share both pools; see SharedTaskPool. Empty means
	// DefaultName.
	Name string
	// LogStatus has the engine log a line when it starts serving, with its
	// name, the addresses it listens on, its task pool and its pollers. It is
	// off by default. The task pools' own lines are switched separately, with
	// taskpool.SetLogStatus.
	LogStatus bool
	// Network and Addr name the listener the way net.Listen does: Network is
	// "tcp", "tcp4" or "tcp6", and Addr is a "host:port" such as ":9000",
	// "127.0.0.1:9000" or "[::1]:9000". A host resolves through the net
	// package, and a zero port asks the kernel to choose one. An empty Network
	// means "tcp", and an empty Addr means ":0", again as net.Listen reads
	// them.
	//
	// "tcp" with no host listens on both families where the kernel has IPv6,
	// "tcp4" and "tcp6" pin it to one. This is the same choice net.Listen
	// makes from the same arguments.
	//
	// "unix" listens on a Unix domain stream socket, with Addr as its path, as
	// net.Listen does: the path must not exist yet, it is removed when the
	// engine closes, and on Linux a leading '@' names an abstract socket.
	// Connections on it behave exactly like TCP ones.
	//
	// "udp", "udp4" and "udp6" bind UDP sockets instead, as net.ListenPacket
	// does. Each peer address that sends a datagram becomes a connection of
	// its own, whose OnData receives one datagram per call and whose Send
	// sends one; see UDPIdleTimeout for how such a connection ends.
	Network string
	Addr    string
	// Addrs, when it is not empty, is the complete set of addresses to listen
	// on and Addr is ignored; they all share Network. One server spanning
	// several addresses shares a single event loop, descriptor table, task
	// pool and buffer pool across all of them, where a server per address
	// gives each its own copy of all four.
	Addrs          []string
	Backlog        int
	WorkerCount    int
	MaxEvents      int
	ReadBufferSize int
	// WriteBufferHighWatermark pauses socket reads while at least this many
	// bytes are waiting to be written. This bounds userspace buffering while
	// TCP backpressure catches up. Set below zero to disable write backpressure.
	//
	// Crossing it is not free: the worker hands a command to the event loop,
	// which costs an eventfd write and an epoll_ctl. A watermark near the
	// message size makes every reply a crossing, so keep it well above one
	// round's worth of output.
	WriteBufferHighWatermark int
	// MaxPendingBytes caps the bytes this server may hold across all of its
	// connections waiting for their sockets. WriteBufferHighWatermark bounds a
	// single connection, which at 100k connections still admits a per-server
	// total of watermark*100k; this is the bound on the sum. Reads pause on
	// every connection while the budget is exhausted. Zero means unlimited.
	MaxPendingBytes int64
	UseWritev       bool
	// SocketSyscalls has connections read and write their sockets with
	// recvfrom, sendto and sendmsg rather than read, write and writev. Both
	// sets end in the same socket code, but read, write and writev reach it
	// through the VFS, which on every call checks the file's access mode,
	// runs the security module's file permission hook and notifies fsnotify.
	// Where a security module mediates file access, as AppArmor does in a
	// Docker container, that is a large share of a busy server's time:
	// go-websocket-benchmark's echo in such a container (50k connections, 8
	// server CPUs) served 611k messages a second at 634% CPU through read and
	// write, and 639k at 507% through the socket calls. A descriptor that is
	// not a socket falls back to read and write.
	//
	// DefaultConfig sets it. Only Linux honours it, and not on 386, which
	// reaches the socket calls only through socketcall, nor in a race, memory
	// or address sanitizer build.
	SocketSyscalls bool
	// TaskPoolMode picks the scheduler the workers run under. Prefer
	// SetTaskPoolMode over assigning it, so that WorkerCount and MaxEvents
	// follow the mode rather than staying at numbers tuned for the other one.
	TaskPoolMode taskpool.Mode
	// MinWorkerCount is the resident floor a ModeAdaptive pool retires down
	// to, with WorkerCount as the ceiling it grows to. Zero, the default,
	// lets an idle pool retire every worker and start them again when work
	// arrives. The other modes ignore it.
	MinWorkerCount int
	// SharedTaskPool has the engines of one Name run on one task pool. The
	// first of them builds it from its own settings, and those that follow
	// run on it as it is, whatever theirs say.
	SharedTaskPool bool
	// TaskPool, when set, runs the engine's connections instead of a pool the
	// engine builds from the fields above. See SetTaskPool.
	TaskPool TaskPool
	// IOPollers splits the engine across several event loops. The engine's
	// own loop is left to accept connections and serve its UDP sockets, and
	// every connection it accepts is handed to one of IOPollerCount further
	// loops, the one its descriptor picks modulo their number, which then
	// waits on that connection for the rest of its life. Connections the
	// engine dials, over TCP, UDP or a Unix socket, go to those loops the
	// same way. A UDP listener's peers share its one socket, so they stay on
	// the engine's own loop, unless ReusePort gives each poller a socket of
	// its own.
	//
	// Spreading the connections spreads the loops' own work, the waits,
	// registrations and wake-ups, over several cores. With or without it, a
	// loop only waits for events, and reads the UDP sockets peers share, and
	// hands the connections they make runnable to the engine's pool of
	// workers, the one TaskPoolMode,
	// WorkerCount and SharedTaskPool describe or SetTaskPool supplies: every
	// round, its reads and the OnData and OnClose they call, runs on a
	// worker, so a callback that takes a while holds up its own connection
	// and no other. The handlers of HTTP/2 and HTTP/3 requests run on a pool
	// of their own, apart from the workers that read their connections; see
	// Engine.HandlerPool.
	//
	// DefaultConfig sets it on more than four CPUs, counting GOMAXPROCS where
	// it is below runtime.NumCPU, and leaves it off on four or fewer, where
	// one loop serves as many requests as a poller would.
	// Without it, the engine serves listeners and connections alike on its
	// one loop. Setting it explicitly gives an engine pollers on any number
	// of CPUs.
	//
	// Only the Linux and macOS backends have pollers; on Windows the engine
	// keeps its single loop, as if this were unset.
	IOPollers bool
	// IOPollerCount is how many loops IOPollers creates. Zero or less means
	// the CPUs divided by four, rounded down, and at least one, up to 32
	// CPUs, and half the CPUs on more: 1 up to 7 CPUs, 2 on 8, 4 on 16, 8 on
	// 32, 32 on 64. The CPUs are runtime.NumCPU, or GOMAXPROCS where that is
	// lower.
	//
	// A loop only waits for events and hands them on, which costs a small
	// share of what the workers spend on them, so one loop keeps up with the
	// workers of many CPUs, and each further loop competes with the workers
	// for the same Ps: after every round a loop yields to the workers it woke
	// and then waits behind them for a P, so each loop's rounds gather fewer
	// connections, and requests wait longer to be read. HTTP/2 echoes over
	// 10k connections on 4 CPUs measured 411k requests/s with one poller and
	// 399k with four, whose 99th percentile latency rose from 55ms to 64ms.
	// More loops pay off where the loops' own work, accepting and registering
	// connections and waking for them, is what runs short, as it may with
	// ReusePort and many connections arriving at once.
	IOPollerCount int
	// ReusePort binds the engine's listeners with SO_REUSEPORT, so that
	// other sockets that set it too, in this process or another of the same
	// user, may listen on the same address and share its connections, or
	// its datagrams.
	//
	// Under IOPollers on Linux the pollers accept TCP connections themselves
	// whether or not it is set: each poller listens on a socket of its own,
	// bound to the engine's address with SO_REUSEPORT, and accepts the
	// connections the kernel spreads onto that socket by a hash of their
	// addresses. The engine's own loop accepting every connection and waking
	// the poller it hands it to capped how fast connections were accepted
	// however many cores there were to serve them: HttpArena's limited-conn
	// profile, ten requests a connection over 4096 at a time on 64 CPUs,
	// served 0.94M requests a second that way and 1.65M with the pollers
	// accepting. What it gives up is balance: a connection stays with the
	// poller its hash picked, however busy that poller is. Without
	// ReusePort the engine first claims the address with a socket bound
	// without SO_REUSEPORT, so that an address another socket holds is
	// refused with EADDRINUSE as it always was, and only a socket that sets
	// SO_REUSEPORT itself can join the engine's afterwards. Unix sockets, and
	// the other platforms, keep the engine's loop accepting.
	//
	// On a UDP address it has the pollers read datagrams in the same way,
	// which they do only with it set: each poller reads a
	// socket of its own bound there, and the kernel hands each datagram to
	// one of the sockets by a hash of its source and destination addresses,
	// waking only the poller that reads it. A peer's datagrams therefore all
	// reach one poller, which keeps its connection, its OnData and its idle
	// timeout; without it the engine's own loop reads every peer. The hash
	// holds only while the sockets sharing the address stay the same, so a
	// socket another process binds there moves some peers to it, and a peer
	// whose address changes reaches whichever poller its new address hashes
	// to, as a new peer; package http3 finds its QUIC connection there by
	// its connection ID.
	//
	// In a child of package prefork it is always set, since the children
	// all listen on the same addresses.
	ReusePort bool
	// UDPIdleTimeout closes a UDP peer's connection once the peer has neither
	// sent nor been sent a datagram for this long, since UDP has no close of
	// its own to end it. OnClose receives ErrUDPIdleTimeout. Zero means
	// DefaultUDPIdleTimeout and a negative value keeps peers until they are
	// closed. Dialed UDP connections are never timed out.
	UDPIdleTimeout time.Duration
	// customPoolSizing records that SetPoolSizing pinned the sizing, so that a
	// later SetTaskPoolMode does not overwrite it.
	customPoolSizing bool
}

func DefaultConfig() Config {
	sizing := DefaultPoolSizing(taskpool.ModeAdaptive)
	return Config{Name: DefaultName, Network: "tcp", Addr: ":9000", Backlog: defaultBacklog(), WorkerCount: sizing.WorkerCount,
		MaxEvents: sizing.MaxEvents, ReadBufferSize: 16 * 1024,
		WriteBufferHighWatermark: defaultWriteHighWatermark, MaxPendingBytes: defaultMaxPendingBytes,
		UseWritev: true, SocketSyscalls: true, TaskPoolMode: taskpool.ModeAdaptive, SharedTaskPool: true,
		IOPollers: defaultIOPollers(defaultCPUs())}
}

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
	// InlineHandlers runs a ready connection's round on the event loop instead
	// of handing it to a worker.
	//
	// The handoff is not free, and at high message rates it is the dominant
	// cost: it makes a goroutine runnable, and that goroutine has to be given a
	// P before it can issue the read. An execution trace of a 100k-connection
	// echo run measured 872 seconds of runnable-but-not-running time in a
	// 2-second window, almost all of it on workers woken from the loop.
	// Skipping the handoff measured 446k echoes/s against 395k for the same
	// build with workers, and 104k accepted connections/s against 95k.
	//
	// The cost is that a handler now blocks its whole server: the loop cannot
	// collect events, accept, or serve any other connection while it runs. Set
	// this only when every handler is short and never blocks. Handlers that do
	// I/O, take contended locks, or run unbounded work want the worker pool,
	// which exists precisely so that one slow connection cannot stall the rest.
	InlineHandlers bool
	// IOPollers splits the engine across several event loops. The engine's
	// own loop is left to accept connections and serve its UDP sockets, and
	// every connection it accepts is handed to one of IOPollerCount further
	// loops, the one its descriptor picks modulo their number, which then
	// serves that connection for the rest of its life. Connections the
	// engine dials, over TCP, UDP or a Unix socket, go to those loops the
	// same way. A UDP listener's peers share its one socket, so they stay on
	// the engine's own loop.
	//
	// Spreading the connections spreads the loop's own work, the waits,
	// registrations and wake-ups, over several cores, and each loop then runs
	// its connections' rounds itself: the engine's task pool is a
	// taskpool.ModeInline one, which recovers a panicking handler the way a
	// worker does. As with InlineHandlers, a handler that blocks stalls every
	// connection on its loop. A connection whose handlers may take a while
	// asks for workers instead, with Connection.SetRunOnWorkers, and runs on
	// the pool TaskPoolMode, WorkerCount and SharedTaskPool describe, built
	// the first time one does: the http package has every HTTP/1 connection
	// do so, while an HTTP/2 or HTTP/3 one reads on the engine's own pool and
	// runs its requests on the stream pool, which is unaffected and still
	// sized from WorkerCount. A pool supplied through SetTaskPool is kept.
	//
	// DefaultConfig sets it. Without it, the engine serves listeners and
	// connections alike on its one loop, and runs every connection's rounds
	// on the pool of workers.
	//
	// Only the Linux and macOS backends have pollers; on Windows the engine
	// keeps its single loop and its task pool, as if this were unset.
	IOPollers bool
	// IOPollerCount is how many loops IOPollers creates. Zero or less means
	// one per CPU, runtime.NumCPU.
	//
	// One per CPU suits connections whose rounds run on their loops. Where
	// they run on workers instead, as every HTTP/1 connection's do (see
	// Connection.SetRunOnWorkers), a loop only waits for events and hands
	// them on, which one loop keeps up with, and each further loop competes
	// with the workers for the same Ps: after every round a loop yields to
	// the workers it woke and then waits behind them for a P, so each loop's
	// rounds gather fewer connections, and requests wait longer to be read.
	// An HTTP/1 echo over 10k connections on three CPUs measured 583k
	// requests/s without pollers, 584k with one poller, and 569k with three,
	// whose 99th percentile latency rose from 25ms to 37ms. Set it low, one
	// or a few, for such a load.
	IOPollerCount int
	// ReusePort binds the engine's TCP listeners with SO_REUSEPORT, so that
	// other sockets that set it too, in this process or another of the same
	// user, may listen on the same address and share its connections.
	//
	// Under IOPollers on Linux it also moves accepting onto the pollers: each
	// poller listens on a socket of its own, bound to the engine's address,
	// and accepts the connections the kernel spreads onto that socket by a
	// hash of their addresses. Without it, the engine's own loop accepts
	// every connection and then wakes the poller it hands it to, and one loop
	// doing that for every connection caps how fast connections are accepted
	// however many cores there are to serve them. What it gives up is
	// balance: a connection stays with the poller its hash picked, however
	// busy that poller is. Unix sockets, and the other platforms, keep the
	// engine's loop accepting.
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
		UseWritev: true, TaskPoolMode: taskpool.ModeAdaptive, SharedTaskPool: true, IOPollers: true}
}

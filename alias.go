// This file re-exports the public API of package core, where the implementation
// lives. alias_test.go fails when the two disagree: add a re-export here
// whenever core's public API changes.

package fib

import (
	"github.com/lesismal/fib/core"
	"github.com/lesismal/fib/taskpool"
)

// AcceptPlacement says which poller serves a connection a poller accepts on
// a listening socket of its own; see Config.AcceptPlacement.
type AcceptPlacement = core.AcceptPlacement

// Binding is one group of addresses an engine listens on, the network they
// share, and the handler that serves the connections, or the UDP peers, they
// take. New takes them, so that one engine serves several protocols at once,
// each on addresses of its own: HTTP on one port, HTTPS on another, HTTP/3 on
// a UDP port, gRPC on a fourth and a Unix socket for administration, all on a
// single event loop, descriptor table, task pool and buffer pool, where an
// engine per protocol gives each its own copy of all four.
//
// The handler is whatever stack the protocol needs:
//
//	engine, err := fib.New(fib.DefaultConfig(),
//		fib.Binding{Addr: ":80", Handler: fibhttp.NewHandler(router)},
//		fib.Binding{Addrs: []string{":443", ":8443"}, Handler: fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), fibhttp.NewHandler(router))},
//		fib.Binding{Network: "udp", Addr: ":443", Handler: http3.NewHandler(tlsConfig, router)},
//		fib.Binding{Addr: ":9000", Handler: grpcServer},
//		fib.Binding{Network: "unix", Addr: "/run/app.sock", Handler: admin},
//	)
//
// Everything else, the backlog, the buffers, the watermarks, the pollers and
// ReusePort, is the engine's and applies to every binding alike. MultipathTCP
// is the exception: whether a listener is a Multipath TCP one is a property of
// the address it listens on, so it stays the binding's, as Network and Handler
// do. With IOPollers, each binding's connections are spread over the same
// pollers as every other binding's.
//
// Bindings can also be added and removed while the engine runs; see
// Engine.AddBinding, Engine.RemoveBinding and Engine.ShutdownBinding.
type Binding = core.Binding

// Config controls listener and worker-pool sizing.
type Config = core.Config

// Connection is safe to use from callback and application goroutines.
//
// A server holds one for every connection, so its fields are laid out to keep
// it small, and in the order a round touches them: the words a round reads or
// writes first, then the flags and booleans it checks, which share words
// rather than each padding one out, and last what only opening or closing
// needs. On 64-bit Linux and macOS it is 192 bytes, a size class whose objects
// start on a cache-line boundary, so a read round touches three lines of it
// rather than the four a 208-byte object always straddles. With the
// attachment kept in it rather than boxed, that cut go-websocket-benchmark's
// user time per message on 8 Ryzen 7800X3D CPUs by 5.5% pipelined (348 to
// 329ns) and 4.6% in echo.
type Connection = core.Connection

// CorkHolder is a Layer that holds what is sent while the connection is
// corked, rather than transforming each send as it is made, and sends it all
// once the cork comes off: the tls package seals a round's replies into one
// record, where a record each would cost a seal, its overhead and an item of
// the write. The layer bounds what it holds, sending it at once past its
// bound, and asks Corked as it takes each send, under the lock that orders
// its sends.
//
// The connection calls ReleaseCorked, without its own lock, each time the
// cork comes off, after it has and before it writes what is queued; the layer
// then sends what it holds through SendRaw or SendRawPooled. A send made in
// between finds the connection uncorked, and the layer sends it behind what
// it held.
type CorkHolder = core.CorkHolder

// DatagramsHandler is a Handler that takes a UDP connection's datagrams in
// bursts. When a Handler implements it, the datagrams that have arrived for
// a connection by the time its worker runs go to OnDatagrams together, in
// the order they arrived, instead of to OnData one at a time. A protocol
// that answers what it receives - with acknowledgements, say - can then
// answer the whole burst at once, in fewer datagrams than it would one
// arrival at a time. The portable backend, on platforms other than Linux,
// macOS, the BSDs and Windows, keeps calling OnData.
//
// Each datagram belongs to the handler, as a UDP connection's OnData data
// does; the slice holding them does not, and must not be kept once
// OnDatagrams returns. A datagram is a buffer from package bufferpool, so a
// handler that is done with one, with nothing left referring to it, may give
// it back with bufferpool.Put for the next one to be read into; one it keeps
// or drops is collected like any other slice.
type DatagramsHandler = core.DatagramsHandler

// DialOptions describes a dial for DialWith: what Dial and DialWithHandler
// take, and the local end the socket is to leave from.
type DialOptions = core.DialOptions

// Engine owns the listeners, the event loop, its command queue, and the worker
// pool. The loop itself is platform code: epoll on Linux, kqueue on macOS and
// the BSDs, and an I/O completion port on Windows, each embedded here as enginePlatform.
//
// With Config.IOPollers, the engine also owns the pollers its connections are
// served on. Each poller is an Engine of its own with no listeners, whose
// loop, command queue and descriptor table serve only the connections handed
// to it, while the settings, the handler, the task pool and the server-wide
// budget are the parent's.
type Engine = core.Engine

// EngineCloseHandler is a Handler told that Engine.Close is about to end one
// of its connections, while it can still send: a protocol may have a last
// frame to go, HTTP/2's GOAWAY, and a datagram protocol, whose peer sees no
// end of a stream at all, has to say so itself, as QUIC does with
// CONNECTION_CLOSE. Without it the peer finds out once the connection has
// ended, or has given up waiting. OnEngineClose runs on the goroutine that
// called Close, once for each open connection whose handler takes it, its
// listeners' UDP peers and those the engine dialed among them, before
// OnClose. A round of the connection may be running OnData at the same time,
// so it must not block, nor take a lock that a handler in OnData may hold for
// long. What it sends goes only while it runs: Close cuts the connection once
// it returns, so it must write any last frame itself, without waiting.
// Engine.Stop leaves connections open, and calls it for none until Close; the
// portable backend, whose Stop closes the sockets, calls it for none.
type EngineCloseHandler = core.EngineCloseHandler

// File is what SendFile sends from: an *os.File, or any type that wraps one
// and passes its descriptor on through SyscallConn.
type File = core.File

// FileLayer is a Layer that carries a file SendFile sends as the socket takes
// it, rather than having the whole range read and handed to Send before
// SendFile returns: the tls package seals a file's records as the socket
// drains, or has the kernel seal them, where it can, and the file go by
// sendfile.
//
// SendFile calls CanSendFile first, and SendFile only if it reports true;
// otherwise the range is read and handed to Send. Context.SendFile in package
// http asks through Connection.QueuesFiles, before it begins a response.
type FileLayer = core.FileLayer

// FileSealer transforms a file's bytes as SendFileRaw reads them for the
// socket: a chunk at a time, each read once the socket has taken the one
// before, so a file costs a chunk's memory however large it is.
//
// The connection calls Seal from its flush, under its own lock, in the
// file's order, and never twice at once: Seal must not send on the
// connection, nor wait.
type FileSealer = core.FileSealer

// Handler callbacks run on a logical worker, except OnOpen, which runs on the
// event-loop goroutine. OnClose runs in the connection's last round, where its
// OnData ran, after every OnData before it; that is the event loop where the
// engine runs rounds there. Data is only valid for the duration of OnData.
type Handler = core.Handler

// HandlerFuncs allows callers to implement only the callbacks they need.
type HandlerFuncs = core.HandlerFuncs

// HangupHandler is a Handler that is told the peer has gone while one of the
// connection's rounds is still running. A round reads the end of input, or
// the socket's error, only once its handler returns from OnData, and then
// closes the connection; a handler that waits in OnData, on a database or on
// another server, would hear nothing until it was done. OnHangup lets the
// protocol tell whatever that handler is serving that its peer has gone, so
// the work can stop early. The connection is still open when OnHangup runs.
// It closes, and OnClose runs, when the round reads the end of input as
// usual.
//
// OnHangup runs on the event loop, at most once per connection, when the
// loop sees the peer end its side of the connection, or the socket fail,
// while a round of the connection is running. A round only queued for a
// worker is not running: it reads the end itself, at once. It must
// not block, nor take a lock that a handler in OnData may hold; work that
// would do either goes to a goroutine or a pool. A hangup that arrives
// while no round is running needs no early word: the round it starts
// reads it and closes the connection.
//
// The epoll and kqueue backends call it. Windows arms a connection's read
// only between its rounds, and the portable backend reports the end of input
// as a close, so there the hangup is heard when the round ends, as it would
// be without OnHangup.
type HangupHandler = core.HangupHandler

// Layer carries a connection's sends, transforming them on the way to the
// socket. The tls package installs one that encrypts; any other protocol that
// frames or encrypts what a connection sends, such as a DTLS implementation
// over UDP, installs its own the same way.
//
// While a layer is installed, Send, SendOwned, SendParts and SendPartsShared
// hand their bytes to its Send, and CloseAfterSend calls its CloseAfterSend. The layer reaches
// the socket itself through SendRaw and CloseAfterSendRaw. Incoming bytes are
// not the layer's concern: the handler that installed it sees them in OnData
// and decides what the handler it wraps receives.
type Layer = core.Layer

// Machine is the hardware the defaults are sized against: the cores the
// process may run on, the memory it may use, and the descriptors it may hold
// open. A field that could not be read is zero, and the sizing falls back to
// its conservative constants rather than guessing.
type Machine = core.Machine

// Parameter is one setting as the startup log reports it: its name, its
// effective value, whether the caller set it or fib's default produced it,
// and what it does.
type Parameter = core.Parameter

// PoolSizing is how many workers a task pool may run and how many tasks it may
// hold waiting for them. How many events an event loop collects in one round
// is Config.PollBatchSize, apart from either.
type PoolSizing = core.PoolSizing

// Protocol is the transport a connection runs over.
type Protocol = core.Protocol

// Settings is a snapshot of an engine's effective configuration: the values
// the engine actually runs with, after the defaults and the machine sizing
// have filled in what the caller left out. It is what the engine's startup log
// prints with Config.LogStatus and what Engine.Settings returns, so that a
// deployment can be read back from its log rather than inferred from its
// config file.
type Settings = core.Settings

// ShutdownHandler is a Handler told that its engine is shutting down; see
// Engine.Shutdown. The engine cannot tell whether a connection is in the
// middle of an exchange or idle between two, and Shutdown waits for every
// connection it accepted to close. OnShutdown is the protocol's cue to end
// the connection as soon as it can without cutting an exchange short: to
// close it now if it is idle, to close it once the exchange in flight is
// done, or to tell the peer it is going away, as HTTP/2's GOAWAY and a
// WebSocket close frame do. Engine.ShuttingDown says the same thing to code
// that asks, such as a protocol about to keep a connection open for its next
// exchange.
//
// OnShutdown runs when Shutdown begins and then again, at a falling rate,
// for as long as the connection stays open, so that a connection busy the
// first time is found idle a later one; it must be safe to call more than
// once. It is not called for a connection the engine dialed, nor for one
// already closing. It runs on the connection's event loop, like OnHangup,
// and may run while one of the connection's rounds is running OnData: it
// must not block, nor take a lock that a handler in OnData may hold. The
// portable backend calls it on the goroutine that called Shutdown, under
// the same rules.
type ShutdownHandler = core.ShutdownHandler

// Sizing is the part of Config the machine sizes: the per-connection write
// watermark, and, when a server-wide budget is chosen, the bytes that budget
// may hold and the hard cap on one connection's queue.
//
// The read buffer and the protocol windows are not sized here. The read
// buffer's memory is O(event loops), not O(connections), and it is a
// throughput choice; the windows are a network path's, not the machine's.
type Sizing = core.Sizing

// Stats reports what backpressure this server has applied. It answers the
// question the counters exist for: whether reads were ever paused, and which
// of the two bounds did it, since the two have very different causes.
type Stats = core.Stats

// TaskPool runs the rounds the event loop schedules. *taskpool.TaskPool
// implements it; set Config.TaskPool to supply a different one.
//
// Each task must eventually run exactly once. GoTasks accepts a prefix of its
// argument and reports how long that prefix is; the engine closes the
// connections of the tasks it did not accept, so a pool that is stopping may
// refuse work, but one that is merely busy should queue it rather than refuse.
// A task may block for as long as the handler it calls does, so a pool that
// runs tasks on a fixed set of goroutines bounds how many connections progress
// at once.
type TaskPool = core.TaskPool

// AcceptFDModulo, the default, hands the connection to the poller its
// descriptor picks modulo the number of pollers.
const AcceptFDModulo = core.AcceptFDModulo

// AcceptKernelHash has the poller that accepted a connection serve it,
// so the kernel's hash of the connection's addresses, which picked the
// socket, picks the poller.
const AcceptKernelHash = core.AcceptKernelHash

// DefaultName is the Name an engine takes when its Config sets none.
const DefaultName = core.DefaultName

// DefaultUDPIdleTimeout is how long a UDP peer may stay silent before its
// connection is closed, when Config.UDPIdleTimeout is left at zero.
const DefaultUDPIdleTimeout = core.DefaultUDPIdleTimeout

// ProtocolTCP is a TCP connection.
const ProtocolTCP = core.ProtocolTCP

// ProtocolUDP is a UDP connection: a dialed UDP socket, or a peer of a
// UDP listener.
const ProtocolUDP = core.ProtocolUDP

// ProtocolUnix is a Unix stream socket.
const ProtocolUnix = core.ProtocolUnix

// ErrNoLocalPort is what a dial reports, wrapped in its *net.OpError, when
// every port in its DialOptions' range is in use.
var ErrNoLocalPort = core.ErrNoLocalPort

// ErrNotUDP is what a datagram-only option reports on a connection that is
// not UDP.
var ErrNotUDP = core.ErrNotUDP

// ErrSendFileDatagram is what SendFile returns on a UDP connection, whose
// sends are datagrams rather than a stream a file could be copied into.
var ErrSendFileDatagram = core.ErrSendFileDatagram

// ErrSendQueueFull is what a send returns when it would take the connection's
// send queue past Config.MaxWriteQueueBytes, or the limit
// Connection.SetWriteQueueLimit set. The connection is closed with it, rather
// than the queue growing without bound for a peer that never reads.
var ErrSendQueueFull = core.ErrSendQueueFull

// ErrUDPIdleTimeout is what OnClose receives for a UDP peer that neither sent
// nor was sent anything for Config.UDPIdleTimeout.
var ErrUDPIdleTimeout = core.ErrUDPIdleTimeout

// ErrWouldBlock is what Read returns when the socket has nothing waiting. The
// connection is event-driven, so a read that finds the socket empty reports
// this rather than waiting for the peer.
var ErrWouldBlock = core.ErrWouldBlock

// DefaultConfig returns fib's defaults, sized for the machine this process
// runs on. The pool follows the CPUs, the backlog the kernel, and the write
// watermark the read buffer and the machine's memory and descriptor limits;
// see DefaultConfigFor and Machine.Sizing.
func DefaultConfig() Config {
	return core.DefaultConfig()
}

// DefaultConfigFor returns fib's defaults with readBuffer for
// Config.ReadBufferSize, and the rest sized for it: zero means fib's own read
// buffer. The write watermark follows the read buffer at four times it,
// bounded by 64 KiB and 256 KiB and by the reference budget and connection
// count; see Machine.Sizing.
//
// The server-wide budget stays off, as it is by default: only the
// per-connection watermark applies. A deployment that wants the aggregate
// bound sets Config.MaxPendingBytes, and Config.MaxWriteQueueBytes for one
// peer's share of it.
func DefaultConfigFor(readBuffer int) Config {
	return core.DefaultConfigFor(readBuffer)
}

// DefaultPoolSizing reports the sizing mode is tuned for, which is what
// DefaultConfig and SetTaskPoolMode install. Every mode now takes the same
// one; the parameter stays so that a mode tuned otherwise can be told apart.
func DefaultPoolSizing(mode taskpool.Mode) PoolSizing {
	return core.DefaultPoolSizing(mode)
}

// DefaultSizing reports the sizing for the machine this process runs on; see
// Machine.Sizing.
func DefaultSizing(readBuffer int, pendingBytes int64, maxConns int) Sizing {
	return core.DefaultSizing(readBuffer, pendingBytes, maxConns)
}

// DefaultStreamPoolSizing reports the sizing of the pool HTTP/2 and HTTP/3
// run their request handlers on while no engine is running: twice what
// DefaultPoolSizing reports for the engine. Once engines run, the pool's
// ceiling is twice the widest of their pools instead, whatever their mode;
// see streamPoolFactor.
func DefaultStreamPoolSizing(mode taskpool.Mode) PoolSizing {
	return core.DefaultStreamPoolSizing(mode)
}

// DefaultWriteWatermark reports the per-connection write watermark for
// readBuffer and the deployment's budget and connection cap. It is what
// Config.WriteBufferHighWatermark is when that is left at zero.
func DefaultWriteWatermark(readBuffer int, pendingBytes int64, maxConns int) int {
	return core.DefaultWriteWatermark(readBuffer, pendingBytes, maxConns)
}

// DetectMachine reports the machine this process runs on. It is read once:
// the cgroup limit and the descriptors' limit do not move while a server
// runs.
func DetectMachine() Machine {
	return core.DetectMachine()
}

// New creates an engine that listens on every binding's addresses and serves
// each binding's connections with the binding's own Handler. Either every
// address listens or, with the error that stopped one, none does;
// BindingAddrs reports where each binding listens, by its index among
// bindings. AddBinding adds more later.
//
// With no bindings, New creates a client engine, whose connections are the
// ones it dials. A dial names the handler that serves its connection through
// DialWithHandler; Dial's connections get one that does nothing.
//
//	engine, err := fib.New(fib.DefaultConfig(),
//		fib.Binding{Addr: ":80", Handler: fibhttp.NewHandler(router)},
//		fib.Binding{Network: "udp", Addr: ":443", Handler: http3.NewHandler(tlsConfig, router)},
//	)
func New(config Config, bindings ...Binding) (*Engine, error) {
	return core.New(config, bindings...)
}

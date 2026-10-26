# Configuration

`fib.Config` configures one engine: its listeners, buffers, backpressure, worker pool, event
loops and connection limits. `fib.New` copies the config, opens the `Binding`s it is given, and
runs with that copy; changing the struct afterwards does not change the engine.

Most programs need only `fib.DefaultConfig()`, which sizes the engine for the machine it runs on,
and one or more bindings:

```go
cfg := fib.DefaultConfig()

engine, err := fib.New(cfg, fib.Binding{
	Addr:    ":8080",
	Handler: handler,
})
if err != nil {
	panic(err)
}
defer engine.Close()
if err := engine.Run(); err != nil {
	panic(err)
}
```

- `DefaultConfig()` follows the CPUs for the worker pool, the kernel for the listen backlog, and
  the machine's memory and descriptor limit for the write watermark and the optional budgets.
- `DefaultConfigFor(readBuffer)` is `DefaultConfig` with a different `ReadBufferSize`, and a
  watermark sized for it. Use it when a proxy or a bulk transfer wants a 64 KiB read, for example.
- A zero `fib.Config{}` is *not* `DefaultConfig`: booleans stay false and the pool and logging
  settings are not filled in. Build from `DefaultConfig` and change what you need.
- A field whose zero is a sentinel means "choose the default": `WriteBufferHighWatermark` (negative
  disables it), `ReadBufferSize`, `MaxEvents`, `PollBatchSize`, `Backlog`, `IOPollerCount`,
  `UDPIdleTimeout` (negative keeps UDP peers forever), `Network` and `Name`. For the others, zero
  has its literal meaning: `MaxPendingBytes`, `MaxWriteQueueBytes` and `MaxConns` are off, and the
  rest of the booleans are as set.

## `Config` reference

The defaults below are those of `DefaultConfig()` on a native backend. Machine-dependent values are
marked *dynamic* and explained in [How the machine sizes the defaults](#how-the-machine-sizes-the-defaults).

### Identity and logging

| Field | Default | Description |
| --- | --- | --- |
| `Name` | `"fib"` | Engine name. It names the worker pool `<Name>-workers` and the HTTP/2 and HTTP/3 handler pool `<Name>-streams`. Engines of the same name share both pools; see `SharedTaskPool`. |
| `LogStatus` | `false` | Log one `fib: engine started` line when the engine starts: name, listen addresses, task pool, pollers and accept placement. |
| `LogSettings` | `true` | Log every effective setting when the engine starts, one line per parameter, with its value, `default` or `custom` source, and a description. Set it to false to silence the list; `Engine.LogSettings()` prints it on demand either way. |

### Listeners and connections

| Field | Default | Description |
| --- | --- | --- |
| `Network` | `"tcp"` | Network for a `Binding` whose own `Network` is empty: `tcp`, `tcp4`, `tcp6`, `unix`, `udp`, `udp4` or `udp6`. |
| `Backlog` | *dynamic* | Listen queue depth for TCP and Unix listeners: the kernel's `somaxconn` where it can be read, otherwise `SOMAXCONN`. An overflowing accept queue silently drops clients, so the kernel's value is the useful default. |
| `MaxConns` | `0` (off) | Most connections the engine's bindings may have accepted and not yet closed, all bindings together. One past it is closed before a handler sees it and counted in `Stats.ConnsRefused`. Dialed connections and UDP peers are not counted. |
| `ReusePort` | `true` | Bind listeners with `SO_REUSEPORT`, so another socket of the same user may share the address. Under `IOPollers` on Linux, pollers accept on sockets of their own whether or not this is set. A prefork child always sets it. |
| `BalanceReusePort` | `false` | On Linux with `IOPollers` and without `EngineLoop`, attach an eBPF selector that hands reuseport connections to the pollers round-robin instead of by the kernel's address hash. Needs more than one poller and `CAP_BPF` (`CAP_SYS_ADMIN` before Linux 5.8); without it, the kernel's hash is kept with a log line. |
| `AcceptPlacement` | `AcceptFDModulo` | Which poller serves a connection a poller accepted on a TCP socket of its own: `AcceptFDModulo` by descriptor (the default, and what every other connection does), or `AcceptKernelHash` by the kernel's address hash, which is the socket's own poller. Applies only on Linux with `IOPollers` without `EngineLoop`. |
| `UDPIdleTimeout` | `0` → `60s` | Close a UDP peer's connection after it has neither sent nor received a datagram this long. `OnClose` receives `ErrUDPIdleTimeout`. Negative keeps peers until they are closed. Dialed UDP connections never time out. |

### Buffers and backpressure

| Field | Default | Description |
| --- | --- | --- |
| `ReadBufferSize` | `16 KiB` | Bytes one read of a connection's socket takes; for a proxy, also the most passed on in one write. One buffer per event-loop round, borrowed from the pool, not one per connection. |
| `WriteBufferHighWatermark` | *dynamic* (64 KiB for the default read buffer) | Bytes a connection may have waiting for its socket before its reads pause. The low watermark is a quarter of it. It bounds userspace buffering while TCP backpressure catches up; it does not cap the queue. Zero sizes it from `ReadBufferSize`, the machine and the budget; negative disables it. |
| `MaxPendingBytes` | `0` (off) | Bytes all connections together may have waiting for their sockets. When the sum reaches it, every connection pauses reads until it falls to a quarter. Zero leaves only the per-connection watermark; set it when the watermark times the connection count is more memory than the deployment can spare. |
| `MaxWriteQueueBytes` | `0` (off) | Per connection, the bytes queued for the socket that it has not taken: what `Send`, `SendParts` and TLS leave behind, not an unread `SendFile` file. A send past it closes the connection with `ErrSendQueueFull`. It is the bound a broadcast wants, since the watermark only pauses reads and `MaxPendingBytes` bounds the total rather than one peer. Zero means no cap; a connection may set its own with `SetWriteQueueLimit`. |

### Writes and batching

| Field | Default | Description |
| --- | --- | --- |
| `UseWritev` | `true` | Send several buffers in one `writev` where the platform has it. A single buffer still uses `write`. |
| `CoalesceWrites` | `true` | Cork a read round's replies and write them together when the round ends, so k pipelined requests cost one write rather than k. Only the native engines have read rounds; the portable backend ignores it. |
| `SocketSyscalls` | `true` | Read and write sockets with `recvfrom`, `sendto` and `sendmsg` rather than `read`, `write` and `writev`, which saves the VFS file-permission hooks, notably under AppArmor. Only Linux honors it, and not on 386, nor under the race, memory or address sanitizers. |

### Worker pool

| Field | Default | Description |
| --- | --- | --- |
| `WorkerCount` | *dynamic* | Ceiling on the pool's workers: `max(GOMAXPROCS×1000, NumCPU×20)`. The pool grows and shrinks with load, so the ceiling costs nothing until work reaches it. |
| `MaxEvents` | *dynamic* | Tasks the pool may hold waiting for a worker: `clamp(WorkerCount×10, 10,000, 100,000)`. |
| `TaskPoolMode` | `ModeAdaptive` | How the pool grows and shrinks: `elastic` forks a worker per submission, `adaptive` keeps parked workers and grows on demand, `adaptive-chan` does the same through a channel, and `inline` runs each task on its submitter. Prefer `SetTaskPoolMode`, which also moves the sizing. |
| `MinWorkerCount` | `0` | Resident floor an adaptive pool retires down to. Zero retires every idle worker and starts one again when work arrives; the other modes ignore it. |
| `SharedTaskPool` | `true` | Engines of one `Name` share one worker pool and one handler pool. The first engine to start builds them from its settings; later engines of that name use them as they are. Set it false, or give the engine another name, to isolate. |
| `TaskPool` | `nil` | An external pool that runs the engine's connections instead of the built-in one; see `SetTaskPool`. When set, the pool mode, worker count, shared-pool setting and sizing no longer apply to it. The engine never stops a pool supplied this way. |

### Event loops

| Field | Default | Description |
| --- | --- | --- |
| `IOPollers` | `true` | Split the engine across event loops that wait for readiness, instead of one loop. Only epoll and kqueue have pollers; Windows keeps one loop. Without it the engine always has its own loop. |
| `IOPollerCount` | `0` → *dynamic* | Loops to create: 1 on up to 4 CPUs, 2 on up to 16, and CPUs/8 above that (2 on 17–23, 3 on 24, 4 on 32, 8 on 64). The CPUs are `min(NumCPU, GOMAXPROCS)`, so a cgroup's quota counts. |
| `EngineLoop` | `false` | Give an engine with `IOPollers` a loop of its own to accept every TCP and Unix connection and read the UDP sockets. By default the pollers listen as well as serve, which lets them accept in parallel. |

## `Binding` reference

A `Binding` is one group of addresses an engine listens on, the network they share, and the
handler for the connections or UDP peers they take. `fib.New` takes any number of them; they share
the engine's loops, pools and buffers.

| Field | Default | Description |
| --- | --- | --- |
| `Network` | `Config.Network` (`"tcp"`) | `tcp`, `tcp4`, `tcp6`, `unix`, `udp`, `udp4` or `udp6`, as `net.Listen` and `net.ListenPacket` name them. |
| `Addr` | `":0"` | `host:port`, a Unix path, or an abstract Unix address with a leading `@`. A zero port asks the kernel to choose one; `BindingAddrs` reports it. |
| `Addrs` | `nil` | A complete set of addresses for the binding, sharing `Network`. When set, `Addr` is ignored. |
| `Handler` | required | The handler that serves the binding: an HTTP handler, a TLS server, an HTTP/3 server, a WebSocket handler, a gRPC server, a raw connection handler, and so on. |

Everything else — the backlog, buffers, watermarks, pollers and `ReusePort` — belongs to the
engine and applies to every binding. Bindings may be added and removed while the engine runs with
`AddBinding`, `RemoveBinding` and `ShutdownBinding`.

## How the machine sizes the defaults

`DefaultConfig` reads the machine once:

- CPUs: `min(runtime.NumCPU(), runtime.GOMAXPROCS(0))`, so a cgroup CPU quota counts.
- Memory: the machine's physical memory, or a lower cgroup memory limit (`memory.max` on cgroup v2,
  `memory.limit_in_bytes` on v1), read from the process's cgroup and its parents.
- Descriptors: the soft `RLIMIT_NOFILE`.

If the memory cannot be read, the watermark stays at 64 KiB and no budget is invented.

The per-connection watermark is:

```text
Bref = MaxPendingBytes, or Memory/2 when no budget is set   (a sizing reference only)
Cref = clamp(min(Bref/16 KiB, 0.8 × RLIMIT_NOFILE, MaxConns or 100,000), 100, ...)
W    = clamp(min(4 × ReadBufferSize, 4 × Bref / Cref), 64 KiB, 256 KiB)
W    = floor(W, 16 KiB)
```

The 4× is a 25% backlog policy: if a quarter of the reference connections were fully backlogged,
they would reach the reference budget. `DefaultConfig` leaves `MaxPendingBytes` at 0, so this only
chooses the watermark; a deployment that wants the aggregate bound sets the budget itself.
`Machine.Sizing(readBuffer, pendingBytes, maxConns)` exposes the same arithmetic, and
`DefaultWriteWatermark` returns just the watermark. `MaxWriteQueueBytes` is derived as
`clamp(Bref/8, 8 MiB, 32 MiB)` when a budget is set; `DefaultConfig` leaves it off.

With the default 16 KiB read buffer the watermark is 64 KiB on every machine. With a 64 KiB read
buffer, as a proxy or a bulk transfer wants, it is:

| Machine memory | Watermark |
| --- | --- |
| 2 GiB or less | 64 KiB |
| 4 GiB | 80 KiB |
| 8 GiB | 160 KiB |
| 16 GiB or more | 256 KiB |

`DefaultConfigFor(64 << 10)` gives that read buffer and watermark directly. It is the sizing a
gateway that proxies bodies usually wants.

## Startup log and effective settings

With `LogSettings` on, the engine prints a header and one line per setting when it starts:

```text
INFO fib: engine config engine=gateway parameters=29
INFO fib: config parameter=readBuffer value="65536 (64 KiB)" source=custom about="bytes one read of a connection's socket takes"
INFO fib: config parameter=writeWatermark value="262144 (256 KiB)" source=default about="bytes a connection may have waiting before its reads pause; zero is sized from readBuffer and the machine"
INFO fib: config parameter=maxPendingBytes value="33255979008 (30.97 GiB)" source=custom about="bytes all connections together may have waiting; zero means no budget"
```

`source=default` means the value came from fib's default or its machine sizing; `source=custom`
means the caller set a different value. `Engine.LogSettings()` prints the same list at any time,
whether or not `LogSettings` is set. `Engine.Settings()` returns the values programmatically, and
`Settings.Parameters()` returns the same rows the log uses.

`LogStatus` is separate: it prints the one-line `fib: engine started` banner, off by default.

## Changing things at runtime

`fib.New` copies the config. To change the pool before the engine runs, use `SetPoolSizing` and
`SetTaskPoolMode`; `SetTaskPool` supplies a pool the caller owns. Per connection, `SetWriteQueueLimit`
caps one peer's queue, `SetWriteBackpressure(false)` lets a relay pace both sides itself, and
`HoldReads` gives application-level read backpressure. Protocol packages (HTTP/1.x, HTTP/2, HTTP/3,
WebSocket, gRPC) have their own configs for their own windows and limits; see
[HTTP/1](http1.md), [HTTP/2](http2.md), [HTTP/3](http3.md) and the [guide](guide.zh-CN.md).

## See also

- [Architecture](architecture.html)
- [Hacks](hacks.md) — the design notes behind the watermark, cork and batching
- [Load testing](loadtest.md)

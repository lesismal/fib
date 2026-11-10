# Performance and operations

[English](22-performance.md) | [简体中文](22-performance.zh-CN.md)

A fib server is fast out of the box, but "out of the box" is a set of defaults that assume one
workload; this chapter is about choosing the sizes for yours and keeping the result observable.
It covers the task pool's modes, sizing and runtime resizing, custom and shared pools, buffers and
backpressure, event loops, `prefork`, `fib/log`, the `pprof` middleware, the engine's counters,
the load-testing campaign and finally the CI conformance suite and a production checklist. It
assumes chapter [02-engine.md](02-engine.md); the tricks that change a handler's shape are in
[21-hacks.md](21-hacks.md).

## The worker pool model

Every read round, handler call, callback and write of a connection runs as a task on a
`taskpool`. The modes differ in how workers are made and retired, not in what a task is:

| Mode | Workers |
| --- | --- |
| `ModeAdaptive` (default) | parked workers between tasks; grows when every worker is busy and none is on its way, retires idle ones, between `MinWorkerCount` and `WorkerCount` |
| `ModeAdaptiveChan` | the same policy on a channel-backed queue rather than a mutex-guarded stack |
| `ModeElastic` | forks a worker per submission while it has capacity, lingers briefly, retires after that |
| `ModeInline` | no workers: runs each task, with recover, on the submitting goroutine |

`ModeAdaptive` is also sharded by P (a submission goes to the shard of the submitting goroutine's
P, plus one random shard if that one has a backlog), queues in a lock-free MPMC ring per shard, and
keeps warm idle workers on a LIFO stack, which is why the README's
[Taskpool section](../../README.md#taskpool) shows it ahead on contended, event-loop-shaped
workloads. `ModeInline` is a library feature only: an engine refuses it, because a connection's
round would then run on the event loop.

## Sizing the pool

`WorkerCount` is a ceiling, not a population: adaptive pools start workers only when work needs
them, so a high ceiling costs nothing until the load reaches it. `DefaultPoolSizing` computes
`WorkerCount = max(GOMAXPROCS×1000, NumCPU×20)` and a queue of
`clamp(WorkerCount×10, 10000, 100000)`; `DefaultConfig` installs those. `MinWorkerCount` is the
resident floor an adaptive pool retires down to; zero lets an idle pool retire every worker.

```go
config.SetPoolSizing(4096, 40960)             // pin the ceiling and the queue
config.MinWorkerCount = 16                    // keep a floor for bursts
config.SetTaskPoolMode(taskpool.ModeAdaptive) // preferred: also moves the sizing to the mode's
```

Pin the sizing only when a load test on your machine says the default ceiling is the wrong shape;
`SetTaskPoolMode` alone is enough to pick a different policy. When the engine runs, `TaskPool`
values can be resized in place with `Resize(min, max)`, which moves the floor and the ceiling
without dropping the connection in flight.

The complete program below is a tuned engine: pool floor, buffers, backpressure, pollers and
logging, then a graceful stop on SIGINT.

```go
package main

import (
	"context"
	"os/signal"
	"syscall"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/taskpool"
)

func main() {
	config := fib.DefaultConfig() // the defaults are already sized for the machine
	config.Name = "gateway"       // labels the pools and the logs

	config.MinWorkerCount = 16                    // resident floor between bursts
	config.SetPoolSizing(4096, 40960)             // ceiling and queue, pinned
	config.SetTaskPoolMode(taskpool.ModeAdaptive) // sizing pinned above is kept

	config.ReadBufferSize = 16 << 10          // bytes one read may take
	config.ReadBufferStartSize = 4 << 10      // small requests keep the small buffer
	config.WriteBufferHighWatermark = 1 << 20 // well above one round of reads
	config.MaxPendingBytes = 1 << 30          // the whole server's queued output
	config.MaxWriteQueueBytes = 4 << 20       // one peer that stops reading
	config.MaxConns = 100000

	config.IOPollers = true
	config.IOPollerCount = 0 // let the machine size the pollers
	config.LogSettings = false

	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(200, "text/plain; charset=utf-8", []byte("ok\n"))
	})
	engine, err := fib.New(config, fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(app),
	})
	if err != nil {
		panic(err)
	}
	go func() { _ = engine.Run() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	engine.Stop()
	_ = engine.Close()
}
```

## Custom and shared pools

`Config.TaskPool` takes any pool implementing the engine's `TaskPool` interface
(`GoTask(taskpool.Task) bool` and `GoTasks([]taskpool.Task) int`); `*taskpool.TaskPool` is one.
The engine never stops a pool supplied this way, so the caller owns its lifetime. For async work
of your own, always use a pool separate from the engine's: an engine worker submits to your pool,
and if it were the same pool a full queue could leave every worker stuck submitting.

```go
pool := taskpool.NewAdaptive(taskpool.AdaptiveConfig{
	Name: "jobs", MinWorkers: 16, MaxWorkers: 4096, QueueSize: 10000,
})
defer pool.Stop()          // the engine does not stop it
config.SetTaskPool(pool)   // the engine's connections now run on it by "jobs"
pool.Resize(8, 2048)       // move the floor and ceiling while it runs
```

`SharedTaskPool` (on by default) makes engines of one `Name` share one worker pool and one handler
pool, so two engines listening on two ports do not double the goroutines. Sharing looks at the
name only: **the first engine's pool settings win**, and later same-name engines' settings are
ignored. Give engines different names, or set `SharedTaskPool = false`, for isolation.

The program below supplies its own pool, prints what it runs with, and resizes it while serving:

```go
package main

import (
	"fmt"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/taskpool"
)

func main() {
	pool := taskpool.NewAdaptive(taskpool.AdaptiveConfig{
		Name:       "gateway-workers",
		MinWorkers: 8,
		MaxWorkers: 1024,
		QueueSize:  8192,
	})
	defer pool.Stop() // the engine does not stop a pool it was given

	config := fib.DefaultConfig()
	config.SetTaskPool(pool) // connections run on the caller's pool now
	config.LogSettings = false

	engine, err := fib.New(config, fib.Binding{
		Addr: "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
			_ = c.Respond(200, "text/plain", []byte("ok\n"))
		})),
	})
	if err != nil {
		panic(err)
	}
	go func() { _ = engine.Run() }()
	time.Sleep(time.Second) // let the pool take some traffic

	fmt.Println("mode:", pool.Mode(), "workers:", pool.Workers())
	pool.Resize(16, 512) // move the floor and the ceiling while it runs
	fmt.Println("after Resize:", pool.Workers())

	engine.Stop()
	_ = engine.Close()
}
```

`pool.Mode()` reports the mode the pool was created with, `pool.Workers()` the current population,
and `pool.Name()` the name it logs under. Panics are recovered by the pool unless you install a
`SetPanicHandler`; `taskpool.SetLogStatus(true)` makes new pools log what they resolved to.

## Buffers and backpressure

The read side and the write side are sized independently. `ReadBufferSize` is the most one read of
a connection's socket takes; `ReadBufferStartSize` lets a connection serving small requests keep a
smaller buffer while one carrying a large transfer reads at the full size from the start of each
round. On the write side `WriteBufferHighWatermark` pauses a connection's reads while that many
bytes wait for its socket, so TCP backpressure catches up (a peer that never reads cannot grow the
queue past `MaxWriteQueueBytes`, which closes it with `ErrSendQueueFull`). `MaxPendingBytes` is the
server-wide total of queued output; it is off by default.

- The watermark must exceed the replies of **one round of reads**, not one message: a round reads at
  most `ReadBufferSize` bytes and its replies sit in the queue together until the round's cork
  flushes. `DefaultConfig` sizes it at four read buffers, bounded by 64KiB and 256KiB.
- Set `MaxPendingBytes` when the watermark times the connection count is more memory than the
  deployment can spare; it pauses every connection while the budget is exhausted.
- Set `MaxWriteQueueBytes` (or `Connection.SetWriteQueueLimit`) for broadcasts to peers that may
  stop reading. It is a hard cap, not a pause.
- `DefaultConfig` has `MaxConns` off; a public listener usually wants a number, so that a flood
  costs a bounded number of descriptors.

### The buffer pool

The same pool serves your own buffers: `bufferpool.Get`/`Put` allocate nothing, sizes are classed
by powers of two (Go's allocator size classes, so no rounding waste), and a reserve that survives
GC — unlike a `sync.Pool`'s — is learned and capped by `SetRetainedBytes` (32MiB by default;
`SetRetainedBytes(0)` gives it back at once, for memory pressure). fib's read buffers, reply
assembly and TLS records flow through the same classes, so your buffers join the same circulation
instead of holding an extra idle copy.

```go
buf := bufferpool.Get(64 << 10)    // len 64KiB; contents are the previous user's, write before reading
buf = bufferpool.Append(buf, data) // grows from the pool, allocating only what is needed
defer bufferpool.Put(buf)          // after Put, not even a sub-slice may be used

joined := bufferpool.Join(nil, a, b) // takes enough at once and joins the parts
```

Use it for a handler's scratch buffer, not for a buffer still referenced by a `SendShared` or a
response body: `Put` returns it to a class where the next `Get` may hand it out again.

See [configuration.md](../configuration.md#buffers-and-backpressure) for the full field table.

## Pollers, GOMAXPROCS and prefork

`IOPollers` splits an engine across event loops (Linux epoll, macOS/BSD kqueue). A poller only
waits for events and hands runnable connections to workers, so pollers cost Ps and are usually
slower unless accept is the bottleneck; the default count follows the machine (one on up to 4
CPUs, two on up to 16, CPUs/8 beyond that). `EngineLoop` gives the engine an accept loop of its own
instead of letting pollers listen. On Windows there is one loop whatever the setting.

```go
config.IOPollers = true
config.IOPollerCount = 0  // 0: machine-sized; > 0: exactly this many loops
config.EngineLoop = false // let the pollers accept, which accepts in parallel
```

`GOMAXPROCS` stays at its default, the cores. Raising it is slower and costs 25–40% more memory in
the measurements in the [hack guide](../hacks.md#17-process-level-tricks-prefork-iopollers-gomaxprocs);
`runtime.NumCPU` already respects a CPU affinity mask or a cgroup quota.

`prefork` (Linux) is the process-level answer to the same problem: on many cores one heap's GC and
lock contention dominate, so a master starts several children, each with its own heap, collector
and a couple of Ps, and the children share the listening port through `SO_REUSEPORT`.

```go
package main

import (
	"context"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/prefork"
)

func main() {
	// Code before Run runs in the master and in every child; work only the
	// children need (loading data, opening pools) belongs in serve.
	if err := prefork.Run(prefork.Config{ProcsPerChild: 2}, serve); err != nil {
		panic(err)
	}
}

func serve(ctx context.Context) error {
	config := fib.DefaultConfig()
	config.Name = "preforked" // engines of one name in a child share their pools
	config.LogSettings = false

	engine, err := fib.New(config, fib.Binding{
		Addr: "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
			_ = c.Respond(200, "text/plain; charset=utf-8", []byte("ok\n"))
		})),
	})
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done() // SIGINT/SIGTERM, or the master asking the child to stop
		engine.Stop()
	}()
	err = engine.Run()
	_ = engine.Close()
	return err
}
```

Children default to two Ps each and the master starts as many as the machine's GOMAXPROCS
divided by `ProcsPerChild`, rounded up, so together they have the Ps one process would have had;
`prefork.Children()` reports how many processes share the machine, so a resource that exists once
per machine — a database connection limit, say — is divided by it. On platforms without
`SO_REUSEPORT` support, `prefork.Run` just calls `serve` in this process, so the program stays
portable. A UDP connection's peer may rebind to another child after a NAT rebind, and in-memory
state is per child, not shared.

## Logging

Every fib package logs through `github.com/lesismal/fib/log`, so one call decides the shape of all
of it. `fiblog.Setup` takes a `Config` with `Format` (`FormatLine` the default, `FormatText`,
`FormatJSON`), `Level` (a `slog.Leveler`, so a `*slog.LevelVar` changes it while the program runs),
`Output` and `TimeFormat`.

```go
level := &slog.LevelVar{}
level.Set(slog.LevelInfo)
fiblog.Setup(fiblog.Config{
	Format: fiblog.FormatJSON,
	Level:  level,
	Output: os.Stdout,
})
```

Without a call in the program, the environment chooses: `FIB_LOG_FORMAT=line|text|json` and
`FIB_LOG_LEVEL=debug|info|warn|error` set fib's logger when the program starts, and
`FIB_LOG_SETTINGS=0` silences the startup settings list without changing the config.
`Engine.LogSettings()` prints that list at any time, `Engine.LogStatus` adds the one-line
`fib: engine started` banner, and the `logger` middleware logs every response through the same
logger.

## pprof

The `pprof` middleware serves the runtime profiles under `/debug/pprof` for `go tool pprof` and a
browser. A profile can take seconds to gather, so each one runs on the engine's handler pool rather
than on the goroutine that ran the handler, and a request whose connection goes away stops its
profile.

```go
handler := fibhttp.NewHandler(middleware.Chain(app, pprof.New()))

// go tool pprof http://localhost:8080/debug/pprof/profile?seconds=10
```

The profiles tell a lot about a process, so serve them only where the people who can reach them are
allowed to know it. The complete program below wires together logging, the pprof middleware and a
server:

```go
package main

import (
	"log/slog"
	"os"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	fiblog "github.com/lesismal/fib/log"
	"github.com/lesismal/fib/middleware"
	"github.com/lesismal/fib/middleware/pprof"
)

func main() {
	level := &slog.LevelVar{} // change it while the program runs
	level.Set(slog.LevelInfo)
	fiblog.Setup(fiblog.Config{
		Format: fiblog.FormatJSON, // FormatLine (the default), FormatText or FormatJSON
		Level:  level,
		Output: os.Stdout,
	})

	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(200, "text/plain; charset=utf-8", []byte("ok\n"))
	})
	handler := fibhttp.NewHandler(middleware.Chain(app, pprof.New(pprof.Config{
		Prefix: "/debug/pprof", // the default
	})))

	config := fib.DefaultConfig()
	config.LogSettings = true // print every effective setting when the engine starts
	config.LogStatus = true

	engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:8080", Handler: handler})
	if err != nil {
		fiblog.Error("engine failed", "error", err)
		panic(err)
	}
	defer engine.Close()
	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

## Stats and troubleshooting

`Engine.Stats()` returns the counters that explain a slowdown:

```go
st := engine.Stats()
fiblog.Info("engine stats",
	"openConns", st.OpenConns, "connsRefused", st.ConnsRefused,
	"pendingBytes", st.PendingBytes,
	"pausedByWatermark", st.ReadsPausedByWatermark,
	"pausedByBudget", st.ReadsPausedByBudget,
	"resumed", st.ReadsResumed)
```

- `ReadsPausedByWatermark` climbing means peers are not reading their replies: the watermark is
  catching them, which is correct under overload and wrong when the watermark is too small for one
  round of output.
- `ReadsPausedByBudget` moving means the server-wide `MaxPendingBytes` is exhausted by the sum of
  the connections, so a connection with almost nothing queued of its own is paused too.
- `ConnsRefused` counts connections closed on accept because `MaxConns` was reached;
  `PendingBytes` is the queued output right now.
- For the settings themselves, `Engine.Settings().Parameters()` returns the same rows the startup
  log prints, and `Engine.LogSettings()` prints them on demand. A panic in a handler or a task is
  logged by the pools under their name, with the stack.

## Load testing

Package tests drive one connection or one hand-written frame at a time; the campaign under
`benchmark/loadtest` drives servers the way a load generator does — hundreds of streams over a few
connections and hundreds of connections with a few streams, for tens of seconds, with a subset of
clients that stop reading their responses:

```text
./benchmark/loadtest/run.sh --mode smoke          # one short case per protocol, about a minute
./benchmark/loadtest/run.sh --mode full --rounds 3 # the full campaign, 30s per case
python3 benchmark/loadtest/report.py /tmp/fib-loadtest/<time>
```

It needs Go, `h2load` from nghttp2-client, `python3`, `curl`, and on Linux `taskset` and `flock`; it
is deliberately not a CI job, because it needs a quiet host and its numbers mean nothing across
machines. The campaign writes `results.tsv`, `growth.tsv`, and per-case server logs and `/stats`
snapshots, so a regression can be read from the same files the benchmark produced. The HTTP/3
driver (`h3load`), the stalled-reader client (`stall`) and the interleaved A/B script (`ab.sh`) are
documented in [loadtest.md](../loadtest.md), together with what the campaign found.

## CI conformance

The repository's CI builds and tests every module on Linux, macOS and Windows with Go 1.27.x and
the current stable:

```sh
go build ./...
go test -race -count=1 -timeout 10m -v ./...
```

On top of that it runs the conformance jobs: HTTP/1.x against `net/http` and curl, h2spec for
HTTP/2, the QUIC Interop Runner for HTTP/3, Autobahn for WebSocket, interop peers for media
(ffmpeg), WebRTC (pion), KCP (kcp-go) and BitTorrent (anacrolix, libutp, libtorrent), the fuzzers,
a coverage floor, `gofmt`/`go vet`/`staticcheck`, `go mod tidy`, a vulnerability scan and a cross
build. If a change touches a protocol, the matching job is the one to watch. The
[README's Build and test section](../../README.md#build-and-test) is the short form.

## Production checklist

1. Start from `fib.DefaultConfig()` and change fields; a zero `fib.Config{}` has no pools, buffers
   or logging settings.
2. Set `MaxConns` and a `MaxWriteQueueBytes` cap on public listeners, so a flood or a peer that
   stops reading costs a bounded amount.
3. Raise `MaxPendingBytes` off zero only with the memory it implies: watermark × connections is
   the number it replaces.
4. Size the read buffer from the largest message you actually send, and keep the write watermark
   well above one round's replies.
5. Keep `GOMAXPROCS` at its default; add `prefork` on many-core Linux instead of a bigger heap.
6. Decide whether two engines of the same name should share pools; if not, set `SharedTaskPool =
   false` or give them distinct names.
7. Set up `fiblog.Setup` (or the `FIB_LOG_FORMAT`/`FIB_LOG_LEVEL` environment) before `fib.New`,
   and serve pprof only on an internal listener.
8. Watch `Engine.Stats()` and the load campaign: `ReadsPausedByWatermark`, `ConnsRefused` and
   `PendingBytes` tell backpressure apart from saturation.
9. Stop with `Shutdown(ctx)` when you want in-flight requests to finish, `Stop()`/`Close()` when you
   do not, and close clients before engines.
10. Run the smoke campaign and the protocol's conformance job before a release, not just `go test`.

## Pitfalls

- **Do not raise `GOMAXPROCS` for throughput.** It is measured slower and uses more memory; the
  event loop already waits in Go's netpoller.
- **Do not give the engine its own worker pool** (the same one your async handlers submit to): a
  full queue can leave every worker stuck submitting and hang the event loop.
- **`ModeInline` is not an engine mode.** The engine rejects it; it exists for standalone use.
- **The first engine of a name wins the shared pool**, including its sizes and mode; a later
  engine's settings are silently ignored.
- **A pinned watermark or read buffer is workload-specific.** Copying numbers from a benchmark run
  on another machine is how a tuned server becomes slower than the default one.
- **`prefork` shares only the port.** In-memory state, rate limiters and connection counts are per
  child; divide machine-wide limits by `prefork.Children()`.
- **Profiles and startup logs are an information leak.** Keep `/debug/pprof` and `LogSettings`
  behind the same access rules as your admin endpoints.

## See also

- [Configuration](../configuration.md) — every `Config` field with its default and sizing rule
- [Load testing](../loadtest.md) — the harness, the campaign and the measured results
- [Engine and configuration](02-engine.md) — `Config`, `Binding`, shutdown semantics
- [The README's Taskpool section](../../README.md#taskpool) — the pool modes and the benchmark tables
- [The README's Benchmarks section](../../README.md#benchmarks) — protocol benchmarks and how they
  were run
- [Hacks](21-hacks.md) — the write-side and read-side controls this chapter sizes

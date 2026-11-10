# Engine and configuration

[English](02-engine.md) | [简体中文](02-engine.zh-CN.md)

An engine is the whole server: it owns the event loops, the worker pool, the buffers and every listener, and each `fib.Binding` names one address (or a group of them) with the handler that serves it. This chapter shows the shape of a fib program, the configuration fields that matter in practice, how one engine serves several protocols at once and how bindings and engines stop — gracefully or at once. Read chapter [01-getting-started.md](01-getting-started.md) first if you have not written a fib server yet; the exhaustive field reference is [Configuration](../configuration.md) and is linked, not copied, here.

## The shape of a fib program

Every program is the same five steps: start from `fib.DefaultConfig()`, build the handlers, wrap each in a `fib.Binding`, hand the bindings to `fib.New`, and serve with `Run` until `Close`:

```go
cfg := fib.DefaultConfig() // machine-sized defaults; a zero fib.Config{} is not the same

engine, err := fib.New(cfg,
	fib.Binding{Addr: "127.0.0.1:8080", Handler: fibhttp.NewHandler(router)},
	fib.Binding{Network: "unix", Addr: sockPath, Handler: adminHandler},
)
if err != nil {
	panic(err) // one address could not be taken: none of the bindings is kept
}
defer engine.Close()

if err := engine.Run(); err != nil { // blocks until Stop, Shutdown or Close ends it
	panic(err)
}
```

`New` copies the config, so changing `cfg` afterwards changes nothing, and it opens every binding as a whole: either every address listens or, with the error that stopped one, none does. `Run` returns after `Stop` (or the stop that ends `Shutdown`); `Close` is the caller's to call once it has, and closes every connection still open. `New` with no bindings makes a client-only engine, whose connections are the ones it dials.

`DefaultConfig` is the right starting point because it reads the machine once — CPUs, memory and the descriptor limit — and sizes the worker pool, the listen backlog and the write watermark for it. Its defaults are described under [How the machine sizes the defaults](../configuration.md#how-the-machine-sizes-the-defaults); `DefaultConfigFor(readBuffer)` is the same with another read buffer and a watermark sized for it, the sizing a proxy or a bulk transfer usually wants.

## Configuration that matters

The fields fall into a few groups; only a handful change how a server behaves. Start with the defaults and set what the deployment needs:

- **Identity and pools** — `Name` labels the engine in logs and names its two pools (`<Name>-workers` for connections, `<Name>-streams` for HTTP/2 and HTTP/3 handlers); engines of one `Name` share the worker pool when `SharedTaskPool` is on, which it is by default, and always share the handler pool. `WorkerCount` is the pool's ceiling, `MinWorkerCount` the floor an adaptive pool retires down to, `SetTaskPoolMode` picks the scheduler (`taskpool.ModeAdaptive` by default) and resets `WorkerCount` and `MaxEvents` to that mode's sizing, and `SetTaskPool` supplies a pool you own.
- **Listeners** — `Network` is the network of a binding that leaves its own empty, `Backlog` the listen queue depth (the kernel's `somaxconn`), `MaxConns` a cap on accepted connections across all bindings, and `ReusePort` lets another socket of the same user share the address.
- **Buffers and backpressure** — `ReadBufferSize` bounds one read (16 KiB by default, one pooled buffer per round, not per connection); `WriteBufferHighWatermark` pauses reads on a connection with that much output waiting; `MaxPendingBytes` bounds the same for the whole server; `MaxWriteQueueBytes` caps one connection's queue and closes the connection with `fib.ErrSendQueueFull` rather than letting a peer that never reads grow it.
- **Writes and batching** — `UseWritev` sends several buffers in one `writev`, `CoalesceWrites` corks a round's replies and writes them when the round ends.
- **Pollers** — `IOPollers` splits the engine across event loops, `IOPollerCount` says how many (zero sizes them from the CPUs), `EngineLoop` gives the engine a loop of its own to accept and read UDP on, and `AcceptPlacement` picks which poller serves a connection a poller accepted.

A tuned gateway config combines them:

```go
package main

import (
	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/taskpool"
)

func main() {
	cfg := fib.DefaultConfig()
	// First: switching the mode resets WorkerCount and MaxEvents to its sizing.
	cfg.SetTaskPoolMode(taskpool.ModeAdaptive)

	cfg.Name = "gateway"             // names the pools "<Name>-workers" and "<Name>-streams"
	cfg.WorkerCount = 4 << 10        // ceiling; adaptive mode retires idle workers
	cfg.MinWorkerCount = 16          // floor the pool retires down to
	cfg.MaxConns = 10_000            // accepted connections, all bindings together
	cfg.MaxPendingBytes = 1 << 30    // server-wide output budget
	cfg.MaxWriteQueueBytes = 8 << 20 // per-connection queue cap
	cfg.ReadBufferSize = 64 << 10    // one read takes at most 64 KiB
	cfg.WriteBufferHighWatermark = 256 << 10
	cfg.IOPollerCount = 2 // two loops; 0 would size them from the CPUs

	handler := fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.String(200, "ok\n")
	}))
	engine, err := fib.New(cfg, fib.Binding{Addr: "127.0.0.1:8080", Handler: handler})
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

The startup list (see [Startup log and logging](#startup-log-and-logging)) is the way to check the result: a field set here reads `source=custom`, for example `parameter=workers value=4096 source=custom`; one that says `default` was overwritten, never set, or set to the value the default already had (as `WriteBufferHighWatermark` is here).

Two cautions. A field whose zero is a sentinel (`ReadBufferSize`, `Backlog`, `IOPollerCount`, `UDPIdleTimeout`, `Name`, `Network`) means "pick the default"; for the others zero has its literal meaning, so `MaxConns`, `MaxPendingBytes` and `MaxWriteQueueBytes` at zero are off. And raising `ReadBufferSize` alone does not resize the watermark that `DefaultConfig` already chose from the old one: use `DefaultConfigFor` or set `WriteBufferHighWatermark` yourself.

## Several protocols on one engine

Bindings are cheap because they share everything else: the loops, descriptor table, task pool and buffer pool. One engine can serve HTTP, HTTPS, HTTP/3, WebSocket, gRPC, raw TCP and a Unix socket at the same time, each on its own addresses, each with its own handler stack:

```go
package main

import (
	"fmt"
	"os"
	"path/filepath"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
	"github.com/lesismal/fib/websocket"
)

func main() {
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}

	router := fibhttp.NewRouter()
	router.Get("/", func(c *fibhttp.Context) {
		_ = c.String(200, "hello over %s\n", c.Request.Proto)
	})

	ws := websocket.NewHandler(websocket.HandlerFuncs{
		Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
			_ = c.WriteMessage(op, data) // echo the message back
		},
	})

	sock := filepath.Join(os.TempDir(), "fib-tutorial.sock")
	_ = os.Remove(sock) // a Unix listener wants a path that does not exist yet

	engine, err := fib.New(fib.DefaultConfig(),
		// HTTP/1.x and h2c.
		fib.Binding{Addr: "127.0.0.1:8080", Handler: fibhttp.NewHandler(router)},
		// HTTPS and HTTP/2 over TLS.
		fib.Binding{Addr: "127.0.0.1:8443", Handler: fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), fibhttp.NewHandler(router))},
		// HTTP/3, on UDP; the port number may repeat, the transport differs.
		fib.Binding{Network: "udp", Addr: "127.0.0.1:8443", Handler: http3.NewHandler(serverTLS, router)},
		// A WebSocket endpoint of its own.
		fib.Binding{Addr: "127.0.0.1:8081", Handler: ws},
		// A Unix stream socket for administration.
		fib.Binding{Network: "unix", Addr: sock, Handler: fibhttp.NewHandler(router)},
	)
	if err != nil {
		panic(err) // one address refused: none of the five is kept
	}
	defer engine.Close()

	for i := range 5 {
		addrs, err := engine.BindingAddrs(i) // where each binding really listens; a ":0" port resolves here
		if err != nil {
			panic(err)
		}
		fmt.Println(i, addrs)
	}
	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

Each binding answers on its own address:

```sh
curl http://127.0.0.1:8080/                           # hello over HTTP/1.1
curl --http2-prior-knowledge http://127.0.0.1:8080/   # hello over HTTP/2.0
curl -k https://127.0.0.1:8443/                       # hello over HTTP/2.0
curl --unix-socket "${TMPDIR:-/tmp}/fib-tutorial.sock" http://localhost/
```

The WebSocket binding on `:8081` echoes what a WebSocket client sends, chapter [09-websocket.md](09-websocket.md) has one, and the HTTP/3 binding needs an HTTP/3 client, as in chapter 01. The socket path follows `os.TempDir()`, which is `$TMPDIR` on macOS and `/tmp` on Linux.

`Binding` also has `Addrs` (a whole set of addresses sharing `Network`; `Addr` is then ignored), `MultipathTCP` (a listener property, so it stays on the binding when everything else is the engine's) and `Filter`, `func(netip.AddrPort) bool`, asked quickly on the event loop about each new peer — a refused TCP peer is closed and a refused UDP peer's datagram dropped, with no `Connection` and no `MaxConns` slot used.

## Bindings at runtime

Bindings can come and go while the engine serves. `AddBinding` listens on one more and returns its index; `RemoveBinding` stops its listeners at once and leaves the connections they took open; `ShutdownBinding` drains it the way `Shutdown` drains the whole engine — its connections' `ShuttingDown` reports true, a `ShutdownHandler` is told through `OnShutdown`, and what is left is closed when its context is done:

```go
canary := fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
	_ = c.String(200, "canary\n")
}))
i, err := engine.AddBinding(fib.Binding{Addr: "127.0.0.1:8082", Handler: canary})
if err != nil {
	panic(err)
}

ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
if err := engine.ShutdownBinding(ctx, i); err != nil {
	panic(err) // ctx.Err() if the drain timed out; the binding is gone either way
}
```

Indices come from `New` and `AddBinding` in order and are never reused: an index stays the binding's after it is removed, and names none.

## Graceful shutdown

`Shutdown(ctx)` closes the listeners immediately, so a client connecting after it starts is refused, and waits for the connections the engine accepted to finish, or until `ctx` is done. It returns `ctx.Err()` if the context ended the wait and `nil` otherwise, then stops the engine, which makes `Run` return; connections still open then stay open until `Close`. Connections the engine dialed are not waited for; a UDP listener's known peers are. `Stop` is the abrupt form: it closes the listeners and makes `Run` return at once, leaving every connection to `Close`. A server that drains on `SIGINT` or `SIGTERM`:

```go
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr: "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
			_ = c.String(200, "hello\n")
		})),
	})
	if err != nil {
		panic(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		// Listeners stop at once; open connections get up to 10s to finish.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := engine.Shutdown(ctx); err != nil {
			_, _ = os.Stderr.WriteString("shutdown: " + err.Error() + "\n")
		}
	}()

	if err := engine.Run(); err != nil {
		panic(err)
	}
	_ = engine.Close() // Run returned: release whatever Shutdown left open
}
```

The engine cannot tell an idle connection from one in the middle of an exchange; the protocol can. A handler that implements `fib.ShutdownHandler` (its `OnShutdown(*fib.Connection)` beside the four `Handler` callbacks) is told when the drain begins and again, at a falling rate, while the connection stays open — that is how the HTTP server closes idle keep-alives at once, busy ones behind their response, and sends HTTP/2 GOAWAY. Application code can ask the same question inside a normal handler with `c.Conn.ShuttingDown()` to fail a readiness probe while the engine drains.

## Several engines, one process

A process may run several engines, and it usually should when they have nothing to share: an engine per protocol costs one more loop, pool and buffer pool, but isolates them. Engines of the same `Name` with `SharedTaskPool` on (the default) run on one worker pool: the first engine to start builds it from its settings and the others use it as it is. Engines of the same `Name` also share one handler pool, `<Name>-streams`, whatever `SharedTaskPool` says; its ceiling follows the widest worker pool of that name. Set `SharedTaskPool` false to give an engine a worker pool of its own, and give it another `Name` to separate the handler pool too. Engines that leave `Name` at its default, `fib`, share both.

## Startup log and logging

`DefaultConfig` sets `LogSettings`, so an engine prints one line per effective setting when it starts, with its value, whether it is a default or the caller's, and what it does. `Engine.LogSettings()` prints the same list on demand, `Engine.Settings()` returns the values, and `FIB_LOG_SETTINGS=0` silences the startup list without changing the config. `LogStatus` is the separate one-line "engine started" banner.

Every fib package logs through `github.com/lesismal/fib/log`; until it is set up, lines go to `slog.Default()`. To choose fib's own format:

```go
import (
	"log/slog"
	"os"

	fiblog "github.com/lesismal/fib/log"
)

fiblog.Setup(fiblog.Config{
	Format: fiblog.FormatJSON, // FormatLine (the default), FormatText or FormatJSON
	Level:  slog.LevelWarn,    // or a *slog.LevelVar to change it while the server runs
	Output: os.Stderr,         // the default
})
```

`FIB_LOG_FORMAT=json` and `FIB_LOG_LEVEL=warn` choose the same before the program configures anything.

## Pitfalls

- **`fib.Config{}` is not `fib.DefaultConfig()`.** Booleans stay false, and the pool, buffer and logging sizes are not filled in; always start from `DefaultConfig`.
- **`New` copies the config.** Configuring `cfg` after `New` returns has no effect on the running engine.
- **One bad binding fails them all.** If any address of any binding cannot be taken, `New` keeps none; make sure a Unix socket path does not exist and a port is free before blaming the handler.
- **`Run` blocks; `Close` comes after it.** `Close` must be called once `Run` has returned; `Shutdown` and `Stop` are the ones to call while it runs.
- **`MaxConns` counts only accepted connections.** Dialed connections and UDP peers are not counted, so a client engine and a UDP binding must bound themselves.
- **A shared pool is the first engine's.** Two engines of the same `Name` with `SharedTaskPool` do not each get their configured sizing; rename or disable sharing. Engines left at the default `Name` all share one pool.
- **`SetTaskPoolMode` resets the pool sizing.** Call it before setting `WorkerCount` and `MaxEvents`, or use `SetPoolSizing`, which pins them in either order.
- **A raised `ReadBufferSize` needs a matching watermark.** `DefaultConfig` sized the watermark for 16 KiB; `DefaultConfigFor` resizes both.
- **Runtime binding indices are not reused.** Keep the index `AddBinding` returned; after `RemoveBinding` it names nothing.
- **`RemoveBinding` does not close TCP connections.** It stops listening only; use `ShutdownBinding` to drain them.

## See also

- [Configuration](../configuration.md) — every `Config` and `Binding` field, defaults, the machine sizing, logging
- [Getting started](01-getting-started.md) — the first server and the same five steps
- [Handlers: sync and async](03-handlers.md) — what the handlers these bindings name actually do
- [Architecture](../architecture.html) — loops, workers and one round of a connection
- [Performance and operations](22-performance.md) — pool sizing, `prefork`, `bufferpool`, load testing
- [The README](../../README.md) — features and the quick start this chapter expands

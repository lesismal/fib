# Getting started

[English](01-getting-started.md) | [简体中文](01-getting-started.zh-CN.md)

This chapter is the shortest path from nothing to a running fib server: install it, write the
short program every fib application is built from, and see the same handler answer
HTTP/1.x, HTTP/2, HTTP/3 and a `net/http` mux. It also maps the rest of the tutorial, so a
reader can jump straight to the protocol they need.

## What fib is

fib is an event-driven networking library for Go. A few event loops wait for I/O readiness, a
shared worker pool runs every read, callback and write, and a connection costs no goroutine
while it is idle. On top of that base it serves TCP, UDP and Unix sockets, TLS, HTTP/1.x,
HTTP/2, HTTP/3, WebSocket, arpc, gRPC, SOCKS, KCP, WebRTC, streaming media and BitTorrent —
each protocol delivered as a `fib.Handler` that an engine runs.

Three properties shape every program in this tutorial:

- **One engine, many bindings.** An `Engine` owns the loops, pools and buffers; each `Binding`
  is one address (or a group of them) with the handler that serves it. HTTP on `:80`, HTTPS on
  `:443`, HTTP/3 on UDP `:443`, gRPC on `:50051` and a Unix socket can all live in one engine.
- **`net/http`'s programming model.** An HTTP handler receives `*fibhttp.Context`, which holds
  the standard `*http.Request` and implements `http.ResponseWriter`. A `net/http` handler,
  `http.ServeMux` or `http.FileServer` runs on fib unchanged.
- **No dependencies outside the standard library.** fib imports nothing but the standard
  library (Go 1.27.3 or later), so the tutorial can show every example as a plain
  `package main`.

## Install

Create a module and add fib:

```sh
mkdir demo && cd demo
go mod init example.com/demo
go get github.com/lesismal/fib
```

No cgo, no C toolchain and no configuration files are needed; the engine opens the sockets
itself.

## The first server

A fib program is a configuration, one or more bindings, and a handler:

```go
package main

import (
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte("hello\n"))
	})

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(app),
	})
	if err != nil {
		panic(err)
	}
	defer engine.Close()

	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

Run it with `go run .` and answer it with curl:

```sh
curl -v http://127.0.0.1:8080/
curl --http2-prior-knowledge http://127.0.0.1:8080/
```

The first request gets an HTTP/1.1 response, the second an HTTP/2 one (h2c, HTTP/2 in the
clear): the same binding and the same handler serve both, because package `http` recognises
an HTTP/1 request and an HTTP/2 preface on the same TCP connection.

Read the program from the inside out:

- `fibhttp.HandlerFunc` adapts a function to `fibhttp.Handler` (`ServeHTTP(*fibhttp.Context)`).
  The context is the request and the response: it holds `c.Request` and implements
  `http.ResponseWriter`, so `c.Respond(status, contentType, body)` is the one-call answer.
- `fibhttp.NewHandler(app)` wraps that handler in the HTTP protocol handler — the component
  that parses requests, frames responses, enforces HTTP limits and hands each request to
  `app`.
- `fib.Binding` says where to listen (`Addr`) and which handler serves the connections.
  `Binding.Network` selects `tcp` (the default), `udp`, `unix` and their variants.
- `fib.New` copies the config, opens every binding, and fails as a whole if one address cannot
  be taken. `engine.Run()` blocks until the engine stops; `engine.Close()` (deferred) tears it
  down.

## One handler, HTTP/2 and HTTP/3

TLS does not change the handler, only the stack in front of it. The `fib/tlstest` package
issues a throwaway certificate for `localhost` and `127.0.0.1`, so the example needs no files:

```go
package main

import (
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

func main() {
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8",
			[]byte("hello over "+c.Request.Proto+"\n"))
	})

	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8443",
		Handler: fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), fibhttp.NewHandler(app)),
	})
	if err != nil {
		panic(err)
	}
	defer engine.Close()

	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

```sh
curl -k --http2 https://127.0.0.1:8443/     # hello over HTTP/2.0
curl -k --http1.1 https://127.0.0.1:8443/   # hello over HTTP/1.1
```

`-k` is needed because `tlstest` makes a new self-signed certificate every time the process
starts, so no client trusts it. `fibhttp.ConfigureTLS` adds `h2` and `http/1.1` to the ALPN list; `fibtls.NewServer` runs the
TLS handshake on the connection and then hands it to the HTTP handler, so `app` still does not
know which protocol it is answering. TLS is chapter [10-tls.md](10-tls.md); real certificates
are discussed there. `tlstest` is for tests and examples only.

HTTP/3 is one more binding, on UDP, with the same handler:

```go
package main

import (
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

func main() {
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8",
			[]byte("hello over "+c.Request.Proto+"\n"))
	})

	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}

	engine, err := fib.New(fib.DefaultConfig(),
		fib.Binding{
			Addr:    "127.0.0.1:8443",
			Handler: fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), fibhttp.NewHandler(app)),
		},
		fib.Binding{
			Network: "udp",
			Addr:    "127.0.0.1:8443",
			Handler: http3.NewHandler(http3.ConfigureTLS(serverTLS), app),
		},
	)
	if err != nil {
		panic(err)
	}
	defer engine.Close()

	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

```sh
curl -k --http3 https://127.0.0.1:8443/    # needs curl built with HTTP/3
```

One `tlstest` config serves both bindings: `fibhttp.ConfigureTLS` and `http3.ConfigureTLS` each
return a copy with their own ALPN list (`h2`/`http/1.1`, and `h3` with TLS 1.3 required), and
`http3.NewHandler` applies `http3.ConfigureTLS` itself, so the explicit call only makes the
requirement visible. Many curl builds, macOS's among them, have no HTTP/3; the fib HTTP/3
client in [08-http3.md](08-http3.md) gets `hello over HTTP/3.0` from the same server.

The TCP and UDP bindings may share a port number: they are different transports. Browsers find
the HTTP/3 endpoint through the `Alt-Svc` header on an HTTP/1 or HTTP/2 response, which chapter
[08-http3.md](08-http3.md) shows how to send.

## A `net/http` handler on fib

Nothing about fib requires fib-style handlers. `*fibhttp.Context` is an `http.ResponseWriter`,
so one adapter line serves any `http.Handler`, including the standard library's mux, file
server and third-party routers:

```go
package main

import (
	"fmt"
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	mux := stdhttp.NewServeMux()
	mux.HandleFunc("GET /hello/{name}", func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		fmt.Fprintf(w, "hello %s\n", r.PathValue("name"))
	})
	mux.Handle("/files/", stdhttp.StripPrefix("/files/", stdhttp.FileServer(stdhttp.Dir("."))))

	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		mux.ServeHTTP(c, c.Request) // c is the http.ResponseWriter
	})

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(app),
	})
	if err != nil {
		panic(err)
	}
	defer engine.Close()

	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

```sh
curl http://127.0.0.1:8080/hello/fib
curl -OJ http://127.0.0.1:8080/files/go.mod   # the file server serves the working directory
```

What carries over and what differs is summarised in [the README](../../README.md); the details
are in chapter [06-http1.md](06-http1.md). The short version: streaming, flushing, files,
trailers and `http.ResponseController`'s deadlines work; `Hijack` does not, because fib
switches protocols with `Context.Upgrade` instead (chapters
[09-websocket.md](09-websocket.md) and [13-socks-connect.md](13-socks-connect.md)).

## Running, logging and stopping

- `go run .` builds and runs the program. Every engine prints its effective settings at
  startup, one line per parameter; `FIB_LOG_SETTINGS=0` silences that list, and
  `engine.LogSettings()` prints it on demand.
- `engine.Run()` blocks. A program that should stop on a signal calls `engine.Shutdown(ctx)`
  (graceful) or `engine.Stop()` (at once) from another goroutine, which makes `Run` return, and
  then `engine.Close()`; `Close` itself is only for after `Run` has returned. Chapter
  [02-engine.md](02-engine.md) shows the pattern.
- The examples in this tutorial listen on `127.0.0.1` so they do not expose anything. Use `:8080`
  for all interfaces. Port 0 asks the kernel for a free port; `engine.BindingAddrs(i)` reports
  which one it picked.
- Every complete program in this tutorial is compiled by the tutorial's own tests
  (`go test ./docs/tutorial`), so the API shown here is the API of the checked-out fib.

## How this tutorial is organised

The chapters are grouped so that a reader can stop after the part they need:

| Part | Chapters | What it covers |
| --- | --- | --- |
| Foundations | 02–05 | the engine and its configuration, the two handler models (sync and async), the router and middleware |
| HTTP, WebSocket and TLS | 06–10 | HTTP/1.x, HTTP/2, HTTP/3, WebSocket, TLS |
| RPC and proxy protocols | 11–14 | gRPC, arpc, SOCKS and HTTP CONNECT, KCP |
| More protocols and modules | 15–19 | several protocols on one port, WebRTC, streaming media, BitTorrent, raw TCP/UDP/Unix |
| Clients, hacks, operations | 20–22 | the client side, the tricks that make fib fast, performance and operations |

Chapters 02, 03 and 21 are the common ones: [02-engine.md](02-engine.md) is the configuration
reference in tutorial form, [03-handlers.md](03-handlers.md) explains both handler models and
the synchronous/asynchronous split, and [21-hacks.md](21-hacks.md) collects the techniques that
go beyond the straightforward API.

## Pitfalls

- **A handler must not block.** Handlers run on the engine's worker pool, not on a goroutine of
  their own. Waiting for a peer, a lock or slow I/O inside a handler occupies a worker; fib's
  answer is the asynchronous style of chapter [03-handlers.md](03-handlers.md), or a task pool
  of your own.
- **Returning without writing is a 200 with an empty body**, as in `net/http`. If the response
  is produced elsewhere, `Retain` it first and `Release` it when the response is done.
- **`fib.Config{}` is not `fib.DefaultConfig()`.** A zero config does not fill in the pool,
  buffer and logging sizes; always start from `DefaultConfig()` and change fields.
- **A bad binding fails the whole engine.** `fib.New` keeps none of the addresses if one of
  them cannot be listened on.
- **`Close` comes after `Run`.** `Close` is for an engine whose `Run` has returned; to stop a
  running engine, call `Shutdown` or `Stop` and close it once `Run` returns.
- **Keep the engine alive.** Closing it stops every binding; a client-only engine still has to
  outlive its connections.

## See also

- [Engine and configuration](02-engine.md) — `Config`, `Binding`, runtime bindings, shutdown
- [Handlers: sync and async](03-handlers.md) — the execution model and the async API
- [Configuration](../configuration.md) — every `Config` and `Binding` field with defaults
- [The README](../../README.md) — features, architecture and benchmarks at a glance
- [The hack guide](../hacks.md) — the techniques chapter [21-hacks.md](21-hacks.md) condenses
- [fib-examples](https://github.com/lesismal/fib-examples) — runnable programs for every
  protocol

# fib — Fast In Balance

[![CI](https://github.com/lesismal/fib/actions/workflows/ci.yml/badge.svg)](https://github.com/lesismal/fib/actions/workflows/ci.yml)

[English](README.md) | [简体中文](README.zh-CN.md)

An event-driven networking library for Go, with a matching C11 implementation. A single
edge-triggered event loop collects I/O readiness, and each connection is scheduled as one
task onto a pool of workers: events on a connection run in order, and any idle worker can
run any connection, so load balances across real work rather than fd counts.

## Features

- **Native backends**: epoll on Linux, kqueue on macOS, IOCP on Windows, and a portable fallback elsewhere
- **Connection-level scheduling**: per-connection FIFO, no worker affinity, adaptive worker pool
- **Backpressure**: a per-connection write watermark plus a server-wide pending-bytes budget
- **Zero-copy paths**: `sendfile`, batched `writev`, pooled buffers
- **Protocols**: TCP, UDP, Unix sockets, TLS, HTTP/1.x, HTTP/2 (h2 and h2c), HTTP/3 over QUIC, WebSocket
- **Asynchronous clients**: non-blocking dial, and HTTP/1.x, HTTP/2 and HTTP/3 clients
- **Middleware**: compress, cors, csrf, etag, limiter, logger, pprof, recover, requestid, responsetime

## Quick start

```sh
go get github.com/lesismal/fib
```

```go
package main

import (
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/middleware"
	"github.com/lesismal/fib/middleware/logger"
	"github.com/lesismal/fib/middleware/recover"
)

func main() {
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte("hello\n"))
	})

	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:8080"
	engine, err := fib.Bind(config, fibhttp.NewHandler(middleware.Chain(app, recover.New(), logger.New())))
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

The same `Handler` serves HTTP/1.x and HTTP/2 on one port. For HTTP/3, see `examples/http3`.

## Packages

| Package | Description |
| --- | --- |
| [`fib`](.) | Event loop, connections, TCP / UDP / Unix sockets, `SendFile`, async dial |
| [`taskpool`](taskpool) | Bounded worker pools: adaptive, cond and elastic modes |
| [`bufferpool`](bufferpool) | Aligned, size-classed buffer pool |
| [`tls`](tls) | TLS layered on a connection, transparent to the protocols above it |
| [`http`](http) | HTTP/1.x and HTTP/2 server and client |
| [`http3`](http3) | HTTP/3, QUIC and QPACK server and client |
| [`websocket`](websocket) | RFC 6455 server and client, with permessage-deflate |
| [`middleware`](middleware) | HTTP middleware chain and the common middleware |

## Examples

```sh
go run ./examples/tcp/nontls/server
go run ./examples/http/nontls/server
go run ./examples/websocket/nontls/server
go run ./examples/http3/tls/server
```

Each server has a matching `client` beside it.

## Documentation

- [Go guide](docs/guide.zh-CN.md) (Chinese): configuration, API and every sub-package
- [HTTP/1.x](docs/http1.md), [HTTP/2](docs/http2.md), [HTTP/3](docs/http3.md): what is supported, the limitations, and the conformance tests
- [Architecture](docs/architecture.html): an interactive, bilingual diagram of read scheduling, write backpressure, load balancing and connection teardown

## C implementation

The original Linux C11 library, which the Go version follows, lives in [`c/`](c). Its API is in
[`c/include/epoll_server.h`](c/include/epoll_server.h).

```sh
make                      # build
./c/echo_server 9000      # try it: printf 'hello\n' | nc 127.0.0.1 9000
make test                 # concurrent integration test
```

## Build and test

```sh
make go         # go build ./...
make go-test    # go test ./...
```

Go 1.27 or later.

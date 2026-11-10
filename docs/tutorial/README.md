# fib Tutorial

[English](README.md) | [简体中文](README.zh-CN.md)

This tutorial teaches fib by usage. The reference documents describe every field of every
configuration and every limitation of every protocol; this tutorial shows how the pieces are
put together: a server for each protocol, the handler model that all of them share, the client
side, and the tricks that make fib fast.

It is written for a Go programmer who knows the basics of `net/http` and wants to serve
HTTP/1.x, HTTP/2, HTTP/3, WebSocket, gRPC, arpc, SOCKS, KCP, WebRTC, streaming media or
BitTorrent — separately or on one engine. No fib knowledge is assumed.

## Before you start

- **Go 1.27.3 or later.** fib imports nothing outside the standard library; there is no cgo and
  nothing to install beyond the module:

  ```sh
  mkdir demo && cd demo
  go mod init example.com/demo
  go get github.com/lesismal/fib
  ```

- **A terminal and curl.** The protocol chapters show the one-line `curl` (or `ffmpeg`, or a
  browser) command that exercises each example.
- **Chapter 01 first.** It installs fib, runs the first server and shows the shape every
  program in the tutorial has. Chapters 02 and 03 are the common ground the rest builds on; the
  protocol chapters can then be read in any order.

## Conventions

- A **complete program** is a code block that starts with `package main`. Every one of them is
  extracted and built by `go test ./docs/tutorial`, in a temporary module that depends on this
  checkout, so a snippet that no longer compiles fails the repository's tests.
- Other blocks are **fragments**: the function or the call being discussed, with the imports
  and boilerplate of the surrounding program left out.
- Examples listen on `127.0.0.1` and use ports like `8080`/`8443`. TLS examples use
  `fib/tlstest`, which issues a throwaway certificate for `localhost`; production certificates
  are covered in [10-tls.md](10-tls.md).
- Each chapter ends with **Pitfalls** and **See also**. The reference documents linked there
  hold the complete field tables that the chapter deliberately does not repeat.

## Structure

Part I is the common ground, Part II to IV are the protocols and modules, and Part V covers the
client side, the low-level tricks and operations.

| Ch. | File | Protocol / module | What it covers |
| --- | --- | --- | --- |
| 01 | [Getting started](01-getting-started.md) | — | Installing fib, the first HTTP/1.x + h2c server, TLS + HTTP/2, HTTP/3, a `net/http` mux on fib, the shape of a fib program, how to read the rest. |
| 02 | [Engine and configuration](02-engine.md) | Engine, `Config`, `Binding` | Every config group in practice: pools, buffers, watermarks, pollers, limits; one engine with many bindings and protocols; adding and removing bindings at runtime; graceful shutdown; several engines in one process; the startup settings log. |
| 03 | [Handlers: sync and async](03-handlers.md) | `fib.Handler`, `fibhttp.Handler` | The two handler levels and the execution model: handlers run on workers, one round of a connection at a time; the synchronous style; `Retain`/`Release`, `OnBody`, `OnCancel`, the handler pool, `MaxConcurrentHandlers`, and the goroutine cheat sheet. |
| 04 | [Router](04-router.md) | `fibhttp.Router` | Parameters, regexps, catch-alls, groups, sub-routers and mounts; 405 and `HEAD`; zero-allocation matching; serving `net/http`'s `ServeMux` instead. |
| 05 | [Middleware](05-middleware.md) | `middleware` | `Chain`, ordering, writing your own middleware, and the built-ins: compress, cors, csrf, etag, limiter, logger, pprof, recover, requestid, responsetime. |
| 06 | [HTTP/1.x](06-http1.md) | HTTP/1.x | Request and response helpers, streaming and SSE, trailers, static files and `FileCache`, sendfile, bodies (`ReadBody`, `OnBody`, `SaveBody`, resumable uploads), full duplex, deadlines, pipelining, `net/http` compatibility and what does not carry over. |
| 07 | [HTTP/2](07-http2.md) | HTTP/2 | h2c and h2 over TLS, one handler for both HTTP/1 and HTTP/2, multiplexing and the handler pool, server push, 1xx responses, streamed bodies, limits. |
| 08 | [HTTP/3](08-http3.md) | HTTP/3, QUIC | The UDP binding, `http3.ConfigureTLS`, one handler for h1/h2/h3, Alt-Svc, early data, migration and its limits, the client. |
| 09 | [WebSocket](09-websocket.md) | WebSocket | The handler and its callbacks, upgrade over HTTP/1.1, HTTP/2 and HTTP/3, `Connection` and prepared-message broadcast, frame-by-frame receiving, permessage-deflate, the client. |
| 10 | [TLS](10-tls.md) | TLS, DTLS | `fibtls.NewServer`/`NewClient` around any handler, ALPN, client certificates, cipher and session settings, test certificates, and DTLS for WebRTC. |
| 11 | [gRPC](11-grpc.md) | gRPC | The server as a fib handler, generated code and codecs, unary and streaming calls, metadata/deadlines/interceptors, the client, and gRPC on the same port as HTTP. |
| 12 | [arpc](12-arpc.md) | arpc | Server and client, calls, notifies, async calls, streams, broadcast, middleware, reconnect, and the differences from `lesismal/arpc`. |
| 13 | [SOCKS and HTTP CONNECT](13-socks-connect.md) | SOCKS5/4, HTTP CONNECT | The SOCKS server and its options, the non-blocking dialer and proxy chaining, the HTTP CONNECT proxy, `Context.Connect` and `Tunnel`, and both proxies on one port. |
| 14 | [KCP](14-kcp.md) | KCP | `kcp.Listen` as a `net.Listener`, `kcp.Dial`, fast3 tuning, FEC and crypts compatible with kcp-go/kcptun, message sessions, and the sans-I/O protocol. |
| 15 | [Multiprotocol ports](15-multiprotocol.md) | `mux`, `multiproto` | Several protocols on one port: how the first bytes select a protocol, `mux` routes, `multiproto`, SNI/TLS routing, fallback, and when to use separate bindings instead. |
| 16 | [WebRTC](16-webrtc.md) | WebRTC | `PeerConnection` on a shared UDP port, offer/answer signaling, data channels, media forwarding for an SFU, and the TURN server and client. |
| 17 | [Streaming media](17-media.md) | RTMP, RTSP, SRT, HLS, WHIP/WHEP | The media hub, one server per protocol, pull and push relays, codecs, VOD, and the interop with ffmpeg and browsers. |
| 18 | [BitTorrent](18-bittorrent.md) | BitTorrent | The client, `.torrent` files and magnets, streaming a file while it downloads, seeding, resume, and the tracker/peer/uTP/DHT pieces used on their own. |
| 19 | [Raw TCP, UDP and Unix sockets](19-raw-io.md) | `fib.Connection`, `fib.Handler` | The raw connection API, corked writes and `SendFile`/`SpliceTo`, `HoldReads`, UDP datagrams and peers, Unix sockets, and async dialing. |
| 20 | [Clients](20-clients.md) | HTTP, WebSocket, dial | Async dial, the HTTP/1.x, HTTP/2 and HTTP/3 clients, Alt-Svc discovery, the WebSocket client, TLS and SOCKS dialing, and one engine as both server and client. |
| 21 | [Hacks](21-hacks.md) | — | The techniques from the [hack guide](../hacks.md) in tutorial form: async responses, zero-copy reads, the write side, `HoldReads`, `OnFrame`, object reuse, pools, process-level tricks, and the anti-patterns. |
| 22 | [Performance and operations](22-performance.md) | taskpool, bufferpool, prefork, log | Worker pool modes and sizing, buffer sizing, `IOPollers`, `prefork`, logging, pprof, load testing, and a production checklist. |

## How to read a chapter

Every chapter has the same skeleton, so it can be used as a reference after the first read:

1. one paragraph saying what the chapter covers;
2. the usage sections — one per thing you can do, each with code;
3. **Pitfalls**: what does not work, what silently degrades, and the mistakes that cost time;
4. **See also**: the module's reference document, adjacent chapters and the relevant README
   section.

For the protocols, a chapter usually starts from a server, then a variant (TLS, streaming, the
client), and ends with the limits. If you only need one protocol, read chapter 01, chapter 03
for the handler model, then the protocol chapter.

## Verifying the tutorial

The tutorial is tested with the rest of the repository:

```sh
go test ./docs/tutorial
```

- `TestLinks` checks every relative link and heading anchor in these pages.
- `TestCodeExamples` builds every complete program in a temporary module that depends on this
  checkout.

Both run in CI, so a stale example or a renamed heading is a failed build, not a stale page.

## Related documentation

- [The README](../../README.md) — what fib is, the features, the architecture and the benchmarks.
- [Configuration](../configuration.md) — every `Config` and `Binding` field, the defaults, the
  machine sizing formula and the startup settings log.
- [Architecture](../architecture.html) and [protocol flows](../flows.html) — the event loops,
  the worker pool, and where each protocol runs.
- [The hack guide](../hacks.md) — the long form of chapter [21-hacks.md](21-hacks.md).
- [HTTP/1.x](../http1.md), [HTTP/2](../http2.md), [HTTP/3](../http3.md) — what each protocol
  supports, the conformance tests and the limitations.
- [WebRTC](../webrtc.md), [streaming media](../media.md), [KCP](../kcp.md),
  [BitTorrent](../bittorrent.md) — the deep references for those modules.
- [Load testing](../loadtest.md) and [roadmap](../roadmap.md) — the benchmark campaign, and
  what is planned but deliberately not done.
- [fib-examples](https://github.com/lesismal/fib-examples) — the runnable program set the
  chapters are distilled from, one directory per protocol.

## Adding a chapter

Chapters are numbered files, one English and one Chinese, in this directory:
`NN-slug.md` and `NN-slug.zh-CN.md`. Both start with the same language switch line, carry the
same code blocks, and link their references in their own language. A new chapter also needs a
row in both README tables. Run `go test ./docs/tutorial` before sending the change; a complete
program that does not build fails the test.

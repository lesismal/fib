# fib — Fast In Balance

[![CI](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Flesismal%2Ffib%2Fbadges%2Fci.json)](https://github.com/lesismal/fib/actions/workflows/ci.yml) [![Coverage](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Flesismal%2Ffib%2Fbadges%2Fcoverage.json)](https://github.com/lesismal/fib/actions/workflows/ci.yml)

[English](README.md) | [简体中文](README.zh-CN.md)

fib is an event-driven networking library for Go. It covers TCP, UDP and Unix sockets, TLS,
HTTP/1.x, HTTP/2, HTTP/3 over QUIC, WebSocket and WebRTC, and live streaming media over RTMP, RTSP,
SRT, HTTP-FLV, HLS and WHIP/WHEP. A few event loops wait for I/O readiness, and a shared worker pool
does the work, so a connection costs no goroutine while it is idle.
HTTP handlers take the standard `*http.Request` and answer through an `http.ResponseWriter`,
which means a `net/http` handler, `http.ServeMux` or `http.FileServer` runs on fib unchanged.

The result is net/http's programming model at event-loop speed and memory. In the benchmarks
below (GitHub Actions), fib against the most-used Go servers was:

- 2.3× net/http's HTTP/1 echo throughput, and 6.9× its pipelined throughput, on a tenth of the memory
- 3.7× net/http's HTTP/2 echo throughput, and 13× its multiplexed throughput
- 3.4–5.4× quic-go's HTTP/3 throughput
- 2.9× crypto/tls over `net` in TLS 1.3 pipelining, on a third of the memory
- the highest WebSocket echo throughput of the 17 servers tested, on 32 MB

fib is also level with or close to the C, C++ and Rust servers in the same benchmarks:
uSockets, workflow, axum, h2, quiche and rustls.

- [Features](#features)
- [Compared with the Go standard library](#compared-with-the-go-standard-library)
- [Documentation](#documentation)

## Features

### Protocols

| Protocol | Server | Client | Notes |
| --- | --- | --- | --- |
| TCP / Unix sockets | ✓ | ✓ (async dial) | `SendFile` (sendfile(2)), `SpliceTo` (splice(2) between connections, Linux), `writev` batching, corking, half close |
| UDP | ✓ | ✓ | batched `recvmmsg`/`sendmmsg` on Linux, one connection per peer |
| TLS 1.0–1.3 | ✓ | ✓ | crypto/tls handshakes; after the handshake, records are sealed and opened by fib itself (AES-GCM, AES-CBC) |
| HTTP/1.x | ✓ | ✓ | pipelining, chunked encoding and trailers, streaming bodies, `sendfile` for files |
| HTTP/2 | ✓ h2 and h2c | ✓ | h2c by prior knowledge or `Upgrade: h2c`, server push, h2spec in CI |
| HTTP/3 + QUIC | ✓ | ✓ | QUIC and QPACK written from scratch on `crypto/tls.QUICConn`, with no quic-go or x/net dependency |
| WebSocket | ✓ | ✓ | RFC 6455 and permessage-deflate, over HTTP/1.1, HTTP/2 (RFC 8441) and HTTP/3 (RFC 9220); Autobahn in CI |
| arpc | ✓ | ✓ | [lesismal/arpc](https://github.com/lesismal/arpc)'s wire format and API: two-way calls, notify, async calls, streams, broadcast, middleware, reconnect |
| HTTP CONNECT proxy | ✓ HTTP/1.x, HTTP/2, HTTP/3 | — | `Context.Connect` tunnels, paced both ways; `ConnectProxy` is a ready proxy for curl `-x`, browsers and `HTTPS_PROXY` |
| SOCKS5 / SOCKS4 / 4a | ✓ | ✓ (async dial) | CONNECT, BIND and UDP ASSOCIATE, username/password; the HTTP, WebSocket and TLS clients dial through it |
| KCP | ✓ | ✓ | the ARQ protocol of skywind3000/kcp, wire compatible with it and with xtaci/kcp-go (so kcptun): kcp-go's block-cipher, XOR and AES-GCM crypts and its Reed-Solomon FEC, without depending on it; sessions are `net.Conn`s or message callbacks, flushed when something is due rather than every interval ([details](docs/kcp.md)) |
| gRPC | ✓ h2 and h2c | ✓ | unary and streaming both ways, metadata, deadlines, interceptors, gzip; works with protoc-gen-go-grpc code and talks to grpc-go |
| WebRTC | ✓ | ✓ | PeerConnections sharing one UDP port: ICE (lite and full, trickle), DTLS 1.2-SRTP, data channels, RTP forwarding for SFUs, and a TURN client and server over UDP, TCP and TLS (quotas, events and stats, hot reload, drain), all written from scratch; talks to browsers and pion ([details](docs/webrtc.md)) |
| Streaming media | ✓ | ✓ | RTMP/RTMPS (Enhanced RTMP), RTSP over TCP and UDP, SRT, HTTP-FLV and WebSocket-FLV, HLS with fMP4 or TS segments and LL-HLS, WHIP and WHEP, joined by a hub that remuxes between them without transcoding; pull and push relays for each; checked against ffmpeg and Chromium ([details](docs/media.md)) |
| BitTorrent | ✓ | ✓ | bencode, .torrent files and magnet links (v1, v2 and hybrid), HTTP and UDP trackers in both roles, the peer wire protocol with the Fast and extension protocols, ut_metadata, ut_pex and MSE encryption, uTP with LEDBAT, and the mainline DHT with BEP 42/44/51; uTP, the DHT and the UDP tracker client share one UDP port; a client that downloads and seeds v1, v2 and hybrid torrents, streams files as they download, and finds peers through trackers, the DHT, PEX, local discovery and web seeds; checked against libtorrent, anacrolix/torrent, anacrolix/dht, libutp and opentracker ([details](docs/bittorrent.md)) |

### Engine

- **Native backends**: edge-triggered epoll on Linux, kqueue on macOS, FreeBSD, NetBSD, OpenBSD
  and DragonFly, and IOCP on Windows. Other systems fall back to the standard `net` package.
- **Per-connection scheduling**: each connection's events run in order, one at a time, on any idle
  worker. No connection is tied to a thread, so load spreads by real work rather than by fd count.
- **Backpressure**: a per-connection write watermark pauses reads when it fills, sized from the
  read buffer and the machine (four read buffers, 64 KiB to 256 KiB; 64 KiB for the default
  16 KiB read buffer), and an optional server-wide budget for pending bytes (`MaxPendingBytes`,
  off by default) bounds the sum. `HoldReads` gives read-side flow control.
- **Pools**: an adaptive worker pool that grows and shrinks with load, and a size-classed buffer
  pool aligned to the Go allocator's size classes.
- **Many protocols, one engine**: each `Binding` passed to `fib.New` gives a group of addresses, TCP, Unix or
  UDP, a handler of its own, so one engine serves HTTP, HTTPS, HTTP/3, WebSocket, gRPC and raw TCP
  side by side on shared loops, pools and buffers. Package `mux` goes further and serves several
  protocols on one port, told apart by the first bytes each client sends.
- **Async clients**: non-blocking dial, from a chosen local address and port range if need be,
  plus HTTP/1.x, HTTP/2, HTTP/3 and WebSocket clients that report results through callbacks or
  futures.
- **Static files**: `http.FileCache` serves a directory from memory, with pre-compressed `.br`/`.gz`
  twins, and follows the disk: an inotify watch on Linux (`filewatch`), under which a request stats
  nothing; elsewhere a stat of each file once `CheckInterval` (5s by default, or every request) has
  passed, which tells a file replaced by one of the same size and time by its inode and change time.
- **Router**: a [chi](https://github.com/go-chi/chi)-style API with parameters, regexps,
  catch-alls, groups, sub-routers and mounts. Routing allocates nothing.
- **Middleware**: compress, cors, csrf, etag, limiter, logger, pprof, recover, requestid and
  responsetime.
- **Prefork**: on Linux, package `prefork` serves a program from one process per CPU, each with its
  own heap and collector, sharing the program's ports through `SO_REUSEPORT`.
- **Platforms**: Linux, macOS, the BSDs and Windows, on amd64, arm64, 386, arm, riscv64 and
  loong64. Needs Go 1.27.3 or later.

## Compared with the Go standard library

"The standard library" here means `net` + `net/http` (HTTP/2 built in) + `crypto/tls`. It has no
HTTP/3, WebSocket or gRPC; those usually come from quic-go, gorilla/websocket and grpc-go.

### What is the same

- **The HTTP programming model**: a handler gets the standard `*http.Request`, and
  `*fibhttp.Context` implements `http.ResponseWriter`, `http.Flusher` and `io.ReaderFrom`. So
  `http.ServeMux`, `http.FileServer`, `ServeContent`, and frameworks built on `http.Handler` such
  as chi and gin run on fib mostly unchanged, over HTTP/1, HTTP/2 and HTTP/3.
- **TLS handshakes by `crypto/tls`**: a `tls.Config` works as it is, and handshakes per second are
  about the same (3,445 against 3,413 for TLS 1.3).
- **Runtime and dependencies**: both are pure Go with no cgo, on the same runtime and GC. fib
  depends on nothing outside the standard library either, and its event loops park in Go's own
  netpoller.
- **Shared interfaces and features**: `fib.Connection` implements `net.Conn`; both have h1/h2/h2c,
  ALPN, graceful shutdown with a context (`Shutdown(ctx)`), and `ResponseController.Flush` and
  `SetWriteDeadline`.

### What is different

| | Standard library | fib |
|---|---|---|
| Concurrency | One goroutine per connection (and one more per HTTP/2 stream), blocking I/O | A few event loops (epoll/kqueue/IOCP) and a shared worker pool; a connection holds no goroutine, and runs on one worker at a time, in order |
| Blocking in a handler | Fine: it costs one goroutine | It holds a scarce worker. Reply asynchronously with `Retain/Release`, take the body with `OnBody`, or hand the work to a pool of your own |
| `Read` / `Write` | `Read` waits for data; `Write` blocks on a slow reader, which is backpressure for free | `Read` returns `ErrWouldBlock` when there is nothing; `Write` never blocks: data queues and the watermark pauses reads |
| Deadlines | The call fails; the connection stays | The connection is closed |
| Request body | Streamed by default, read from the network as `r.Body` is read | Read whole before the handler runs (up to `MaxBodyBytes`); `StreamRequestBody` streams it |
| Object lifetimes | Fresh objects per request, safe to keep after the handler returns | Request, Header, URL and Context are reused by default, with fasthttp's rules: touching them after the handler is done reads another request's data, with no error |
| Client disconnect | `r.Context()` is cancelled | `r.Context()` is not; use `c.Context()`, `c.OnCancel` or `c.Err()` |
| Hijack | Supported | Not supported, so libraries that hijack, such as gorilla/websocket, do not work; `EnableFullDuplex` returns `ErrNotSupported` |
| TLS records | `crypto/tls` throughout | fib encrypts and decrypts records itself after the handshake (AES-GCM, ChaCha20, CBC), and can offload them to the NIC with kTLS |
| Protocols | h1/h2; the rest from third-party modules | Its own h2, HPACK, QUIC/HTTP/3, WebSocket, gRPC transport, KCP, WebRTC, streaming media and more; one engine serves many protocols, and can tell them apart on one port by the first bytes |
| Writing | Each `Write` goes through `bufio` | A round's output is corked and leaves in one `writev`; files go out with `sendfile`, and relays between connections can `splice` |
| Clients | `http.Client` blocks | Non-blocking dial and async h1/h2/h3/WS clients reporting through callbacks or futures (calling `Wait()` in a handler is an anti-pattern) |
| Platforms | The same everywhere | Native backends on Linux, macOS, the BSDs and Windows, `net` elsewhere; prefork, `recvmmsg`, inotify and raw syscalls are Linux only |
| Tuning | Next to none | Worker pool, watermarks, `SplitEventLoops`, `EventLoopCount`, `MaxPendingBytes` and more, with defaults sized from the machine |

### Where fib is stronger

1. **Throughput**: on GitHub Actions (2 vCPUs for the server, 10,000 connections, 1 KiB
   payloads), against net/http (against `crypto/tls` + `net` for the TLS row):

   | Case | Throughput |
   |---|---|
   | HTTP/1 echo | 2.3× |
   | HTTP/1 pipelined | 6.9× |
   | HTTP/2 echo | 3.7× |
   | HTTP/2 multiplexed | 13× |
   | TLS 1.3 pipelined | 2.9× |

   HTTP/3 is 3.4–5.4× quic-go, and in the same runs fib is close to the C, C++ and Rust servers.
   The numbers and how to reproduce them are in
   [go-http1-benchmark](https://github.com/lesismal/go-http1-benchmark),
   [go-http2-benchmark](https://github.com/lesismal/go-http2-benchmark),
   [go-http3-benchmark](https://github.com/lesismal/go-http3-benchmark),
   [go-tls-benchmark](https://github.com/lesismal/go-tls-benchmark) and
   [go-websocket-benchmark](https://github.com/lesismal/go-websocket-benchmark).
2. **Memory, more so with more connections**: an idle connection holds no goroutine stack and no
   I/O buffer; buffers are borrowed for one round. In the HTTP/1 echo fib used 32 MB, net/http
   314 MB.
3. **Bounded backlog**: watermarks on both reads and writes kept fib at 32–36 MB in the pipelined
   tests, where most other servers grew to hundreds of MB.
4. **Many protocols and no third-party dependencies**: one engine serves HTTP, HTTPS, HTTP/3,
   WebSocket, gRPC, SOCKS, arpc and raw TCP on shared loops, pools and buffers.
5. **A cheap migration**: most existing `http.Handler`s plug in as they are, which takes part of the
   gain at once; hot paths can move to the `Context` API later.
6. **More control**: `HoldReads`, `Cork`, `SendFile`, `FileCache`, and prefork for a heap per
   process, for gateways, proxies, push and long-lived connections.

### Where fib is weaker, and the risks

1. **More to keep in mind**: a handler must not block casually; breaking the object-reuse lifetime
   rules mixes up requests' data silently instead of panicking; the `data` a callback gets is valid
   only inside it. Developers have to keep to these themselves; the
   [hack guide's anti-patterns](docs/hacks.md#20-anti-patterns) lists them.
2. **Not 100% compatible**: no Hijack; `r.Context()` is not cancelled on disconnect; the body is
   read whole by default; `Read` does not block, so `bufio.Reader` cannot wrap it; a deadline
   closes the connection. Standard-library code that relies on these needs changes.
3. **Defaults to watch**:
   - HTTP/1 writes do not block, so a whole response to a slow client sits in memory.
     `MaxWriteQueueBytes` and `MaxPendingBytes` are unlimited by default: stream large responses,
     or set the limits (see the slow-client section of [load testing](docs/loadtest.md)).
   - Object reuse is on by default.
4. **More code of its own**: the TLS record layer, QUIC, HTTP/2 and the rest are fib's own, which
   is more to audit and more edge cases than the standard library, which has the Go team and the
   whole community behind it.
5. **Maturity**: few maintainers, a small community, an API still changing (the constructors are
   already down to `fib.New`), Go 1.27.3 or later, and no Go 1 compatibility promise.
6. **Harder to debug**: a standard-library goroutine dump shows the line each connection waits on;
   fib is state machines and callbacks, so finding a stall or a leak takes more tools and
   experience.
7. **Less gain for heavier handlers**: the benchmarks use light handlers such as echo. Where the
   handler is heavy (databases, remote calls) the network is not the bottleneck and fib's lead
   shrinks; used wrongly (blocking on a worker), it can do worse than the standard library.

### Where the standard library is stronger

1. **Simple and intuitive**: synchronous, blocking code; block whenever you like, goroutines are
   cheap, and there are few traps for newcomers.
2. **Safe semantics**: no object reuse, contexts cancelled on disconnect, backpressure built into
   `Write`, streamed bodies.
3. **Stable and trusted**: the Go 1 compatibility promise, maintained by the Go team, prompt
   security fixes, and proven in the largest production deployments.
4. **The whole ecosystem**: every middleware, framework and hijacking library (gorilla/websocket
   and the like), and `httputil.ReverseProxy`, works as it is.
5. **Easy to debug**: pprof and goroutine dumps map straight onto connections.

### Where the standard library is weaker

1. **A goroutine and `bufio` buffers per connection, so memory grows with connections**: about
   300 MB at 10,000 connections, and far more at hundreds of thousands or millions of long-lived
   ones.
2. **Lower throughput**: no batching of writes across requests, so pipelining and HTTP/2
   multiplexing suffer most (7–13× behind); every HTTP/2 stream starts a goroutine.
3. **No global bound on backlog**: with slow clients or deep pipelines, memory grows with the
   goroutines and buffers.
4. **No HTTP/3, WebSocket or gRPC**: they come from third-party modules, each with its own model and
   memory profile (quic-go's throughput and memory are well behind fib's).
5. **Little low-level control**: no read-side flow control, no cork, no several protocols on one
   port.

### Which to choose

- **Ordinary business services** (handlers mostly query databases and call services, connections
  are few, the team changes often): the standard library is enough: stable, simple, the whole
  ecosystem.
- **Services where the network layer is the bottleneck** (gateways, proxies, long-lived push, chat,
  real-time media, many connections or deep pipelines, tight memory, or many protocols in one
  process): fib pays off, provided the team keeps to these rules:
  - handlers do not block; heavy work goes behind `Retain` or to a pool of its own;
  - reused objects never leave the handler;
  - large responses are streamed, or the write queue is bounded.
- **A middle way**: plug the existing `http.Handler` into fib unchanged and take the connection and
  memory gains; if a handler keeps the Request after it returns, turn off `ReuseRequests` or
  `Clone` the request first. Then move hot paths to the native `Context` API one by one.

## Documentation

- [Tutorial](docs/tutorial/README.md): a usage-first walkthrough in 22 chapters, protocol by
  protocol, with compiled code examples, plus the engine configuration, the sync/async handler
  models and the hack techniques as chapters of their own
- [Configuration](docs/configuration.md): every `Config` and `Binding` field, its default, the
  machine sizing formula and the startup settings log
- [Architecture](docs/architecture.html): event loops, pollers and pools, handler layers, read
  scheduling, write backpressure, load balancing and connection teardown
- [Protocol flows](docs/flows.html): which loop and which pool runs what for TLS, HTTP/1, HTTP/2,
  HTTP/3 and WebSocket
- [Hack guide](docs/hacks.md): async responses with Retain/Release, large bodies with OnBody, pipeline/multiplex,
  HoldReads, Cork, object reuse and the pitfalls of each
- [HTTP/1.x](docs/http1.md), [HTTP/2](docs/http2.md), [HTTP/3](docs/http3.md): what is supported,
  the limitations, and the conformance tests
- [WebRTC](docs/webrtc.md): the layers, the shared Mux and the callback model, a server and a client,
  data channels, media forwarding, TURN, the limits, and the interop with pion, OpenSSL and Chromium
- [Streaming media](docs/media.md): RTMP, RTSP, SRT, HTTP-FLV, HLS and WHIP/WHEP on one hub, the
  codecs each carries, a server for every protocol, relays, the limits, and the interop with ffmpeg and Chromium
- [KCP](docs/kcp.md): the layers, flushing when something is due, the mapping from kcp-go and kcptun,
  the server's session rules, and the interop with kcp-go
- [BitTorrent](docs/bittorrent.md): the packages, one port for TCP and UDP, each protocol, the client,
  the BEPs covered, and the interop with libtorrent, anacrolix/torrent, anacrolix/dht, libutp and opentracker
- [Roadmap](docs/roadmap.md): what is planned but not done, and what is deliberately not done, with the reason, the rough cost and when to reconsider each
- [Dev plan](docs/dev-plan.md): for every hot-path feature, whether it has been benchmarked and
  optimized, and what is left to measure
- [Load testing](docs/loadtest.md): the `benchmark/loadtest` campaign, its results and the
  memory cost of whole responses to slow readers

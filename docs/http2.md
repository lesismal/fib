# HTTP/2: Limitations, Intentional Omissions and Planned Improvements

[English](http2.md) | [简体中文](http2.zh-CN.md)

This document records the boundaries of the HTTP/2 implementation in the Go
`http` package: how it currently behaves in ways users need to know about,
what was deliberately left out, and where it can be improved. For usage, see
the [HTTP/2 section of the Go guide](guide.zh-CN.md#http2) (Chinese).

The implementation lives in [`http`](../http) (`h2_*.go`) and an
internal hpack package. It is written from scratch and does not depend on
`golang.org/x/net`.

## What is supported (overview)

| Side | Features |
| --- | --- |
| Server | TLS + ALPN (h2), cleartext with prior knowledge (h2c), HTTP/1.1 `Upgrade: h2c`; multiplexing, flow control in both directions, HPACK (Huffman and dynamic table), CONTINUATION, trailers, server push, 1xx interim responses, automatic 100 Continue, graceful GOAWAY on `Response.Close`, `Request.TLS` |
| Client | h2 over https through ALPN, cleartext prior knowledge with `UnencryptedHTTP2`; multiplexing on one connection, honouring the server's `MAX_CONCURRENT_STREAMS`, cancellation that resets only its own stream, automatic retry after GOAWAY / REFUSED_STREAM |
| Conformance | h2spec: 145 of 145 cases, over h2c and TLS |
| Interop | Go `net/http` client and server (TLS and h2c); curl (nghttp2) over h2, h2c and Upgrade |

The suite that checks all of this runs in CI; see
[Conformance testing](#conformance-testing) below.

## Current limitations

Behaviour to be aware of when using it.

### Request bodies stream on request; responses are sent whole

- A request body is read completely into memory before the handler runs,
  unless `Config.StreamRequestBody` is set: then the handler runs as soon as
  the body has begun — at the HEADERS for a body with a `content-length` past
  `StreamRequestBodyThreshold`, and once more than the threshold of it has
  arrived for one without — and takes the body from `Request.Body`, a
  `*BodyStream` read without waiting, or through `Context.OnBody`, exactly as
  on HTTP/1 (see [`http1.md`](http1.md#streaming-request-bodies)). The stream's
  receive window is then given back only as the handler consumes the body, so
  a client uploading faster than the handler takes it is paced by HTTP/2 flow
  control, one window (1MB) at most ahead, while the connection's other
  streams carry on. `MaxStreamedBodyBytes` bounds such a body in place of
  `MaxBodyBytes`, and `Expect: 100-continue` is answered only once the handler
  asks for the body.
- A response is given all at once as `Response.Body []byte`. The client
  buffers the whole response body before its callback.
- A handler may write its response through `Context`'s `http.ResponseWriter`
  methods (`Header`/`WriteHeader`/`Write`/`Flush`), trailers included, and
  hand `Context` to `http.ServeFile` or `http.ServeContent`. On HTTP/1 that
  streams, chunked, with files sent by sendfile (see [`http1.md`](http1.md));
  on HTTP/2 the response is held until the handler returns and then sent
  whole, and `Flush` does nothing.
- Server-sent events, streamed long-polling output and gRPC streaming are
  therefore not possible over HTTP/2; large uploads can be processed as they
  arrive with `StreamRequestBody`.
- Memory bound: on the server, one connection can hold up to about
  `MaxConcurrentStreams × MaxBodyBytes` (250 × 16MB by default) of bodies read
  whole, and a window (1MB) per streamed body; on the client,
  one response is bounded by `MaxResponseBodyBytes`.
- A `CONNECT` request is parsed and handed to the handler, but no tunnel can be
  established (there is no bidirectional streaming channel).

### Handlers run on a pool of their own

- A connection's frames are processed in order, and a request that is complete
  goes to the handler pool that `Config.StreamPool` describes, so the requests
  one client has open on a connection are served concurrently and a handler
  that blocks holds up only itself. There is one such pool for each engine
  `Config.Name`, called `<Name>-streams` (`fib-streams` by default), shared by
  every HTTP/2 and HTTP/3 server whose connections come from engines of that
  name and never by an engine: an engine worker submits each request it
  frames, and were the queue it submits to its own, every worker could end up
  waiting for room that none is left to make. Its ceiling is twice the widest
  engine pool of that name running, or `fib.DefaultStreamPoolSizing` while
  none is, and its floor is zero, so an idle one keeps no worker. It stops once the
  last engine of its name closes, without waiting for handlers still
  running.
- `StreamPool.MaxConcurrentHandlers` bounds how many of one connection's
  requests are served at once. The last of the N runs on the goroutine reading
  the connection, which reads nothing further until it returns, so the limit is
  paid for by the peer's flow control rather than by a queue of requests on the
  server. 1, like `StreamPool.Disable`, serves every request on the reader, one
  at a time, which is how the server behaved before the pool existed
  (application-level head-of-line blocking).
- `Push` runs the handler of the pushed request synchronously, on the goroutine
  that called it, and returns only when it does, so a slow pushed handler
  delays the parent response.

### No direct writes to the connection

- Do not call `Context.Conn.Send` or similar on an HTTP/2 connection: raw bytes
  break the framing. All output must go through `Context`.

### Fixed parameters

These are constants today and cannot be configured:

| Parameter | Value |
| --- | --- |
| Receive window per stream | 1MB, replenished after half is consumed |
| Receive window per connection | 16MB, replenished after half is consumed |
| Advertised `SETTINGS_MAX_FRAME_SIZE` | 16384 |
| HPACK dynamic table | 4096 bytes (the encoder never uses more than 4096) |
| Concurrent streams the client assumes before the server's SETTINGS arrive | 100 |

### Protocol details

- **Priority**: PRIORITY frames and the priority fields in HEADERS are validated
  and ignored. When several streams have data waiting, the order they are sent
  in follows the iteration order of an internal map, with no fairness or
  weighting.
- **The peer's `SETTINGS_MAX_HEADER_LIST_SIZE`**: this side advertises its own
  limit but does not check the peer's when sending.
- **TCP RST on graceful close**: after GOAWAY the connection closes once every
  stream has finished, but it does not half-close and drain what the peer is
  still sending first; if unread data is left in the socket, the kernel sends a
  RST.
- **Engine shutdown**: `Engine.Stop`/`Close` sends no GOAWAY to HTTP/2
  connections; they are closed outright, and clients see a broken connection
  rather than a graceful close.
- **Idle and keep-alive**: the server has no idle timeout for HTTP/2
  connections and sends no PING keep-alives. The client only has
  `IdleConnTimeout` (closing when idle) and does no PING health checks, so a
  connection that dies silently is not noticed promptly.
- **SETTINGS acknowledgement timeout**: nothing checks that the peer
  acknowledges this side's SETTINGS in reasonable time (SETTINGS_TIMEOUT).

### Client

- Until the first connection to an https host has revealed its protocol, only
  one connection to it is dialed at a time (as in `net/http`), so the first
  burst of requests to an HTTP/1.1-only https server waits one extra TLS
  handshake.
- Among several HTTP/2 connections, the first one with room is chosen rather
  than the least loaded one.
- A request whose connection breaks after part of its response has arrived
  fails and is not retried. Only requests that received nothing and use an
  idempotent method, and requests GOAWAY or REFUSED_STREAM marks as
  unprocessed, are retried.
- A request with `Expect: 100-continue` does not wait for the 100; the body is
  sent along with the header (which the protocol allows).
- Cleartext HTTP/2 is prior knowledge only; the client does not upgrade from
  HTTP/1.1 with `Upgrade: h2c`.
- A request with `Expect: 100-continue` does not wait for the 100 (see above),
  so a server that would refuse the body still receives it.

## Intentionally not implemented

| Feature | Reason |
| --- | --- |
| Receiving server push in the client | Chrome and Firefox have removed push, and Go's `net/http` client never supported it. The client sends `ENABLE_PUSH=0`; the server's push support remains for clients that still want it. For preloading, 103 Early Hints (`Context.WriteInterim`) is the recommended replacement. |
| RFC 9218 extensible priorities (`priority` header, PRIORITY_UPDATE) | With bodies buffered whole, scheduling has little to gain. The RFC 7540 priority tree is deprecated by RFC 9113 and is not implemented either. |
| Extended CONNECT (RFC 8441, WebSocket over HTTP/2) | Needs bidirectional streaming streams, which need streaming bodies first; WebSocket uses the HTTP/1.1 Upgrade. |
| `Upgrade: h2c` initiated by the client | RFC 9113 deprecates this upgrade; cleartext HTTP/2 uses prior knowledge (`UnencryptedHTTP2`). The server still accepts the upgrade for compatibility with curl and others. |
| `Upgrade: h2c` over TLS | The RFCs allow switching protocols over TLS only through ALPN. |
| 1xx interim responses to HTTP/1.0 requests | HTTP/1.0 clients do not understand 1xx; `WriteInterim` returns `http.ErrNotSupported`. |
| Pushing from a pushed request | The protocol only allows PUSH_PROMISE on client-initiated streams. |

## Conformance testing

`http/http2_conformance_test.go` holds the suite, every test named
`TestHTTP2Conformance…`, and CI runs it on Linux, macOS and Windows in the
`HTTP/2 conformance` job. The peers are the standard library and tools the
runners already have or install as tools, so the module itself gains no
dependency:

| Peer | What it checks |
| --- | --- |
| [h2spec](https://github.com/summerwind/h2spec) v2.2.1, installed with `go install` as staticcheck is | The server against RFC 9113 and RFC 7541 case by case: framing, stream states, flow control, HPACK and error codes. 145 of 145 cases pass over h2c and over TLS |
| `net/http`'s HTTP/2 client and server | Methods, bodies past the flow-control windows, HEAD, bodiless statuses, trailers both ways, 1xx, 100-continue, multiplexing 50 requests on one connection |
| curl (nghttp2) | The three ways a client starts HTTP/2: prior knowledge, `Upgrade: h2c`, and ALPN over TLS |
| A raw frame server in the test | What the client does that no ordinary server would show: its preface and settings, REFUSED_STREAM and GOAWAY retries, RST_STREAM, flow control against a small window (the raw server rejects a single byte past what it granted), the server's concurrency limit, CONTINUATION, PING, trailers, and a server that pushes although push is disabled |
| A raw frame client in the test | Stream states, server push, h2c upgrade, graceful GOAWAY, and the HTTP/2-only mode |

h2spec is run against a server with `Config.HTTP2Only` set, since it speaks
nothing but HTTP/2: a sniffing server would answer its deliberately invalid
preface with an HTTP/1 response, which h2spec cannot read.

## Planned improvements

In order of priority.

### 1. Hardening against abuse (high)

Resources are bounded today only by `MaxConcurrentStreams`, `MaxHeaderBytes`,
`MaxBodyBytes` and the flow-control windows. Against a malicious client it
still lacks:

- **Rapid Reset (CVE-2023-44487)**: a client can open streams and immediately
  RST_STREAM them without end. Resets per unit of time should be counted, with
  GOAWAY(ENHANCE_YOUR_CALM) past a threshold.
- **Control-frame floods**: PING, SETTINGS, empty DATA and WINDOW_UPDATE frames
  are not rate-limited, and PING/SETTINGS acknowledgements keep entering the
  send queue. The number of queued control frames should be capped.
- **CONTINUATION floods**: a single header block is bounded by
  `MaxHeaderBytes`, but the number of CONTINUATION frames, and of zero-length
  frames, should be bounded too.
- **Slow connections**: with no header-read timeout or idle timeout (see
  above), many half-open connections can tie up resources.

### 2. Conformance testing

Done: see [Conformance testing](#conformance-testing). Parsing HTTP/1
requests, reading HTTP/2 frames and coding HPACK all have fuzz targets
(`http/fuzz_test.go` and `internal/hpack/fuzz_test.go`), which the
`Fuzz the parsers` CI job runs for 20 seconds each. What is still missing is a
load-oriented check (h2load, for example) to catch what only shows up under
concurrency.

### 3. Streaming bodies and the handler model (medium)

- Make the `http.ResponseWriter` methods `Context` already has stream on
  HTTP/2 as they do on HTTP/1, for SSE, gRPC and large downloads. Streamed
  request bodies are done, with window updates that follow the handler's
  consumption; a body read whole still has its window replenished on
  receipt.

### 4. Performance (medium)

- **HPACK Huffman decoding** walks the decoding tree bit by bit; table-driven
  decoding 4 or 8 bits at a time would be faster.
- **HPACK encoder** searches the dynamic table linearly; an index would help
  (the static table is already a map).
- **Allocations**: every request allocates a header map, frame buffers, an
  `stdhttp.Request` and more. Frame and header-decoding buffers could be
  recycled with `sync.Pool`, as the HTTP/1 path does.
- **Batching sends**: control frames produced in one pass (WINDOW_UPDATE,
  SETTINGS ACK, PING ACK) are sent one by one and could be coalesced into one
  write.
- **Benchmarks**: add HTTP/2 benchmarks (multiplexed throughput on one
  connection, HPACK encode/decode) and let pprof guide optimisation.

### 5. Configurability and scheduling (low)

- Make the receive windows, maximum frame size, HPACK table size and the
  client's initial stream allowance configurable; optionally size windows from
  the bandwidth-delay product.
- Schedule streams with pending data round-robin or by weight, for fairness.
- Honour the peer's `SETTINGS_MAX_HEADER_LIST_SIZE` when sending.
- Server: configurable idle timeout and PING keep-alive; GOAWAY to HTTP/2
  connections when the `Engine` stops, waiting for in-flight streams (graceful
  shutdown); half-close and drain input after GOAWAY to avoid RST.
- Client: optional PING health checks, least-loaded connection selection, and
  a SETTINGS acknowledgement timeout.
- Support sending request trailers.

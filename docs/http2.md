# HTTP/2: Limitations, Intentional Omissions and Planned Improvements

[English](http2.md) | [简体中文](http2.zh-CN.md)

This document records the boundaries of the HTTP/2 implementation in the Go
`http` package: how it currently behaves in ways users need to know about,
what was deliberately left out, and where it can be improved. For usage, see
the [HTTP/2 section of the Go guide](guide.zh-CN.md#http2) (Chinese).

The implementation lives in [`http`](../http) (`h2_*.go`) and the
[`hpack`](../hpack) package. It is written from scratch and does not depend on
`golang.org/x/net`.

## What is supported (overview)

| Side | Features |
| --- | --- |
| Server | TLS + ALPN (h2), cleartext with prior knowledge (h2c), HTTP/1.1 `Upgrade: h2c`; multiplexing, flow control in both directions, HPACK (Huffman and dynamic table), CONTINUATION, trailers, server push, 1xx interim responses, automatic 100 Continue, graceful GOAWAY on `Response.Close`, `Request.TLS`, extended CONNECT (RFC 8441) switched to a tunnel with `Context.Upgrade`, which is how `websocket.ServerHandler.Upgrade` serves WebSocket over HTTP/2 |
| Client | h2 over https through ALPN, cleartext prior knowledge with `UnencryptedHTTP2`; multiplexing on one connection, honouring the server's `MAX_CONCURRENT_STREAMS`, cancellation that resets only its own stream, automatic retry after GOAWAY / REFUSED_STREAM; new streams on the least-loaded connection; `Expect: 100-continue` waits for the 100; optional PING health check |
| Conformance | h2spec: 145 of 145 cases, over h2c and TLS |
| Interop | Go `net/http` client and server (TLS and h2c); curl (nghttp2) over h2, h2c and Upgrade |

The suite that checks all of this runs in CI; see
[Conformance testing](#conformance-testing) below.

### TLS 0-RTT

A client that resumes a TLS 1.3 session can send its first request frames with
its first flight, before the handshake is over (RFC 8446 section 2.3). Setting
`Config.EarlyData` turns that on for the HTTP/2 server (and for the
`tls.Handler` it is wrapped in, through `tls.EarlyDataOptIn`): a stream whose
HEADERS arrived that way carries `Early-Data: 1` (RFC 8470), and only a GET,
HEAD, OPTIONS or TRACE method is served from it. Any other method is answered
425 Too Early on that stream alone, its connection and the other streams
carrying on, as HTTP/3 does; `Config.EarlyDataUnsafe` serves every method and
leaves the check to the handler. The TLS layer accepts a ticket's early data
once and bounds it to `tls.Handler.MaxEarlyData`, so a replay is turned away
before it reaches the server. The client does not send HTTP/2 early data: the
framing depends on the protocol ALPN picks, which it learns only when the
handshake is over.

## Current limitations

Behaviour to be aware of when using it.

### Request bodies stream on request; responses stream as they are written

- A request body is read completely into memory before the handler runs,
  unless `Config.StreamRequestBody` is set: then the handler runs as soon as
  the body has begun — at the HEADERS for a body with a `content-length` past
  `StreamRequestBodyThreshold`, and once more than the threshold of it has
  arrived for one without — and takes the body from `Request.Body`, a
  `*BodyStream` read without waiting, or through `Context.OnBody`, exactly as
  on HTTP/1 (see [`http1.md`](http1.md#streaming-request-bodies)). The stream's
  receive window is then given back only as the handler consumes the body, so
  a client uploading faster than the handler takes it is paced by HTTP/2 flow
  control, one window (`HTTP2Config.StreamWindow`, 1MB by default) at most
  ahead, while the connection's other streams carry on.
  `MaxStreamedBodyBytes` bounds such a body in place of `MaxBodyBytes`, and
  `Expect: 100-continue` is answered only once the handler asks for the body.
- A response given to `WriteResponse` or `Respond` is given all at once as
  `Response.Body []byte`. The client buffers the whole response body before
  its callback.
- A handler may instead write its response through `Context`'s
  `http.ResponseWriter` methods (`Header`/`WriteHeader`/`Write`/`Flush`),
  trailers included, and hand `Context` to `http.ServeFile` or
  `http.ServeContent`. A response no longer than 4KB (or with a
  `Content-Length` of at most 64KB) that is not flushed is still sent whole
  when the handler returns, with its `content-length`. A longer or flushed one
  streams, as on HTTP/1: HEADERS that leave the stream open, DATA frames as it
  is written and as the client's windows let them go, and END_STREAM or the
  trailers' HEADERS once the handler is done. So server-sent events, long
  polling and large downloads work over HTTP/2.
- A handler that writes faster than the client reads waits in `Write` once
  64KB of its body is held back by flow control, until the client has taken
  some or the stream ends, so that memory stays bounded. It waits on the
  handler pool, which grows for blocked handlers; a handler the connection
  runs on its own reader (see `StreamPool` below) never waits, since that
  goroutine reads the WINDOW_UPDATE that would end the wait, and its body is
  held until the client's windows let it go. A body shorter than the
  `Content-Length` the handler set resets the stream rather than ending it.
- Package [`grpc`](../grpc) serves gRPC, streaming included, through this
  (`Server.ServeHTTP`), besides its own HTTP/2 transport.
- Memory bound: on the server, one connection can hold up to about
  `MaxConcurrentStreams × MaxBodyBytes` (250 × 16MB by default) of bodies read
  whole, and a stream window per streamed body; on the client, one response is
  bounded by `MaxResponseBodyBytes`.
- A `CONNECT` request is parsed and handed to the handler, but no tunnel can be
  established (there is no bidirectional streaming channel).

### Handlers run on a pool

- A connection's frames are processed in order, and a request that is complete
  goes to the engine's handler pool (`fib.Engine.HandlerPool`), as
  `Config.StreamPool` describes, so the requests one client has open on a
  connection are served concurrently and a handler that blocks holds up only
  itself.
- The engine's workers read and frame the connections, with `IOPollers` or
  without, and the handler pool is one of its own for each engine `Config.Name`, called
  `<Name>-streams` (`fib-streams` by default), shared by every HTTP/2 and
  HTTP/3 server whose connections come from engines of that name and never
  by an engine: an engine worker submits each request it frames, and were the
  queue it submits to its own, every worker could end up waiting for room
  that none is left to make. Its ceiling is twice the widest
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
- Responses are queued and written by a flush the first of them sends to the
  handler pool, behind the requests already handed to it, so a burst of
  responses leaves in one write rather than one each, and none waits for a
  slow handler's. Over TLS the burst is also sealed as one record, up to
  16 KB, rather than a record a frame: the TLS layer holds what is sent while
  the connection is corked and seals it when the flush uncorks it
  (`fib.CorkHolder`). That took a fixed response over h2 on 7 CPUs from
  1.64M to 3.52M requests a second, against 3.82M over h2c.
- A stream, with the `*http.Request`, `Header`, `URL` and `*Context` it holds,
  is recycled once its response is finished and its handler has returned and
  released every `Retain`, under `Config.ReuseRequests` and its siblings, which
  are on by default, as an HTTP/1 request's objects are: a handler must not keep
  them past that (see the HTTP/1 notes). Streams whose body streams and
  extended CONNECT tunnels are not recycled: their body callbacks can outlive
  the response, which a recycled stream cannot carry. A pushed stream is
  recycled too, its `Request`, `URL` and `Context` with it. On 64 CPUs these
  took HttpArena's baseline-h2c from 3.9M requests a second to 18.5M.

### No direct writes to the connection

- Do not call `Context.Conn.Send` or similar on an HTTP/2 connection: raw bytes
  break the framing. All output must go through `Context`.

### Configurable parameters

`Config.HTTP2` sets what a server's connections let their clients send, and
`ClientConfig.HTTP2` the same for a client's connections. Both are
`http.HTTP2Config`, whose zero value is the defaults in the table below.

| Field | Default | What it sets |
| --- | --- | --- |
| `StreamWindow` | 1MB | `SETTINGS_INITIAL_WINDOW_SIZE`: the receive window each stream is granted, given back after half is consumed. A streamed body is bounded by it; raising it lets one stream move more per round trip. Kept between 65535 and 2^31-1. |
| `ConnWindow` | 16MB | The connection's receive window, shared by its streams, given back after half is consumed. Kept between 65535 and 2^31-1. |
| `MaxFrameSize` | 16384 | The advertised `SETTINGS_MAX_FRAME_SIZE`: the largest frame payload the peer may send. A larger one lets a body arrive in fewer frames, for as much more memory per read. Kept between 16384 and 2^24-1. What this side sends is still framed to what the peer allows. |
| `HeaderTableSize` | 4096 | The HPACK dynamic table each direction may use: the size this side advertises in `SETTINGS_HEADER_TABLE_SIZE` and the most its encoder uses when the peer allows that much. Negative turns the table off. The encoder caps it at `hpack.MaxEncoderTableSize` (1MB). |
| `AdaptiveWindow` | off | Grow the windows to the connection's bandwidth-delay product, as gRPC does, by measuring DATA between a PING and its acknowledgement and doubling a window that the peer fills within a round trip. The PINGs are at least 25ms apart, the windows never shrink, and the stream window is bounded by `MaxAdaptiveWindow`. |
| `MaxAdaptiveWindow` | 16MB | The bound on the stream window `AdaptiveWindow` grows. |
| `InitialMaxStreams` | 100 | How many streams a client opens on a new connection before the server's `SETTINGS_MAX_CONCURRENT_STREAMS` arrives. Ignored by a server. |

```go
config := fibhttp.DefaultConfig()
config.HTTP2 = fibhttp.HTTP2Config{
    StreamWindow:    4 << 20,  // a long, high-latency download moves 4MB per round trip
    ConnWindow:      32 << 20,
    MaxFrameSize:    32 << 10, // fewer frames per body, more memory per read
    HeaderTableSize: 16 << 10, // more of a repeated header kept per connection
    AdaptiveWindow:  true,     // or let the connection measure its own path
}
server, err := fib.New(fibConfig, fib.Binding{Addr: ":443", Handler: fibhttp.NewHandlerWithConfig(config, handler)})
```

A client sets the same fields in `fibhttp.ClientConfig.HTTP2`, which describe
what its server may send to it. The windows are the flow control that a
streamed body is bounded by, the frame size what a peer may send at once, and
the table what either side's HPACK encoder may keep between header blocks.

### Protocol details

- **Priority**: PRIORITY frames and the priority fields in HEADERS are validated
  and ignored. Streams whose bytes the connection's window holds back take
  turns a frame at a time, in the order they began to wait, so a large
  download does not starve the other responses on its connection; there is no
  weighting, and a stream with nothing ahead of it sends at once.
- **The peer's `SETTINGS_MAX_HEADER_LIST_SIZE`**: both sides keep within the
  limit the peer announces. A response header or trailer that would not fit
  resets its stream rather than going out, an interim response or a pushed
  request's header that would not fit is left out, and the client fails a
  request whose header is too large before it opens a stream, and resets one
  whose streamed body's trailer turns out to be.
- **Engine shutdown**: `Engine.Shutdown` sends GOAWAY and closes each
  connection once its streams have finished. `Engine.Stop` on its own leaves
  connections open, and `Engine.Close` writes GOAWAY on each HTTP/2
  connection before it cuts it, in one non-blocking write: the peer learns
  that the server is going where the socket can take the frame, while a
  connection already backed up with output may still be cut with it queued.
  `Engine.Shutdown` remains the graceful path.
- **TCP RST on close**: a connection the server retires itself (idle, a
  response's `Close`, a shutdown, a peer's GOAWAY or a connection error)
  half-closes after its GOAWAY and drains what the client is still sending,
  for `lingerTimeout` (500ms) at most, so the client reads the GOAWAY rather
  than a reset. A connection `Engine.Close` cuts does not linger, and one
  with unread input may still be reset.
- **Idle and keep-alive**: the server retires a connection that has carried
  no stream for `Config.IdleTimeout` (`ReadTimeout` when it is not set; 2
  minutes in `DefaultConfig`) with GOAWAY `NO_ERROR`. With
  `Config.PingInterval` it also pings a connection that has sent nothing for
  an interval, and closes one that answers nothing within
  `Config.PingTimeout`; the default sends no PING. The client's PING health
  check is described under [Client](#client).
- **SETTINGS acknowledgement timeout**: the server ends a connection with
  GOAWAY(SETTINGS_TIMEOUT) when the client has not acknowledged its SETTINGS
  within `Config.SettingsTimeout` (10 seconds by default). The client does not
  time a server out the same way.

### Client

- Until the first connection to an https host has revealed its protocol, only
  one connection to it is dialed at a time (as in `net/http`), so the first
  burst of requests to an HTTP/1.1-only https server waits one extra TLS
  handshake.
- Among several HTTP/2 connections to a host, a new stream goes to the one
  with the most streams to spare, by each connection's own
  `MAX_CONCURRENT_STREAMS`, so the load spreads over them rather than piling
  onto the first; a tie goes to the earlier one. A second connection is only
  dialed once every one is full, and once there are several they all stay in
  use while requests keep them busy, closing once the host goes quiet for
  `IdleConnTimeout`.
- **Health check**: with `ClientConfig.ReadIdleTimeout` set, a connection that
  has heard nothing from the server for that long sends it a PING, and one
  that then hears nothing at all, acknowledgement or other frame, for
  `PingTimeout` (15 seconds by default) is taken for dead and closed. Its
  requests fail with `ErrPingTimeout`, or go again on another connection by
  the rule below. Names and defaults are x/net/http2's; `ReadIdleTimeout`'s
  zero, the default, leaves the check off.
- **Retries**: a request whose connection breaks after any of its response
  has arrived fails with the connection's error and is not sent again,
  whatever its method. This is deliberate, and what `net/http` and
  x/net/http2 do: the server has begun to answer, so it has acted on the
  request, and repeating a POST would act twice; even for an idempotent
  method, the callback of a streaming response may already have passed part
  of the body on, which a second response cannot be stitched to, and the
  second response may differ. A request that received nothing goes again,
  once, on another connection when its method is idempotent (GET, HEAD,
  OPTIONS, TRACE) and its body can be read again (`GetBody`, for one that
  streams); one that GOAWAY or REFUSED_STREAM marks unprocessed goes again
  whatever its method, since the server says it did nothing with it. A caller
  that knows a request is safe to repeat after a partial answer can send it
  again itself.
- **`Expect: 100-continue`**: a request with a body and this header sends its
  HEADERS alone and its DATA once the server answers 100, or once
  `ClientConfig.ExpectContinueTimeout` (1 second in `DefaultClientConfig`;
  zero sends the body at once) has passed. A final response that comes first
  is the answer: the body is never sent, and the stream is reset once the
  response is complete. HTTP/1 and HTTP/3 requests do the same.
- Cleartext HTTP/2 is prior knowledge only; the client does not upgrade from
  HTTP/1.1 with `Upgrade: h2c`.

## Intentionally not implemented

| Feature | Reason |
| --- | --- |
| Receiving server push in the client | Chrome and Firefox have removed push, and Go's `net/http` client never supported it. The client sends `ENABLE_PUSH=0`; the server's push support remains for clients that still want it. For preloading, 103 Early Hints (`Context.WriteInterim`) is the recommended replacement. |
| RFC 9218 extensible priorities (`priority` header, PRIORITY_UPDATE) | With bodies buffered whole, scheduling has little to gain. The RFC 7540 priority tree is deprecated by RFC 9113 and is not implemented either. |
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

## Benchmarks

`go test ./http -run '^$' -bench '^BenchmarkH2Server' -benchmem -count 5`
drives one connection with 100 requests in flight, each answered as it ends
with a new one, as h2load does. One op is one request; client and server
share the process, so compare runs with each other rather than with a load
generator's numbers. The host is a shared 16-thread Ryzen 7 7800X3D, whose
load moved ns/op by a few percent between runs; the allocation counts are
exact, and only the two changes below moved time outside that noise. The
`hpack` package has its own encode and decode benchmarks.

| Benchmark | ns/op | B/op | allocs/op |
| --- | --- | --- | --- |
| GET | 2230 | 10 | 0 |
| POST | 3136 | 145 | 2 |
| Static 1KB | 2480 | 9 | 0 |
| Static 16KB | 5630 | 28 | 0 |
| Static 64KB | 15503 | 126 | 0 |
| Static TLS 1KB | 3026 | 12 | 0 |
| Static TLS 16KB | 9191 | 53 | 1 |
| Static TLS 64KB | 22612 | 131 | 1 |
| Streamed POST | 5976 | 6425 | 13 |
| Push | 5032 | 233 | 4 |

Pushed-stream recycling took `Push` from 8 allocations and 1954 bytes a
request to 4 and 233, and its time from 5364 to 5032 ns/op (medians of four
interleaved runs against main). The one-frame send path
took POST from 3452 to 3226 ns/op and static 16KB from 5833 to 5519 in an
A/B of the branch with and without it (medians of three interleaved pairs);
the 64KB bodies, which take several frames, did not move outside noise, and
neither did TLS, where the layer seals what it is handed into one record
anyway. A profiler run over these shows the send path waiting on the socket
(syscalls, and AES-GCM over TLS) far more than it copies: memmove was 2.5% of
the samples, which is why the copy removed below is worth a few percent.
The client and the test harness account for most of the remaining
allocations (the task pool's ring, timers, and the benchmark's own reads).

## Planned improvements

In order of priority.

### 1. Hardening against abuse

Done. Besides `MaxConcurrentStreams`, `MaxHeaderBytes`, `MaxBodyBytes` and the
flow-control windows, the server ends a connection with
GOAWAY(ENHANCE_YOUR_CALM) when its client:

- resets streams while they are open, or makes the server reset them with
  stream errors, past a burst of 1000 refilled at 100 a second (Rapid Reset,
  CVE-2023-44487), which `Config.StreamResetBurst` and `StreamResetRate` set;
- sends PING, SETTINGS (after the first), or DATA and CONTINUATION frames that
  are empty without ending anything, past a budget of the same size, which
  bounds the acknowledgements the server queues too, and which
  `Config.ControlFrameBurst` and `ControlFrameRate` set;
- sends one header block in more than 1000 CONTINUATION frames
  (`Config.MaxContinuationFrames`), or takes longer than `ReadHeaderTimeout`
  (`ReadTimeout` when it is not set) to finish one.

PRIORITY frames, and WINDOW_UPDATE or RST_STREAM frames for a stream that has
ended, cost no more than their bytes and are not counted, since a client may
send them in flight. Each limit left at zero takes the default above
(`DefaultStreamResetBurst` and the like), and one set below zero is lifted. A
gRPC client cancels a call by resetting its stream, and deadlines or hedged
calls cancel many at once, so a server or proxy in front of busy gRPC clients
may need a higher reset rate than the default.

The client holds a server to the same control-frame budget and CONTINUATION
bound, with the defaults: a server that floods it with PING or SETTINGS
frames, each of which the client acknowledges, or with empty DATA or
CONTINUATION frames, has its connection ended with GOAWAY(ENHANCE_YOUR_CALM),
and the requests on it fail with that `*H2ConnError`. A server resetting the
client's streams costs the client nothing, so it has no reset budget.

### 2. Conformance testing

Done: see [Conformance testing](#conformance-testing). Parsing HTTP/1
requests, reading HTTP/2 frames and coding HPACK all have fuzz targets
(`http/fuzz_test.go` and `hpack/fuzz_test.go`), which the
`Fuzz the parsers` CI job runs for 20 seconds each. What is still missing is a
load-oriented check (h2load, for example) to catch what only shows up under
concurrency.

### 3. Streaming bodies and the handler model (medium)

- Streamed request bodies are done, with window updates that follow the
  handler's consumption, and so are streamed responses, paced by the
  client's windows; a body read whole still has its window replenished on
  receipt. The client streams a response body to its callback under
  `ClientConfig.StreamResponseBody`, and buffers it whole otherwise.

### 4. Performance (medium)

- **HPACK Huffman decoding** is done: it takes four bits a step from a table.
- **HPACK encoder dynamic-table index** is done: past 16 entries it finds a
  field through a hash index of names and name-value pairs instead of
  searching the table, and the table grows to what `HeaderTableSize` and the
  peer allow.
- **Benchmarks** are done: `http/h2_benchmark_test.go` drives one connection
  with 100 requests in flight, as h2load does, over h2c and TLS, for GET,
  POST, static bodies of 1KB, 16KB and 64KB, streamed uploads and server push;
  `hpack` has encode and decode benchmarks. See
  [Benchmarks](#benchmarks) above.
- **Pushed streams** are done: a pushed stream and the `Request`, `URL` and
  `Context` it holds come from the same recycling as a client's stream's. A
  stream whose body streams still allocates, since its callbacks can outlive
  the response.
- **Copies on the send path** are cut for a whole response whose body fits
  one frame over h2c: it is copied once rather than twice. A larger body, and
  one over TLS, is still copied into the frames and then into the connection
  or record; cutting the rest needs a vectored send the protocol layer does
  not have.

### 5. Scheduling (low)

- Schedule streams with pending data by weight; round-robin is done.
- Client: a SETTINGS acknowledgement timeout of its own. (PING health checks,
  least-loaded connection selection and waiting for 100-continue are done; see
  [Client](#client).)
- Support sending request trailers.

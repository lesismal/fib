# fib Hack Guide

[English](hacks.md) · [简体中文](hacks.zh-CN.md)

This document collects the ways fib differs from the standard library's `net.Conn` / `net/http`
and from nbio, plus how the "asynchronous, non-blocking, batch per round" habits of C/C++
servers translate to Go. It is not an introduction. It is for people who already use fib and
want to push it: what you can do, why it works, where the edges are, and what happens when you
step over them.

Keep three premises in mind. Every trick below follows from them:

1. **No goroutine per connection.** A connection is a unit of work in a `taskpool`. One round
   (read → parse → handler → write) runs serially on one worker, and rounds of the same
   connection never run concurrently.
2. **Handlers run on workers, and workers are scarce.** Waiting for a peer, a lock or slow I/O
   inside a handler occupies one of the Engine's workers, not a cheap goroutine. So almost every
   fib API is "register a callback and return", not "block until done".
3. **Writes within a round are corked.** Every `Send` in a round is queued first and flushed at
   the end of the round as one `write`/`writev`.

> Wherever the text says "note", it is a real pitfall, not politeness.

---

## Contents

1. [Async responses: Retain / Release](#1-async-responses-retain--release)
2. [Large bodies and OnBody: no buffering, no goroutine, built-in backpressure](#2-large-bodies-and-onbody-no-buffering-no-goroutine-built-in-backpressure)
3. [Cancellation: OnCancel / Err / idempotent cleanup](#3-cancellation-oncancel--err--idempotent-cleanup)
4. [Pipeline: one write per round](#4-pipeline-one-write-per-round)
5. [Multiplex (HTTP/2, HTTP/3): the handler pool and `MaxConcurrentHandlers`](#5-multiplex-http2-http3-the-handler-pool-and-maxconcurrenthandlers)
6. [Object reuse: using Context like fasthttp](#6-object-reuse-using-context-like-fasthttp)
7. [Zero-copy reads: `Context.Body` / `Context.Query`](#7-zero-copy-reads-contextbody--contextquery)
8. [Read backpressure: `HoldReads`](#8-read-backpressure-holdreads)
9. [The write side: `Cork` / `Flush` / `SendParts` / `SendFile`](#9-the-write-side-cork--flush--sendparts--sendfile)
10. [`Read` does not block, a deadline closes the connection](#10-read-does-not-block-a-deadline-closes-the-connection)
11. [WebSocket: receiving by frame (OnFrame)](#11-websocket-receiving-by-frame-onframe)
12. [One Engine as both server and client](#12-one-engine-as-both-server-and-client)
13. [One Handler for h1 / h2 / h3, and Upgrade / Tunnel](#13-one-handler-for-h1--h2--h3-and-upgrade--tunnel)
14. [FileCache and sendfile: static files without touching the disk or user space](#14-filecache-and-sendfile-static-files-without-touching-the-disk-or-user-space)
15. [UDP: `DatagramsHandler` and `SendBatch`](#15-udp-datagramshandler-and-sendbatch)
16. [Pools: your own, separate ones, and avoiding deadlock](#16-pools-your-own-separate-ones-and-avoiding-deadlock)
17. [Process-level tricks: Prefork, IOPollers, GOMAXPROCS](#17-process-level-tricks-prefork-iopollers-gomaxprocs)
18. [bufferpool: your buffers can use the same pool](#18-bufferpool-your-buffers-can-use-the-same-pool)
19. [Cheat sheet: what may be called from which goroutine](#19-cheat-sheet-what-may-be-called-from-which-goroutine)
20. [Anti-patterns](#20-anti-patterns)

---

## 1. Async responses: Retain / Release

**In one sentence:** the response is reference counted. Serving a request takes one reference,
returned when the handler returns; take one more and the response does not finish until you
`Release` it. The handler can then **return at once and give the worker back to the pool**, and
answer when the result is ready.

A handler that returns without writing anything **and without `Retain`** is answered with an empty `200`, as in
`net/http`; the unanswered-response wait only exists while some `Retain` (or `OnBody`) is outstanding.

```go
func(c *fibhttp.Context) {
    c.Retain()                       // no automatic write-back when the handler returns
    go func() {                      // or hand it to your own pool / another reactor
        result := slowQuery(c.Request.URL.Query().Get("q")) // ← note: see "don't touch r" below
        _ = c.Respond(200, "application/json", result)
        c.Release()                  // count reaches zero → finish the response, hand it to the connection
    }()
}
```

**Why this is an advantage**

- Unlike the standard library: in `net/http` the response ends when the handler returns, so
  going async means the handler itself blocks and holds a goroutine (which is fine there, since
  every connection already has one). In fib a blocked handler holds a worker, so the ability to
  *suspend a request* is a necessity.
- It is the same thing as a C/C++ epoll server: the request is pending, and when the result
  arrives you `send` and carry on. The only difference is that fib manages the lifetime with a
  reference count.
- One `Context` can be retained by several async branches. Fan out N backend calls, each
  `Release`s, and the last to come back finishes the response ("the last releaser finishes").

**Fan-out and aggregate**

Each branch takes a reference and the last one to finish writes the response. An atomic counter
decides who is last; write first, then `Release`. `Release` is the moment the response ends, so
**the response must be written before the last `Release`**:

```go
func(c *fibhttp.Context) {
    var (
        mu      sync.Mutex
        parts   [3][]byte
        pending atomic.Int32
    )
    pending.Store(3)
    for i := range parts {
        c.Retain()
        i := i
        backend[i].CallAsync(func(b []byte) { // your async client; the callback runs on any goroutine
            mu.Lock()
            parts[i] = append([]byte(nil), b...)
            mu.Unlock()
            if pending.Add(-1) == 0 { // the last one back writes the response
                _ = c.Respond(200, "application/json", merge(parts[:]))
            }
            c.Release()
        })
    }
    // The handler's own reference is released on return; each branch releases its own.
}
```

**Together with fib's own async clients**

`fibhttp.Client`, `websocket.Dialer` and `Engine.Dial` are callback based and never block the
caller, so they pair naturally with Retain/Release. For a reverse proxy or gateway, answer from
the upstream callback with `c.Respond`, then `c.Release()`:

```go
c.Retain()
upstream.Do(req, func(resp *http.Response, err error) {
    defer c.Release()
    if err != nil {
        _ = c.Respond(502, "text/plain", []byte(err.Error()))
        return
    }
    _ = c.Respond(resp.StatusCode, resp.Header.Get("Content-Type"), bodyOf(resp))
})
```

**Edges and pitfalls**

- **`Retain` and `Release` must pair.** Releasing too often ends the response early (the
  handler's own reference is also released once); releasing too rarely pins the connection. There
  is no timeout: a handler that never releases holds its connection until the peer goes away or a
  read timeout fires.
- **On HTTP/1 a retained request holds back the pipelined requests behind it** (responses must
  stay in order); they are parsed once it is released. So on HTTP/1 one slow async request makes
  the later requests of the same connection wait. On HTTP/2 and HTTP/3 streams do not affect
  each other.
- **Don't touch `r` if object reuse is on.** See [§6](#6-object-reuse-using-context-like-fasthttp):
  a retained request is yours until its last `Release`; **after** that the `*http.Request`,
  `Header`, `URL` and `Context` are recycled for the next request. So either finish with them
  before `Release`, or copy what you need first.
- On a request whose response is already written, or whose connection is gone, `Retain` /
  `Release` / `Finish` do nothing, and writing returns an error instead of corrupting the
  connection. Cleanup code can be called repeatedly without fear.
- `Retained()` tells you whether the response is still held; it is mostly for tests and
  diagnostics.
- At most 65535 `Retain`s may be held on one request at a time; more panics.

---

## 2. Large bodies and OnBody: no buffering, no goroutine, built-in backpressure

By default the handler is called once the body has fully arrived, so a 1GB upload needs 1GB of
memory. With `Config.StreamRequestBody` the handler is called as soon as the header is in, and
the body is processed as it arrives through the `OnBody` callback:

```go
httpConfig := fibhttp.DefaultConfig()
httpConfig.StreamRequestBody = true
httpConfig.StreamRequestBodyThreshold = 0      // 0: stream every body; otherwise bodies up to it are still buffered whole
httpConfig.MaxStreamedBodyBytes = 64 << 30     // upper bound for a streamed body
httpConfig.StreamRequestBodyBuffer = 1 << 20   // stop reading the socket when this much body is waiting unread
httpConfig.ReadTimeout = 0                     // see "slow clients" below

handler := fibhttp.NewHandlerWithConfig(httpConfig, fibhttp.HandlerFunc(
    func(c *fibhttp.Context) {
        f, _ := os.Create("upload.bin")
        c.OnBody(func(data []byte, fin bool, err error) {
            if err != nil {            // connection gone / over the limit / framing error: the last call
                f.Close(); os.Remove("upload.bin")
                return
            }
            f.Write(data)              // data is valid only during this call; copy it to keep it
            if fin {
                f.Close()
                _ = c.Respond(200, "text/plain", []byte("stored"))
            }
        })
        // OnBody retains the request; the framework releases it after the last call returns, and the response is written.
    },
))
```

A complete runnable example (with SHA-256 verification, a client, and a 1GiB test-file
generator) is in [`examples/http/upload`](../examples/http/upload).

**The advantages, one by one**

1. **The whole body is never buffered.** For a 128MB upload the heap peaks at about 470MB when
   buffered whole and about 4MB when streamed.
2. **No goroutine.** `OnBody` only registers the callback and never calls it itself. What has
   already arrived is delivered after the handler returns, on the goroutine that ran it; the
   rest is delivered by the worker that reads it (or by the Engine's handler pool with
   IOPollers on, and on HTTP/2 and HTTP/3). Calls come one at a time, in order. This is exactly
   the `on_body(chunk)` callback model of C.
3. **Backpressure is free.** If the callback is slow the body piles up in
   `StreamRequestBodyBuffer`; when that is full the connection calls `HoldReads(true)` and stops
   reading the socket, the kernel buffer fills, and TCP's sliding window slows the peer down. You
   write no rate-limiting code: **the callback itself is the rate limiter**. HTTP/2 and HTTP/3
   do the same through flow control (the window is replenished only after the body is consumed),
   and other requests on the same connection are unaffected.
4. **You can refuse before receiving.** `Expect: 100-continue` is lazy: `100 Continue` is sent
   the first time the handler asks for the body. So you can reject with 413/403 before the
   client has started uploading, without reading a byte:

   ```go
   if c.Request.ContentLength > limit {
       _ = c.Respond(413, "text/plain", []byte("too large"))
       return                    // OnBody is never called, so the client never starts uploading
   }
   ```
5. **Write to disk as you go, or forward as you go.** `Send` `data` straight to an upstream from
   the callback and you have a streaming proxy that never holds the body in memory.

**Mixed style: read what has arrived synchronously, fall back to OnBody**

With streaming, `Request.Body` is a `*fibhttp.BodyStream` and `Read` does not block:

| Result | Meaning |
| --- | --- |
| `n > 0` | bytes that have arrived |
| `io.EOF` | the whole body has been read |
| `fibhttp.ErrWouldBlock` | nothing right now, more is coming → take over with `OnBody` |
| `ErrBodyAbandoned` | the body was taken over by `Close`/`OnBody`, or the handler returned without retaining |
| any other error | nothing more will come (disconnect, over the limit, framing error) |

The handover loses no bytes: `OnBody` starts exactly where the last `Read` stopped. Small
requests often have header and body in the same read; read synchronously to `io.EOF` and answer
directly, saving a callback dispatch. `c.BodyComplete()` lets you ask first.

**Going async from inside the callback**

The automatic Retain/Release of `OnBody` only covers the time until the last call returns. To
answer elsewhere (say the disk write is slow and goes to your own pool), `c.Retain()` once more
in the callback and `c.Release()` when done:

```go
c.OnBody(func(data []byte, fin bool, err error) {
    ...
    if fin {
        c.Retain()
        diskPool.Go(func() { // your pool
            defer c.Release()
            _ = c.Respond(200, "text/plain", []byte(finish(f)))
        })
    }
})
```

**Note**

- **Slow uploads** are bounded by `ReadTimeout`, which counts from the request's first byte, and
  **a streamed body is still arriving while the handler runs, so it is bound by it too**. For
  large uploads set it high enough or to 0, and rely on `IdleTimeout`. The deadline is removed
  only once the request has fully arrived, so a slow *handler* is not limited by `ReadTimeout`.
- **Don't block for long in the callback.** While your callback runs the connection does not
  read; other connections are unaffected (the callback is on a worker), but it holds a worker.
  For slow work use your own pool plus `HoldReads` (see [§8](#8-read-backpressure-holdreads)).
- `Request.Body` can no longer be read inside the callback; closing the `Body` ends the callback
  with `ErrBodyAbandoned` and discards the rest of the body.
- If the handler returns without reading everything: up to 256KB of remainder is read and
  discarded and the connection stays reusable; more than that, or a chunked body, closes the
  connection after the response.
- A streamed request **never reuses** `Request`/`Header`/`URL`, whatever the reuse options say.
- The streaming style is the same on HTTP/1.x, HTTP/2 and HTTP/3; gRPC bidirectional streaming
  needs the same switch so that a call runs when its request begins to arrive.

---

## 3. Cancellation: OnCancel / Err / idempotent cleanup

The big problem with suspended requests is "the peer is gone and you are still working". How fib
handles it:

- Connection closed, read timeout, or a body that cannot be finished → the request is
  **cancelled**: nothing more is written, the remaining references are void, and you are told
  **once**.
- A handler using `OnBody` gets `err` in the callback; an async handler that only `Retain`s uses
  `Context.OnCancel(func(error))`; you can also poll `Context.Err()`.
- The notification runs on the Engine's handler pool, not on the event loop, so it cannot hold
  the server back.

```go
func(c *fibhttp.Context) {
    ctx, cancel := context.WithCancel(context.Background())
    var once sync.Once
    done := func() { once.Do(c.Release) } // exactly one Release per Retain
    c.Retain()
    c.OnCancel(func(err error) {
        cancel()   // cancel the upstream call
        done()     // still release the reference you retained
    })
    upstream.DoCtx(ctx, req, func(resp *http.Response, err error) {
        if err == nil {
            _ = c.Respond(200, "application/json", bodyOf(resp))
        }
        done()
    })
}
```

**Key points**

- After cancellation the request, its body and the Context are still yours until every `Retain`
  has been released; only then are they recycled. **Cancelled does not mean you may skip
  `Release`.**
- `Release` on a failed request writes nothing but still counts: one release too many gives up a
  reference that belongs to another holder. The cancel callback and the normal path may both run,
  so use `sync.Once` as above to keep it to one `Release` per `Retain`.
- **Don't close your own connection inside `OnData` / an HTTP/1 handler and then wait for the
  `OnCancel` notification:** `OnClose` waits for the current round to return, so this deadlocks
  (see "OnClose ordering" in the guide).

---

## 4. Pipeline: one write per round

HTTP/1 pipelining (write N requests, read N responses in order) and WebSocket sending many frames
at once are among the scenarios where fib leads `net/http` the most (HTTP/1 pipeline is about
6.9x `net/http`, with about a tenth of the memory). There is no magic flag. It is several designs
working together:

1. **Read until a short read.** Edge triggered: a round drains the socket, and one `read` often
   carries many requests.
2. **Incremental parsing, consumed in place.** The HTTP parser and the WebSocket frame parser
   work directly on this round's read buffer. What has been parsed is not moved; coalescing and
   splitting copy only the small remainder.
3. **Cork: all replies of a round are queued and written once at the end of the round.** N
   pipelined responses become one system call instead of N. `Send` tries a direct write first and
   copies any remainder into the queue; the two parts of `SendParts` and a queue of several
   buffers go through `writev`.
4. **Object reuse, zero allocation.** Request, Header, URL and Context are recycled by default;
   the Router allocates nothing per request. At tens of millions of requests per second, GC and
   heap locks are the bottleneck (see [§6](#6-object-reuse-using-context-like-fasthttp)).
5. **Bounded backlog.** When the peer does not read its responses, the write queue grows to
   `WriteBufferHighWatermark` and then fib stops reading that peer's requests
   (`MaxPendingBytes` is the server-wide total). That is why memory stays at a few tens of MB in
   pipeline tests rather than growing to hundreds.

**Getting the same benefit in your own protocol**

- In `OnData`, **parse in a loop and `Send` in a loop**; don't `Flush` per request:

  ```go
  Data: func(c *fib.Connection, data []byte) {
      for len(data) > 0 {
          msg, n, ok := parse(data)
          if !ok { break }                 // partial message: keep the remainder yourself, join it next time
          data = data[n:]
          _ = c.Send(handle(msg))          // queued; merged into one write at the end of the round
      }
  }
  ```
- The watermark must exceed the replies produced by **one round of reads**, not just one
  message: a round of reads is corked, so all the round's replies briefly sit in the queue
  together. A round reads at most `ReadBufferSize` bytes (16KiB by default), so 1KiB replies with
  an 8KiB watermark leave an 8x margin. A watermark that is too small makes pipelines pause
  reads needlessly.
- When an async branch (another goroutine) answers several requests of one connection at once,
  `c.Cork()` first and `c.Flush()` after the last reply so they merge into one write too (see
  [§9](#9-the-write-side-cork--flush--sendparts--sendfile)).

**Pipelining and async on HTTP/1**

A retained request holds back the pipelined requests behind it until it is released. That is the
price of keeping **order**, and the only correct behavior. If you want no head-of-line blocking,
use HTTP/2 or HTTP/3, or have the client not pipeline.

---

## 5. Multiplex (HTTP/2, HTTP/3): the handler pool and `MaxConcurrentHandlers`

An HTTP/2 or HTTP/3 connection has **one reader and many requests**. Reading and parsing
(framing, HPACK/QPACK, QUIC packets) happen on the Engine's workers, and **handlers do not run
on the goroutine that reads the connection**: they go to the Engine's handler pool
(`Engine.HandlerPool()`, named `<Name>-streams`). The consequences:

- Requests that arrive concurrently on one connection are handled concurrently; a blocking
  handler only hurts itself.
- The reader never stops reading because one handler is slow (so on a multiplexed connection you
  **cannot** do backpressure by stopping reads; it is done with the stream flow-control window,
  which is replenished only after a handler consumes the body).

**Why the streams pool is separate, and is not the Engine's pool:** after an Engine worker
parses a request it submits it to the streams pool. If both were the same pool, a full queue
could leave every worker stuck submitting with nobody to take tasks, and even the event loop
would hang. Waiting only goes one way (engine worker → other pools), so the pools cannot deadlock
each other.

**Limiting concurrency per connection**

```go
httpConfig := fibhttp.DefaultConfig()
httpConfig.StreamPool.MaxConcurrentHandlers = 8   // at most 8 requests at a time per connection
```

The Nth request runs directly on the goroutine reading the connection, and that connection reads
nothing new until it returns, so **the peer's flow control carries the limit and the server queues
nothing**. N=1 (or `StreamPool.Disable`) is the old one-at-a-time behavior. It is a very cheap
"per-connection concurrency limiter", good for heavy handlers, or when you want the requests of
one connection to stay in order.

**Multiplex + Retain**

On HTTP/2 and HTTP/3 `Retain` suspends only that one stream while the others carry on. One
connection can have hundreds of async requests in flight, which is a qualitative change for
gateway and aggregation services: N concurrent requests on a connection and not N goroutines.

```go
// The same code on h1, h2 and h3:
c.Retain()
upstream.Do(req, func(resp *http.Response, err error) { ...; c.Release() })
```

**Other multiplex notes**

- `Config.MaxConcurrentStreams` (default 250) caps streams per connection; extra streams get
  `REFUSED_STREAM`.
- Responses go out in the stream's own DATA frames and respect the peer's window; a body that
  does not fit the window is held until `WINDOW_UPDATE`.
- 1xx interim responses: `c.WriteInterim(103, hdr)` sends Early Hints, which is the better choice
  than server push (mainstream browsers have turned push off).
- Client side: `fibhttp.Client` runs several requests on one h2 connection; a timeout or cancel
  only sends `RST_STREAM` for that stream, and on GOAWAY / `REFUSED_STREAM` requests the server
  did not process are resent on a new connection automatically.

---

## 6. Object reuse: using Context like fasthttp

`Config.ReuseRequests`, `ReuseHeaders`, `ReuseURLs` and `ReuseContexts` are all on by default.
This is a major source of fib's throughput at high concurrency: at a high request rate and a
high core count, **all cores share one heap**, and GC and heap locks become the bottleneck
(HttpArena, 64 cores: without reuse baseline reaches 1.23M rps and uses only 36 cores; with reuse
1.63M; pipelined goes from 5.7M to 15M).

**The price: the same rule as fasthttp's `RequestCtx`.**

> A reused object belongs to the handler until it is done with the request: the handler has
> returned and has released every `Retain` it took. After that it is handed to the next request
> (possibly on this connection, possibly on another).

In practice:

- **Don't hand `*http.Request`, `c.Request.Header`, `c.Request.URL`, `*Context` or slices of `Context.Body()` to
  a goroutine that outlives the response**, unless you `Retain`ed and only `Release` after it is
  finished with them.
- To keep something, copy it: `c.Request.Clone(ctx)`, `c.Request.Header.Clone()`, `append([]byte(nil), body...)`.
- A handler written for `net/http` that still holds `*http.Request` after returning (for example
  passed to a goroutine without `Retain`): turn off `ReuseRequests`, or `c.Request.Clone(ctx)` first.
- Each option governs only its own object, so a handler that cares only about `Context` can keep
  reuse of the others.
- A `Context` waiting for its next request is in the "finished" state: a stray `Retain` /
  `Release` / `Respond` does nothing and cannot corrupt another request. **That also means the
  bug is silently swallowed**; in tests use `-race` and look for such calls.

```go
// Using request data safely from a heavy async handler:
c.Retain()
q := c.Query("q")                       // zero copy: a substring of c.Request.URL.RawQuery
q = strings.Clone(q)                    // to take it out of the handler, copy it
go func() {
    defer c.Release()
    _ = c.Respond(200, "text/plain", search(q))
}()
```

HTTP/3 reuses request streams as well; `http3.Config.DisableReuse` turns it off.

---

## 7. Zero-copy reads: `Context.Body` / `Context.Query`

- `Context.Body()`: for a non-streamed body (fully read before the handler runs) it returns the
  server's own buffer. `io.ReadAll(c.Request.Body)` grows a buffer and copies again. These bytes **belong
  to the handler until the response ends; they must not be modified or kept**. `Respond` and
  `Write` copy what they are given, so an echo can be
  `c.Respond(200, "application/octet-stream", c.Body())`. It returns nil for a streamed body.
- `Context.Query(name)`: decoded exactly like `c.Request.URL.Query().Get(name)` but without building a
  map; a value that needs no decoding is a substring of the raw query, with zero allocation.
- The `Router` keeps its routing state on the `Context`, reused with it: routing a request
  allocates nothing; read parameters with `c.Param(name)` / `c.Params()` / `c.RoutePattern()`.
  `SetPathValues(true)` makes `c.Request.PathValue` work too, at the cost of one allocation per request
  with parameters.
- "Zero copy" here means **borrowing**: combine it with the lifetime rule of §6 and don't let it
  escape the handler.

---

## 8. Read backpressure: `HoldReads`

On the write side fib pauses reads automatically at two watermarks; **on the read side the
application decides**:

```go
Data: func(c *fib.Connection, data []byte) {
    q.push(append([]byte(nil), data...)) // data is valid only during this callback
    if q.size() >= highWater {
        c.HoldReads(true)                // called inside OnData, it stops this round's read loop immediately too
    }
},
// After another goroutine has drained to the low watermark:
//    c.HoldReads(false)                 // re-registers read interest; the data already in the socket is delivered
```

- The data stays in the **kernel buffer** and TCP's window slows the peer; your side's memory does
  not grow without bound.
- Resuming reads **needs no further byte from the peer**: re-registering read interest delivers
  what has piled up.
- Callable from any goroutine; calls do not nest (the last call wins); `ReadsHeld()` reports the
  state.
- No effect on UDP connections (the event loop has already read the datagrams).
- `fibhttp`'s streamed body uses it for backpressure; use it for your own binary protocol or for
  a forwarding proxy that must not let a slow consumer blow up memory.
- This is fundamentally different from "blocking in the callback": blocking occupies a worker,
  while `HoldReads` makes the connection **stop being scheduled** and the worker goes straight
  back to the pool.

To debug "backpressure triggered although the watermark is far above one message", look at
`ReadsPausedByWatermark` and `ReadsPausedByBudget` in `Engine.Stats()`. The latter means the
`MaxPendingBytes` budget was used up by other connections, **and a connection with almost no
backlog of its own is paused too**.

---

## 9. The write side: `Cork` / `Flush` / `SendParts` / `SendFile`

| API | What it does | When to use |
| --- | --- | --- |
| `Send(b)` | tries a direct write, copies the remainder into the send queue; never blocks | default |
| `SendParts(a, b)` | sends two parts through `writev` without joining them; under backpressure copies only the unsent suffix | frame header + payload, avoiding a join copy |
| `SendFile(f, off, n)` | queues a file range in order with surrounding `Send`s; `sendfile(2)` straight to the socket, the file is read as the peer receives | large files, memory does not grow with file size |
| `Cork()` / `Flush()` | manual control of write coalescing | answering several requests **outside the round**; or when a handler must hand data to the socket midway |

**Corking by hand outside OnData**

`Send` inside `OnData` is corked automatically (merged at the end of the round). From **another
goroutine**, when one connection must receive several replies:

```go
c.Cork()
for _, r := range replies {
    _ = c.Send(r)
}
_ = c.Flush()          // ← required: nothing else writes what a Cork holds
```

> Unless a round of that connection happens to end and flush it along the way. **Don't rely on that.**

**Streaming responses need `Flush`**

On HTTP/1 `Context` implements `http.Flusher`: `fmt.Fprintf(c, ...)` + `c.Flush()` goes out
immediately without waiting for the handler to return. SSE and large downloads work this way
(`http.ServeFile(c, c.Request, path)` uses `sendfile`).

**`SendFile` details**

- The connection duplicates the file descriptor, so `f` may be closed immediately.
- A file shorter than declared closes the connection (the peer is waiting for those bytes).
- TLS connections cannot be zero copy: `SendFile` reads the whole range and encrypts it before
  returning, and all of it is queued in memory.
- Ranges under 16KB are copied directly in HTTP, which is cheaper than an extra system call.

---

## 10. `Read` does not block, a deadline closes the connection

`*fib.Connection` implements `net.Conn`, but **its semantics differ from a blocking socket**:

- **`Read` does not block:** with no data in the socket it returns `fib.ErrWouldBlock`. Normal
  data is delivered through `OnData`; `Read` is for a handler that wants to read the rest of a
  message itself inside `OnData`. **Don't give it to `bufio.Reader` or any code that expects
  blocking semantics.**
- **`Write` does not wait:** it goes through `Send`, copies and queues.
- **A deadline does not fail the call, it closes the connection:** `SetReadDeadline(t)` closes
  the connection at t and `OnClose` receives `os.ErrDeadlineExceeded`. For a rolling timeout,
  reset it every time data arrives; setting the same instant again does not rebuild the timer, so
  resetting every round costs nothing extra.
- After close, `Read`/`Write` return the reason for the close (`os.ErrDeadlineExceeded` for a
  timeout, otherwise `net.ErrClosed`).

**Dark corners you can use**

- **`Read` to pull out what is still in the socket beyond `OnData`:** `OnData` gives you what this
  round has read; to keep reading, call `c.Read(buf)` until `ErrWouldBlock` rather than waiting for
  the next round to be scheduled. Keep the remainder of a partial message yourself and join it in
  the next `OnData`.
- **`Attachment` / `SetAttachment`:** hang any per-connection state on the connection, with no
  locked `map[*Connection]State` and no leak.
- **`c.IsAccepted()` / `c.IsDialed()`:** tell the two ends apart when one handler serves both
  accepted and dialed connections.
- **`c.CloseWrite()`:** shuts the write side only after everything `Send` accepted has been handed
  to the kernel, so the peer reads all the data and then EOF; handy for "reply, then half-close"
  protocols. **It works below the Layer and does not send a TLS close_notify.**
- **`c.CloseRead()`:** `OnData` is no longer called, the EOF from the peer's half-close no longer
  closes the connection, and the connection can still send. Note that on macOS, if the peer keeps
  sending after the read side is shut, the kernel resets the connection.

---

## 11. WebSocket: receiving by frame (OnFrame)

`OnMessage` receives the reassembled **whole** message, so a big message has to be assembled in
memory first. Setting `HandlerFuncs.Frame` delivers messages **frame by frame, without
reassembly**:

```go
handler := websocket.HandlerFuncs{
    // With Frame set, Text/Binary no longer call Message
    Frame: func(c *websocket.Connection, op websocket.Opcode, fin bool, data []byte) {
        _, _ = file.Write(data) // process as it arrives; the connection never holds the whole message
        if fin { _ = file.Close() }
    },
}
```

- Nothing is buffered **between frames**: the peer may send a message far larger than
  `MaxMessageBytes`, and the limit then applies to a **single frame**.
- Each frame carries the message's own opcode (Text/Binary, never Continuation) and `fin`; an
  unfragmented message is one call with `fin=true`.
- Text UTF-8 is still validated incrementally (a character may span frames); invalid text closes
  with 1007.
- **Exception:** the frames of a compressed message (permessage-deflate) are pieces of one DEFLATE
  stream and cannot be inflated frame by frame, so they are still reassembled, inflated and
  delivered as one `fin=true` call.
- It is the same idea as `OnBody`: process large payloads as they arrive. Good for file transfer,
  log upload, streaming ASR and the like.

**Other WebSocket things worth knowing**

- The permessage-deflate sender **keeps no context**: each message borrows a `flate.BestSpeed`
  compressor from a `sync.Pool`, and the connection holds no compressor, so memory is small with
  many connections.
- A custom `Ping` handler **replaces** the default Pong reply (as in gorilla); call
  `c.Pong(payload)` yourself when you want to reply.
- Callbacks run serially with `OnMessage` on the same worker, and the payload is valid only during
  the callback.
- Pipelined WebSocket frames are also parsed in place and merged into one `writev` in the same
  round (see [§4](#4-pipeline-one-write-per-round)).
- To upgrade inside an HTTP server: `ws.Upgrade(c, nil)`; on h2/h3 it is Extended CONNECT
  (RFC 8441 / 9220), only that stream switches over and other requests on the connection carry on.

---

## 12. One Engine as both server and client

All of fib's clients (`Engine.Dial`, `fibhttp.Client`, `websocket.Dialer`, the `http3` client,
`arpc.Dial`, `grpc.NewClient`) are **callback based and never block the caller**, and they share
the server's event loop, worker pool and backpressure.

```go
engine, _ := fib.Bind(config, handler)           // server
client := fibhttp.NewClient(engine, fibhttp.DefaultClientConfig()) // reuses the server's Engine
```

- For a gateway or proxy, use the **same Engine** for the upstream client; no second set of
  threads or pools is needed.
- `Engine.DialWithHandler(...)`: the connection uses **its own handler** and the Engine's handler
  never sees it, so one Engine can host servers and clients of different protocols at once.
- `fib.NewEngine(config, nil)` creates an Engine that **does not listen**, only for dialing.
- `client.Go(req).Wait()` is the Future form, for places that may block (such as `main` or tests).
  **Never `Wait()` inside a handler**, which holds a worker; in a handler use a callback with
  `Retain`/`Release`.
- Callbacks may run on any goroutine (the worker that read the response, the timer goroutine, the
  `fib-client` pool, the caller's goroutine); **don't block for long in them**.
- Client DNS resolution and completion callbacks run on the process-wide `fib-client` pool and TLS
  handshakes on `fib-tls-handshake`; neither occupies the Engine's workers.
- Shutdown order: `client.Close()` first, then the Engine. Closing the Engine directly does not
  notify the client, and requests already sent can only wait for their timeout.

---

## 13. One Handler for h1 / h2 / h3, and Upgrade / Tunnel

- **One `fibhttp.Handler` serves HTTP/1, HTTP/2 (TLS+ALPN, h2c prior knowledge, `Upgrade: h2c`)
  and HTTP/3**, with no change to the handler code. `Request.Proto` tells you which. HTTP/3 uses
  UDP on the same port number, and responses on TCP carry `Alt-Svc` to steer browsers over.
- `Config.HTTP2Only` and `Config.DisableHTTP2` force one or the other.
- `ResponseWriter` `Write`/`Flush` go out immediately on h1; on h2/h3 the response is buffered
  and sent whole after the handler returns. For very large responses use h1 streaming, or
  `FileCache` / `SendFile`.
- **`Context.Upgrade(protocol, header, TunnelHandler)`** answers 101 (or 200 on h2/h3) and hands
  the connection/stream to the `TunnelHandler`; WebSocket's `Upgrade` is built exactly this way.
  Your own protocols (custom binary, CONNECT proxies, tunnels) can borrow it: do handshake, auth
  and routing in HTTP, then switch seamlessly to a raw byte stream.

---

## 14. FileCache and sendfile: static files without touching the disk or user space

```go
files, _ := fibhttp.NewFileCache(fibhttp.FileCacheConfig{Root: "/data/static", Precompressed: true})
files.ServeFile(c, strings.TrimPrefix(c.Request.URL.Path, "/static/"))
```

- A file is read into memory on first request and then **follows the disk**: on Linux an inotify
  watch on the directory invalidates an entry the instant it is created, written, `mv`ed or
  deleted, and a request itself does not stat; on other platforms every request stats once.
- `Precompressed`: picks the on-disk `name.br` / `name.gz` according to `Accept-Encoding` (q
  values included) and sets `Content-Encoding` and `Vary`. **Compression CPU is spent at deploy
  time, not at request time.**
- Unconditional, range-less GET/HEAD is written straight from memory with zero allocation;
  conditional and Range requests go to `net/http.ServeContent`.
- Files larger than `MaxFileBytes` (1MB by default) are sent from disk every time through
  `SendFile`; the total is bounded by `MaxBytes` (64MB by default).
- For plain `sendfile`: `http.ServeFile(c, c.Request, path)` / `http.FileServer` (`Context` implements
  `io.ReaderFrom`, and regular files and a `*io.LimitedReader` wrapping one use `SendFile`).

---

## 15. UDP: `DatagramsHandler` and `SendBatch`

- On a listening socket **each peer address is a `Connection`**: the first datagram triggers
  `OnOpen`, later datagrams from that peer go to it, and it closes after `UDPIdleTimeout` (60
  seconds by default).
- **`DatagramsHandler.OnDatagrams`:** if the Handler implements it, the datagrams already queued
  when the worker runs are delivered **together**, in order, so you can **ACK the whole batch at
  once** and send fewer datagrams. This is the ACK coalescing QUIC / HTTP/3 uses, and it suits
  your own reliable-UDP protocol too.
- Datagrams come from `bufferpool`; give them back with `bufferpool.Put` when done and no longer
  referenced. **The slice holding them must not be kept after `OnDatagrams` returns.**
- **`SendBatch`:** `sendmmsg` on Linux, `sendmsg_x` on macOS, one system call for a batch.
- UDP sends never queue: if the socket has no room the datagram is **dropped** and an error is
  returned, and the connection stays open; UDP does not take part in the write watermark.
- On Linux with `ReusePort`, each poller binds its own `SO_REUSEPORT` UDP socket and reads it
  itself; the kernel hashes the four-tuple to pick one socket, so there is no thundering herd, and
  a given peer always lands on the same poller.

---

## 16. Pools: your own, separate ones, and avoiding deadlock

fib's worker pool (`taskpool`) can be used on its own, or replace the Engine's:

```go
pool := taskpool.NewAdaptive(taskpool.AdaptiveConfig{
    Name: "jobs", MinWorkers: 16, MaxWorkers: 4096, QueueSize: 10000,
})
config.SetTaskPool(pool)          // the Engine uses your pool; closing the Engine does not stop it
```

- **Use a separate pool for async handlers' work, not the Engine's own.** An Engine worker submits
  to your pool; if it is the same pool, a full queue leaves every worker stuck submitting and the
  event loop hangs too. fib does the same internally: `<Name>-streams` (HTTP/2 and HTTP/3
  handlers), `fib-client` and `fib-tls-handshake` are all separate pools, and
  **waiting goes one way only, engine worker → other pools**.
- Blocking handlers (which wait on your application's own I/O) belong in `<Name>-streams` (its
  ceiling is twice the Engine pool's) or your own pool, not the Engine's.
- `SharedTaskPool` (on by default): Engines **with the same name** in a process share workers and
  queue, avoiding duplicate goroutines per listening port. Sharing looks at the name, not the
  configuration: **the first Engine's configuration wins** and later same-name Engines' pool
  settings are ignored. For isolation give them different `Name`s, or set
  `SharedTaskPool=false`.
- `taskpool.ModeInline` starts no workers and runs a task, with recover, on the submitting
  goroutine. **The Engine does not accept it** (a connection's round would then run on the event
  loop), but it is usable as a standalone library feature.
- `Resize(min, max)` adjusts the bounds at runtime; `Workers()` reports the current count.
- `arpc` / `gRPC`: handlers run asynchronously on the streams pool by default;
  `Handler.SetAsyncResponse(false)` or `Handle(method, h, false)` makes a method run
  **synchronously** on the worker that reads the connection (a very short handler saves one
  dispatch); `SetTaskPool` / `grpc.TaskPool` switch to your own pool.

---

## 17. Process-level tricks: Prefork, IOPollers, GOMAXPROCS

**Prefork** (Linux): several processes with `SO_REUSEPORT`, each with its own heap and GC.

```go
func main() {
    if err := prefork.Run(prefork.Config{}, run); err != nil { log.Fatal(err) }
}
```

- Why: with 64 Ps in one process, GC mark work buffers and heap locks are shared by all of them,
  and an allocation-heavy service spends most of its time there instead of on requests. HttpArena,
  64 cores: pipelined 18.2M → 25.4M, latency-1m p99 from 1.6ms → 124µs while using a quarter fewer
  cores.
- By default each child runs with `GOMAXPROCS=2` (`ProcsPerChild`), and the number of children is
  the master's GOMAXPROCS / 2.
- **Put initialization that only children need inside `serve`**; code before `Run` runs in the
  master and in every child. Children share nothing but the port, so in-memory state exists once
  per child, and a resource that exists once per machine (a database connection limit, say) is
  divided by `prefork.Children()`.
- UDP (HTTP/3): after the peer address changes (NAT rebinding / migration) datagrams may land on
  another child, which has no such QUIC connection.
- fib's defaults shift inside a child: the CPU count is `min(NumCPU, GOMAXPROCS)`, and with 4 Ps
  or fewer IOPollers is off.

**IOPollers:** splits an Engine across several event loops (Linux epoll / macOS kqueue). It is on
by default when CPUs > 4; the default poller count is `max(1, CPU/4)` up to 32 CPUs, `CPU/2`
beyond. A poller only waits for events and hands runnable connections to workers, so **more
pollers compete with workers for Ps and are usually slower**. They pay off only when the pollers'
own work (accept) is the bottleneck: with frequent connect/close (limited-conn) on Linux, pollers
accepting for themselves via `SO_REUSEPORT` raised 0.94M to 1.65M requests per second.

**GOMAXPROCS:** **keep the default (equal to the core count) and don't raise it.** Twice the cores
was faster under the early architecture; now the event loop waits in Go's netpoller, that benefit
is gone, and raising it is slower and uses 25–40% more memory. `runtime.NumCPU()` reads the
process's CPU affinity mask, so under taskset/cpuset it already is the usable core count.

---

## 18. bufferpool: your buffers can use the same pool

```go
buf := bufferpool.Get(n)                    // len=n, cap=Align(n); contents are the previous user's, write before reading
defer bufferpool.Put(buf)                   // after Put, not even a sub-slice may be used
buf = bufferpool.Append(buf, data)
out := bufferpool.Join(nil, a, b)           // takes enough at once and joins the parts
```

- **Get/Put allocate nothing and cost about 7ns**; sizes are classed by powers of two, which are
  exactly Go allocator size classes, so there is no rounding waste.
- There is a reserve that **survives GC** (`sync.Pool` is emptied each GC cycle, the reserve is
  not). Its size is learned and capped by `SetRetainedBytes` (32MiB by default);
  `SetRetainedBytes(0)` releases it at once, for memory pressure.
- **One global pool:** fib's read buffers, reply assembly and TLS records are the same buffers
  flowing between classes. Your own buffers can use it too, without holding an extra idle copy.
- `Put` is safe with buffers of unknown origin: ones under 64B or over 64MiB are simply dropped.

---

## 19. Cheat sheet: what may be called from which goroutine

| API | Where it may be called | Notes |
| --- | --- | --- |
| `Connection.Send` / `SendParts` / `SendFile` / `Close` | any goroutine | never blocks |
| `Connection.HoldReads` | any goroutine | called inside `OnData` it stops this round's read loop at once |
| `Connection.Cork` / `Flush` | any goroutine | every `Cork` needs a `Flush` |
| `Connection.Set*Deadline` | any goroutine | closes the connection when it expires, doesn't fail the call |
| `Context.Respond` / `Write` / `Finish` | any goroutine, **before the last `Release`** | `Respond` and `Write` can't be mixed |
| `Context.Retain` / `Release` | any goroutine | paired; no-ops on a finished request |
| `Context.OnBody` / `OnCancel` | in the handler or after (`OnBody` delivers on the handler pool when called later) | callbacks must not block |
| `Context.Conn.Send` (h2/h3) | **don't call** | h2/h3 must go through `Respond`/`WriteResponse` |
| `OnOpen` (TCP) | event loop | must not block |
| `OnData` / `OnClose` | worker | serial per connection; `OnClose` comes after earlier `OnData`s |
| `f` inside `SyscallConn().Control(f)` | holds the connection lock | **don't call this connection's methods inside f**, it deadlocks |
| Dial done / client callbacks | any goroutine (see §12) | don't block for long |

---

## 20. Anti-patterns

1. **Blocking in a handler / `OnData` / `OnBody` callback** (`time.Sleep`, synchronous RPC,
   `future.Wait()`, `io.ReadAll(remote)`, slow disk I/O). It occupies a worker. Use `Retain` plus
   a callback, or hand off to your own pool.
2. **`Retain` without `Release`** (including the error branch, the `OnCancel` branch and the panic
   branch). The connection stays pinned. Use `defer` or `sync.Once` to keep it at exactly one
   `Release` per `Retain`.
3. **Touching `r`/`c`/`c.Body()` after `Release`**, or passing them to a longer-lived goroutine
   (with object reuse you read another request's data, and nothing panics).
4. **`Respond` after `Release`**: the response has ended and the write returns an error.
5. **Keeping `data`** (`OnData`, `OnBody`, `OnFrame` and Ping/Pong payloads are all valid only
   during the callback).
6. **Using `bufio.Reader` on a `Connection`**, or expecting `Read` to block.
7. **A watermark smaller than the replies of one round of reads**, which makes pipelines pause
   reads for no reason.
8. **Turning on `StreamRequestBody` but keeping the default `ReadTimeout`**, so large uploads are
   cut off by the timeout.
9. **Putting async work on the Engine's pool**, or letting `SharedTaskPool` settings of a
   same-name Engine be silently overridden by the first Engine.
10. **Raising GOMAXPROCS** thinking it is faster.
11. **Closing your own connection and then, inside the same `OnData`, waiting for the `OnClose`
    notification** (such as `OnCancel`): deadlock.
12. **`Wait()`ing on a client future inside a handler:** holds a worker until upstream returns.

---

## Related documents

- [Guide (Chinese)](guide.zh-CN.md): full description of each API
- [HTTP/1.x](http1.md) · [HTTP/2](http2.md) · [HTTP/3](http3.md): limits and conformance
- [`examples/http/upload`](../examples/http/upload): a complete `OnBody` large-upload example
- [`taskpool`](../taskpool): the standalone pool and its benchmarks

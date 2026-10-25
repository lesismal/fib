# HTTP/1.x: Support, Limitations and Conformance Tests

[English](http1.md) | [简体中文](http1.zh-CN.md)

This document records what the Go `http` package supports of HTTP/1.0 and
HTTP/1.1 (RFC 9110, RFC 9112), where it stops, and how that is tested. For
usage, see the [HTTP section of the Go guide](guide.zh-CN.md#http-子-package)
(Chinese). HTTP/2 and HTTP/3 have their own documents:
[`http2.md`](http2.md), [`http3.md`](http3.md).

## What is supported

| Area | Server | Client |
| --- | --- | --- |
| Versions | HTTP/1.0 and HTTP/1.1 requests; answers in the request's version | Sends HTTP/1.1, or HTTP/1.0 when the request's `ProtoMinor` is 0 |
| Connections | Keep-alive (HTTP/1.1 by default, HTTP/1.0 with `Connection: keep-alive`), `Connection: close` from either side, pipelining (responses in request order) | Keep-alive pool per host:port; an HTTP/1.0 connection is reused only if the request asked for keep-alive and the server agreed |
| Request bodies | `Content-Length`, chunked with extensions and trailers (`Request.Trailer`); buffered whole by default, or streamed to the handler as they arrive with `StreamRequestBody` | `Content-Length`; chunked with `c.Request.Trailer` when `ContentLength` is -1 |
| Response bodies | `Content-Length`, chunked with trailers, close-delimited for HTTP/1.0 streams | `Content-Length`, chunked with trailers (`Response.Trailer`), close-delimited |
| Streaming | Responses: `Context` is an `http.ResponseWriter`, `http.Flusher` and `io.ReaderFrom`, so `Write` + `Flush` stream the body as it is produced. Requests: with `StreamRequestBody` `Request.Body` is a `*BodyStream`, read without waiting or taken through `Context.OnBody` | Bodies are buffered whole before the callback |
| Files | `Connection.SendFile` / `Context.ReadFrom` / `Context.SendFile`: `sendfile(2)` on Linux and macOS, `TransmitFile` on Windows; over TLS, `sendfile(2)` with kernel TLS on Linux, otherwise sealed a chunk at a time as the socket drains; `http.ServeFile`, `http.ServeContent` and `http.FileServer` work through `Context` (Range, multipart ranges, conditional requests) | — |
| Timeouts | `ReadHeaderTimeout`, `ReadTimeout` and `IdleTimeout`, as `net/http.Server` resolves them, plus `ReadBodyTimeout` | Per-request `Timeout` |
| Interim responses | 1xx through `WriteInterim` or `WriteHeader(1xx)`, automatic `100 Continue` | 1xx skipped |
| Bodiless responses | HEAD (length of the GET kept), 204 and 1xx without `Content-Length`, 304 only with the handler's own `Content-Length` | HEAD, 204, 304 read no body |
| Validation | 400 for a missing or repeated `Host` in HTTP/1.1, for `Transfer-Encoding` in HTTP/1.0 and for malformed messages; 501 for transfer codings other than chunked; 417 for unknown expectations; 413/431 for limits | Malformed responses, unknown transfer codings and short bodies fail the request |
| Framing conflicts | `Content-Length` together with chunked: read as chunked, connection closed after the response | — |
| Headers | `Date` added unless the handler set one; the framing headers the server writes replace the handler's | — |

### Response framing chosen by the writer

When a handler uses `Write`/`WriteHeader` rather than `WriteResponse`, the
server picks the framing:

1. `Content-Length` set by the handler: identity, and writes past it fail
   with `http.ErrContentLength`; a body left short closes the connection.
2. Otherwise, a body no longer than 4KB written before the handler returns:
   `Content-Length` of what was written.
3. Otherwise, HTTP/1.1: chunked, which also carries trailers.
4. Otherwise, HTTP/1.0: the body ends when the connection closes.

`Flush` sends the header and whatever is held back and hands it to the socket
at once; without it the engine batches a read round's output until the
handler returns (see `Connection.Flush`).

### Holding a response open, and body callbacks

A response is held open by a reference count. Serving a request takes one
hold, which the handler's return gives back, so a handler that answers and
returns needs none of this: the response is ended and handed to the connection
for it. A handler that answers later takes another hold with
`Context.Retain()` and gives it back with `Context.Release()`; the response is
written when the last hold goes, wherever that happens.

On an HTTP/1 connection a retained request holds the ones pipelined behind it,
which are parsed and served once it is released, so responses keep their
order. On HTTP/2 and HTTP/3 a stream holds up nothing else.

`Context.OnBody` takes the request body as a callback instead of through
`Request.Body`, the way a websocket handler's `OnFrame` is given a message
frame by frame. `Context.BodyComplete()` says whether the whole body has
arrived already, for a handler that would rather read a complete body
straight from `Request.Body` and use the callback only for one still on its
way:

```go
func(c *fibhttp.Context) {
	f, _ := os.Create("upload.bin")
	c.OnBody(func(data []byte, fin bool, err error) {
		if err != nil {         // the connection went, or the body failed
			f.Close()
			os.Remove("upload.bin")
			return
		}
		f.Write(data)
		if fin {
			f.Close()
			c.Respond(200, "text/plain", []byte("stored"))
		}
	})
}
```

- `OnBody` never calls the callback itself and never waits: it registers it
  and returns. What of the body has already arrived — all of it, when
  `BodyComplete` says so — is handed over once the handler returns, on the
  goroutine that ran it. No goroutine is ever started for a callback: what
  arrives later is handed over as the connection is read — on the worker
  reading an HTTP/1 connection, and on the engine's handler pool
  (`fib.Engine.HandlerPool`) for HTTP/2 and HTTP/3, whose reader serves
  every stream on the connection and must not run a callback — and what is
  already here when `OnBody` is called after the handler returned, which no
  read will bring again, on the handler pool.
- `OnBody` retains the request, and the server releases it once the callback
  has returned from its last call, so the response the callback wrote on
  `fin` is sent then, with no `Retain` or `Release` in the handler. A handler
  that answers somewhere else takes a `Retain` of its own in the callback and
  releases it when it has answered.
- `fin` marks the last call, and `err` a body that will not be finished, which
  is also a last call and comes exactly once. `data` is only valid for the
  duration of the call.
- The callbacks run one at a time and in order, so a body of any size is
  taken without a goroutine of the handler's own and without being
  buffered. What paces the
  peer is the callback itself.
- A body arrives in pieces only when it streams, which
  `StreamRequestBody` decides; one that was read whole before the
  handler ran arrives in a single call with `fin` set.
- `Request.Body` is not readable once `OnBody` has taken it over. Closing it
  ends the callback with `ErrBodyAbandoned` and has the rest of the body
  discarded; a handler that means to refuse an upload without reading it
  answers without calling `OnBody` at all.

### When a request ends before its response does

A connection that closes, a read timeout, or a body that cannot be finished
ends a request the handler may still be working on. The request is cancelled:
nothing more is written, every hold left on it is void, and the handler is
told once, through `OnBody`'s `err` and through `Context.OnCancel`. Both run
on the engine's handler pool rather than in the connection's round, so a handler's
cleanup cannot hold the server up. `Context.Err()` reports the same reason to a handler that
would rather ask than be told.

That holds for a handler that waits where it is, on a database or another
server, as well as for one that retained the request. Such a handler holds up
the connection's round, which is what would read the hangup, so the engine
tells the server as soon as its event loop sees the peer go, through
`fib.HangupHandler`. The request is cancelled while the handler still waits,
and the connection closes, since nothing pipelined behind the request can be
answered in its place. A client that half-closes its side after sending a
request is taken to have gone, as nginx takes it, and as `net/http` takes
it when it cancels the request's context: it gets no response if its handler
is still working when the FIN arrives. This is the
epoll and kqueue backends' doing. On Windows, which arms a read only between
rounds, a handler that waits in place hears nothing until it returns, by which
time its response has gone out.

`Request.Context()` is not cancelled, so that a request needs no context of
its own. Code that hears of a request ending only through a
`context.Context`, as `database/sql`, `httputil.ReverseProxy` and gRPC
clients do, is handed `Context.Context()` instead, or a request carrying it,
`c.Request.WithContext(c.Context())`. It is made the first time it is asked
for, and cancelled when the request is, with what `OnCancel` is told as its
`context.Cause`, or otherwise once the request has ended: the handler has
returned and every `Retain` has been released, as `net/http` cancels a
request's context once `ServeHTTP` returns. It holds nothing of the
`Context`, so it may outlive the request even when the Reuse options recycle
the `Context` for another.

From there everything is idempotent: `Retain`, `Release` and `Finish` on a
request that has ended do nothing, so cleanup code may call them without
checking, and writing to it fails with the reason rather than corrupting the
connection.

### Read timeouts

`Config.ReadHeaderTimeout`, `Config.ReadTimeout` and `Config.IdleTimeout` are
`net/http.Server`'s, resolved the same way: `ReadHeaderTimeout` falls back to
`ReadTimeout`, so does `IdleTimeout`, and zero everywhere means no limit.
Unlike `net/http`, `DefaultConfig` sets them: `ReadHeaderTimeout` 10s
(`DefaultReadHeaderTimeout`), `ReadBodyTimeout` 5 minutes
(`DefaultReadBodyTimeout`) and `IdleTimeout` 2 minutes (`DefaultIdleTimeout`),
leaving `ReadTimeout` at zero since the header's and the body's bound the
request between them; a server that takes longer uploads raises
`ReadBodyTimeout`, and a `Config` built without `DefaultConfig` has none. A connection that outstays one is closed, and its
`OnClose` is given `os.ErrDeadlineExceeded`.

Which one applies is decided by what the connection is waiting for:

| Waiting for | Bounded by | Measured from |
| --- | --- | --- |
| Its first request, or the next one on a kept-alive connection | `IdleTimeout` | the response before it, or the connection opening |
| A header to finish | `ReadHeaderTimeout` | the request's first byte |
| A body to finish, streamed or not | `ReadTimeout` | the request's first byte |
| … and also | `ReadBodyTimeout` | the end of the request's header |
| Nothing — the request has all arrived | nothing | — |

The last row is the difference from `net/http`, where the read deadline is set
for the whole of a request and a handler notices it only when it reads.
Here a deadline closes the connection, so it is dropped once the request has
all arrived: a handler slower than `ReadTimeout` still gets to answer, and
`ReadTimeout` bounds the arrival of a request rather than the work done for
it. A streamed body is still arriving while its handler runs, so `ReadTimeout`
does bound that, which is what keeps a slow upload from holding a connection.
`ReadBodyTimeout` bounds the body alone, alongside `ReadTimeout`, and
whichever ends first applies.

They are also what ends a connection whose client has gone while its body is
unread: a handler that stops reading the body, or holds it with
`Context.HoldBody`, stops the connection's reads once
`StreamRequestBodyBuffer` of it waits, and a connection that reads nothing
does not hear a FIN, since the rest of the body may be ahead of it in the
socket. The read deadline does not depend on reading, so it closes such a
connection when it passes, as `net/http`'s server relies on its own. A hold
has a bound of its own too: `Config.MaxBodyHold` (default
`DefaultMaxBodyHold`, a minute; below zero, none) fails a body held longer at
a time with `ErrBodyHeldTooLong`, which its `OnBody` callback is told.

On HTTP/2, which never stops reading a connection for one stream,
`ReadTimeout` and `ReadBodyTimeout` bound each request's body on its own, from
its HEADERS: a body streaming to its handler fails with
`os.ErrDeadlineExceeded`, one still collected before its handler runs has its
stream reset, and the connection goes on serving. `http3.Config` has the same
three settings for HTTP/3.

A connection that becomes HTTP/2, by the preface or through an `h2c` upgrade,
is released from these: HTTP/2 is served by its own connection state, and
nothing would refresh a deadline the HTTP/1 parser had set. HTTP/2 applies
them its own way instead: `IdleTimeout` (falling back to `ReadTimeout`)
retires a connection that has carried no stream for that long with GOAWAY
`NO_ERROR` and closes it, and the read timeouts bound each request's body (see
below).

### Streaming request bodies

`Config.StreamRequestBody` is off by default: a handler runs only once its
request has arrived whole, so `Request.Body` holds every byte of the body.
Set, it hands a request to the handler as soon as its body has begun — once
the header is in, for a body with a `Content-Length`, and once some of it is,
for a chunked one — and the body arrives as a `*BodyStream` in
`Request.Body`. `Context.RequestBody()` returns it, or nil for a body that was
read whole, and `Context.BodyComplete()` says whether all of it is here. The
same switch streams HTTP/2 bodies, paced by flow control rather than by
holding the connection's reads (see [`http2.md`](http2.md)), and
`http3.Config` has one of its own for HTTP/3.

`Context.HoldBody(true)` pauses what `OnBody` delivers, without waiting:
what arrives waits in the server's buffer and paces the client as an unread
body does, and `HoldBody(false)` resumes it, on the handler pool. A callback
that passes the body on to a slower consumer holds it while the consumer is
behind. That is how a proxy forwards an upload as it arrives with nothing
waiting for it: the handler sends the upstream request with a
`fibhttp.BodyWriter` as its body, `Retain`s the request and returns, and its
`OnBody` callback writes each piece to the writer, which the client sends as
the upstream connection has room; past the writer's limit the callback holds
the body, and `BodyWriter.OnDrain` resumes it (see the `BodyWriter` example,
and [`fib-examples/gateway`](https://github.com/lesismal/fib-examples/tree/main/gateway)).

`Config.StreamRequestBodyThreshold` keeps the smaller bodies buffered whole
while streaming is on: only a body whose `Content-Length` is larger than it,
or a chunked body that has already sent more than it, streams. Zero streams
every body; without `StreamRequestBody` it does nothing.

```go
config := fibhttp.DefaultConfig()
config.StreamRequestBody = true            // hand bodies over as they arrive
config.StreamRequestBodyThreshold = 1 << 20 // but buffer those up to 1MB whole
```

A body read whole sits in a pooled buffer, which the server takes back once
the handler is done with the request: it has returned and released every
`Retain` it took, even one outlasting the connection. For a handler that
retains nothing that is its return, which is the point at which `net/http`
closes a request's body too. Reading it afterwards returns `ErrBodyReleased`; a handler
that needs the bytes later keeps a copy of them, as `io.ReadAll` makes.

**Reading it never waits.** The handler runs on a worker like any other, the
one reading the connection, and a worker that waited for the peer would be waiting on itself,
or held for nothing, so `Read` answers with what has arrived:

| Read returns | Means |
| --- | --- |
| `n > 0` | that much of the body was here |
| `io.EOF` | the whole body has been read |
| `ErrWouldBlock` | none of it is here yet; the rest is still coming |
| `ErrBodyAbandoned` | something else has the body: `Close`, `OnBody`, or the handler returned without retaining the request or calling `OnBody` |
| anything else | the rest will never arrive — the connection closed, the body outgrew `MaxStreamedBodyBytes`, its framing broke |

`ErrWouldBlock` is `fib.ErrWouldBlock`, the same one a `Connection`'s own
`Read` reports, so a handler may test for either. The whole body may be there
already, when it arrived in the same read as its header, in which case the
handler reads through to `io.EOF` and answers without any of what follows.

A handler that meets `ErrWouldBlock` and wants the rest takes it through
`Context.OnBody`, which keeps the request open and delivers the body as it
arrives; see [Holding a response open](#holding-a-response-open-and-body-callbacks).
Nothing is lost across the handover: what `OnBody` is given begins where the
last `Read` stopped.

```go
func(c *fibhttp.Context) {
	n, err := c.Request.Body.Read(buf)          // whatever is here
	switch {
	case errors.Is(err, io.EOF):        // that was all of it
		answer(c)
	case errors.Is(err, fibhttp.ErrWouldBlock): // the rest is still coming
		c.OnBody(func(data []byte, fin bool, err error) { ... })
	}
}
```

- **Backpressure.** Body bytes that have arrived and not been read are
  buffered up to `Config.StreamRequestBodyBuffer` (256KB by default), past
  which the connection stops reading its socket through
  `Connection.HoldReads`, and resumes when the reader has taken half of it.
  A client uploading faster than the handler consumes is slowed by TCP flow
  control rather than by memory growing here, the read-side counterpart of
  the write watermarks.
- **Size.** `MaxBodyBytes` does not bound a streamed body — the point is to
  accept an upload larger than the server will hold — `MaxStreamedBodyBytes`
  does, and zero leaves it unbounded. Growing past it fails the `Read` with
  `ErrBodyTooLarge` and ends the connection; a declared `Content-Length` past
  it is refused with 413 before the handler runs. A handler or middleware
  bounds one request's body lower with `Context.LimitBody(n)`: a body known
  to be past `n` already is refused at once with 413, and one of no declared
  length is ended past `n` as past `MaxStreamedBodyBytes`; a request so ended
  and left unanswered is answered with 413.
- **Pipelining.** A request behind a streamed one is parsed only once that
  request's response has been written, so responses keep their order.
- **A body the handler did not read** is taken off the connection and dropped,
  up to 256KB, so the connection stays usable; more than that (or a chunked
  body, whose length is not known) ends the connection after the response, as
  `net/http` does.
- **`Expect: 100-continue` becomes lazy**: the 100 Continue goes out when the
  handler first asks for the body, by reading it or by taking it with
  `OnBody`, so a handler can refuse the request with 413 or 403 before the
  upload starts. A handler that answers such a request without asking for its
  body ends the connection, since the client is still owed permission.
- **A connection that fails or closes mid-body** fails the `Read` with
  `io.ErrUnexpectedEOF` rather than reporting `io.EOF`, so a truncated upload
  is never taken for a complete one. Reading after the handler has returned,
  or after `Close`, returns `ErrBodyAbandoned`.
- HTTP/2 and HTTP/3 still buffer request bodies whole.

A 128MB upload into a handler that counts it peaks at around 470MB of heap
buffered — the body, the parser buffer it grew in, and what was left behind on
the way — and around 4MB streamed, with `StreamRequestBodyBuffer` at its 256KB
default.

### Recycling request objects

`Config.ReuseRequests`, `ReuseHeaders`, `ReuseURLs` and `ReuseContexts`,
all on in `DefaultConfig`, recycle the `*http.Request`, its `Header`, its
`URL` and the `*Context` of an HTTP/1 request once its response is finished,
instead of leaving a request's worth of them to the collector for every
request. At a high request rate collecting them is what holds a server back:
in go-http-benchmark's pipelined test, 10,000 connections on three cores went
from 1.77-1.81M to 1.99M responses a second with all four on, the most the
client asks for, at less CPU. On many cores it matters more, since one heap
serves every core: in HttpArena's HTTP/1 profiles on 64 CPUs, leaving them to
the collector held the server at 1.23M requests a second in the baseline
profile and 5.7M in the pipelined one, on 36 and 27 of the CPUs, the rest
waiting on the runtime's heap lock, against 1.63M and 15.0M recycling them.
Set an option to false to have its object left to the collector instead. The
HTTP/2 server recycles its streams, which hold a request's objects together,
under the same options and the same rule; see the HTTP/2 notes.

What they ask of a handler is fasthttp's rule for its `RequestCtx`: a
recycled object is the handler's until it is done with the request: it has
returned and released every `Retain` it took. A request retained past its
response, or past its connection going (as `OnCancel` reports), stays its own
until the last `Release`, however long that takes. After that the next
request, on this connection or another, is given it, so a handler that keeps one longer, or hands it to a goroutine
that outlives the response, reads or writes another request's. Keep a copy
of what is needed instead, as `http.Request.Clone` and `http.Header.Clone`
make. A `Context` waiting for its next request reads as finished, so a stray
`Retain`, `Release` or `Respond` on one does nothing.

This is the rule Fiber, Gin and Echo have for their contexts as well. A
handler written for `net/http` that keeps the `*http.Request` after it
returns, say in a goroutine it starts without a `Retain`, turns
`ReuseRequests` off, or takes `c.Request.Clone(ctx)` first.

Each option recycles its own object, so a handler that keeps only, say, the
`Context` can recycle the rest. A request whose body streams, and one outside
the shape the server parses itself (which `net/http` parses instead), keep
their `Request`, `Header` and `URL` whatever the options say.

### The client's request and response heads

The HTTP/1 client writes a request's head and reads a response's head itself,
as the server reads requests and writes responses, rather than through
`net/http`'s `Request.Write` and `ReadResponse`: those cost a proxied request
more than the rest of the client together. What it writes and reads is what
`net/http` would, byte for byte and field for field, which tests and fuzzing
against `net/http` check; anything outside the common shape (a request that
sets `Close` or carries an `httptrace` trace, a host that needs cleaning, a
method that is not a token, a response header folded over lines, an announced
trailer, more than one `Content-Length`) goes through `net/http` as before. A
whole chunked body without a trailer is decoded by the client too, refusing
what `net/http`'s chunked reader refuses. Writing a proxied request takes one
allocation and reading a plain response head five, against eight and thirteen
through `net/http`.

### The client and `Expect: 100-continue`

A client request with a body and `Expect: 100-continue` sends its head alone
and its body once the server answers 100 Continue, or once
`ClientConfig.ExpectContinueTimeout` has passed (1 second in
`DefaultClientConfig`, as in `net/http`'s default transport; zero sends the
body with the head), since a server that does not know the expectation never
answers it. A final response that comes first is the answer: the body is never
sent, and the connection, whose head announced a body that did not follow, is
closed rather than kept. Bodies read whole and bodies that stream are both
held. The HTTP/2 and HTTP/3 clients do the same with their streams.

### Reading the body and the query without copying

`Context.Body()` returns the request body that arrived whole before the
handler ran, as every body does that does not stream: the server's own buffer,
where `io.ReadAll(c.Request.Body)` copies it out through a buffer it grows a piece at
a time. The bytes are the handler's for as long as the body is, until the
response is finished, and must be neither changed nor kept past that.
`Respond` and `Write` copy what they are given, so an echo answers with
`c.Respond(200, "application/octet-stream", c.Body())`. For a body that
streams, `Body` returns nil.

`Context.Query(name)` returns the first value of a query parameter, decoded
as `c.Request.URL.Query().Get(name)` decodes it, without building the map `Query`
builds on every call; a value that needs no decoding is a slice of the raw
query and costs no allocation.

### Zero-copy file sending

`Connection.SendFile(f, offset, count)` queues a file range in order with the
connection's other sends. The connection duplicates the descriptor, so the
caller may close its file at once. On Linux and macOS the bytes go from the
file to the socket by `sendfile(2)`; when the socket is full, the rest waits
for the next write edge, so the file is read only as fast as the peer
consumes it. On Linux this can be seen with
`strace -e sendfile`, for example `sendfile(10, 12, [0] => [2673856], 3145851) = 2673856`
followed by the remainder once the socket drains. On Windows the range goes
by an overlapped `TransmitFile`, posted once what is queued ahead of it has
gone, whose completion plays the write edge; a socket it refuses (a Unix
socket) has the file read a chunk at a time instead. Client editions of
Windows run at most two `TransmitFile` calls at once system-wide and queue the
rest, as they do for Go's `net` package; server editions have no such limit.

Over TLS (`tls.NewServer` or `tls.Dial`, once the handshake is done and the
connection's records are protected by fib's own record layer: AES-GCM over
TLS 1.3 or 1.2, AES-CBC over TLS 1.2 or 1.1):

- **On Linux with kernel TLS**, the first file a connection sends over an
  AES-GCM suite hands the sealing of everything it sends from then on to the
  kernel (`TCP_ULP` "tls" and `TLS_TX`, with the traffic key, IV and next
  sequence number), and the file goes by `sendfile(2)` from the page cache,
  encrypted in the kernel. The switch is made only at a moment when nothing
  sealed in user space is still queued (`Connection.ControlIdle`); a file sent
  at another moment is sealed in user space as below, and the next one tries
  again. Afterwards sends are written as plaintext the kernel seals, and
  alerts such as `close_notify` go as records of their own type
  (`Connection.SendRawRecord`). A KeyUpdate the peer asks for is sent through
  the kernel at the next moment nothing is queued, and the kernel moves to the
  next keys behind it; a kernel that cannot take new keys (Linux before 6.14)
  ends the connection instead. Reading stays in user space. A kernel without
  the `tls` module, or one the process may not load, is noted once and
  never asked again. `tls.Handler.NoKernelTLS` turns the switch off.
- **Otherwise** (macOS, the BSDs, Windows, AES-CBC suites, or kernel TLS off or
  missing) the connection queues the file and its flush reads it 64KB at a
  time as the socket drains, seals each chunk into four full records
  (`fib.FileSealer`), and writes them. The records the file will take are set
  aside in the sequence when it is queued, so whatever is sent after it is
  sealed at once behind them and the order on the wire is kept. A file costs
  one sealed chunk of memory however large it is, and a slow reader holds it
  up exactly as plain `sendfile` does: the rest stays in the file, counted in
  the backlog for `NotifyOutput`, `OnWritable` and the write deadline.

A file shorter than its range closes the connection in every case, since the
peer was promised the whole range.

`Context.ReadFrom` uses it for regular files and for an `*io.LimitedReader`
over one (what `http.ServeContent` passes). Ranges under 16KB are copied
instead, which costs less than the extra system calls.

### Switching protocols

`Context.Upgrade(protocol, header, handler)` switches a request to another
protocol and returns the `Tunnel` its bytes go through. Over HTTP/1.1 the
request has to ask for `protocol` in `Upgrade` and `Connection: Upgrade`; the
server answers `101 Switching Protocols` with `header`, and the whole
connection is the tunnel's from then on, bytes the client sent behind the
request included. Over HTTP/2 and HTTP/3 the servers advertise
`SETTINGS_ENABLE_CONNECT_PROTOCOL`, and an extended CONNECT whose `:protocol`
is `protocol` (reported as `Request.Header[":protocol"]`) switches only its
own stream: the answer is a 200 that leaves it open, and the stream's DATA
frames carry the tunnel both ways under its flow control. Anything else gets
`ErrNotUpgradable`, and the handler answers it as usual.

The `TunnelHandler` hears `OnTunnelOpen` before `Upgrade` returns, then
`OnTunnelData` for what arrives, and `OnTunnelClose` once, with its calls one
at a time and in order. `Tunnel.Send`, `SendParts`, `SendOwned` and
`SendShared` may be called from any goroutine; `CloseAfterSend` ends the connection, or this side
of the stream, once what was sent has gone, and `Close` ends it at once.
`Upgrade` is called from the handler, before it has written anything.

```go
ws := websocket.NewHandler(wsHandler)
handler := http.HandlerFunc(func(c *http.Context) {
	if c.Request.URL.Path == "/ws" {
		_, _ = ws.Upgrade(c, nil) // answers 400/403 itself when it fails
		return
	}
	_ = c.Respond(200, "text/plain", []byte("hello"))
})
```

## Current limitations

- **Request bodies are buffered whole by default.** A request is handed to the
  handler once its body has arrived, bounded by `MaxBodyBytes`. Setting
  `Config.StreamRequestBody` lifts this; see
  [Streaming request bodies](#streaming-request-bodies).
- **Client response bodies are buffered whole**, bounded by
  `MaxResponseBodyBytes`; the client has no streaming download.
- **The client does not pipeline**: one request per HTTP/1 connection at a
  time.
- **A response is ended when the last hold on it goes**, which for a handler
  that retained nothing is its own return, as in `net/http`. A handler that
  wants to answer later, or to go on writing from another goroutine, retains
  the request first; see
  [Holding a response open](#holding-a-response-open-and-body-callbacks).
- **Reads do not block either.** A streamed body reports `ErrWouldBlock`
  rather than waiting for the peer, since the handler runs on a worker that
  would otherwise be held for it; the rest of it is taken through `OnBody`.
- **Writes do not block.** A handler that writes faster than the peer reads
  queues the difference in memory (reads on the connection pause, but the
  handler is not held back). Files sent with `SendFile` are the exception:
  they are read only as the socket drains.
- **Suites fib's record layer does not implement** (ChaCha20-Poly1305 and
  the others that keep a connection on `crypto/tls`) cannot carry a file as it
  drains: there `Connection.SendFile` reads the range and encrypts it before
  returning, so the whole range is queued in memory, and `Context.SendFile`,
  which asks `Connection.QueuesFiles` first, reads it a piece at a time
  instead.
- **HTTP/2 and HTTP/3** accept the same `ResponseWriter` calls and stream the
  same way, in DATA frames; there a write that outruns the client's flow
  control does wait for it (see their documents).
- **No transfer codings besides chunked** (gzip, deflate, compress) are
  decoded; such requests get 501, such responses fail.
- **`Upgrade` is up to the handler**, apart from h2c: `Context.Upgrade`
  answers 101 and hands the connection to a `TunnelHandler`, which is how
  `websocket.ServerHandler.Upgrade` serves WebSocket from an HTTP handler (see
  [Switching protocols](#switching-protocols)). A classic `CONNECT` to a host
  and port reaches the handler but cannot become a tunnel.

## Planned improvements

- File sending that drains with the socket on connections that stay on
  `crypto/tls` (ChaCha20-Poly1305 suites), which needs those suites in fib's
  own record layer.

## Conformance tests

`http/http1_conformance_test.go` (every test is named
`TestHTTP1Conformance…`) checks the fib server and client against peers that
are not fib:

- **fib server** against Go's `net/http` client, raw TCP connections (for
  requests `net/http` never sends: HTTP/1.0 with and without keep-alive,
  pipelined requests, malformed and conflicting framing, chunk extensions),
  and **curl** (`--http1.0`, keep-alive reuse, chunked upload, downloads of the
  sendfile path, ranges, raw chunked trailers, and HTTPS downloads of
  `ServeFile`, `io.Copy` and `Context.SendFile`, on one connection and on one
  each, with kernel TLS and without).
- **fib client** against Go's `net/http` server (`httptest`) and raw servers
  (HTTP/1.0 keep-alive and close-delimited responses, malformed responses).
- **fib client against fib server**, including HTTP/1.0 and sendfile.
- **the client's heads** (`http/client_codec_test.go`): request heads, response
  heads and whole chunked bodies held to `net/http`'s output, by tables and by
  fuzzing.
- **retained responses and body callbacks** (`http/retain_test.go`):
  answering from another goroutine, a retained request holding the pipelined
  ones behind it, nested holds, `OnBody` on a buffered and on a streamed body,
  `OnBody` registering without calling back, `BodyComplete`, `OnBody` called
  after the handler returned, the release after a body's last callback,
  chunked with trailers, answering early and closing the body, a client that
  walks away mid-upload reaching `OnBody` and
  `OnCancel` exactly once, a client that hangs up on a handler waiting in
  place reaching `OnCancel` and `Err` while it waits, with nothing pipelined
  behind it answered, a read timeout reaching `OnCancel`, an oversized
  body, holds taken after the response was written, the same handler over
  HTTP/2, and a stress test running all of it at once and checking that
  nothing is left behind.
- **streaming request bodies** also cover reading without waiting: a read
  that reports `ErrWouldBlock` and hands over to `OnBody` losing nothing, a
  body that is all there reading through to `io.EOF`, `Expect: 100-continue`
  granted by a read and by `OnBody`, and 64 uploads held open at once without
  a goroutine between them.
- **read timeouts** (`http/timeout_test.go`): a header and a body that stop
  arriving, a streamed body that stops arriving, a handler slower than
  `ReadTimeout` still answering, an idle connection closed and an idle timeout
  refreshed by each request, a connection that never speaks, a server without
  timeouts, a connection that becomes HTTP/2 being released from them, and the
  timeout reaching `OnClose`. `netconn_test.go` checks
  `Connection`'s own deadlines and its other `net.Conn` methods.
- **streaming request bodies** (`http/body_test.go`): the handler running
  before the body ends, bodies under the threshold staying buffered, chunked
  uploads with trailers fed a chunk at a time, reads held while the handler is
  behind, discard and close after an unread body, lazy and refused
  100-continue, `MaxStreamedBodyBytes`, a truncated upload, pipelining behind a
  streamed request, a panicking handler, uploads from `net/http`'s own client,
  and the incremental chunked decoder fed one byte at a time.
  `hold_reads_test.go` checks `Connection.HoldReads` itself.
- **the body matrix** (`http/body_matrix_test.go`, and
  `http3/body_matrix_test.go` for HTTP/3): every combination of protocol
  (HTTP/1.1 and HTTP/2, each in cleartext and over TLS; HTTP/3), framing (no
  body, a length, chunked or HTTP/2 and HTTP/3 DATA with no length, a trailer,
  `Expect: 100-continue`), size (a body read in one go and one taking many
  reads), sending (all in one write, or in pieces with pauses, cut mid chunk
  line and mid frame), the handler's way of taking the body (reading it, OnBody,
  `BodyComplete` deciding between them, OnBody from another goroutine after
  the handler returned) and server configuration (buffered, streaming every
  body, streaming past a threshold), with a second request behind each on the
  same connection. Each checks every byte, the trailer, whether the body
  streamed, that a buffered body was complete, that no callback came inside
  OnBody or before the handler returned and exactly one last call came, and
  that the answer written from that call went out. Alongside: a request cut
  off mid-body on each protocol, where the body limits fall, and the
  flow-control windows pacing a slow handler on HTTP/2 and HTTP/3.
- `sendfile_test.go` checks `Connection.SendFile` over TCP and Unix
  sockets, from the handler and from other goroutines, with a slow reader, and
  a file shorter than its range; `sendfile_sealed_test.go` checks a file
  sealed as the socket drains (memory bounded to a chunk with the peer reading
  nothing, order with sends around it, a short file) and `ControlIdle`, and
  `sendfile_windows_test.go` checks `TransmitFile`, a short file and a socket
  that refuses it.
- `tls/file_test.go` checks `SendFile` over TLS against `crypto/tls` clients,
  for TLS 1.3, AES-GCM and AES-CBC over TLS 1.2, with kernel TLS and without:
  from the handler and outside it, sends before and after, a slow reader with
  the heap checked, a short file, `close_notify` after it, a connection left
  on `crypto/tls`, and a KeyUpdate through kernel TLS.

The peers are the standard library and the curl binary, so the Go module takes
no new dependency. CI runs the suite as its own job, **HTTP/1.x conformance**,
on Linux, macOS and Windows with `FIB_REQUIRE_CURL=1`, which makes a missing
curl fail the job instead of skipping those cases. To run it locally:

```sh
go test -race -run 'TestHTTP1Conformance|TestSendFile|TestSendableFileOf|TestResponseWriter|TestStreamRequestBody|TestChunkedDecoder|TestHoldReads|TestServerRead|TestServerIdle|TestServerTimeout|TestServerWithoutTimeouts|TestReadDeadline|TestWriteDeadline|TestZeroDeadline|TestConnectionAddresses|TestRetain|TestOnBody|TestBodyComplete|TestBodyMatrix|TestOnCancel|TestTransmitFile|TestControlIdle|TestKernelTLS' -v . ./http/ ./http3/ ./tls/
```

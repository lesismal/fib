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
| Request bodies | `Content-Length`, chunked with extensions and trailers (`Request.Trailer`); buffered whole by default, or streamed to the handler as they arrive with `StreamRequestBody` | `Content-Length`; chunked with `req.Trailer` when `ContentLength` is -1 |
| Response bodies | `Content-Length`, chunked with trailers, close-delimited for HTTP/1.0 streams | `Content-Length`, chunked with trailers (`Response.Trailer`), close-delimited |
| Streaming | Responses: `Context` is an `http.ResponseWriter`, `http.Flusher` and `io.ReaderFrom`, so `Write` + `Flush` stream the body as it is produced. Requests: with `StreamRequestBody` `Request.Body` is a `*BodyStream`, read without waiting or taken through `Context.OnBody` | Bodies are buffered whole before the callback |
| Files | `Connection.SendFile` / `Context.ReadFrom`: `sendfile(2)` on Linux and macOS, chunked reads on Windows; `http.ServeFile`, `http.ServeContent` and `http.FileServer` work through `Context` (Range, multipart ranges, conditional requests) | — |
| Timeouts | `ReadHeaderTimeout`, `ReadTimeout` and `IdleTimeout`, as `net/http.Server` resolves them | Per-request `Timeout` |
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
func(c *fibhttp.Context, r *http.Request) {
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
  goroutine that ran it; `OnBody` called after the handler has returned hands
  it over on a goroutine of its own.
- `OnBody` retains the request, and the server releases it once the callback
  has returned from its last call, so the response the callback wrote on
  `fin` is sent then, with no `Retain` or `Release` in the handler. A handler
  that answers somewhere else takes a `Retain` of its own in the callback and
  releases it when it has answered.
- `fin` marks the last call, and `err` a body that will not be finished, which
  is also a last call and comes exactly once. `data` is only valid for the
  duration of the call.
- The callbacks run one at a time and in order, the ones after the handover
  on the connection's worker, so a body of any size is taken without a
  goroutine of the handler's own and without being buffered. What paces the
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
on a goroutine rather than on the event loop, so a handler's cleanup cannot
hold the server up. `Context.Err()` reports the same reason to a handler that
would rather ask than be told.

From there everything is idempotent: `Retain`, `Release` and `Finish` on a
request that has ended do nothing, so cleanup code may call them without
checking, and writing to it fails with the reason rather than corrupting the
connection.

### Read timeouts

`Config.ReadHeaderTimeout`, `Config.ReadTimeout` and `Config.IdleTimeout` are
`net/http.Server`'s, resolved the same way: `ReadHeaderTimeout` falls back to
`ReadTimeout`, so does `IdleTimeout`, and zero everywhere means no limit,
which is the default. A connection that outstays one is closed, and its
`OnClose` is given `os.ErrDeadlineExceeded`.

Which one applies is decided by what the connection is waiting for:

| Waiting for | Bounded by | Measured from |
| --- | --- | --- |
| Its first request, or the next one on a kept-alive connection | `IdleTimeout` | the response before it, or the connection opening |
| A header to finish | `ReadHeaderTimeout` | the request's first byte |
| A body to finish, streamed or not | `ReadTimeout` | the request's first byte |
| Nothing — the request has all arrived | nothing | — |

The last row is the difference from `net/http`, where the read deadline is set
for the whole of a request and a handler notices it only when it reads.
Here a deadline closes the connection, so it is dropped once the request has
all arrived: a handler slower than `ReadTimeout` still gets to answer, and
`ReadTimeout` bounds the arrival of a request rather than the work done for
it. A streamed body is still arriving while its handler runs, so `ReadTimeout`
does bound that, which is what keeps a slow upload from holding a connection.

A connection that becomes HTTP/2, by the preface or through an `h2c` upgrade,
is released from these: HTTP/2 is served by its own connection state, and
nothing would refresh a deadline the HTTP/1 parser had set.

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

**Reading it never waits.** The handler runs on the connection's worker like
any other, and a worker that waited for the peer would be waiting on itself,
so `Read` answers with what has arrived:

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
func(c *fibhttp.Context, r *http.Request) {
	n, err := r.Body.Read(buf)          // whatever is here
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
  it is refused with 413 before the handler runs.
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
each off by default, recycle the `*http.Request`, its `Header`, its `URL` and
the `*Context` of an HTTP/1 request once its response is finished, instead
of leaving a request's worth of them to the collector for every request. At
a high request rate collecting them is what holds a server back: in
go-http-benchmark's pipelined test, 10,000 connections on three cores went
from 1.77-1.81M to 1.99M responses a second with all four on, the most the
client asks for, at less CPU.

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

Each option recycles its own object, so a handler that keeps only, say, the
`Context` can recycle the rest. A request whose body streams, and one outside
the shape the server parses itself (which `net/http` parses instead), keep
their `Request`, `Header` and `URL` whatever the options say.

### Zero-copy file sending

`Connection.SendFile(f, offset, count)` queues a file range in order with the
connection's other sends. The connection duplicates the descriptor, so the
caller may close its file at once. On Linux and macOS the bytes go from the
file to the socket by `sendfile(2)`; when the socket is full, the rest waits
for the next write edge, so the file is read only as fast as the peer
consumes it. On Linux this can be seen with
`strace -e sendfile`, for example `sendfile(10, 12, [0] => [2673856], 3145851) = 2673856`
followed by the remainder once the socket drains.

`Context.ReadFrom` uses it for regular files and for an `*io.LimitedReader`
over one (what `http.ServeContent` passes). Ranges under 16KB are copied
instead, which costs less than the extra system calls.

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
  rather than waiting for the peer, since the handler runs on the connection's
  own worker; the rest of it is taken through `OnBody`.
- **Writes do not block.** A handler that writes faster than the peer reads
  queues the difference in memory (reads on the connection pause, but the
  handler is not held back). Files sent with `SendFile` are the exception:
  they are read only as the socket drains.
- **TLS cannot use sendfile.** Over TLS, `SendFile` reads the range and
  encrypts it before returning, so the whole range is queued in memory.
- **HTTP/2 and HTTP/3** accept the same `ResponseWriter` calls but hold the
  response until the handler returns (see their documents).
- **No transfer codings besides chunked** (gzip, deflate, compress) are
  decoded; such requests get 501, such responses fail.
- **No `Upgrade` handling other than h2c and WebSocket** (the latter in the
  `websocket` package); `CONNECT` requests reach the handler but cannot become
  tunnels.

## Planned improvements

- Streaming request bodies for HTTP/2 and HTTP/3, which still buffer them
  whole, and streaming response bodies for the client.
- A way for a handler to wait for its queued output to drain, so that
  streaming a large generated body does not grow memory.
- Lazily encrypted file sending over TLS, reading the file only as the socket
  drains, and kTLS on Linux for true zero-copy HTTPS.
- `TransmitFile` on Windows.

## Conformance tests

`http/http1_conformance_test.go` (every test is named
`TestHTTP1Conformance…`) checks the fib server and client against peers that
are not fib:

- **fib server** against Go's `net/http` client, raw TCP connections (for
  requests `net/http` never sends: HTTP/1.0 with and without keep-alive,
  pipelined requests, malformed and conflicting framing, chunk extensions),
  and **curl** (`--http1.0`, keep-alive reuse, chunked upload, downloads of the
  sendfile path, ranges, raw chunked trailers).
- **fib client** against Go's `net/http` server (`httptest`) and raw servers
  (HTTP/1.0 keep-alive and close-delimited responses, malformed responses).
- **fib client against fib server**, including HTTP/1.0 and sendfile.
- **retained responses and body callbacks** (`http/retain_test.go`):
  answering from another goroutine, a retained request holding the pipelined
  ones behind it, nested holds, `OnBody` on a buffered and on a streamed body,
  `OnBody` registering without calling back, `BodyComplete`, `OnBody` called
  after the handler returned, the release after a body's last callback,
  chunked with trailers, answering early and closing the body, a client that
  walks away mid-upload reaching `OnBody` and
  `OnCancel` exactly once, a read timeout reaching `OnCancel`, an oversized
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
  a file shorter than its range.

The peers are the standard library and the curl binary, so the Go module takes
no new dependency. CI runs the suite as its own job, **HTTP/1.x conformance**,
on Linux, macOS and Windows with `FIB_REQUIRE_CURL=1`, which makes a missing
curl fail the job instead of skipping those cases. To run it locally:

```sh
go test -race -run 'TestHTTP1Conformance|TestSendFile|TestSendableFileOf|TestResponseWriter|TestStreamRequestBody|TestChunkedDecoder|TestHoldReads|TestServerRead|TestServerIdle|TestServerTimeout|TestServerWithoutTimeouts|TestReadDeadline|TestWriteDeadline|TestZeroDeadline|TestConnectionAddresses|TestRetain|TestOnBody|TestBodyComplete|TestBodyMatrix|TestOnCancel' -v . ./http/ ./http3/
```

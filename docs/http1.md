# HTTP/1.x: Support, Limitations and Conformance Tests

[English](http1.md) | [简体中文](http1.zh-CN.md)

This document records what the Go `http` package supports of HTTP/1.0 and
HTTP/1.1 (RFC 9110, RFC 9112), where it stops, and how that is tested. For
usage, see the [HTTP section of the Go README](../go/README.zh-CN.md#http-子-package)
(Chinese). HTTP/2 and HTTP/3 have their own documents:
[`http2.md`](http2.md), [`http3.md`](http3.md).

## What is supported

| Area | Server | Client |
| --- | --- | --- |
| Versions | HTTP/1.0 and HTTP/1.1 requests; answers in the request's version | Sends HTTP/1.1, or HTTP/1.0 when the request's `ProtoMinor` is 0 |
| Connections | Keep-alive (HTTP/1.1 by default, HTTP/1.0 with `Connection: keep-alive`), `Connection: close` from either side, pipelining (responses in request order) | Keep-alive pool per host:port; an HTTP/1.0 connection is reused only if the request asked for keep-alive and the server agreed |
| Request bodies | `Content-Length`, chunked with extensions and trailers (`Request.Trailer`); buffered whole, or streamed to the handler as they arrive past `StreamRequestBodyThreshold` | `Content-Length`; chunked with `req.Trailer` when `ContentLength` is -1 |
| Response bodies | `Content-Length`, chunked with trailers, close-delimited for HTTP/1.0 streams | `Content-Length`, chunked with trailers (`Response.Trailer`), close-delimited |
| Streaming | Responses: `Context` is an `http.ResponseWriter`, `http.Flusher` and `io.ReaderFrom`, so `Write` + `Flush` stream the body as it is produced. Requests: `Request.Body` is a `*BodyStream` past `StreamRequestBodyThreshold` | Bodies are buffered whole before the callback |
| Files | `Connection.SendFile` / `Context.ReadFrom`: `sendfile(2)` on Linux and macOS, chunked reads on Windows; `http.ServeFile`, `http.ServeContent` and `http.FileServer` work through `Context` (Range, multipart ranges, conditional requests) | — |
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

### Streaming request bodies

`Config.StreamRequestBodyThreshold`, when positive, hands a request to the
handler before its whole body has arrived: a body whose `Content-Length` is
larger than it, or a chunked body that has already sent more than it, arrives
as a `*BodyStream` in `Request.Body` — an `io.ReadCloser`, so `io.Copy`,
`multipart.Reader` and `json.Decoder` read it as they read any net/http body.
`Context.RequestBody()` returns it, or nil for a body that was read whole.
A body under the threshold is unaffected: buffered whole, handler on the
connection's worker, nothing else changed.

- **The handler runs on its own goroutine**, since reading the body blocks;
  one per request whose body streams, not one per connection.
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
  handler has returned, so responses keep their order.
- **A body the handler did not read** is taken off the connection and dropped,
  up to 256KB, so the connection stays usable; more than that (or a chunked
  body, whose length is not known) ends the connection after the response, as
  `net/http` does.
- **`Expect: 100-continue` becomes lazy**: the 100 Continue goes out on the
  first read of the body, so a handler can refuse the request with 413 or 403
  before the upload starts. A handler that answers such a request without
  reading it ends the connection, since the client is still owed permission.
- **A connection that fails or closes mid-body** fails the `Read` with
  `io.ErrUnexpectedEOF` rather than reporting `io.EOF`, so a truncated upload
  is never taken for a complete one. Reading after the handler has returned,
  or after `Close`, returns `ErrBodyAbandoned`.
- HTTP/2 and HTTP/3 still buffer request bodies whole.

A 128MB upload into a handler that copies it to `io.Discard` peaks at around
491MB of heap buffered — the body, the parser buffer it grew in, and what the
copy left behind — and around 3MB streamed, with
`StreamRequestBodyBuffer` at its 256KB default.

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
  `Config.StreamRequestBodyThreshold` lifts this for bodies past it; see
  [Streaming request bodies](#streaming-request-bodies).
- **Client response bodies are buffered whole**, bounded by
  `MaxResponseBodyBytes`; the client has no streaming download.
- **The client does not pipeline**: one request per HTTP/1 connection at a
  time.
- **Streaming responses are written from the handler.** The response is ended
  when the handler returns, as in `net/http`; a handler that wants to answer
  later from another goroutine must use `WriteResponse` and not start the
  response with `Write` first.
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
- **No idle, header or body read timeouts on the server.** A slow client can
  hold a connection open; see the planned improvements.

## Planned improvements

- Server-side timeouts: read-header, body and idle, like `net/http.Server`'s
  `ReadHeaderTimeout`, `ReadTimeout` and `IdleTimeout`.
- Streaming request bodies for HTTP/2 and HTTP/3, which still buffer them
  whole, and streaming response bodies for the client.
- A way for a handler to wait for its queued output to drain, so that
  streaming a large generated body does not grow memory.
- Lazily encrypted file sending over TLS, reading the file only as the socket
  drains, and kTLS on Linux for true zero-copy HTTPS.
- `TransmitFile` on Windows.

## Conformance tests

`go/http/http1_conformance_test.go` (every test is named
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
- **streaming request bodies** (`go/http/body_test.go`): the handler running
  before the body ends, bodies under the threshold staying buffered, chunked
  uploads with trailers fed a chunk at a time, reads held while the handler is
  behind, discard and close after an unread body, lazy and refused
  100-continue, `MaxStreamedBodyBytes`, a truncated upload, pipelining behind a
  streamed request, a panicking handler, uploads from `net/http`'s own client,
  and the incremental chunked decoder fed one byte at a time.
  `go/hold_reads_test.go` checks `Connection.HoldReads` itself.
- `go/sendfile_test.go` checks `Connection.SendFile` over TCP and Unix
  sockets, from the handler and from other goroutines, with a slow reader, and
  a file shorter than its range.

The peers are the standard library and the curl binary, so the Go module takes
no new dependency. CI runs the suite as its own job, **HTTP/1.x conformance**,
on Linux, macOS and Windows with `FIB_REQUIRE_CURL=1`, which makes a missing
curl fail the job instead of skipping those cases. To run it locally:

```sh
cd go
go test -race -run 'TestHTTP1Conformance|TestSendFile|TestSendableFileOf|TestResponseWriter|TestStreamRequestBody|TestChunkedDecoder|TestHoldReads' -v . ./http/
```

# HTTP/2

[English](07-http2.md) | [简体中文](07-http2.zh-CN.md)

Package `fib/http` speaks HTTP/2 as well as HTTP/1.x, from one handler and one binding: cleartext with prior knowledge or `Upgrade: h2c`, and over TLS through ALPN. HTTP/2 is where the fib execution model changes — many streams share a connection, their handlers run on a pool, and flow control replaces the HTTP/1 rule of one request at a time. This chapter shows both ways to start an HTTP/2 server, how multiplexing and the handler pool behave, and the parts of HTTP/2 fib implements (push, 1xx, trailers, streaming) with their limits; the protocol reference is [HTTP/2](../http2.md).

## The model

An HTTP/2 connection has one reader and many streams. The engine's workers frame the connection and decode HPACK/QPACK headers; each complete request is submitted to the engine's handler pool — the shared stream pool named `<Name>-streams`, `fib-streams` by default — where its handler runs. A slow handler therefore holds up only its own stream, and requests a client has open are served concurrently. `Config.MaxConcurrentStreams` (250 by default) bounds the streams per connection, and `Config.StreamPool.MaxConcurrentHandlers` can bound how many of one connection's requests run at once; the last of the N runs on the reader, which then reads nothing further until it returns, so the peer's flow control pays for the limit instead of a queue on the server. Responses are framed as DATA and paced by the peer's flow-control windows. Raw bytes must never go through `Context.Conn`: that would corrupt the framing, so every write goes through `Context`.

## h2c: HTTP/2 without TLS

One plaintext binding serves HTTP/1.x and h2c at once — a connection that starts with the HTTP/2 preface is served as HTTP/2, and an HTTP/1.1 request with `Upgrade: h2c` is upgraded. The handler does not change.

```go
package main

import (
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.String(stdhttp.StatusOK, "hello over %s\n", c.Request.Proto)
	})
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080", // h1 and h2c on the same port
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

`c.Request.Proto` is `HTTP/2.0` for these requests. The two ways a client starts h2c look different on the wire:

```sh
curl --http2-prior-knowledge http://127.0.0.1:8080/   # the preface; preferred
curl --http2 http://127.0.0.1:8080/                   # Upgrade: h2c, deprecated by RFC 9113
```

Set `Config.HTTP2Only` when only HTTP/2 will ever arrive (h2spec needs it, since it cannot read an HTTP/1 error response), and `Config.DisableHTTP2` to serve HTTP/1 only.

## h2 over TLS

Over TLS, ALPN picks the protocol: `fibhttp.ConfigureTLS` copies a `*crypto/tls.Config` with `h2` and `http/1.1` in its ALPN list, `fibtls.NewServer` runs the handshake and then hands the connection to the HTTP handler. The same handler answers HTTP/1.x clients that did not negotiate HTTP/2.

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
	serverTLS, _, err := tlstest.Configs() // a throwaway certificate for 127.0.0.1
	if err != nil {
		panic(err)
	}
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.String(stdhttp.StatusOK, "hello over %s\n", c.Request.Proto)
	})
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
curl -k --http2 https://127.0.0.1:8443/    # ALPN: h2
curl -k --http1.1 https://127.0.0.1:8443/  # ALPN: http/1.1, same handler
```

`tlstest` is for examples and tests only; real certificates are chapter [10](10-tls.md).

## Multiplexing and the handler pool

Because handlers run on the pool, a handler that waits — on a database, an upstream server, a channel — blocks only itself. That makes `Retain`/`Release` much cheaper than on HTTP/1: a retained HTTP/2 request suspends one stream and the connection carries the rest, so one connection can have hundreds of asynchronous requests in flight, and a request that is cancelled (`OnCancel`) or reset affects only its own stream. The per-connection and per-server limits are the two config fields below, and the flow-control windows trade memory for throughput.

```go
config := fibhttp.DefaultConfig()
config.MaxConcurrentStreams = 250            // streams the client may open; the default
config.StreamPool.MaxConcurrentHandlers = 16 // at most 16 of one connection's requests at once
config.HTTP2 = fibhttp.HTTP2Config{
	StreamWindow:   4 << 20,  // each stream may have 4MB in flight
	ConnWindow:     32 << 20, // shared by the connection's streams
	AdaptiveWindow: true,     // let the connection grow them to the path's bandwidth-delay product
}
```

The windows matter for streamed bodies: the receive window is given back only as the handler consumes the body, so a fast client is paced by flow control rather than buffered. A bounded server should also remember that a connection can hold up to about `MaxConcurrentStreams × MaxBodyBytes` of whole request bodies (250 × 16MB with the defaults), which is an argument for `StreamRequestBody` on upload endpoints.

## Streaming bodies and responses

`Config.StreamRequestBody` has the same meaning as on HTTP/1: the handler runs once the body has begun and takes it through `Request.Body` (a `*BodyStream`, read without waiting) or `Context.OnBody`. The difference is how the client is paced: HTTP/2 cannot stop reading a connection for one stream, so the stream's receive window is replenished only as the handler consumes, one window ahead at most, while the connection's other streams keep running. A response streams the other way: `Write` + `Flush` produce DATA frames as the handler produces them, and a handler that outruns the client's window waits once 64KB of its body is held back.

```go
config := fibhttp.DefaultConfig()
config.StreamRequestBody = true
config.StreamRequestBodyThreshold = 1 << 20 // keep bodies up to 1MiB whole
config.MaxStreamedBodyBytes = 64 << 20
```

```go
func events(c *fibhttp.Context) {
	c.Header().Set("Content-Type", "text/event-stream")
	for i := 0; i < 10; i++ {
		if _, err := fmt.Fprintf(c, "data: %d\n\n", i); err != nil {
			return
		}
		c.Flush() // DATA frames as they are produced, paced by the client's window
	}
}
```

A response below 4KB that is not flushed is still sent whole with a `content-length`; a flush or a larger body turns it into a stream of DATA frames ending in END_STREAM or trailer HEADERS. SSE, long polling and large downloads therefore work over HTTP/2 with the same handler code as chapter [06](06-http1.md).

## Server push

`Context.Push` promises a resource before the client asks for it, and serves the promised request through the handler on its own stream before `Push` returns. Call it before writing the response that would lead the client to ask; it returns `stdhttp.ErrNotSupported` on HTTP/1 and HTTP/3, on a request that was itself pushed, and when the client disabled push.

```go
func index(c *fibhttp.Context) {
	if err := c.Push("/style.css", nil); err != nil {
		// stdhttp.ErrNotSupported on HTTP/1 and HTTP/3, or when the client disabled push
	}
	_ = c.HTML(stdhttp.StatusOK, `<link rel="stylesheet" href="/style.css">`)
}
```

Two practical notes: the pushed handler runs synchronously on the goroutine that calls `Push`, so a slow pushed handler delays the parent response; and mainstream browsers have removed push, and both fib's and `net/http`'s HTTP/2 clients send `ENABLE_PUSH=0`. For preloading, `WriteInterim` with 103 Early Hints is the supported replacement.

## 1xx interim responses and trailers

`WriteInterim(status, header)` sends an informational response ahead of the final one — 103 Early Hints with `Link` headers is the common use — and may be called more than once. 101 is not allowed, and an HTTP/1.0 client gets `stdhttp.ErrNotSupported`. Trailers work as on HTTP/1: `WriteResponse` with `Response.Trailer`, or `http.ResponseWriter` plus a `Trailer` header, arrive as trailing HEADERS after the DATA frames.

```go
func hints(c *fibhttp.Context) {
	_ = c.WriteInterim(stdhttp.StatusEarlyHints, stdhttp.Header{
		"Link": {`</app.css>; rel=preload; as=style`},
	})
	_ = c.HTML(stdhttp.StatusOK, `<link rel="stylesheet" href="/app.css">`)
}
```

## Limits and what HTTP/2 does not do

- **No client-initiated h2c upgrade**: cleartext HTTP/2 is prior knowledge only for fib's client (`UnencryptedHTTP2`); the server still accepts `Upgrade: h2c` for curl and others. Over TLS, switching protocols is ALPN only.
- **Priority is ignored**: PRIORITY frames and the RFC 7540 priority tree are validated and dropped (RFC 9113 deprecates them); an RFC 9218 `priority` header's urgency is honoured for response scheduling.
- **Ping and SETTINGS are bounded**: the server ends a connection with GOAWAY when its client resets streams or sends control frames past a budget (`StreamResetBurst`/`StreamResetRate`, `ControlFrameBurst`/`ControlFrameRate`, `MaxContinuationFrames`), and a client that does not acknowledge SETTINGS within `SettingsTimeout` (10s) is dropped. `Config.PingInterval` adds a keep-alive PING, off by default.
- **A classic CONNECT gets no tunnel**: it is parsed and handed to a handler, but no bidirectional tunnel is established. Extended CONNECT (RFC 8441) is different — that is how WebSocket runs over HTTP/2 through `Context.Upgrade` (chapter [09](09-websocket.md)).
- **Memory**: whole request bodies are bounded per connection by `MaxConcurrentStreams × MaxBodyBytes`; use `StreamRequestBody` or `StreamPool.MaxConcurrentHandlers` to keep it down.

## Pitfalls

- **Never write to `Context.Conn` directly.** HTTP/2 output must go through `Context`; raw bytes break the frame stream and the connection dies.
- **The handler pool is shared.** A blocked handler holds a pool worker, not the connection reader, but enough blocked handlers starve every HTTP/2 and HTTP/3 server on engines of that name. Keep slow work off the pool or size it with `StreamPool.MaxConcurrentHandlers`.
- **`Push` runs inline.** A pushed handler that waits delays the response that called `Push`; push is also disabled by most clients.
- **A response body is not yours after the write.** `Respond`/`WriteResponse` keep the slice until the stream is finished (HTTP/3 adds the peer's acknowledgement), so do not change it; use `RespondCopy`/`WriteResponseCopy` when the buffer must be reused.
- **Streamed bodies need `StreamRequestBody`.** Otherwise the body is buffered whole before the handler runs and a big upload costs `MaxBodyBytes` per stream.
- **Stream and request objects are recycled.** The rule from chapter [06](06-http1.md) applies unchanged: don't keep the `Request`, `Header`, `URL` or `Context` past the response and the last `Release`.
- **h2c is not what browsers speak.** Browsers use h2 over TLS; h2c is for internal services, gRPC cleartext, and conformance tools.

## See also

- [HTTP/2](../http2.md) — the protocol reference: configurable parameters, limits, h2spec, benchmarks.
- [HTTP/3](08-http3.md) — the same handler over QUIC, and the differences multiplexing brings.
- [HTTP/1.x](06-http1.md) — the request/response API this chapter assumes, with the most detail.
- [Handlers: sync and async](03-handlers.md) — `Retain`/`Release` and why they are cheap on a multiplexed connection.
- [Hacks](../hacks.md) — the multiplex, handler-pool and pipeline tricks in depth.
- [Clients](20-clients.md) — the HTTP/2 client, connection pooling and automatic retries.

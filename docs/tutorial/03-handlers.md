# Handlers: sync and async

[English](03-handlers.md) | [简体中文](03-handlers.zh-CN.md)

fib has two handler levels: a **connection** handler, which sees bytes on a raw TCP, UDP or Unix connection, and an **HTTP** handler, which sees a parsed request and answers it. Both run on the engine's worker pool under the same rule — one round of a connection at a time, never block — which is why the asynchronous API exists. This chapter shows both levels, the execution model that dictates their shape, and the four things handlers are asked to do most: answer synchronously, answer later with `Retain`/`Release`, stream a request body, and stream a response.

## The two handler levels

A connection handler implements `fib.Handler`:

```go
type Handler interface {
	OnOpen(*Connection)
	OnData(*Connection, []byte)
	OnPriorityData(*Connection, []byte)
	OnClose(*Connection, error)
}
```

`fib.HandlerFuncs` implements it from whichever callbacks you set (`Open`, `Data`, `PriorityData`, `Close`), so a raw protocol needs only the parts it uses; an unset one does nothing. `OnPriorityData` receives TCP urgent (out-of-band) bytes and almost every protocol leaves it unset. This complete echo server sets the other three:

```go
package main

import (
	"fmt"

	fib "github.com/lesismal/fib"
)

func main() {
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr: "127.0.0.1:9000",
		Handler: fib.HandlerFuncs{
			Open: func(c *fib.Connection) {
				fmt.Println("open", c.RemoteAddr())
			},
			Data: func(c *fib.Connection, data []byte) {
				// data belongs to this round: Send copies it, so this echo is safe.
				if err := c.Send(data); err != nil {
					c.Close()
				}
			},
			Close: func(c *fib.Connection, err error) {
				fmt.Println("close", c.RemoteAddr(), err)
			},
		},
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
printf 'ping\n' | nc 127.0.0.1 9000   # ping; the server prints the open and the close
```

An HTTP handler implements `fibhttp.Handler` instead: `ServeHTTP(*fibhttp.Context)`, adapted from a function by `fibhttp.HandlerFunc`. The context holds the standard `*http.Request` in `c.Request`, implements `http.ResponseWriter`, and carries the response helpers (`Respond`, `JSON`, `String`, `Data`, `HTML`, …). A binding's HTTP handler stack is `fibhttp.NewHandler(handler)`, as in every other chapter; a raw binding names the `fib.Handler` directly, as above.

## The execution model

Everything a handler does follows from one design decision: a connection is served by **one round at a time**, and each round runs on whichever worker is free. The loop reads readiness and hands the connection to the pool; a worker reads until a short read, runs the protocol (`OnData`, the HTTP parser, the WebSocket frame parser) and your handler, and writes the round's output corked at the end.

For a handler this means:

- `OnData` and an HTTP handler run on a worker, serially per connection, in the order the data arrived. `OnOpen` is the exception: it runs on the event-loop goroutine.
- Blocking in a handler — a sleep, a synchronous RPC, `io.ReadAll` on another socket — occupies a worker and delays every other connection that worker could have served. It does **not** block the event loop.
- `data` in `OnData` is valid only for the duration of the call: `Send` copies it, keeping it does not.
- Responses written during a round are corked and leave in one write (`CoalesceWrites`); writing several responses by hand from another goroutine needs an explicit `Cork`/`Flush`.
- HTTP/2 and HTTP/3 handlers do not run on the goroutine that reads the connection: they go to the engine's handler pool (`Engine.HandlerPool()`), `<Name>-streams`, as gRPC calls and arpc's async handlers do. Concurrent streams are therefore handled concurrently and a blocking handler only delays its own request; `fibhttp.Config.StreamPool.MaxConcurrentHandlers` (and `http3.Config.StreamPool`) bounds how many of one connection's requests run at once; zero, the default, does not.

The goroutine rules, condensed from the [hack guide's cheat sheet](../hacks.md#19-cheat-sheet-what-may-be-called-from-which-goroutine):

| Call | From where |
| --- | --- |
| `Connection.Send` / `SendParts` / `SendFile` / `Close` | any goroutine; never blocks |
| `Connection.Cork` / `Flush` | any goroutine; every `Cork` needs a `Flush` |
| `Context.Respond` / `Write` / `Finish` | any goroutine, before the last `Release` |
| `Context.Retain` / `Release` | any goroutine, paired and no-ops on a finished request |
| `OnData` / `OnClose` | one round at a time per connection; `OnClose` comes after every `OnData` before it |
| `OnOpen` | the event loop: quick work only |

## A synchronous HTTP handler

The default style is the easiest: answer before returning. A handler that returns having written nothing and retained nothing gets an empty `200` — exactly as in `net/http`. `Respond`, `JSON`, `String` and the rest either send the whole response or return an error without sending, so a handler can fall back:

```go
package main

import (
	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		name := c.Query("name") // decoded, and zero-copy when no decoding is needed
		if name == "" {
			name = "world"
		}
		_ = c.JSON(200, map[string]string{"hello": name})
	})

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
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

```sh
curl 'http://127.0.0.1:8080/?name=fib%20dev'   # {"hello":"fib dev"}
curl http://127.0.0.1:8080/                    # {"hello":"world"}
```

A response written piece by piece through the `http.ResponseWriter` methods can be flushed midway; that is how SSE and long downloads stream, and it works the same over HTTP/1, HTTP/2 and HTTP/3:

```go
_, _ = c.WriteString("data: one\n\n")
c.Flush() // send now instead of when the handler returns
_, _ = c.WriteString("data: two\n\n")
```

`Respond`/`WriteResponse` and the `ResponseWriter` methods cannot be mixed in one response: once one style has begun the response, a call of the other returns `fibhttp.ErrResponseWritten` and sends nothing. Pick one style per handler.

`Respond`, `Data` and `WriteResponse` keep the body slice until the response is finished rather than copying it, so a handler must not reuse or change that slice afterwards; `RespondCopy` copies it at the call, for a body built in a scratch or pooled buffer.

## Asynchronous responses: Retain and Release

A handler that cannot answer yet — it waits on a database, an upstream call or a disk — must return the worker, not hold it. The response is reference counted for exactly this: serving the request takes one reference, released when the handler returns, and `Retain` takes one more so the response stays open. When the last reference goes, the response is finished and handed to the connection. `Release` is the way to give a reference back, and the rule is one `Release` per `Retain`:

```go
package main

import (
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		// The handler's own reference is released when it returns. Take one
		// more so that the response stays open while the goroutine works.
		c.Retain()
		q := c.Query("q")
		go func() {
			defer c.Release()                  // the last Release finishes the response
			time.Sleep(100 * time.Millisecond) // stand-in for a slow backend
			_ = c.Respond(200, "text/plain; charset=utf-8", []byte("result for "+q))
		}()
	})

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
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

```sh
curl 'http://127.0.0.1:8080/?q=abc'   # result for abc, after about 100ms
```

Everything the retained request owns — `c.Request`, its header, URL and body, `c.Query`'s zero-copy values — stays valid until the last `Release`; after it, object reuse may hand them to another request. Copy what you must keep, or finish with it before releasing. `c.Async(fn)` is the shorthand: it takes the `Retain` and returns a func that runs `fn` and then releases, deferred, for your own goroutine or pool.

A request can end **before** its response does: the peer closes, a read deadline fires, the body fails. `OnCancel` tells a retained handler once, on the engine's handler pool, so it can stop the upstream work — and it must still release its reference, because cancellation writes nothing but does not give holds back:

```go
var once sync.Once
release := func() { once.Do(c.Release) } // exactly one Release per Retain
c.Retain()
c.OnCancel(func(err error) {
	cancel()  // stop the work the peer is no longer waiting for
	release() // cancellation does not release the hold Retain took
})
go func() {
	defer release()
	_ = c.Respond(200, "application/json", slowQuery())
}()
```

`c.Err()` reports the same reason a cancellation carries, and a plain write to a gone connection returns it rather than corrupting anything. Async is the wrong tool when the work is only a few microseconds — a goroutine and a second wake-up cost more than answering in place — and on HTTP/1 a retained request holds the pipelined ones behind it until it is released.

## Streaming request bodies with OnBody

By default the handler is called once the whole body has arrived, so the server holds all of it in memory, and a body over `MaxBodyBytes` (16 MiB in `fibhttp.DefaultConfig`) is answered `413`. With `Config.StreamRequestBody` the handler runs as soon as the header is in and `OnBody` delivers the body in pieces, in order, one call at a time, with no goroutine per chunk and backpressure for free: if the callback is slower than the peer, the connection stops reading, TCP's window slows the sender down, and the callback itself is the rate limiter.

```go
package main

import (
	"crypto/sha256"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	config := fibhttp.DefaultConfig()
	config.StreamRequestBody = true
	config.StreamRequestBodyThreshold = 0  // stream every body, however small
	config.MaxStreamedBodyBytes = 64 << 30 // bound the streamed body at 64 GiB

	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		h := sha256.New()
		var n int64
		c.OnBody(func(data []byte, fin bool, err error) {
			if err != nil {
				return // connection gone, over the limit, or framing broken
			}
			n += int64(len(data))
			h.Write(data) // data is valid only during this call
			if fin {
				_ = c.String(200, "digest=%x size=%d\n", h.Sum(nil), n)
			}
		})
	})

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandlerWithConfig(config, app),
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
head -c 50000000 /dev/urandom > big.bin
curl --data-binary @big.bin http://127.0.0.1:8080/                          # digest=<sha256> size=50000000
curl -H 'Transfer-Encoding: chunked' --data-binary @big.bin http://127.0.0.1:8080/
curl --http2-prior-knowledge --data-binary @big.bin http://127.0.0.1:8080/  # HTTP/2 streams it too
shasum -a 256 big.bin                                                       # the same digest
```

`OnBody` retains the request itself and the server releases it after the callback's last call, so a handler that answers from the callback needs neither `Retain` nor `Release`. Four things save time later:

- **`Expect: 100-continue` is lazy.** `100 Continue` goes out the first time the handler asks for the body, so checking `c.Request.ContentLength` and answering `413` before registering `OnBody` refuses an upload without reading a byte.
- **Streamed bodies are bound by the read timeouts.** `ReadBodyTimeout` (5 minutes in `fibhttp.DefaultConfig`, from the end of the header) and `ReadTimeout` (off by default, from the request's first byte) both run while the body is still arriving, and the connection is closed when one fires. Raise them for slow, large uploads, or lift them for one request with `c.SetReadDeadline(time.Time{})`.
- **Don't block in the callback.** It runs on the connection's worker; hand slow work to your own pool and take an extra `Retain`, release it yourself.
- **`Request.Body` is not readable once `OnBody` took the body over**, and a streamed request never reuses its `Request`, header or URL.

## Pitfalls

- **A handler must not block.** Waiting on a lock, a peer or slow I/O holds a worker; use `Retain` plus a callback, or a pool of your own. See the anti-patterns in [Hacks §20](../hacks.md#20-anti-patterns).
- **Pair every `Retain` with exactly one `Release`.** The error branch, the cancel branch and the panic branch included; a missing `Release` pins the connection, an extra one ends the response early.
- **Don't touch `c`, `c.Request` or `c.Body()` after the last `Release`.** With object reuse on (the default) they belong to another request, and nothing panics.
- **`data` is borrowed.** `OnData`, `OnBody` and WebSocket payloads are valid only during the call; copy before keeping.
- **A retained HTTP/1 request holds the pipelined ones behind it** until it is released, because responses must stay in order. HTTP/2 and HTTP/3 streams do not wait for each other.
- **Cancellation is not a release.** `OnCancel` says the request is gone; the holds it took still have to be given back.
- **Answer from `OnBody` at `fin`.** A response sent from the callback before `fin` goes out at once and gives up the rest of the body: the callback's last call then carries an error (`io.ErrUnexpectedEOF` on HTTP/1, whose connection closes after the response; on HTTP/2 an error saying the body is no longer being read). That is the way to refuse an upload part-way, not to answer a complete one.
- **Don't read `Context` in another goroutine without retaining it first.** The zero-copy getters are the common trap.
- **`Respond` and the `ResponseWriter` methods are exclusive.** Whichever begins the response wins; a call of the other style returns `ErrResponseWritten`. Choose per handler.
- **`Respond` keeps its body slice.** Don't reuse a buffer handed to `Respond` until the response is finished; use `RespondCopy` for scratch buffers.

## See also

- [Hacks §1–10](../hacks.md) — async responses, `OnBody`, cancellation, pipelining, object reuse, zero-copy, `HoldReads`, the write side, `Read`
- [Engine and configuration](02-engine.md) — pools, the handler pool and its sizing
- [Router](04-router.md) — routing the HTTP handlers this chapter writes
- [HTTP/1.x](06-http1.md) and [HTTP/2](07-http2.md) — the protocol details behind these handlers
- [Raw TCP, UDP and Unix sockets](19-raw-io.md) — the full `Connection` API
- [The README](../../README.md) — the standard-library `http.Handler` compatibility notes

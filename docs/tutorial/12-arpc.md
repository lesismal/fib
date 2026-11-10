# arpc

[English](12-arpc.md) | [简体中文](12-arpc.zh-CN.md)

arpc is a compact two-way RPC protocol with the API of
[lesismal/arpc](https://github.com/lesismal/arpc), served by fib's engine: two peers share one
connection, and either of them calls the other's methods, sends one-way notifies, opens streams
and broadcasts to every connection. Its frames are lean — a 16-byte header, a method name and a
payload — and the wire format is the original package's, so a fib endpoint
talks to a `lesismal/arpc` endpoint. This chapter covers the server, the client, where handlers
run, the Context, streams, middleware, coders, TLS and reconnection. It assumes the handler
model from chapter 03: a connection is served one round at a time on a worker, so a handler must
not block.

## The model

- **The server is a `fib.Handler`.** `arpc.NewServer()` returns a value to put in a
  `fib.Binding`, or to wrap with `fibtls.NewServer` for TLS. `Server.Handler` holds the method
  table, the middlewares, the coders and the callbacks, and every accepted connection shares it.
- **A client is one connection, on either side.** `arpc.Dial` / `arpc.NewClient` open a dialed
  client, which reconnects on its own when the connection breaks; a `Server` makes an
  `*arpc.Client` for every connection it accepts. Both sides can call and serve the peer.
- **Handlers run away from the read path by default.** A method registered after
  `Handler.SetAsyncResponse(true)`, which is the default, runs on the engine's task pool — the
  same pool HTTP/2 and HTTP/3 handlers use — so a slow handler does not hold up its connection.
  `SetAsyncResponse(false)`, or the optional `bool` of `Handle` / `HandleStream`, runs a handler
  where its connection is read, one message at a time; that is the only mode with a defined
  order, and calling the same peer and waiting for the answer from it deadlocks.
- **The wire is fixed.** A 16-byte little-endian header, then the method name, then the payload:

```text
[0, 4)  body length: len(method) + len(payload)
[4]     reserved for the application's flag bits (Message.SetFlagBit)
[5]     command in the low six bits; a stream's EOF and local bits above
[6]     flags: error response 0x01, asynchronous call 0x02
[7]     method length
[8, 16) sequence number
```

`Codec` turns a Go value into the payload; it is `JSONCodec` unless a server or client says
otherwise, and `[]byte`, `string` and `error` values pass through it untouched.

## A server and a client

An echo server on a fib binding, then a client with an engine of its own:

```go
package main

import (
	"log"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/arpc"
)

func main() {
	server := arpc.NewServer()
	server.Handler.Handle("/echo", func(ctx *arpc.Context) {
		var msg string
		if err := ctx.Bind(&msg); err != nil {
			_ = ctx.Error(err)
			return
		}
		_ = ctx.Write(msg)
	})

	srv, err := fib.New(fib.DefaultConfig(), fib.Binding{Addr: "127.0.0.1:8888", Handler: server})
	if err != nil {
		panic(err)
	}
	defer srv.Close()
	go func() { _ = srv.Run() }()

	clientEngine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer clientEngine.Close()
	go func() { _ = clientEngine.Run() }()

	client, err := arpc.Dial(clientEngine, "tcp", "127.0.0.1:8888", 3*time.Second, nil)
	if err != nil {
		panic(err)
	}
	defer client.Stop()

	var rsp string
	if err := client.Call("/echo", "hello arpc", &rsp, time.Second); err != nil {
		panic(err)
	}
	log.Println(rsp)
}
```

`Call` encodes the request with the client's codec, sends it and waits for the response,
decoding it into `rsp`; an error response comes back as a non-nil `error`. `Dial` takes a
`*arpc.Handler` as its last argument — nil means a clone of `arpc.DefaultHandler` — and returns
only once the connection is up. `Server.Run(addr)` and `Server.Serve(config, addr)` are the
shortcuts when the caller wants the server to make its own engine.

## Handlers and Context

Every request or notify reaches a `HandlerFunc` as an `*arpc.Context`. The Context carries the
message, the client it came on, and the decoder:

```go
type addReq struct{ A, B int }
type addRsp struct{ Sum int }

server.Handler.Handle("/add", func(ctx *arpc.Context) {
	var req addReq
	if err := ctx.Bind(&req); err != nil {
		_ = ctx.Error(err) // arrives at the caller as the Call error
		return
	}
	_ = ctx.Write(addRsp{Sum: req.A + req.B})
	ctx.Set("served-by", "add") // values stay on this side; they are never sent
})

server.Handler.Handle("/bytes", func(ctx *arpc.Context) {
	_ = ctx.Write(ctx.Body()) // a []byte payload goes back exactly as it came
})
```

On the client, the same decoding rules apply in the other direction:

```go
var sum addRsp
if err := client.Call("/add", addReq{A: 2, B: 3}, &sum, time.Second); err != nil {
	panic(err)
}
var raw []byte
if err := client.Call("/bytes", []byte{0x00, 0x01, 0x02}, &raw, time.Second); err != nil {
	panic(err)
}
```

`Bind` gives a `*[]byte` the payload itself, a `*string` a copy, and anything else is decoded by
the codec. `Write` with an `error` sends an error response, and so does `Error`. A handler may
also register a whole service with `Handler.Register`: an exported method
`func(ctx context.Context, req *Req, rsp *Rsp)` is routed as `name.Method`, with the request
bound and the response written by reflection, while a `func(*arpc.Context)` method is registered
as it is:

```go
type Greeter struct{}

func (Greeter) Hello(ctx context.Context, req *addReq, rsp *addRsp) {
	rsp.Sum = req.A + req.B
}

if err := server.Handler.Register("math", Greeter{}); err != nil {
	panic(err) // the "math.Hello" route now answers calls
}
```

## Async calls and notifies

`Call` blocks the caller. `CallAsync` sends the request and returns at once; its callback runs
once, with the response where the connection is read, or with an error on the task pool.
`Notify` sends a message that expects no response at all:

```go
package main

import (
	"log"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/arpc"
)

func main() {
	server := arpc.NewServer()
	server.Handler.Handle("/slow", func(ctx *arpc.Context) {
		time.Sleep(100 * time.Millisecond) // stand-in for real work
		_ = ctx.Write(ctx.Body())
	})
	server.Handler.Handle("/event", func(ctx *arpc.Context) {
		log.Printf("event: %s", ctx.Body())
	})

	srv, err := fib.New(fib.DefaultConfig(), fib.Binding{Addr: "127.0.0.1:8889", Handler: server})
	if err != nil {
		panic(err)
	}
	defer srv.Close()
	go func() { _ = srv.Run() }()

	clientEngine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer clientEngine.Close()
	go func() { _ = clientEngine.Run() }()

	client, err := arpc.Dial(clientEngine, "tcp", "127.0.0.1:8889", 3*time.Second, nil)
	if err != nil {
		panic(err)
	}
	defer client.Stop()

	done := make(chan struct{})
	if err := client.CallAsync("/slow", "async request", func(ctx *arpc.Context, err error) {
		defer close(done)
		if err != nil {
			log.Printf("async call failed: %v", err) // ErrTimeout, ErrClientReconnecting, ...
			return
		}
		var rsp string
		if err := ctx.Bind(&rsp); err != nil {
			log.Printf("bind: %v", err)
			return
		}
		log.Printf("async response: %s", rsp)
	}, 2*time.Second); err != nil {
		panic(err)
	}
	if err := client.Notify("/event", "fired once", time.Second); err != nil {
		panic(err)
	}
	<-done
}
```

A notify handler that calls `ctx.Write` gets `ErrContextResponseToNotify`; the peer is not
waiting for anything.

## Streams

A stream is a two-way sequence of messages over one client, bound to a method and opened from
the client side. The server registers it with `HandleStream`, and the client opens it with
`NewStream`; each side closes its sending and its receiving half separately, and `Recv` returns
`io.EOF` once the peer closed its sending half:

```go
server.Handler.HandleStream("/upper", func(st *arpc.Stream) {
	defer st.CloseSend()
	for {
		var msg string
		if err := st.Recv(&msg); err != nil {
			return // io.EOF when the client closed its sending half
		}
		if err := st.Send(strings.ToUpper(msg)); err != nil {
			return
		}
	}
})

st := client.NewStream("/upper")
defer st.CloseSend()
if err := st.Send("hello"); err != nil {
	panic(err)
}
var upper string
if err := st.Recv(&upper); err != nil {
	panic(err)
}
```

Stream messages carry the stream command on the wire, so neither side confuses them with calls.
`Handler.SetStreamQueueSize` bounds how many messages a stream holds for its handler; a
connection whose stream is full stops reading until the handler takes one.

## Broadcast and lifecycle callbacks

Every accepted connection is a `*arpc.Client` the server keeps. `Broadcast` sends a notify to
all of them, `BroadcastWithFilter` to those a callback picks, and `ForEach` visits them:

```go
server.Handler.HandleConnected(func(c *arpc.Client) {
	log.Printf("joined: %v", c.Conn().RemoteAddr())
})
server.Handler.HandleDisconnected(func(c *arpc.Client) {
	log.Print("left")
})

server.Broadcast("/tick", time.Now().UnixMilli())
server.BroadcastWithFilter("/tick", "vip only", func(c *arpc.Client) bool {
	plan, _ := c.Get("plan") // values a handler or callback stored on the client
	return plan == "vip"
})
```

The callbacks run on the task pool: `HandleConnected` runs when a server accepts a connection or
a dialed client reconnects, so the connection's first messages may already be handled before it
returns, and `HandleDisconnected` runs when the client stops. `Server.MaxLoad` bounds how many
connections a server serves; one beyond it is closed as it is accepted. `Server.Accepted` and
`Server.CurrLoad` report the counters.

## Middleware and where handlers run

`Handler.Use` adds a middleware to every method, in the order the chain runs it. The chain is
assembled per method: a middleware registered before a method runs before its handler, one
registered after it runs after the handler. A middleware may call `ctx.Next()` to run the rest of
the chain and then do something, or `ctx.Abort()` to stop it:

```go
server.Handler.Use(func(ctx *arpc.Context) {
	start := time.Now()
	ctx.Next() // the rest of the chain: the method handler and later middlewares
	log.Printf("%s took %v", ctx.Message.Method(), time.Since(start))
})
server.Handler.Use(func(ctx *arpc.Context) {
	if string(ctx.Body()) == "deny" {
		_ = ctx.Error("denied by middleware")
		ctx.Abort()
	}
})
server.Handler.Handle("/echo", func(ctx *arpc.Context) { _ = ctx.Write(ctx.Body()) })

server.Handler.SetAsyncResponse(false) // from here on: run where the connection is read
server.Handler.Handle("/count", func(ctx *arpc.Context) { /* must not block */ }, true)
```

The default is asynchronous: a handler runs on `Handler.TaskPool()`, a pool the caller installs
with `Handler.SetTaskPool`, or else the engine's `HandlerPool`. `SetAsyncResponse(false)`, a
per-method `false`, or `HandleStream(..., false)` keeps a handler on the connection's read path.
Async handlers of one connection may run concurrently and out of order; choose synchronous for a
method whose calls must be handled in arrival order.

## Coders and TLS

A `MessageCoder` transforms messages on the wire — to compress, encrypt or sign them. Coders run
on what is sent in the order they were added, and on what is received in the opposite order, so a
symmetric coder is installed on both sides:

```go
type xorCoder struct{ key byte }

func (c xorCoder) apply(m *arpc.Message) *arpc.Message {
	for i := arpc.HeadLen + m.MethodLen(); i < len(m.Buffer); i++ { // payload only
		m.Buffer[i] ^= c.key
	}
	return m
}
func (c xorCoder) Encode(_ *arpc.Client, m *arpc.Message) *arpc.Message { return c.apply(m) }
func (c xorCoder) Decode(_ *arpc.Client, m *arpc.Message) *arpc.Message { return c.apply(m) }

server.Handler.UseCoder(xorCoder{0x5a})
client.Handler.UseCoder(xorCoder{0x5a})
```

TLS is the `tls` package in front of the same handler, and `TLSDialer` on the client:

```go
engine, err := fib.New(fib.DefaultConfig(),
	fib.Binding{Addr: "127.0.0.1:8443", Handler: fibtls.NewServer(serverTLS, server)})
if err != nil {
	panic(err)
}
client, err := arpc.NewClient(
	arpc.TLSDialer(engine, "tcp", "127.0.0.1:8443", 3*time.Second, clientTLS), nil)
```

The wire inside TLS is unchanged, so an arpc endpoint can be reached by any TLS dialer that
trusts the certificate.

## Reconnection

A dialed client reconnects by itself after the connection breaks, once per `ReconnectInterval`
until `MaxReconnectTimes` says to stop (zero or less, the default, means forever). While there is
no connection, calls fail with `ErrClientReconnecting`, and `CheckState` reports it. The
callbacks describe the process:

```go
handler := arpc.NewHandler()
handler.SetReconnectInterval(time.Second)
handler.SetMaxReconnectTimes(0) // 0: never give up
handler.HandleReconnect(func(c *arpc.Client, info *arpc.ReconnectInfo) {
	log.Printf("attempt %d/%d to %s: success=%v err=%v",
		info.Times, info.MaxTimes, info.Addr, info.Success, info.Err)
})
client, err := arpc.NewClient(
	arpc.TCPDialer(engine, "tcp", "127.0.0.1:8888", 3*time.Second), handler)
if err != nil {
	panic(err)
}
defer client.Stop()
client.Keepalive(30 * time.Second) // ping the peer so a dead path is noticed
```

A client that reconnects has not stopped: `HandleDisconnected` is not called for it. `Stop`
closes the connection and ends the reconnection; a stopped client returns `ErrClientStopped`.
Stop every client before closing its engine, which drops connections without telling them.

## Wire compatibility with lesismal/arpc

The header, the commands, the method and payload layout, the stream bits and the error and async
flags are the original package's, so a `lesismal/arpc` client talks to a fib `arpc.Server` and a
fib client talks to a `lesismal/arpc` server. The differences are in the send path and in the
options that no longer exist:

- fib sends through `fib.Connection`'s nonblocking `Send`, whose queue and watermarks bound what
  waits for the socket. There is no send queue, no send goroutine and nothing to tune: the batch,
  writev, send-queue and buffer-size options are gone, along with the reader options.
- A message goes to the connection at once, so the timeouts of `Notify`, `PushMsg` and
  `Context.WriteWithTimeout` bound nothing.
- A `Client` has no `Restart`: a dialed one reconnects on its own.
- `Handler` keeps the rest of the API: method registration, middleware, coders, service
  registration, singleflight (`Handler.Singleflight`), the connection callbacks and the task
  pool.

## Pitfalls

- **Do not call the same peer and wait inside a synchronous handler.** A handler run on the read
  path blocks its connection's reads, so the peer's answer is never read and the call times out.
  Leave the default asynchronous mode for such a method, or call from another connection.
- **Do not keep a `*arpc.Context` after the handler returns.** With `Handler.EnablePool` it goes
  back to a pool, and a `*[]byte` from `Bind` is only valid while its message lives. Copy what
  you need, or `Message.Retain` it.
- **Async handlers have no order.** Two calls handled asynchronously may complete in either
  order; if the method needs arrival order, register it with `false`.
- **A notify is not a call.** A notify handler has no response to write — `ctx.Write` returns
  `ErrContextResponseToNotify` — and a request handler that writes nothing leaves its caller
  waiting until the call times out.
- **Limits are enforced by closing the connection.** A method name longer than `MaxMethodLen`
  (127) or a body longer than `Handler.MaxBodyLen` (`DefaultMaxBodyLen`, 64 MiB − 16) closes the
  connection; raise the limit with `SetMaxBodyLen`.
- **Configure the Handler before clients use it.** `Handler` methods and middlewares are not safe
  to change while a client is serving, and a `Server` shares one Handler among all its
  connections.
- **A call timeout does not cancel the server's handler.** The response arrives later and is
  reported to `HandleSessionMiss`; the work was still done.
- **Stop the client before its engine closes.** Closing an engine ends its connections without a
  word to their handlers, so asynchronous calls get no callback.

## See also

- The [`arpc` package](../../arpc) and its package doc, the full API reference.
- The root README's [arpc section](../../README.md#arpc).
- [Chapter 11: gRPC](11-grpc.md) for the other RPC protocol, and [chapter 13: SOCKS and HTTP
  CONNECT](13-socks-connect.md) for proxying and tunneling.
- [Chapter 15: Multiprotocol ports](15-multiprotocol.md) for arpc on a shared port, and
  [chapter 20: Clients](20-clients.md) for client lifecycles.
- [docs/hacks.md](../hacks.md) for the async-response, cork and pool tricks that apply to arpc
  handlers too.

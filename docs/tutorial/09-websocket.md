# WebSocket

[English](09-websocket.md) | [简体中文](09-websocket.zh-CN.md)

`fib/websocket` implements RFC 6455 on top of a fib connection: a `websocket.ServerHandler` performs
the opening handshake, parses frames as bytes arrive and calls your `Handler` with whole messages,
while the `Connection` it hands you sends messages, pings and close frames from any goroutine. The
same handler serves WebSocket on a binding of its own or upgrades one request of an HTTP/1.1,
HTTP/2 or HTTP/3 server, and the client side dials `ws://` and `wss://` without blocking the
caller. This chapter assumes the handler model of [03-handlers.md](03-handlers.md) and the HTTP
servers of [06-http1.md](06-http1.md) and [07-http2.md](07-http2.md).

## The model

- **One handler, two entries.** `websocket.NewHandler(handler)` returns a `fib.Handler` that speaks
  WebSocket on the connection it is bound to. `Upgrade` is the other entry: it turns the request a
  package `http` handler is serving into a WebSocket, so WebSocket and plain HTTP share a port.
- **Callbacks are serial.** For one connection, the opening handshake and then every message and
  close callback run one after another — the handshake in `OnOpen` on the event loop, the rest on
  whichever worker scheduled that round. Two connections' callbacks may run at the same time on
  different workers, so state shared by connections needs a lock.
- **Payloads are borrowed.** `OnMessage` and `OnFrame` receive a slice that is valid only for the
  duration of the callback; copy it before retaining it.
- **Writes are safe and never block.** `WriteMessage`, `WriteText`, `WriteBinary`,
  `WritePreparedMessage`, `Ping`, `Pong` and `Close` may be called from application goroutines. A
  send queues the frame and returns; a peer that never reads grows its queue, so close such a peer
  yourself.
- **Limits.** `Config.MaxMessageBytes` (16 MiB in `websocket.DefaultConfig()`) bounds a message after
  reassembly; control frames do not count, and an invalid Text payload closes the connection with
  1007. A ping is answered with a pong carrying the same payload automatically, but a custom
  `HandlerFuncs.Ping` replaces that reply, so it must call `Connection.Pong` itself.

## Echo server on its own binding

Binding the WebSocket handler directly gives a server that speaks nothing else:

```go
package main

import (
	"log"
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/websocket"
)

func main() {
	handler := websocket.NewHandler(websocket.HandlerFuncs{
		Open: func(c *websocket.Connection, r *stdhttp.Request) {
			log.Printf("open from %s", r.RemoteAddr)
		},
		Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
			_ = c.WriteMessage(op, data)
		},
		Close: func(c *websocket.Connection, code uint16, reason string, err error) {
			log.Printf("close %d %q: %v", code, reason, err)
		},
	})
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: handler,
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

`HandlerFuncs` is the struct that saves you from writing six methods: `Open`, `Message`, `Close`,
`Frame`, `Ping` and `Pong`. A nil `Ping` keeps the default pong reply; a nil `Pong` discards pongs.
`Connection` writes what the client reads here: `WriteMessage` takes any data opcode, while
`WriteText` and `WriteBinary` are the two shorthands, and `Close(code, reason)` sends a close frame.
Test it with any client, for example `websocat ws://127.0.0.1:8080/`, and every line comes back.

## Upgrading from an HTTP server

A WebSocket usually shares a port with the site that opens it. Route the path to
`ServerHandler.Upgrade` from inside the HTTP handler; it writes the handshake response and returns
the `*websocket.Connection`, which the same `Handler` serves from then on:

```go
ws := websocket.NewHandler(websocket.HandlerFuncs{
	Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
		_ = c.WriteMessage(op, data)
	},
})
app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
	if c.Request.URL.Path == "/ws" {
		_, _ = ws.Upgrade(c, nil) // the header argument adds response headers; nil sends none
		return
	}
	_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte("hello\n"))
})
engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
	Addr:    "127.0.0.1:8080",
	Handler: fibhttp.NewHandler(app),
})
```

What `Upgrade` does depends on the request's own protocol. Over HTTP/1.1 it is RFC 6455's opening
handshake and the whole connection switches to WebSocket. Over HTTP/2 and HTTP/3 it is an extended
CONNECT whose `:protocol` is `websocket` (RFC 8441, RFC 9220): only that stream switches, the
connection carries on serving other requests, and package `http` sends the SETTINGS that invite a
browser to try it. The `Config`'s origin check, subprotocols and compression apply either way.
Return from the HTTP handler as soon as `Upgrade` returns; the connection belongs to the WebSocket
handler now.

## Broadcast with prepared messages

A fan-out message should be checked and framed once, not once per connection. `NewPreparedMessage`
does that; it is immutable and safe for concurrent use:

```go
package main

import (
	"log"
	stdhttp "net/http"
	"sync"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/websocket"
)

func main() {
	var (
		mu    sync.Mutex
		conns = map[*websocket.Connection]struct{}{}
	)
	ws := websocket.NewHandler(websocket.HandlerFuncs{
		Open: func(c *websocket.Connection, r *stdhttp.Request) {
			mu.Lock()
			conns[c] = struct{}{}
			mu.Unlock()
			_ = c.WriteText("welcome\n")
		},
		Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
			m, err := websocket.NewPreparedMessage(op, data)
			if err != nil {
				return
			}
			mu.Lock()
			targets := make([]*websocket.Connection, 0, len(conns))
			for conn := range conns {
				targets = append(targets, conn)
			}
			mu.Unlock()
			for _, conn := range targets {
				_ = conn.WritePreparedMessage(m)
			}
		},
		Close: func(c *websocket.Connection, code uint16, reason string, err error) {
			mu.Lock()
			delete(conns, c)
			mu.Unlock()
		},
	})
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		if c.Request.URL.Path == "/ws" {
			_, _ = ws.Upgrade(c, nil)
			return
		}
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte("hello\n"))
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
		log.Printf("run: %v", err)
	}
}
```

Three details make the loop cheap. The prepared message is validated and framed once, and
compressed at most once for each permessage-deflate window its receivers agreed; every connection
then queues the *same* frame instead of a copy each, so a slow peer costs its queue, not a frame
copy. Sends never block, so one goroutine looping over the connections is enough — split the fan-out
across a few goroutines only for very many connections or under TLS, which seals per connection. And
the map is shared by callbacks that run on different workers, so it needs the mutex.

## Receiving frame by frame

`OnMessage` reassembles a message before calling you, which means a large upload lives in memory
first. Setting `HandlerFuncs.Frame` instead delivers each data frame as it arrives, with nothing
held between frames:

```go
var frameBuf bytes.Buffer // or a file, a hash, a decoder: any writer
handler := websocket.NewHandler(websocket.HandlerFuncs{
	Frame: func(c *websocket.Connection, op websocket.Opcode, fin bool, payload []byte) {
		_, _ = frameBuf.Write(payload) // payload is valid only during this call
		if fin {
			log.Printf("message of %d bytes", frameBuf.Len())
			frameBuf.Reset()
		}
	},
})
```

With `Frame` set, `Message` is not called for Text or Binary messages. Every frame carries the
message's own opcode, never `Continuation`, and `fin` is true on the last one; an unfragmented
message is one call with `fin` true. Because nothing accumulates between frames, a peer may stream a
message far beyond `MaxMessageBytes`, which then bounds a single frame rather than the message. The
exception is a compressed message: its frames are pieces of one DEFLATE stream, so it is reassembled
and delivered as one `fin` call. The same switch, and the same reasoning, is in the hack guide's
section on receiving by frame ([../hacks.md](../hacks.md)).

## Compression and handshake options

The handshake is configured with `websocket.Config`, passed to `NewHandlerWithConfig`; its `HTTP`
field names the package `http` settings used to answer a bad handshake:

```go
config := websocket.Config{
	MaxMessageBytes:   4 << 20,
	EnableCompression: true,
	CheckOrigin: func(r *stdhttp.Request) bool {
		return r.Header.Get("Origin") == "https://example.com"
	},
	Subprotocols: []string{"chat.v2", "chat.v1"},
}
handler := websocket.NewHandlerWithConfig(config, websocket.HandlerFuncs{
	Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
		_ = c.WriteMessage(op, data)
	},
})
```

`EnableCompression` accepts a client's permessage-deflate offer (RFC 7692); afterwards messages are
sent compressed and may arrive compressed, and `MaxMessageBytes` bounds the message once
decompressed. The sender keeps no deflate context — each message borrows a compressor from a
`sync.Pool` — so memory stays bounded with many connections. `CheckOrigin` is the cross-site
WebSocket hijacking guard: a nil one means `CheckSameOrigin`, which accepts a request without an
`Origin` header or with an `Origin` naming the request's host, and refuses a browser page from
another site with 403; accepting every origin is a conscious choice. The server picks a subprotocol
from `Subprotocols` during the handshake, reported by `Connection.Subprotocol()`.

## The client

A `websocket.Dialer` dials through an engine and reports the outcome in a callback, or through a
`DialFuture` for callers that would rather wait. The engine must be running; it may be a client-only
engine with no bindings:

```go
package main

import (
	"log"
	stdhttp "net/http"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/websocket"
)

func main() {
	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	go func() { _ = engine.Run() }()

	done := make(chan struct{})
	handler := websocket.HandlerFuncs{
		Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
			log.Printf("echo: %s", data)
			_ = c.Close(websocket.CloseNormal, "done")
			close(done)
		},
		Close: func(c *websocket.Connection, code uint16, reason string, err error) {
			log.Printf("closed %d %q", code, reason)
		},
	}
	dialer := websocket.NewDialer(engine, websocket.DefaultDialerConfig())
	dialer.Dial("ws://127.0.0.1:8080/ws", nil, handler,
		func(c *websocket.Connection, r *stdhttp.Response, err error) {
			if err != nil {
				log.Printf("dial: %v", err)
				return
			}
			_ = c.WriteText("hello from fib")
		})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Println("timed out")
	}
}
```

Once the handshake completes, a client connection is served exactly as a server one is: the same
`Handler` callbacks run on the engine's workers, and the same `Connection` methods mask what the
client sends as RFC 6455 requires. `DialerConfig` bounds the handshake (`HandshakeTimeout`; zero
means no timeout), limits incoming messages (`MaxMessageBytes`), offers subprotocols, turns on
compression, sets the `crypto/tls` config used for `wss://`, or replaces the dial itself with a
`Dial` hook such as a SOCKS proxy's. `Go` returns a `DialFuture` instead of taking a callback:
`future := dialer.Go(url, nil, handler)`, then `conn, resp, err := future.Wait()`.

## Pitfalls

- **A WebSocket-only binding cannot answer plain HTTP.** Use `Upgrade` from the HTTP handler when
  the same port must serve both, and return from the HTTP handler immediately after it.
- **Over HTTP/2 and HTTP/3 only the stream switches.** Other requests on the connection continue,
  which is the point of RFC 8441/9220, but it also means the request's handler must not touch the
  connection after `Upgrade`.
- **The message payload is borrowed.** Copy it before appending it to a ring, a broadcast buffer or
  a channel; the slice dies with the callback.
- **Sends never block.** A peer that stops reading accumulates frames in its queue. Close slow
  peers yourself; setting `MaxWriteQueueBytes` or `MaxPendingBytes` on the engine (chapter
  [02-engine.md](02-engine.md)) is the blunt version.
- **A custom `Ping` suppresses the default pong.** Call `c.Pong(payload)` when you still want the
  RFC 6455 reply.
- **A nil `CheckOrigin` is not "accept everything".** It is `CheckSameOrigin`; a browser page from
  another site is refused with 403.

## See also

- [Handlers: sync and async](03-handlers.md) — the connection callbacks and why they must not block
- [HTTP/2](07-http2.md) — extended CONNECT, the transport behind WebSocket over h2
- [TLS](10-tls.md) — `wss://`, certificates and the ALPN stack in front of an HTTP server
- [Clients](20-clients.md) — the WebSocket client beside the HTTP clients
- [The hack guide](../hacks.md) — receiving by frame, prepared messages and other tricks
- [The README](../../README.md) — the WebSocket feature summary and the Autobahn CI run

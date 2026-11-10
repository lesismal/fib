# Multiprotocol ports

[English](15-multiprotocol.md) | [简体中文](15-multiprotocol.zh-CN.md)

One port can serve HTTPS, h2c, SOCKS and an RPC protocol at once if the server can tell
from the client's first bytes which one it is speaking. Package `mux` does that: it holds
an ordered table of routes, reads until one matcher claims the connection, and then hands
the connection to that route's handler for good. Package `multiproto` builds that table
from the protocols you name, and adds the extra reading — a whole request head for a
WebSocket handshake, the first HTTP/2 headers for gRPC — that those need. The chapter
assumes you know `fib.Binding` from [02-engine.md](02-engine.md) and the HTTP handler from
[06-http1.md](06-http1.md).

## The model

A `mux.Handler` is a `fib.Handler` that serves a connection only until it knows the
protocol. On open it sets a read deadline; on every read it asks each matcher in order
what the bytes so far are; and when a route matches it calls `Connection.SetHandler` with
that route's handler, then runs the handler's `OnOpen` and hands it every byte the mux
read. From then on the engine calls the chosen handler directly — no dispatch in between —
and asks that handler, not the mux, about `OnHangup` and `OnShutdown`, so the protocol
handler keeps its normal lifecycle and stops only when the engine shuts down.

A matcher is a `mux.Matcher`, a `func(input []byte) mux.Result` returning `mux.NoMatch`, `mux.NeedMore` or
`mux.Match`. It runs on the connection's worker, must not block, and must not keep the
input slice, which the engine recycles after the round. `NoMatch` rules a route out for
good; `NeedMore` keeps it waiting for the next read; the mux asks again with everything
received so far until a route matches or `Config.MaxPeek` bytes have arrived.

The one rule every matcher inherits: **the client must speak first**. TLS, HTTP, SOCKS and
RPC protocols do; SMTP, FTP and MySQL wait for the server's greeting, so there is nothing
to sniff. Such a protocol needs its own `fib.Binding`, and a silent peer that reaches the
mux is closed when `Config.Timeout` passes, which is 10 seconds unless you change it.

## One port with `mux`

The routes are tried in order of priority. TLS and HTTP cannot be confused by their first
byte, but TLS comes first because a ClientHello is the least ambiguous start; SOCKS sits
beside them; and the last route is a toy text protocol that opens with `PING`. A connection
no route recognises is closed with `mux.ErrNoMatch`, since this handler has no `Fallback`:

```go
package main

import (
	"fmt"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/mux"
	"github.com/lesismal/fib/socks"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

func main() {
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}

	web := fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_, _ = fmt.Fprintf(c, "hello over %s\n", c.Request.Proto)
	}))
	pinger := fib.HandlerFuncs{
		Data: func(c *fib.Connection, b []byte) {
			_ = c.Send(append([]byte("PONG "), b...))
		},
	}

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr: "127.0.0.1:8000",
		Handler: mux.New(
			// TLS clients: HTTPS and h2, told apart by ALPN inside.
			mux.Route{Match: mux.TLS(), Handler: fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), web)},
			// Clear HTTP/1.x and h2c, which one fib/http handler serves.
			mux.Route{Match: mux.HTTP(), Handler: web},
			mux.Route{Match: mux.SOCKS(), Handler: socks.NewServer(socks.Config{})},
			mux.Route{Match: mux.Prefix("PING"), Handler: pinger},
		),
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
curl -k https://127.0.0.1:8000/                       # TLS route: hello over HTTP/2.0 (ALPN)
curl http://127.0.0.1:8000/                           # HTTP route: hello over HTTP/1.1
curl --http2-prior-knowledge http://127.0.0.1:8000/   # HTTP route, h2c: hello over HTTP/2.0
curl --socks5 127.0.0.1:8000 https://example.com/     # SOCKS route, out to the internet
printf 'PING' | nc -w 1 127.0.0.1 8000                # Prefix route: PONG PING
printf 'XYZ' | nc -w 1 127.0.0.1 8000                 # no route: closed at once
```

The handler stack inside a route is the ordinary one: `fibtls.NewServer` runs the
handshake and hands the connection to the HTTP handler, and `fibhttp.ConfigureTLS` is what
makes a TLS client choose HTTP/2. The mux knows none of that; it only knows that the first
byte was a TLS record. A route whose `Match` is a looser test than the handler's protocol
is a bug: the handler gets the connection after the mux has committed, so it fails the
handshake or parses garbage rather than falling through to a later route.

TLS is one family to the mux: `mux.TLS()` decides at the third byte of the ClientHello,
before the SNI has even been parsed, so no route can be chosen by host name. SNI is the
TLS layer's business — one TLS handler picks a certificate per name with
`Config.GetCertificate`, and `fibhttp.ConfigureTLS` picks HTTP/1.1 or HTTP/2 by ALPN:

```go
serverTLS := &stdtls.Config{
	GetCertificate: func(hello *stdtls.ClientHelloInfo) (*stdtls.Certificate, error) {
		cert, ok := certs[hello.ServerName] // one certificate per host name
		if !ok {
			return nil, fmt.Errorf("no certificate for %q", hello.ServerName)
		}
		return &cert, nil
	},
}
```

A handler that needs genuinely different stacks per name implements `tls.HandshakeHandler`:
its `OnHandshake` receives the negotiated `stdtls.ConnectionState` (SNI, ALPN) before any
plaintext arrives, and can dispatch its inner handler from there.

## Matchers

`mux` ships matchers for the protocols that are common on a shared port:

| Matcher | Recognises |
| --- | --- |
| `mux.TLS()` | a TLS handshake record, every version from TLS 1.0 up |
| `mux.HTTP()` | `mux.HTTP1()` and `mux.HTTP2()` |
| `mux.HTTP1()` | a request line beginning with an HTTP or WebDAV method |
| `mux.HTTP2()` | the HTTP/2 connection preface, h2c with prior knowledge |
| `mux.SOCKS()` | `mux.SOCKS5()` and `mux.SOCKS4()` |
| `mux.SOCKS5()` | a SOCKS5 greeting: version 5 and a non-zero method count |
| `mux.SOCKS4()` | a SOCKS4 or SOCKS4a CONNECT or BIND request |
| `mux.Prefix(...)` | a protocol's magic number or first command; it answers `NeedMore` while the input is still a prefix of one |
| `mux.Any()` | everything, for a last route or a fallback |
| `mux.AnyOf(...)` | the union of other matchers |

`mux.Prefix` is the usual escape hatch for a protocol with a magic number. A matcher for
one that sends a recognisable first command instead is a few lines:

```go
// resp matches a client that opens with a RESP array, as redis-cli does.
func resp(input []byte) mux.Result {
	if len(input) == 0 {
		return mux.NeedMore
	}
	if input[0] != '*' {
		return mux.NoMatch
	}
	return mux.Match
}
```

Such a route can be added beside the built-in ones:
`mux.Route{Match: resp, Handler: redisServer}`. Test the matcher against real client bytes,
not against what the RFC says the first bytes are — the matcher sees exactly what the
socket delivered, possibly one byte at a time.

## Fallback, limits and handover

`mux.New` is `mux.NewWithConfig(mux.Config{}, routes...)`. The config has three fields that
matter operationally: `Fallback` is a `fib.Handler` for connections no route recognises
(nil closes them with `mux.ErrNoMatch`), `MaxPeek` bounds how many bytes the mux buffers
while deciding (64 by default; the HTTP/2 preface is 24), and `Timeout` bounds a silent
peer (10 seconds by default, negative disables it).

```go
app := mux.NewWithConfig(mux.Config{
	Fallback: fib.HandlerFuncs{ // anything else: say so and close
		Open: func(c *fib.Connection) { _ = c.Send([]byte("unknown protocol\n")) },
	},
	MaxPeek: 256, // room for a longer first command
	Timeout: 5 * time.Second,
},
	mux.Route{Match: mux.TLS(), Handler: tlsHandler},
)
```

`mux.Route` also has a `Nudge` hook, `func(c *fib.Connection, input []byte) (done bool)`,
which runs each time the mux waits for more bytes because that route answered `NeedMore`,
until it returns true. It is how a protocol that waits for the *server* to
speak before it says enough to be matched is served anyway: `multiproto` uses it to send an
empty HTTP/2 SETTINGS frame to a gRPC client that waits for one before sending its first
headers. The hook runs on the worker and may send on the connection but must not block.
`OnPriorityData` has nothing to route before the decision, so the mux drops it.

## One port with `multiproto`

`multiproto.New` returns a `*mux.Handler` built from the routes `multiproto.Routes` derives
from the non-nil fields of `multiproto.Config`. One field per protocol family, so the
standard set is a single literal:

```go
package main

import (
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/arpc"
	"github.com/lesismal/fib/grpc"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/multiproto"
	"github.com/lesismal/fib/socks"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
	"github.com/lesismal/fib/websocket"
)

func main() {
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}

	web := fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte("hello\n"))
	}))
	ws := websocket.NewHandler(websocket.HandlerFuncs{
		Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
			_ = c.WriteMessage(op, data) // echo
		},
	})
	arpcServer := arpc.NewServer()
	arpcServer.Handler.Handle("/echo", func(ctx *arpc.Context) {
		var s string
		if err := ctx.Bind(&s); err != nil {
			_ = ctx.Error(err)
			return
		}
		_ = ctx.Write(s)
	})

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr: "127.0.0.1:8000",
		Handler: multiproto.New(multiproto.Config{
			TLS:       fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), web),
			HTTP1:     web,
			HTTP2:     web,
			WebSocket: ws,
			GRPC:      grpc.NewServer(), // register generated services on it
			SOCKS:     socks.NewServer(socks.Config{}),
			HTTPProxy: fibhttp.NewHandler(fibhttp.NewConnectProxy(fibhttp.ConnectProxyConfig{})),
			ARPC:      arpcServer,
		}),
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
curl -k https://127.0.0.1:8000/                          # TLS: HTTP/2 by ALPN
curl http://127.0.0.1:8000/                              # HTTP1
curl --http2-prior-knowledge http://127.0.0.1:8000/      # HTTP2: h2c that is not gRPC
curl --socks5 127.0.0.1:8000 http://127.0.0.1:8000/      # SOCKS, back into the same port
curl -p -x http://127.0.0.1:8000 http://127.0.0.1:8000/  # HTTPProxy: a CONNECT tunnel
```

A protocol whose handler is nil is not served; its connections go to `Fallback`, or are
closed. That is how to run a subset, and it is honest about what the port can do: with
`GRPC` nil and `HTTP2: web`, every h2c connection goes to the HTTP handler.

`fibhttp.NewConnectProxy` is a CONNECT proxy only. `multiproto` hands it the forward-proxy
requests for an absolute URL too (`curl -x` without `-p`), and it answers them with 405 Method
Not Allowed unless `ConnectProxyConfig.Next` serves them.

## How `multiproto` sniffs

The routes it builds, in order, and why:

- **TLS** — a ClientHello. Whatever is inside TLS is the TLS handler's to tell apart by
  ALPN, which `fibhttp.ConfigureTLS` does for HTTP/1.1 and HTTP/2.
- **SOCKS** — a SOCKS5 greeting or a SOCKS4/4a request.
- **HTTP/1.x** — `HTTPProxy` first (a `CONNECT`, or a request for an absolute
  `http://`/`https://` URL), then `WebSocket` (a `GET` whose head carries
  `Upgrade: websocket`), then `HTTP1` takes the rest. The first request of a connection
  decides for the whole of it, and whichever of these is served last takes every HTTP/1.x
  connection the others did not claim.
- **h2c** — with both `GRPC` and `HTTP2` set, `GRPC` reads the first request's header block
  and matches `content-type: application/grpc...`, with `NudgeHTTP2` sending the server
  SETTINGS that grpc-go's clients wait for; `HTTP2` takes the rest. With only one of them
  served, it takes every h2c connection.
- **ARPC** — a heuristic on the first arpc header. arpc has no magic number, so when it is
  served the TLS and SOCKS matchers are replaced by stricter variants (`ClientHello`, and a
  SOCKS match that excludes arpc), and the ARPC route is tried last. `ARPCMaxBodyLen` bounds
  the body length a header may announce before it is recognised; set it as the arpc server's
  `Handler.SetMaxBodyLen` is set.

The reading is as short as the set allows: with `WebSocket` and `HTTP1` both set it waits
for a whole request head, with only one of them the method is enough. `MaxPeek` (8192 by
default) is the bound on a head or header block, and `Timeout` is the bound on a silent
peer; `Routes` lets you build your own `mux.Handler` with the same table, and
`Config.Routes` appends routes of your own after the built-in ones.

## Bindings, `mux` or `multiproto`?

- **Separate bindings** are the cheapest and the clearest: one port per protocol, no
  sniffing, per-port limits and certificates, and the only option for a protocol whose
  server speaks first. Two bindings can share a port number only across TCP and UDP.
- **`mux`** is for a small, explicit table, custom matchers, and protocols whose first
  bytes are cheap to classify. You own the order and the fallback.
- **`multiproto`** is for the standard protocol families: it knows how far to read for a
  WebSocket upgrade, how to tell gRPC from other h2c, and how to keep ARPC from being taken
  for TLS. Prefer it unless you need a matcher it does not have.

## Pitfalls

- **The client must speak first.** SMTP, FTP, MySQL and anything else server-greeted needs a
  binding of its own; on a mux these peers are closed after `Config.Timeout` with
  `os.ErrDeadlineExceeded` reported to the mux's `OnClose`.
- **The timeout is not the handler's.** The chosen handler starts with no read deadline and
  sets its own; a mux timeout only bounds the sniffing phase.
- **The first HTTP request decides the connection.** An upgrade or a protocol switch later
  in the connection is the chosen handler's business, not the mux's.
- **Too small a `MaxPeek` silently misroutes.** A WebSocket handshake that does not fit
  counts as "not a WebSocket" and falls through to the plain HTTP route; `multiproto`'s
  default of 8192 exists for browsers' heads with cookies.
- **`HTTPProxy` takes absolute-URL requests too.** A plain `GET http://...` sent to the port
  goes to the proxy handler, not to `HTTP1`; with `NewConnectProxy` and no `Next` it gets 405.
- **A route needs both fields.** `mux.New` panics if a `Route` has a nil `Match` or
  `Handler`; there is no implicit "match anything".
- **`Fallback` is only for no-match, not for a match that then fails.** If the SNI inside
  TLS is unknown, the TLS handler answers, not the fallback.
- **Priority is not first-match-wins in the naive sense.** A route matches only after the
  routes before it have ruled themselves out, so a broad matcher listed first shadows
  everything after it.

## See also

- [mux](../../mux) and [multiproto](../../multiproto) — the package docs and every matcher
- [SOCKS and HTTP CONNECT](13-socks-connect.md) — the proxy handlers used on the shared port
- [WebSocket](09-websocket.md) — the handshake `multiproto` reads a whole head for
- [gRPC](11-grpc.md) — the h2c handler that needs `NudgeHTTP2` to be recognised
- [Engine and configuration](02-engine.md) — bindings, addresses and shutdown
- [The README](../../README.md) — the `mux` and `multiproto` snippets in context

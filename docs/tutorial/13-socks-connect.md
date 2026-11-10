# SOCKS and HTTP CONNECT

[English](13-socks-connect.md) | [简体中文](13-socks-connect.zh-CN.md)

This chapter is about being the middle: a fib program that lets a client reach a destination it
cannot reach itself, with SOCKS5, SOCKS4/4a, HTTP CONNECT, or both on one port. fib ships three
pieces for it: `socks.Server`, a SOCKS proxy that is a plain `fib.Handler`; `socks.Dialer`, a
non-blocking client dialer that plugs into the HTTP, WebSocket and TLS clients' dial hooks; and
`fibhttp.ConnectProxy`, an HTTP CONNECT proxy for `curl -x` and `HTTPS_PROXY`. It assumes the
handler model from chapter 03 and the HTTP context from chapter 06.

## The model

- **A proxy is two connections and a relay.** `socks.Server` is a `fib.Handler`: bind it like any
  other protocol handler. After a CONNECT succeeds, what either side sends goes to the other from
  the callback that received it, with no goroutine in between; `Config.MaxQueuedBytes` holds the
  faster side's reads back while the slower side's socket catches up, so a fast sender is paced by
  TCP flow control rather than by memory. `Config.IdleTimeout` ends idle relays.
- **The client side never blocks.** `socks.ParseURL` reads the proxy URL forms that curl and the
  environment use; the resulting `Dialer`'s `Dial` has the contract of
  `fib.Engine.DialWithHandler` and fits the `Dial` field of `fibhttp.ClientConfig`,
  `websocket.DialerConfig` and `fibtls.DialOptions`. The connection it opens is an ordinary fib
  connection whose `RemoteAddr` is the proxy's; what its handler sends and receives travels through
  the proxy to the destination.
- **CONNECT is a tunnel over a request.** `fibhttp.ConnectProxy` serves CONNECT over HTTP/1.x,
  HTTP/2 and HTTP/3: it retains the request, dials the target, then accepts the request with
  `Context.Connect`, which gives it a `*fibhttp.Tunnel`. The tunnel's own flow control paces the
  relay (`Unsent`, `HoldReads`, `WaitSendable`, `NotifySendable`), and `ConnectProxy` is the
  worked example of all of them.
- **One port, one matcher.** `mux` looks at a connection's first bytes: SOCKS5 starts with byte 5,
  SOCKS4 with byte 4, HTTP with a request line, CONNECT included. A `mux.New` engine therefore
  serves SOCKS and an HTTP CONNECT proxy on the same port, without TLS inspection.

## A SOCKS5 server with authentication

A complete program: an HTTP server, a SOCKS5 proxy in front of it, and an HTTP client that reaches
the server only through the proxy. The proxy asks for a username and password and logs every
request:

```go
package main

import (
	"io"
	"log"
	nethttp "net/http"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/socks"
)

func main() {
	web := fibhttp.NewRouter()
	web.Get("/hello", func(c *fibhttp.Context) {
		_ = c.String(200, "hello through the SOCKS5 proxy")
	})

	srv, err := fib.New(fib.DefaultConfig(),
		fib.Binding{Addr: "127.0.0.1:18080", Handler: fibhttp.NewHandler(web)},
		fib.Binding{Addr: "127.0.0.1:11080", Handler: socks.NewServer(socks.Config{
			Authenticate: func(username, password string) bool {
				return username == "fib" && password == "secret"
			},
			Allow: func(r *socks.Request) error {
				log.Printf("%s %s as %q", r.Version, r.Addr, r.Username)
				return nil
			},
			IdleTimeout: 2 * time.Minute,
		})},
	)
	if err != nil {
		panic(err)
	}
	defer srv.Close()
	go func() { _ = srv.Run() }()

	proxy, err := socks.ParseURL("socks5h://fib:secret@127.0.0.1:11080")
	if err != nil {
		panic(err)
	}
	clientEngine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer clientEngine.Close()
	go func() { _ = clientEngine.Run() }()

	config := fibhttp.DefaultClientConfig()
	config.Dial = proxy.Dial // every request goes through the proxy
	client := fibhttp.NewClient(clientEngine, config)
	defer client.Close()

	req, err := nethttp.NewRequest(nethttp.MethodGet, "http://127.0.0.1:18080/hello", nil)
	if err != nil {
		panic(err)
	}
	rsp, err := client.Go(req).Wait()
	if err != nil {
		panic(err)
	}
	body, err := io.ReadAll(rsp.Body)
	if err != nil {
		panic(err)
	}
	log.Printf("%s: %s", rsp.Proto, body)
}
```

`Authenticate` runs the RFC 1929 username/password exchange on every SOCKS5 client; without it,
every client is admitted. `Allow` decides each request after admission: a nil error grants it, an
error refuses it with "not allowed by ruleset", and a `*socks.ReplyError` refuses it with the reply
code it names. `Allow` sees the destination as the client named it, which may be a host name that
resolves somewhere else. A rule about addresses belongs in `AllowResolved`, which sees each
address the proxy resolved right before it dials, and is called only when `Config.Dial` is nil:

```go
server := socks.NewServer(socks.Config{
	Authenticate: func(username, password string) bool {
		return username == "fib" && password == "secret"
	},
	Allow: func(r *socks.Request) error {
		if r.Addr == "169.254.169.254:80" { // a name-free rule, on what the client asked for
			return &socks.ReplyError{Version: r.Version, Code: socks.ReplyNotAllowed}
		}
		return nil
	},
	AllowResolved: func(r *socks.Request, addr netip.AddrPort) error {
		if addr.Addr().IsLoopback() || addr.Addr().IsPrivate() {
			return &socks.ReplyError{Version: r.Version, Code: socks.ReplyNotAllowed}
		}
		return nil
	},
	HandshakeTimeout: 10 * time.Second,
	DialTimeout:      10 * time.Second,
	MaxBinds:         64,
	MaxUDPTargets:    256,
})
```

The rest of `Config` is about resource bounds: `MaxBinds` caps the BINDs waiting for their peers,
`MaxUDPTargets` caps the destinations one UDP association keeps a socket to, `MaxQueuedBytes`
caps what waits in each relay direction, and `HandshakeTimeout` and `DialTimeout` bound the
handshake and the dial to the destination.

## SOCKS4, 4a, BIND and UDP ASSOCIATE

One `Server` serves all three versions: it reads the first byte of the greeting and picks the
protocol. SOCKS4 has no password exchange, so a `Config.Authenticate` turns SOCKS4 clients away;
SOCKS4 resolves names at the client, and SOCKS4a sends the proxy a host name to resolve.

UDP ASSOCIATE needs a UDP binding of the same engine. The server tells each client the address of
the UDP binding of the same family as the one it reached the server at, and a server with no UDP
binding refuses UDP ASSOCIATE. `Allow` is asked again for each destination a client sends a
datagram to, and the datagrams to one it refuses are dropped:

```go
server := socks.NewServer(socks.Config{})
engine, err := fib.New(fib.DefaultConfig(),
	fib.Binding{Addr: "127.0.0.1:1080", Handler: server},
	fib.Binding{Network: "udp", Addr: "127.0.0.1:1080", Handler: server}, // UDP ASSOCIATE
)
if err != nil {
	panic(err)
}
```

BIND asks the proxy to listen for one connection from the peer whose address the client names,
which is what FTP's active mode needs. The `Dialer` side of it is `Dialer.Bind`: it reports the
address the proxy listens on so the caller can tell the peer, then hands over the connection when
the peer arrives:

```go
if err := proxy.Bind(clientEngine, "127.0.0.1:9000", 2*time.Minute,
	fib.HandlerFuncs{Data: func(c *fib.Connection, data []byte) {
		_ = c.Send(data) // bytes from the peer
	}},
	func(addr net.Addr) {
		log.Printf("tell the peer to connect to %v", addr)
	},
	func(c *fib.Connection, err error) {
		log.Printf("peer connected: %v, %v", c, err)
	}); err != nil {
	panic(err)
}
```

A BIND is accepted only from the destination's IP, or from one the destination's name resolves
to; the unspecified address `0.0.0.0` or `::` lets any host connect.

## A non-blocking dialer for the clients

`socks.ParseURL` takes the URL forms curl and proxy settings use; a missing port means
`socks.DefaultPort`, 1080. The scheme picks the protocol and the name-resolution side:
`socks5://` and `socks4://` resolve names locally, `socks5h://` and `socks4a://` let the proxy
resolve them:

```go
proxy, err := socks.ParseURL("socks5h://fib:secret@127.0.0.1:1080")
if err != nil {
	panic(err)
}

httpConfig := fibhttp.DefaultClientConfig()
httpConfig.Dial = proxy.Dial // HTTP/1.x and HTTP/2 through the proxy

wsConfig := websocket.DefaultDialerConfig()
wsConfig.Dial = proxy.Dial // wss:// and ws:// through the proxy

tlsOptions := fibtls.DialOptions{Dial: proxy.Dial} // any TLS dial through the proxy
```

A `udp` network asks a SOCKS5 proxy for a UDP ASSOCIATE instead of a CONNECT: each `Send` on the
connection is one datagram to the destination, through the proxy's relay, and each `OnData` one
datagram back.

`Dialer.Dial` is also the whole `DialFunc` contract, so proxies chain two ways. On the server,
`socks.Config.Dial` makes a proxy reach its destinations through another proxy; on the client,
`Dialer.Forward` makes a dialer reach its proxy through another one:

```go
back, err := socks.ParseURL("socks5h://127.0.0.1:1080")
if err != nil {
	panic(err)
}
// A SOCKS server whose CONNECTs and UDP destinations go out through back:
front := socks.NewServer(socks.Config{Dial: back.Dial})

// A dialer that reaches the proxy at :1081 through back, then the destination through :1081:
viaBack, err := socks.ParseURL("socks5h://127.0.0.1:1081")
if err != nil {
	panic(err)
}
viaBack.Forward = back.Dial
```

The destination sees only the last proxy's address. With `Config.Dial` set, `AllowResolved` is not
called: the chained dialer resolves names itself.

Under the hood the connect to the proxy, the negotiation with it and any DNS lookup all happen on
the engine's loop and workers, never on the caller's goroutine, so the dialer is safe to call from
anywhere and the engine keeps working while it waits:

```go
if err := proxy.Dial(clientEngine, "tcp", "example.com:443", 5*time.Second,
	fib.HandlerFuncs{Data: func(c *fib.Connection, data []byte) {
		log.Printf("from the destination: %s", data)
	}},
	func(conn *fib.Connection, err error) {
		if err != nil {
			log.Print(err)
			return
		}
		_ = conn.Send([]byte("hello through the proxy"))
	}); err != nil {
	panic(err)
}
```

## An HTTP CONNECT proxy, and one port for SOCKS and CONNECT

A complete program: one engine serves an HTTP site on one port, and on another port a `mux` that
routes SOCKS5 and the CONNECT proxy by the first bytes. The proxy wants Basic credentials; the
client speaks CONNECT by hand, as curl does for `-x`, then sends an ordinary GET through the
tunnel:

```go
package main

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net"
	nethttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/mux"
	"github.com/lesismal/fib/socks"
)

func main() {
	auth := func(username, password string) bool {
		return username == "fib" && password == "secret"
	}

	web := fibhttp.NewRouter()
	web.Get("/hello", func(c *fibhttp.Context) {
		_ = c.String(200, "hello through CONNECT")
	})

	proxy := fibhttp.NewConnectProxy(fibhttp.ConnectProxyConfig{
		Authenticate: auth,
		Realm:        "fib",
		Allow: func(r *nethttp.Request) error {
			log.Printf("CONNECT %s", r.Host)
			return nil
		},
	})

	engine, err := fib.New(fib.DefaultConfig(),
		fib.Binding{Addr: "127.0.0.1:18080", Handler: fibhttp.NewHandler(web)},
		fib.Binding{Addr: "127.0.0.1:18082", Handler: mux.New(
			mux.Route{Match: mux.SOCKS(), Handler: socks.NewServer(socks.Config{Authenticate: auth})},
			mux.Route{Match: mux.HTTP(), Handler: fibhttp.NewHandler(proxy)},
		)},
	)
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	go func() { _ = engine.Run() }()

	// The client: CONNECT with Basic credentials, then a request in the tunnel.
	conn, err := net.Dial("tcp", "127.0.0.1:18082")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	credential := base64.StdEncoding.EncodeToString([]byte("fib:secret"))
	if _, err := fmt.Fprintf(conn,
		"CONNECT 127.0.0.1:18080 HTTP/1.1\r\nHost: 127.0.0.1:18080\r\nProxy-Authorization: Basic %s\r\n\r\n",
		credential); err != nil {
		panic(err)
	}
	reader := bufio.NewReader(conn)
	accepted, err := nethttp.ReadResponse(reader, &nethttp.Request{Method: nethttp.MethodConnect})
	if err != nil {
		panic(err)
	}
	if accepted.StatusCode != nethttp.StatusOK {
		panic(accepted.Status)
	}
	if _, err := fmt.Fprintf(conn,
		"GET /hello HTTP/1.1\r\nHost: 127.0.0.1:18080\r\nConnection: close\r\n\r\n"); err != nil {
		panic(err)
	}
	rsp, err := nethttp.ReadResponse(reader, &nethttp.Request{Method: nethttp.MethodGet})
	if err != nil {
		panic(err)
	}
	body, err := io.ReadAll(rsp.Body)
	if err != nil {
		panic(err)
	}
	log.Printf("%s: %s", rsp.Status, body)
}
```

`ConnectProxyConfig.Authenticate` reads the `Proxy-Authorization: Basic` header (RFC 9110 §11.7.2)
and answers a client without it with `407 Proxy Authentication Required` and a `Proxy-Authenticate`
challenge naming `Realm` (`proxy` when empty). An error from `Allow` answers the CONNECT with
`403 Forbidden`; a target the proxy cannot reach gets `502 Bad Gateway`, or `504 Gateway Timeout`
when the dial timed out, and a target that is not a host and port `400 Bad Request`. `Dial`,
`DialTimeout`, `MaxQueuedBytes`, `IdleTimeout` and `Next` are the rest: `Next` serves requests
that are not CONNECTs to a host and port, and a nil `Next` answers them with
`405 Method Not Allowed`. Point `Dial` at a `socks.Dialer`'s `Dial` and the CONNECT proxy chains
through a SOCKS proxy. The proxy's handler does not wait for the dial: it retains the request,
returns, and the dial's callback answers it.

## Context.Connect and tunnel pacing

`ConnectProxy` is built on `Context.Connect`, which any handler may use. It accepts the CONNECT
request with a 200, and from then on a `TunnelHandler` receives what the client sends through the
tunnel, while `Tunnel.Send` and `CloseAfterSend` carry bytes the other way. A complete program, a
tunnel that echoes instead of dialing anywhere:

```go
package main

import (
	"log"
	nethttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

// echoTunnel answers everything the client sends through the tunnel.
type echoTunnel struct{}

func (echoTunnel) OnTunnelOpen(t *fibhttp.Tunnel) { log.Print("tunnel opened") }

func (echoTunnel) OnTunnelData(t *fibhttp.Tunnel, data []byte) {
	_ = t.Send(append([]byte("echo: "), data...)) // data is valid for the call only
}

func (echoTunnel) OnTunnelClose(t *fibhttp.Tunnel, err error) {
	log.Printf("tunnel closed: %v", err)
}

func main() {
	// A CONNECT names host:port, not a path, so it is served by a plain
	// handler rather than a router route.
	handler := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		if c.Request.Method != nethttp.MethodConnect {
			_ = c.Respond(nethttp.StatusMethodNotAllowed, "text/plain; charset=utf-8", []byte("CONNECT only\n"))
			return
		}
		log.Printf("CONNECT %s", c.Request.Host)
		if _, err := c.Connect(nil, echoTunnel{}); err != nil {
			_ = c.Respond(nethttp.StatusBadRequest, "text/plain; charset=utf-8", []byte(err.Error()))
		}
	})
	engine, err := fib.New(fib.DefaultConfig(),
		fib.Binding{Addr: "127.0.0.1:18084", Handler: fibhttp.NewHandler(handler)})
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

Try it with `nc`; the 200 comes first, then the echo of what was sent after the request:

```sh
$ (printf 'CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n'; sleep 0.3; printf 'ping'; sleep 0.5) | nc 127.0.0.1 18084
HTTP/1.1 200 OK

echo: ping
```

A classic CONNECT's target is in authority form (`example.com:443`), with no path, so router
patterns such as `router.Connect("/x", ...)` never match it: they serve CONNECTs that carry a path,
such as HTTP/2's extended CONNECT. Serve tunnels from a plain handler, as here, and let it hand
every request that is not a CONNECT to a router when one port serves both.

For a relay to a slower peer, pace each side by the other's backlog instead of buffering it. The
primitives are the tunnel's own flow control: `Unsent` reports what is still waiting to go,
`HoldReads` stops taking what the peer sends, and `NotifySendable`/`WaitSendable` call back or wait
until the backlog has drained. With `in` the side data comes from and `out` the side it is sent to,
each time `in`'s data has been passed to `out`:

```go
const limit = 256 << 10 // what may wait for out, as Config.MaxQueuedBytes bounds a relay
in.HoldReads(true) // hold first, so that the callback cannot run before the hold
if !out.NotifySendable(limit, func(error) { in.HoldReads(false) }) {
	in.HoldReads(false) // no more than limit waits for out: keep reading
}
// The callback runs once out is down to half of limit. In a goroutine of your own,
// never in OnTunnelData, this is the waiting form:
go func() {
	if err := out.WaitSendable(limit); err != nil {
		log.Print(err) // out has ended
	}
}()
```

A tunnel over HTTP/1.x switches the whole connection; over HTTP/2 and HTTP/3 only its stream
switches, and the shared connection is left alone. On HTTP/2 and HTTP/3 what the peer's window
allowed already still arrives while reads are held, so the hold is a bound, not a stop.

## curl, curl -x and HTTPS_PROXY

Once a proxy is running, the shell tools use it: `curl -x` for the proxy of one command, and
`HTTPS_PROXY`/`ALL_PROXY` for programs that read the environment. To try them on the one-port
program above, keep its engine running (end `main` with `select {}` in place of the hand-written
client); port 18082 then answers both SOCKS5 and CONNECT:

```sh
# SOCKS5 with the proxy resolving the destination name (socks5h vs socks5)
curl -x socks5h://fib:secret@127.0.0.1:18082 http://127.0.0.1:18080/hello

# HTTP CONNECT with Basic credentials in the URL; curl sends Proxy-Authorization.
# -p makes curl tunnel an http:// URL through CONNECT too.
curl -p -x http://fib:secret@127.0.0.1:18082 http://127.0.0.1:18080/hello

# Any HTTPS site, through CONNECT, as curl does for https:// URLs on its own
curl -x http://fib:secret@127.0.0.1:18082 https://example.com/

# The same proxy for programs that read the environment
HTTPS_PROXY=http://fib:secret@127.0.0.1:18082 curl https://example.com/
```

`curl -x` with an `https://` URL uses CONNECT; with an `http://` URL and no `-p` it sends the
request to the proxy whole, in absolute form, which is `ConnectProxyConfig.Next`'s business, not
CONNECT's: here it gets `405 Method Not Allowed`. Without credentials curl gets
`407 Proxy Authentication Required` from CONNECT, and a failed SOCKS5 login ends the handshake
(curl exits with code 97).

## Pitfalls

- **A CONNECT proxy tunnels CONNECT only.** A request for an `http://` URL in absolute form is
  never dialed by `ConnectProxy`; give `Next` a forwarding handler to serve it, or clients will
  see `405 Method Not Allowed`.
- **Wrap the connect proxy in `fibhttp.NewHandler`.** `NewConnectProxy` returns a request handler,
  not a connection handler; a binding needs `fibhttp.NewHandler(proxy)`.
- **`Authenticate` turns SOCKS4 clients away.** SOCKS4 has no password field, so a server that
  checks passwords but keeps SOCKS4 reachable is a hole; the `Server` closes such clients.
- **Address rules belong in `AllowResolved`.** `Allow` sees the name the client sent, which may
  resolve to a forbidden address by the time it is dialed (DNS rebinding). `AllowResolved` is not
  called when `Config.Dial` is set: a dialer resolves names itself, so chain carefully.
- **UDP ASSOCIATE needs a UDP binding.** Without one the server refuses the association, and the
  binding's address family must be the one the client reaches the server at. `SendBatch` bypasses
  the proxy's UDP framing and must not be used on a UDP-association connection.
- **A relayed connection's `RemoteAddr` is the proxy's.** The destination is what the handler talks
  to, but the address it sees belongs to the proxy connection.
- **The peer that speaks first is the mux rule.** SOCKS and HTTP clients do; a protocol whose
  client waits for the server is closed after the mux timeout. Give such protocols a port of their
  own.
- **HTTP/1 CONNECT switches the whole connection.** A refused CONNECT is answered with `Connection:
  close`, because the client may already have sent the tunnel's first bytes; nothing after a
  CONNECT on that connection is HTTP.
- **Pace, do not buffer.** `MaxQueuedBytes` bounds one relay direction; a relay with no idle
  timeout and no backlog bound can pin memory and sockets.

## See also

- The [`socks` package](../../socks), the [`http` package](../../http) for `ConnectProxy`, and
  their package docs.
- The root README's [SOCKS section](../../README.md#socks), with the one-port `mux` snippet.
- [Chapter 06: HTTP/1.x](06-http1.md) for CONNECT from the server's side, and [chapter 07:
  HTTP/2](07-http2.md) for CONNECT over a stream.
- [Chapter 15: Multiprotocol ports](15-multiprotocol.md) for `mux` and `multiproto` in depth.
- [Chapter 20: Clients](20-clients.md) for the HTTP, WebSocket and TLS clients whose dial hooks
  take a `socks.Dialer`.
- [docs/hacks.md](../hacks.md) for `HoldReads`, cork and backpressure from the connection side.

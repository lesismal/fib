# Clients

[English](20-clients.md) | [简体中文](20-clients.zh-CN.md)

Every protocol in fib has a client built on the same engine, and none of them blocks its caller:
`Engine.Dial`, `fibhttp.Client`, the `http3` client, `websocket.Dialer`, `fibtls` and `socks`
all "register a callback and return", exactly like the server handlers of chapter
[03-handlers.md](03-handlers.md). This chapter shows dialing, the HTTP/1.x, HTTP/2 and HTTP/3
clients, the WebSocket client, TLS and SOCKS options, and the lifetime rules that all of them
share. It assumes the engine and configuration chapter [02-engine.md](02-engine.md) and the async
handler model.

## The client model

A client is a set of connections an engine dials and serves, so a client-only program is an
engine with no bindings:

```go
engine, err := fib.New(fib.DefaultConfig()) // no bindings: dials only, listens nowhere
if err != nil {
	panic(err)
}
go func() { _ = engine.Run() }()
defer func() { engine.Stop(); _ = engine.Close() }()

client := fibhttp.NewClient(engine, fibhttp.DefaultClientConfig())
defer client.Close() // close clients before the engine
```

Three rules follow from that design and every example below depends on them:

- **Nothing waits.** A dial, a request or a WebSocket handshake reports its outcome to a callback;
  the caller's goroutine is never held. The only blocking forms (`Future.Wait`, `DialFuture.Wait`)
  are for `main`, tests and other places that may block — never inside a handler.
- **Callbacks run on whatever goroutine had the outcome**: an engine worker that read the
  response, a timer on timeout, the process-wide `fib-client` pool for DNS and completion, or the
  caller's own goroutine when a request is rejected outright. A callback must not block for long.
- **The engine outlives its clients.** The client pools connections; closing the client stops it,
  and `client.Close()` must come before `engine.Close()`, which drops connections without telling
  the client. A request whose engine is gone can only fail by its timeout.

By default a response body is buffered whole before its callback runs, and `ClientResponse`
embeds the standard `*http.Response`. The streaming style is opt-in per client
(`StreamResponseBody`) or per request (`Framed`/`Upgrade`), and is shown in
[Streaming bodies](#streaming-bodies).

## Asynchronous dialing

`Engine.Dial` is the lowest level: it opens a raw connection and hands it to a handler, as
[19-raw-io.md](19-raw-io.md) uses it. `DialWithHandler` gives the connection a handler of its own,
which is what lets one engine carry a server and its clients at the same time, and `DialWith`
takes options for the local address and port the socket is bound to.

```go
// A connection of its own handler, so the engine's server bindings are unaffected.
err = engine.DialWithHandler("tcp", "127.0.0.1:9000", 3*time.Second, myHandler,
	func(c *fib.Connection, err error) {
		if err != nil {
			log.Println("dial:", err)
			return
		}
		_ = c.Send([]byte("ping")) // queued; flushed when the round ends
	})

// Bound to a chosen local address and port range, for a firewall that admits one source.
err = engine.DialWith(fib.DialOptions{
	Network:   "tcp",
	Addr:      "127.0.0.1:9000",
	Timeout:   3 * time.Second,
	Handler:   myHandler,
	LocalAddr: netip.AddrPortFrom(netip.MustParseAddr("192.0.2.10"), 0),
	PortMin:   40000,
	PortMax:   40100, // a port in use is skipped; no free port fails with fib.ErrNoLocalPort
}, func(c *fib.Connection, err error) {
	if err != nil {
		return
	}
	_ = c.Send([]byte("ping"))
})
```

Both calls return an error only when the dial cannot start at all (unknown network, malformed
address, stopped engine); every later failure reaches `done`, which runs exactly once. A `nil`
handler means the engine's own, which is how `Engine.Dial` behaves. DNS resolution happens off the
caller's goroutine, on the `fib-client` pool.

## HTTP/1.x client

`fibhttp.Client` speaks HTTP/1.x and HTTP/2 over connections the engine dials, keeps them alive
between requests, and turns a URL into a pooled connection per host. Start from
`DefaultClientConfig` — a zero `ClientConfig` keeps its zero timeouts:

```go
config := fibhttp.DefaultClientConfig() // Timeout 30s, DialTimeout 10s, 64 conns/host
config.Timeout = 5 * time.Second        // one request, from Do to the complete response
config.MaxConnsPerHost = 16
config.MaxIdleConnsPerHost = 8
config.IdleConnTimeout = 90 * time.Second
client := fibhttp.NewClient(engine, config)
defer client.Close()
```

The callback form is `Do`; the request is a standard `*http.Request`:

```go
req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://127.0.0.1:8080/", nil)
if err != nil {
	return
}
client.Do(req, func(resp *fibhttp.ClientResponse, err error) {
	if err != nil {
		return // exactly once: the response or the reason there is none
	}
	defer resp.Body.Close()
	fmt.Println(resp.StatusCode, resp.Header.Get("Content-Type"))
})
```

The same request can be waited on, which is right in `main` and wrong in a handler:

```go
resp, err := client.Go(req).Wait()
if err != nil {
	return
}
defer resp.Body.Close()
```

`Go` always waits for the whole response, as `net/http` does; `Do` is the one to use on a handler's
goroutine. `Start(req, cb)` is `Do` that returns a `Call`, which abandons the request without a
cancellable context:

```go
call := client.Start(req, func(resp *fibhttp.ClientResponse, err error) {})
call.Cancel(context.Canceled) // callback hears it; a no-op once the request has settled
```

A request body with a `ContentLength` of up to 1MB — what `net/http.NewRequest` sets for a small
`bytes.Buffer`, `bytes.Reader` or `strings.Reader` — is read whole by `Do` and sent with the
header. Any other body is read piece by piece by a goroutine of its own and paced by the server
(64KB unsent at most), so a file upload streams instead of loading into memory. The body is closed
once sent. Requests a connection failed are retried once when they are idempotent and their body
can be re-read through `req.GetBody`; a request with `Expect: 100-continue` sends its header first
and its body once the server answers.

The complete program below runs a server and a client on one engine: the future form for the
first request, the callback form for the second.

```go
package main

import (
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	config := fib.DefaultConfig()
	config.LogSettings = false

	engine, err := fib.New(config, fib.Binding{
		Addr: "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
			_ = c.Respond(200, "text/plain; charset=utf-8", []byte("hello "+c.Query("name")))
		})),
	})
	if err != nil {
		panic(err)
	}
	go func() { _ = engine.Run() }()
	defer func() { engine.Stop(); _ = engine.Close() }()

	client := fibhttp.NewClient(engine, fibhttp.DefaultClientConfig())
	defer client.Close()

	// Future form: right in main, never in a handler.
	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://127.0.0.1:8080/?name=fib", nil)
	if err != nil {
		panic(err)
	}
	resp, err := client.Go(req).Wait()
	if err != nil {
		panic(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	fmt.Println("sync:", string(body))

	// Callback form: the response arrives on an engine worker.
	req, err = stdhttp.NewRequest(stdhttp.MethodPost, "http://127.0.0.1:8080/upload",
		strings.NewReader("payload"))
	if err != nil {
		panic(err)
	}
	done := make(chan struct{})
	client.Do(req, func(resp *fibhttp.ClientResponse, err error) {
		defer close(done)
		if err != nil {
			fmt.Println("async:", err)
			return
		}
		body, _ := io.ReadAll(resp.Body)
		fmt.Println("async:", resp.StatusCode, string(body))
	})
	<-done
}
```

## Streaming bodies

Both directions can stream. For a response, set `StreamResponseBody` and take the body with
`OnBody` inside the callback; the callback then runs once the header (or a threshold of the body)
has arrived, and `data` is valid only during the call:

```go
config := fibhttp.DefaultClientConfig()
config.StreamResponseBody = true
config.StreamResponseBodyThreshold = 64 << 10 // bodies up to 64 KiB are still buffered whole
config.StreamUnknownLengthAtHeader = true     // SSE and long polls hand on as they trickle
client := fibhttp.NewClient(engine, config)

client.Do(req, func(resp *fibhttp.ClientResponse, err error) {
	if err != nil {
		return
	}
	resp.OnBody(func(data []byte, fin bool, err error) {
		if err != nil {
			return // connection gone, timed out or cancelled: the last call
		}
		_, _ = w.Write(data)
		if fin {
			_ = w.Close()
		}
	})
})
```

`OnBody` never calls its function itself: what has arrived is delivered after the callback
returns, the rest on the worker that reads the connection, one call at a time and in order. A slow
function stops the client reading that connection, so the sender is paced by TCP or by HTTP/2 flow
control rather than by memory here. Reading `resp.Body` inside the callback instead is the pull
form; it reports `fibhttp.ErrWouldBlock` when nothing has arrived yet and must not be mixed with
`OnBody`.

For a request, pass any `io.Reader` and leave `ContentLength` unset (`-1`) to stream it:

```go
pr, pw := io.Pipe()
req, _ := stdhttp.NewRequest(stdhttp.MethodPost, "http://127.0.0.1:8080/upload", pr)
req.ContentLength = -1 // chunked on HTTP/1, DATA frames on HTTP/2

go func() {
	defer pw.Close()
	// write the body in pieces; the client reads it as the server accepts it
}()

client.Do(req, func(resp *fibhttp.ClientResponse, err error) {
	if err != nil {
		return
	}
	_ = resp.Body.Close()
})
```

`http3.Client` has the same `StreamResponseBody` options, and `ClientResponse.TakeBody` /
`BodyBytes` hand a whole body on without a copy — the reverse-proxy path used by
[21-hacks.md](21-hacks.md).

## HTTP/2 client

There is no separate HTTP/2 client: `fibhttp.Client` speaks HTTP/2 to any `https://` origin that
chooses it through ALPN, and carries as many concurrent requests on one connection as the server
allows. The same connection is reused and requests are multiplexed, so a gateway needs far fewer
connections than requests.

```go
config := fibhttp.DefaultClientConfig()
config.MaxConnsPerHost = 4                 // enough for the fallback to HTTP/1.1
config.ReadIdleTimeout = 30 * time.Second  // PING a silent h2 connection
config.PingTimeout = 15 * time.Second      // and take it for dead if the PING is not answered
config.HTTP2.AdaptiveWindow = true         // grow the flow-control windows to the path's BDP
config.HTTP2.StreamWindow = 4 << 20
client := fibhttp.NewClient(engine, config) // https:// then negotiates h2 by default
```

`UnencryptedHTTP2 = true` sends `http://` requests as h2c with prior knowledge (chapter
[07-http2.md](07-http2.md)), and `DisableHTTP2 = true` keeps `https://` on HTTP/1.1. A per-request
timeout or cancellation resets only that stream; requests the server says it did not process
(`GOAWAY`, `REFUSED_STREAM`) are sent again on a new connection. `ReadIdleTimeout` and
`SettingsTimeout` are the two ways a connection that died without a word — a NAT that forgot it,
a server that went quiet — is found, instead of waiting for TCP.

## HTTP/3 and Alt-Svc

HTTP/3 is always encrypted and always over QUIC on a UDP socket, so the `http3.Client` needs a TLS
1.3 config and an `https://` URL. Where the address of the HTTP/3 endpoint is known, dial that
origin directly; where it is not — the usual case for a web server that advertises HTTP/3 on its
TCP responses — wrap the TCP and HTTP/3 clients in `http3.AltSvcClient`, which follows the
origin's `Alt-Svc` header:

```go
package main

import (
	"fmt"
	"io"
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
	"github.com/lesismal/fib/tlstest"
)

func main() {
	_, clientTLS, err := tlstest.Configs() // a throwaway certificate for localhost
	if err != nil {
		panic(err)
	}

	engine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		panic(err)
	}
	go func() { _ = engine.Run() }()
	defer func() { engine.Stop(); _ = engine.Close() }()

	tcpConfig := fibhttp.DefaultClientConfig()
	tcpConfig.TLSConfig = clientTLS
	tcp := fibhttp.NewClient(engine, tcpConfig)

	quicConfig := http3.DefaultClientConfig()
	quicConfig.TLSConfig = clientTLS
	quic := http3.NewClient(engine, quicConfig)

	// The first request to an origin goes over TCP; once its response has
	// carried Alt-Svc: h3=..., the next ones take QUIC to that endpoint.
	client := http3.NewAltSvcClient(tcp, quic)
	defer client.Close()

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "https://localhost:8443/", nil)
	if err != nil {
		panic(err)
	}
	resp, err := client.Go(req).Wait()
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println(resp.Proto, string(body))
}
```

The server side of that handshake is chapter [08-http3.md](08-http3.md): a UDP binding served by
`http3.NewHandler(http3.ConfigureTLS(tlsConfig), app)`, and `http3.AltSvc(port)` in a TCP
response's header. A QUIC attempt that fails falls back to TCP for a repeatable request, and the
origin stays on TCP for a while, so a network that blocks UDP costs one timeout rather than one
per request. `ClientStats` reports resumption and 0-RTT (`EarlyData`).

## WebSocket client

`websocket.Dialer` performs the opening handshake over an engine connection and then serves the
client connection with the same `HandlerFuncs` as a server connection — the same `OnMessage`,
`OnFrame`, `WriteMessage` and `Close` from chapter [09-websocket.md](09-websocket.md). The
handshake returns the connection and the server's `*http.Response`:

```go
package main

import (
	"fmt"
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
	go func() { _ = engine.Run() }()
	defer func() { engine.Stop(); _ = engine.Close() }()

	config := websocket.DefaultDialerConfig()
	config.HandshakeTimeout = 5 * time.Second
	config.Subprotocols = []string{"chat"}
	config.EnableCompression = true
	dialer := websocket.NewDialer(engine, config)

	handler := websocket.HandlerFuncs{
		Open: func(c *websocket.Connection, r *stdhttp.Request) {
			fmt.Println("open, subprotocol:", c.Subprotocol())
		},
		Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
			fmt.Println("got:", string(data))
			_ = c.Close(websocket.CloseNormal, "")
		},
	}

	done := make(chan struct{})
	dialer.Dial("ws://127.0.0.1:8080/ws", nil, handler,
		func(conn *websocket.Connection, resp *stdhttp.Response, err error) {
			defer close(done)
			if err != nil {
				if resp != nil {
					fmt.Println("refused with", resp.StatusCode)
				}
				fmt.Println("dial:", err)
				return
			}
			_ = conn.WriteText("hello")
		})
	<-done
	time.Sleep(time.Second) // let the server's answer reach the handler
}
```

The third argument is any `websocket.Handler`, so a client connection can take frames with
`Frame` or reassembled messages with `Message`, and a `nil` handler accepts the connection and
ignores its traffic. `Dialer.Go(url, header, handler).Wait()` is the same dial for a caller that
may block. For a client that must carry a raw byte stream after the handshake, the HTTP client's
`StartUpgrade` turns the connection into a `*fibhttp.Tunnel` for a `TunnelHandler`; the WebSocket
handshake itself is that mechanism.

## TLS client options

A `wss://` or `https://` client gets a `*tls.Config` field, and fib runs the record layer after
`crypto/tls`'s handshake. To dial TLS yourself — a protocol of your own, TLS over a proxy or over a
Unix socket — use `fibtls.DialWithOptions`, whose `DialOptions` are the client-side knobs:

```go
err = fibtls.DialWithOptions(engine, "tcp", "127.0.0.1:8443", 5*time.Second, tlsConfig,
	fibtls.DialOptions{
		HandshakeTimeout:  5 * time.Second,
		Blocking:          false, // state-machine handshake: no goroutine parked per connection
		EarlyData:         true,  // 0-RTT for a resumable session
		SessionCache:      fibtls.NewQUICSessionCache(64),
		TLS13CipherSuites: []uint16{tls.TLS_AES_128_GCM_SHA256},
		Dial:              proxy.Dial, // a socks.Dialer's Dial reaches the server through it
	},
	myHandler, func(c *fib.Connection, err error) {
		if err != nil {
			return
		}
		_ = c.Send([]byte("secure ping"))
	})
```

`ServerName` is filled from the address when the config names none, so one config serves many
origins. `Blocking: true` runs the handshake through `crypto/tls` on a handshake-pool worker, which
is the switch to compare the state-machine handshake against; the default parks no goroutine while
waiting. mTLS, session tickets and cipher choices are chapter [10-tls.md](10-tls.md).

## SOCKS and proxy dialing

The HTTP-family clients accept a `Dial` function in place of the engine's dialer — `fibhttp.Client`,
`websocket.Dialer` and `fibtls.DialOptions` — and a `socks.Dialer.Dial` is exactly that function. So
one field routes every request a client makes through a SOCKS proxy:

```go
proxy, err := socks.ParseURL("socks5h://user:secret@127.0.0.1:1080") // h: the proxy resolves names
if err != nil {
	panic(err)
}
config := fibhttp.DefaultClientConfig()
config.Dial = proxy.Dial
client := fibhttp.NewClient(engine, config)
```

A `socks.Dialer` can chain to another proxy through its `Forward` field, and a SOCKS connection is
a normal fib connection, so `Dialer.Dial` opens a UDP association for a `"udp"` network. The
complete program below runs an HTTP origin, a SOCKS5 server and the client that fetches through
it, all on one engine:

```go
package main

import (
	"fmt"
	"io"
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/socks"
)

func main() {
	config := fib.DefaultConfig()
	config.LogSettings = false

	origin := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(200, "text/plain", []byte("through the proxy\n"))
	})
	engine, err := fib.New(config,
		fib.Binding{Addr: "127.0.0.1:8080", Handler: fibhttp.NewHandler(origin)},
		fib.Binding{Addr: "127.0.0.1:1080", Handler: socks.NewServer(socks.Config{})},
	)
	if err != nil {
		panic(err)
	}
	go func() { _ = engine.Run() }()
	defer func() { engine.Stop(); _ = engine.Close() }()

	proxy := &socks.Dialer{Addr: "127.0.0.1:1080"} // Version5, no auth
	clientConfig := fibhttp.DefaultClientConfig()
	clientConfig.Dial = proxy.Dial // every request goes through the proxy
	client := fibhttp.NewClient(engine, clientConfig)
	defer client.Close()

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://127.0.0.1:8080/", nil)
	if err != nil {
		panic(err)
	}
	resp, err := client.Go(req).Wait()
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Print(string(body))
}
```

The proxy side — authentication, `Allow` rules, CONNECT and UDP ASSOCIATE — is chapter
[13-socks-connect.md](13-socks-connect.md).

## Pitfalls

- **Don't `Wait()` inside a handler.** `Future.Wait`, `DialFuture.Wait` and
  `AltSvcFuture.Wait` block the calling goroutine; in a handler that is an engine worker, which is
  the mistake [21-hacks.md](21-hacks.md) opens its anti-pattern list with. Use `Do` with a
  callback and `Retain`/`Release` the request until it arrives.
- **Close the client before the engine.** `engine.Close()` drops the client's connections without
  telling it, so in-flight requests only end by their timeout. `client.Close()` first, then the
  engine.
- **Don't keep using a request after `Do` returns.** The client may still be reading its body and
  headers; the callback is the point at which it is done with them.
- **A response body that streams must be taken.** With `StreamResponseBody`, a callback that
  neither registers `OnBody` nor reads `Body` leaves the rest to be thrown away (and, past a
  threshold, the connection closed). `data` passed to `OnBody` is valid only during the call; copy
  it to keep it.
- **`ClientConfig{}` is not `DefaultClientConfig()`.** `NewClient` fills only a few zero fields;
  a zero `Timeout`, `DialTimeout` or `MaxConnsPerHost` stays zero, which means no timeout and no
  connection cap.
- **One HTTP/1.1 connection carries one request at a time**, and the client does not pipeline. A
  stuck request delays only its own connection; but do set `MaxConnsPerHost` for a burst of slow
  requests, and `MaxPendingPerHost` so a dead host costs a bounded queue rather than a queue that
  grows until the timeouts.
- **A callback is not the place for long work.** Responses are read on engine workers and the
  callback holds one until it returns; a slow callback slows the client, and on HTTP/2 it holds
  every stream the worker reads.

## See also

- [HTTP/1.x](../http1.md#the-client-and-expect-100-continue) — the client's request and response
  heads, streaming bodies, `Expect: 100-continue`
- [HTTP/2](../http2.md#client) — the client's windows, health checks and multiplexing
- [HTTP/3](../http3.md#client) — the QUIC client, Alt-Svc and 0-RTT
- [WebSocket](09-websocket.md) — the connection API the client's handler shares with the server
- [SOCKS and HTTP CONNECT](13-socks-connect.md) — proxy servers, auth and dial chaining
- [Hacks](21-hacks.md) — one engine as server and client, and the callback lifetime rules
- [The README](../../README.md) — the async clients at a glance and the benchmark tables

# HTTP/3

[English](08-http3.md) | [简体中文](08-http3.zh-CN.md)

Package `fib/http3` serves HTTP/3 over QUIC on a UDP binding, with the same `fibhttp.Handler` and `*fibhttp.Context` that serve HTTP/1.x and HTTP/2 — the handler does not know which protocol its request arrived on. QUIC and QPACK are implemented in fib on top of `crypto/tls.QUICConn`, with no quic-go or `x/net` dependency, and TLS 1.3 plus the `h3` ALPN is mandatory: there is no plaintext HTTP/3. This chapter shows an HTTP/3-only server, one engine serving all three protocols, the Alt-Svc announcement that lets browsers find the QUIC endpoint, the client, and the limits that come with QUIC; the protocol reference is [HTTP/3](../http3.md).

## The model

An HTTP/3 server is a UDP binding whose handler is `http3.NewHandler(tlsConfig, app)`. The engine hands each datagram to the `fib.Connection` of its sender's address, and each address that sends a QUIC Initial gets a QUIC connection of its own over that shared engine connection; the server demultiplexes by peer address, not by goroutine per connection. The handshake is a state machine on the engine's workers by default (`BlockingTLSHandshake` parks a goroutine in `crypto/tls` instead). A complete request goes to the engine's handler pool, exactly as over HTTP/2, so handlers run concurrently and a blocking handler holds up only itself. Responses are written as QUIC stream frames and paced by the connection's congestion and flow control, and datagrams are read in batches, so acknowledgements and responses leave packed in as few packets as they fit.

## An HTTP/3 server

The handler is the same function as everywhere else, `c.Request.Proto` reports `HTTP/3.0`, and the binding's network is `udp`:

```go
package main

import (
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
	"github.com/lesismal/fib/tlstest"
)

func main() {
	tlsConfig, _, err := tlstest.Configs() // a throwaway certificate for 127.0.0.1
	if err != nil {
		panic(err)
	}
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.String(stdhttp.StatusOK, "hello over %s\n", c.Request.Proto)
	})
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Network: "udp",
		Addr:    "127.0.0.1:8443",
		Handler: http3.NewHandler(http3.ConfigureTLS(tlsConfig), app),
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

`http3.ConfigureTLS` copies the config with `h3` in its ALPN list and TLS 1.3 required; `NewHandler` applies it itself, and calling it explicitly (as above) only makes the requirement visible. Test with a client that speaks HTTP/3:

```sh
curl --http3 -k https://127.0.0.1:8443/   # needs curl built with HTTP/3
```

Many curl builds (macOS's among them) lack HTTP/3; the client program [below](#the-client) is the fallback.

## One handler for h1, h2 and h3

One engine can serve HTTP/1.x and h2 over TCP on a port and HTTP/3 over UDP on the same port number — they are different transports, so the numbers may match. The TLS config is split because each stack has its own ALPN: `fibhttp.ConfigureTLS` for h1/h2, `http3.ConfigureTLS` for h3. The handler `app` is shared.

```go
package main

import (
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

func main() {
	tlsConfig, _, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		// Tell h1/h2 clients that this origin also speaks HTTP/3 on 8443.
		if !c.Request.ProtoAtLeast(3, 0) {
			c.Header().Set("Alt-Svc", http3.AltSvc(8443))
		}
		_ = c.String(stdhttp.StatusOK, "hello over %s\n", c.Request.Proto)
	})
	engine, err := fib.New(fib.DefaultConfig(),
		fib.Binding{
			Addr:    "127.0.0.1:8443",
			Handler: fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), fibhttp.NewHandler(app)),
		},
		fib.Binding{
			Network: "udp",
			Addr:    "127.0.0.1:8443",
			Handler: http3.NewHandler(tlsConfig, app),
		},
	)
	if err != nil {
		panic(err)
	}
	defer engine.Close()

	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

An HTTP/1.1 or HTTP/2 client sees `Alt-Svc: h3=":8443"; ma=86400` (`curl -ki https://127.0.0.1:8443/` shows it) and may switch its next requests to QUIC; a client that was given the h3 address directly needs no header. A field set on `c.Header()` before answering goes out with `String`, `Respond`, `WriteResponse` and the `ResponseWriter` methods alike.

## Announcing HTTP/3 with Alt-Svc

`http3.AltSvc(port)` is only the header value — `h3=":<port>"; ma=86400` — so setting it on a normal h1/h2 response is the whole integration:

```go
func announce(c *fibhttp.Context) {
	c.Header().Set("Alt-Svc", http3.AltSvc(8443)) // h3=":8443"; ma=86400
	_ = c.String(stdhttp.StatusOK, "this origin also speaks h3\n")
}
```

`http3.AltSvcClient` is the client-side counterpart: it wraps an `*fibhttp.Client` and an `*http3.Client`, follows the header, and falls back to TCP when the QUIC attempt fails. Chapter [20](20-clients.md) covers it.

## Configuring QUIC and TLS

Options that differ from the HTTP/1 and HTTP/2 servers go in `http3.Config`, passed through `NewHandlerWithConfig`. `TLSConfig` is required in the config; `NewHandlerWithConfig` does not fill it from anywhere.

```go
config := http3.DefaultConfig()
config.TLSConfig = tlsConfig             // required: TLS 1.3 and ALPN h3 are added by the handler
config.MaxConcurrentStreams = 100        // requests a client may have open, the default
config.MaxIdleTimeout = 30 * time.Second // keep it below the engine's UDPIdleTimeout (60s)
config.StreamRequestBody = true          // big uploads stream instead of buffering
config.StreamPool.MaxConcurrentHandlers = 16
handler := http3.NewHandlerWithConfig(config, app)
```

The engine-level UDP settings still apply: `Config.UDPIdleTimeout` (60s by default) closes a UDP peer that has been silent, and QUIC's `MaxIdleTimeout` must be below it or the engine closes a quiet connection first. QUIC needs real certificates in production; `tlstest.Configs()` exists for examples and tests, and [TLS](10-tls.md) covers certificates and `http3.ConfigureTLS` in depth.

## The client

`http3.NewClient(engine, config)` sends requests over connections an engine dials, one multiplexed connection per host:port, non-blocking, with `Do` for a callback and `Go` returning a `Future`. The engine can be a client-only engine with no bindings, or, as here, a server's own: `tlstest` certificates are trusted only inside the process that issued them, so the example runs its server on the same engine and prints `HTTP/3.0 200 OK: hello over HTTP/3.0`. Against a real server, set `TLSConfig` to one that trusts its certificate (or leave it nil for the system roots).

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
	// tlstest issues its certificate once per process, so the server the
	// client trusts runs in this program too, on the same engine.
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.String(stdhttp.StatusOK, "hello over %s\n", c.Request.Proto)
	})
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Network: "udp",
		Addr:    "127.0.0.1:8443",
		Handler: http3.NewHandler(serverTLS, app),
	})
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	go engine.Run()

	config := http3.DefaultClientConfig()
	config.TLSConfig = clientTLS
	client := http3.NewClient(engine, config)
	defer client.Close()

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "https://127.0.0.1:8443/", nil)
	if err != nil {
		panic(err)
	}
	resp, err := client.Go(req).Wait() // Future: block instead of taking a callback
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s %s: %s", resp.Proto, resp.Status, body) // HTTP/3.0 200 OK: hello over HTTP/3.0
}
```

The response body is buffered whole before the callback by default; `ClientConfig.StreamResponseBody` hands a large one over as it arrives. A timeout or cancellation resets only its own stream, and only requests the server marks as unprocessed are retried. This is the pointer — chapter [20](20-clients.md) has the full client story.

## Migration, early data and limits

- **NAT rebinding is followed, active migration is not.** A 1-RTT packet from a new address is routed by its Connection ID, validated with `PATH_CHALLENGE`, and the connection continues there (RFC 9000 §9.3); the server still advertises `disable_active_migration`, so a client that probes a path of its own choosing will not hear the answer.
- **0-RTT is off by default.** `http3.Config.EarlyData` on the server and `ClientConfig.EarlyData` on the client enable it; a request that arrives as early data carries `Early-Data: 1`, and only GET, HEAD, OPTIONS and TRACE are served, anything else is answered 425 Too Early. Early data is replayable by design, so the safe-method rule is the protection.
- **No server push.** `Context.Push` returns `stdhttp.ErrNotSupported` over HTTP/3; use 103 Early Hints instead.
- **No DATAGRAM or WebTransport.** A DATAGRAM frame from the peer is an unknown frame and closes the connection with FRAME_ENCODING_ERROR.
- **Memory bounds** are `MaxConcurrentStreams × MaxBodyBytes` per connection (100 × 16MB with the defaults) for whole bodies, and a streamed body is bounded by its QUIC stream window (1MB).
- **Engine shutdown**: `Engine.Shutdown` sends GOAWAY, finishes in-flight requests and closes with `H3_NO_ERROR`; `Engine.Close` sends CONNECTION_CLOSE (`H3_NO_ERROR`) at once, ending the requests in flight, and `Engine.Stop` leaves the connections open until `Close`.
- **Platforms**: like `fib/http`, `fib/http3` builds only on the native Linux, macOS and Windows backends; on the portable backend the package is empty.
- **Interop**: the conformance suite runs the server and the client against quic-go and quiche in CI; see [HTTP/3](../http3.md#conformance-tests).

## Pitfalls

- **There is no plaintext HTTP/3.** A `udp` binding without a TLS config does not serve HTTP/3: QUIC is built on TLS 1.3, and both `ConfigureTLS` (ALPN `h3`) and a certificate are required.
- **Keep `MaxIdleTimeout` below `UDPIdleTimeout`.** With the defaults QUIC's 30s is below the engine's 60s; raising the former past the latter lets the engine reap quiet connections first.
- **Do not write to `Context.Conn`.** On HTTP/3 it is the engine's UDP connection; responses must go through `Context` or the stream framing dies.
- **Two bindings, two protocols, two ports or one number.** TCP :8443 and UDP :8443 are distinct bindings; changing the h1/h2 port without changing the Alt-Svc value sends clients to a port nothing listens on.
- **Set `Alt-Svc` before answering.** `c.Header()` goes out with every kind of answer, but an `Alt-Svc` in a `WriteResponse` header replaces it, and a change after the response has begun has no effect.
- **Retries are stream-scoped.** A timeout cancels its own stream and leaves the others; a request whose connection breaks after part of its response arrived fails rather than being repeated.
- **Handler and request lifetime rules are unchanged.** Request/stream objects are recycled on HTTP/3 too (unless `Config.DisableReuse`); do not keep them past the response and the last `Release`, and do not mutate a `Respond` body after the call.
- **The tutorial's certificate is a test one.** `tlstest.Configs()` is for examples; browsers will not trust it, and real deployments need a certificate for the origin name.

## See also

- [HTTP/3](../http3.md) — the protocol reference: QUIC, timeouts, QPACK, 0-RTT, interop.
- [HTTP/2](07-http2.md) — the multiplexed model HTTP/3 shares, and how handlers run on a pool.
- [HTTP/1.x](06-http1.md) — the request/response API this chapter assumes.
- [TLS](10-tls.md) — certificates, ALPN and `http3.ConfigureTLS` in depth.
- [Clients](20-clients.md) — the HTTP/3 client, `AltSvcClient` and connection management.
- [Configuration](../configuration.md) — engine-level UDP settings and every server config field.

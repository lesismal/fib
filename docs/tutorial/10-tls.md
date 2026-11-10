# TLS

[English](10-tls.md) | [简体中文](10-tls.zh-CN.md)

`fib/tls` runs TLS over fib's connections, with `crypto/tls` doing the cryptography: a
`fibtls.Handler` sits in front of an ordinary `fib.Handler`, plays the handshake, and then hands the
wrapped handler plaintext — so the `http`, `websocket`, `grpc` and raw TCP handlers run over TLS
unchanged. This chapter covers `fibtls.NewServer` and `NewClient`, the ALPN stack that decides which
protocol a connection carries, the client's `DialOptions`, mutual TLS, cipher and version choices,
and DTLS for WebRTC. It assumes the HTTP and WebSocket chapters ([06-http1.md](06-http1.md),
[07-http2.md](07-http2.md), [09-websocket.md](09-websocket.md)) and the `crypto/tls` API.

## The model

- **Two layers.** `crypto/tls` owns the handshake: `Config`, certificates, verification, ALPN and the
  version and cipher choices — the same `*crypto/tls.Config` you would give `tls.Listen`. After the
  handshake, for the common AES-GCM (TLS 1.3 and 1.2) and AES-CBC (TLS 1.2 and 1.1) suites, fib
  seals and opens the records itself. The wrapped handler never sees a record: its `Send` encrypts,
  its `OnData` receives decrypted bytes.
- **Handshakes do not block the engine.** By default a handshake that can run as a state machine
  does, taking no goroutine while it waits for the peer; a `Config` that needs `crypto/tls` runs on
  a worker of a handshake pool kept apart from the engine's workers. `Handler.Blocking` forces the
  second form, and `Handler.HandshakeTimeout` bounds a handshake (10 s by default; a negative value
  means no bound).
- **The wrapped handler sees the connection early.** Its `OnOpen` runs before the handshake and may
  send at once; what it sends is held and encrypted in order after the handshake. `OnData` then
  receives plaintext only. A handler that wants to know when the handshake completed implements
  `tls.HandshakeHandler` (`OnHandshake(*fib.Connection, tls.ConnectionState)`); `fibtls.Handshaking`
  and `fibtls.ConnectionState` answer the same question on demand.
- **ALPN is not interpreted by `fibtls`.** The negotiated protocol is in the connection state; the
  wrapped handler uses it to pick the protocol it serves. The `ConfigureTLS` helpers of the protocol
  packages add the right `NextProtos`: `http.ConfigureTLS` adds `h2` and `http/1.1`,
  `http3.ConfigureTLS` adds `h3` and requires TLS 1.3, and `grpc.ConfigureTLS` adds `h2`.

## A TLS echo server and client

`tlstest.Configs()` issues a throwaway certificate for `localhost` and `127.0.0.1` and a client
config that trusts it, so an example needs no files. Configs are per process, so this example keeps
both ends in one program:

```go
package main

import (
	"log"
	"time"

	fib "github.com/lesismal/fib"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

func main() {
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}
	server, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr: "127.0.0.1:8443",
		Handler: fibtls.NewServer(serverTLS, fib.HandlerFuncs{
			Data: func(c *fib.Connection, data []byte) {
				_ = c.Send(data)
			},
		}),
	})
	if err != nil {
		panic(err)
	}
	defer server.Close()
	go func() { _ = server.Run() }()

	client, err := fib.New(fib.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer client.Close()
	go func() { _ = client.Run() }()

	done := make(chan struct{})
	echo := fib.HandlerFuncs{
		Open: func(c *fib.Connection) {
			_ = c.Send([]byte("hello over TLS\n"))
		},
		Data: func(c *fib.Connection, data []byte) {
			log.Printf("echo: %s", data)
			if state, ok := fibtls.ConnectionState(c); ok {
				log.Printf("version 0x%04x, ALPN %q", state.Version, state.NegotiatedProtocol)
			}
			close(done)
		},
	}
	err = fibtls.Dial(client, "tcp", "127.0.0.1:8443", 5*time.Second, clientTLS, echo,
		func(c *fib.Connection, err error) {
			if err != nil {
				log.Printf("dial: %v", err)
			}
		})
	if err != nil {
		panic(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Println("timed out")
	}
}
```

`NewServer(config, handler)` and `NewClient(config, handler)` both just return a `*fibtls.Handler`
that fib binds like any other handler; `fibtls.Handler` exposes the handshake knobs as fields.
`fibtls.Dial` is `Engine.DialWithHandler` plus a TLS handshake: it fills in `ServerName` from the
address when the config names no server (as `crypto/tls.Dial` does), and its `done` callback fires
when the connection is established, *before* the handshake completes — anything sent from `done` or
`OnOpen` waits for it and goes out encrypted in order.

## HTTPS and HTTP/2

Wrapping package `http`'s handler in TLS gives HTTPS, and `http.ConfigureTLS` adds ALPN so the same
server answers HTTP/2:

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
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8",
			[]byte("hello over "+c.Request.Proto+"\n"))
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
curl -k --http2 https://127.0.0.1:8443/
```

The client's ALPN offer decides: with `h2` in it the connection is served as HTTP/2, otherwise as
HTTP/1.1, and `app` is none the wiser — `c.Request.Proto` reports which one answered. Without
`http.ConfigureTLS` the TLS stack has no `NextProtos`, no client negotiates h2, and the same
listener turns into an HTTP/1.1-only HTTPS server. For real certificates, replace
`tlstest.Configs()` with a config whose `Certificates` (or `GetCertificate`) holds them; `tlstest`
is for tests and examples only.

## What `crypto/tls` still owns

`fib/tls` does not replace the TLS API, it schedules it. These are still `crypto/tls` and should be
configured there:

- certificates and their selection: `Certificates`, `GetCertificate`, `GetConfigForClient`;
- client authentication: `ClientAuth`, `ClientCAs`, and a client's `Certificates`,
  `GetClientCertificate`;
- verification: `RootCAs`, `ServerName`, `InsecureSkipVerify`, `VerifyPeerCertificate`,
  `VerifyConnection` — the callbacks and the verification run on a handshake-pool worker, never on
  the engine's event loop;
- versions and suites: `MinVersion`, `MaxVersion`, and `CipherSuites` for TLS 1.2; TLS 1.3 suites
  have no `crypto/tls` field, so the client's offer order is `Handler.TLS13CipherSuites`;
- resumption: `SessionTicketKey` on a server — one whose handshakes run as state machines issues
  its own tickets, under a key of the `Handler`'s — and `ClientSessionCache` on a client, which
  keeps the connection in `crypto/tls`; a client resumes without a worker through
  `DialOptions.SessionCache` (`Handler.SessionCache`) instead.

What fib adds on top is the data path — record sealing and opening, writes corked into one record
per round, and, on Linux, handing the sealing of large `SendFile` responses to the kernel's TLS
(`Handler.KernelTLSMinFile`, off with `NoKernelTLS`).

## An ALPN-aware handler

The wrapped handler is the one that decides what the negotiated protocol means. A custom
`HandshakeHandler` receives the result once, after the handshake and before any `OnData`, and can
route the connection's plaintext from then on. This one serves h2 and HTTP/1.1 with two
differently configured HTTP handlers:

```go
package main

import (
	stdtls "crypto/tls"
	stdhttp "net/http"
	"sync"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

// alpnSwitch is the handler fibtls wraps. Once ALPN has settled it picks a
// handler for the connection and forwards the plaintext to it.
type alpnSwitch struct {
	h2, h11 fib.Handler
	picked  sync.Map // *fib.Connection -> fib.Handler
}

func (m *alpnSwitch) OnOpen(c *fib.Connection) {} // before the handshake: nothing chosen yet

func (m *alpnSwitch) OnHandshake(c *fib.Connection, state stdtls.ConnectionState) {
	h := m.h11
	if state.NegotiatedProtocol == "h2" {
		h = m.h2
	}
	m.picked.Store(c, h)
	h.OnOpen(c)
	if hh, ok := h.(fibtls.HandshakeHandler); ok {
		hh.OnHandshake(c, state)
	}
}

func (m *alpnSwitch) OnData(c *fib.Connection, data []byte) {
	if h, ok := m.picked.Load(c); ok {
		h.(fib.Handler).OnData(c, data)
	}
}

func (m *alpnSwitch) OnPriorityData(c *fib.Connection, data []byte) {}

func (m *alpnSwitch) OnClose(c *fib.Connection, err error) {
	if h, ok := m.picked.LoadAndDelete(c); ok {
		h.(fib.Handler).OnClose(c, err)
	}
}

func main() {
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.String(stdhttp.StatusOK, "hello over %s\n", c.Request.Proto)
	})
	h2 := fibhttp.DefaultConfig()
	h2.HTTP2Only = true
	h11 := fibhttp.DefaultConfig()
	h11.DisableHTTP2 = true
	sw := &alpnSwitch{
		h2:  fibhttp.NewHandlerWithConfig(h2, app),
		h11: fibhttp.NewHandlerWithConfig(h11, app),
	}
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8443",
		Handler: fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), sw),
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
curl -k --http2 https://127.0.0.1:8443/    # hello over HTTP/2.0, from h2
curl -k --http1.1 https://127.0.0.1:8443/  # hello over HTTP/1.1, from h11
```

The switch remembers its choice per connection and forwards every later callback; the chosen
handler's `OnOpen` runs at the handshake rather than before it. Do not call `c.SetHandler` for
this: the connection's handler is the `fibtls.Handler` itself, and replacing it hands ciphertext to
the new handler. Package `http` already makes this choice on its own — its handler serves HTTP/2
when ALPN chose `h2` — so most programs only add `http.ConfigureTLS` and keep one handler stack;
a switch like this is for protocols fib does not pair for you.

## Client dial options

`fibtls.DialOptions` adds what `fibtls.Dial` cannot take as arguments:

```go
err := fibtls.DialWithOptions(engine, "tcp", "127.0.0.1:8443", 5*time.Second, clientTLS,
	fibtls.DialOptions{
		HandshakeTimeout: 3 * time.Second,
		Blocking:         true,
	}, handler, func(c *fib.Connection, err error) {
		if err != nil {
			log.Printf("dial: %v", err)
		}
	})
```

- `HandshakeTimeout` bounds the handshake, as `Handler.HandshakeTimeout` does.
- `Blocking` runs the handshake through `crypto/tls` on a handshake-pool worker even when it could
  run as a state machine — the switch for comparing the two, or for a config that behaves
  differently under them.
- `TLS13CipherSuites` orders the client's TLS 1.3 offers; `SessionCache` and `EarlyData` enable
  session resumption and 0-RTT on the handshakes that run as state machines (the server side of
  0-RTT is `Handler.EarlyData` and `EarlyDataHandler`, as `http.Config.EarlyData` uses).
- `Dial` replaces the underlying dial, with the same arguments as `Engine.DialWithHandler`: a
  `socks.Dialer`'s `Dial` reaches the server through a SOCKS proxy.

`fibtls.NewClient` is the lower-level form for callers that drive `Engine.DialWithHandler`
themselves.

## Mutual TLS

Mutual TLS is `crypto/tls` configuration; fib only schedules the handshake:

```go
clientCAs := x509.NewCertPool()
if !clientCAs.AppendCertsFromPEM(caPEM) {
	log.Fatal("no certificates in the client CA file")
}
serverTLS := &stdtls.Config{
	Certificates: []stdtls.Certificate{serverCert},
	ClientAuth:   stdtls.RequireAndVerifyClientCert,
	ClientCAs:    clientCAs,
	MinVersion:   stdtls.VersionTLS12,
}
clientTLS := &stdtls.Config{
	Certificates: []stdtls.Certificate{clientCert}, // the client's own certificate
	RootCAs:      serverCAs,
	ServerName:   "api.example.com",
}
```

`RequireAndVerifyClientCert` makes the server verify the client's chain against `ClientCAs`; a
client without a certificate or with one from another CA fails the handshake, and the wrapped
server handler gets `OnClose` with the error rather than any `OnData`. Once the handshake is done,
`fibtls.ConnectionState(c)` exposes `PeerCertificates`, `VerifiedChains` and the rest, so the
handler can read the client's identity after `OnHandshake` (or from the first `OnData`, by which
time the handshake is complete).

## Cipher suites and versions

TLS versions and TLS 1.2 suites are `crypto/tls.Config` fields; TLS 1.3 suites are a `Handler` field
because `crypto/tls` has no config field for them:

```go
serverTLS := &stdtls.Config{
	Certificates: []stdtls.Certificate{cert},
	MinVersion:   stdtls.VersionTLS12,
	MaxVersion:   stdtls.VersionTLS13,
	CipherSuites: []uint16{
		stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	},
}
client := fibtls.NewClient(clientTLS, handler)
client.TLS13CipherSuites = []uint16{stdtls.TLS_AES_128_GCM_SHA256}
```

Leaving `CipherSuites` empty keeps Go's safe defaults. Prefer controlling `MinVersion` to enumerating
suites: the default suites are chosen for security and speed on the machine, and TLS 1.3 suites are
not affected by `CipherSuites`.

## DTLS for WebRTC

DTLS is not TLS over a stream, so it is a separate API: `fibtls.NewDTLSServer` and
`fibtls.NewDTLSClient` return a `*DTLSConn` that runs DTLS 1.2 (RFC 6347) with DTLS-SRTP (RFC 5764)
sans-IO — the caller moves its datagrams. Package `webrtc` uses it for media and data channels; see
chapter [16-webrtc.md](16-webrtc.md) for the WebRTC side. For ordinary TCP services, use
`fibtls.NewServer` and `fibtls.Dial`, not the DTLS API.

## Pitfalls

- **`tlstest` is per process.** Its certificate and CA are issued once per process, so two
  separately started programs do not trust each other. Use it for tests within one process, or
  supply real certificates through `Config.Certificates`/`RootCAs`.
- **Change the config before the first handshake.** The `Handler` clones the config the first time
  it is used; changes made afterwards are not seen, and a `Config` in use must not be changed, as
  `crypto/tls` requires anyway.
- **A missing ALPN list silently means HTTP/1.1.** TLS succeeds, h2 clients fall back to HTTP/1.1,
  and gRPC clients fail with a protocol error. Add the protocol's `NextProtos` with its
  `ConfigureTLS` helper.
- **No plaintext fallback exists.** A binding whose handler is `fibtls.NewServer` accepts TLS only;
  there is no sniffing and no "try TLS then plain". For several protocols on one port use package
  `mux` or `multiproto`, as chapter [15-multiprotocol.md](15-multiprotocol.md) shows.
- **Some `Config` fields keep a connection in `crypto/tls`.** `GetConfigForClient` on a server and
  `ClientSessionCache` or renegotiation on a client make the handshake run on a pool worker and
  leave the records to `crypto/tls`, which is slower; it still works, but measure before relying
  on them.
- **The `done` callback is not the end of the handshake.** It fires when the TCP connection is
  established; failures such as a bad certificate arrive later, in `OnClose`.
- **Client verification must have a name.** A client `Config` with `RootCAs` but no `ServerName` and
  no `InsecureSkipVerify` fails; `fibtls.Dial` fills it in, `NewClient` does not.

## See also

- [HTTP/2](07-http2.md) — ALPN, h2 over TLS and 0-RTT, the reference for the TLS record path
- [HTTP/3](08-http3.md) — QUIC's TLS 1.3, `http3.ConfigureTLS` and h3 ALPN
- [WebSocket](09-websocket.md) — `wss://` from the WebSocket client and server
- [WebRTC](16-webrtc.md) — DTLS 1.2 and DTLS-SRTP, the reason the DTLS API exists
- [Clients](20-clients.md) — the HTTP/2 and HTTP/3 clients that use these dial options
- [The README](../../README.md) — the TLS feature summary

# Gateway example

[中文](README.zh-CN.md)

A reverse proxy / gateway built on fib. It accepts **HTTP/1.1, HTTP/2, HTTP/3 and WebSocket** and
forwards to upstreams by path prefix. Nothing blocks an engine worker: request bodies, upstream
requests, upstream response bodies, WebSocket dials and WebSocket messages are all handled in
callbacks.

```text
examples/gateway/
├── server/     the gateway command: listeners for HTTPS/h2, HTTP/3 and cleartext
│   └── proxy/      the gateway itself (routing, HTTP forwarding, WebSocket relay)
├── upstream/   a demo backend command
│   └── backend/    the demo backend's handlers (server's tests use them too)
└── client/     a demo client that drives the gateway over every protocol
```

## Run it

Three terminals, in this order:

```sh
go run ./examples/gateway/upstream
go run ./examples/gateway/server
go run ./examples/gateway/client
```

The client prints one line per check and ends with `all checks passed`:

```text
ok   HTTP/3   -> gateway -> h2 download: HTTP/3.0, 8388608 bytes in 7680 pieces, 49ms
ok   wss:// -> gateway -> h3 websocket: subprotocol "superchat", text and 1024 KiB binary echoed, closed code=4001 reason="bye"
```

Self-signed certificates are issued automatically: the upstream writes its own to
`$TMPDIR/fib-example-cert.pem` (the gateway trusts it through `-ca`), and the gateway writes its own to
`$TMPDIR/fib-gateway-cert.pem` (the client trusts it through `-ca`). Pass `-cert` and `-key` to use real ones.

## Ports

| Program  | Address | Serves |
|----------|---------|--------|
| upstream | `127.0.0.1:9000` | cleartext HTTP/1.1 and ws:// |
| upstream | `127.0.0.1:9443` | TCP: HTTPS (HTTP/2, HTTP/1.1); UDP: HTTP/3 |
| server   | `127.0.0.1:8081` | cleartext HTTP/1.1, h2c, ws:// (`-plain-addr`, empty to disable) |
| server   | `127.0.0.1:8446` | TCP: HTTPS (HTTP/2, HTTP/1.1), wss://; UDP: HTTP/3 (`-addr`) |

## Routes

A route is `prefix=target`, passed with the repeatable `-route` flag. The longest matching prefix wins,
and the prefix is stripped from the path that goes upstream (`/api/users` on `-route /api=http://host/v1`
reaches the upstream as `/v1/users`). The target's scheme chooses how the upstream is reached:

| Target | Upstream protocol |
|--------|-------------------|
| `http://host:port` | HTTP/1.1 |
| `https://host:port` | HTTP/2 if the server picks it through ALPN, otherwise HTTP/1.1 |
| `h3://host:port` | HTTP/3 |

WebSocket requests go to `ws://` for an `http://` target and `wss://` for the others (an `h3://` target uses
`wss://` on the same host and port over TCP, since the dialer speaks WebSocket over HTTP/1.1).

Without `-route` the gateway uses the demo upstream:

```text
/h1 -> http://127.0.0.1:9000     HTTP/1.1
/h2 -> https://127.0.0.1:9443    HTTP/2
/h3 -> h3://127.0.0.1:9443       HTTP/3
```

## Flags

`server`:

| Flag | Default | Meaning |
|------|---------|---------|
| `-addr` | `127.0.0.1:8446` | TLS listen address (TCP and UDP of the same number) |
| `-plain-addr` | `127.0.0.1:8081` | cleartext listen address; empty disables it |
| `-route` | see above | `prefix=target`, repeatable |
| `-timeout` | `5m` | longest an upstream request may take, body included; `0` for no limit |
| `-max-request-body` | `64MiB` | largest request body forwarded |
| `-subprotocols` | `chat,superchat` | WebSocket subprotocols accepted from clients and asked of the upstream |
| `-cert`, `-key`, `-cert-out` | self-signed | the gateway's certificate |
| `-ca`, `-insecure` | the upstream example's certificate | how the upstreams are verified |

`client`: `-addr`, `-plain-addr`, `-size` (download size, default 8 MiB), `-ca` (default: the gateway's
certificate), `-insecure`. `upstream`: `-addr`, `-plain-addr`, `-cert`, `-key`, `-cert-out`.

## Try it by hand

With the upstream and the gateway running (`curl` needs HTTP/3 support for `--http3`):

```sh
curl -k --http1.1 -d hello https://127.0.0.1:8446/h2/echo
curl -k --http2   'https://127.0.0.1:8446/h1/stream?n=5'
curl -k --http3   -o /dev/null -w '%{size_download}\n' 'https://127.0.0.1:8446/h3/download?size=104857600'
curl -i 'http://127.0.0.1:8081/h1/status?code=418'
```

The demo upstream serves `/echo`, `/download?size=N`, `/stream?n=5&delay=200ms` (ends with a trailer),
`/upload`, `/status?code=N` and `/ws` (an echo that offers the subprotocols `chat` and `superchat`, and closes
with code 4002 when sent the text `close`).

## What the gateway does

- **Request bodies** that arrive after the header are taken with `Context.OnBody`; small ones arrive with the
  request. The upstream request goes out with `Client.Do` once the body is complete, and the handler has
  returned by then (it `Retain`ed the request).
- **Responses** are written from the upstream client's callback, piece by piece with `ClientResponse.OnBody`, so a
  download of any size passes through in constant memory and the slower peer paces the other (TCP, HTTP/2 and
  QUIC flow control). Trailers are forwarded.
- **Headers**: hop-by-hop headers are dropped; `X-Forwarded-For/Proto/Host` and `Via` are added; responses on
  HTTP/1.1 and HTTP/2 carry `Alt-Svc` so browsers can move to HTTP/3.
- **Errors**: an unreachable upstream is `502`, a slow one `504`, an unknown prefix `404`; a client that goes away
  cancels the upstream request; an upstream that fails mid-body aborts the downstream response.
- **WebSocket** works on HTTP/1.1, and on HTTP/2 and HTTP/3 through extended CONNECT (RFC 8441 / RFC 9220). Messages
  and close codes are relayed in both directions.

## Limits

- **Request bodies are collected before they are forwarded**, up to `-max-request-body`, because a client request is
  sent whole (`Client.Do` does not stream a request body).
- **WebSocket upgrades the client first and dials the upstream after**, because `Context.Upgrade` has to be called
  before the handler returns. An upstream that cannot be reached therefore closes the client's connection with code
  1014 (Bad Gateway) rather than answering HTTP 502. What the client sends meanwhile is queued.
- On HTTP/2 and HTTP/3, `Context.Write` waits for the client once about 64 KB are held back by flow control, which
  paces the upstream through its own flow control.
- The WebSocket client in fib speaks HTTP/1.1 only, so the tests and the demo client cover ws:// and wss://; the
  gateway's HTTP/2 and HTTP/3 WebSocket path is for browsers and other clients.

## Tests

```sh
go test -race ./examples/gateway/...
```

They start an upstream and a gateway in-process and cover the 3×3 protocol matrix, large uploads, cancellation,
timeouts, 404/502/504, a failing upstream, slow clients, and WebSocket relaying with close codes.

# HTTP/3: Limitations, Intentional Omissions and Planned Improvements

[English](http3.md) | [简体中文](http3.zh-CN.md)

This document records the boundaries of the Go `http3` package: how it
currently behaves in ways users need to know about, what was deliberately left
out, and where it can be improved. For usage, see the
[HTTP/3 section of the Go README](../go/README.zh-CN.md#http3-子-package) (Chinese).

The implementation lives in [`go/http3`](../go/http3), with the QUIC transport
in [`go/http3/internal/quic`](../go/http3/internal/quic) and QPACK in
[`go/http3/internal/qpack`](../go/http3/internal/qpack). All three are written
from scratch. The TLS 1.3 handshake is the standard library's
`crypto/tls.QUICConn`; there is no dependency on quic-go or `golang.org/x/net`.

## What is supported (overview)

| Area | Content |
| --- | --- |
| QUIC | Version 1; Initial, Handshake and 1-RTT packet number spaces; AES-128-GCM, AES-256-GCM and ChaCha20-Poly1305 packet and header protection; key updates in both directions (started here when a key reaches its AEAD limit, and answered when the peer starts one) and a limit on decryption failures; Retry on the client; Version Negotiation; stateless reset; RFC 9002 loss detection, PTO and NewReno congestion control; the server's 3x amplification limit; connection- and stream-level flow control in both directions; MAX_STREAMS; idle timeout and optional keep-alive |
| Server | Shares one `Handler` and `Context` with HTTP/1 and HTTP/2; 1xx interim responses, automatic 100 Continue, request and response trailers, graceful `Response.Close` via GOAWAY, 413/431/417, malformed requests (including an `:authority` and `Host` that disagree) reset with H3_MESSAGE_ERROR, protocol errors closing the connection with RFC 9114 and RFC 9204 codes, `Request.TLS`, an `AltSvc` helper |
| Client | Asynchronous `Do`/`Go`; one multiplexed connection per host:port; honors MAX_STREAMS and queues the rest; cancellation resets only its own stream; requests the server marks as unprocessed (GOAWAY, H3_REQUEST_REJECTED) are retried on a new connection; request and response trailers; a peer that breaks the protocol ends the connection with its error code |
| Conformance | Both directions against quic-go, and protocol errors written frame by frame, all run in CI |
| Interop | quic-go client and server, both directions, including 5% packet loss; the client against the live services of Cloudflare, Google, nginx, Facebook (mvfst), Varnish and quiche |

All of it is covered by tests that run in CI; see [Conformance tests](#conformance-tests).

## Current limitations

Behavior users need to be aware of.

### Connections are keyed by peer address; no migration

- The engine's UDP socket hands each datagram to the `fib.Connection` of its
  sender's IP:port, and each address has one QUIC connection. Routing is **not
  by Connection ID**. When a client changes networks or its NAT rebinds, the
  old connection receives nothing more; it ends on idle timeout or when the
  client reconnects. The server advertises `disable_active_migration`.
- For the same reason, connections cannot be spread across processes or
  `SO_REUSEPORT` sockets.
- Only the Connection ID from the handshake is used; NEW_CONNECTION_ID is never
  sent. CIDs the peer offers are recorded and retire_prior_to is honored, but
  this side never rotates CIDs. PATH_CHALLENGE is answered but never sent,
  and at most the 4 latest answers wait to go out (RFC 9000 §8.2.2 asks only
  that the latest challenge be answered); preferred_address is ignored.

### Working with the engine

- QUIC's `MaxIdleTimeout` (default 30s) must stay below the engine's
  `Config.UDPIdleTimeout` (default 60s), or the engine closes a quiet peer first.
- The engine queues at most 1024 unprocessed datagrams per UDP connection; more
  are dropped and QUIC treats them as lost.
- The QUIC layer relies on the engine handing every UDP datagram to it as a
  copy of its own: it keeps those slices, without copying, while it reorders
  data and waits for keys, and keeps a request body that arrived in one piece
  where it arrived.
- The server takes a connection's datagrams in bursts, as a
  `fib.DatagramsHandler`: whatever has piled up by the time the connection's
  worker runs is processed together, and the acknowledgement and the
  responses written meanwhile leave in as few packets as they fit in.
- `Engine.Stop`/`Close` sends neither CONNECTION_CLOSE nor GOAWAY; connections
  are dropped and clients find out only when they time out.
- Like the `http` package, `http3` builds only on the three native backends
  (Linux, macOS, Windows); on the portable backend (e.g. FreeBSD) the package is
  empty.

### Whole bodies

- A request body is read into memory in full before the handler runs, and a
  response is given at once as `Response.Body []byte`; the client likewise
  buffers the whole response body before the callback, so SSE and streaming
  uploads or downloads are not possible. `Context`'s `http.ResponseWriter`
  methods work, trailers included, but as on HTTP/2 the response they write
  is held until the handler returns and sent whole; only HTTP/1 streams it.
- Memory bounds: a server connection can hold up to about
  `MaxConcurrentStreams × MaxBodyBytes` (100 × 16MB by default); a single
  client response is bounded by `MaxResponseBodyBytes`.
- A request that is complete goes to the handler pool that `Config.StreamPool`
  describes — the same pool the HTTP/2 server uses — so the requests one client
  has open on a connection are served concurrently and a handler that blocks
  holds up only itself. `StreamPool.MaxConcurrentHandlers` bounds how many of
  one connection's requests run at once, the last of them on the goroutine
  reading the connection, which then reads nothing further until it returns;
  1, like `StreamPool.Disable`, serializes the handler calls of a connection
  again, as they were before the pool existed.
- Do not write with `Context.Conn.Send`: `Conn` is the UDP connection, and all
  output must go through `Context`.

### Fixed parameters

These are constants, or exist only in the internal `quic.Config` without being
exposed through `http3.Config` or `http3.ClientConfig`:

| Parameter | Value |
| --- | --- |
| Datagram size | 1200 bytes, no PMTU discovery; a server can raise it up to 1452 with `Config.MaxDatagramSize` on a network known to carry that |
| Per-stream receive window | 1MB, replenished when half is consumed |
| Connection receive window | 16MB, replenished when half is consumed |
| Unidirectional streams the peer may open | 16 |
| Bidirectional stream limit the client advertises | 1 (servers may not open request streams) |
| QPACK dynamic table capacity | 0 |
| ACK policy | Every 2 ack-eliciting packets, or after at most 25ms |
| Received packet number ranges remembered | 32; older packets are treated as duplicates |
| Keep-alive | Off on the client by default; the connection closes after 30s idle and the next request handshakes again |

### Protocol details

- **Closing**: after sending CONNECTION_CLOSE the connection is released at
  once, without the 3×PTO closing period of RFC 9000 §10.2 and without repeating
  CONNECTION_CLOSE for packets that arrive later. Receiving CONNECTION_CLOSE
  also releases at once, with no draining period. If the close is lost, the peer
  ends only on a stateless reset or idle timeout.
- **Key updates**: this side starts one once a key has protected as many
  packets as RFC 9001 §6.6 allows (about 2^23 for AES-GCM), and answers the
  peer's; past the integrity limit on failed decryptions the connection ends
  with AEAD_LIMIT_REACHED. What it does not do is rotate keys on a timer or
  on request.
- **Stateless reset**: the reset key is random per `ServerHandler` and not
  configurable. After a server restart the key changes, so clients cannot
  recognize resets for connections from before it. Only clients recognize
  stateless resets.
- **BLOCKED frames**: DATA_BLOCKED, STREAM_DATA_BLOCKED and STREAMS_BLOCKED are
  never sent.
- **Congestion control**: NewReno only, with no pacing, no app-limited
  detection and no persistent congestion. A datagram the send buffer has no room
  for (`EAGAIN`) is dropped and treated as lost.
- **The peer's `SETTINGS_MAX_FIELD_SECTION_SIZE`** is parsed but not checked
  when sending.
- **Priorities**: the `priority` header and PRIORITY_UPDATE frames are ignored;
  streams with data to send take turns.
- **ECN**: ECN marks are neither set nor reported. ACK_ECN frames are parsed,
  but their counts are ignored.

### Client

- Only `https://`. The client does not switch from HTTP/1.1 or HTTP/2 to HTTP/3
  on `Alt-Svc`, nor fall back to TCP when UDP is blocked; combine `http.Client`
  and `http3.Client` in the application if needed.
- A request whose connection fails after part of its response has arrived
  fails and is not retried. Retries happen only for requests the server marks as
  unprocessed (GOAWAY or H3_REQUEST_REJECTED) and for requests still queued.
- Requests with `Expect: 100-continue` do not wait for the 100; the body is sent
  together with the header.
- With `TLSConfig.ClientSessionCache` set, session resumption (1-RTT) should
  work, but it has not been tested.

## Intentionally not implemented

| Feature | Reason |
| --- | --- |
| 0-RTT | 0-RTT requests can be replayed, which needs handlers that recognize and refuse non-idempotent requests; the gain is not worth the complexity. The client sends no early data; the server drops 0-RTT packets, which the client then resends as 1-RTT. |
| Connection migration / CID routing | Needs Connection ID demultiplexing in the engine's UDP layer, which changes the engine's model of keying UDP connections by address. |
| QPACK dynamic table | Both sides advertise a capacity of 0: no head-of-line blocking from blocked streams, silent encoder and decoder streams, and no extra state or memory. The cost is that repeated headers are sent in full every time. |
| Server push | No major browser enables HTTP/3 push. The client sends no MAX_PUSH_ID, and `Context.Push` returns `http.ErrNotSupported`. Use 103 Early Hints (`WriteInterim`) for preloading. |
| Server-sent Retry / NEW_TOKEN | Address validation relies on the handshake and the 3x amplification limit, which spares issuing and checking tokens. The client handles a Retry from a server but ignores NEW_TOKEN. |
| QUIC v2 (RFC 9369) and other versions | Version 1 is enough in practice. The client fails on Version Negotiation. |
| DATAGRAM (RFC 9221), Extended CONNECT (RFC 9220), WebTransport | They need APIs for streaming or unreliable datagrams, which the current handler model lacks. A DATAGRAM frame from the peer is an unknown frame and closes the connection with FRAME_ENCODING_ERROR. |
| RFC 9218 extensible priorities | With whole bodies, scheduling gains little. |
| ECN | Needs reading and writing the IP header's ECN bits in the engine's UDP layer; the gain is mostly in congestion control. |

## Planned improvements

In order of priority.

### 1. Correctness and hardening (high)

- **Control frame and reset floods**: there is no rate limit on PING, MAX_*
  frames, or streams opened and reset over and over (as in HTTP/2 Rapid Reset).
- **Handshake cost**: the server neither sends Retry nor limits concurrent
  handshakes, so a flood of Initials from spoofed addresses can burn CPU on TLS.
  Consider enabling Retry under load.
- **Closing and draining states**: implement the 3×PTO closing period and
  CONNECTION_CLOSE retransmission; allow a fixed stateless reset key; send GOAWAY
  and CONNECTION_CLOSE when the `Engine` stops.

### 2. Performance (medium)

- **Pacing**: send evenly according to the congestion window and RTT, and stop
  the round on `EAGAIN` instead of letting datagrams drop. Measured: 20 MiB each
  way takes about 7s under 5% random loss versus about 0.3s over lossless
  loopback; under loss, throughput is bound mainly by this and by NewReno.
- **PMTU discovery (DPLPMTUD, RFC 8899)**: Ethernet paths usually carry about
  1450-byte datagrams, which would cut per-packet header and AEAD overhead.
  Until then, `Config.MaxDatagramSize` sets a larger size by hand.
- **Congestion control**: add CUBIC or BBR, app-limited detection and persistent
  congestion.
- **Batched I/O**: GSO/GRO and `sendmmsg`/`recvmmsg`, which need the engine's
  UDP layer.
- **Fewer copies and allocations**: sending copies once in `Write`, into a
  pooled buffer, and again when packets are built; receiving copies in the
  engine, and in the HTTP/3 frame parser only a frame split across pieces.
  Every sent packet still allocates the record loss recovery keeps of it.
- **Data structures**: sent packets live in a slice, so ACK processing and loss
  detection scan linearly; every round of sending resets a `time.Timer`.
- **ACK frequency**: implement the ACK_FREQUENCY extension to send fewer ACKs
  under heavy traffic.
- **ChaCha20-Poly1305** is pure Go and slower than assembly. Machines with AES
  hardware negotiate AES-GCM first, so it is rarely the bottleneck.
- **Benchmarks**: add QUIC and HTTP/3 benchmarks (single-connection throughput,
  concurrent requests, throughput under loss) and profile with pprof.

### 3. Streaming bodies and the handler model (medium)

- As for HTTP/1 and HTTP/2: streaming request bodies and response writes, with
  receive-window replenishment tied to actual consumption for end-to-end
  backpressure. This is also what Extended CONNECT and WebTransport need.

### 4. Configurability (low)

- Expose keep-alive, receive windows, the unidirectional stream limit and the
  server's handshake timeout in `http3.Config` and `http3.ClientConfig`.
- Honor the peer's `SETTINGS_MAX_FIELD_SECTION_SIZE` when sending.
- Send request trailers.
- Client: pick HTTP/3 automatically from `Alt-Svc`, and fall back to TCP when
  UDP is blocked.

### 5. Test coverage (low)

The conformance suite is done; see [Conformance tests](#conformance-tests).
What is still not covered end to end:

- ChaCha20-Poly1305 negotiated in a real handshake: the development machine
  and the CI runners have AES hardware, so handshakes always pick AES-GCM, and
  only RFC 9001's test vectors cover that path.
- Session resumption (`TLSConfig.ClientSessionCache`).
- The concurrency problems only load testing shows. Frame parsing, building
  requests, QPACK decoding, QUIC packet headers, transport parameters and
  frame handling all have fuzz targets, which the `Fuzz the parsers` CI job
  runs for 20 seconds each.
- Consider joining the
  [QUIC Interop Runner](https://github.com/quic-interop/quic-interop-runner) for
  continuous interop testing against quiche, ngtcp2, mvfst and the rest.

## Conformance tests

The suite is `go/http3/http3_conformance_test.go` (its tests start with
`TestHTTP3Conformance`) and `go/http3/protocol_test.go` (protocol errors
written frame by frame). The `HTTP/3 conformance` CI job runs them on Linux,
macOS and Windows.

The standard library has no HTTP/3, so the peer for the interop cases is
quic-go — but it lives in `go/http3/interop`, a **module of its own**: fib's
`go.mod` gains no dependency and `go build ./...` does not see it. The tests
build it as a program and drive it as a subprocess, a line of JSON at a time.
When it cannot be built, for want of network access for instance, those cases
skip; CI sets `FIB_REQUIRE_H3_INTEROP=1`, which turns a skip into a failure.

| Peer | What it checks |
| --- | --- |
| quic-go's HTTP/3 client, against the fib server | GET/POST/PUT/HEAD, request and response headers, several cookies and Set-Cookies, 4 MiB uploads and downloads checked by hash, 50 concurrent requests on one connection, 204, response trailers, request trailers, 103 Early Hints, 100-continue, 417, 413, 431, status codes |
| quic-go's HTTP/3 server, against the fib client | The same ground from the other side, and: the connection still serves after a timeout, a request in flight finishes when the server retires the connection with GOAWAY, a server that validates the address with Retry, and a certificate the client does not trust |
| The suite's own hand-written HTTP/3 client (no dependency), against the fib server | What a sound implementation never sends: DATA before HEADERS, a control stream that does not start with SETTINGS, a second control stream, a request missing a pseudo-header, a reference to the QPACK dynamic table, CANCEL_PUSH, a lowered MAX_PUSH_ID, a raised GOAWAY, a QPACK insertion, an `:authority` and `Host` that disagree, and neither of them |
| The suite's own hand-written HTTP/3 server (no dependency), against the fib client | How the client polices its peer: MAX_PUSH_ID from a server, CANCEL_PUSH, GOAWAY naming another kind of stream, a raised GOAWAY, a QPACK insertion, and a response that refers to the dynamic table |
| Two QUIC connections over an in-memory path | Handshake, transfers under 5% and 20% loss, stream limits, resets, application close, idle timeout, keep-alive, stateless reset, key updates (with the AEAD limits lowered) and the decryption failure limit |
| Fuzzing (`go test -fuzz`) | The parsers that take untrusted bytes: the HTTP/3 frame parser, building requests from fields, QPACK decoding and its round trip, QUIC packet headers, transport parameters and 1-RTT frame handling |

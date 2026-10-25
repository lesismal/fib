# HTTP/3: Limitations, Intentional Omissions and Planned Improvements

[English](http3.md) | [简体中文](http3.zh-CN.md)

This document records the boundaries of the Go `http3` package: how it
currently behaves in ways users need to know about, what was deliberately left
out, and where it can be improved. For usage, see the
[HTTP/3 section of the Go guide](guide.zh-CN.md#http3-子-package) (Chinese).

The implementation lives in [`http3`](../http3), with the QUIC transport
in [`http3/quic`](../http3/quic) and QPACK in
[`http3/qpack`](../http3/qpack). All three are written
from scratch. The TLS 1.3 handshake is the standard library's
`crypto/tls.QUICConn`; there is no dependency on quic-go or `golang.org/x/net`.

## What is supported (overview)

| Area | Content |
| --- | --- |
| QUIC | Version 1; Initial, Handshake and 1-RTT packet number spaces; AES-128-GCM, AES-256-GCM and ChaCha20-Poly1305 packet and header protection; key updates in both directions (started here when a key reaches its AEAD limit, and answered when the peer starts one) and a limit on decryption failures; Retry, sent by the server under load and handled by the client; Version Negotiation; stateless reset; RFC 9002 loss detection, PTO and persistent congestion; CUBIC (RFC 9438, the default) or NewReno congestion control, with pacing and app-limited detection; the server's 3x amplification limit; connection- and stream-level flow control in both directions; MAX_STREAMS; idle timeout and optional keep-alive |
| Server | Shares one `Handler` and `Context` with HTTP/1 and HTTP/2; 1xx interim responses, automatic 100 Continue, request and response trailers, graceful `Response.Close` via GOAWAY, 413/431/417, malformed requests (including an `:authority` and `Host` that disagree) reset with H3_MESSAGE_ERROR, protocol errors closing the connection with RFC 9114 and RFC 9204 codes, `Request.TLS`, an `AltSvc` helper, extended CONNECT (RFC 9220) switched to a tunnel with `Context.Upgrade`, which is how `websocket.ServerHandler.Upgrade` serves WebSocket over HTTP/3 |
| Client | Asynchronous `Do`/`Go`; one multiplexed connection per host:port; honors MAX_STREAMS and queues the rest; cancellation resets only its own stream; requests the server marks as unprocessed (GOAWAY, H3_REQUEST_REJECTED) are retried on a new connection; `Expect: 100-continue` waits for the 100; request and response trailers; a peer that breaks the protocol ends the connection with its error code |
| Resumption and 0-RTT | Session tickets, resumption, and 0-RTT early data, for the server (`Config.EarlyData`) and the client (`ClientConfig.EarlyData`); see [0-RTT](#0-rtt) |
| Conformance | Both directions against quic-go, and protocol errors written frame by frame, all run in CI |
| Interop | quic-go client and server, both directions, including 5% packet loss; quiche client and server, both directions, in a container (`http3/quicheinterop`); the client against the live services of Cloudflare, Google, nginx, Facebook (mvfst), Varnish and quiche |

All of it is covered by tests that run in CI; see [Conformance tests](#conformance-tests).

## Current limitations

Behavior users need to be aware of.

### Connections follow a NAT rebinding; no active migration

- The engine's UDP socket hands each datagram to the `fib.Connection` of its
  sender's IP:port, and a QUIC connection hangs off the address its client
  started from. A 1-RTT packet from an address with no connection is routed
  by its Connection ID to the connection it belongs to, which is how a
  client whose NAT rebound keeps its connection: once an authentic packet
  from the new address proves to be the client's latest (numbered above
  every one before it, with a frame other than a probing one), the server
  sends there, at most three times what arrived there, validates the address
  with PATH_CHALLENGE, and closes the old path once the client answers
  (RFC 9000 §9.3). A client that does not answer in time is taken back to
  the path it left, and a later packet on the old path takes it back too.
  Congestion control and RTT start over unless only the port changed, and
  datagrams drop to 1200 bytes until the new path is validated. Requests
  after the move report the new address in `Request.RemoteAddr`.
- Under `Config.IOPollers` with `ReusePort` on Linux, the new address may
  fall to another poller's socket; the connection follows it there, since a
  QUIC connection takes datagrams from any goroutine.
- The server still advertises `disable_active_migration`, and issues no
  Connection IDs beyond the handshake's: a client may not move on purpose,
  which would take a fresh Connection ID for the new path. Its NAT may.
- Only the Connection ID from the handshake is used; NEW_CONNECTION_ID is never
  sent. CIDs the peer offers are recorded and retire_prior_to is honored, but
  this side never rotates CIDs. PATH_CHALLENGE is answered, on the path the
  connection sends on, and at most the 4 latest answers wait to go out
  (RFC 9000 §8.2.2 asks only that the latest challenge be answered);
  preferred_address is ignored.

### Working with the engine

- QUIC's `MaxIdleTimeout` (default 30s) must stay below the engine's
  `Config.UDPIdleTimeout` (default 60s), or the engine closes a quiet peer first.

### Server timeouts

`http3.Config` bounds every wait a client could stretch out, and
`DefaultConfig` sets each of them:

| Bounds | Setting | `DefaultConfig` | On expiry |
| --- | --- | --- | --- |
| The QUIC handshake | `HandshakeTimeout` | QUIC's 10s | the connection closes |
| A connection that hears no packet | `MaxIdleTimeout` (QUIC's) | 30s | the connection closes |
| A connection with no request open | `IdleTimeout` | 2 minutes | GOAWAY, then the connection closes |
| A request's header | `ReadHeaderTimeout` | 10s | the stream is reset with `H3_REQUEST_CANCELLED` |
| A request's body | `ReadTimeout`, `ReadBodyTimeout` | 5 minutes (body) | a body streaming to its handler fails with `os.ErrDeadlineExceeded`; one still collected, its stream is reset |
| A held body | `MaxBodyHold` | 1 minute | the body fails with `http.ErrBodyHeldTooLong` |
| A response | `WriteTimeout` | none | the stream is reset with `H3_INTERNAL_ERROR` |

`IdleTimeout` and `ReadHeaderTimeout` fall back to `ReadTimeout`, as on HTTP/1
and HTTP/2, and a negative value means no limit. `MaxIdleTimeout` alone does
not bound an idle connection: a client that sends PINGs, QUIC keep-alives,
keeps it open without a request, which `IdleTimeout` is for. The header and
body timers are armed only for a request a delivery left incomplete, so a
request that arrives whole in one packet, as most do, costs none.
- The engine queues at most 1024 unprocessed datagrams per UDP connection; more
  are dropped and QUIC treats them as lost.
- The engine hands every UDP datagram over in a buffer of its own from
  `bufferpool`, and the QUIC layer gives it back once the handler calls it led
  to have run. What has to outlive it is copied: data that arrived ahead of a
  gap, a packet waiting for its keys, and the request body, which is gathered
  in a pooled buffer of its own. That buffer goes back once the response is
  finished, as on HTTP/1: a handler that reads the body after that, having
  neither retained the request nor read it before responding, gets
  `http.ErrBodyReleased`.
- The server takes a connection's datagrams in bursts, as a
  `fib.DatagramsHandler`: whatever has piled up by the time the connection's
  worker runs is processed together, and the acknowledgement and the
  responses written meanwhile leave in as few packets as they fit in. A
  request handed to the handler pool tells its connection to expect its
  response (`quic.Conn.ExpectWrite`), so that answers written on the pool's
  goroutines wait for the rest of the burst's, a millisecond at most, and
  leave packed together with its acknowledgement, as they would from a
  handler answering inline. The requests a burst completes go to the pool
  together, in one batch, so that they land on one of its shards and are
  answered close together rather than as far apart as the shards' queues
  are. The millisecond is counted from when there is something to send: an
  acknowledgement that is not yet due waits for its own deadline instead.
- A request's state — its `requestStream`, the `http.Request`, its header
  and its Context — is let go once its response is written and the handler
  has returned, rather than kept until the client has acknowledged the
  response, and a server lets its TLS connection go once the handshake is
  over, keeping the `tls.ConnectionState` it reports.
- Acknowledgements stop covering what the peer has seen acknowledged (RFC
  9000 section 13.2.4), so a peer that skips packet numbers, as quiche does,
  gets ACK frames of a few ranges rather than of every range kept.
- `Engine.Shutdown` sends GOAWAY, lets the requests in flight finish, and
  closes each connection with H3_NO_ERROR, but `Engine.Stop`/`Close` on their
  own send neither CONNECTION_CLOSE nor GOAWAY; connections are dropped and
  clients find out only when they time out.
- Like the `http` package, `http3` builds only on the three native backends
  (Linux, macOS, Windows); on the portable backend (e.g. FreeBSD) the package is
  empty.

### Whole bodies

- A request body is read into memory in full before the handler runs, unless
  `Config.StreamRequestBody` is set: then the handler runs as soon as the body
  has begun — at the HEADERS for a body with a `content-length` past
  `StreamRequestBodyThreshold`, and once more than the threshold of it has
  arrived for one without — and takes the body from `Request.Body`, an
  `*http.BodyStream` read without waiting, or through `Context.OnBody`, as on
  HTTP/1 and HTTP/2. QUIC then gives the stream's receive window back only as
  the handler consumes the body, so a fast client is paced one window (1MB)
  ahead at most. `MaxStreamedBodyBytes` bounds such a body in place of
  `MaxBodyBytes`, and `Expect: 100-continue` is answered only once the handler
  asks for the body.
- A response given to `WriteResponse` or `Respond` is given at once as
  `Response.Body []byte`; the client buffers the whole response body before
  the callback. `Context`'s `http.ResponseWriter` methods work, trailers
  included, and stream as on HTTP/1 and HTTP/2: a response no longer than 4KB
  (or with a `Content-Length` of at most 64KB) that is not flushed is sent
  whole when the handler returns, and a longer or flushed one goes out as it
  is written, its header in a HEADERS frame of its own, its body in DATA
  frames and its trailers after them, so SSE and streaming downloads work.
  A handler that writes faster than the client reads waits in `Write` once
  64KB of its body waits in the QUIC stream for flow or congestion control,
  until some of it has gone or the stream ends; one the connection runs on
  the goroutine its datagrams arrive on never waits, and what it writes waits
  in the stream instead.
- Memory bounds: a server connection can hold up to about
  `MaxConcurrentStreams × MaxBodyBytes` (100 × 16MB by default); a single
  client response is bounded by `MaxResponseBodyBytes`.
- A request that is complete goes to the engine's handler pool, as
  `Config.StreamPool` describes — the engine name's `<Name>-streams` pool,
  never an engine's; the HTTP/2 server uses the same — so the requests one client
  has open on a connection are served concurrently and a handler that blocks
  holds up only itself. `StreamPool.MaxConcurrentHandlers` bounds how many of
  one connection's requests run at once, the last of them on the goroutine
  reading the connection, which then reads nothing further until it returns;
  1, like `StreamPool.Disable`, serializes the handler calls of a connection
  again, as they were before the pool existed.
- Do not write with `Context.Conn.Send`: `Conn` is the UDP connection, and all
  output must go through `Context`.
- A request's stream, with the `*http.Request`, `Header`, `URL` and `*Context`
  it holds, is recycled once the request has ended, unless
  `Config.DisableReuse` is set: a handler must not keep any of them past its
  response and its last `Release`, the rule `http.Config.ReuseRequests`
  describes. Requests whose body streams and extended CONNECTs are not
  recycled. On a server held to 8 CPUs this served 1.56M requests a second
  where leaving them to the collector served 1.19M.

### Fixed parameters

These are constants, or exist only in `quic.Config` without being
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

- **Closing**: a connection that sends CONNECTION_CLOSE keeps a closing
  period of three probe timeouts, at least a second and at most its idle
  timeout (RFC 9000 §10.2.1), in which it answers what still arrives with the
  CONNECTION_CLOSE again, at the 1st, 2nd, 4th, 8th and so on of them.
  Receiving CONNECTION_CLOSE releases the connection at once, with no draining
  period. `Engine.Close` sends CONNECTION_CLOSE (H3_NO_ERROR) on every
  connection, the server's and the client's, before it ends them;
  `Engine.Stop` leaves them open until then.
- **Key updates**: this side starts one once a key has protected as many
  packets as RFC 9001 §6.6 allows (about 2^23 for AES-GCM), and answers the
  peer's; past the integrity limit on failed decryptions the connection ends
  with AEAD_LIMIT_REACHED. What it does not do is rotate keys on a timer or
  on request.
- **Stateless reset**: the reset key is random per `ServerHandler` unless
  `Config.StatelessResetKey` sets one; with a random key, clients cannot
  recognize the resets a restarted server sends for connections from before
  it. Only clients recognize stateless resets.
- **Floods**: what a peer can make a connection do without asking for
  anything is bounded, past a burst of 1000 refilled at 100 a second, after
  which the connection closes with H3_EXCESSIVE_LOAD; see
  [Planned improvements](#1-correctness-and-hardening).
- **BLOCKED frames**: DATA_BLOCKED, STREAM_DATA_BLOCKED and STREAMS_BLOCKED are
  never sent.
- **Congestion control**: CUBIC (RFC 9438) by default, NewReno with
  `quic.Config.CongestionControl`; no BBR, HyStart++ or ECN. Sending is paced at
  1.25 congestion windows a smoothed RTT. A round of datagrams the send buffer
  has no room for (`EAGAIN`, or `ENOBUFS` on macOS) is kept and sent again a
  millisecond later, before anything new: the engine says when a UDP socket can
  be read, but not when a full one can be written again.
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
  fails and is not retried, whatever its method, as with `net/http`: the
  server has begun to answer, so it has acted on the request, and a streaming
  response's callback may already have passed part of the body on. Retries
  happen only for requests the server marks as unprocessed (GOAWAY or
  H3_REQUEST_REJECTED) and for requests still queued; a caller that knows a
  request is safe to repeat after a partial answer can send it again itself.
  The HTTP/2 client's reasoning is the same; see
  [HTTP/2](http2.md#client).
- A request with a body and `Expect: 100-continue` sends its HEADERS alone and
  its body once the server answers 100, or once
  `ClientConfig.ExpectContinueTimeout` (1 second in `DefaultClientConfig`;
  zero sends the body at once) has passed. A final response that comes first
  is the answer: the body is never sent, and the stream's sending side is
  reset once the response is complete.
- Sessions are resumed from `ClientConfig.SessionCache` by this module's
  handshake, or from `TLSConfig.ClientSessionCache` when the handshake is
  crypto/tls's (`BlockingTLSHandshake`, or a TLSConfig only crypto/tls serves).
  Only the requests that are safe to repeat go as 0-RTT data; see
  [0-RTT](#0-rtt).

### 0-RTT

A client that resumes a session may send its first requests with its first
flight (RFC 9001 section 4.6), a round trip ahead of the handshake. The data is
encrypted under keys that come from the session alone, so it is not forward
secret, and an attacker who recorded it can send it again. What this module does
about that:

- **Off by default** on both sides: `Config.EarlyData` on the server (its
  session tickets then allow early data), `ClientConfig.EarlyData` on the
  client.
- **A ticket carries early data once.** The server keeps the tickets that have,
  until they expire, and refuses early data on a second use (the connection
  still resumes). It also refuses it when the age the client reports for the
  ticket is more than 10 seconds off the server's own, when the handshake is
  retried with a HelloRetryRequest, and when the transport limits it gave with
  the ticket are higher than it gives now. The register is per `Handler`; a
  server that is not the one that issued the ticket cannot know, so servers
  behind a load balancer have to see to replays themselves.
- **Safe methods only.** The server marks requests that arrived as early data
  with `Early-Data: 1` (RFC 8470) and serves GET, HEAD, OPTIONS and TRACE; any
  other method is answered 425 Too Early (`Config.EarlyDataUnsafe` serves them
  all, and the handler must look at the header). `Stream.EarlyData` tells the
  same of a QUIC stream. The client sends only those methods, and without a
  body, as early data (`ClientConfig.EarlyData`), and sends a request answered
  425 again once the handshake is over. `Client.Stats` counts what was sent early,
  accepted, rejected (the server turned the early data away, which the QUIC
  layer then sends again under the 1-RTT keys) and answered 425.
- **Responses before the handshake ends.** A server that accepts early data
  answers it as soon as it has sent its Finished, before the client's has
  arrived (0.5-RTT). `quic.EarlyDataHandler` is how a handler takes early data
  as it arrives; one that is not told of it after the handshake.

The client's sessions are kept by this module's TLS (`tls.NewQUICSessionCache`,
or `ClientConfig.SessionCache`); crypto/tls's QUICConn, which the other handshake
uses, resumes and sends early data through the same QUIC code.

## Intentionally not implemented

| Feature | Reason |
| --- | --- |
| Active connection migration | A client moving on purpose has to use a fresh Connection ID on its new path, so the server would have to issue and route several per connection, and retire them. A NAT rebinding, which keeps the Connection ID, is followed (see above). |
| QPACK dynamic table | Both sides advertise a capacity of 0: no head-of-line blocking from blocked streams, silent encoder and decoder streams, and no extra state or memory. The cost is that repeated headers are sent in full every time. |
| Server push | No major browser enables HTTP/3 push. The client sends no MAX_PUSH_ID, and `Context.Push` returns `http.ErrNotSupported`. Use 103 Early Hints (`WriteInterim`) for preloading. |
| NEW_TOKEN | The server sends Retry under load (see [Planned improvements](#1-correctness-and-hardening)), but no NEW_TOKEN, so a client's next connection is validated afresh: by its handshake and the 3x amplification limit, or by another Retry. The client ignores NEW_TOKEN. |
| QUIC v2 (RFC 9369) and other versions | Version 1 is enough in practice. The client fails on Version Negotiation. |
| DATAGRAM (RFC 9221), WebTransport | They need APIs for streaming or unreliable datagrams, which the current handler model lacks. A DATAGRAM frame from the peer is an unknown frame and closes the connection with FRAME_ENCODING_ERROR. |
| RFC 9218 extensible priorities | With whole bodies, scheduling gains little. |
| ECN | Needs reading and writing the IP header's ECN bits in the engine's UDP layer; the gain is mostly in congestion control. |

## Planned improvements

In order of priority.

### 1. Correctness and hardening

Done:

- **Control frame and reset floods**: a QUIC connection closes, with
  H3_EXCESSIVE_LOAD under HTTP/3 (`quic.Config.ExcessiveLoadCode`), when its
  peer, past a burst of 1000 refilled at 100 a second as in HTTP/2:
  - abandons streams while they are open, with RESET_STREAM or STOP_SENDING,
    or makes the server reset them for malformed requests, each stream counted
    once (HTTP/3's form of Rapid Reset, CVE-2023-44487); only a server counts
    these, and not the RESET_STREAM that answers its own STOP_SENDING;
  - sends frames that ask for work and carry nothing: a packet with nothing
    but PING, PATH_CHALLENGE, MAX_DATA, MAX_STREAM_DATA and MAX_STREAMS that
    raise no limit, DATA_BLOCKED, STREAM_DATA_BLOCKED and STREAMS_BLOCKED
    that report no new limit (a sender says so once per limit it reaches),
    a STREAM frame with no data that ends nothing, and NEW_CONNECTION_ID for a
    connection ID already retired.

  A PING bundled with an acknowledgement of something new is not counted,
  since a receiver that only acknowledges, as a client downloading does, sends
  one now and then to have its acknowledgements acknowledged; nor are frames a
  peer may send in flight for a stream that has ended. HTTP/3's own frames
  cost no more than their bytes, since flow control is QUIC's, and are not
  counted. The budgets are not configurable yet.
- **Handshake cost**: past `Config.RetryHandshakes` handshakes under way
  (1024 by default; below zero, always), a new client is first sent a Retry,
  whose token proves its address before the server does any TLS work or keeps
  any state for it; past `Config.MaxHandshakes` (4096) its Initial is dropped.
  A token that is not good, from another address or more than 10 seconds old,
  is answered with INVALID_TOKEN. The tokens' key is random to each handler,
  so a Retry from one server is not good at another behind the same address.
- **Closing and draining states**: the 3×PTO closing period, with
  CONNECTION_CLOSE sent again to what arrives in it; a fixed stateless reset
  key (`Config.StatelessResetKey`); CONNECTION_CLOSE on `Engine.Close`, which
  `Engine.Stop` leaves the connections open for (see
  [Protocol details](#protocol-details)).

### 2. Performance (medium)

- **Pacing, congestion control, loss recovery's data structures**: done.
  Sending is paced at 1.25 congestion windows a smoothed RTT, with a burst of
  the initial window or 2ms of the rate (RFC 9002 section 7.7), the timer going
  on with what pacing holds back; a round the socket refuses with `EAGAIN` is
  kept and sent again rather than lost (`fib.Connection.SendDatagrams` says how
  much of a batch went). CUBIC (RFC 9438) is the default, NewReno selectable in
  `quic.Config.CongestionControl`; the window grows only while in use (RFC 9002
  section 7.8), and persistent congestion (section 7.6) collapses it. Sent
  packets are a ring indexed by packet number, so an ACK frame costs what it
  newly acknowledges and loss detection looks only up to the largest
  acknowledged; the timer is set again only for a sooner deadline.
  Measured on the benchmark host (`TestLossyTransfer`, 20 MiB each way over an
  in-memory path, three rounds): under 5% random loss 1.2-1.7s before, 0.40s,
  0.43s and 1.06s after; lossless 235-275ms before, 82-92ms after. fit's
  fixed-h3 went from 1658k to 1773k requests a second (+7%), static-h3 was
  unchanged (1175k, 1165k).
- **PMTU discovery (DPLPMTUD, RFC 8899)**: Ethernet paths usually carry about
  1450-byte datagrams, which would cut per-packet header and AEAD overhead.
  Until then, `Config.MaxDatagramSize` sets a larger size by hand.
- **Congestion control**: BBR, HyStart++ and ECN are still to do; so is a
  writability event for UDP sockets, which would resume a round the socket
  refused as soon as it has room, rather than a millisecond later.
- **Batched I/O**: the engine reads with `recvmmsg` on Linux and `recvmsg_x`
  on macOS, and a round of sending with more than one datagram leaves with
  `sendmmsg` or `sendmsg_x` (`fib.Connection.SendBatch`), on the server's
  connections as on the client's. On Linux a run of
  datagrams of one size in a batch goes as one message segmented with GSO
  (`UDP_SEGMENT`), which took static-h3 on an 8-CPU server from 40k to 77k
  requests a second. GRO, the receiving side, is still to do.
- **Fewer copies and allocations**: sending copies once in `Write`, into a
  pooled buffer, and again when packets are built; receiving copies in the
  engine, into a pooled buffer, and in the HTTP/3 frame parser only a frame
  split across pieces and the request body. A request still allocates its
  QUIC stream; its request stream and header are recycled (see above).
- **ACK frequency**: implement the ACK_FREQUENCY extension to send fewer ACKs
  under heavy traffic.
- **ChaCha20-Poly1305** is pure Go and slower than assembly. Machines with AES
  hardware negotiate AES-GCM first, so it is rarely the bottleneck.
- **Benchmarks**: add QUIC and HTTP/3 benchmarks (single-connection throughput,
  concurrent requests, throughput under loss) and profile with pprof.

### 3. Streaming bodies and the handler model (medium)

- As for HTTP/2: streaming response writes. Streamed request bodies are done,
  with the receive window given back as the handler consumes the body.
  Bidirectional streaming is what WebTransport needs; extended CONNECT has
  it through `Context.Upgrade`.

### 4. Configurability (low)

- Expose keep-alive, receive windows and the unidirectional stream limit in
  `http3.Config` and `http3.ClientConfig`.
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
- 0-RTT against more servers than quiche and quic-go, and replays across
  servers that share ticket keys (the register of tickets is per process).
- The concurrency problems only load testing shows. Frame parsing, building
  requests, QPACK decoding, QUIC packet headers, transport parameters and
  frame handling all have fuzz targets, which the `Fuzz the parsers` CI job
  runs for 20 seconds each.
- Consider joining the
  [QUIC Interop Runner](https://github.com/quic-interop/quic-interop-runner) for
  continuous interop testing against quiche, ngtcp2, mvfst and the rest.

## Conformance tests

The suite is `http3/http3_conformance_test.go` (its tests start with
`TestHTTP3Conformance`) and `http3/protocol_test.go` (protocol errors
written frame by frame). The `HTTP/3 conformance` CI job runs them on Linux,
macOS and Windows.

The standard library has no HTTP/3, so the peer for the interop cases is
quic-go — but it lives in `http3/interop`, a **module of its own**: fib's
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
| The body matrix's raw QUIC client (`http3/body_matrix_test.go`), against the fib server | Request bodies every way they can come: no body, a content-length, DATA with no length, a trailer and `Expect: 100-continue`, small and large, the stream written whole or in pieces with pauses, taken by the handler by reading, through OnBody, as `BodyComplete` decides or from another goroutine, with the server reading bodies whole, streaming them all and streaming past a threshold; also a stream reset mid-body, the body limits, the stream window pacing a slow handler, and OnBody holding the body back until the handler returns |
| quiche, in a container (`http3/quicheinterop/run.sh`) | Both directions with both handshakes: 300 connections and 30 000 echoes, multiplexed streams, session resumption, 0-RTT in both directions (a GET goes in the first flight, a POST in early data is answered 425 and sent again, a server without early data is not sent any), certificate verification through `RootCAs` |
| quic-go and fib, for early data (`http3/early_data_test.go`) | quic-go's client resuming and sending GET_0RTT to the fib server, the fib client sending early requests to quic-go's server, a server that does not accept it, a ticket used twice, and a 425 |
| Two QUIC connections over an in-memory path | Handshake, transfers under 5% and 20% loss, stream limits, resets, application close, idle timeout, keep-alive, stateless reset, key updates (with the AEAD limits lowered), the decryption failure limit, Retry and its tokens, and the flood limits |
| Fuzzing (`go test -fuzz`) | The parsers that take untrusted bytes: the HTTP/3 frame parser, building requests from fields, QPACK decoding and its round trip, QUIC packet headers, transport parameters and 1-RTT frame handling |

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
| QUIC | Version 1; Initial, Handshake and 1-RTT packet number spaces; AES-128-GCM, AES-256-GCM and ChaCha20-Poly1305 packet and header protection; key updates in both directions (started here when a key reaches its AEAD limit, or optionally after a time, and answered when the peer starts one) and a limit on decryption failures; Retry, sent by the server under load and handled by the client, and NEW_TOKEN, so that a later connection from the same address skips the Retry; Version Negotiation; stateless reset; RFC 9002 loss detection, PTO and persistent congestion; CUBIC (RFC 9438, the default) or NewReno congestion control, with pacing and app-limited detection; HyStart++ (RFC 9406), which leaves the initial slow start on a rise in the round-trip time; ACK_FREQUENCY (draft-ietf-quic-ack-frequency), which a bulk transfer asks for; the server's 3x amplification limit; connection- and stream-level flow control in both directions, with BLOCKED frames; MAX_STREAMS; Connection IDs issued with NEW_CONNECTION_ID and retired; a draining period; idle timeout and optional keep-alive |
| Server | Shares one `Handler` and `Context` with HTTP/1 and HTTP/2; 1xx interim responses, automatic 100 Continue, request and response trailers, graceful `Response.Close` via GOAWAY, 413/431/417, malformed requests (including an `:authority` and `Host` that disagree) reset with H3_MESSAGE_ERROR, protocol errors closing the connection with RFC 9114 and RFC 9204 codes, `Request.TLS`, an `AltSvc` helper, extended CONNECT (RFC 9220) switched to a tunnel with `Context.Upgrade`, which is how `websocket.ServerHandler.Upgrade` serves WebSocket over HTTP/3 |
| Client | Asynchronous `Do`/`Go`; one multiplexed connection per host:port; honors MAX_STREAMS and queues the rest; cancellation resets only its own stream; requests the server marks as unprocessed (GOAWAY, H3_REQUEST_REJECTED) are retried on a new connection; `Expect: 100-continue` waits for the 100; request and response trailers; a peer that breaks the protocol ends the connection with its error code; `AltSvcClient` composes it with an `http.Client` to follow `Alt-Svc` (RFC 7838) and fall back to TCP |
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
  (RFC 9000 §9.3). The challenge is in the first packet the server sends to
  the new address even when the congestion window, still the old path's and
  full of its packets in flight, has no room for it: it goes as a probe
  timeout's packets do, since what the peer has to hear first on the address
  it moved to is the challenge. A client that does not answer in time is
  taken back to the path it left, and a later packet on the old path takes it
  back too.
  Congestion control and RTT start over unless only the port changed, and
  datagrams drop to 1200 bytes until the new path is validated, after which
  path MTU discovery searches it afresh. Requests
  after the move report the new address in `Request.RemoteAddr`.
- Under `Config.IOPollers` with `ReusePort` on Linux, the new address may
  fall to another poller's socket; the connection follows it there, since a
  QUIC connection takes datagrams from any goroutine.
- Once the handshake is confirmed, each side issues Connection IDs beyond the
  handshake's with NEW_CONNECTION_ID, as many as the peer's
  `active_connection_id_limit` allows up to `TransportConfig.ActiveConnectionIDs`
  (4 by default), and issues another for each one the peer retires with
  RETIRE_CONNECTION_ID (RFC 9000 §5.1). The server routes every one of them to
  its connection, before the client is sent it, so a client that switches to a
  fresh Connection ID as its address changes, which keeps the change from
  being linked to the old path, is followed as a NAT rebinding is. The routes
  go with the connection.
- The server still advertises `disable_active_migration`: it answers
  PATH_CHALLENGE on the path the connection sends on, and at most the 4 latest
  answers wait to go out (RFC 9000 §8.2.2 asks only that the latest challenge
  be answered), so a client probing a path of its own choosing would not hear
  the answer. Its NAT may move it. CIDs the peer offers are recorded and
  retire_prior_to is honored, but this side keeps sending to the one it has;
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
  `Response.Body []byte`; the client buffers a response body whole by default,
  and hands a large one to the callback as it arrives with
  `ClientConfig.StreamResponseBody`. `Context`'s `http.ResponseWriter` methods work, trailers
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
  client response is bounded by `MaxResponseBodyBytes`. A whole response keeps
  the handler's body as it is and frames it as the peer's windows, and then
  its acknowledgements, let it go, as HTTP/2 does, so a stalled stream costs
  its window of frames rather than the body: 16 stalled responses of 8MiB cost
  19MiB of live heap against the 275MiB they cost when the body was copied
  into one power-of-two buffer (see [Load testing](loadtest.md)). Whatever its
  size, the body stays where the handler gave it until the peer has
  acknowledged the response, and the frames are the stream's own, so it must
  not be changed once `Respond` or `WriteResponse` returns;
  `fibhttp.Response.Body` states that rule for every protocol. A handler that
  wants its buffer back at the call answers with `RespondCopy` or
  `WriteResponseCopy`: the copy is taken from the pool, and QUIC keeps it as an
  owned run, returning it once the peer has acknowledged it, so with a warm
  pool the opt-in copy allocates nothing. `WriteResponseCopy` copies the
  trailer too, which is otherwise kept like the body.
  Streaming the body through `Write` and `Flush` keeps what waits to 64KiB
  instead.
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

`Config.Transport` and `ClientConfig.Transport` (an `http3.TransportConfig`)
set how the QUIC connections under a server or a client behave:

| Parameter | Default | Field |
| --- | --- | --- |
| Per-stream receive window | 1MB, replenished when half is consumed | `StreamReceiveWindow` |
| Connection receive window | 16MB, replenished when half is consumed | `ConnReceiveWindow` |
| Unidirectional streams the peer may open | 16 | `MaxIncomingUniStreams` |
| Longest an acknowledgement is held back (`max_ack_delay`) | 25ms; otherwise every 2 ack-eliciting packets are acknowledged at once | `MaxAckDelay` |
| Keep-alive | Off: a connection that carries nothing closes after `MaxIdleTimeout` (30s) and the next request handshakes again | `KeepAlivePeriod` |
| Flood budgets | Bursts of 1000, refilled at 100 a second (see [Protocol details](#protocol-details)) | `StreamResetBurst`, `StreamResetRate`, `ControlFrameBurst`, `ControlFrameRate` |
| Key updates by time | Off: keys are updated at the AEAD's packet limit | `KeyUpdateInterval` |
| Connection IDs issued to the peer | 4, as far as the peer allows | `ActiveConnectionIDs` |

These are still constants:

| Parameter | Value |
| --- | --- |
| Datagram size | 1200 bytes until path MTU discovery finds more (see [Protocol details](#protocol-details)), at most 1472 over IPv4 and 1452 over IPv6; a server can start higher with `Config.MaxDatagramSize` on a network known to carry that |
| Bidirectional stream limit the client advertises | 1 (servers may not open request streams) |
| QPACK dynamic table capacity | 0 |
| Received packet number ranges remembered | 32; older packets are treated as duplicates |

### Protocol details

- **Closing**: a connection that sends CONNECTION_CLOSE keeps a closing
  period of three probe timeouts, at least a second and at most its idle
  timeout (RFC 9000 §10.2.1), in which it answers what still arrives with the
  CONNECTION_CLOSE again, at the 1st, 2nd, 4th, 8th and so on of them.
  A connection that receives CONNECTION_CLOSE drains for as long (RFC 9000
  §10.2.2): it sends nothing more, but keeps its path, so that what the peer
  still has in flight is taken in silence rather than answered with a
  stateless reset or taken for a new connection. `Engine.Close` sends
  CONNECTION_CLOSE (H3_NO_ERROR) on every connection, the server's and the
  client's, before it ends them; `Engine.Stop` leaves them open until then.
- **Key updates**: this side starts one once a key has protected as many
  packets as RFC 9001 §6.6 allows (about 2^23 for AES-GCM), and answers the
  peer's; past the integrity limit on failed decryptions the connection ends
  with AEAD_LIMIT_REACHED. `TransportConfig.KeyUpdateInterval` also updates
  the sending keys once they have been in use that long; an update still
  waits for the peer to acknowledge a packet of the current keys, so an idle
  connection does not update. There is no update on request.
- **Stateless reset**: the reset key is random per `ServerHandler` unless
  `Config.StatelessResetKey` sets one; with a random key, clients cannot
  recognize the resets a restarted server sends for connections from before
  it. Only clients recognize stateless resets.
- **Floods**: what a peer can make a connection do without asking for
  anything is bounded, past a burst of 1000 refilled at 100 a second by
  default (`TransportConfig.StreamResetBurst`/`StreamResetRate` and
  `ControlFrameBurst`/`ControlFrameRate`, below zero for no limit), after
  which the connection closes with H3_EXCESSIVE_LOAD; see
  [Planned improvements](#1-correctness-and-hardening).
- **BLOCKED frames**: a side with data to send that the peer's flow control
  holds back says so once for each limit it reaches, with DATA_BLOCKED for the
  connection's, STREAM_DATA_BLOCKED for a stream's, and STREAMS_BLOCKED when it
  wants a stream the peer does not allow yet (RFC 9000 §4.1); a lost one goes
  again only while its limit holds, and one waiting to go is dropped once the
  peer raises the limit.
- **Congestion control**: CUBIC (RFC 9438) by default, NewReno with
  `quic.Config.CongestionControl`, both with HyStart++ leaving the initial slow
  start on a rise in the round trip; no BBR or ECN. The congestion window is
  counted in bytes, in units of the path's current maximum datagram size —
  1200 until path MTU discovery finds more — so the initial window (ten
  datagrams, but no more than 14720 bytes and no fewer than two), the minimum
  window (two datagrams) and the congestion-avoidance increase (a datagram a
  round trip for NewReno, and CUBIC's curve in datagram-sized segments) are
  that size's bytes. A window already in bytes does not widen by itself when
  the datagram size grows; only the minimum follows the path. Sending is paced
  at 1.25 congestion windows a smoothed RTT, the same byte rate for a given
  window. A round of datagrams the send buffer
  has no room for (`EAGAIN`) is kept, and the engine says when the socket has
  room again (`fib.Connection.NotifyWritable`), so it goes then, before anything
  new; where nothing can say that — Windows' completion ports, the portable
  backend, or `ENOBUFS`, which macOS reports for a full socket and Linux for a
  full device queue — a millisecond timer tries again.
- **Path MTU discovery** (DPLPMTUD, RFC 8899; RFC 9000 §14.3), on by default
  at both ends: once the handshake is confirmed a connection sends a probe, a
  PING padded to 1472 bytes over IPv4 or 1452 over IPv6 (`MaxProbeDatagramSize`,
  no more than the peer's `max_udp_payload_size`, and what an Ethernet MTU of
  1500 bytes leaves for UDP), and sends datagrams that large once it is
  acknowledged. `MaxDatagramSize` and `MaxProbeDatagramSize`, in `Config` and
  `ClientConfig`, are clamped to the path's limit, 1472 on IPv4 and 1452 on
  anything not identifiable as IPv4, so an IPv6 connection never probes or
  sends the IPv4 size. A probe lost three times has the sizes below searched by
  halves, to within 20 bytes, and from the top again ten minutes later. Lost
  probes do not count as congestion. It runs only where the engine's UDP
  socket can be set not to fragment (`fib.Connection.SetDontFragment`: Linux
  with `IP_PMTUDISC_PROBE`, macOS, FreeBSD and Windows; IPv6 only on the other
  BSDs), since a fragmented probe proves nothing; `DisablePathMTUDiscovery`
  in `Config` and `ClientConfig` turns it off. Black holes: a connection
  sending more than 1200 bytes goes back to 1200 when three larger packets
  are lost with none sent after them acknowledged, or after three probe
  timeouts in a row, and searches again a minute later; this holds for a size
  set with `MaxDatagramSize` too. The datagram size the congestion window is
  counted in follows the path, back to 1200 with it.
- **The peer's `SETTINGS_MAX_FIELD_SECTION_SIZE`** is honored once its SETTINGS
  have arrived, each field counted as RFC 9114 §4.2.2 counts it: a response
  header, interim response or trailer of the server's that is larger fails
  its write, so the handler can answer otherwise, and a trailer behind a body
  already sent resets the stream instead, so that the client cannot take the
  body for whole; a request of the client's whose header or trailer is larger
  fails without being sent.
- **Priorities**: the `priority` header and PRIORITY_UPDATE frames are ignored;
  streams with data to send take turns.
- **ECN**: ECN marks are neither set nor reported. ACK_ECN frames are parsed,
  but their counts are ignored.

### Client

- Only `https://`. A `Client` by itself does not switch from HTTP/1.1 or
  HTTP/2 to HTTP/3 on `Alt-Svc`, nor fall back to TCP when UDP is blocked:
  `http3.AltSvcClient` is the integration layer for that. It sends an
  origin's first request through an `http.Client`, follows a response's
  `Alt-Svc: h3="host:port"` (RFC 7838) for the requests after it, keeps the
  origin's `:authority` and the name its certificate is verified for, and
  falls back to TCP for a request that is safe to repeat when the QUIC
  attempt fails, leaving that origin on TCP for a while.
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
- `ClientConfig.TLS13CipherSuites` is what the ClientHello offers, by ID, in
  the order to prefer them; empty keeps the default order, AES-GCM first where
  the CPU computes it in hardware and ChaCha20-Poly1305 first where it does not,
  as crypto/tls orders them. A TLS 1.3 server settles on one of the suites its
  client offered, so a list of one narrows what the connection uses: the
  runner's `chacha20` testcase is `[]uint16{tls.TLS_CHACHA20_POLY1305_SHA256}`.
  There is no server switch, since a TLS 1.3 server proposes no suite of its
  own, and a handshake crypto/tls runs chooses its own, since crypto/tls has no
  field for TLS 1.3's suites.

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

The complete list, including the HTTP/1, HTTP/2, engine and test-infrastructure items, is in the [roadmap](roadmap.md), with the reason, the rough cost and when each is worth reconsidering.

| Feature | Reason |
| --- | --- |
| Active connection migration | The server issues and routes several Connection IDs per connection, but answers PATH_CHALLENGE on the path it sends on, not the one the challenge came from, so a client probing a new path before it moves would not validate it; it also keeps sending to the client's first Connection ID on the new path. A NAT rebinding is followed, with the client's Connection ID changed or not (see above). |
| QPACK dynamic table | Both sides advertise a capacity of 0: no head-of-line blocking from blocked streams, silent encoder and decoder streams, and no extra state or memory. The cost is that repeated headers are sent in full every time. |
| Server push | No major browser enables HTTP/3 push. The client sends no MAX_PUSH_ID, and `Context.Push` returns `http.ErrNotSupported`. Use 103 Early Hints (`WriteInterim`) for preloading. |
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
  counted. The budgets are set in `TransportConfig`.
- **Handshake cost**: past `Config.RetryHandshakes` handshakes under way
  (1024 by default; below zero, always), a new client is first sent a Retry,
  whose token proves its address before the server does any TLS work or keeps
  any state for it; past `Config.MaxHandshakes` (4096) its Initial is dropped.
  A Retry token that is not good, from another address or more than 10 seconds
  old, is answered with INVALID_TOKEN. The tokens' key is random to each
  handler, so a Retry from one server is not good at another behind the same
  address.
  A server that completes a handshake gives the client a token in a NEW_TOKEN
  frame in return (RFC 9000 section 8.1.3), bound to the client's address and
  good for 10 minutes: a later connection from that address carries it in
  its first Initial, and the server takes the address as proven, without the
  Retry's round trip (or its TLS work before the token is checked). A token
  of a NEW_TOKEN frame from another address, expired, or made with a key the
  server no longer has is no token at all: the connection starts as any new
  client's would, and the server may send a Retry instead of closing it. A
  client keeps tokens for a few addresses at most, for five minutes each, in
  one cache per `Client`; `Transport.DisableNewTokens` turns the exchange off
  on either side.
- **Closing and draining states**: the 3×PTO closing period, with
  CONNECTION_CLOSE sent again to what arrives in it, and as long a draining
  period after the peer's CONNECTION_CLOSE, sending nothing; a fixed stateless reset
  key (`Config.StatelessResetKey`); CONNECTION_CLOSE on `Engine.Close`, which
  `Engine.Stop` leaves the connections open for (see
  [Protocol details](#protocol-details)).

### 2. Performance (medium)

- **Pacing, congestion control, loss recovery's data structures**: done.
  Sending is paced at 1.25 congestion windows a smoothed RTT, with a burst of
  the initial window or 2ms of the rate (RFC 9002 section 7.7), the timer going
  on with what pacing holds back; a round the socket refuses with `EAGAIN` is
  kept and sent again as soon as the engine says the socket has room, rather
  than lost (`fib.Connection.SendDatagrams` says how much of a batch went).
  CUBIC (RFC 9438) is the default, NewReno selectable in
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
- **PMTU discovery (DPLPMTUD, RFC 8899)**: done, with black hole detection
  (see [Protocol details](#protocol-details)), and the congestion window
  counted in the discovered datagram size. It searches up to 1472 bytes over
  IPv4 and 1452 over IPv6, what an Ethernet MTU of 1500 bytes leaves for UDP.
  Still open: searching above that (jumbo frames), which needs larger send
  buffers.
- **HyStart++** (RFC 9406): done, and on for the initial slow start. Each
  round it keeps the minimum round trip of the round and of the one before,
  and when the current round's has risen over the last one's by an eighth of
  it (at least 4ms, at most 16ms) it leaves slow start for a conservative
  phase, which grows by a quarter of what slow start would; if the round trip
  comes back down the phase was premature and slow start resumes, and after
  five rounds the window becomes the threshold and congestion avoidance
  begins. A loss ends it: from then on loss recovery's threshold is the exit
  point. On a path whose round trip does not rise the 4ms floor means it does
  not fire at all, which is the case on the in-memory and loopback paths the
  benchmarks use, so its behaviour is covered by unit tests rather than by
  the numbers below.
- **BBR** was evaluated and is deliberately not implemented: it is a different
  model of the path rather than a different window, with a bandwidth estimate
  and pacing rate from per-round delivery samples and its own startup, drain
  and probe-bw phases. Doing it soundly means extending the sent-packet
  bookkeeping to delivery-rate samples per round and reconciling that with
  RFC 9002 loss recovery and persistent congestion, which is a large change
  with no way to test it here beyond the in-memory path; a half
  implementation would be worse than none.
- **ECN** is still to do.
- **Batched I/O**: the engine reads with `recvmmsg` on Linux and `recvmsg_x`
  on macOS, and a round of sending with more than one datagram leaves with
  `sendmmsg` or `sendmsg_x` (`fib.Connection.SendBatch`), on the server's
  connections as on the client's. On Linux a run of
  datagrams of one size in a batch goes as one message segmented with GSO
  (`UDP_SEGMENT`), which took static-h3 on an 8-CPU server from 40k to 77k
  requests a second. On the receiving side the Linux socket sets `UDP_GRO`: a
  run of datagrams the kernel coalesced arrives as one read, which the engine
  splits again by the size the read's control message gives, so a burst costs
  one receive rather than one a datagram; a kernel without `UDP_GRO`, and the
  other systems, read a datagram at a time.
- **Fewer copies and allocations**: a response given whole is kept where the
  handler put it, whatever its size, and framed from there, so the body is
  copied once, into the packets, which the AEAD needs; a streamed one is
  copied into its DATA frame's buffer as well, which is what the stream's send
  buffer is for. The HEADERS frame and the DATA frame's header are one pool
  buffer, and a trailer another, so a small response's frames cost what they
  did when they shared a buffer with its body. What is sent is copied into
  packets as they are built, and each packet is built where it is sent, its
  payload behind the room its header takes, so sealing a datagram copies
  nothing. Receiving copies in the engine, into a
  pooled buffer, and in the HTTP/3 frame parser only a frame split across
  pieces and the request body. Packet protection allocates nothing now: the
  ChaCha20-Poly1305 authenticator was one heap allocation per packet and is a
  value. QUIC streams are deliberately not pooled: a `*Stream` is handed to
  the application and its lifetime is the application's, so reusing one would
  turn a use after the stream is done into corruption of another stream. A
  stream opened and closed costs 8 allocations, a request over the in-memory
  path 12, and the HTTP/3 request stream and header are recycled (see above).
  A whole response with a trailer costs one allocation more than one without,
  its third run taking the send buffer's list past the two it carries inline
  (see above).
- **ACK frequency**: the ACK_FREQUENCY extension
  (draft-ietf-quic-ack-frequency) is done. Both ends advertise `min_ack_delay`
  in their transport parameters; a connection whose peer advertises it asks,
  after 32 KiB sent, for an acknowledgement every tenth ack-eliciting packet,
  tolerating a gap of two, and no later than a quarter of the smoothed round
  trip, so that a window smaller than the threshold still gets feedback
  several times a round trip. A loss makes it ask again, at most once a round
  trip, for an acknowledgement every other packet, which is the default, until
  the connection ends: a window that a loss has just shrunk is too small to
  fill the threshold, and loss recovery depends on the acknowledgements. A
  peer that does not advertise the extension is never sent the frame, and one
  that ignores it simply keeps acknowledging as before, since the request is
  advice and nothing depends on it. The receiver applies the most recent
  request and answers an IMMEDIATE_ACK at once. On the shared benchmark host, medians of six samples taken in two
  alternating rounds, a 64 KiB transfer stayed within the run-to-run noise
  (567 to 533 MiB/s in the medians, 582 to 692 MiB/s in the quieter first
  round), 1 MiB went from 714 to 877 MiB/s and 4 MiB from 882 to 1350 MiB/s,
  an HTTP/3 upload from 369 to 589 MiB/s, small requests from 6018 to 5780 ns
  and concurrent ones from 4876 to 3672 ns; at 1% loss the throughput was
  20.0 against 19.0 MiB/s and at 5% loss 7.2 against 4.4 MiB/s. Fewer
  acknowledgements cost loss recovery a little, which the request after a loss
  keeps from being worse, and they leave fewer of their own packets to be
  lost.
- **ChaCha20-Poly1305** is the library's own, since the standard library
  exports no ChaCha20-Poly1305 AEAD: `crypto/hpke` wraps one, but through
  internal packages and a vendored `golang.org/x/crypto` that a module outside
  the standard library cannot import, and `crypto/cipher` has none. It is now
  written for speed in pure Go: the block's sixteen words live in variables
  rather than an array, a block's key stream is XORed eight bytes at a time,
  and the authenticator is set up in place instead of being allocated for
  every packet. It lives in `internal/chacha20poly1305`, which the TLS record
  layer protects its TLS_CHACHA20_POLY1305_SHA256 and RFC 7905 suites with
  too. A 1200-byte packet's seal went from 355 to 548 MiB/s and its
  open from 358 to 551 MiB/s, a 16 KiB plaintext from 398 to 682 MiB/s, and
  one header protection block from 141 to 78 ns. Assembly (AVX2 or NEON) would
  be faster still, but it is a large per-architecture piece whose constant
  time is easy to get wrong, for a suite that only machines without AES
  hardware negotiate first, so it is not written. The path is covered end to
  end now: `TestChaCha20Poly1305Handshake` runs the handshake in a child
  process with `GODEBUG=cpu.aes=off`, where crypto/tls prefers ChaCha20, and
  checks that both ends protect packets and headers with it and that data
  crosses both ways.
- **Benchmarks**: done, as plain `go test -bench` targets. `http3/quic` has
  bulk transfers of 64 KiB, 1 MiB and 4 MiB over the in-memory path, requests
  one at a time and from as many goroutines as processors, transfers at 1% and
  5% loss, a stream opened and closed, and ChaCha20-Poly1305's seal, open and
  header protection; `http3` has a 4 KiB, 64 KiB and 1 MiB response body, an
  upload, small requests and concurrent requests over a UDP socket on the
  loopback interface, which include the engine and the framing. The numbers
  above are their medians on the benchmark host, before and after the changes
  of this round, six samples each in two alternating rounds.

### 3. Streaming bodies and the handler model (medium)

- Done: streamed request bodies, with the receive window given back as the
  handler consumes the body, and streamed responses, written as the handler
  produces them.
- Bidirectional streaming is what WebTransport needs; extended CONNECT has it
  through `Context.Upgrade`, but the handler model has no APIs for the two
  directions at once.

### 4. Configurability (low)

- Done: keep-alive, receive windows, the unidirectional stream limit, the
  acknowledgement delay, the flood budgets, key updates by time and the
  Connection IDs issued are set in `Config.Transport` and
  `ClientConfig.Transport`; the peer's `SETTINGS_MAX_FIELD_SECTION_SIZE` is
  honored when sending; the client sends request trailers, behind a body sent
  whole or streamed; `AltSvcClient` picks HTTP/3 from `Alt-Svc` and falls back
  to TCP when UDP is blocked.

### 5. Test coverage (low)

The conformance suite is done; see [Conformance tests](#conformance-tests).
What is still not covered end to end:

- 0-RTT against more servers than quiche and quic-go, and replays across
  servers that share ticket keys (the register of tickets is per process).
- The concurrency problems only load testing shows: checked by the campaign
  in [Load testing](loadtest.md), which drove HTTP/3 for 30 seconds per
  configuration with fib on both ends and no failed request, reset or GOAWAY.
  It exposed the whole-response memory cost above; a lossy path and a long
  soak are still not covered. Frame parsing, building requests, QPACK
  decoding, QUIC packet headers, transport parameters and frame handling all
  have fuzz targets, which the `Fuzz the parsers` CI job runs for 20 seconds
  each.
- QPACK's dynamic table, server push, DATAGRAM and ECN are not
  implemented, so no test can cover them. The [QUIC Interop
  Runner](#quic-interop-runner) endpoint reports the ECN testcase as
  unsupported rather than passing it silently.

Interop with other implementations now has a containerised endpoint and a
CI job; see [QUIC Interop Runner](#quic-interop-runner) just below.

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

### QUIC Interop Runner

`http3/interoprunner` is an endpoint for the
[QUIC Interop Runner](https://github.com/quic-interop/quic-interop-runner),
a module of its own like `http3/interop`. One image is both roles: with
`ROLE=server` it serves `/www` on UDP port 443 with the certificate in
`/certs` for `TESTCASE`; with `ROLE=client` it downloads `REQUESTS` into
`DOWNLOADS`, writing its TLS secrets to `SSLKEYLOGFILE` so the runner can
decrypt the capture. The runner drives all but the `http3` testcase over
HTTP/0.9 — a request line and the file on a QUIC stream — so those run on
fib's QUIC transport directly, and `http3` runs on fib's HTTP/3 server and
client.

```sh
docker build -f http3/interoprunner/Dockerfile -t fib-interop .
http3/interoprunner/local-test.sh       # the image against itself, no runner
http3/interoprunner/run-runner.sh       # the real runner, fib vs quic-go
```

`local-test.sh` needs only Docker and openssl and runs seven cases with the
image on both sides. `run-runner.sh` needs Docker with compose, tshark and
the runner's Python dependencies, clones the runner, registers the image and
runs both directions against quic-go; `TESTCASES`, `PEER` and `DIRECTION`
pick a subset. The testcase-by-testcase support table, the reasons for the
cases that exit 127, and the verified results are in
`http3/interoprunner/README.md`.

The CI jobs are `HTTP/3 interop endpoint image` (blocking: builds the image
and runs the supported cases against itself) and `HTTP/3 QUIC Interop
Runner` (`workflow_dispatch` only and non-blocking: the real runner against
quic-go, because it pulls the simulator and peer images and needs tshark).

| Peer | What it checks |
| --- | --- |
| The QUIC Interop Runner's fib endpoint (`http3/interoprunner`), against quic-go's endpoint | Both roles, both directions: HTTP/3 and the HTTP/0.9-over-QUIC cases — handshake, transfer, Retry, resumption, 0-RTT, key update, multiplexing, loss, IPv6, NAT rebinding and long RTT. fib's missing ECN, QUIC v2 and `preferred_address` make those cases exit 127, which the runner reports as unsupported; every other case passes, including the server under address rebinding and the client under long RTT, which that README explains. `chacha20` passes in both directions: the client offers ChaCha20-Poly1305 alone through `http3.ClientConfig.TLS13CipherSuites`, and the server takes it. |
| quic-go's HTTP/3 client, against the fib server | GET/POST/PUT/HEAD, request and response headers, several cookies and Set-Cookies, 4 MiB uploads and downloads checked by hash, 50 concurrent requests on one connection, 204, response trailers, request trailers, 103 Early Hints, 100-continue, 417, 413, 431, status codes |
| quic-go's HTTP/3 server, against the fib client | The same ground from the other side, and: the connection still serves after a timeout, a request in flight finishes when the server retires the connection with GOAWAY, a server that validates the address with Retry, and a certificate the client does not trust |
| The suite's own hand-written HTTP/3 client (no dependency), against the fib server | What a sound implementation never sends: DATA before HEADERS, a control stream that does not start with SETTINGS, a second control stream, a request missing a pseudo-header, a reference to the QPACK dynamic table, CANCEL_PUSH, a lowered MAX_PUSH_ID, a raised GOAWAY, a QPACK insertion, an `:authority` and `Host` that disagree, and neither of them |
| The suite's own hand-written HTTP/3 server (no dependency), against the fib client | How the client polices its peer: MAX_PUSH_ID from a server, CANCEL_PUSH, GOAWAY naming another kind of stream, a raised GOAWAY, a QPACK insertion, and a response that refers to the dynamic table |
| The body matrix's raw QUIC client (`http3/body_matrix_test.go`), against the fib server | Request bodies every way they can come: no body, a content-length, DATA with no length, a trailer and `Expect: 100-continue`, small and large, the stream written whole or in pieces with pauses, taken by the handler by reading, through OnBody, as `BodyComplete` decides or from another goroutine, with the server reading bodies whole, streaming them all and streaming past a threshold; also a stream reset mid-body, the body limits, the stream window pacing a slow handler, and OnBody holding the body back until the handler returns |
| quiche, in a container (`http3/quicheinterop/run.sh`) | Both directions with both handshakes: 300 connections and 30 000 echoes, multiplexed streams, session resumption, 0-RTT in both directions (a GET goes in the first flight, a POST in early data is answered 425 and sent again, a server without early data is not sent any), certificate verification through `RootCAs` |
| quic-go and fib, for early data (`http3/early_data_test.go`) | quic-go's client resuming and sending GET_0RTT to the fib server, the fib client sending early requests to quic-go's server, a server that does not accept it, a ticket used twice, and a 425 |
| Two QUIC connections over an in-memory path | Handshake, transfers under 5% and 20% loss, stream limits, resets, application close, idle timeout, keep-alive, stateless reset, key updates (with the AEAD limits lowered), the decryption failure limit, Retry and its tokens, the flood limits and their settings, BLOCKED frames, draining, and Connection IDs issued, routed and retired |
| Fuzzing (`go test -fuzz`) | The parsers that take untrusted bytes: the HTTP/3 frame parser, building requests from fields, QPACK decoding and its round trip, QUIC packet headers, transport parameters and 1-RTT frame handling |

## Load testing

`benchmark/loadtest` drove fib's HTTP/3 server with fib's own client for 30
seconds per configuration: 897,233 requests a second at 4 connections with
128 streams each, 689,402 at 32 x 16 and 373,362 on one connection with 256
streams, with no failed request, reset or GOAWAY; p99 was 1.56ms, 2.90ms and
1.34ms. HTTP/3 peaked below h2c's 2,836,822 at 64 x 8 in the same campaign,
and more QUIC connections cost more than they bought. Stalled readers were the
one memory limit the campaign found: every whole response was held in a
power-of-two buffer until acknowledged. That is fixed — the body is kept as
the handler gave it, and 16 stalled responses of 8MiB now cost 19MiB of live
heap instead of 275MiB. The method, the comparison with HTTP/1 and HTTP/2, and
the memory measurements are in [Load testing](loadtest.md).

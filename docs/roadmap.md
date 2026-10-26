# fib roadmap: what is planned but not done, and what is deliberately not done

Each protocol document (`http1*.md`, `http2*.md`, `http3*.md`) has its own "Intentionally not implemented" and "Planned improvements" sections. This page gathers them, plus the trade-offs recorded in `guide.zh-CN.md`, so that the whole picture is in one place: for every item, **why** it is not done, roughly **what it would cost**, and **when it would be worth reconsidering**.

- The cost column is an order of magnitude: S (a day or two), M (a few days), L (a week or more, or it needs test infrastructure that does not exist yet).
- "Deliberately not done" is a conclusion, not an oversight. When the requirement changes, re-evaluate it as described in section 6.
- The authoritative detail stays in the protocol documents; this page indexes it and records the reason, citing the source at the end of each row.

## 1. QUIC and HTTP/3

| Item | Status | Why | Cost | Reconsider when |
| --- | --- | --- | --- | --- |
| BBR congestion control | Deliberately not done | It replaces the *model of the path*, not just the window: it estimates bandwidth from per-round delivery samples, derives a pacing rate, and has its own startup, drain and probe-bw phases. Doing it soundly means extending the sent-packet record to per-round delivery sampling and reconciling it with RFC 9002 loss recovery and persistent-congestion detection. Large, and beyond in-memory pipes there is no reliable way to validate it here; half of it would be worse than none | L | There is production traffic on real paths, and measurements show CUBIC is the bottleneck on lossy or long-fat ones |
| ECN | Deliberately not done | Reading and writing the ECN bits of the IP header needs the Engine's UDP layer; the benefit is mostly in the congestion controller, and without one that uses it the gain is small | M | A datacentre deployment, or an ECN-aware congestion controller |
| Path MTU above 1452 (jumbo frames, 1472 on IPv4) | Planned | It needs larger send buffers, and buys a 1–2% reduction in packet count. The item that was worth more, counting the congestion window in bytes, is done | S–M | The path is known to carry bigger datagrams, or packet-count overhead becomes the bottleneck |
| ChaCha20-Poly1305 in assembly (AVX2/NEON) | Deliberately not done | A per-architecture, constant-time-sensitive body of code, for a suite only machines *without* AES hardware negotiate first. The pure-Go version is already 54–81% faster than the implementation it replaced | L | The target machines are mostly without AES hardware |
| Pooling QUIC `*Stream` objects | Deliberately not done | A `*Stream` is handed to the application, which owns its lifetime; reuse would turn "used after it ended" into corruption of another stream. Opening and closing a stream costs 8 allocations; request streams and headers are already recycled | M | Never: this is the API's ownership boundary |
| Active connection migration, `preferred_address` | Deliberately not done | The server issues and routes several Connection IDs per connection, but `PATH_CHALLENGE` is answered on the path it is sending on rather than on the path the challenge arrived from, so a client cannot validate a new path before migrating. NAT rebinding, including with a changed Connection ID, is followed, which covers the practical case | L | Mobile clients that move between Wi-Fi and cellular often |
| QPACK dynamic table | Deliberately not done | Both sides declare capacity 0: no blocked-stream head-of-line blocking, no encoder or decoder stream content, no extra state or memory. The cost is that repeated headers are sent in full every time | M | Headers are large and highly repetitive (large cookies, internal RPC) and become a bandwidth bottleneck |
| Server push | Deliberately not done | No mainstream browser enables HTTP/3 push; the client sends no `MAX_PUSH_ID` and `Context.Push` returns `http.ErrNotSupported`. Preloading uses 103 Early Hints (`WriteInterim`) | L | Never: the browser side is gone |
| DATAGRAM (RFC 9221), WebTransport | Deliberately not done | They need streaming streams or unreliable datagrams, which the handler model does not offer; an incoming DATAGRAM frame is treated as an unknown frame and closes the connection with `FRAME_ENCODING_ERROR` | L | Building real-time media or game services, which also means extending the handler model |
| QUIC v2 (RFC 9369) and other versions | Deliberately not done | Version 1 is enough for deployment; a client fails outright on Version Negotiation | S–M | A deployment requires v2 |
| A bidirectional-streaming handler API | Planned | Extended CONNECT is there through `Context.Upgrade` (one of WebTransport's prerequisites), but the handler model has no API for driving both directions at once | M | Same as WebTransport |
| One fewer copy on the receive path | Planned | The Engine copies once on receive (into a pooled buffer) and the frame parser copies only split frames and request bodies; removing the last copy needs zero-copy delivery and its lifetime rules | M | Receive throughput becomes the bottleneck |
| The Interop Runner's `ecn`, `v2` and `connectionmigration` cases | Deliberately not done | They follow directly from the three items above; the endpoint exits 127 and the runner records them as unsupported rather than passed | — | Same as those items |

## 2. HTTP/2

| Item | Status | Why | Cost | Reconsider when |
| --- | --- | --- | --- | --- |
| Client receives server push | Deliberately not done | Chrome and Firefox removed push, and Go's `net/http` client never supported it; the client declares `ENABLE_PUSH=0`. Preloading uses 103 Early Hints | M | Never |
| RFC 9218's incremental hint `i`, PRIORITY_UPDATE frames | Deliberately not done | The `priority` header's urgency **is** used for send scheduling; with whole response bodies there is nothing for `i` to reorder. RFC 7540's priority tree was obsoleted by RFC 9113 | M | A genuine streaming or chunk-reordering scheduler |
| Client-initiated `Upgrade: h2c` | Deliberately not done | RFC 9113 obsoleted that upgrade; cleartext HTTP/2 uses prior knowledge (`UnencryptedHTTP2`). The server still accepts the upgrade for curl's sake | S | Never |
| `Upgrade: h2c` over TLS | Deliberately not done | TLS may only switch protocol through ALPN | — | Never |
| 1xx interim responses to HTTP/1.0 requests | Deliberately not done | HTTP/1.0 clients do not know 1xx; `WriteInterim` returns `http.ErrNotSupported` | — | Never |
| Pushing a pushed request | Deliberately not done | PUSH_PROMISE may only be sent on a client-initiated stream | — | Never |
| Recycling streams whose body streams to the handler | Planned | Their `feed` callbacks can outlive the response, so safe reuse needs an extra feed-lifetime reference; the risk outweighs the payoff, one stream allocation | M–H | Streamed request bodies become the common case and allocations show up in profiles |
| Vectored send for multi-frame bodies | Planned | It needs the protocol layer to hand several buffers to `Layer` at once. pprof puts the h2 send path at the syscall (44.7% `Syscall6`) with memmove at 2.5%, so the ceiling is a few percent; the second copy of a one-frame body is already gone over h2c | M | Copies on the send path climb into the profile's top rows |
| Client-side 0-RTT | Planned | HTTP/2's framing depends on ALPN, which is only known after the handshake. The server side is done (`Config.EarlyData`, `Early-Data: 1`, 425 Too Early) | M | The TLS layer can know ALPN in advance (for example, offering only h2) |

## 3. HTTP/1.x

| Item | Status | Why | Cost | Reconsider when |
| --- | --- | --- | --- | --- |
| The `compress` (LZW) transfer coding | Deliberately not done | Go's `compress/lzw` stops at 12-bit codes while `compress(1)` grows to 16, so it cannot read past the first few KiB of a stream; supporting it means writing a 9→16-bit LZW. RFC 9110 deprecated the coding | M | An old client or proxy that only sends `compress` (expected: never) |
| `br` (Brotli) | Deliberately not done | No standard-library implementation, and a third-party module breaks fib's zero-dependency rule | L | Dependencies become acceptable |
| Client pipelining | Deliberately not done | One request per connection keeps the semantics simple and avoids head-of-line amplification; concurrency belongs to HTTP/2 | M | Never |

## 4. The fib engine and the other packages

| Item | Status | Why | Cost | Reconsider when |
| --- | --- | --- | --- | --- |
| UDP writability events on Windows | Deliberately not done | Completion ports have no "writable" notion (readability is a zero-byte `WSARecv`, writability is inferred from a `WSASend` completion), so it would degrade to polling; the 1 ms retry timer already approximates it, and Windows rarely serves QUIC | M | Windows becomes a primary QUIC platform |
| `ENOBUFS` (a full socket) notification on macOS | Deliberately not done | `sendmsg_x` has no "there is room again" signal, so the 1 ms timer stays; `EAGAIN` already goes through the writable event | S–M | A high-throughput QUIC server on macOS |
| kqueue writable events verified only on darwin/arm64 | Known limit | The `EVFILT_WRITE` path has run on macOS; FreeBSD, NetBSD, OpenBSD and DragonFly have only cross-compilation behind them | S | A BSD production deployment |
| `MaxWriteQueueBytes` lets one message overshoot | Known limit | The cap is checked after queueing, so a single oversized send can pass it by one message before the connection closes; that keeps "one send either succeeds or closes the connection" simple | S | A strict cap is required |
| The WebSocket broadcast send queue | Mitigated | Unlimited by default (only `MaxWriteQueueBytes`/`SetWriteQueueLimit` bound it); a peer that never reads must be dropped by application-level heartbeats | — | See the broadcast section of `guide.zh-CN.md` |

## 5. Testing and infrastructure

| Item | Status | Why | Cost | Reconsider when |
| --- | --- | --- | --- | --- |
| A CI guard that a clean checkout of main builds | Planned | Several outside commits carried other people's files back to a stale state and left main unbuildable; it was found by hand and repaired each time | S | Do it soon |
| The `http3-interop-image` job depends on Docker Hub | Known risk | It pulls the `golang:1.27.3` base image; a rate limit can fail a PR | S | On the first failure: make it non-blocking, or mirror the image |
| Interop Runner's `handshakecorruption`/`transfercorruption` | Planned | They share the endpoint paths of `handshakeloss`/`transferloss` and have not been run on their own | S | When completing the matrix |
| Interop with more QUIC implementations | Partial | There is a containerised endpoint (`http3/interoprunner/`) and a CI job, but only quic-go is a peer; quiche, ngtcp2 and mvfst are not wired in | M | When broader compatibility evidence is needed |
| 0-RTT interop with more servers, and replay protection across servers | Planned | The ticket registry is per process; a server behind a load balancer must protect itself | M | When 0-RTT is actually deployed |
| Load testing (concurrency problems) | Done | The full campaign in `benchmark/loadtest/` plus `stallmem.sh`; conclusions in `docs/loadtest*.md` | — | See "a clean A/B" in that document |
| The `arpc` reconnect test's ephemeral-port race | Known limit | `TestReconnectGivesUp` relies on the server's port staying closed during the retry window; another process rebinding it could confuse the test, which 6600+ runs have not shown | S | On a false failure: use a fixed port or inject connection refusal |
| HTTP/3 whole responses with a trailer cost one extra list allocation | Known cost | The send buffer inlines two pieces; a third slot would cost ~56 bytes on every stream, judged not worth it | S | Trailers become the norm |

## 6. How to re-evaluate an item

1. Measure first: is there evidence that it is a bottleneck now (pprof, the load campaign, real traffic)? The method in `docs/loadtest*.md` applies.
2. Weigh the cost column: S and M items can be scheduled directly; L items (BBR, WebTransport, active migration, assembly) should wait until there is a way to validate them.
3. After doing one, delete its row here and record the reasoning in the commit message and the protocol document; this page keeps no history.

## 7. Where each item is recorded

| Content | Chinese | English |
| --- | --- | --- |
| HTTP/1 limits and planned work | `docs/http1.zh-CN.md`, "当前限制", "待优化" | `docs/http1.md`, "Current limitations", "Planned improvements" |
| HTTP/2 limits, intentional omissions, planned work | `docs/http2.zh-CN.md`, "当前限制", "有意没有实现的功能", "待优化项" | `docs/http2.md`, "Current limitations", "Intentionally not implemented", "Planned improvements" |
| HTTP/3 limits, intentional omissions, planned work | `docs/http3.zh-CN.md`, "当前限制", "有意没有实现的功能", "待优化项" | `docs/http3.md`, "Current limitations", "Intentionally not implemented", "Planned improvements" |
| Engine, TLS and WebSocket trade-offs | `docs/guide.zh-CN.md`, the relevant sections | The same (the guide is Chinese only) |
| Load-test method and conclusions | `docs/loadtest.zh-CN.md` | `docs/loadtest.md` |
| Per-case interop results | `http3/interoprunner/README.md` | The same |

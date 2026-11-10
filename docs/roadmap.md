# fib roadmap: what is planned but not done, and what is deliberately not done

Each protocol document (`http1*.md`, `http2*.md`, `http3*.md`, `webrtc*.md`, `media*.md`) has its own "Intentionally not implemented" and "Planned improvements" sections. This page gathers them, plus the trade-offs recorded in `guide.zh-CN.md`, so that the whole picture is in one place: for every item, **why** it is not done, roughly **what it would cost**, and **when it would be worth reconsidering**.

- The cost column is an order of magnitude: S (a day or two), M (a few days), L (a week or more, or it needs test infrastructure that does not exist yet).
- "Deliberately not done" is a conclusion, not an oversight. When the requirement changes, re-evaluate it as described in section 8.
- The authoritative detail stays in the protocol documents; this page indexes it and records the reason, citing the source at the end of each row.

## 1. QUIC and HTTP/3

| Item | Status | Why | Cost | Reconsider when |
| --- | --- | --- | --- | --- |
| BBR congestion control | Deliberately not done | It replaces the *model of the path*, not just the window: it estimates bandwidth from per-round delivery samples, derives a pacing rate, and has its own startup, drain and probe-bw phases. Doing it soundly means extending the sent-packet record to per-round delivery sampling and reconciling it with RFC 9002 loss recovery and persistent-congestion detection. Large, and beyond in-memory pipes there is no reliable way to validate it here; half of it would be worse than none | L | There is production traffic on real paths, and measurements show CUBIC is the bottleneck on lossy or long-fat ones |
| ECN | Deliberately not done | Reading and writing the ECN bits of the IP header needs the Engine's UDP layer; the benefit is mostly in the congestion controller, and without one that uses it the gain is small | M | A datacentre deployment, or an ECN-aware congestion controller |
| ChaCha20-Poly1305 in assembly (AVX2/NEON) | Deliberately not done | A per-architecture, constant-time-sensitive body of code, for a suite only machines *without* AES hardware negotiate first. The pure-Go version is already 54–81% faster than the implementation it replaced | L | The target machines are mostly without AES hardware |
| Pooling QUIC `*Stream` objects | Deliberately not done | A `*Stream` is handed to the application, which owns its lifetime; reuse would turn "used after it ended" into corruption of another stream. Opening and closing a stream costs 8 allocations; request streams and headers are already recycled | M | Never: this is the API's ownership boundary |
| Active connection migration, `preferred_address` | Deliberately not done | The server issues and routes several Connection IDs per connection, but `PATH_CHALLENGE` is answered on the path it is sending on rather than on the path the challenge arrived from, so a client cannot validate a new path before migrating. NAT rebinding, including with a changed Connection ID, is followed, which covers the practical case | L | Mobile clients that move between Wi-Fi and cellular often |
| Server push | Deliberately not done | No mainstream browser enables HTTP/3 push; the client sends no `MAX_PUSH_ID` and `Context.Push` returns `http.ErrNotSupported`. Preloading uses 103 Early Hints (`WriteInterim`) | L | Never: the browser side is gone |
| DATAGRAM (RFC 9221), WebTransport | Deliberately not done | They need streaming streams or unreliable datagrams, which the handler model does not offer; an incoming DATAGRAM frame is treated as an unknown frame and closes the connection with `FRAME_ENCODING_ERROR` | L | Building real-time media or game services, which also means extending the handler model |
| QUIC v2 (RFC 9369) and other versions | Deliberately not done | Version 1 is enough for deployment; a client fails outright on Version Negotiation | S–M | A deployment requires v2 |
| One fewer copy on the receive path | Deliberately not done | A datagram is decrypted in place, and in-order stream data is handed on as slices of the engine's pooled copy. pprof (Linux, HTTP/3 256 KiB uploads) puts the engine's copy at 0.68% of CPU and the body gathering the API needs at 0.76%, against 3.9% for AES-GCM; zero-copy reads would save at most the 0.68%, but need refcounts on GRO reads that share a 64 KiB slot, and queued datagrams would hold whole slots | M | The engine's copy climbs into the profile's top rows, e.g. with jumbo or GRO-heavy receive |
| The Interop Runner's `ecn`, `v2` and `connectionmigration` cases | Deliberately not done | They follow directly from the three items above; the endpoint exits 127 and the runner records them as unsupported rather than passed | — | Same as those items |

## 2. HTTP/2

| Item | Status | Why | Cost | Reconsider when |
| --- | --- | --- | --- | --- |
| Client receives server push | Deliberately not done | Chrome and Firefox removed push, and Go's `net/http` client never supported it; the client declares `ENABLE_PUSH=0`. Preloading uses 103 Early Hints | M | Never |
| RFC 9218's incremental hint `i`, PRIORITY_UPDATE frames | Deliberately not done | The `priority` header's urgency **is** used for send scheduling; `i` is not. RFC 7540's priority tree was obsoleted by RFC 9113 | M | A genuine streaming or chunk-reordering scheduler |
| Client-initiated `Upgrade: h2c` | Deliberately not done | RFC 9113 obsoleted that upgrade; cleartext HTTP/2 uses prior knowledge (`UnencryptedHTTP2`). The server still accepts the upgrade for curl's sake | S | Never |
| `Upgrade: h2c` over TLS | Deliberately not done | TLS may only switch protocol through ALPN | — | Never |
| 1xx interim responses to HTTP/1.0 requests | Deliberately not done | HTTP/1.0 clients do not know 1xx; `WriteInterim` returns `http.ErrNotSupported` | — | Never |
| Pushing a pushed request | Deliberately not done | PUSH_PROMISE may only be sent on a client-initiated stream | — | Never |
| Recycling streams whose body streams to the handler | Planned | Their `feed` callbacks can outlive the response, so safe reuse needs an extra feed-lifetime reference; the risk outweighs the payoff, one stream allocation | M–H | Streamed request bodies become the common case and allocations show up in profiles |
| Vectored send of multi-frame bodies over TLS | Planned | Done for h2c (bodies over 4 KB go out beside their frame headers: 64 KB–4 MB responses 24–46% faster). Over TLS the layer seals each send into records of its own, so a header and payload sent apart would cost a record per frame; it needs an N-part seal in the `Layer` interface, and AES-GCM, not the copy, dominates that path | M | TLS send throughput becomes copy-bound |
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
| KCP: Salsa20, FEC auto-tuning, kcp-go's out-of-band packets, several conversations per address | Deliberately not done | Salsa20 needs `golang.org/x/crypto` and is not a block cipher; kcptun's default is AES. Auto-tuning only papers over a misconfigured peer. Out-of-band packets and per-address multiplexing are kcp-go- and reference-specific extras that kcp-go's own Listener does not offer either | S each | A user who needs one of them |
| BitTorrent: BEP 33 (DHT scrape) | Deliberately not done | Its bloom filters estimate how many seeds and peers a torrent has; nothing in fib uses that, and a client gets its counts from tracker scrapes and the peers a lookup returns | S | A client that shows swarm sizes for trackerless torrents |
| BitTorrent: BEP 35 (torrent signing), BEP 38 (local data hints) | Deliberately not done | BEP 35 is a draft that mainstream clients neither sign with nor check; BEP 38's `similar` and `collections` keys only help a client that searches the data of other torrents it already has | S–M each | A publisher that signs its torrents; a client with a shared store to search |
| BitTorrent tracker: BEP 31 (`retry in`), full scrape | Deliberately not done | The tracker client returns a failure as an `*Error` and leaves when to try again to its caller; a full scrape (no `info_hash`) hands every torrent a tracker has to anyone who asks, in one large reply, so the server answers only scrapes that name their hashes | S each | A tracker that relies on `retry in`; a private tracker that wants full scrape for its own tools |
| BitTorrent uTP: path MTU discovery, the clock-drift penalty | Deliberately not done | Packets are a fixed 1400 bytes, below the Ethernet MTU with IPv6, UDP and a tunnel's headers; discovery means probing with larger packets and telling their loss apart from congestion. The drift penalty guards LEDBAT against a peer whose clock runs fast, which libutp measures and fib does not | M | Jumbo-frame links where packet size limits throughput, or peers with drifting clocks |
| BitTorrent DHT: answering for the other address family | Deliberately not done | A `Node` is of one address family, as libtorrent's are, and ignores `want` for the other; answering both needs a client's IPv4 and IPv6 Nodes to share their tables | S–M | A dual-stack client that wants each family to bootstrap the other |
| BitTorrent DHT: `cas` when putting BEP 44 mutable items | Planned | A `Node` honours `cas` in the puts it stores, but `Put` sends none, so two writers of one key can overwrite each other's newer item between their get and put | S | An application with more than one writer per key |
| BitTorrent client: BEP 52 proofs served with `proof_layers` > 0 | Known limit | The client asks for hashes with `proof_layers` 0 and answers hash requests from its piece layers; uncles for a request with proof layers follow a reading of libtorrent's semantics, and neither libtorrent nor anacrolix asked for them in the interop runs | S | A peer that asks for proofs, or a libtorrent interop case that does |
| BitTorrent client: BEP 16 (superseeding), BEP 40 (canonical peer priority), ut_holepunch (BEP 55) | Deliberately not done | Superseeding helps only the first seed of a torrent with few peers; canonical priority orders which peers to keep when connections are scarce, which `MaxConns` and the dial backoff handle well enough; holepunching needs a relay peer both sides reach and only helps two peers that are both behind NATs; a client with a reachable port does not need it | M each | An initial seeder of rare content; swarms bounded by connection slots; many NATed peers that cannot reach each other |
| BitTorrent client: BEP 30 (merkle torrents) | Deliberately not done | Superseded by BEP 52's per-file merkle trees, which the client supports; few torrents and clients ever used BEP 30 | M | Never |
| BitTorrent client: persisted DHT node state, a fast-resume file | Planned | `dht.Node.State` and `AddNodes` can save and restore the routing table, but the client does not yet store them, so each start bootstraps from the routers; without a resume file every added torrent is hash-checked, which costs a full read of its data | S each | Restarts that must be fast, or large libraries of seeded torrents |
| BitTorrent client: local service discovery over IPv6 | Deliberately not done | fib has no multicast bindings, so LSD uses the standard library's IPv4 `net.ListenMulticastUDP`; IPv6 LSD (ff15::efc0:988f) would need another socket and is rarely deployed | S | LANs that are IPv6-only |
| The WebSocket broadcast send queue | Mitigated | Unlimited by default (only `MaxWriteQueueBytes`/`SetWriteQueueLimit` bound it); a peer that never reads must be dropped by application-level heartbeats | — | See the broadcast section of `guide.zh-CN.md` |

## 5. WebRTC

| Item | Status | Why | Cost | Reconsider when |
| --- | --- | --- | --- | --- |
| Transport-wide congestion control feedback and bandwidth estimation | Deliberately not done | The PeerConnection forwards RTP and has no encoder to steer; an SFU leaves rate control to the browsers at each end, and REMB can be sent through `WriteRTCP` | L | A Go sender with its own encoder, or an SFU that adapts per viewer |
| FEC, and NACKs from fib's receivers | Deliberately not done | NACKs are answered from a per-track history, as RTX when negotiated (`Configuration.Retransmit`, off by default; media/rtc WHEP on with 256 packets per track). FEC (ulpfec, flexfec, red) is not offered, and fib sends no NACKs for what it receives (an application can, with `rtp.NACK` and `WriteRTCP`) | M | Paths where loss is too high or the round trip too long for retransmission |
| Sending simulcast | Deliberately not done | Receiving it by RID works, which is what an SFU needs; sending layers means an encoder per layer | M | A Go publisher of simulcast |
| ICE over TCP (RFC 6544) | Deliberately not done | Browsers reach a server with a public UDP port without it, and a browser behind a UDP-blocking firewall reaches the server through TURN over TCP or TLS (turnserver serves both, and the PeerConnection's own TURN client speaks UDP, TCP and TLS) | M | Deployments behind UDP-blocking firewalls |
| DTLS 1.3 | Deliberately not done | Every browser speaks DTLS 1.2, which is what WebRTC requires today | L | Browsers require DTLS 1.3 |
| mDNS resolution of host candidates | Deliberately not done | A browser's checks reveal its address as a peer-reflexive candidate anyway, and a lite server needs nothing more | S | Two full agents behind mDNS on one LAN must connect without STUN |
| Provisional answers and rollback | Deliberately not done | Glare handling belongs to the signaling; neither WHIP nor WHEP uses them | S | An application negotiates from both sides at once |
| Interop tests in CI | Partly done | `webrtc/interop` (CI job `webrtc-interop`, non-blocking) runs fib against pion: PeerConnections both ways with a data channel and media, TURN relay through turnserver, and DTLS; OpenSSL, Chromium and pion's ice, srtp, sctp and sdp checks are still run by hand | M | Before declaring the API stable; make the job blocking once it has run stably |

## 6. Streaming media

| Item | Status | Why | Cost | Reconsider when |
| --- | --- | --- | --- | --- |
| Transcoding (AAC to Opus for WHEP, renditions for HLS) | Deliberately not done | It needs audio and video decoders and encoders, which means cgo or a dependency, and CPU per stream that a remuxing server does not spend; the hub carries what publishers send and each egress leaves out what it cannot carry | L | Never in fib itself: an external transcoder subscribes and republishes into the hub |
| RTSP over TLS, over HTTP tunnels, RTSP 2.0, multicast | Deliberately not done | Cameras and players speak plain RTSP 1.0 over TCP or UDP unicast | M | Deployments that require rtsps or tunnelling through HTTP proxies |
| SRT rendezvous, file mode, AES-GCM, FEC, bonding | Deliberately not done | Live mode with AES-CTR is what ffmpeg, OBS and encoders use for ingest and distribution | L | An encoder or network that needs one of them |
| WHIP/WHEP ICE restart, simulcast layers | Known limit | ICE restart is answered 422, as RFC 9725 asks; only a section's first simulcast layer is received | M | Mobile publishers that change networks, or SFU-style layer selection |
| Recording, DVR and VOD | Deliberately not done | Every stream is live; a recorder is a hub subscriber the application writes (fmp4 and mpegts mux to files as well as to segments) | M | Recording becomes a common request |
| HLS delta playlists, rendition reports, PROGRAM-DATE-TIME, encryption | Deliberately not done | One variant per stream, no transcoding, and players play without them | M | Long playlists where delta updates save bandwidth, or DRM |
| Interop tests in CI | Partly done | `media/interop` (CI job `media-interop`, non-blocking) runs the ffmpeg 7.1 matrix: H.264/H.265 with AAC/Opus, 4 ingests by 8 egresses, plus WHEP; ffmpeg 5.1, G.711, Chromium, A/V sync, republish, load and stalled players are still run by hand | M | Before declaring the API stable; make the job blocking once it has run stably |

## 7. Testing and infrastructure

| Item | Status | Why | Cost | Reconsider when |
| --- | --- | --- | --- | --- |
| Interop with more QUIC implementations | Mostly done | The QUIC Interop Runner (`http3/interoprunner/`, a CI job on every push) passes against quic-go in both roles, and against ngtcp2 except `connectionmigration` (fib's client does not move to a `preferred_address`); the failures left against quiche are on quiche's side or come from running its amd64 image emulated (see the README there); mvfst has not been run | S | When mvfst compatibility is needed, or the client learns `preferred_address` |
| 0-RTT interop with more servers, and replay protection across servers | Planned | The ticket registry is per process; a server behind a load balancer must protect itself | M | When 0-RTT is actually deployed |
| Load testing (concurrency problems) | Done | The full campaign in `benchmark/loadtest/` plus `stallmem.sh`; conclusions in `docs/loadtest*.md` | — | See "a clean A/B" in that document |
| The `arpc` reconnect test's ephemeral-port race | Known limit | `TestReconnectGivesUp` relies on the server's port staying closed during the retry window; another process rebinding it could confuse the test, which 6600+ runs have not shown | S | On a false failure: use a fixed port or inject connection refusal |
| HTTP/3 whole responses with a trailer cost one extra list allocation | Known cost | The send buffer inlines two pieces; a third slot would cost ~56 bytes on every stream, judged not worth it | S | Trailers become the norm |

## 8. How to re-evaluate an item

1. Measure first: is there evidence that it is a bottleneck now (pprof, the load campaign, real traffic)? The method in `docs/loadtest*.md` applies.
2. Weigh the cost column: S and M items can be scheduled directly; L items (BBR, WebTransport, active migration, assembly) should wait until there is a way to validate them.
3. After doing one, delete its row here and record the reasoning in the commit message and the protocol document; this page keeps no history.

## 9. Where each item is recorded

| Content | Chinese | English |
| --- | --- | --- |
| HTTP/1 limits and planned work | `docs/http1.zh-CN.md`, "当前限制", "待优化" | `docs/http1.md`, "Current limitations", "Planned improvements" |
| HTTP/2 limits, intentional omissions, planned work | `docs/http2.zh-CN.md`, "当前限制", "有意没有实现的功能", "待优化项" | `docs/http2.md`, "Current limitations", "Intentionally not implemented", "Planned improvements" |
| HTTP/3 limits, intentional omissions, planned work | `docs/http3.zh-CN.md`, "当前限制", "有意没有实现的功能", "待优化项" | `docs/http3.md`, "Current limitations", "Intentionally not implemented", "Planned improvements" |
| WebRTC limits and intentional omissions | `docs/webrtc.zh-CN.md`, "配置与限制", "有意没有实现的功能" | `docs/webrtc.md`, "Configuration and limits", "Intentionally not implemented" |
| Streaming media limits and intentional omissions | `docs/media.zh-CN.md`, "配置与限制", "有意没有实现的功能" | `docs/media.md`, "Configuration and limits", "Intentionally not implemented" |
| KCP decisions and compatibility | `docs/kcp.zh-CN.md`, "待审的决定" | `docs/kcp.md`, "Decisions pending review" |
| BitTorrent decisions, BEP coverage and interop | `docs/bittorrent.zh-CN.md`, "待审的决定", "BEP 覆盖" | `docs/bittorrent.md`, "Decisions pending review", "BEP coverage" |
| Engine, TLS and WebSocket trade-offs | `docs/guide.zh-CN.md`, the relevant sections | The same (the guide is Chinese only) |
| Load-test method and conclusions | `docs/loadtest.zh-CN.md` | `docs/loadtest.md` |
| Per-case interop results | `http3/interoprunner/README.md` | The same |

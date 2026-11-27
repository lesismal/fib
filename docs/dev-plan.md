# fib dev plan: benchmark and optimization status

English | [简体中文](dev-plan.zh-CN.md)

This page records, for every feature of fib that needs a benchmark, whether it has been
benchmarked and whether it has been optimized. What is planned but not built, and what is
deliberately not built, is in the [roadmap](roadmap.md); how features and benchmarks are worked on
is in the [rules](rules.md); the load-test method is in [load testing](loadtest.md).

A feature needs a benchmark when it sits on a hot path: the engine's reads and writes, parsers,
framing, crypto, flow control, buffer and task pools, and the per-packet or per-message work of a
protocol. Every row whose two status columns are not both "yes" is an open todo item for a
benchmark task. Under rule 2 of the [rules](rules.md), a feature task runs no benchmarks: it adds
(or keeps) its feature's row here as "no" and names what should be measured; a benchmark task
measures, optimizes where it pays, and updates the row.

- **Benchmarked**: "yes" when Go benchmarks or a recorded measurement cover the hot path; "partial"
  when only part of it is covered, or only indirectly through another feature's benchmark; "no"
  when nothing measures it.
- **Optimized**: "yes" when measurement-driven optimizations have landed; "partial" when some have
  but a known part has not been looked at; "no" when none have.
- Evidence is a date, a commit or a doc; the commit log's `perf(...)` entries hold the detail.
- Status as of 2026-11-26.

## Status

| Feature | Package | Benchmarked | Optimized | Evidence or what is left |
| --- | --- | --- | --- | --- |
| Engine event loop: reads, writes, pollers, backpressure | `fib` (root), `core` | yes (2026-10-26, `benchmark/loadtest`, [loadtest](loadtest.md)) | yes (d57d895, 1d6841b, a1ba88b, c16c260, 9dcfc6b) | No Go micro-benchmark in the package itself; measured through the HTTP and WebSocket benchmarks and the load campaign |
| UDP batching and GSO | `fib` (root), `core` | partial (only through the HTTP/3 and QUIC benchmarks) | yes (8643164, e4cb6ab; eb2b9d2 reverted) | A datagram send/receive benchmark of its own, GRO-heavy receive |
| Buffer pool and Arena allocator | `bufferpool` | yes (2026-11-19, [bufferpool-allocator](bufferpool-allocator.md)) | yes (5c574b0, 3df429f, d91b7a4, 419f7bd) | — |
| Task pools | `taskpool` | partial (`taskpool/benchmark` against nbio, ants and gopool, 662e533; the on-demand queue watcher 759cb6a against 9e0eaa0 on .51, 2026-11-27, 8 server CPUs: HttpArena baseline +1.5%, pipelined +0.8%, limited-conn +2.0%, latency-10k CPU/req -2.1%; an idle process with 8 adaptive pools 1,250 -> 165 µs/s CPU, 54 -> 8 wake-ups/s; pool load scenarios within ±2%; single-handoff GoTask/Handoff +3-7% in every mode, elastic too, with the same instruction count and +26-28% branch misses, so code placement, not the change; one watcher tick over 8 armed pools 194 ns before (every tick, always), 284 ns now (only while armed); Hand's per-pool lists of 2026-11-26 not measured; since 2026-11-27 the watcher samples every 500 ms by default (was 100 ms; `SetSampleInterval` sets it) and disarms after 2 s calm (was 1 s), `SetAutoSample(false)` switches it off so a deep submission reads one switch, and `taskpool.Sample` samples by hand) | yes (13b3d41, 8380486, a6d78cd, 8895a6a) | The deep submissions with automatic sampling on against off (`taskpool/benchmark`, a queue held past half full); `taskpool.Sample` over many pools; `Engine.Hand` with one pool full while another takes a load, against one shared list; the `TryGo` turn-away path under a full queue; the memory one worker of each pool holds, which internal/poolsize estimates (64 KiB engine, handler and handshake workers, 32 KiB client, 16 KiB flush and notify, 256 KiB file) |
| TLS records: sealing, kernel TLS, SendFile | `tls` | yes (`BenchmarkSendFileTLS`, 322315d) | yes (9beb221, 42867d8, 5a02929, 881ff97, 08faed6) | — |
| TLS handshakes | `tls` | yes (`BenchmarkServerHandshake`, `BenchmarkClientHandshake`, 01a143d) | partial | The benchmarks exist; no optimization pass driven by them is recorded |
| HTTP/1 server: parser, response framing | `http` | yes (`BenchmarkParser*`, `BenchmarkServerKeepAlive*`, `BenchmarkParseRequestHead`, load campaign) | yes (8bb8a82, de62b0b, e17bf98, 534f420, 2e2fef1) | — |
| HTTP/1 client | `http` | yes (`BenchmarkMarshalRequest`, `BenchmarkParseResponseHead`, `BenchmarkClientNewConnection`) | yes (0a9339f, e51b8f5, a9493a8, 415e755) | — |
| HTTP/2 server: frames, flow control, streams | `http` | yes (`BenchmarkH2Server*`, 10e984f, [http2](http2.md) "Benchmarks"; 2026-11-26 h2load A/B of where the flush runs on the benchmark host) | yes (73c0c3b, d03e91e, 6d145b4, e02d9a9, 089a5b0) | — The flusher offered to the handler pool with `TryGo` first and sent to `fib-flush` when that is full, against 7404d01, which offered it with `Go` (h2load, 64 connections, server on 8 CPUs, 5 rotated rounds, medians): requests/s within ±1% at 1, 10 and 100 streams, writes per response the same (1.00, 0.101, 0.0109), p99 and CPU per request at 1,000 and 4,000 requests/s within 1%. Sending every flush to `fib-flush` matched the throughput but raised p99 10-21%, p999 about 30% and CPU per request 2-5% at those rates. Flushing when each handler returns held a response back until its handler returned, and with 0.59 writes per response at 10 streams lost 54% at 10 and 100. Neither is kept |
| HTTP/2 client | `http` | partial (no client-side HTTP/2 benchmark with many streams) | partial (cb9bbbe) | A client benchmark of one connection with many requests in flight |
| HPACK | `hpack` | yes (encode and decode benchmarks, 0e7016b) | yes (0c47caf, 346f48e, 871bdb7, d7e117c) | — |
| Router, Context.JSON | `http`, `http/routerbench` | yes (`BenchmarkRouter*`, `BenchmarkGitHub`, `BenchmarkJSONResponse`) | yes (240182e, b39373d) | — |
| Streamed request and response bodies | `http`, `internal/reqbody` | yes (`BenchmarkBodyStreamBusySink`, `BenchmarkPusherWrite`, 9417002) | yes (309a6cd, 0eb21d6, 6c61693, 4ad4ebe) | — |
| QUIC transport: packets, ACKs, loss recovery, pacing | `http3/quic` | yes (`BenchmarkQUIC*`, e1468a0, [http3](http3.md)) | yes (ab315b7, 5c23798, c6c2e10, b926ebf, 97f0c53) | — |
| QUIC ChaCha20-Poly1305 | `http3/quic` | yes (`BenchmarkChaCha20Poly1305`, 4c6e733) | yes (4c6e733, 54–81% faster) | Assembly deliberately not done ([roadmap](roadmap.md) §1) |
| HTTP/3 and QPACK | `http3`, `http3/qpack` | yes (`BenchmarkHTTP3*`, `qpack.BenchmarkSections`) | yes (12e1829, c6aac3c, 47948dd, d431987) | — |
| WebSocket: frames, masking, handshake, echo | `websocket` | yes (`frame_benchmark_test.go`, `BenchmarkServerEcho1KiB`, `BenchmarkBroadcast`) | yes (901ebf5, 1d6f630, aaedfb4, 4bd8818, 081d83e) | — |
| WebSocket permessage-deflate | `websocket`, `internal/inflate` | yes (`BenchmarkCompressMessage`, `BenchmarkDecompressMessage`, `inflate.BenchmarkDecode`) | partial | No optimization of compression recorded; measure the compress path's cost and allocations |
| gRPC over package http | `grpc` | yes (`BenchmarkServeHTTPUnary`) | yes (fddcb68) | Streaming RPCs are not benchmarked |
| arpc | `arpc`, `benchmark/rpc` | yes (`benchmark/rpc` against grpc, 1905364) | partial | The benchmark compares CoalesceWrites; no profiling of arpc's own call path recorded |
| KCP: ARQ, FEC (Reed-Solomon), packet crypt | `kcp` | no | no | Throughput and allocations per packet against kcp-go; FEC encode/decode; crypt per packet |
| WebRTC SCTP data channels | `webrtc/sctp` | yes (`BenchmarkThroughput`, 2026-11-01, [webrtc](webrtc.md)) | partial | 240 MB/s against pion's 163 MB/s and no allocation per message; no profiling pass recorded |
| SRTP, RTP, SDP | `webrtc/srtp`, `webrtc/rtp`, `webrtc/sdp` | yes (`BenchmarkProtectRTP`, `BenchmarkUnprotectRTP`, `BenchmarkPacketUnmarshal`, `BenchmarkParse`) | partial | No optimization recorded; AES-CM with HMAC-SHA1 (890 ns a 1200-byte packet) is the candidate |
| STUN, ICE, TURN relay | `webrtc/stun`, `webrtc/ice`, `webrtc/turn` | no | no | TURN relay throughput and allocations per relayed packet; STUN message parsing |
| Media hub fan-out and RTMP, RTSP, SRT, HTTP-FLV, MPEG-TS, fMP4 muxing | `media/...` | no | no | Hub fan-out to many viewers; per-frame cost of each muxer and demuxer; SRT packet path |
| HLS muxer | `media/hls` | partial (`BenchmarkMuxerEncryption`, `BenchmarkMuxerPlaylistWait`) | no | The segmenting path without encryption, and many concurrent playlist readers |
| BitTorrent download | `bittorrent`, `bittorrent/peer` | yes (2026-11-09, [bittorrent](bittorrent.md) "Performance") | yes (a5511a8, d12e3f3) | — |
| Bencode | `bittorrent/bencode` | yes (`BenchmarkMarshalStruct`, `BenchmarkUnmarshalStruct`) | no | No optimization recorded |
| uTP | `bittorrent/utp` | no | no | Throughput and allocations per packet against libutp |
| SOCKS relay | `socks` | no | no | Relay throughput and allocations per connection |
| File watching | `filewatch` | yes (`BenchmarkWatchWatched`) | yes (047fd40) | — |

## Open todo

From the table, for benchmark tasks:

- Not benchmarked at all: KCP, STUN/ICE/TURN relay, media hub and the muxers other than HLS, uTP,
  SOCKS relay.
- Benchmarked only in part: UDP batching and GSO, the HTTP/2 client, the HLS muxer.
- Benchmarked but not (fully) optimized: TLS handshakes, permessage-deflate, arpc, SCTP, SRTP,
  bencode.

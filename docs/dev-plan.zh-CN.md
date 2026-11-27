# fib 开发计划：基准测试与优化状态

[English](dev-plan.md) | 简体中文

本页记录 fib 每个需要基准测试的功能：是否已做基准测试、是否已做优化。计划中但未做、以及有意不做的项见
[路线图](roadmap.zh-CN.md)；功能开发与压测如何进行见[开发规则](rules.zh-CN.md)；压测方法见[压测](loadtest.zh-CN.md)。

处在热路径上的功能需要基准测试：引擎的读写、解析器、分帧、加解密、流控、缓冲池和任务池，以及协议中按包或按消息
进行的工作。两个状态列不都是"是"的行，都是留给压测任务的待办项。按[开发规则](rules.zh-CN.md)第 2 条，功能任务不
跑基准测试：它在这里新增（或保留）该功能的行，标为"否"，并写明应当测什么；压测任务负责测量、在值得时优化，并更
新这一行。

- **已测**："是"表示 Go 基准测试或有记录的测量覆盖了热路径；"部分"表示只覆盖了一部分，或只通过其他功能的基准间
  接覆盖；"否"表示没有任何测量。
- **已优化**："是"表示已有基于测量的优化合入；"部分"表示做过一些，但已知有部分未看过；"否"表示没有。
- 依据写日期、提交或文档；细节见提交日志中的 `perf(...)` 条目。
- 状态截至 2026-11-26。

## 状态

| 功能 | 包 | 已测 | 已优化 | 依据或剩余工作 |
| --- | --- | --- | --- | --- |
| 引擎事件循环：读、写、poller、背压 | `fib`（根包）、`core` | 是（2026-10-26，`benchmark/loadtest`，[压测](loadtest.zh-CN.md)） | 是（d57d895、1d6841b、a1ba88b、c16c260、9dcfc6b） | 包内没有 Go 微基准；通过 HTTP、WebSocket 基准和压测间接测量 |
| UDP 批量收发与 GSO | `fib`（根包）、`core` | 部分（只通过 HTTP/3 和 QUIC 基准） | 是（8643164、e4cb6ab；eb2b9d2 已回退） | 单独的数据报收发基准、GRO 密集的接收 |
| 缓冲池与 Arena 分配器 | `bufferpool` | 是（2026-11-19，[bufferpool-allocator](bufferpool-allocator.zh-CN.md)） | 是（5c574b0、3df429f、d91b7a4、419f7bd） | — |
| 任务池 | `taskpool` | 部分（`taskpool/benchmark`，对比 nbio、ants、gopool，662e533；按需上膛的队列采样 759cb6a 对比 9e0eaa0，2026-11-27 在 .51 上，服务端 8 个 CPU：HttpArena baseline +1.5%、pipelined +0.8%、limited-conn +2.0%、latency-10k 每请求 CPU -2.1%；带 8 个 adaptive 池的空闲进程 CPU 1250 -> 165 µs/s，唤醒 54 -> 8 次/s；各池负载场景在 ±2% 内；单次交接的 GoTask/Handoff 三种模式都 +3~7%（elastic 也是），指令数相同、分支预测失败 +26~28%，是代码布局所致而非改动本身；8 个已上膛池的一次采样 194 ns（旧：每次都采），现 284 ns（只在上膛时）；2026-11-26 加的 Hand 按池分列表未测；2026-11-27 起自动采样默认关闭（`SetAutoSample`），深的提交只读一个开关，`taskpool.Sample` 供手动采样） | 是（13b3d41、8380486、a6d78cd、8895a6a） | 自动采样关闭与打开时深的提交（`taskpool/benchmark`，队列保持过半）；多个池时 `taskpool.Sample` 的耗时；一个池满、另一个池在承载负载时的 `Engine.Hand`，对比共用一个列表；队列满时 `TryGo` 被拒的路径；各池每个 worker 实际占的内存，internal/poolsize 目前是估计值（engine、handler、握手 64 KiB，client 32 KiB，flush、notify 16 KiB，file 256 KiB） |
| TLS 记录：加密、内核 TLS、SendFile | `tls` | 是（`BenchmarkSendFileTLS`，322315d） | 是（9beb221、42867d8、5a02929、881ff97、08faed6） | — |
| TLS 握手 | `tls` | 是（`BenchmarkServerHandshake`、`BenchmarkClientHandshake`，01a143d） | 部分 | 基准已有；没有据此做的优化记录 |
| HTTP/1 服务端：解析器、响应分帧 | `http` | 是（`BenchmarkParser*`、`BenchmarkServerKeepAlive*`、`BenchmarkParseRequestHead`、压测） | 是（8bb8a82、de62b0b、e17bf98、534f420、2e2fef1） | — |
| HTTP/1 客户端 | `http` | 是（`BenchmarkMarshalRequest`、`BenchmarkParseResponseHead`、`BenchmarkClientNewConnection`） | 是（0a9339f、e51b8f5、a9493a8、415e755） | — |
| HTTP/2 服务端：帧、流控、流 | `http` | 是（`BenchmarkH2Server*`，10e984f，[http2](http2.zh-CN.md) 基准一节；2026-11-26 在 benchmark 主机上用 h2load 对 flush 在哪里执行做了 A/B） | 是（73c0c3b、d03e91e、6d145b4、e02d9a9、089a5b0） | — flusher 先用 `TryGo` 投给 handler 池，满了再投 `fib-flush`，对比用 `Go` 投 handler 池的 7404d01（h2load，64 个连接，服务端 8 个 CPU，5 轮交替，取中位数）：1、10、100 个 stream 时请求数/秒在 ±1% 内，每响应 write 次数相同（1.00、0.101、0.0109），每秒 1000 和 4000 个请求时 p99 和每请求 CPU 在 1% 内。每次 flush 都投 `fib-flush` 的做法吞吐持平，但在这两档负载下 p99 高 10-21%、p999 高约 30%、每请求 CPU 高 2-5%。在每个 handler 返回时 flush 的做法会把响应扣到 handler 返回才发出，10 个 stream 时每响应 0.59 次 write，10 和 100 个 stream 时慢 54%。两者都未采用 |
| HTTP/2 客户端 | `http` | 部分（没有多流并发的客户端 HTTP/2 基准） | 部分（cb9bbbe） | 一个连接上大量请求并发的客户端基准 |
| HPACK | `hpack` | 是（编解码基准，0e7016b） | 是（0c47caf、346f48e、871bdb7、d7e117c） | — |
| 路由、Context.JSON | `http`、`http/routerbench` | 是（`BenchmarkRouter*`、`BenchmarkGitHub`、`BenchmarkJSONResponse`） | 是（240182e、b39373d） | — |
| 流式请求体与响应体 | `http`、`internal/reqbody` | 是（`BenchmarkBodyStreamBusySink`、`BenchmarkPusherWrite`，9417002） | 是（309a6cd、0eb21d6、6c61693、4ad4ebe） | — |
| QUIC 传输：包、ACK、丢包恢复、pacing | `http3/quic` | 是（`BenchmarkQUIC*`，e1468a0，[http3](http3.zh-CN.md)） | 是（ab315b7、5c23798、c6c2e10、b926ebf、97f0c53） | — |
| QUIC ChaCha20-Poly1305 | `http3/quic` | 是（`BenchmarkChaCha20Poly1305`，4c6e733） | 是（4c6e733，快 54–81%） | 汇编实现有意不做（[路线图](roadmap.zh-CN.md)第 1 节） |
| HTTP/3 与 QPACK | `http3`、`http3/qpack` | 是（`BenchmarkHTTP3*`、`qpack.BenchmarkSections`） | 是（12e1829、c6aac3c、47948dd、d431987） | — |
| WebSocket：帧、掩码、握手、回显 | `websocket` | 是（`frame_benchmark_test.go`、`BenchmarkServerEcho1KiB`、`BenchmarkBroadcast`） | 是（901ebf5、1d6f630、aaedfb4、4bd8818、081d83e） | — |
| WebSocket permessage-deflate | `websocket`、`internal/inflate` | 是（`BenchmarkCompressMessage`、`BenchmarkDecompressMessage`、`inflate.BenchmarkDecode`） | 部分 | 没有压缩方向的优化记录；测量压缩路径的耗时与分配 |
| 基于 http 包的 gRPC | `grpc` | 是（`BenchmarkServeHTTPUnary`） | 是（fddcb68） | 流式 RPC 未做基准 |
| arpc | `arpc`、`benchmark/rpc` | 是（`benchmark/rpc`，对比 grpc，1905364） | 部分 | 该基准比较的是 CoalesceWrites；没有对 arpc 自身调用路径做剖析的记录 |
| KCP：ARQ、FEC（Reed-Solomon）、包加密 | `kcp` | 否 | 否 | 对比 kcp-go 的吞吐和每包分配；FEC 编解码；每包加解密 |
| WebRTC SCTP 数据通道 | `webrtc/sctp` | 是（`BenchmarkThroughput`，2026-11-01，[webrtc](webrtc.zh-CN.md)） | 部分 | 240 MB/s，pion 为 163 MB/s，每条消息零分配；没有剖析记录 |
| SRTP、RTP、SDP | `webrtc/srtp`、`webrtc/rtp`、`webrtc/sdp` | 是（`BenchmarkProtectRTP`、`BenchmarkUnprotectRTP`、`BenchmarkPacketUnmarshal`、`BenchmarkParse`） | 部分 | 没有优化记录；AES-CM 加 HMAC-SHA1（1200 字节包 890 ns）是候选 |
| STUN、ICE、TURN 中继 | `webrtc/stun`、`webrtc/ice`、`webrtc/turn` | 否 | 否 | TURN 中继吞吐和每个中继包的分配；STUN 消息解析 |
| 媒体 hub 分发，RTMP、RTSP、SRT、HTTP-FLV、MPEG-TS、fMP4 复用 | `media/...` | 否 | 否 | hub 向大量观众分发；各复用器和解复用器的每帧开销；SRT 包路径 |
| HLS 复用器 | `media/hls` | 部分（`BenchmarkMuxerEncryption`、`BenchmarkMuxerPlaylistWait`） | 否 | 不加密的切片路径，以及大量并发读播放列表 |
| BitTorrent 下载 | `bittorrent`、`bittorrent/peer` | 是（2026-11-09，[bittorrent](bittorrent.zh-CN.md) 性能一节） | 是（a5511a8、d12e3f3） | — |
| Bencode | `bittorrent/bencode` | 是（`BenchmarkMarshalStruct`、`BenchmarkUnmarshalStruct`） | 否 | 没有优化记录 |
| uTP | `bittorrent/utp` | 否 | 否 | 对比 libutp 的吞吐和每包分配 |
| SOCKS 中继 | `socks` | 否 | 否 | 中继吞吐和每连接分配 |
| 文件监视 | `filewatch` | 是（`BenchmarkWatchWatched`） | 是（047fd40） | — |

## 待办

根据上表，留给压测任务的：

- 完全没有基准：KCP、STUN/ICE/TURN 中继、媒体 hub 和 HLS 以外的复用器、uTP、SOCKS 中继。
- 只测了一部分：UDP 批量收发与 GSO、HTTP/2 客户端、HLS 复用器。
- 测过但未（完全）优化：TLS 握手、permessage-deflate、arpc、SCTP、SRTP、bencode。

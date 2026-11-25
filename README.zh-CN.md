# fib — Fast In Balance

[![CI](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Flesismal%2Ffib%2Fbadges%2Fci.json)](https://github.com/lesismal/fib/actions/workflows/ci.yml) [![Coverage](https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Flesismal%2Ffib%2Fbadges%2Fcoverage.json)](https://github.com/lesismal/fib/actions/workflows/ci.yml)

[English](README.md) | [简体中文](README.zh-CN.md)

fib 是一个事件驱动的 Go 网络库，支持 TCP、UDP、Unix socket、TLS、HTTP/1.x、HTTP/2、基于 QUIC 的
HTTP/3、WebSocket 和 WebRTC，以及基于 RTMP、RTSP、SRT、HTTP-FLV、HLS 和 WHIP/WHEP 的直播流媒体。
少量事件循环负责等待 I/O 就绪，具体工作交给共享的 worker 协程池，所以连接
不占用 goroutine，空闲连接几乎没有开销。HTTP handler 接收标准库的 `*http.Request`，通过
`http.ResponseWriter` 回写，因此 `net/http` 的 handler、`http.ServeMux`、`http.FileServer` 不用改动
就能跑在 fib 上。

也就是说：编程模型和 net/http 一样，性能和内存是事件循环的水平。在下文的 GitHub Actions 基准测试
里，和最常用的 Go 实现相比：

- HTTP/1：echo 吞吐是 net/http 的 2.3 倍，pipeline 吞吐是 6.9 倍，内存只有它的十分之一
- HTTP/2：echo 吞吐是 net/http 的 3.7 倍，多路复用吞吐是 13 倍
- HTTP/3：吞吐是 quic-go 的 3.4–5.4 倍
- TLS 1.3 pipeline：吞吐是 crypto/tls + `net` 的 2.9 倍，内存约为三分之一
- WebSocket：echo 吞吐在参测的 17 个服务端中最高，内存 32 MB

同一组测试里，fib 也与 C、C++、Rust 实现（uSockets、workflow、axum、h2、quiche、rustls）持平或接近。

- [特性](#特性)
- [与 Go 标准库的比较](#与-go-标准库的比较)
- [文档](#文档)

## 特性

### 协议

| 协议 | 服务端 | 客户端 | 说明 |
| --- | --- | --- | --- |
| TCP / Unix socket | ✓ | ✓（异步 dial） | `SendFile`（sendfile(2)）、`SpliceTo`（连接之间的 splice(2)，Linux）、`writev` 批量写、cork、半关闭 |
| UDP | ✓ | ✓ | Linux 上用 `recvmmsg`/`sendmmsg` 批量收发，每个对端一个连接 |
| TLS 1.0–1.3 | ✓ | ✓ | 握手用 crypto/tls；握手完成后由 fib 自己加解密记录（AES-GCM、AES-CBC） |
| HTTP/1.x | ✓ | ✓ | pipelining、chunked 与 trailer、流式 body、文件走 `sendfile` |
| HTTP/2 | ✓ h2 与 h2c | ✓ | h2c 支持 prior knowledge 和 `Upgrade: h2c`，支持 server push，CI 跑 h2spec |
| HTTP/3 + QUIC | ✓ | ✓ | QUIC 和 QPACK 基于 `crypto/tls.QUICConn` 自行实现，不依赖 quic-go 或 x/net |
| WebSocket | ✓ | ✓ | RFC 6455、permessage-deflate，可经 HTTP/1.1、HTTP/2（RFC 8441）、HTTP/3（RFC 9220）升级；CI 跑 Autobahn |
| arpc | ✓ | ✓ | 与 [lesismal/arpc](https://github.com/lesismal/arpc) 相同的协议格式和 API：双向调用、notify、异步调用、stream、广播、中间件、断线重连 |
| HTTP CONNECT 代理 | ✓ HTTP/1.x、HTTP/2、HTTP/3 | — | `Context.Connect` 建立隧道，双向背压；`ConnectProxy` 是现成的代理，可供 curl `-x`、浏览器与 `HTTPS_PROXY` 使用 |
| SOCKS5 / SOCKS4 / 4a | ✓ | ✓（异步拨号） | CONNECT、BIND 与 UDP ASSOCIATE，用户名/密码认证；HTTP、WebSocket 与 TLS 客户端可经它拨号 |
| KCP | ✓ | ✓ | skywind3000/kcp 的 ARQ 协议，与它及 xtaci/kcp-go（因而与 kcptun）线上兼容：支持 kcp-go 的分组密码、XOR 与 AES-GCM 加密及 Reed-Solomon FEC，但不依赖它；会话可当 `net.Conn` 用，也可按消息回调；有事要发时才 flush，而不是每个 interval 都 flush（[详见](docs/kcp.zh-CN.md)） |
| gRPC | ✓ h2 和 h2c | ✓ | unary 与双向 streaming、metadata、deadline、拦截器、gzip；可直接用 protoc-gen-go-grpc 生成的代码，与 grpc-go 互通 |
| WebRTC | ✓ | ✓ | 多个 PeerConnection 共用一个 UDP 端口：ICE（lite 与 full、trickle）、DTLS 1.2-SRTP、data channel、供 SFU 使用的 RTP 转发、支持 UDP、TCP 与 TLS 的 TURN 客户端与服务端（配额、事件与统计、热加载、drain），全部自行实现；可与浏览器和 pion 互通（[详见](docs/webrtc.zh-CN.md)） |
| 流媒体 | ✓ | ✓ | RTMP/RTMPS（Enhanced RTMP）、TCP 与 UDP 的 RTSP、SRT、HTTP-FLV 与 WebSocket-FLV、fMP4 或 TS 分片的 HLS 及 LL-HLS、WHIP 与 WHEP，由一个 hub 在它们之间转封装、不转码；每种协议都有拉流转发和转推；与 ffmpeg 和 Chromium 互通验证（[详见](docs/media.zh-CN.md)） |
| BitTorrent | ✓ | ✓ | bencode、.torrent 文件与磁力链接（v1、v2 与 hybrid）、HTTP 与 UDP tracker 的客户端和服务端、带 Fast 与扩展协议、ut_metadata、ut_pex 和 MSE 加密的 peer wire 协议、带 LEDBAT 的 uTP，以及支持 BEP 42/44/51 的主线 DHT；uTP、DHT 与 UDP tracker 客户端共用一个 UDP 端口；下载和做种 v1、v2、hybrid torrent 的客户端，可边下载边读文件，通过 tracker、DHT、PEX、本地发现和 web seed 找数据源；与 libtorrent、anacrolix/torrent、anacrolix/dht、libutp 和 opentracker 互通验证（[详见](docs/bittorrent.zh-CN.md)） |

### 引擎

- **原生后端**：Linux 用边沿触发的 epoll，macOS、FreeBSD、NetBSD、OpenBSD、DragonFly 用 kqueue，
  Windows 用 IOCP。其他系统回退到标准库 `net`。
- **按连接调度**：同一连接的事件按顺序、一次一个地在任意空闲 worker 上执行。连接不绑定线程，
  负载按实际工作量分摊，而不是按 fd 数量。
- **背压**：每连接写水位线按读缓冲和机器计算（四倍读缓冲，64–256 KiB；默认 16 KiB 读缓冲
  时为 64 KiB），满了就暂停读取；全服务待发字节预算（`MaxPendingBytes`）可选、默认关闭，
  用来限制总量；`HoldReads` 提供读端流控。
- **池**：随负载伸缩的自适应 worker 池；按 Go 分配器 size class 对齐的分级 buffer 池。
- **一个引擎，多种协议**：传给 `fib.New` 的每个 `Binding` 为一组地址（TCP、Unix 或 UDP）各配一个 handler，
  一个引擎即可同时提供 HTTP、HTTPS、HTTP/3、WebSocket、gRPC 和裸 TCP，共享事件循环、池和 buffer。
  `mux` 包更进一步：按客户端最先发送的字节识别协议，让多种协议共用同一个端口。
- **异步客户端**：非阻塞 dial（需要时可指定本地地址和端口范围），以及 HTTP/1.x、HTTP/2、HTTP/3、
  WebSocket 客户端，结果通过回调或 future 返回。
- **静态文件**：`http.FileCache` 从内存提供一个目录下的文件，支持预压缩的 `.br`/`.gz` 文件，并跟随
  磁盘变化：Linux 上用 inotify（`filewatch`），请求本身不 stat；其他平台在 `CheckInterval`（默认 5s，
  也可每个请求）过后 stat 文件一次，凭 inode 和 change time 识别大小、时间都相同的替换。
- **Router**：[chi](https://github.com/go-chi/chi) 风格的 API，支持参数、正则、通配、分组、
  子路由和挂载，路由过程零分配。
- **中间件**：compress、cors、csrf、etag、limiter、logger、pprof、recover、requestid、
  responsetime。
- **Prefork**：Linux 上 `prefork` 包把一个程序拆到每 CPU 一个进程上运行，各自有自己的堆和 GC，
  通过 `SO_REUSEPORT` 共享程序的端口。
- **平台**：Linux、macOS、BSD、Windows，支持 amd64、arm64、386、arm、riscv64、loong64，需要 Go 1.27.3
  或更新版本。

## 与 Go 标准库的比较

这里的"标准库方案"指 `net` + `net/http`（已内置 HTTP/2）+ `crypto/tls`。标准库没有 HTTP/3、WebSocket 和
gRPC，通常另外用 quic-go、gorilla/websocket、grpc-go。

### 相同点

- **HTTP 编程接口相同**：handler 拿到的是标准库的 `*http.Request`，`*fibhttp.Context` 实现了
  `http.ResponseWriter`、`http.Flusher`、`io.ReaderFrom`。所以 `http.ServeMux`、`http.FileServer`、
  `ServeContent`，以及 chi、gin 这类基于 `http.Handler` 的框架，基本不用改就能在 fib 上跑，HTTP/1、HTTP/2、
  HTTP/3 都可以。
- **TLS 握手都用 `crypto/tls`**：`tls.Config` 可以直接用，每秒握手数基本一样（TLS 1.3 下 3,445 对 3,413）。
- **运行时和依赖**：两者都是纯 Go、不用 cgo，跑在同一个 runtime 和 GC 上。fib 也不依赖标准库以外的模块，
  它的事件循环停在 Go 自己的 netpoller 里。
- **共同的接口和能力**：`fib.Connection` 实现了 `net.Conn`；两边都有 h1/h2/h2c、ALPN、带 context 的优雅关闭
  （`Shutdown(ctx)`）、`ResponseController.Flush` 和 `SetWriteDeadline`。

### 不同点

| 维度 | 标准库 | fib |
|---|---|---|
| 并发模型 | 每个连接一个 goroutine（HTTP/2 每个 stream 再起一个），阻塞式 I/O | 少量事件循环（epoll/kqueue/IOCP）加共享 worker 池，连接不占 goroutine；同一连接同一时刻只在一个 worker 上处理，按顺序执行 |
| handler 里阻塞 | 随便阻塞，代价只是一个 goroutine | 阻塞会占住稀缺的 worker。要用 `Retain/Release` 异步回复、`OnBody` 回调，或者交给独立的池 |
| `Read` / `Write` | `Read` 会阻塞等数据；`Write` 在对端读得慢时阻塞，背压是自然产生的 | `Read` 没数据时返回 `ErrWouldBlock`；`Write` 不阻塞，数据先排队，靠水位线暂停读取 |
| deadline | 到期让这次调用失败，连接还在 | 到期直接关闭连接 |
| 请求 body | 默认流式，读 `r.Body` 时才从网络取 | 默认整个收完才调用 handler（受 `MaxBodyBytes` 限制）；流式要打开 `StreamRequestBody` |
| 对象生命周期 | 每个请求一份新对象，handler 返回后仍可安全持有 | 默认复用 Request、Header、URL、Context，规则和 fasthttp 一样：handler 返回后再碰这些对象，会读到别的请求的数据，而且不会报错 |
| 客户端断开 | `r.Context()` 会被取消 | `r.Context()` 不会被取消，要用 `c.Context()`、`c.OnCancel` 或 `c.Err()` |
| Hijack | 支持 | 不支持，所以 gorilla/websocket 这类依赖 Hijack 的库用不了；`EnableFullDuplex` 返回 `ErrNotSupported` |
| TLS 加解密 | 全程由 `crypto/tls` 完成 | 握手后由 fib 自己做记录层的加解密（AES-GCM、ChaCha20、CBC），还支持 kTLS 网卡卸载 |
| 协议栈 | h1/h2，其余协议要靠第三方库 | 自己实现了 h2、HPACK、QUIC/HTTP/3、WebSocket、gRPC 传输层、KCP、WebRTC、流媒体等，一个引擎可以同时服务多种协议，还能按首字节识别协议共用一个端口 |
| 写出方式 | 每次 `Write` 经过 `bufio` | 一轮处理里的所有输出先 cork，轮末一次 `writev`；文件走 `sendfile`，连接之间转发可用 `splice` |
| 客户端 | `http.Client` 同步阻塞 | 异步 dial 和异步 h1/h2/h3/WS 客户端，结果通过回调或 future 返回（在 handler 里 `Wait()` 是反模式） |
| 平台 | 处处一致 | Linux、macOS、BSD、Windows 有原生后端，其他系统回退到 `net`；prefork、`recvmmsg`、inotify、原始 syscall 等只在 Linux 上有 |
| 调参 | 几乎不用调 | 有 worker 池、水位线、`SplitEventLoops`、`EventLoopCount`、`MaxPendingBytes` 等，默认值按机器自动算 |

### fib 的优势

1. **吞吐高**：以下是 GitHub Actions 上的基准（服务端 2 vCPU，1 万连接，1 KiB 负载），倍数都是相对 net/http
   （TLS 一行相对 `crypto/tls` + `net`）：

   | 场景 | 吞吐 |
   |---|---|
   | HTTP/1 echo | 2.3 倍 |
   | HTTP/1 pipeline | 6.9 倍 |
   | HTTP/2 echo | 3.7 倍 |
   | HTTP/2 多路复用 | 13 倍 |
   | TLS 1.3 pipeline | 2.9 倍 |

   HTTP/3 是 quic-go 的 3.4–5.4 倍。在这些测试里 fib 也接近 C、C++、Rust 的实现。数据和复现方法见
   [go-http1-benchmark](https://github.com/lesismal/go-http1-benchmark)、
   [go-http2-benchmark](https://github.com/lesismal/go-http2-benchmark)、
   [go-http3-benchmark](https://github.com/lesismal/go-http3-benchmark)、
   [go-tls-benchmark](https://github.com/lesismal/go-tls-benchmark) 和
   [go-websocket-benchmark](https://github.com/lesismal/go-websocket-benchmark)。
2. **内存少，连接越多差距越大**：空闲连接不持有 goroutine 栈，也不持有 I/O buffer，buffer 只在一轮处理里借用。
   HTTP/1 echo 时 fib 占 32 MB，net/http 占 314 MB。
3. **积压有上限**：读写都有水位线，pipeline 测试里 fib 稳在 32–36 MB，多数其他实现涨到几百 MB。
4. **协议全，而且零第三方依赖**：一个引擎可以同时提供 HTTP、HTTPS、HTTP/3、WebSocket、gRPC、SOCKS、arpc 和
   裸 TCP，共享事件循环、池和 buffer。
5. **迁移成本低**：已有的 `http.Handler` 大多能直接接上，先拿到一部分收益，再按需改成 `Context` 的写法。
6. **高级控制手段多**：`HoldReads`、`Cork`、`SendFile`、`FileCache`、prefork 多进程分堆，适合网关、代理、推送和
   长连接这类场景。

### fib 的缺点和风险

1. **心智负担重**：handler 不能随手阻塞；对象复用的生命周期规则一旦违反，会静默串数据而不是 panic；回调里拿到的
   `data` 只在回调内有效。这些坑全靠开发者自己守，[hack 指南的反模式清单](docs/hacks.zh-CN.md#20-反模式清单)
   专门列了出来。
2. **兼容不是 100%**：没有 Hijack；`r.Context()` 不随断开取消；body 默认整个缓存；`Read` 不阻塞，不能套
   `bufio.Reader`；deadline 到期是关连接。依赖这些语义的标准库生态代码要改。
3. **默认值里有几处要注意的地方**：
   - HTTP/1 的写不阻塞，慢客户端时整份响应排在内存里。`MaxWriteQueueBytes` 和 `MaxPendingBytes` 默认不限，
     大响应要改成流式写，或者自己设上限（见[压测](docs/loadtest.zh-CN.md)的慢客户端一节）。
   - 对象复用默认开启。
4. **自己实现的代码多**：TLS 记录层、QUIC、HTTP/2 等都是自研，安全审计和边界情况的负担比标准库大，标准库背后是
   Go 团队和整个社区。
5. **项目成熟度**：维护者少，社区小，API 还在变（构造函数就已经改为只剩 `fib.New`），要求 Go 1.27.3 以上，
   没有 Go 1 兼容承诺。
6. **调试更难**：标准库的 goroutine dump 能直接看出每个连接卡在哪一行；fib 是状态机加回调，排查卡住、泄漏要靠
   更多工具和经验。
7. **业务越重，收益越小**：基准测的是 echo 这类轻 handler。handler 本身很重（查库、远程调用）时，瓶颈不在
   网络层，fib 的优势会缩小；用法不对（在 worker 上阻塞）时，还可能比标准库更差。

### 标准库的优势

1. **简单、符合直觉**：同步阻塞的写法，想阻塞就阻塞，goroutine 很便宜，新人几乎没有坑。
2. **语义安全**：对象不复用，context 随断开取消，`Write` 自带背压，body 流式读取。
3. **稳定可信**：有 Go 1 兼容承诺，Go 团队维护，安全修复及时，在最大规模的生产环境里被验证过。
4. **生态最全**：所有中间件、框架、Hijack 类的库（gorilla/websocket 等）、`httputil.ReverseProxy` 都能直接用。
5. **好调试**：pprof 和 goroutine dump 能直接对应到连接。

### 标准库的缺点

1. **每连接一个 goroutine，加上 `bufio` 读写 buffer，连接多时内存高**：1 万连接 300 MB 级，十万、百万长连接时
   压力更大。
2. **吞吐偏低**：没有跨请求的批量写，pipeline 和 h2 多路复用尤其吃亏（差 7–13 倍）；每个 HTTP/2 stream 都起
   一个 goroutine。
3. **积压没有全局上限**：慢客户端、大 pipeline 时，内存跟着 goroutine 和 buffer 一起涨。
4. **没有 HTTP/3、WebSocket、gRPC**，要拼第三方库，各自的模型和内存特性也不一样（quic-go 的吞吐和内存都明显
   不如 fib）。
5. **底层控制手段少**：没有读端流控、cork、多协议共用端口这类能力。

### 怎么选

- **普通业务服务**（handler 主要是查库、调服务，连接数不多，团队人员流动大）：标准库就够了，稳定、简单、生态全。
- **网络层本身就是瓶颈的服务**（网关、代理、长连接推送、IM、实时音视频、高连接数或 pipeline 多、对内存敏感，
  或者需要一个进程同时提供多种协议）：fib 的收益明显。前提是团队守住这几条规则：
  - handler 不阻塞，重活用 `Retain` 或交给独立的池；
  - 不把复用的对象带出 handler；
  - 大响应流式写，或设好写队列上限。
- **折中做法**：先把现有的 `http.Handler` 原样接到 fib 上，拿到连接和内存方面的收益；如果有 handler 在返回后
  还持有 Request，就关掉 `ReuseRequests`，或者先 `Clone`。热点路径再逐步改成 `Context` 的原生写法。

## 文档

- [教程](docs/tutorial/README.zh-CN.md)：按用法组织的 22 章教程，逐协议讲解，代码示例参与编译；
  Engine 配置、handler 同步/异步写法和 hack 技巧各自成章
- [配置](docs/configuration.zh-CN.md)：全部 `Config`、`Binding` 字段、默认值、机器 sizing 公式和启动
  配置日志
- [架构](docs/architecture.zh-CN.html)：事件循环、poller 与池、handler 分层、读调度、写背压、负载
  均衡、连接关闭
- [协议流程](docs/flows.zh-CN.html)：TLS、HTTP/1、HTTP/2、HTTP/3、WebSocket 各由哪个循环、哪个池执行
- [奇技淫巧](docs/hacks.zh-CN.md)：Retain/Release 异步响应、OnBody 大 body、pipeline/multiplex、
  HoldReads、Cork、对象复用等非标准库式用法与坑
- [HTTP/1.x](docs/http1.zh-CN.md)、[HTTP/2](docs/http2.zh-CN.md)、[HTTP/3](docs/http3.zh-CN.md)：
  支持范围、限制和一致性测试
- [WebRTC](docs/webrtc.zh-CN.md)：分层、共用的 Mux 与回调模型、服务端与客户端、data channel、媒体转发、
  TURN、各项限制，以及与 pion、OpenSSL、Chromium 的互通验证
- [流媒体](docs/media.zh-CN.md)：一个 hub 上的 RTMP、RTSP、SRT、HTTP-FLV、HLS 与 WHIP/WHEP，各协议承载的编码，
  跑全部协议的服务端、转发、各项限制，以及与 ffmpeg、Chromium 的互通验证
- [KCP](docs/kcp.zh-CN.md)：分层、按需 flush、与 kcp-go 和 kcptun 的对应、服务端的会话规则，以及与 kcp-go 的互通
- [BitTorrent](docs/bittorrent.zh-CN.md)：各个包、TCP 与 UDP 共用一个端口、各协议、客户端、BEP 覆盖情况，以及与
  libtorrent、anacrolix/torrent、anacrolix/dht、libutp 和 opentracker 的互通验证
- [路线图](docs/roadmap.zh-CN.md)：计划中但未做、以及有意不做的项，各带原因、成本量级和重新评估的条件
- [开发计划](docs/dev-plan.zh-CN.md)：每个热路径功能是否已做基准测试和优化，以及还要测什么
- [压测](docs/loadtest.zh-CN.md)：`benchmark/loadtest` 压测、结果，以及整体响应发给慢客户端时的内存代价

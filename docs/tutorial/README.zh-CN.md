# fib 教程

[English](README.md) | [简体中文](README.zh-CN.md)

本教程按用法讲 fib。参考文档描述每个配置字段和每种协议的限制；本教程展示这些东西怎么组合起来：
每种协议的服务端、它们共用的 handler 模型、客户端，以及让 fib 变快的那些技巧。

读者对象是了解 `net/http` 基础、想用 fib 提供 HTTP/1.x、HTTP/2、HTTP/3、WebSocket、gRPC、
arpc、SOCKS、KCP、WebRTC、流媒体或 BitTorrent 的 Go 程序员——这些协议可以各占一个 Engine，也
可以同处一个 Engine。不要求事先了解 fib。

## 开始之前

- **Go 1.27.3 或更新。** fib 只 import 标准库；没有 cgo，除了模块本身也不需要安装任何东西：

  ```sh
  mkdir demo && cd demo
  go mod init example.com/demo
  go get github.com/lesismal/fib
  ```

- **一个终端和 curl。** 各协议章节会给出验证示例的一行 `curl`（或 `ffmpeg`、浏览器）命令。
- **先读第 01 章。** 它安装 fib、跑起第一个服务，并展示教程里每个程序共有的骨架。第 02、03 章是
  后面所有内容的基础；之后各协议章节可以按任意顺序读。

## 约定

- **完整程序**是以 `package main` 开头的代码块。每一个都会被 `go test ./docs/tutorial` 抽取出来，
  放在一个依赖当前 checkout 的临时模块里构建；示例过期就编译失败、仓库测试失败。
- 其他代码块是**片段**：正在讲的函数或调用，省略了外层程序的 import 和样板代码。
- 示例监听 `127.0.0.1`，端口用 `8080`/`8443` 之类。TLS 示例用 `fib/tlstest` 给 `localhost` 签发
  一次性证书；正式证书见 [10-tls.zh-CN.md](10-tls.zh-CN.md)。
- 每章以 **坑与限制** 和 **相关文档** 收尾。那里的参考文档才是完整字段表，章节有意不重复。

## 结构

第一部分是公共基础，第二到第四部分是各协议与模块，第五部分是客户端、底层技巧与运维。

| 章 | 文件 | 协议 / 模块 | 内容 |
| --- | --- | --- | --- |
| 01 | [快速开始](01-getting-started.zh-CN.md) | — | 安装 fib；第一个 HTTP/1.x + h2c 服务；TLS + HTTP/2；HTTP/3；在 fib 上跑 `net/http` mux；fib 程序的骨架；后面的读法。 |
| 02 | [引擎与配置](02-engine.zh-CN.md) | Engine、`Config`、`Binding` | 按实践讲各组配置：协程池、buffer、水位线、poller、限制；一个 Engine 承载多个 Binding 与协议；运行时增删 Binding；优雅停机；一个进程多个 Engine；启动配置日志。 |
| 03 | [Handler：同步与异步](03-handlers.zh-CN.md) | `fib.Handler`、`fibhttp.Handler` | 两层 handler 与执行模型：handler 跑在 worker 上，一条连接一轮只跑一个；同步写法；`Retain`/`Release`、`OnBody`、`OnCancel`、handler 池、`MaxConcurrentHandlers`，以及"哪些调用能在哪个 goroutine 里做"的速查表。 |
| 04 | [路由](04-router.zh-CN.md) | `fibhttp.Router` | 参数、正则参数、catch-all、分组、子路由与 mount；405 与 `HEAD`；零分配匹配；改用 `net/http` 的 `ServeMux`。 |
| 05 | [中间件](05-middleware.zh-CN.md) | `middleware` | `Chain`、顺序、自己写中间件，以及内置的 compress、cors、csrf、etag、limiter、logger、pprof、recover、requestid、responsetime。 |
| 06 | [HTTP/1.x](06-http1.zh-CN.md) | HTTP/1.x | 请求与应答 helper、流式响应与 SSE、trailer、静态文件与 `FileCache`、sendfile、body（`ReadBody`、`OnBody`、`SaveBody`、可续传上传）、全双工、超时、pipelining、`net/http` 兼容性以及哪些行为不同。 |
| 07 | [HTTP/2](07-http2.zh-CN.md) | HTTP/2 | h2c 与 TLS 上的 h2；同一个 handler 服务 HTTP/1 与 HTTP/2；多路复用与 handler 池；server push；1xx；流式 body；限制。 |
| 08 | [HTTP/3](08-http3.zh-CN.md) | HTTP/3、QUIC | UDP Binding、`http3.ConfigureTLS`、同一个 handler 服务 h1/h2/h3、Alt-Svc、early data、连接迁移及其限制、客户端。 |
| 09 | [WebSocket](09-websocket.zh-CN.md) | WebSocket | handler 与各回调；在 HTTP/1.1、HTTP/2、HTTP/3 上升级；`Connection` 与 prepared message 广播；按帧接收；permessage-deflate；客户端。 |
| 10 | [TLS](10-tls.zh-CN.md) | TLS、DTLS | 用 `fibtls.NewServer`/`NewClient` 包住任意 handler；ALPN；客户端证书；cipher 与 session 配置；测试证书；WebRTC 用的 DTLS。 |
| 11 | [gRPC](11-grpc.zh-CN.md) | gRPC | 把 Server 当 fib handler；生成代码与 codec；一元与双向流；metadata/deadline/拦截器；客户端；与 HTTP 共用端口。 |
| 12 | [arpc](12-arpc.zh-CN.md) | arpc | 服务端与客户端、call、notify、异步 call、stream、广播、中间件、重连，以及与 `lesismal/arpc` 的差异。 |
| 13 | [SOCKS 与 HTTP CONNECT](13-socks-connect.zh-CN.md) | SOCKS5/4、HTTP CONNECT | SOCKS 服务端与选项；非阻塞 dialer 与代理链；HTTP CONNECT 代理；`Context.Connect` 与 `Tunnel`；两种代理同端口。 |
| 14 | [KCP](14-kcp.zh-CN.md) | KCP | `kcp.Listen` 作为 `net.Listener`、`kcp.Dial`、fast3 调参、与 kcp-go/kcptun 兼容的 FEC 与加密、消息式 session、sans-I/O 协议层。 |
| 15 | [单端口多协议](15-multiprotocol.zh-CN.md) | `mux`、`multiproto` | 一个端口上的多种协议：首字节如何选协议、`mux` 路由、`multiproto`、SNI/TLS 路由、fallback，以及什么时候该用多个 Binding。 |
| 16 | [WebRTC](16-webrtc.zh-CN.md) | WebRTC | 共享 UDP 端口上的 `PeerConnection`、offer/answer 信令、data channel、SFU 的媒体转发、TURN 服务端与客户端。 |
| 17 | [流媒体](17-media.zh-CN.md) | RTMP、RTSP、SRT、HLS、WHIP/WHEP | media hub、每种协议的服务端、拉流与推流 relay、编解码、VOD，以及和 ffmpeg、浏览器的互操作。 |
| 18 | [BitTorrent](18-bittorrent.zh-CN.md) | BitTorrent | 客户端、`.torrent` 与 magnet、边下边播、做种、续传，以及单独使用的 tracker/peer/uTP/DHT 组件。 |
| 19 | [原生 TCP、UDP 与 Unix socket](19-raw-io.zh-CN.md) | `fib.Connection`、`fib.Handler` | 原生连接 API、corked 写与 `SendFile`/`SpliceTo`、`HoldReads`、UDP 数据报与 peer、Unix socket、异步 dial。 |
| 20 | [客户端](20-clients.zh-CN.md) | HTTP、WebSocket、Dial | 异步 dial；HTTP/1.x、HTTP/2、HTTP/3 客户端；Alt-Svc 发现；WebSocket 客户端；TLS 与 SOCKS dial；一个 Engine 同时做服务端和客户端。 |
| 21 | [进阶技巧](21-hacks.zh-CN.md) | — | 把 [Hack 指南](../hacks.zh-CN.md) 改写成教程：异步响应、零拷贝读、写侧、`HoldReads`、`OnFrame`、对象复用、协程池、进程级技巧，以及反模式。 |
| 22 | [性能与运维](22-performance.zh-CN.md) | taskpool、bufferpool、prefork、log | worker 池的模式与容量、buffer 尺寸、`IOPollers`、`prefork`、日志、pprof、压测，以及上线检查清单。 |

## 怎么读一章

每章的骨架相同，第一遍读完之后可以当参考用：

1. 一段话说明本章讲什么；
2. 用法章节——每节一件事，都带代码；
3. **坑与限制**：哪些不能用、哪些会静默退化、哪些错误最费时间；
4. **相关文档**：本模块的参考文档、相邻章节、README 的相关小节。

协议类章节通常从服务端开始，然后是变体（TLS、流式、客户端），最后讲限制。如果只需要一种协议，
读第 01 章、讲 handler 模型的第 03 章，再读那个协议的章节即可。

## 验证教程

教程与仓库其余部分一起测试：

```sh
go test ./docs/tutorial
```

- `TestLinks` 检查这些页面里的每一个相对链接与标题锚点。
- `TestCodeExamples` 把每个完整程序放进一个依赖当前 checkout 的临时模块里构建。

两者都在 CI 里跑，所以过期的示例或改名的标题是构建失败，而不是过期页面。

## 相关文档

- [README](../../README.zh-CN.md) — fib 是什么、特性、架构与基准。
- [配置](../configuration.zh-CN.md) — 全部 `Config`、`Binding` 字段、默认值、机器 sizing 公式和
  启动配置日志。
- [架构](../architecture.zh-CN.html) 与 [协议流程](../flows.zh-CN.html) — 事件循环、worker 池，
  以及每种协议在哪里跑。
- [Hack 指南](../hacks.zh-CN.md) — 第 [21-hacks.zh-CN.md](21-hacks.zh-CN.md) 章的长文版本。
- [HTTP/1.x](../http1.zh-CN.md)、[HTTP/2](../http2.zh-CN.md)、[HTTP/3](../http3.zh-CN.md) —
  每种协议支持什么、一致性测试与限制。
- [WebRTC](../webrtc.zh-CN.md)、[流媒体](../media.zh-CN.md)、[KCP](../kcp.zh-CN.md)、
  [BitTorrent](../bittorrent.zh-CN.md) — 这些模块的深入参考。
- [压测](../loadtest.zh-CN.md) 与 [路线图](../roadmap.zh-CN.md) — 基准测试活动，以及计划中
  和有意不做的内容。
- [fib-examples](https://github.com/lesismal/fib-examples) — 各章所提炼的可运行示例集，每种协议
  一个目录。

## 新增一章

章节是本章录下的编号文件，一篇英文、一篇中文：`NN-slug.md` 和 `NN-slug.zh-CN.md`。两篇开头有
同一行语言切换链接，代码块一致，参考链接各用本语言版本。新增章节还要在两个 README 的表格里各加
一行。提交前运行 `go test ./docs/tutorial`；不能构建的完整程序会让测试失败。

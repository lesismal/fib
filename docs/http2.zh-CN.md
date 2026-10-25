# HTTP/2：限制、有意未实现的功能与待优化项

[English](http2.md) | [简体中文](http2.zh-CN.md)

本文记录 Go 版 `http` package 中 HTTP/2 实现的边界：当前行为上的限制、出于设计考虑
有意没有实现的功能，以及后续可以优化的方向。功能用法见
[Go 使用指南的 HTTP/2 章节](guide.zh-CN.md#http2)。

实现位于 [`http`](../http)（`h2_*.go`）以及内部的 hpack package，全部自行实现，
不依赖 `golang.org/x/net`。

## 已支持的范围（概览）

| 方向 | 内容 |
| --- | --- |
| 服务端 | TLS + ALPN（h2）、明文 prior knowledge（h2c）、HTTP/1.1 `Upgrade: h2c`；多路复用、双向流控、HPACK（含 Huffman、动态表）、CONTINUATION、trailer、server push、1xx 中间响应、自动 100 Continue、`Response.Close` 优雅 GOAWAY、`Request.TLS`、Extended CONNECT（RFC 8441）经 `Context.Upgrade` 切换为隧道，`websocket.ServerHandler.Upgrade` 即借此提供 WebSocket over HTTP/2；响应调度支持 RFC 9218 紧急度权重 |
| 客户端 | https 经 ALPN 协商 h2、`UnencryptedHTTP2` 明文 prior knowledge；单连接多路复用、遵守服务端 `MAX_CONCURRENT_STREAMS`、取消只重置单个 stream、GOAWAY / REFUSED_STREAM 自动重发；新 stream 交给负载最低的连接；`Expect: 100-continue` 等待 100；可选的 PING 健康检查与 SETTINGS 确认超时 |
| 一致性 | h2spec：145 项全部通过，h2c 与 TLS 两种方式 |
| 互通验证 | Go `net/http` 客户端与服务端（TLS 与 h2c）、curl（nghttp2）的 h2 / h2c / Upgrade |

以上内容都有测试覆盖并在 CI 中运行，见下面的[一致性测试](#一致性测试)。

### TLS 0-RTT

恢复 TLS 1.3 会话的客户端可以把头几个请求帧随第一个飞行一起发出，握手结束前就到（RFC 8446 第 2.3 节）。
`Config.EarlyData` 打开 HTTP/2 服务端的这个能力（也通过 `tls.EarlyDataOptIn` 打开包装它的 `tls.Handler`）：
HEADERS 这样到达的 stream 带 `Early-Data: 1`（RFC 8470），只有 GET、HEAD、OPTIONS、TRACE 方法才从它服务。
其他方法只在该 stream 上回 425 Too Early，连接和其他 stream 照常，和 HTTP/3 一致；`Config.EarlyDataUnsafe`
让所有方法都交给 handler，由它自己检查这个头。TLS 层一张票据只接受一次 early data，总量以
`tls.Handler.MaxEarlyData` 为限，重放到不了这一层。它需要不占协程的握手：`tls.Handler.Blocking` 下由
`crypto/tls` 做，不接受 early data。客户端不发送 HTTP/2 early data：帧格式取决于是 ALPN 选出的协议，而它要到握手结束才知道。

## 当前限制

使用时需要了解的行为约束。

### 请求 body 可以流式交付，响应边写边发

- 请求 body 默认在 handler 运行前完整读入内存。设置 `Config.StreamRequestBody` 后，body
  一开始到达就调用 handler——带 `content-length` 且超过 `StreamRequestBodyThreshold` 的在
  HEADERS 到达时，不带 `content-length` 的在收到超过阈值的数据时——handler 从
  `Request.Body`（不阻塞的 `*BodyStream`）读，或用 `Context.OnBody` 接收，和 HTTP/1 完全
  一样（见 [`http1.zh-CN.md`](http1.zh-CN.md#流式请求-body)）。这时 stream 的接收窗口只在
  handler 消费了 body 之后才补充，上传快于 handler 处理的客户端由 HTTP/2 流控限速，最多
  领先一个窗口（`HTTP2Config.StreamWindow`，默认 1MB），同一连接上的其它 stream 不受影响。流式 body 的上限是
  `MaxStreamedBodyBytes` 而不是 `MaxBodyBytes`；`Expect: 100-continue` 要等 handler
  第一次要 body 时才回复 100。
- 交给 `WriteResponse` 或 `Respond` 的响应通过 `Response.Body []byte` 一次性给出；客户端
  同样把响应 body 完整缓存后再回调。
- handler 也可以通过 `Context` 的 `http.ResponseWriter` 方法（`Header`/`WriteHeader`/
  `Write`/`Flush`）写响应（含 trailer），或把 `Context` 交给 `http.ServeFile`、
  `http.ServeContent`。不超过 4KB（或设置了不超过 64KB 的 `Content-Length`）且没有
  `Flush` 的响应仍在 handler 返回后整体发送，带 `content-length`；更长的或 `Flush` 过的
  响应和 HTTP/1 一样流式输出：先发一个不结束 stream 的 HEADERS，body 边写边按客户端的
  窗口以 DATA 帧发出，handler 结束后发 END_STREAM 或携带 trailer 的 HEADERS。因此 SSE、
  长轮询、大文件下载在 HTTP/2 上都可以实现。
- handler 写得比客户端读得快时，一旦有 64KB body 被流控压住，`Write` 就会等待，直到客户端
  取走一部分或 stream 结束，内存因此有上界。等待发生在 handler 协程池上，池会为阻塞的
  handler 扩容；连接在自己的读协程上运行的 handler（见下文 `StreamPool`）从不等待，因为
  结束等待的 WINDOW_UPDATE 正要由这个协程读取，它写的 body 会一直保留到客户端窗口放行。
  body 比 handler 设置的 `Content-Length` 短时，stream 会被重置而不是正常结束。
- [`grpc`](../grpc) 包通过这一机制（`Server.ServeHTTP`）提供 gRPC（含 streaming），它也有
  自己的 HTTP/2 传输层。
- 内存上限：服务端单连接最坏约为 `MaxConcurrentStreams × MaxBodyBytes`
  （默认 250 × 16MB），外加每个流式 body 一个 stream 窗口；客户端单个响应受
  `MaxResponseBodyBytes` 限制。整体响应保留 handler 的 body，只把窗口允许的帧排队，
  因此不读的客户端大约只占一个 stream 窗口，而不是 body 本身；见[压测](loadtest.zh-CN.md)。
- `CONNECT` 请求能被解析并交给 handler，但无法建立隧道（没有双向流式通道）。

### handler 在协程池上执行

- 一个连接的帧按顺序处理，请求完整后按 `Config.StreamPool` 的描述交给 engine 的 handler
  协程池（`fib.Engine.HandlerPool`），因此同一连接上客户端并发发起的多个请求是并发处理的，
  阻塞的 handler 只拖累它自己。
- 不管开不开 `IOPollers`，连接都由 engine 的 worker 读和分帧，每个 engine 名字（`Config.Name`）有一个
  单独的 handler 协程池，名为 `<Name>-streams`
  （默认 `fib-streams`），由连接来自该名字 engine 的所有 HTTP/2 与 HTTP/3 server 共用，且
  永远不与 engine 的协程池共用（共用会在队列满时让 engine 的 worker 全部卡在提交上而死锁）。
  它的上限是当前运行的同名 engine 中最大协程池的 2 倍，没有 engine 时取
  `fib.DefaultStreamPoolSizing`；下限为 0，空闲时不保留 worker。该名字的最后一个
  engine 关闭后它随之停止，不等待仍在运行的 handler。
- `StreamPool.MaxConcurrentHandlers` 限制单个连接同时处理的请求数 N：第 N 个请求在读取
  该连接的协程上执行，在它返回前该连接不再读取新数据，因此这个上限由对端的流控承担，
  而不是在服务端排队。N 为 1 时（与 `StreamPool.Disable` 相同）每个请求都在读取协程上
  逐个执行，也就是引入协程池之前的行为（应用层的队头阻塞）。
- `Push` 仍然在调用它的协程上同步执行被推送请求的 handler，在它返回后才返回；被推送的
  handler 慢，父请求的响应也会随之推迟。
- 响应先排队，由其中第一个发往 handler 协程池的 flush 统一写出，排在已经交给协程池的请求
  之后，因此一批响应一次写出而不是各写一次，也没有响应要等慢的 handler。TLS 上这一批也
  合成一个 record（最多 16 KB）加密，而不是每个 frame 一个 record：连接 cork 期间发送的
  明文由 TLS layer 留着，flush 解除 cork 时一起加密（`fib.CorkHolder`）。7 个 CPU 上 h2 的
  fixed 响应因此从每秒 164 万提高到 352 万请求，h2c 是 382 万。
- stream 连同它持有的 `*http.Request`、`Header`、`URL`、`*Context`，在响应完成、handler
  返回并释放所有 `Retain` 之后被复用，受 `Config.ReuseRequests` 等选项控制（默认开启），规则与
  HTTP/1 请求对象相同：handler 不能在那之后继续持有它们（见 HTTP/1 文档）。body 流式交付的
  stream 和 extended CONNECT 隧道不复用：它们的 body 回调可能在响应结束之后才发生，复用的
  stream 无法承载。push 的 stream 也复用，连同它的 `Request`、`URL` 和 `Context`。64 核上
  这两项把 HttpArena 的 baseline-h2c 从每秒 390 万请求提到 1850 万。

### 不能向连接直接写字节

- HTTP/2 连接上不要调用 `Context.Conn.Send` 等方法直接写数据，否则会破坏帧结构；
  所有输出都必须经过 `Context`。

### 可配置参数

`Config.HTTP2` 设置服务端连接允许客户端发送什么，`ClientConfig.HTTP2` 对客户端连接做同样
的事。两者都是 `http.HTTP2Config`，零值即下表中的默认值。

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| `StreamWindow` | 1MB | `SETTINGS_INITIAL_WINDOW_SIZE`：每个 stream 的接收窗口，消费一半后补充。流式 body 受它限制；调大能让单个 stream 每个往返多传一些。取值限制在 65535 到 2^31-1。 |
| `ConnWindow` | 16MB | 整个连接的接收窗口，由所有 stream 共享，消费一半后补充。取值限制在 65535 到 2^31-1。 |
| `MaxFrameSize` | 16384 | 声明给对端的 `SETTINGS_MAX_FRAME_SIZE`：对端可发送的最大帧负载。调大后同样的 body 用更少的帧，但每次读取可能占用更多内存。取值限制在 16384 到 2^24-1。本端发送时仍按对端允许的大小分帧。 |
| `HeaderTableSize` | 4096 | 双向使用的 HPACK 动态表大小：本端在 `SETTINGS_HEADER_TABLE_SIZE` 中声明的值，也是对端允许时本端编码器使用的上限。设为负数关闭动态表。编码器上限为 `hpack.MaxEncoderTableSize`（1MB）。 |
| `AdaptiveWindow` | 关闭 | 像 gRPC 那样把窗口涨到连接的带宽时延积：统计一个 PING 到其确认之间到达的 DATA，若对端在一个往返里填满了窗口，就把窗口翻倍。PING 至少间隔 25ms，窗口只增不减，stream 窗口受 `MaxAdaptiveWindow` 限制。 |
| `MaxAdaptiveWindow` | 16MB | `AdaptiveWindow` 可涨到的 stream 窗口上限。 |
| `InitialMaxStreams` | 100 | 客户端在收到服务端 `SETTINGS_MAX_CONCURRENT_STREAMS` 之前，在新连接上假定可并发的 stream 数。服务端忽略。 |

```go
config := fibhttp.DefaultConfig()
config.HTTP2 = fibhttp.HTTP2Config{
    StreamWindow:    4 << 20,  // 高延迟长链路上一次往返传 4MB
    ConnWindow:      32 << 20,
    MaxFrameSize:    32 << 10, // body 帧更少，每次读取内存更多
    HeaderTableSize: 16 << 10, // 每个连接保留更多重复出现的头
    AdaptiveWindow:  true,     // 或者让连接自己测量路径
}
server, err := fib.New(fibConfig, fib.Binding{Addr: ":443", Handler: fibhttp.NewHandlerWithConfig(config, handler)})
```

客户端在 `fibhttp.ClientConfig.HTTP2` 中设置同样的字段，含义是它允许服务端向它发送什么。
窗口是流式 body 受制的流控，帧大小是对端一次能发送多少，动态表是两侧 HPACK 编码器在
header block 之间能保留多少。

### 协议细节

- **优先级**：PRIORITY 帧和 HEADERS 中的优先级字段会被校验后忽略，RFC 7540 的优先级树
  也没有实现（RFC 9113 已废弃它）。改为支持请求的 RFC 9218 `priority` 头：它的紧急度
  （`u`，0 到 7，默认 3）决定该 stream 轮到连接窗口时一次可以发多少帧——紧急度 0 为 8 帧，
  1 为 4 帧，2 为 2 帧，默认紧急度 3 及更不紧急的都是每轮 1 帧，也就是每个 stream 原本就
  有的轮询，因此不发 priority、或只写了默认紧急度的客户端行为完全不变。增量提示（`i`）
  被忽略，因为响应 body 是整体缓存后才发送的；PRIORITY_UPDATE 也没有实现。轮转顺序仍是
  各 stream 开始等待的先后，也不会饿死任何 stream：每个等待的 stream 都能轮到，被连接窗口
  在一轮中途截断的会保留位置和这一轮剩余的帧数，等窗口补充后继续；被自己的窗口挡住的则
  让出这一轮。前面没有其它 stream 等待时仍然立即发送。
- **对端的 `SETTINGS_MAX_HEADER_LIST_SIZE`**：双向都会遵守对端声明的上限。响应头或
  trailer 超限时重置该 stream 而不是照发，中间响应或 push 请求的头部超限则不发；客户端
  对头部超限的请求在开 stream 之前就直接失败，流式 body 的 trailer 超限时重置该 stream。
- **引擎停止**：`Engine.Shutdown` 会发送 GOAWAY，并在连接的 stream 都完成后关闭连接。
  `Engine.Stop` 本身不关闭连接；`Engine.Close` 在切断每个 HTTP/2 连接之前，用一次非阻塞
  写发出 GOAWAY：socket 还收得下时对端就能知道服务端要走了，但已经有大量输出排队的
  连接仍可能在 GOAWAY 还排在队列里时被切断。优雅关闭仍然要用 `Engine.Shutdown`。
- **关闭时的 TCP RST**：服务端自己结束的连接（空闲、响应的 `Close`、停机、对端 GOAWAY
  或连接错误）在 GOAWAY 之后会半关闭写方向并排空客户端仍在发送的数据，最多
  `lingerTimeout`（500ms），客户端因此能读到 GOAWAY 而不是 RST。`Engine.Close` 切断的
  连接不会 linger，其中仍有未读输入的可能被 RST。
- **空闲与保活**：服务端在连接 `Config.IdleTimeout`（未设时用 `ReadTimeout`；`DefaultConfig`
  中为 2 分钟）内没有任何流时发出 GOAWAY `NO_ERROR` 并关闭。设置 `Config.PingInterval`
  后，连接超过一个间隔没有发送任何数据时服务端会发 PING，`Config.PingTimeout` 内没有
  任何回应则关闭连接；默认不发 PING。客户端的 PING 健康检查见[客户端](#客户端)。
- **SETTINGS 确认超时**：客户端未在 `Config.SettingsTimeout`（默认 10 秒）内确认服务端的
  SETTINGS 时，服务端以 GOAWAY(SETTINGS_TIMEOUT) 结束连接。客户端也以
  `ClientConfig.SettingsTimeout` 给服务端计时：到期仍未确认客户端 SETTINGS 时，客户端
  关闭该连接，其上的请求以 `ErrSettingsTimeout` 失败，或按下面的重发规则在另一条连接上
  重发。

### 客户端

- https 的 host 在第一次连接确定协议之前只会同时拨号一条连接（与 `net/http` 相同），
  对只支持 HTTP/1.1 的 https 服务，首批并发请求要多等一次 TLS 握手。
- 同一 host 有多条 HTTP/2 连接时，新 stream 交给剩余 stream 额度最多的那条（按各连接
  自己的 `MAX_CONCURRENT_STREAMS` 计算），负载分摊到各条连接上，而不是先压满第一条；额度
  相同时选靠前的。只有所有连接都满了才会拨第二条；有了多条之后，只要请求不断它们都会被使用，
  host 安静 `IdleConnTimeout` 后关闭。
- **健康检查**：设置 `ClientConfig.ReadIdleTimeout` 后，连接在这么久内没有收到服务端的任何
  数据就发一个 PING；之后 `PingTimeout`（默认 15 秒）内什么都没收到（ACK 或其他帧都算）
  就认为连接已死并关闭。上面的请求以 `ErrPingTimeout` 失败，或按下面的规则在另一条连接上
  重发。参数名和默认值与 x/net/http2 相同；`ReadIdleTimeout` 为零（默认）时不检查。
- **SETTINGS 超时**：`ClientConfig.SettingsTimeout`（默认 10 秒；小于零不限制）到期而
  服务端仍未确认客户端 SETTINGS 时，连接被当作卡死并关闭，其上的请求以
  `ErrSettingsTimeout` 失败，或按下面的重发规则在另一条连接上重发。这能发现已经不再处理
  读入数据的服务端，而 PING 健康检查发现不了：后者只看安静下来的连接，这种服务端的
  socket 可能仍在接收写入。
- **重发**：已收到部分响应后连接断开的请求以连接的错误失败，不论方法都不重发。这是有意的，
  与 `net/http`、x/net/http2 一致：服务端已经开始回应，说明已经处理了请求，重发 POST 会
  执行两次；即使方法幂等，流式响应的回调可能已经把部分 body 交出去了，第二个响应无法接上，
  而且第二次的响应也可能不同。没有收到任何响应数据的请求，方法幂等（GET、HEAD、OPTIONS、
  TRACE）且 body 能重新读取（流式 body 要有 `GetBody`）时，在另一条连接上重发一次；被 GOAWAY
  或 REFUSED_STREAM 标为未处理的请求不论方法都会重发，因为服务端声明了没有处理它。调用方
  确认请求在部分响应后重发也安全时，可以自己再发一次。
- **`Expect: 100-continue`**：带 body 和这个头的请求先单独发送 HEADERS，等服务端回 100、
  或 `ClientConfig.ExpectContinueTimeout`（`DefaultClientConfig` 中为 1 秒；为零时立即发送
  body）到期后再发送 DATA。如果先收到最终响应，它就是答复：body 不会发送，响应完整后
  重置 stream。HTTP/1 和 HTTP/3 的请求也是如此。
- 明文 HTTP/2 只支持 prior knowledge，不支持从 HTTP/1.1 `Upgrade: h2c` 升级。

## 有意没有实现的功能

| 功能 | 原因 |
| --- | --- |
| 客户端接收 server push | Chrome、Firefox 已移除 push，Go 的 `net/http` 客户端也从未支持。客户端在 SETTINGS 中声明 `ENABLE_PUSH=0`，服务端 push 能力保留给仍然需要它的客户端。预加载推荐用 103 Early Hints（`Context.WriteInterim`）。 |
| RFC 9218 的增量提示与 PRIORITY_UPDATE | `priority` 头的紧急度已用于发送调度（见[协议细节](#协议细节)）；响应 body 整体缓存时 `i` 没有可重排的内容，PRIORITY_UPDATE 帧不实现。RFC 7540 的优先级树已被 RFC 9113 废弃，同样不实现。 |
| 客户端发起 `Upgrade: h2c` | RFC 9113 已废弃这种升级方式；需要明文 HTTP/2 的场景使用 prior knowledge（`UnencryptedHTTP2`）。服务端仍然接受升级，以兼容 curl 等客户端。 |
| TLS 连接上的 `Upgrade: h2c` | RFC 规定 TLS 上只能通过 ALPN 切换协议。 |
| HTTP/1.0 请求的 1xx 中间响应 | HTTP/1.0 客户端不认识 1xx，`WriteInterim` 返回 `http.ErrNotSupported`。 |
| 在被推送的请求上再次 push | 协议规定 PUSH_PROMISE 只能在客户端发起的 stream 上发送。 |

## 一致性测试

测试集在 `http/http2_conformance_test.go`，测试名统一以 `TestHTTP2Conformance`
开头，CI 的 `HTTP/2 conformance` job 在 Linux、macOS、Windows 上运行。对端全部使用
标准库或 CI 上安装的工具，fib 本身不因此增加任何第三方依赖：

| 对端 | 验证内容 |
| --- | --- |
| [h2spec](https://github.com/summerwind/h2spec) v2.2.1（和 staticcheck 一样用 `go install` 安装） | 逐条验证服务端是否符合 RFC 9113、RFC 7541：帧格式、stream 状态、流控、HPACK、错误码。h2c 与 TLS 两种方式下 145 项全部通过 |
| `net/http` 的 HTTP/2 客户端与服务端 | 各种方法、超过流控窗口的 body、HEAD、无 body 的状态码、双向 trailer、1xx、100-continue、单连接并发 50 个请求 |
| curl（nghttp2） | 客户端进入 HTTP/2 的三种方式：prior knowledge、`Upgrade: h2c`、TLS ALPN |
| 测试内置的原始帧服务端 | 普通服务端不会暴露的客户端行为：preface 与 settings 内容、REFUSED_STREAM 与 GOAWAY 重发、RST_STREAM、小窗口下的流控（服务端对超出授权哪怕 1 字节都会报错）、服务端并发上限、CONTINUATION、PING、trailer，以及服务端在 push 被禁用时仍然 push |
| 测试内置的原始帧客户端 | stream 状态、server push、h2c 升级、优雅 GOAWAY、HTTP/2-only 模式 |

h2spec 只会说 HTTP/2，所以跑它时服务端要设置 `Config.HTTP2Only`：否则协议嗅探的
服务端会把它故意发送的非法 preface 当作 HTTP/1 请求回复，h2spec 读不懂这个回复。

## 基准测试

`go test ./http -run '^$' -bench '^BenchmarkH2Server' -benchmem -count 5`，在单连接上
保持 100 个请求并发，每个请求结束就补一个新的，和 h2load 一样。一次 op 是一个请求；
客户端与服务端在同一进程内，因此适合互相对比，不要与压测工具的结果直接比较。测试机是
共享的 16 线程 Ryzen 7 7800X3D，负载会让 ns/op 在不同次运行间浮动几个百分点；分配次数
是准确的，只有下面两项改动带来的时间变化超出了这个噪声。`hpack` 有自己的编码、解码
benchmark。

| Benchmark | ns/op | B/op | allocs/op |
| --- | --- | --- | --- |
| GET | 2230 | 10 | 0 |
| POST | 3136 | 145 | 2 |
| Static 1KB | 2480 | 9 | 0 |
| Static 16KB | 5630 | 28 | 0 |
| Static 64KB | 15503 | 126 | 0 |
| Static TLS 1KB | 3026 | 12 | 0 |
| Static TLS 16KB | 9191 | 53 | 1 |
| Static TLS 64KB | 22612 | 131 | 1 |
| Streamed POST | 5976 | 6425 | 13 |
| Push | 5032 | 233 | 4 |

push stream 复用把 `Push` 从每个请求 8 次分配、1954 字节降到 4 次、233 字节，时间从
5364 ns/op 降到 5032（与 main 交替跑四次取中位数）。单帧发送路径让 POST 从
3452 ns/op 降到 3226、静态 16KB 从 5833 降到 5519（同一分支开/关该路径交替跑三次取
中位数）；需要多帧的 64KB body 在噪声内没有变化，TLS 也没有，因为 TLS layer 本来就会把
交给它的数据一次封进一个 record。对这几个 benchmark 做 pprof 显示发送路径主要等在
socket 上（系统调用，TLS 上还有 AES-GCM），拷贝占比不大：memmove 只占采样 2.5%，这也是
去掉一次拷贝只值几个百分点的原因。其余分配主要来自客户端和测试框架（task pool 的环形
队列、定时器，以及 benchmark 自己的读取）。

## 压测

`benchmark/loadtest` 每次压 30 秒，覆盖 4 条连接 512 个 stream 与 64 条连接 512 个
stream，并在快速负载旁加一组慢读、一组完全不读的客户端。三轮交替的中位数：h2c 在
4 x 128 时 1,199,024 req/s、64 x 8 时 2,836,822；HTTP/2 over TLS 为 1,092,137 和
2,404,461；没有请求失败、出错或超时。HTTP/2 随连接数扩展，不读的客户端大约只占一个
stream 窗口，而不是响应 body。方法、HTTP/3 对比和整体响应路径的内存代价见
[压测](loadtest.zh-CN.md)。

## 待优化项

按优先级排列。

### 1. 安全加固

已完成。除了 `MaxConcurrentStreams`、`MaxHeaderBytes`、`MaxBodyBytes` 和流控窗口，客户端有下列行为时
服务端以 GOAWAY(ENHANCE_YOUR_CALM) 结束连接：

- 重置仍打开的 stream，或用 stream 错误让服务端重置它们，超过突发 1000 个、每秒补充 100 个的额度
  （Rapid Reset，CVE-2023-44487），由 `Config.StreamResetBurst` 和 `StreamResetRate` 设置；
- 发送 PING、SETTINGS（第一个除外），或者既为空又不结束任何东西的 DATA、CONTINUATION 帧，超过同样
  大小的额度，这也限制了服务端排队等待发送的 ACK，由 `Config.ControlFrameBurst` 和
  `ControlFrameRate` 设置；
- 一个 header block 用了超过 1000 个 CONTINUATION 帧（`Config.MaxContinuationFrames`），或者超过
  `ReadHeaderTimeout`（未设置时为 `ReadTimeout`）仍未结束。

PRIORITY 帧，以及针对已结束 stream 的 WINDOW_UPDATE、RST_STREAM 帧，处理代价不超过读取它们的字节，
客户端也可能在途中发出，因此不计入。每项限制为零时取上面的默认值（`DefaultStreamResetBurst` 等），
小于零则不限制。gRPC 客户端通过重置 stream 取消调用，超时或对冲请求会一次取消很多，所以面向繁忙
gRPC 客户端的服务端或代理可能需要比默认值更高的重置速率。

客户端以同样的控制帧额度和 CONTINUATION 上限约束服务端，使用默认值：服务端用 PING 或 SETTINGS 帧
（客户端都要回 ACK），或者空的 DATA、CONTINUATION 帧泛洪时，客户端以 GOAWAY(ENHANCE_YOUR_CALM)
结束连接，其上的请求以该 `*H2ConnError` 失败。服务端重置客户端的 stream 不会让客户端付出代价，
因此客户端没有重置额度。

### 2. 协议一致性测试

已完成，见[一致性测试](#一致性测试)。HTTP/1 请求解析、HTTP/2 帧读取和 HPACK
编解码都有 fuzz 目标（`http/fuzz_test.go`、`hpack/fuzz_test.go`），
CI 的 `Fuzz the parsers` job 每个目标跑 20 秒。压测也做了：[压测](loadtest.zh-CN.md)
用 h2load 和 fib 的 HTTP/3 客户端对每种配置压 30 秒，总计十亿多个请求，没有请求失败、
reset 风暴或 GOAWAY。它暴露出来的是整体响应发给不读的客户端时的内存代价，HTTP/1 和
HTTP/3 比 HTTP/2 明显。更长时间的 soak 和带丢包的网络路径仍未覆盖。

### 3. 流式 body 与 handler 模型（中）

- 流式请求 body 已经实现，接收窗口随 handler 的消费补充；流式响应也已实现，由客户端的
  窗口限速。整体缓存的 body 仍然是“收到即补充窗口”。客户端在 `ClientConfig.StreamResponseBody` 下把响应 body
  流式交给回调，否则整体缓存。

### 4. 性能（中）

- **HPACK Huffman 解码**：已完成，按查表每步解码 4 bit。
- **HPACK 编码器动态表索引**：已完成，动态表超过 16 项后按名字和名字-值的哈希索引查找，不再逐项搜索；
  表也可以长到 `HeaderTableSize` 与对端允许的范围。
- **基准测试**：已完成。`http/h2_benchmark_test.go` 像 h2load 一样在单连接上保持 100 个请求并发，
  覆盖 h2c 与 TLS、GET、POST、1KB/16KB/64KB 静态 body、流式上传和 server push；`hpack` 有编码与
  解码 benchmark。数字见下文[基准测试](#基准测试)。
- **push 的 stream**：已完成，和服务端 client stream 一样复用，连同它持有的 `Request`、`URL` 和
  `Context`。body 流式交付的 stream 仍然分配，因为它的回调可能在响应结束之后发生。
- **发送路径的拷贝**：h2c 上 body 能装进一帧的整体响应已经从两次拷贝降到一次；更大的 body
  以及 TLS 上的响应仍要先拷进帧，再拷进连接或 record，要再减少需要协议层支持分散写
  （vectored send），当前没有。

### 5. 请求 trailer（低）

- 已完成：客户端会发送请求 trailer，body 整体发送和流式发送都是（`Request.Trailer`，
  取 body 读完时的值），服务端把它交给 handler 的 `Request.Trailer`，与 HTTP/1 的 chunked
  请求一样。见[客户端](#客户端)。

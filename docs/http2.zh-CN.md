# HTTP/2：限制、有意未实现的功能与待优化项

[English](http2.md) | [简体中文](http2.zh-CN.md)

本文记录 Go 版 `http` package 中 HTTP/2 实现的边界：当前行为上的限制、出于设计考虑
有意没有实现的功能，以及后续可以优化的方向。功能用法见
[Go README 的 HTTP/2 章节](../go/README.zh-CN.md#http2)。

实现位于 [`go/http`](../go/http)（`h2_*.go`）以及内部的 hpack package，全部自行实现，
不依赖 `golang.org/x/net`。

## 已支持的范围（概览）

| 方向 | 内容 |
| --- | --- |
| 服务端 | TLS + ALPN（h2）、明文 prior knowledge（h2c）、HTTP/1.1 `Upgrade: h2c`；多路复用、双向流控、HPACK（含 Huffman、动态表）、CONTINUATION、trailer、server push、1xx 中间响应、自动 100 Continue、`Response.Close` 优雅 GOAWAY、`Request.TLS` |
| 客户端 | https 经 ALPN 协商 h2、`UnencryptedHTTP2` 明文 prior knowledge；单连接多路复用、遵守服务端 `MAX_CONCURRENT_STREAMS`、取消只重置单个 stream、GOAWAY / REFUSED_STREAM 自动重发 |
| 一致性 | h2spec：145 项全部通过，h2c 与 TLS 两种方式 |
| 互通验证 | Go `net/http` 客户端与服务端（TLS 与 h2c）、curl（nghttp2）的 h2 / h2c / Upgrade |

以上内容都有测试覆盖并在 CI 中运行，见下面的[一致性测试](#一致性测试)。

## 当前限制

使用时需要了解的行为约束。

### 消息体整体缓存，不支持流式

- 请求 body 在 handler 运行前完整读入内存，响应通过 `Response.Body []byte` 一次性给出；
  客户端同样把响应 body 完整缓存后再回调。
- handler 也可以通过 `Context` 的 `http.ResponseWriter` 方法（`Header`/`WriteHeader`/
  `Write`/`Flush`）写响应（含 trailer），或把 `Context` 交给 `http.ServeFile`、
  `http.ServeContent`。在 HTTP/1 上这是真正的流式输出（chunked，文件走 sendfile，见
  [`http1.zh-CN.md`](http1.zh-CN.md)）；在 HTTP/2 上响应会先缓存，handler 返回后整体
  发送，`Flush` 不起作用。
- 因此在 HTTP/2 上无法实现 SSE、长轮询流式输出、gRPC streaming、边收边处理的大文件上传等场景。
- 内存上限：服务端单连接最坏约为 `MaxConcurrentStreams × MaxBodyBytes`
  （默认 250 × 16MB）；客户端单个响应受 `MaxResponseBodyBytes` 限制。
- `CONNECT` 请求能被解析并交给 handler，但无法建立隧道（没有双向流式通道）。

### handler 在连接的 worker 上同步执行

- 一个连接的帧按顺序处理，请求完整后在读取它的 worker 上同步调用 handler。handler
  同步阻塞时，同一连接上其他 stream 的请求也要等待（应用层的队头阻塞）。
- 耗时操作应在 handler 中启动 goroutine 后异步调用 `Respond`/`WriteResponse`，
  不同 stream 的响应互不阻塞。
- `Push` 会同步执行被推送请求的 handler，在它返回后才返回；被推送的 handler 慢，
  父请求的响应也会随之推迟。

### 不能向连接直接写字节

- HTTP/2 连接上不要调用 `Context.Conn.Send` 等方法直接写数据，否则会破坏帧结构；
  所有输出都必须经过 `Context`。

### 固定参数

以下参数目前是常量，不能配置：

| 参数 | 值 |
| --- | --- |
| 每个 stream 的接收窗口 | 1MB，消费一半后补充 |
| 连接的接收窗口 | 16MB，消费一半后补充 |
| 本端 `SETTINGS_MAX_FRAME_SIZE` | 16384 |
| HPACK 动态表 | 4096 字节（编码器不会使用超过 4096 的表） |
| 客户端在收到服务端 SETTINGS 前假定的并发 stream 数 | 100 |

### 协议细节

- **优先级**：PRIORITY 帧和 HEADERS 中的优先级字段会被校验后忽略；多个 stream
  同时有待发送数据时，发送顺序由内部 map 的遍历顺序决定，没有公平性或权重保证。
- **对端的 `SETTINGS_MAX_HEADER_LIST_SIZE`**：本端会声明自己的上限，但发送时不检查
  对端声明的上限。
- **优雅关闭时的 TCP RST**：GOAWAY 后连接在所有 stream 完成时关闭，但不会先半关闭、
  排空对端仍在发送的数据；如果此时 socket 中还有未读数据，内核会发送 RST。
- **引擎停止**：`Engine.Stop`/`Close` 不会给 HTTP/2 连接发送 GOAWAY，连接被直接关闭，
  客户端看到的是连接中断而不是优雅关闭。
- **空闲与保活**：服务端没有 HTTP/2 连接的空闲超时，也不发送 PING 保活；客户端只有
  `IdleConnTimeout`（空闲时关闭），不做 PING 健康检查，无法及时发现静默断开的连接。
- **SETTINGS 确认超时**：不检测对端是否在合理时间内确认了本端的 SETTINGS
  （SETTINGS_TIMEOUT）。

### 客户端

- https 的 host 在第一次连接确定协议之前只会同时拨号一条连接（与 `net/http` 相同），
  对只支持 HTTP/1.1 的 https 服务，首批并发请求要多等一次 TLS 握手。
- 多条 HTTP/2 连接之间按顺序选择第一条还有余量的连接，而不是选择负载最低的。
- 已收到部分响应后连接断开的请求直接失败，不会重发；只有没有收到任何响应数据且方法
  幂等的请求，以及被 GOAWAY / REFUSED_STREAM 明确标为未处理的请求会重发。
- 带 `Expect: 100-continue` 的请求不会等待 100，body 和请求头一起发送（协议允许）。
- 明文 HTTP/2 只支持 prior knowledge，不支持从 HTTP/1.1 `Upgrade: h2c` 升级。
- 带 `Expect: 100-continue` 的请求不等 100 就发送 body（见上），所以服务端即使想拒绝
  body 也仍然会收到。

## 有意没有实现的功能

| 功能 | 原因 |
| --- | --- |
| 客户端接收 server push | Chrome、Firefox 已移除 push，Go 的 `net/http` 客户端也从未支持。客户端在 SETTINGS 中声明 `ENABLE_PUSH=0`，服务端 push 能力保留给仍然需要它的客户端。预加载推荐用 103 Early Hints（`Context.WriteInterim`）。 |
| RFC 9218 可扩展优先级（`priority` 头、PRIORITY_UPDATE） | 在当前“body 整体缓存、handler 同步执行”的模型下，调度收益有限；RFC 7540 的优先级树已被 RFC 9113 废弃，同样不实现。 |
| Extended CONNECT（RFC 8441，WebSocket over HTTP/2） | 依赖双向流式 stream，需要先支持流式 body；WebSocket 目前走 HTTP/1.1 Upgrade。 |
| 客户端发起 `Upgrade: h2c` | RFC 9113 已废弃这种升级方式；需要明文 HTTP/2 的场景使用 prior knowledge（`UnencryptedHTTP2`）。服务端仍然接受升级，以兼容 curl 等客户端。 |
| TLS 连接上的 `Upgrade: h2c` | RFC 规定 TLS 上只能通过 ALPN 切换协议。 |
| HTTP/1.0 请求的 1xx 中间响应 | HTTP/1.0 客户端不认识 1xx，`WriteInterim` 返回 `http.ErrNotSupported`。 |
| 在被推送的请求上再次 push | 协议规定 PUSH_PROMISE 只能在客户端发起的 stream 上发送。 |

## 一致性测试

测试集在 `go/http/http2_conformance_test.go`，测试名统一以 `TestHTTP2Conformance`
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

## 待优化项

按优先级排列。

### 1. 安全加固（高）

目前只靠 `MaxConcurrentStreams`、`MaxHeaderBytes`、`MaxBodyBytes` 和流控窗口限制资源，
面对恶意客户端还缺少以下防护：

- **Rapid Reset（CVE-2023-44487）**：客户端可以不断打开 stream 后立即 RST_STREAM。
  需要统计单位时间内被重置的 stream 数，超过阈值时以 ENHANCE_YOUR_CALM 发送 GOAWAY。
- **控制帧洪泛**：PING、SETTINGS、空 DATA、WINDOW_UPDATE 等帧没有速率限制；
  PING/SETTINGS 的 ACK 会不断进入发送队列。需要限制待发送控制帧的数量。
- **CONTINUATION 洪泛**：单个 header block 已受 `MaxHeaderBytes` 限制，但还应限制
  CONTINUATION 帧的数量，以及零长度帧的数量。
- **慢速连接**：没有 header 读取超时、空闲超时（见上文），可被大量半开连接占用资源。

### 2. 协议一致性测试

已完成，见[一致性测试](#一致性测试)。HTTP/1 请求解析、HTTP/2 帧读取和 HPACK
编解码都有 fuzz 目标（`go/http/fuzz_test.go`、`go/internal/hpack/fuzz_test.go`），
CI 的 `Fuzz the parsers` job 每个目标跑 20 秒。还缺的是压测（例如 h2load）才能暴露的
并发问题。

### 3. 流式 body 与 handler 模型（中）

- 让 `Context` 已有的 `http.ResponseWriter` 方法在 HTTP/2 上也像 HTTP/1 一样流式输出，
  并提供流式的请求 body 读取，以支持 SSE、gRPC、大文件传输；同时可以把接收窗口的补充
  与实际消费挂钩，形成真正的端到端背压，而不是现在“收到即补充窗口”。

### 4. 性能（中）

- **HPACK Huffman 解码**：目前逐 bit 遍历解码树，可改为按 4 bit 或 8 bit 查表解码。
- **HPACK 编码器**：动态表按线性查找，可以加索引；静态表查找已经是 map。
- **内存分配**：每个请求会分配 header map、帧缓冲、`stdhttp.Request` 等对象，可以像
  HTTP/1 路径一样用 `sync.Pool` 复用帧缓冲和 header 解码缓冲。
- **发送合并**：同一次处理中产生的多个控制帧（WINDOW_UPDATE、SETTINGS ACK、PING ACK）
  分别调用发送，可以合并为一次写入。
- **基准测试**：补充 HTTP/2 的 benchmark（单连接多路复用吞吐、HPACK 编解码），
  用 pprof 指导优化。

### 5. 可配置性与调度（低）

- 把接收窗口大小、最大帧大小、HPACK 表大小、客户端初始并发数做成配置项；可选根据
  BDP 动态调整窗口。
- 发送侧对多个待发送 stream 做轮询或按权重调度，保证公平。
- 发送时遵守对端的 `SETTINGS_MAX_HEADER_LIST_SIZE`。
- 服务端：可配置的空闲超时和 PING 保活；`Engine` 停止时对 HTTP/2 连接发送 GOAWAY
  并等待在途 stream 完成（优雅停机）；GOAWAY 后半关闭并排空输入，避免 RST。
- 客户端：可选的 PING 健康检查、按负载选择连接、SETTINGS 确认超时检测。
- 客户端可选地等待 100-continue 再发送 body。

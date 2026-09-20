# HTTP/1.x：支持情况、限制与一致性测试

[English](http1.md) | [简体中文](http1.zh-CN.md)

本文整理 Go `http` package 对 HTTP/1.0 和 HTTP/1.1（RFC 9110、RFC 9112）的支持情况、
边界以及测试方式。用法见 [Go README 的 HTTP 章节](../go/README.zh-CN.md#http-子-package)。
HTTP/2、HTTP/3 各有单独的文档：[`http2.zh-CN.md`](http2.zh-CN.md)、
[`http3.zh-CN.md`](http3.zh-CN.md)。

## 支持情况

| 方面 | 服务端 | 客户端 |
| --- | --- | --- |
| 版本 | 接受 HTTP/1.0 和 HTTP/1.1 请求，按请求的版本回复 | 发送 HTTP/1.1；请求的 `ProtoMinor` 为 0 时发送 HTTP/1.0 |
| 连接 | keep-alive（HTTP/1.1 默认开启，HTTP/1.0 需 `Connection: keep-alive`）、任一方的 `Connection: close`、pipelining（按请求顺序回复） | 每个 host:port 一个 keep-alive 连接池；HTTP/1.0 连接只有在请求要求 keep-alive 且服务端同意时才复用 |
| 请求 body | `Content-Length`、chunked（含 chunk 扩展和 trailer，`Request.Trailer`）；默认整体缓存，超过 `StreamRequestBodyThreshold` 的 body 边收边交给 handler | `Content-Length`；`ContentLength` 为 -1 时用 chunked，可带 `req.Trailer` |
| 响应 body | `Content-Length`、chunked（含 trailer）、HTTP/1.0 流式响应以关闭连接结束 | `Content-Length`、chunked（含 trailer，`Response.Trailer`）、以关闭连接结束 |
| 流式 | 响应：`Context` 实现了 `http.ResponseWriter`、`http.Flusher`、`io.ReaderFrom`，`Write` + `Flush` 边生成边发送。请求：超过 `StreamRequestBodyThreshold` 时 `Request.Body` 是 `*BodyStream` | body 完整缓存后再回调 |
| 文件 | `Connection.SendFile` / `Context.ReadFrom`：Linux、macOS 用 `sendfile(2)`，Windows 分块读取；`http.ServeFile`、`http.ServeContent`、`http.FileServer` 可以直接通过 `Context` 使用（Range、多段 Range、条件请求） | — |
| 1xx 中间响应 | `WriteInterim` 或 `WriteHeader(1xx)`，自动 `100 Continue` | 自动跳过 |
| 无 body 的响应 | HEAD（保留 GET 的长度）；204、1xx 不带 `Content-Length`；304 只带 handler 自己给的 `Content-Length` | HEAD、204、304 不读 body |
| 校验 | HTTP/1.1 缺少或重复 `Host`、HTTP/1.0 带 `Transfer-Encoding`、报文格式错误时返回 400；chunked 以外的 transfer coding 返回 501；无法满足的 `Expect` 返回 417；超限返回 413/431 | 格式错误的响应、未知 transfer coding、body 不完整都会让请求失败 |
| 帧格式冲突 | 同时有 `Content-Length` 和 chunked：按 chunked 读取，回复后关闭连接 | — |
| 头部 | handler 未设置时自动添加 `Date`；服务端写的帧格式相关头会替换 handler 设置的同名头 | — |

### 响应帧格式的选择

handler 用 `Write`/`WriteHeader` 而不是 `WriteResponse` 写响应时，服务端这样选择帧格式：

1. handler 设置了 `Content-Length`：按该长度发送，超出的写入返回
   `http.ErrContentLength`；写得不够时关闭连接。
2. 否则，body 不超过 4KB 且在 handler 返回前写完：用实际长度作 `Content-Length`。
3. 否则，HTTP/1.1：chunked，trailer 也靠它携带。
4. 否则，HTTP/1.0：body 以关闭连接结束。

`Flush` 发送响应头和缓存的数据，并立即交给 socket；不调用时，engine 会把一轮读取中
产生的输出合并，等 handler 返回后一次写出（见 `Connection.Flush`）。

### 流式请求 body

`Config.StreamRequestBodyThreshold` 大于 0 时，body 不必收齐就会调用 handler：
`Content-Length` 超过它的 body，以及已经发来超过这么多字节的 chunked body，以
`*BodyStream` 的形式出现在 `Request.Body` 里。它是普通的 `io.ReadCloser`，`io.Copy`、
`multipart.Reader`、`json.Decoder` 和读 `net/http` 的 body 一样读它；
`Context.RequestBody()` 返回它，body 已经收全时返回 nil。小于阈值的 body 行为不变：
整体缓存、handler 在连接的 worker 上执行。

- **handler 跑在自己的 goroutine 上**：读 body 会阻塞，所以不能占用 worker。是每个流式
  请求一个 goroutine，不是每个连接一个。
- **背压**：已到达但还没被读走的 body 最多缓存 `Config.StreamRequestBodyBuffer`
  （默认 256KB），超过后连接通过 `Connection.HoldReads` 停止读 socket，读走一半之后
  恢复。上传快过 handler 处理速度时，被减速的是对端而不是这一侧的内存——这是写方向
  水位的读方向对应物。
- **大小限制**：`MaxBodyBytes` 不再约束流式 body（流式的意义就是接受超过服务端内存的
  上传），改由 `MaxStreamedBodyBytes` 约束，为 0 表示不限。超限时 `Read` 返回
  `ErrBodyTooLarge` 并关闭连接；`Content-Length` 一开始就超限的请求在 handler 之前就用
  413 拒绝。
- **pipelining**：排在流式请求后面的请求要等它的 handler 返回之后才解析，响应顺序不变。
- **handler 没读完的 body** 会被读掉丢弃，最多 256KB，连接继续复用；超过这个量（或者
  长度未知的 chunked body）则在响应发完后关闭连接，与 `net/http` 一致。
- **`Expect: 100-continue` 变成惰性的**：第一次读 body 时才发 100 Continue，handler 可以
  在上传开始之前用 413、403 拒绝请求。这样回复而不读 body 时连接会关闭，因为客户端还在
  等这个许可。
- **连接中途断开**时 `Read` 返回 `io.ErrUnexpectedEOF` 而不是 `io.EOF`，截断的上传不会被
  当成完整的。handler 返回之后或 `Close` 之后再读返回 `ErrBodyAbandoned`。
- HTTP/2 和 HTTP/3 的请求 body 仍然整体缓存。

handler 把 128MB 的上传拷进 `io.Discard`：整体缓存时堆内存峰值约 491MB（body 本身、
它增长所在的解析缓冲区，以及拷贝留下的垃圾），流式时约 3MB
（`StreamRequestBodyBuffer` 为默认的 256KB）。

### 零拷贝发送文件

`Connection.SendFile(f, offset, count)` 把文件的一段排进发送队列，与连接上其他发送保持
顺序。连接会复制描述符，调用方可以立即关闭自己的文件。Linux 和 macOS 上数据由
`sendfile(2)` 从文件直接发到 socket；socket 写满时剩余部分等待下一次可写事件，所以文件
只按对端的接收速度读取。Linux 上可以用 `strace -e sendfile` 观察到，例如
`sendfile(10, 12, [0] => [2673856], 3145851) = 2673856`，socket 排空后再发送剩余部分。

`Context.ReadFrom` 对普通文件、以及包着文件的 `*io.LimitedReader`（`http.ServeContent`
传入的就是它）使用 `SendFile`。小于 16KB 的范围直接拷贝，因为这比多出的系统调用更省。

## 当前限制

- **请求 body 默认整体缓存**：body 收完后才调用 handler，受 `MaxBodyBytes` 限制。设置
  `Config.StreamRequestBodyThreshold` 后超过阈值的 body 不再受此限制，见
  [流式请求 body](#流式请求-body)。
- **客户端响应 body 整体缓存**：受 `MaxResponseBodyBytes` 限制，客户端没有流式下载。
- **客户端不做 pipelining**：一个 HTTP/1 连接同时只有一个请求。
- **流式响应在 handler 中写出**：与 `net/http` 一样，handler 返回时响应结束；需要之后
  在其他 goroutine 中回复的 handler 应使用 `WriteResponse`，不要先用 `Write` 开始响应。
- **写入不阻塞**：handler 写得比对端读得快时，差额排队在内存中（连接的读取会暂停，但
  handler 本身不会被阻塞）。`SendFile` 发送的文件例外：它只随 socket 的排空读取。
- **TLS 无法使用 sendfile**：TLS 连接上 `SendFile` 在返回前读出并加密整段文件，整段
  都会排队在内存中。
- **HTTP/2 和 HTTP/3** 同样接受这些 `ResponseWriter` 调用，但响应会缓存到 handler 返回
  后整体发送（见各自的文档）。
- **不解码 chunked 以外的 transfer coding**（gzip、deflate、compress）：这样的请求返回
  501，这样的响应会失败。
- **除 h2c 和 WebSocket（在 `websocket` package 中）外不处理 `Upgrade`**；`CONNECT`
  请求会交给 handler，但无法建立隧道。
- **服务端没有空闲、读头、读 body 超时**：慢客户端可以一直占着连接，见待优化项。

## 待优化

- 服务端超时：读头、读 body、空闲超时，对应 `net/http.Server` 的
  `ReadHeaderTimeout`、`ReadTimeout`、`IdleTimeout`。
- HTTP/2 和 HTTP/3 的流式请求 body（目前仍然整体缓存），客户端流式读取响应 body。
- 让 handler 能等待已排队的输出排空，使流式生成大 body 时内存不再增长。
- TLS 上按 socket 排空进度读取并加密文件；Linux 上用 kTLS 实现真正的 HTTPS 零拷贝。
- Windows 上使用 `TransmitFile`。

## 一致性测试

`go/http/http1_conformance_test.go`（所有测试都以 `TestHTTP1Conformance` 开头）用非 fib
的对端检查 fib 的服务端和客户端：

- **fib 服务端**对 Go `net/http` 客户端、原始 TCP 连接（用来发送 `net/http` 不会发出的
  请求：带或不带 keep-alive 的 HTTP/1.0、pipelining、格式错误或帧格式冲突的请求、chunk
  扩展），以及 **curl**（`--http1.0`、keep-alive 复用、chunked 上传、sendfile 路径的
  下载、Range、原始 chunked trailer）。
- **fib 客户端**对 Go `net/http` 服务端（`httptest`）和原始服务端（HTTP/1.0 keep-alive
  和以关闭连接结束的响应、格式错误的响应）。
- **fib 客户端对 fib 服务端**，包括 HTTP/1.0 和 sendfile。
- **流式请求 body**（`go/http/body_test.go`）：handler 在 body 结束前就运行、小于阈值的
  body 仍然整体缓存、一块一块发来的带 trailer 的 chunked 上传、handler 落后时暂停读、
  没读完的 body 的丢弃与关闭、惰性与被拒绝的 100-continue、`MaxStreamedBodyBytes`、
  截断的上传、流式请求之后的 pipelining、handler panic、用 `net/http` 客户端上传，以及
  逐字节喂给增量 chunked 解码器。`go/hold_reads_test.go` 单独检查
  `Connection.HoldReads`。
- `go/sendfile_test.go` 在 TCP 和 Unix socket 上检查 `Connection.SendFile`：在 handler
  中和其他 goroutine 中调用、对端读得慢、文件比声明的范围短。

对端只有标准库和 curl 可执行文件，Go module 不增加任何依赖。CI 中单独的
**HTTP/1.x conformance** job 在 Linux、macOS、Windows 上运行这些测试，并设置
`FIB_REQUIRE_CURL=1`：缺少 curl 时直接失败而不是跳过 curl 用例。本地运行：

```sh
cd go
go test -race -run 'TestHTTP1Conformance|TestSendFile|TestSendableFileOf|TestResponseWriter|TestStreamRequestBody|TestChunkedDecoder|TestHoldReads' -v . ./http/
```

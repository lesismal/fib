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
| 流式 | 响应：`Context` 实现了 `http.ResponseWriter`、`http.Flusher`、`io.ReaderFrom`，`Write` + `Flush` 边生成边发送。请求：超过 `StreamRequestBodyThreshold` 时 `Request.Body` 是 `*BodyStream`，读取不阻塞，也可以用 `Context.OnBody` 接管 | body 完整缓存后再回调 |
| 文件 | `Connection.SendFile` / `Context.ReadFrom`：Linux、macOS 用 `sendfile(2)`，Windows 分块读取；`http.ServeFile`、`http.ServeContent`、`http.FileServer` 可以直接通过 `Context` 使用（Range、多段 Range、条件请求） | — |
| 超时 | `ReadHeaderTimeout`、`ReadTimeout`、`IdleTimeout`，回退规则与 `net/http.Server` 相同 | 每个请求的 `Timeout` |
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

### 保持响应不结束，以及 body 回调

响应由引用计数持有。处理一个请求先占一个引用，handler 返回时还回去——所以「回复完就
返回」的 handler 什么都不用做：外层替它结束响应并交给连接。要稍后再回复的 handler 用
`Context.Retain()` 多占一个、用 `Context.Release()` 还回去，最后一个引用释放时（不管在
哪个 goroutine）响应才被写回。

HTTP/1 连接上，被持有的请求会挡住排在它后面的流水线请求，等它释放之后才解析和处理，
所以响应顺序不变。HTTP/2 和 HTTP/3 上一个 stream 不挡别的 stream。

`Context.OnBody` 用回调的方式接收请求 body，而不是从 `Request.Body` 读——和 websocket
handler 的 `OnFrame` 按帧拿到消息是一个路子：

```go
func(c *fibhttp.Context, r *http.Request) {
	f, _ := os.Create("upload.bin")
	c.Retain()
	c.OnBody(func(data []byte, fin bool, err error) {
		if err != nil {         // 连接断了，或者 body 出错了
			f.Close()
			os.Remove("upload.bin")
			c.Release()
			return
		}
		f.Write(data)
		if fin {
			f.Close()
			c.Respond(200, "text/plain", []byte("stored"))
			c.Release()
		}
	})
}
```

- `fin` 标记最后一次回调；`err` 表示这个 body 不会有后续了，同样是最后一次回调，且只来
  一次。`data` 只在回调期间有效，需要保留先复制。
- 回调在连接的 worker 上执行，一次一个、保持顺序，所以任意大的 body 都能处理，既不需要
  handler 自己的 goroutine，也不会被缓存下来。给对端限速的就是回调本身。
- body 只有在流式交付时才会分多次到达（由 `StreamRequestBodyThreshold` 决定）；handler
  运行前就收全的 body 会在一次 `fin` 为 true 的回调里全部给出。
- `OnBody` 接管之后 `Request.Body` 不能再读。只用 `OnBody` 而不 `Retain` 的 handler 在
  返回时就回复、剩下的 body 被丢弃——这正是「不读 body 直接拒绝上传」的写法。

### 请求在响应之前就结束了

连接断开、读超时、body 收不完，都会让一个 handler 可能还在处理的请求提前结束。这时请求
被取消：不会再写出任何东西，它上面剩余的引用全部作废，handler 只会被告知一次——通过
`OnBody` 的 `err`，以及 `Context.OnCancel`。两者都在单独的 goroutine 上执行而不是事件
循环上，所以 handler 的清理逻辑拖不住服务端。想主动查的话 `Context.Err()` 给出同样的
原因。

从这里开始一切都是幂等的：已经结束的请求上再调 `Retain`、`Release`、`Finish` 都是空操作，
清理代码可以放心调用而不必先判断；往上面写数据会返回那个原因，而不会把连接写坏。

### 读超时

`Config.ReadHeaderTimeout`、`Config.ReadTimeout`、`Config.IdleTimeout` 就是
`net/http.Server` 的那三个，回退规则也一样：`ReadHeaderTimeout` 为 0 时用
`ReadTimeout`，`IdleTimeout` 同理，全为 0 表示不限（默认）。超时的连接会被关闭，
`OnClose` 收到 `os.ErrDeadlineExceeded`。

用哪一个取决于连接在等什么：

| 在等 | 受限于 | 从什么时候算 |
| --- | --- | --- |
| 第一个请求，或 keep-alive 连接上的下一个请求 | `IdleTimeout` | 上一个响应之后，或连接建立时 |
| header 收完 | `ReadHeaderTimeout` | 该请求的第一个字节 |
| body 收完（流式与否都一样） | `ReadTimeout` | 该请求的第一个字节 |
| 不在等——请求已经收全 | 不限 | — |

最后一行是和 `net/http` 的区别：`net/http` 把读 deadline 设在整个请求期间，handler
只有在读的时候才会察觉；这里 deadline 是直接关连接的，所以请求收全之后就会撤掉——
比 `ReadTimeout` 慢的 handler 仍然能把响应写完，`ReadTimeout` 约束的是请求到达的时间
而不是处理它花的时间。流式 body 在 handler 运行期间仍在到达，所以它确实受
`ReadTimeout` 约束，这正是慢上传占不住连接的原因。

通过 preface 或 `h2c` 升级变成 HTTP/2 的连接会被解除这些超时：HTTP/2 有自己的连接
状态，HTTP/1 解析器设下的 deadline 之后没人会去刷新它。

### 流式请求 body

`Config.StreamRequestBodyThreshold` 大于 0 时，body 不必收齐就会调用 handler：
`Content-Length` 超过它的 body，以及已经发来超过这么多字节的 chunked body，以
`*BodyStream` 的形式出现在 `Request.Body` 里。`Context.RequestBody()` 返回它，body
已经收全时返回 nil。小于阈值的 body 行为不变：整体缓存，其余一切照旧。

**读取不阻塞。** handler 和别的 handler 一样跑在连接的 worker 上，而 worker 去等对端
就是在等自己，所以 `Read` 只给出已经到达的部分：

| Read 返回 | 含义 |
| --- | --- |
| `n > 0` | 这些字节已经到了 |
| `io.EOF` | 整个 body 已经读完 |
| `ErrWouldBlock` | 现在一个字节都没有，后续还会来 |
| `ErrBodyAbandoned` | body 已经被别的东西接管：`Close`、`OnBody`，或者 handler 没有 Retain 就返回了 |
| 其它错误 | 后续不会再来了——连接断了、超过 `MaxStreamedBodyBytes`、帧格式错误 |

`ErrWouldBlock` 就是 `fib.ErrWouldBlock`，和 `Connection.Read` 用的是同一个，判断哪个
都行。body 有可能在 handler 运行时就已经全在了（和 header 在同一次读里到达），这种情况
下 handler 一路读到 `io.EOF`、直接回复即可。

遇到 `ErrWouldBlock` 又想要后续的 handler，就 Retain 住请求、用 `Context.OnBody` 接管
剩下的部分，见[保持响应不结束](#保持响应不结束以及-body-回调)。交接不会丢字节：`OnBody`
拿到的正好从上一次 `Read` 停下的地方开始。

```go
func(c *fibhttp.Context, r *http.Request) {
	n, err := r.Body.Read(buf)          // 先拿已经到的
	switch {
	case errors.Is(err, io.EOF):        // 就这么多，全了
		answer(c)
	case errors.Is(err, fibhttp.ErrWouldBlock):
		c.Retain()                      // 后面还有
		c.OnBody(func(data []byte, fin bool, err error) { ... })
	}
}
```

- **背压**：已到达但还没被读走的 body 最多缓存 `Config.StreamRequestBodyBuffer`
  （默认 256KB），超过后连接通过 `Connection.HoldReads` 停止读 socket，读走一半之后
  恢复。上传快过 handler 处理速度时，被减速的是对端而不是这一侧的内存——这是写方向
  水位的读方向对应物。
- **大小限制**：`MaxBodyBytes` 不再约束流式 body（流式的意义就是接受超过服务端内存的
  上传），改由 `MaxStreamedBodyBytes` 约束，为 0 表示不限。超限时 `Read` 返回
  `ErrBodyTooLarge` 并关闭连接；`Content-Length` 一开始就超限的请求在 handler 之前就用
  413 拒绝。
- **pipelining**：排在流式请求后面的请求要等它的响应写完之后才解析，响应顺序不变。
- **handler 没读完的 body** 会被读掉丢弃，最多 256KB，连接继续复用；超过这个量（或者
  长度未知的 chunked body）则在响应发完后关闭连接，与 `net/http` 一致。
- **`Expect: 100-continue` 变成惰性的**：handler 第一次要 body 时（读它，或者用 `OnBody`
  接管它）才发 100 Continue，所以可以在上传开始之前就用 413、403 拒绝请求。回复了却不要
  body 时连接会关闭，因为客户端还在等这个许可。
- **连接中途断开**时 `Read` 返回 `io.ErrUnexpectedEOF` 而不是 `io.EOF`，截断的上传不会被
  当成完整的。handler 返回之后或 `Close` 之后再读返回 `ErrBodyAbandoned`。
- HTTP/2 和 HTTP/3 的请求 body 仍然整体缓存。

handler 收下 128MB 的上传并计数：整体缓存时堆内存峰值约 470MB（body 本身、它增长所在
的解析缓冲区，以及沿途留下的垃圾），流式时约 4MB（`StreamRequestBodyBuffer` 为默认的
256KB）。

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
- **响应在最后一个引用释放时结束**：什么都不 Retain 的 handler 就是它自己返回的时候，
  与 `net/http` 一致。要稍后回复、或者要在其他 goroutine 里接着写的 handler 先
  `Retain`，见[保持响应不结束](#保持响应不结束以及-body-回调)。
- **读取同样不阻塞**：handler 跑在连接自己的 worker 上，所以流式 body 的读取返回
  `ErrWouldBlock` 而不是等对端；剩下的部分用 `OnBody` 接管。
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

## 待优化

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
- **持有响应与 body 回调**（`go/http/retain_test.go`）：在别的 goroutine 里回复、被持有
  的请求挡住后面的流水线请求、嵌套持有、`OnBody` 对缓存 body 和流式 body、在 body 回调
  里释放、带 trailer 的 chunked、不 Retain 直接回复、客户端上传到一半跑掉时 `OnBody` 和
  `OnCancel` 各只收到一次、读超时触发 `OnCancel`、body 超限、响应写完之后再 Retain、同一
  个 handler 跑在 HTTP/2 上，以及把以上全部并发跑一遍并检查没有残留的压力测试。
- **读超时**（`go/http/timeout_test.go`）：header 和 body 收到一半就不来了、流式 body
  收到一半就不来了、比 `ReadTimeout` 慢的 handler 仍然能回复、空闲连接被关闭、每个请求
  都会刷新空闲超时、一个字节都不发的连接、没配超时的服务端、变成 HTTP/2 的连接被解除
  超时，以及超时传到 `OnClose`。`go/netconn_test.go` 检查 `Connection` 自己的 deadline
  和其余 `net.Conn` 方法。
- **流式请求 body 的非阻塞读取**：`Read` 返回 `ErrWouldBlock` 后交给 `OnBody` 不丢字节、
  body 已经全在时一路读到 `io.EOF`、读和 `OnBody` 都能触发 100-continue，以及 64 个上传
  同时挂起而不多出一条 goroutine。
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
go test -race -run 'TestHTTP1Conformance|TestSendFile|TestSendableFileOf|TestResponseWriter|TestStreamRequestBody|TestChunkedDecoder|TestHoldReads|TestServerRead|TestServerIdle|TestServerTimeout|TestServerWithoutTimeouts|TestReadDeadline|TestWriteDeadline|TestZeroDeadline|TestConnectionAddresses|TestRetain|TestOnBody|TestOnCancel' -v . ./http/
```

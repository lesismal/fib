# HTTP/1.x：支持情况、限制与一致性测试

[English](http1.md) | [简体中文](http1.zh-CN.md)

本文整理 Go `http` package 对 HTTP/1.0 和 HTTP/1.1（RFC 9110、RFC 9112）的支持情况、
边界以及测试方式。用法见 [Go 使用指南的 HTTP 章节](guide.zh-CN.md#http-子-package)。
HTTP/2、HTTP/3 各有单独的文档：[`http2.zh-CN.md`](http2.zh-CN.md)、
[`http3.zh-CN.md`](http3.zh-CN.md)。

## 支持情况

| 方面 | 服务端 | 客户端 |
| --- | --- | --- |
| 版本 | 接受 HTTP/1.0 和 HTTP/1.1 请求，按请求的版本回复 | 发送 HTTP/1.1；请求的 `ProtoMinor` 为 0 时发送 HTTP/1.0 |
| TLS 0-RTT | `Config.EarlyData`：作为 early data 到达的请求带 `Early-Data: 1`，只服务 GET/HEAD/OPTIONS/TRACE，其他方法回 425 并关闭连接，见 [TLS 0-RTT](#tls-0-rtt) | `ClientConfig.EarlyData` + `SessionCache`：只把安全方法且没有 body 的请求作为 early data 发出，收到 425 后重发；只走 HTTP/1.1 |
| 连接 | keep-alive（HTTP/1.1 默认开启，HTTP/1.0 需 `Connection: keep-alive`）、任一方的 `Connection: close`、pipelining（按请求顺序回复） | 每个 host:port 一个 keep-alive 连接池；HTTP/1.0 连接只有在请求要求 keep-alive 且服务端同意时才复用 |
| 请求 body | `Content-Length`、chunked（含 chunk 扩展和 trailer，`Request.Trailer`）、chunked 之上再用 gzip、x-gzip 或 deflate 编码的 body；默认整体缓存，开启 `StreamRequestBody` 后边收边交给 handler | `Content-Length`；`ContentLength` 为 -1 时用 chunked，可带 `c.Request.Trailer` |
| 响应 body | `Content-Length`、chunked（含 trailer）、HTTP/1.0 流式响应以关闭连接结束 | `Content-Length`、chunked（含 trailer，`Response.Trailer`）、以关闭连接结束，以及 gzip、x-gzip、deflate 编码的 body |
| 流式 | 响应：`Context` 实现了 `http.ResponseWriter`、`http.Flusher`、`io.ReaderFrom`，`Write` + `Flush` 边生成边发送。请求：开启 `StreamRequestBody` 后 `Request.Body` 是 `*BodyStream`，读取不阻塞，也可以用 `Context.OnBody` 接管 | body 完整缓存后再回调 |
| 文件 | `Connection.SendFile` / `Context.ReadFrom` / `Context.SendFile`：Linux、macOS 用 `sendfile(2)`，Windows 用 `TransmitFile`；TLS 上 Linux 用内核 TLS 加 `sendfile(2)`，其他情况随 socket 排空逐块加密；`http.ServeFile`、`http.ServeContent`、`http.FileServer` 可以直接通过 `Context` 使用（Range、多段 Range、条件请求） | — |
| 超时 | `ReadHeaderTimeout`、`ReadTimeout`、`IdleTimeout`，回退规则与 `net/http.Server` 相同，另有 `ReadBodyTimeout` | 每个请求的 `Timeout` |
| 1xx 中间响应 | `WriteInterim` 或 `WriteHeader(1xx)`，自动 `100 Continue` | 自动跳过 |
| 无 body 的响应 | HEAD（保留 GET 的长度）；204、1xx 不带 `Content-Length`；304 只带 handler 自己给的 `Content-Length` | HEAD、204、304 不读 body |
| 校验 | HTTP/1.1 缺少或重复 `Host`、HTTP/1.0 带 `Transfer-Encoding`、报文格式错误时返回 400；不支持的 transfer coding（`compress`、`br`，或 chunked 之外的多个 coding）返回 501；无法满足的 `Expect` 返回 417；超限返回 413/431 | 格式错误的响应、客户端不支持的 transfer coding、body 不完整都会让请求失败 |
| 帧格式冲突 | 同时有 `Content-Length` 和 chunked：按 chunked 读取，回复后关闭连接 | — |
| 头部 | handler 未设置时自动添加 `Date`；服务端写的帧格式相关头会替换 handler 设置的同名头 | — |

### TLS 0-RTT

恢复 TLS 1.3 会话的客户端可以把头几个请求随第一个飞行一起发出（RFC 8446 第 2.3 节），比握手早一个往返。
`Config.EarlyData` 打开 HTTP/1 服务端的这个能力，也通过 `tls.EarlyDataOptIn` 打开包装它的 `tls.Handler`。
TLS 层一张票据只接受一次 early data，总量以 `tls.Handler.MaxEarlyData`（默认 16KiB）为限，不接受的部分按
RFC 8446 4.2.10 跳过，和 HTTP/3 一样。

这样到达的请求带 `Early-Data: 1`（RFC 8470），只有方法是 GET、HEAD、OPTIONS、TRACE 时才服务：这些方法
不改变任何东西，而重放其他方法可能改变。其他方法回 425 Too Early 并关闭连接，客户端在握手结束后重发；
`Config.EarlyDataUnsafe` 让所有方法都交给 handler，由它自己检查这个头。客户端一侧是
`ClientConfig.EarlyData`：配合 `SessionCache`，只把安全方法且没有 body 的请求作为 early data 发出，收到
425 的请求在握手结束后重发。只对不会协商成 HTTP/2 的连接生效（`DisableHTTP2`，或
`TLSConfig.NextProtos` 不含 `h2`），因为 HTTP/2 的帧格式要等 ALPN。

### 响应帧格式的选择

handler 用 `Write`/`WriteHeader` 而不是 `WriteResponse` 写响应时，服务端这样选择帧格式：

1. handler 设置了 `Content-Length`：按该长度发送，超出的写入返回
   `http.ErrContentLength`；写得不够时关闭连接。
2. 否则，body 不超过 4KB 且在 handler 返回前写完：用实际长度作 `Content-Length`。
3. 否则，HTTP/1.1：chunked，trailer 也靠它携带。
4. 否则，HTTP/1.0：body 以关闭连接结束。

`Flush` 发送响应头和缓存的数据，并立即交给 socket；不调用时，engine 会把一轮读取中
产生的输出合并，等 handler 返回后一次写出（见 `Connection.Flush`）。

### Transfer coding

chunked 不是唯一会被解码的 transfer coding（RFC 9112 第 7 节）。chunked 之上再加 gzip、
它的旧名字 x-gzip、或 deflate 编码的 body，在服务端和客户端都会被解码。HTTP 里的 deflate
本意是 zlib；有些发送方一直在这个名字下发裸 DEFLATE，这里和浏览器一样按头部区分。请求的
coding 必须以 chunked 结尾，由它定界；响应若以其他 coding 结尾，则以关闭连接定界。

编码过的 body 随读取、或随 `OnBody` 的交付才解码，不会跑到读取者前面：内存里等待的始终是
编码后的 body，它和普通 body 一样给对端施加背压。所以 `MaxBodyBytes`、
`MaxStreamedBodyBytes`、`MaxResponseBodyBytes` 既约束收到的字节，也约束解码后的字节：
一个很小的 body 解压后超过上限时，流式请求得到 `ErrBodyTooLarge`、整体缓存的请求回复 413、
响应得到 `ErrResponseBodyTooLarge`，而不会先把它缓冲下来。解不开的 body 同样失败：整体缓存
的请求回复 400，流式请求的 body 返回 `ErrMalformed`，响应返回 `ErrMalformedResponse`。
`Content-Encoding` 归调用方处理，与 `net/http` 一致：只去掉 transfer coding，头部所指的
表示原样交出。

`compress`（LZW）不解码，`br` 和 chunked 之外的多个 coding 也不解码：`compress/lzw` 的
code 最大 12 位，而 `compress(1)` 会增长到 16 位，解不了流的开头几 KB 之后的部分。请求
带这些 coding 返回 501，响应带这些 coding 则失败。

### CONNECT 隧道

`Context.Connect(header, handler)` 接受传统的 `CONNECT host:port`（RFC 9110 第 9.3.6 节）：
HTTP/1.x 上先回 200，然后把整条连接交给 `TunnelHandler`；HTTP/2 和 HTTP/3 上只切换请求
自己的 stream，受该 stream 的流控约束。目标地址在 `Request.Host`，如何连到目标由 handler
负责。`ConnectProxy` 是现成的实现：拨号 `Request.Host`、用 `Connect` 接受隧道、双向转发并
带背压，还可以限制排队字节、空闲超时和谁可以连接；不是这种 CONNECT 的请求交给它的 `Next`。
注意 fib 客户端没有发起 CONNECT 隧道的 API。

### 保持响应不结束，以及 body 回调

响应由引用计数持有。处理一个请求先占一个引用，handler 返回时还回去——所以「回复完就
返回」的 handler 什么都不用做：外层替它结束响应并交给连接。要稍后再回复的 handler 用
`Context.Retain()` 多占一个、用 `Context.Release()` 还回去，最后一个引用释放时（不管在
哪个 goroutine）响应才被写回。

HTTP/1 连接上，被持有的请求会挡住排在它后面的流水线请求，等它释放之后才解析和处理，
所以响应顺序不变。HTTP/2 和 HTTP/3 上一个 stream 不挡别的 stream。

`Context.OnBody` 用回调的方式接收请求 body，而不是从 `Request.Body` 读——和 websocket
handler 的 `OnFrame` 按帧拿到消息是一个路子。`Context.BodyComplete()` 判断 body 是否
已经全部到达：body 已完整时可以直接从 `Request.Body` 读，只有还在路上时才用回调接收：

```go
func(c *fibhttp.Context) {
	f, _ := os.Create("upload.bin")
	c.OnBody(func(data []byte, fin bool, err error) {
		if err != nil {         // 连接断了，或者 body 出错了
			f.Close()
			os.Remove("upload.bin")
			return
		}
		f.Write(data)
		if fin {
			f.Close()
			c.Respond(200, "text/plain", []byte("stored"))
		}
	})
}
```

- `OnBody` 不阻塞，也从不在自身内部调用回调：它只登记回调然后返回。已经到达的 body
  （`BodyComplete` 为 true 时就是全部）在 handler 返回之后、在运行 handler 的那个
  goroutine 上交给回调。框架从不为回调单独启动 goroutine：之后到达的数据随读取交付——HTTP/1
  就在读这条连接的 worker 上；HTTP/2 和 HTTP/3 的读取者要服务连接上的所有 stream，回调不能在
  那里执行，所以交给 engine 的 handler 协程池（`fib.Engine.HandlerPool`）；handler 返回之后
  才调用 `OnBody` 时已经到达的部分，不会再有读来带它，也交给 handler 协程池。
- `OnBody` 会自动 `Retain` 请求，回调的最后一次调用返回之后，框架自动 `Release`，回调在
  `fin` 时写的响应随即发出，handler 里不需要 `Retain`/`Release`。要在别处回复的 handler，
  在回调里自己再 `Retain` 一次，回复后 `Release`。
- `fin` 标记最后一次回调；`err` 表示这个 body 不会有后续了，同样是最后一次回调，且只来
  一次。`data` 只在回调期间有效，需要保留先复制。
- 回调一次一个、保持顺序，所以任意大的 body 都能处理，既不需要 handler 自己的 goroutine，也不会被缓存下来。给对端限速的就是回调本身。
- body 只有在流式交付时才会分多次到达（由 `StreamRequestBody` 决定）；handler
  运行前就收全的 body 会在一次 `fin` 为 true 的回调里全部给出。
- `OnBody` 接管之后 `Request.Body` 不能再读。关闭它会让回调以 `ErrBodyAbandoned` 结束、
  剩下的 body 被丢弃；想不读 body 直接拒绝上传的 handler，直接回复、不调用 `OnBody`。

### 请求在响应之前就结束了

连接断开、读超时、body 收不完，都会让一个 handler 可能还在处理的请求提前结束。这时请求
被取消：不会再写出任何东西，它上面剩余的引用全部作废，handler 只会被告知一次——通过
`OnBody` 的 `err`，以及 `Context.OnCancel`。两者都在 engine 的 handler 协程池上执行而不是
连接的一轮里，所以 handler 的清理逻辑拖不住服务端。想主动查的话 `Context.Err()` 给出同样的
原因。

这对原地阻塞的 handler（等数据库、等上游服务）同样成立，不只限于 `Retain` 了请求的
handler。这样的 handler 占着连接的这一轮，而读到对端断开本该由这一轮来做，所以 engine 的
事件循环一看到对端断开，就通过 `fib.HangupHandler` 提前告知服务端：请求在 handler 还在
等待时就被取消，连接随之关闭，因为排在它后面的流水线请求不能顶替它被回复。发完请求就
半关闭写端的客户端被视为已经离开，与 nginx 的处理一致，`net/http` 也会因此取消请求的
context：如果 FIN 到达时 handler 还在处理，它就收不到响应。这是 epoll 和 kqueue 后端才有的能力；Windows 只在两轮
之间才挂读，原地阻塞的 handler 要等到返回才会知道，而那时响应已经发出去了。

`Request.Context()` 不会被取消，这样请求不必各自带一个 context。只通过 `context.Context`
得知请求结束的代码（`database/sql`、`httputil.ReverseProxy`、gRPC 客户端等）改用
`Context.Context()`，或者带上它的请求 `c.Request.WithContext(c.Context())`。它在第一次被调用时
才创建；请求被取消时它随之取消，`context.Cause` 就是 `OnCancel` 收到的原因；否则在请求结束时
取消：handler 已返回、每个 `Retain` 都已 `Release`，与 `net/http` 在 `ServeHTTP` 返回后取消请求
context 一致。它不引用 `Context` 本身，所以即使 Reuse 选项把 `Context` 回收去服务别的请求，
它也可以比请求活得久。

从这里开始一切都是幂等的：已经结束的请求上再调 `Retain`、`Release`、`Finish` 都是空操作，
清理代码可以放心调用而不必先判断；往上面写数据会返回那个原因，而不会把连接写坏。

### 读超时

`Config.ReadHeaderTimeout`、`Config.ReadTimeout`、`Config.IdleTimeout` 就是
`net/http.Server` 的那三个，回退规则也一样：`ReadHeaderTimeout` 为 0 时用
`ReadTimeout`，`IdleTimeout` 同理，全为 0 表示不限。和 `net/http` 不同，`DefaultConfig`
会设置它们：`ReadHeaderTimeout` 10 秒（`DefaultReadHeaderTimeout`）、`ReadBodyTimeout`
5 分钟（`DefaultReadBodyTimeout`）、`IdleTimeout` 2 分钟（`DefaultIdleTimeout`）；
`ReadTimeout` 保持 0，因为 header 和 body 的超时已经把整个请求约束住了。上传更久的 server
调大 `ReadBodyTimeout`；不经 `DefaultConfig` 构造的 `Config` 没有这些超时。超时的连接会被关闭，
`OnClose` 收到 `os.ErrDeadlineExceeded`。

用哪一个取决于连接在等什么：

| 在等 | 受限于 | 从什么时候算 |
| --- | --- | --- |
| 第一个请求，或 keep-alive 连接上的下一个请求 | `IdleTimeout` | 上一个响应之后，或连接建立时 |
| header 收完 | `ReadHeaderTimeout` | 该请求的第一个字节 |
| body 收完（流式与否都一样） | `ReadTimeout` | 该请求的第一个字节 |
| ……同时还有 | `ReadBodyTimeout` | 该请求 header 收完时 |
| 不在等——请求已经收全 | 不限 | — |

最后一行是和 `net/http` 的区别：`net/http` 把读 deadline 设在整个请求期间，handler
只有在读的时候才会察觉；这里 deadline 是直接关连接的，所以请求收全之后就会撤掉——
比 `ReadTimeout` 慢的 handler 仍然能把响应写完，`ReadTimeout` 约束的是请求到达的时间
而不是处理它花的时间。流式 body 在 handler 运行期间仍在到达，所以它确实受
`ReadTimeout` 约束，这正是慢上传占不住连接的原因。`ReadBodyTimeout` 只约束 body，
和 `ReadTimeout` 同时生效，先到期的那个起作用。

客户端已经离开、body 却没被读的连接，也是靠它们关掉的：handler 不再读 body，或用
`Context.HoldBody` 暂停它，待读的部分达到 `StreamRequestBodyBuffer` 后连接就停止读；
不读的连接收不到 FIN 的结果（剩下的 body 可能还排在 FIN 前面）。读 deadline 不依赖读，
到期就关闭这样的连接，`net/http` 的 server 也是靠它自己的读超时。暂停本身也有上限：
`Config.MaxBodyHold`（默认 `DefaultMaxBodyHold`，一分钟；小于 0 不限）——单次暂停超过它，
body 以 `ErrBodyHeldTooLong` 失败，`OnBody` 回调会收到。

HTTP/2 不会为某一个流停止读连接，所以 `ReadTimeout` 和 `ReadBodyTimeout` 对每个请求的
body 单独计时，从它的 HEADERS 算起：流式交给 handler 的 body 以 `os.ErrDeadlineExceeded`
失败；handler 运行前还在收集的 body，其流被 reset；连接继续服务其他流。`http3.Config`
为 HTTP/3 提供同样的三个设置。

通过 preface 或 `h2c` 升级变成 HTTP/2 的连接会被解除这些超时：HTTP/2 有自己的连接
状态，HTTP/1 解析器设下的 deadline 之后没人会去刷新它。HTTP/2 用自己的方式应用它们：
`IdleTimeout`（未设时用 `ReadTimeout`）让一段时间内没有任何流的连接发出 GOAWAY
`NO_ERROR` 后关闭；读超时对每个请求的 body 分别计时（见下文）。

### 流式请求 body

`Config.StreamRequestBody` 默认关闭：请求完整收齐（包括 body）之后才调用 handler，
`Request.Body` 里就是完整的 body。开启后，body 一开始到达就调用 handler——带
`Content-Length` 的在 header 收齐时，chunked 的在收到一部分 body 时——body 以
`*BodyStream` 的形式出现在 `Request.Body` 里。`Context.RequestBody()` 返回它，body
已经收全时返回 nil；`Context.BodyComplete()` 判断 body 是否已经全部到达。同一个开关
也让 HTTP/2 的 body 流式交付，只是由流控而不是暂停读来限速（见
[`http2.zh-CN.md`](http2.zh-CN.md)）；HTTP/3 在 `http3.Config` 里有同样的开关。

`Context.HoldBody(true)` 暂停 `OnBody` 的交付，但不等待：到达的部分留在 server 的
缓冲里，像没读的 body 一样给客户端限速；`HoldBody(false)` 在 handler 池上恢复交付。
把 body 转交给较慢消费者的回调，在消费者跟不上时暂停它。代理就是这样边收边转发上传、
而不让任何东西等待的：handler 用 `fibhttp.BodyWriter` 作为上游请求的 body 发出请求，
`Retain` 请求后返回；`OnBody` 回调把每段写进 writer，client 在上游连接有空间时发送；
超过 writer 的上限时回调暂停 body，由 `BodyWriter.OnDrain` 恢复（见 `BodyWriter` 的
示例和 [`fib-examples/gateway`](https://github.com/lesismal/fib-examples/tree/main/gateway)）。

`Config.StreamRequestBodyThreshold` 在开启流式时让较小的 body 仍然整体缓存：只有
`Content-Length` 超过它的 body，以及已经发来超过这么多字节的 chunked body 才流式交付。
为 0 时所有 body 都流式交付；不开 `StreamRequestBody` 时它不起作用。

```go
config := fibhttp.DefaultConfig()
config.StreamRequestBody = true            // body 边收边交给 handler
config.StreamRequestBodyThreshold = 1 << 20 // 不超过 1MB 的仍然整体缓存
```

整体缓存的 body 放在池化缓冲里，handler 用完这个请求后由 server 收回：handler 已经
返回，并且释放了每一个 `Retain`（即使 Retain 持续到连接断开之后）。对不做 Retain 的
handler 来说就是它返回的时刻，`net/http` 也是在这个时刻关闭请求 body 的。之后再读
会返回 `ErrBodyReleased`；handler 如果之后还要用这些字节，应自己保留一份副本（比如
`io.ReadAll` 得到的结果）。

**读取不阻塞。** handler 和别的 handler 一样跑在读这条连接的 worker 上，worker 去等对端
要么是在等自己、要么是白占着，
所以 `Read` 只给出已经到达的部分：

| Read 返回 | 含义 |
| --- | --- |
| `n > 0` | 这些字节已经到了 |
| `io.EOF` | 整个 body 已经读完 |
| `ErrWouldBlock` | 现在一个字节都没有，后续还会来 |
| `ErrBodyAbandoned` | body 已经被别的东西接管：`Close`、`OnBody`，或者 handler 既没有 Retain 也没有调用 `OnBody` 就返回了 |
| 其它错误 | 后续不会再来了——连接断了、超过 `MaxStreamedBodyBytes`、帧格式错误 |

`ErrWouldBlock` 就是 `fib.ErrWouldBlock`，和 `Connection.Read` 用的是同一个，判断哪个
都行。body 有可能在 handler 运行时就已经全在了（和 header 在同一次读里到达），这种情况
下 handler 一路读到 `io.EOF`、直接回复即可。

遇到 `ErrWouldBlock` 又想要后续的 handler，就用 `Context.OnBody` 接管剩下的部分（它会
自动保持请求不结束），见[保持响应不结束](#保持响应不结束以及-body-回调)。交接不会丢字节：`OnBody`
拿到的正好从上一次 `Read` 停下的地方开始。

```go
func(c *fibhttp.Context) {
	n, err := c.Request.Body.Read(buf)          // 先拿已经到的
	switch {
	case errors.Is(err, io.EOF):        // 就这么多，全了
		answer(c)
	case errors.Is(err, fibhttp.ErrWouldBlock): // 后面还有
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
  413 拒绝。handler 或中间件可以用 `Context.LimitBody(n)` 给单个请求的 body 设更低的上限：
  已知超过 `n` 的 body 立即以 413 拒绝，长度未知的 body 超过 `n` 时像超过
  `MaxStreamedBodyBytes` 一样被结束；这样结束、又没有被回复的请求由服务端回复 413。
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

### 复用请求对象

`Config.ReuseRequests`、`ReuseHeaders`、`ReuseURLs`、`ReuseContexts` 在 `DefaultConfig`
中默认全部开启：HTTP/1 请求的 `*http.Request`、它的 `Header`、`URL` 以及 `*Context` 会在
响应结束后回收复用，而不是每个请求都留一整套对象给 GC。请求速率很高时，回收这些对象正是
server 的瓶颈：go-http-benchmark 的流水线测试里，3 核 1 万连接从每秒 177–181 万响应
提升到 199 万（即客户端请求的上限），CPU 还更少。核数越多影响越大，因为所有核共用一个堆：
HttpArena 的 HTTP/1 测试在 64 核上，不复用时 baseline 只有每秒 123 万请求、pipelined
570 万，只用到 36 和 27 个核，其余都在等 runtime 的堆锁；复用后分别是 163 万和 1500 万。
把某个选项设为 false 即可让对应对象交回 GC。

对 handler 的要求与 fasthttp 对 `RequestCtx` 的要求相同：被复用的对象在 handler 用完
这个请求之前都属于它，即 handler 已经返回，并且释放了它取得的每一个 `Retain`。被 Retain
的请求即使响应早已发出、甚至连接已经断开（`OnCancel` 会通知），也要到最后一次 `Release`
才失效，不论这需要多久。之后它会交给下一个请求——可能在这条连接上，也可能在别的连接上。所以 handler 如果持有得
更久，或者交给一个活得比响应更久的 goroutine，读写的就是别的请求的数据。需要的内容请
自己保留副本，比如用 `http.Request.Clone`、`http.Header.Clone`。等待下一个请求的
`Context` 处于已结束状态，误调用的 `Retain`、`Release`、`Respond` 不会产生任何作用。

Fiber、Gin、Echo 对各自的 context 也是这条规则。为 `net/http` 写的 handler 如果在返回后
还持有 `*http.Request`（比如交给一个没有 `Retain` 的 goroutine），请关闭 `ReuseRequests`，
或者先 `c.Request.Clone(ctx)`。

每个选项只管自己那个对象，所以比如只持有 `Context` 的 handler 可以复用其余对象。流式
body 的请求，以及不在 server 自行解析范围内、改由 `net/http` 解析的请求，无论选项如何
都不复用 `Request`、`Header`、`URL`。

### 客户端的请求头与响应头

HTTP/1 客户端像 server 读请求、写响应那样，自己写请求头、读响应头，不再经过 `net/http` 的
`Request.Write` 和 `ReadResponse`：对代理的请求来说，这两者的开销比客户端其余部分加起来还多。
写出和读到的内容与 `net/http` 逐字节、逐字段相同，由对照 `net/http` 的测试和模糊测试检查；常见形式
之外的（设置了 `Close` 或带 `httptrace` 跟踪的请求、需要清理的 host、不是 token 的方法、跨行折叠的
响应头字段、声明了 trailer、多个 `Content-Length`）仍像以前一样交给 `net/http`。没有 trailer 的整个
分块 body 也由客户端解码，`net/http` 的分块读取器拒绝的，它同样拒绝；chunked 之上再用
gzip 或 deflate 编码的 body 也一并解码（见 [Transfer coding](#transfer-coding)）。写一个代理请求只分配一次，
读一个普通响应头五次，经 `net/http` 分别是八次和十三次。

### 客户端与 `Expect: 100-continue`

客户端带 body 和 `Expect: 100-continue` 的请求先单独发送请求头，等服务端回 100 Continue，
或者 `ClientConfig.ExpectContinueTimeout` 到期后（`DefaultClientConfig` 中为 1 秒，与
`net/http` 默认 transport 相同；为零时 body 随请求头一起发送）再发送 body，因为不认识这个
期望的服务端永远不会回 100。如果先收到最终响应，它就是答复：body 不会发送，连接的请求头
声明了 body 却没有跟上，所以关闭而不复用。整体读入的 body 和流式 body 都会等待。HTTP/2 和
HTTP/3 客户端对各自的 stream 做同样的处理。

### 不拷贝地读取 body 与 query

`Context.Body()` 返回 handler 运行前已整体读完的请求 body（不走流式的 body 都是如此），
即 server 自己的 buffer；`io.ReadAll(c.Request.Body)` 则要用一个逐步增长的 buffer 再拷贝一遍。
这些字节在 body 有效期内（直到响应结束）属于 handler，不能修改，也不能保留到之后。
`Respond` 和 `Write` 都会拷贝传入的数据，所以 echo 可以直接
`c.Respond(200, "application/octet-stream", c.Body())`。流式 body 时 `Body` 返回 nil。

`Context.Query(name)` 返回某个 query 参数的第一个值，解码方式与 `c.Request.URL.Query().Get(name)`
相同，但不像 `Query` 那样每次都构建 map；不需要解码的值直接是原始 query 的子串，零分配。

### 零拷贝发送文件

`Connection.SendFile(f, offset, count)` 把文件的一段排进发送队列，与连接上其他发送保持
顺序。连接会复制描述符，调用方可以立即关闭自己的文件。Linux 和 macOS 上数据由
`sendfile(2)` 从文件直接发到 socket；socket 写满时剩余部分等待下一次可写事件，所以文件
只按对端的接收速度读取。Linux 上可以用 `strace -e sendfile` 观察到，例如
`sendfile(10, 12, [0] => [2673856], 3145851) = 2673856`，socket 排空后再发送剩余部分。
Windows 上用 overlapped 的 `TransmitFile` 发送，等排在它前面的数据发完再投递，它的完成
通知就相当于可写事件；被拒绝的 socket（Unix socket）改为逐块读取。Windows 客户端版本
全系统同时最多执行两个 `TransmitFile`，其余排队，Go 的 `net` 包同样如此；服务器版本没有
这个限制。

TLS 上（`tls.NewServer` 或 `tls.Dial`，握手完成、连接的记录由 fib 自己的记录层保护之后：
TLS 1.3 或 1.2 的 AES-GCM，TLS 1.2 或 1.1 的 AES-CBC）：

- **Linux 有内核 TLS 时**，连接在 AES-GCM 套件上发送的第一个文件会把此后所有发送的加密
  交给内核（`TCP_ULP` "tls" 与 `TLS_TX`，带上流量密钥、IV 和下一个序列号），文件经
  `sendfile(2)` 从页缓存发出、由内核加密。切换只在用户态加密的数据都已离开队列时进行
  （`Connection.ControlIdle`）；在其他时刻发送的文件仍按下面的方式在用户态加密，下一个
  文件再尝试切换。切换之后发送的是交给内核加密的明文，`close_notify` 等 alert 以各自类型
  的记录发出（`Connection.SendRawRecord`）。对端要求的 KeyUpdate 在下一次队列为空时经由
  内核发出，内核随后换用下一代密钥；不能换密钥的内核（6.14 之前的 Linux）则改为关闭连接。
  读取仍在用户态。内核没有 `tls` 模块、或进程无权加载时只记录一次，之后不再尝试。
  `tls.Handler.NoKernelTLS` 关闭这一切换。
- **其他情况**（macOS、BSD、Windows、AES-CBC 套件，或关闭了、缺少内核 TLS）连接把文件
  排进队列，由 flush 随 socket 排空每次读取 64KB，封装成四条满的记录（`fib.FileSealer`）
  再写出。文件入队时就为它预留好要占用的序列号，之后的发送立刻在其后加密，线上顺序保持
  不变。无论文件多大都只占一个加密块的内存；对端读得慢时与明文 `sendfile` 一样被拖住：
  剩余部分留在文件里，计入积压，`NotifyOutput`、`OnWritable` 和写超时都能看到。

无论哪种方式，文件比声明的范围短都会关闭连接，因为已经向对端承诺了整个范围。

`Context.ReadFrom` 对普通文件、以及包着文件的 `*io.LimitedReader`（`http.ServeContent`
传入的就是它）使用 `SendFile`。小于 16KB 的范围直接拷贝，因为这比多出的系统调用更省。

## 当前限制

- **请求 body 默认整体缓存**：body 收完后才调用 handler，受 `MaxBodyBytes` 限制。设置
  `Config.StreamRequestBody` 后不再受此限制，见
  [流式请求 body](#流式请求-body)。
- **客户端响应 body 整体缓存**：受 `MaxResponseBodyBytes` 限制，客户端没有流式下载。
- **客户端不做 pipelining**：一个 HTTP/1 连接同时只有一个请求。
- **响应在最后一个引用释放时结束**：什么都不 Retain 的 handler 就是它自己返回的时候，
  与 `net/http` 一致。要稍后回复、或者要在其他 goroutine 里接着写的 handler 先
  `Retain`，见[保持响应不结束](#保持响应不结束以及-body-回调)。
- **读取同样不阻塞**：handler 跑在 worker 上，等对端只会白占着它，所以流式 body 的读取返回
  `ErrWouldBlock` 而不是等对端；剩下的部分用 `OnBody` 接管。
- **写入不阻塞**：handler 写得比对端读得快时，差额排队在内存中（连接的读取会暂停，但
  handler 本身不会被阻塞）。`SendFile` 发送的文件例外：它只随 socket 的排空读取。
- **fib 记录层未实现的套件**（ChaCha20-Poly1305 以及其他让连接留在 `crypto/tls` 上的
  套件）无法随排空发送文件：在那里 `Connection.SendFile` 在返回前读出并加密整段文件，
  整段都会排队在内存中；`Context.SendFile` 会先问 `Connection.QueuesFiles`，改为分块读取。
- **HTTP/2 和 HTTP/3** 同样接受这些 `ResponseWriter` 调用，并以同样的方式用 DATA 帧
  流式输出；在那里，超过客户端流控的写入会等待客户端（见各自的文档）。
- **transfer coding**：gzip、x-gzip、deflate 会被解码，见
  [Transfer coding](#transfer-coding)；`compress`、`br` 以及 chunked 之外的多个 coding
  不解码。解码后的长度同样受 `MaxBodyBytes`、`MaxStreamedBodyBytes`、`MaxResponseBodyBytes`
  限制。
- **除 h2c 外，`Upgrade` 由 handler 处理**：`Context.Upgrade` 回复 101 并把连接交给
  `TunnelHandler`，`websocket.ServerHandler.Upgrade` 就是这样在 HTTP handler 里提供
  WebSocket 的。指向 host:port 的传统 `CONNECT` 也是隧道：`Context.Connect` 回复 200 后
  交给 `TunnelHandler`，`ConnectProxy` 负责拨号目标并双向转发；fib 客户端没有主动发起
  CONNECT 隧道的 API。

## 待优化

- 留在 `crypto/tls` 上的连接（ChaCha20-Poly1305 套件）随 socket 排空发送文件，这需要
  fib 自己的记录层实现这些套件。
- `compress`（LZW）transfer coding 的解码器。`compress/lzw` 读的是 GIF、TIFF、PDF 那种
  12 位 code 的变体，而 `compress(1)` 的 code 会增长到 16 位，所以它解不了这类流开头几 KB
  之后的部分；在有自研解码器之前，这个 coding 会被拒绝。

## 一致性测试

`http/http1_conformance_test.go`（所有测试都以 `TestHTTP1Conformance` 开头）用非 fib
的对端检查 fib 的服务端和客户端：

- **fib 服务端**对 Go `net/http` 客户端、原始 TCP 连接（用来发送 `net/http` 不会发出的
  请求：带或不带 keep-alive 的 HTTP/1.0、pipelining、格式错误或帧格式冲突的请求、chunk
  扩展），以及 **curl**（`--http1.0`、keep-alive 复用、chunked 上传、sendfile 路径的
  下载、Range、原始 chunked trailer，以及 HTTPS 下载 `ServeFile`、`io.Copy` 和
  `Context.SendFile`，同一连接和各自一个连接，有内核 TLS 和没有内核 TLS）。
- **fib 客户端**对 Go `net/http` 服务端（`httptest`）和原始服务端（HTTP/1.0 keep-alive
  和以关闭连接结束的响应、格式错误的响应）。
- **fib 客户端对 fib 服务端**，包括 HTTP/1.0 和 sendfile。
- **客户端的头部**（`http/client_codec_test.go`）：请求头、响应头和整个分块 body，通过用例表和模糊测试
  与 `net/http` 的结果对照。
- **持有响应与 body 回调**（`http/retain_test.go`）：在别的 goroutine 里回复、被持有
  的请求挡住后面的流水线请求、嵌套持有、`OnBody` 对缓存 body 和流式 body、`OnBody` 只登记
  不回调、`BodyComplete`、handler 返回后才调用 `OnBody`、最后一次 body 回调之后自动释放、
  带 trailer 的 chunked、提前回复并关闭 body、客户端上传到一半跑掉时 `OnBody` 和
  `OnCancel` 各只收到一次、原地阻塞的 handler 在等待期间就能通过 `OnCancel` 和 `Err`
  得知客户端已断开且其后流水线的请求不会被回复、读超时触发 `OnCancel`、body 超限、响应写完之后再 Retain、同一
  个 handler 跑在 HTTP/2 上，以及把以上全部并发跑一遍并检查没有残留的压力测试。
- **读超时**（`http/timeout_test.go`）：header 和 body 收到一半就不来了、流式 body
  收到一半就不来了、比 `ReadTimeout` 慢的 handler 仍然能回复、空闲连接被关闭、每个请求
  都会刷新空闲超时、一个字节都不发的连接、没配超时的服务端、变成 HTTP/2 的连接被解除
  超时，以及超时传到 `OnClose`。`netconn_test.go` 检查 `Connection` 自己的 deadline
  和其余 `net.Conn` 方法。
- **流式请求 body 的非阻塞读取**：`Read` 返回 `ErrWouldBlock` 后交给 `OnBody` 不丢字节、
  body 已经全在时一路读到 `io.EOF`、读和 `OnBody` 都能触发 100-continue，以及 64 个上传
  同时挂起而不多出一条 goroutine。
- **流式请求 body**（`http/body_test.go`）：handler 在 body 结束前就运行、小于阈值的
  body 仍然整体缓存、一块一块发来的带 trailer 的 chunked 上传、handler 落后时暂停读、
  没读完的 body 的丢弃与关闭、惰性与被拒绝的 100-continue、`MaxStreamedBodyBytes`、
  截断的上传、流式请求之后的 pipelining、handler panic、用 `net/http` 客户端上传，以及
  逐字节喂给增量 chunked 解码器。`hold_reads_test.go` 单独检查
  `Connection.HoldReads`。
- **body 矩阵**（`http/body_matrix_test.go`，HTTP/3 在 `http3/body_matrix_test.go`）：
  协议（HTTP/1.1、HTTP/2 各自明文和 TLS；HTTP/3）× body 格式（无 body、带长度、chunked
  或 HTTP/2/3 不带长度的 DATA、带 trailer、`Expect: 100-continue`）× 大小（一次读得完、
  要读多次）× 发送方式（一次写完，或分片并间隔发送，切点落在 chunk 行中间、帧中间）×
  handler 接收方式（直接读、OnBody、按 `BodyComplete` 二选一、handler 返回后在别的
  goroutine 调 OnBody）× 服务端配置（整体缓存、全部流式、超过阈值才流式）的全部组合，
  每个连接上后面再跟一个请求。每个组合都检查每个字节、trailer、是否流式、缓存的 body
  是否完整、回调没有在 OnBody 内部或 handler 返回前执行、最后一次回调恰好一次、以及从
  回调里写的响应确实发出。另外还有：各协议上 body 发到一半被切断、body 上限落在哪、
  HTTP/2 和 HTTP/3 上流控窗口对慢 handler 的限速。
- `sendfile_test.go` 在 TCP 和 Unix socket 上检查 `Connection.SendFile`：在 handler
  中和其他 goroutine 中调用、对端读得慢、文件比声明的范围短；`sendfile_sealed_test.go`
  检查随排空加密的文件（对端不读时内存不超过一块、与前后发送的顺序、文件过短）和
  `ControlIdle`；`sendfile_windows_test.go` 检查 `TransmitFile`、文件过短和拒绝它的 socket。
- `tls/file_test.go` 用 `crypto/tls` 客户端检查 TLS 上的 `SendFile`，覆盖 TLS 1.3、
  TLS 1.2 的 AES-GCM 与 AES-CBC，有内核 TLS 和没有内核 TLS：在 handler 内外调用、前后的
  发送、对端读得慢并检查堆、文件过短、之后的 `close_notify`、留在 `crypto/tls` 上的连接，
  以及经内核 TLS 的 KeyUpdate。

对端只有标准库和 curl 可执行文件，Go module 不增加任何依赖。CI 中单独的
**HTTP/1.x conformance** job 在 Linux、macOS、Windows 上运行这些测试，并设置
`FIB_REQUIRE_CURL=1`：缺少 curl 时直接失败而不是跳过 curl 用例。本地运行：

```sh
go test -race -run 'TestHTTP1Conformance|TestSendFile|TestSendableFileOf|TestResponseWriter|TestStreamRequestBody|TestChunkedDecoder|TestHoldReads|TestServerRead|TestServerIdle|TestServerTimeout|TestServerWithoutTimeouts|TestReadDeadline|TestWriteDeadline|TestZeroDeadline|TestConnectionAddresses|TestRetain|TestOnBody|TestBodyComplete|TestBodyMatrix|TestOnCancel|TestTransmitFile|TestControlIdle|TestKernelTLS' -v . ./http/ ./http3/ ./tls/
```

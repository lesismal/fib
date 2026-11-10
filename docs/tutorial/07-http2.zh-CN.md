# HTTP/2

[English](07-http2.md) | [简体中文](07-http2.zh-CN.md)

`fib/http` 同时支持 HTTP/2 与 HTTP/1.x：同一个 handler、同一个绑定即可；明文下通过 prior knowledge 或 `Upgrade: h2c`，TLS 下通过 ALPN。HTTP/2 是 fib 执行模型发生变化的地方——一条连接上多条流共享，handler 跑在协程池里，HTTP/1 的「一次一个请求」被流量控制取代。本章展示两种启动 HTTP/2 服务的方式、多路复用与 handler 池的行为，以及 fib 实现的 HTTP/2 特性（push、1xx、trailer、流式）及其限制；协议参考见 [HTTP/2](../http2.zh-CN.md)。

## 模型

一条 HTTP/2 连接有一个 reader 和许多 stream。引擎的 worker 负责分帧、解码 HPACK/QPACK header；每个完整请求被提交到引擎的 handler 池——按引擎 `Config.Name` 命名的共享 streams 池，默认叫 `fib-streams`——handler 在那里运行。因此慢 handler 只拖住自己那条流，客户端同时打开的请求是并发处理的。`Config.MaxConcurrentStreams`（默认 250）限制每条连接的流数，`Config.StreamPool.MaxConcurrentHandlers` 可以限制一条连接同时运行多少个请求的 handler；第 N 个请求会直接在 reader 上运行，reader 在它返回前不再读取，所以这个限制由对端流量控制来承担，服务端不排队。响应以 DATA 帧发出，受对端流量控制窗口约束。绝不能直接通过 `Context.Conn` 写字节：那会破坏分帧，所有输出都必须走 `Context`。

## h2c：不带 TLS 的 HTTP/2

一个明文绑定同时服务 HTTP/1.x 和 h2c——以 HTTP/2 preface 开头的连接按 HTTP/2 处理，带 `Upgrade: h2c` 的 HTTP/1.1 请求会被升级。handler 完全不用改。

```go
package main

import (
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.String(stdhttp.StatusOK, "hello over %s\n", c.Request.Proto)
	})
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080", // h1 and h2c on the same port
		Handler: fibhttp.NewHandler(app),
	})
	if err != nil {
		panic(err)
	}
	defer engine.Close()

	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

这类请求的 `c.Request.Proto` 是 `HTTP/2.0`。客户端启动 h2c 的两种方式在线上并不相同：

```sh
curl --http2-prior-knowledge http://127.0.0.1:8080/   # the preface; preferred
curl --http2 http://127.0.0.1:8080/                   # Upgrade: h2c, deprecated by RFC 9113
```

如果只会收到 HTTP/2 就设 `Config.HTTP2Only`（h2spec 需要它，因为它读不懂 HTTP/1 的错误响应）；只服务 HTTP/1 则设 `Config.DisableHTTP2`。

## TLS 上的 h2

走 TLS 时由 ALPN 选择协议：`fibhttp.ConfigureTLS` 复制一份 `*crypto/tls.Config` 并把 `h2`、`http/1.1` 放进 ALPN 列表，`fibtls.NewServer` 完成握手后把连接交给 HTTP handler。没有协商到 HTTP/2 的客户端由同一个 handler 作答。

```go
package main

import (
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

func main() {
	serverTLS, _, err := tlstest.Configs() // a throwaway certificate for 127.0.0.1
	if err != nil {
		panic(err)
	}
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.String(stdhttp.StatusOK, "hello over %s\n", c.Request.Proto)
	})
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8443",
		Handler: fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), fibhttp.NewHandler(app)),
	})
	if err != nil {
		panic(err)
	}
	defer engine.Close()

	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

```sh
curl -k --http2 https://127.0.0.1:8443/    # ALPN: h2
curl -k --http1.1 https://127.0.0.1:8443/  # ALPN: http/1.1, same handler
```

`tlstest` 只用于示例和测试；真实证书见第 [10](10-tls.zh-CN.md) 章。

## 多路复用与 handler 池

因为 handler 跑在池里，会等待的 handler（数据库、上游服务、channel）只阻塞自己。这让 `Retain`/`Release` 在 HTTP/2 上比 HTTP/1 便宜得多：挂起一个 HTTP/2 请求只暂停一条流，连接继续服务其他流，所以一条连接上可以有几百个异步请求在飞；一个被取消（`OnCancel`）或 reset 的请求只影响自己那条流。每连接的、每服务的限制就是下面两个配置字段，流量控制窗口则是在内存与吞吐之间做取舍。

```go
config := fibhttp.DefaultConfig()
config.MaxConcurrentStreams = 250            // streams the client may open; the default
config.StreamPool.MaxConcurrentHandlers = 16 // at most 16 of one connection's requests at once
config.HTTP2 = fibhttp.HTTP2Config{
	StreamWindow:   4 << 20,  // each stream may have 4MB in flight
	ConnWindow:     32 << 20, // shared by the connection's streams
	AdaptiveWindow: true,     // let the connection grow them to the path's bandwidth-delay product
}
```

窗口对流式 body 很关键：接收窗口只在 handler 消费 body 时才归还，所以快速客户端被流量控制拖慢，而不是被缓冲。同时要记住，一条连接可能持有约 `MaxConcurrentStreams × MaxBodyBytes` 的整体请求 body（默认 250 × 16MB），这就是上传接口要打开 `StreamRequestBody` 的理由。

## 流式 body 与流式响应

`Config.StreamRequestBody` 的含义与 HTTP/1 相同：body 一开始到达 handler 就运行，并通过 `Request.Body`（`*BodyStream`，读取不会等待）或 `Context.OnBody` 接收。区别在于如何给客户端施加背压：HTTP/2 不能为了一条流停止读取整条连接，所以流的接收窗口只在 handler 消费时归还，最多领先一个窗口，同时连接上的其他流继续运行。响应方向相反：`Write` + `Flush` 边产生边发 DATA 帧；handler 若写快于客户端，积压到 64KB 就会等待。

```go
config := fibhttp.DefaultConfig()
config.StreamRequestBody = true
config.StreamRequestBodyThreshold = 1 << 20 // keep bodies up to 1MiB whole
config.MaxStreamedBodyBytes = 64 << 20
```

```go
func events(c *fibhttp.Context) {
	c.Header().Set("Content-Type", "text/event-stream")
	for i := 0; i < 10; i++ {
		if _, err := fmt.Fprintf(c, "data: %d\n\n", i); err != nil {
			return
		}
		c.Flush() // DATA frames as they are produced, paced by the client's window
	}
}
```

不超过 4KB 且没有 flush 的响应仍会带 `content-length` 整体发出；flush 或更大的 body 会变成一串 DATA 帧，以 END_STREAM 或 trailer HEADERS 结束。因此 SSE、长轮询和大文件下载在 HTTP/2 上不用改代码，与第 [06](06-http1.zh-CN.md) 章的 handler 相同。

## Server push

`Context.Push` 在客户端索要之前就把资源承诺给它，并在 `Push` 返回前用自己的流把被 push 的请求交给 handler 处理完。要在「会诱导客户端索要该资源」的响应之前调用；在 HTTP/1、HTTP/3、对被 push 的请求、以及客户端关闭 push 时，它返回 `stdhttp.ErrNotSupported`。

```go
func index(c *fibhttp.Context) {
	if err := c.Push("/style.css", nil); err != nil {
		// stdhttp.ErrNotSupported on HTTP/1 and HTTP/3, or when the client disabled push
	}
	_ = c.HTML(stdhttp.StatusOK, `<link rel="stylesheet" href="/style.css">`)
}
```

两个实际注意点：被 push 的 handler 在调用 `Push` 的 goroutine 上同步执行，慢的 pushed handler 会拖慢父响应；而主流浏览器已经移除 push，fib 和 `net/http` 的 HTTP/2 客户端都发送 `ENABLE_PUSH=0`。预加载请改用 `WriteInterim` 发 103 Early Hints。

## 1xx 中间响应与 trailer

`WriteInterim(status, header)` 在最终响应之前发送一个信息性响应——常见用法是带 `Link` header 的 103 Early Hints——可以调用多次。101 不允许，HTTP/1.0 客户端会得到 `stdhttp.ErrNotSupported`。trailer 与 HTTP/1 一样：`WriteResponse` 配 `Response.Trailer`，或者 `http.ResponseWriter` 方法配 `Trailer` header，最终作为 DATA 帧之后的尾部 HEADERS 到达。

```go
func hints(c *fibhttp.Context) {
	_ = c.WriteInterim(stdhttp.StatusEarlyHints, stdhttp.Header{
		"Link": {`</app.css>; rel=preload; as=style`},
	})
	_ = c.HTML(stdhttp.StatusOK, `<link rel="stylesheet" href="/app.css">`)
}
```

## 限制与 HTTP/2 不做的事

- **客户端不能发起 h2c 升级**：fib 客户端的明文 HTTP/2 只走 prior knowledge（`UnencryptedHTTP2`）；服务端仍接受 `Upgrade: h2c` 以兼容 curl 等工具。TLS 上切换协议只能靠 ALPN。
- **优先级被忽略**：PRIORITY 帧和 RFC 7540 的优先级树只做校验后丢弃（RFC 9113 已废弃它们）；RFC 9218 `priority` header 的 urgency 会用于响应调度。
- **PING 与 SETTINGS 有上限**：客户端重置流或发送控制帧超过预算（`StreamResetBurst`/`StreamResetRate`、`ControlFrameBurst`/`ControlFrameRate`、`MaxContinuationFrames`）时，服务端以 GOAWAY 关闭连接；客户端在 `SettingsTimeout`（默认 10 秒）内不确认 SETTINGS 也会被断开。`Config.PingInterval` 可以加 keep-alive PING，默认关闭。
- **经典 CONNECT 没有隧道**：请求会被解析并交给 handler，但不会建立双向隧道。扩展 CONNECT（RFC 8441）不同——WebSocket 正是通过 `Context.Upgrade` 走 HTTP/2 的（第 [09](09-websocket.zh-CN.md) 章）。
- **内存**：整体请求 body 的上限是每连接 `MaxConcurrentStreams × MaxBodyBytes`；用 `StreamRequestBody` 或 `StreamPool.MaxConcurrentHandlers` 把它压下来。

## 坑与限制

- **绝不要直接写 `Context.Conn`。** HTTP/2 的输出必须走 `Context`；裸字节会破坏帧流并杀死连接。
- **handler 池是共享的。** 阻塞的 handler 占的是池里的 worker，不是连接 reader，但足够多的阻塞 handler 会饿死同名的所有 HTTP/2 与 HTTP/3 服务。把慢活移出池，或用 `StreamPool.MaxConcurrentHandlers` 限流。
- **`Push` 是同步的。** 会等待的 pushed handler 拖慢调用 `Push` 的响应；而且多数客户端已禁用 push。
- **响应 body 写出去后不再属于你。** `Respond`/`WriteResponse` 会一直持有切片到流结束（HTTP/3 还要等对端确认），所以不要改动它；缓冲区要复用就用 `RespondCopy`/`WriteResponseCopy`。
- **流式 body 需要 `StreamRequestBody`。** 否则 body 会整体缓冲，大上传按每条流 `MaxBodyBytes` 计费。
- **流与请求对象会被复用。** 第 [06](06-http1.zh-CN.md) 章的规则不变：不要把 `Request`、`Header`、`URL`、`Context` 留到响应和最后一次 `Release` 之后。
- **h2c 不是浏览器使用的协议。** 浏览器走 TLS 上的 h2；h2c 用于内部服务、明文 gRPC 和一致性测试工具。

## 相关文档

- [HTTP/2](../http2.zh-CN.md) — 协议参考：可配置参数、限制、h2spec、基准测试。
- [HTTP/3](08-http3.zh-CN.md) — 同一个 handler 跑在 QUIC 上，以及多路复用的差异。
- [HTTP/1.x](06-http1.zh-CN.md) — 本章假定的请求/响应 API，细节最全。
- [Handler：同步与异步](03-handlers.zh-CN.md) — `Retain`/`Release`，以及在多路复用连接上为什么便宜。
- [进阶技巧](../hacks.zh-CN.md) — 多路复用、handler 池与 pipeline 技巧的深入讲解。
- [客户端](20-clients.zh-CN.md) — HTTP/2 客户端、连接池与自动重试。

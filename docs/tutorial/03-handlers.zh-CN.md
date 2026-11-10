# Handler：同步与异步

[English](03-handlers.md) | [简体中文](03-handlers.zh-CN.md)

fib 有两级 handler：**连接** handler 处理原生 TCP、UDP 或 Unix 连接上的字节流，**HTTP** handler 处理已经解析好的请求并作答。两者都跑在引擎的工作协程池上，遵守同一条规则——同一连接一次只跑一轮，handler 绝不能阻塞——异步 API 正是为此存在。本章讲这两级 handler、决定它们形态的执行模型，以及 handler 最常做的四件事：同步作答、用 `Retain`/`Release` 稍后作答、流式读请求体、流式写响应。

## 两级 handler

连接 handler 实现 `fib.Handler`：

```go
type Handler interface {
	OnOpen(*Connection)
	OnData(*Connection, []byte)
	OnPriorityData(*Connection, []byte)
	OnClose(*Connection, error)
}
```

`fib.HandlerFuncs` 用你设置的任意几个回调（`Open`、`Data`、`PriorityData`、`Close`）实现它，所以原生协议只需要写用到的部分。下面这个完整的 echo 服务器四个都用了，回调名就是字段名：

```go
package main

import (
	"fmt"

	fib "github.com/lesismal/fib"
)

func main() {
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr: "127.0.0.1:9000",
		Handler: fib.HandlerFuncs{
			Open: func(c *fib.Connection) {
				fmt.Println("open", c.RemoteAddr())
			},
			Data: func(c *fib.Connection, data []byte) {
				// data belongs to this round: Send copies it, so this echo is safe.
				if err := c.Send(data); err != nil {
					c.Close()
				}
			},
			Close: func(c *fib.Connection, err error) {
				fmt.Println("close", c.RemoteAddr(), err)
			},
		},
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

HTTP handler 实现的则是 `fibhttp.Handler`：`ServeHTTP(*fibhttp.Context)`，用 `fibhttp.HandlerFunc` 从函数适配。Context 里是标准的 `*http.Request`（`c.Request`），它实现 `http.ResponseWriter`，并带有响应辅助方法（`Respond`、`JSON`、`String`、`Data`、`HTML` 等）。绑定的 HTTP handler 栈是 `fibhttp.NewHandler(handler)`，和其他章节一样；原生绑定直接放 `fib.Handler`，如上。

## 执行模型

handler 的一切写法都源于一个设计决定：同一连接**一次只跑一轮**，每轮交给一个空闲的工作协程。事件循环只读就绪并连接提交给协程池；工作协程一直读到短读为止，跑协议（`OnData`、HTTP 解析器、WebSocket 帧解析器）和你的 handler，并在本轮末尾把输出 cork 后写出。

对 handler 来说这意味着：

- `OnData` 和 HTTP handler 在工作协程上运行，同一连接串行，按数据到达顺序执行。`OnOpen` 是例外：它在事件循环的协程上运行。
- 在 handler 里阻塞——sleep、同步 RPC、对另一个 socket 做 `io.ReadAll`——会占住一个工作协程，并拖慢它本可以服务的其他连接；但它**不会**阻塞事件循环。
- `OnData` 的 `data` 只在本次调用期间有效：`Send` 会复制它，自己保存则不会。
- 一轮内写出的响应会被 cork，在一次写里发出（`CoalesceWrites`）；从别的协程手工写多条响应则需要显式 `Cork`/`Flush`。
- HTTP/2 与 HTTP/3 的 handler 不跑在读连接的协程上，而是进引擎的 handler 池（`Engine.HandlerPool()`）`<Name>-streams`，gRPC 调用和 arpc 的异步 handler 也在那里。因此同一连接上的并发流会被并发处理，阻塞的 handler 只拖慢它自己的请求；`Config.StreamPool.MaxConcurrentHandlers` 限制单个连接同时跑多少个请求。

从[进阶技巧的速查表](../hacks.zh-CN.md)浓缩出的协程规则：

| 调用 | 可以在哪里调 |
| --- | --- |
| `Connection.Send` / `SendParts` / `SendFile` / `Close` | 任意协程；从不阻塞 |
| `Connection.Cork` / `Flush` | 任意协程；每个 `Cork` 都要有 `Flush` |
| `Context.Respond` / `Write` / `Finish` | 任意协程，最后一次 `Release` 之前 |
| `Context.Retain` / `Release` | 任意协程，成对使用，对已结束的请求是空操作 |
| `OnData` / `OnClose` | 同一连接一次一轮；`OnClose` 排在它之前的全部 `OnData` 之后 |
| `OnOpen` | 事件循环：只能做很快的事 |

## 同步 HTTP handler

默认写法最简单：返回之前就作答。handler 返回时什么都没写、也没有 `Retain`，服务器会补一个空的 `200`——与 `net/http` 完全一致。`Respond`、`JSON`、`String` 等方法要么整体发出响应，要么不发并返回错误，因此 handler 可以回退到别的响应：

```go
package main

import (
	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		name := c.Query("name") // decoded, and zero-copy when no decoding is needed
		if name == "" {
			name = "world"
		}
		_ = c.JSON(200, map[string]string{"hello": name})
	})

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
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

用 `http.ResponseWriter` 的方法一段一段写响应时，可以在中途 flush；SSE 和长下载就是这样流式发送的，且在 HTTP/1、HTTP/2 和 HTTP/3 上写法相同：

```go
_, _ = c.WriteString("data: one\n\n")
c.Flush() // send now instead of when the handler returns
_, _ = c.WriteString("data: two\n\n")
```

`Respond`/`WriteResponse` 与 `ResponseWriter` 方法不能在同一个响应里混用：每个 handler 选定一种写法。

## 异步响应：Retain 与 Release

暂时无法作答的 handler——在等数据库、上游调用或磁盘——必须把工作协程还回去，而不是占着它。响应正是为此做了引用计数：服务请求时持有一个引用，handler 返回时释放；`Retain` 再多持有一个，响应就不会结束。最后一个引用释放时，响应完成并交给连接。`Release` 用来归还一个引用，规则是每个 `Retain` 对应一次 `Release`：

```go
package main

import (
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		// The handler's own reference is released when it returns. Take one
		// more so that the response stays open while the goroutine works.
		c.Retain()
		q := c.Query("q")
		go func() {
			defer c.Release()                  // the last Release finishes the response
			time.Sleep(100 * time.Millisecond) // stand-in for a slow backend
			_ = c.Respond(200, "text/plain; charset=utf-8", []byte("result for "+q))
		}()
	})

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
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

被 retain 的请求所拥有的一切——`c.Request`、它的 header、URL 和 body，`c.Query` 返回的零拷贝值——在最后一次 `Release` 之前都有效；之后对象复用可能把它们交给另一个请求。需要保存的就复制，或者在释放前用完。`c.Async(fn)` 是简写：它替你 `Retain`，返回一个先执行 `fn` 再（用 defer）`Release` 的函数，交给自己的协程或池执行。

请求可能**在响应之前**就结束：对端关闭、读超时触发、body 出错。`OnCancel` 会在引擎的 handler 池上通知被 retain 的 handler 一次，让它停掉上游工作——但它仍必须归还自己持有的引用，因为取消只是不再写响应，并不会替你释放：

```go
var once sync.Once
release := func() { once.Do(c.Release) } // exactly one Release per Retain
c.Retain()
c.OnCancel(func(err error) {
	cancel()  // stop the work the peer is no longer waiting for
	release() // cancellation does not release the hold Retain took
})
go func() {
	defer release()
	_ = c.Respond(200, "application/json", slowQuery())
}()
```

`c.Err()` 返回与取消相同的原因；对已经消失的连接直接写也只会得到它，而不会破坏任何东西。如果活只需要几微秒，异步就是错的工具——开协程再加一次唤醒比就地作答更贵；而且 HTTP/1 上被 retain 的请求会挡住它后面流水线化的请求，直到它被释放。

## 用 OnBody 流式读请求体

默认情况下整个 body 到齐后才调 handler，因此 1 GiB 的上传要占 1 GiB 内存。打开 `Config.StreamRequestBody` 后，header 一到就调 handler，`OnBody` 按顺序、一次一个地把 body 分片交给你，不为每个分片开 goroutine，而且背压是免费的：回调比对端慢时连接就停止读，TCP 窗口会让发送方慢下来，**回调本身就是限速器**。

```go
package main

import (
	"crypto/sha256"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	config := fibhttp.DefaultConfig()
	config.StreamRequestBody = true
	config.StreamRequestBodyThreshold = 0  // stream every body, however small
	config.MaxStreamedBodyBytes = 64 << 30 // bound the streamed body at 64 GiB

	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		h := sha256.New()
		var n int64
		c.OnBody(func(data []byte, fin bool, err error) {
			if err != nil {
				return // connection gone, over the limit, or framing broken
			}
			n += int64(len(data))
			h.Write(data) // data is valid only during this call
			if fin {
				_ = c.String(200, "digest=%x size=%d\n", h.Sum(nil), n)
			}
		})
	})

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandlerWithConfig(config, app),
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

`OnBody` 自己会 retain 请求，服务器在回调最后一次调用返回后释放，所以在回调里作答的 handler 不需要 `Retain` 也不需要 `Release`。下面四点以后能省很多时间：

- **`Expect: 100-continue` 是懒的。** handler 第一次要 body 时才发 `100 Continue`，所以先检查 `c.Request.ContentLength` 并在注册 `OnBody` 之前回 `413`，可以在一个字节都没读的情况下拒绝上传。
- **流式 body 受 `ReadTimeout` 约束。** 它从请求第一个字节开始计时，body 还在到达时就一直生效；大文件上传要把它调大或设为 0，转而依赖 `IdleTimeout`。
- **不要在回调里阻塞。** 它跑在连接的 worker 上；慢活交给自己的池，并额外 `Retain`，自己负责 `Release`。
- **`OnBody` 接管后 `Request.Body` 就不能再读**，而且流式请求永远不会复用它的 `Request`、header 和 URL。

## 坑与限制

- **handler 绝不能阻塞。** 等锁、等对端或慢 I/O 都会占住 worker；请用 `Retain` 加回调，或自己的协程池。反模式清单见[进阶技巧](../hacks.zh-CN.md)。
- **每个 `Retain` 恰好配一次 `Release`。** 错误分支、取消分支、panic 分支都算；少一次会钉住连接，多一次会提前结束响应。
- **最后一次 `Release` 之后不要再碰 `c`、`c.Request` 或 `c.Body()`。** 对象复用默认打开，它们已经属于另一个请求，而且不会 panic，bug 会被静默吞掉。
- **`data` 是借来的。** `OnData`、`OnBody` 和 WebSocket 的负载只在调用期间有效；要保留请先复制。
- **被 retain 的 HTTP/1 请求会挡住它后面流水线化的请求**，直到它被释放，因为响应必须保持顺序。HTTP/2 和 HTTP/3 的流互不等待。
- **取消不等于释放。** `OnCancel` 只说明请求已经没了；之前取的 hold 仍要归还。
- **`OnBody` 不是提前作答的办法。** 响应在最后一次回调之后才写出；要在 body 还在到达时就响应，请从回调里 `Retain` 并 `Respond`。
- **没有 retain 就别在别的协程里读 `Context`。** 零拷贝取值方法是最常见的陷阱。
- **`Respond` 与 `ResponseWriter` 方法互斥。** 混用会让响应失败；每个 handler 只选一种。

## 相关文档

- [进阶技巧](../hacks.zh-CN.md)——异步响应、`OnBody`、取消、流水线、对象复用、零拷贝、`HoldReads`、写侧、`Read`
- [引擎与配置](02-engine.zh-CN.md)——协程池、handler 池及其尺寸
- [路由](04-router.zh-CN.md)——给本章的 HTTP handler 做路由
- [HTTP/1.x](06-http1.zh-CN.md) 与 [HTTP/2](07-http2.zh-CN.md)——这些 handler 背后的协议细节
- [原生 TCP、UDP 与 Unix socket](19-raw-io.zh-CN.md)——完整的 `Connection` API
- [README](../../README.zh-CN.md)——标准库 `http.Handler` 的兼容性说明

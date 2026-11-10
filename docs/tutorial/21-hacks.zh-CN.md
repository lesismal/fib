# 进阶技巧

[English](21-hacks.md) | [简体中文](21-hacks.zh-CN.md)

fib 的速度来自从 C 的 epoll 服务器借来的三条习惯：连接的一轮在同一个 worker 上串行跑、handler 绝不等待、
一轮里的所有写先 cork 再一次性发出。本章是这些习惯带来的能力精选导览——每个技巧给出最小示例、对应的坑，
以及指向[奇技淫巧指南](../hacks.zh-CN.md)对应编号章节的链接（详细解释在那里）。本章面向已经会写 fib
handler 的读者；否则请先看 [03-handlers.md](03-handlers.zh-CN.md)。

导览顺序：[1](#1-异步响应retain-与-release) 异步响应 ·
[2](#2-大-bodyonbody) 大 body · [3](#3-多路复用handler-池) 多路复用 ·
[4](#4-一轮一写) 一轮一写 · [5](#5-借用对象复用与零拷贝) 借用对象 ·
[6](#6-读背压holdreads) 读背压 · [7](#7-写侧) 写侧 ·
[8](#8-读deadline-与取消) 读与 deadline ·
[9](#9-websocket-按帧接收) WebSocket 帧 ·
[10](#10-单引擎服务端与客户端) 单引擎双角色 ·
[11](#11-一个-handler-服务三种协议) 一个 Handler 三种协议 ·
[12](#12-静态文件filecache-与-sendfile) 静态文件 · [13](#13-udp-批处理) UDP 批处理 ·
[14](#14-池进程与-buffer) 池与进程 ·
[15](#15-速查表) 速查表 · [16](#16-反模式) 反模式。

## 1. 异步响应：Retain 与 Release

**技巧。** 响应是引用计数的：处理请求占一个引用，handler 可以用 `Retain` 再多占一个，稍后用 `Release`
还掉。在这两者之间 handler 已经返回、worker 已还给池，所以慢后端不占协程。`Context.Async(fn)` 为它
包好了这一对，交给自己的池执行：调用它时就已 `Retain`，返回的函数只执行一次 `fn`，然后 `Release`；所以不要
再自己 `Retain`，并确保这个函数一定会被执行（从未执行的函数就是一个永不释放的 `Retain`）。

```go
pool.Go(c.Async(func() { // Async retains now; the func runs fn, then releases even if fn panics
	_ = c.Respond(200, "application/json", result)
}))
```

**坑。** 每个 `Retain` 都必须在每条路径上恰好对应一次 `Release`，包括错误路径和取消路径；没有任何东西会让
`Retain` 超时，漏一次 `Release` 就会一直占住连接（HTTP/1 上还有排在它后面的所有 pipeline 请求），直到对端
或连接超时结束它。最后一次 `Release` 之后，请求对象可能已经属于另一个请求，要保留的内容必须先拷贝。

下面的完整程序展示了整个模式，包括对借用的 query 值的拷贝：

```go
package main

import (
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	config := fib.DefaultConfig()
	config.LogSettings = false

	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		name := strings.Clone(c.Query("name")) // a borrowed substring: copy it to keep it
		c.Retain()                             // the response waits for a Release now
		go func() {                            // real code: your own pool, not a goroutine per request
			defer c.Release()
			time.Sleep(10 * time.Millisecond) // a slow backend
			_ = c.Respond(200, "text/plain", []byte("hello "+name))
		}()
	})

	engine, err := fib.New(config, fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(app),
	})
	if err != nil {
		panic(err)
	}
	go func() { _ = engine.Run() }()
	defer func() { engine.Stop(); _ = engine.Close() }()

	client := fibhttp.NewClient(engine, fibhttp.DefaultClientConfig())
	defer client.Close()

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://127.0.0.1:8080/?name=fib", nil)
	if err != nil {
		panic(err)
	}
	resp, err := client.Go(req).Wait()
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println(resp.StatusCode, string(body))
}
```

一个响应可以被多个分支分别 retain，最后一个 `Release` 的分支在写完响应后结束它。用 `OnCancel` 得知对端
已离开，并释放该分支占用的引用——用 `sync.Once`，因为正常路径与取消路径可能都会走。

见 [§1](../hacks.zh-CN.md#1-异步响应retain--release) 与
[§3](../hacks.zh-CN.md#3-取消与异常oncancel--err--幂等的收尾)。

## 2. 大 body：OnBody

**技巧。** 在 HTTP handler 上设置 `Config.StreamRequestBody`，handler 在 header 到达后立即运行；body 通过
`c.OnBody` 按顺序、一次一个地到达，运行在读取该连接的 worker 上。body 从不整体缓冲，也不额外占协程；
回调慢时连接停止读取，TCP 自然给发送方限速，背压是免费的。

```go
httpConfig := fibhttp.DefaultConfig()
httpConfig.StreamRequestBody = true
httpConfig.StreamRequestBodyThreshold = 0 // stream every body
httpConfig.MaxStreamedBodyBytes = 64 << 30
httpConfig.ReadBodyTimeout = time.Hour // the 5-minute default would cut a long upload off

app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
	if c.Request.ContentLength > limit {
		_ = c.Respond(413, "text/plain", []byte("too large"))
		return // no OnBody: with Expect: 100-continue the client never starts uploading
	}
	c.OnBody(func(data []byte, fin bool, err error) {
		if err != nil {
			return // connection gone, over the limit or cancelled: the last call
		}
		_, _ = f.Write(data) // data is valid only during this call
		if fin {
			_ = c.Respond(200, "text/plain", []byte("stored"))
		}
	})
})
```

**坑。** 流式 body 在到达期间仍受读超时约束：`ReadBodyTimeout`（`DefaultConfig` 中为 5 分钟，从 header 结束
算起）与 `ReadTimeout`（默认关闭，从第一个字节算起）。大上传要调大它们，或用 `c.SetReadDeadline` 为单个
请求解除限制。`OnBody` 接管之后 `Request.Body` 不能再读，回调也不能阻塞——它占着 worker。

下面的程序把一个 2MiB 上传流式送进只统计字节数的服务端：

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	stdhttp "net/http"
	"sync/atomic"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	config := fib.DefaultConfig()
	config.LogSettings = false

	httpConfig := fibhttp.DefaultConfig()
	httpConfig.StreamRequestBody = true
	httpConfig.StreamRequestBodyThreshold = 0
	httpConfig.MaxStreamedBodyBytes = 1 << 30
	httpConfig.ReadBodyTimeout = 0 // no limit: the body streams while the handler runs

	var received atomic.Int64
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		if c.Request.ContentLength > 8<<20 {
			_ = c.Respond(413, "text/plain", []byte("too large"))
			return
		}
		c.OnBody(func(data []byte, fin bool, err error) {
			if err != nil {
				return
			}
			received.Add(int64(len(data)))
			if fin {
				_ = c.Respond(200, "text/plain", fmt.Appendf(nil, "got %d bytes", received.Load()))
			}
		})
	})

	engine, err := fib.New(config, fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandlerWithConfig(httpConfig, app),
	})
	if err != nil {
		panic(err)
	}
	go func() { _ = engine.Run() }()
	defer func() { engine.Stop(); _ = engine.Close() }()

	client := fibhttp.NewClient(engine, fibhttp.DefaultClientConfig())
	defer client.Close()

	payload := bytes.Repeat([]byte("x"), 2<<20)
	// A reader net/http does not recognise as a buffer: the client streams it.
	body := io.LimitReader(bytes.NewReader(payload), int64(len(payload)))
	req, err := stdhttp.NewRequest(stdhttp.MethodPost, "http://127.0.0.1:8080/upload", body)
	if err != nil {
		panic(err)
	}
	resp, err := client.Go(req).Wait()
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	answer, _ := io.ReadAll(resp.Body)
	fmt.Println(string(answer))
}
```

见 [§2](../hacks.zh-CN.md#2-大-body-与-onbody不缓存不开-goroutine自带背压)。

## 3. 多路复用：handler 池

**技巧。** HTTP/2 与 HTTP/3 上一条连接承载多个请求，它们的 handler 运行在引擎的 handler 池
（`Engine.HandlerPool()`）上，而不是读取连接的 worker 上。`StreamPool.MaxConcurrentHandlers` 限定一条
连接同时运行多少个请求：第 N 个直接在连接自己的读取者上跑，在它返回前这条连接不再读取，于是由对端的流控承载这个上限，服务端不
排队。默认值 0 不设限，只受 `MaxConcurrentStreams` 约束。

```go
httpConfig := fibhttp.DefaultConfig()
httpConfig.StreamPool.MaxConcurrentHandlers = 8 // per connection; 1 = one at a time
handler := fibhttp.NewHandlerWithConfig(httpConfig, app)
```

**坑。** 多路复用连接上的阻塞 handler 会拖累自己的 stream 池；retain 的请求只挂起那一个 stream——但
handler 仍然不能等待，因为它占用的 worker 同时在读这条连接上的所有其他 stream。h2/h3 上不能靠停止读来做
背压，那是 stream 窗口的职责。

见 [§5](../hacks.zh-CN.md#5-multiplexhttp2http3handler-池与-maxconcurrenthandlers)。

## 4. 一轮一写

**技巧。** 在一次 `OnData` 里循环解析、循环发送；fib 会把这一轮的所有写 cork 起来，在轮末一起写出，于是
一次携带 N 个 pipeline 请求的读只花一次 `writev`，而不是 N 次写。无需开启——这就是默认的
`CoalesceWrites`。

```go
Data: func(c *fib.Connection, data []byte) {
	for len(data) > 0 {
		msg, n, ok := parse(data)
		if !ok {
			break // partial: keep the remainder yourself and join it in the next OnData
		}
		data = data[n:]
		_ = c.Send(answer(msg)) // queued; the round flushes once at its end
	}
},
```

**坑。** 每条消息一次 `Flush` 就把收益丢掉了。而且写 watermark 要大于一次读对应的回复量，而不是一条消息的量：
socket 有数据时，一轮会以 `ReadBufferSize`（默认 16KiB）为单位持续读，每次读完都把 cork 住的回复与
`WriteBufferHighWatermark` 比较，并且每读 256KiB flush 一次；watermark（默认 64-256KiB）要明显高于一次读的
回复量，否则每个 pipeline 都会无谓地暂停读。

见 [§4](../hacks.zh-CN.md#4-pipeline-优化一轮一写) 与
[§8](../hacks.zh-CN.md#8-读背压holdreads)。

## 5. 借用对象：复用与零拷贝

**技巧。** Request、Header、URL、Context 默认复用（`ReuseRequests`、`ReuseHeaders`、`ReuseURLs`、
`ReuseContexts`），这是 fib 在高并发下吞吐的重要来源。代价是 fasthttp 的那条规则：复用对象属于 handler，
直到它返回并释放了所有 `Retain`。此时 `c.Query`、`c.Body()`、`c.Param` 是原地借用，零分配。

```go
q := c.Query("q")    // a substring of the raw query, no map, no allocation
b := c.Body()        // the server's own buffer for a whole body
_ = c.RespondCopy(200, "application/json", scratchJSON(q, b)) // the copy is made at the call
```

**坑。** 借来的内存不能活过 handler。把 `c.Body()`、`c.Request` 或 `c.Query` 的值交给活得更久的协程，读到
的是另一个请求的数据，而且**不会有任何 panic**。用 `strings.Clone`、`append([]byte(nil), ...)` 或
`TakeBody` 拷贝；handler 想立刻拿回自己的临时 buffer 时用 `RespondCopy`/`WriteResponseCopy`。

见 [§6](../hacks.zh-CN.md#6-对象复用像-fasthttp-一样用-context) 与
[§7](../hacks.zh-CN.md#7-零拷贝读contextbody--contextquery)。

## 6. 读背压：HoldReads

**技巧。** 写侧 fib 自己会在 watermark 处暂停读；读侧由应用决定。`HoldReads(true)` 让连接停止读取（正在进行的一轮在
下一次读之前停下），数据留在内核缓冲里，TCP 窗口给对端限速；`HoldReads(false)` 恢复读取，已经堆积的数据无需
对端再发一个字节就会送达。它可以在任意 goroutine 上调用；hold 不嵌套，以最后一次调用为准。它便宜，因为连接停止被调度，而不是占住一个 worker。

```go
Data: func(c *fib.Connection, data []byte) {
	q.push(append([]byte(nil), data...)) // data is valid only during this callback
	if q.size() >= highWater {
		c.HoldReads(true) // from inside OnData it stops this round's read loop too
	}
},
// From the goroutine that drains the queue, once below the low watermark:
//     c.HoldReads(false)
```

**坑。** HoldReads 不是 sleep：在回调里阻塞会占住 worker，而 hold 读是把连接还给事件循环。它对 UDP 无效
（数据报已经被读掉了）；`Stats().ReadsPausedByBudget` 非零说明是别的连接通过 `MaxPendingBytes` 把这条
连接一起暂停了。

见 [§8](../hacks.zh-CN.md#8-读背压holdreads)。

## 7. 写侧

**技巧。** 按数据形态选发送方式：常规用 `Send`，帧头加负载用 `SendParts`（`writev`，不拼接），交出或共享
buffer 用 `SendOwned`/`SendShared`，文件用 `SendFile` 让内核直接发。`Cork`/`Flush` 用来合并**在一轮之外**
写出的多个回复。

```go
c.Cork()
for _, reply := range replies { // from a goroutine of your own, not from OnData
	_ = c.Send(reply)
}
_ = c.Flush() // required: nothing else writes what a Cork holds
```

**坑。** 每个 `Cork` 都需要它的 `Flush`，一直不 flush 会让对端干等。`SendFile` 把文件区间按顺序和前后
的 `Send` 排在一起；声明的长度比文件长会关闭连接。`MaxWriteQueueBytes`（或
`Connection.SetWriteQueueLimit`）是对从不读数据的对端的硬上限：超过后发送返回 `ErrSendQueueFull` 并关闭
连接。

见 [§9](../hacks.zh-CN.md#9-写侧cork--flush--sendparts--sendfile)。

## 8. 读、deadline 与取消

**技巧。** `*fib.Connection` 实现了 `net.Conn`，但 `Read` 不会阻塞：socket 没有数据时返回
`fib.ErrWouldBlock`，于是 handler 可以把这一轮已读到的数据自己抽干。deadline 不会让调用失败，而是关闭
连接；rolling timeout 只要时间点不变就不额外重建定时器。暗角里还有：`CloseWrite` 在队列排空后半关写端，
`CloseRead` 停止 `OnData` 但保留写端，`SetAttachment` 在连接上挂每连接状态，`IsAccepted`/`IsDialed`
区分两端。

```go
Data: func(c *fib.Connection, data []byte) {
	for {
		n, err := c.Read(buf)
		if errors.Is(err, fib.ErrWouldBlock) {
			break
		}
		// ... handle buf[:n], or stop on any other error
	}
},
```

**坑。** 绝不要用 `bufio.Reader` 包 `Connection`，也不要交给期望阻塞读的代码。不要在 `OnData` 里关闭自己
的连接、然后在同一轮里等 `OnClose`/`OnCancel`：`OnClose` 要等这一轮返回，会死锁。用 `OnCancel` 加
`sync.Once` 释放被取消请求仍然占用的东西。

见 [§10](../hacks.zh-CN.md#10-read-不阻塞deadline-是关连接) 与
[§3](../hacks.zh-CN.md#3-取消与异常oncancel--err--幂等的收尾)。

## 9. WebSocket 按帧接收

**技巧。** `OnMessage` 会重组完整消息；改设 `HandlerFuncs.Frame` 则逐帧交付，于是大于 `MaxMessageBytes`
的消息也能流式处理，上限变成单帧的大小。这就是 WebSocket 版的 `OnBody`。

```go
handler := websocket.HandlerFuncs{
	Frame: func(c *websocket.Connection, op websocket.Opcode, fin bool, data []byte) {
		_, _ = f.Write(data) // data is valid only during the call
		if fin {
			_ = f.Close()
		}
	},
}
```

**坑。** 设置 `Frame` 之后，Text/Binary 不再调用 `Message`；每次调用带消息自己的 opcode 与 `fin`——未分片
消息就是一次 `fin=true` 的调用。permessage-deflate 压缩消息仍会被重组，因为它的帧是同一个 DEFLATE 流的
片段。自定义 `Ping` handler 会**替换**默认的 pong 回复：需要自己调用 `c.Pong`。

见 [§11](../hacks.zh-CN.md#11-websocket按帧接收onframe)。

## 10. 单引擎：服务端与客户端

**技巧。** fib 的所有客户端共享服务端的事件循环、worker 池与背压，所以网关用一个引擎就能同时服务两个
方向。`DialWithHandler` 让每条拨出的连接拥有自己的 handler；不带 binding 的 `fib.New(config)` 就是纯客户端
引擎。

**坑。** 回调可能运行在任意协程上，所以不要在回调里阻塞，也不要在 handler 里 `Wait()` 客户端 future。
先关客户端，再关引擎。

下面的程序是 TCP echo 服务端和它的客户端，一个引擎、一套 handler 类型：

```go
package main

import (
	"fmt"
	"time"

	fib "github.com/lesismal/fib"
)

func main() {
	config := fib.DefaultConfig()
	config.LogSettings = false

	echo := fib.HandlerFuncs{
		Data: func(c *fib.Connection, data []byte) {
			_ = c.Send(data) // corked and flushed once at the end of the round
		},
	}
	engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:9000", Handler: echo})
	if err != nil {
		panic(err)
	}
	go func() { _ = engine.Run() }()
	defer func() { engine.Stop(); _ = engine.Close() }()

	got := make(chan string, 1)
	client := fib.HandlerFuncs{
		Open: func(c *fib.Connection) { _ = c.Send([]byte("ping")) },
		Data: func(c *fib.Connection, data []byte) { got <- string(data) },
	}
	err = engine.DialWithHandler("tcp", "127.0.0.1:9000", 3*time.Second, client,
		func(c *fib.Connection, err error) {
			if err != nil {
				got <- "dial: " + err.Error()
			}
		})
	if err != nil {
		panic(err)
	}
	fmt.Println("client got:", <-got)
}
```

见 [§12](../hacks.zh-CN.md#12-同一个-engine-同时当-server-和-client)。

## 11. 一个 Handler 服务三种协议

**技巧。** 同一个 `fibhttp.Handler` 原样服务 HTTP/1.x、HTTP/2（ALPN、h2c 先验知识或 `Upgrade: h2c`）与
HTTP/3；由 `c.Request.Proto` 告知当前是哪个。`Context.Upgrade(protocol, header, tunnelHandler)` 回应 101
（h2/h3 上是 200），把连接或 stream 交给 `TunnelHandler`——WebSocket 升级和 CONNECT 隧道正是这么实现的。

```go
func(c *fibhttp.Context) {
	tunnel, err := c.Upgrade("myproto", stdhttp.Header{"X-Ver": {"1"}}, myTunnelHandler)
	if err != nil {
		return
	}
	_ = tunnel // OnTunnelOpen has run; the raw bytes follow
}
```

**坑。** 通过 `Write`/`Flush` 写的响应在三种协议上都是流式的，并由客户端限速：未发出的数据达到 64KB 时
`Write` 会等待（受 `WriteTimeout` 约束），所以流式发送大 body 的 handler 会在客户端慢时占住它的 goroutine——
要从自己 retain 后的 goroutine 里流式写，文件则用 `FileCache`/`SendFile`。作答之前设置的 `c.Header()`
会随 `Write` 发出，也同样随 `Respond`、`String`、`JSON` 和 `WriteResponse` 发出（完整响应自己的同名字段优先）；
响应开始之后再改则没有效果。`http.Hijacker` 没有实现；h2/h3 上不能调用
`Context.Conn.Send`，要走 `Respond`/`WriteResponse`。

见 [§13](../hacks.zh-CN.md#13-一个-handler-同时服务-h1--h2--h3以及-upgrade--tunnel)。

## 12. 静态文件：FileCache 与 sendfile

**技巧。** `http.NewFileCache` 在第一次请求后把文件放在内存里，并跟随磁盘变化（Linux 上 inotify）；开启
`Precompressed` 时按 `Accept-Encoding` 选 `name.br`/`name.gz`，压缩发生在部署时。大于 `MaxFileBytes` 的
文件每次走磁盘 `SendFile`；普通 `http.ServeFile` 也一样，因为 `Context` 实现了 `io.ReaderFrom`。

```go
files, err := fibhttp.NewFileCache(fibhttp.FileCacheConfig{
	Root:          "/data/static",
	Precompressed: true,
})
if err != nil {
	panic(err)
}
app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
	files.ServeFile(c, strings.TrimPrefix(c.Request.URL.Path, "/static/"))
})
```

**坑。** 没有 watch 能报告变化的地方（非 Linux、符号链接文件），条目在 `CheckInterval`（默认 5 秒）内被
信任，之后才重新 stat；负的间隔表示每个请求都 stat 并弃用 watch。缓存总量由 `MaxBytes` 限制；超过
`MaxFileBytes` 的文件每次请求都从磁盘读，那不是缓存未命中，而是 sendfile。

见 [§14](../hacks.zh-CN.md#14-filecache-与-sendfile静态文件不碰磁盘不过用户态)。

## 13. UDP 批处理

**技巧。** 在监听 UDP socket 上，每个对端都是一个 `Connection`。实现了 `DatagramsHandler` 的 handler 会把
worker 运行前已排队的多个数据报**一起**收到，于是可靠 UDP 协议可以一次 ACK 整批、少发很多数据报；
`SendBatch` 把它们变成一次 `sendmmsg`/`sendmsg_x`。`Engine.OpenUDPPeer` 用于服务端必须先开口的对端
（ICE、STUN）。

```go
type acker struct{ fib.HandlerFuncs }

func (a acker) OnDatagrams(c *fib.Connection, datagrams [][]byte) {
	_ = c.SendBatch(acksFor(datagrams)) // one system call for the batch
}
```

**坑。** 每个数据报都是 handler 自己的副本，是一个 `bufferpool` buffer，不再被引用后可以用 `bufferpool.Put`
归还；承载它们的 `[][]byte` 在 `OnDatagrams` 返回后不能保留。UDP 发送从不排队——socket 没空间时数据报被
丢弃、发送返回错误。开启 `ReusePort`（默认）时，`OpenUDPPeer` 打开的对端可能把回复发到另一个 poller 的
socket，后者会为同一地址再开一个连接；NAT rebind 的对端则会作为新对端到达：按地址而不是按连接来索引对端
状态。

见 [§15](../hacks.zh-CN.md#15-udpdatagramshandler-与-sendbatch)。

## 14. 池、进程与 buffer

**技巧。** 四个更小的杠杆，主题一致：不要让引擎稀缺的 worker 去跑你的工作，也不要为了省事让所有核共享一个
堆。

- **自己的池**用于异步 handler 的工作，绝不用引擎的池：engine worker 向你的池提交任务，如果是同一个池，
  队列满时所有 worker 都可能卡在提交上。`SharedTaskPool`（默认开）让同名 `Name` 的多个引擎共享 worker 和
  队列，且第一个引擎的配置生效。`Config.SetTaskPool(p)` 是反方向的：
  让引擎在 `p` 上跑它的连接，`p` 归调用方所有，由调用方在引擎关闭后停止。
- **prefork**（Linux）把同一程序跑在多个子进程里，每个有自己的堆和 GC，通过 `SO_REUSEPORT` 共享监听端口。
  子进程需要的初始化放在 `serve` 里；`Run` 之前的代码在 master 和每个子进程都会执行。
- **IOPollers** 把引擎拆到多个事件循环上；更多 poller 会和 worker 抢 P，通常更慢，所以只在有 profile 时
  才动 `IOPollerCount` 和 `EngineLoop`。
- **GOMAXPROCS** 保持默认。调高只会更慢、更费内存。
- **bufferpool** 是 fib 自己的 buffer 池；`Get`/`Put` 零分配，有一个能活过 GC 的储备
  （`SetRetainedBytes`），你的 buffer 也能和 fib 的 buffer 走同一个池。

```go
jobs := taskpool.NewAdaptive(taskpool.AdaptiveConfig{
	Name: "jobs", MinWorkers: 16, MaxWorkers: 4096, QueueSize: 10000,
})
defer jobs.Stop()

app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
	jobs.Go(c.Async(func() { // async work on a pool of its own, not the engine's
		_ = c.Respond(200, "text/plain", slowQuery())
	}))
})

buf := bufferpool.Get(4096) // len 4096; contents are the previous user's
defer bufferpool.Put(buf)   // after Put, not even a sub-slice may be used
```

**坑。** `ModeInline` 不被引擎接受（否则一轮连接就会跑在事件循环上）；prefork 的子进程之间除端口外不共享
任何东西，所以内存状态每个子进程一份，而"整机唯一"的资源（比如数据库连接上限）要除以
`prefork.Children()`。

见 [§16](../hacks.zh-CN.md#16-协程池自己的池独立的池不要死锁)、
[§17](../hacks.zh-CN.md#17-进程级技巧preforkiopollersgomaxprocs) 与
[§18](../hacks.zh-CN.md#18-bufferpool自己的-buffer-也走同一个池)。

## 15. 速查表

**技巧。** 拿不准时先问：这个调用在哪个 goroutine 上跑。精简版：

| 调用 | 可调用位置 |
| --- | --- |
| `Connection.Send` / `SendParts` / `SendFile` / `Close` | 任意 goroutine；不阻塞 |
| `Connection.HoldReads`、`Cork`/`Flush`、`Set*Deadline` | 任意 goroutine；每个 `Cork` 都需要 `Flush` |
| `Context.Respond` / `Write` / `Finish` | 任意 goroutine，在最后一次 `Release` 之前；两种写方式不能混用 |
| `Context.Retain` / `Release` | 任意 goroutine；成对；在已结束的请求上是 no-op |
| `Context.OnBody` / `OnCancel` | handler 中或之后；回调不能阻塞 |
| h2/h3 上的 `Context.Conn.Send` | 不要调用；用 `Respond`/`WriteResponse` |
| `OnOpen`（TCP） | 事件循环：必须快 |
| `OnData` / `OnClose` | worker；同一连接上串行 |
| `SyscallConn().Control(f)` 的 `f` 内部 | 持有连接锁：不要调用该连接的方法 |

见 [§19](../hacks.zh-CN.md#19-速查哪些东西能在哪个-goroutine-上调)。

## 16. 反模式

**技巧。** 这张清单是这套指南里最省时间的调试工具，以下是最常踩的：

1. 在 handler、`OnData` 或 `OnBody` 回调里阻塞（`time.Sleep`、同步 RPC、`Wait()`、慢磁盘 I/O）——占住
   一个 worker。
2. 某条路径上 `Retain` 了却没有 `Release`；用 `defer` 或 `sync.Once` 保证恰好一次。
3. 在 `Release` 之后还碰 `r`、`c` 或 `c.Body()`，或把它们交给活得更久的协程。
4. `Release` 之后调用 `Respond`，或保留 `OnData`/`OnBody`/`OnFrame` 的 `data`。
5. 在 `Connection` 上用 `bufio.Reader`，或期望 `Read` 会阻塞。
6. 写 watermark 小于一轮的回复量。
7. 开了 `StreamRequestBody` 却保留默认 `ReadBodyTimeout`（5 分钟），大上传被超时切断。
8. 把异步工作放到引擎自己的池上；调高 `GOMAXPROCS`；在 `OnData` 里关闭自己的连接后等 `OnClose`。

见 [§20](../hacks.zh-CN.md#20-反模式清单)。

## 坑与限制

- **这些是边角，不是默认值。** 只有在 profile 或协议需要时才用 `Retain`、`HoldReads`、`SendParts` 或
  自定义池；大多数代码仍然适合 [03-handlers.md](03-handlers.zh-CN.md) 里的普通 handler 写法。
- **这些技巧不能盲目组合。** 借用内存加异步分支、跨 goroutine 持有 `Cork`——正是让 fib 变快的同一批 API
  在这样组合时让数据出错、让程序死锁。
- **后端随平台不同。** IOPollers 需要 epoll/kqueue（Windows 保持单个事件循环）；`SendFile` 在 Linux、macOS、
  FreeBSD、DragonFly 上用 `sendfile(2)`，Windows 上用 `TransmitFile`，NetBSD/OpenBSD 上分块读取；`SendBatch`
  只在 Linux 与 macOS 上是一次系统调用；`SpliceTo` 只支持 Linux。请在实际部署的系统上验证。
- **测量，而不是猜。** 指南里每一个"快 X 倍"都附带了基准和机器；把它变成策略之前先在你的机器上复现。

## 相关文档

- [奇技淫巧指南](../hacks.zh-CN.md) —— 本章浓缩的每个小节的完整解释与基准条件
- [Handler：同步与异步](03-handlers.zh-CN.md) —— `Retain`/`Release`、`OnBody`、handler 池
- [HTTP/1.x](06-http1.zh-CN.md) —— cork、sendfile、`FileCache`、流式响应
- [原生 TCP、UDP 与 Unix socket](19-raw-io.zh-CN.md) —— 本章反复使用的 `Connection` API
- [性能与运维](22-performance.zh-CN.md) —— 真实的池模式、容量、prefork 与 pprof
- [flows.html](../flows.html) —— 一轮、cork 与 handler 池的图示
- [架构](../../README.zh-CN.md#架构) —— README 里的三条规则与基准测试表

# fib 奇技淫巧（Hack Guide）

[English](hacks.md) · [简体中文](hacks.zh-CN.md)

这篇文档收集 fib 里和标准库 `net.Conn` / `net/http`、也和 nbio 不一样的用法，以及在 C/C++
里常见的"异步、非阻塞、一轮批处理"思路在 Go 里的落地方式。它不是入门文档，而是写给已经会用 fib、
想把它压到极限的人：哪些东西可以做、为什么能做、边界在哪、踩了会怎样。

阅读前先记住 fib 的三条前提，下面所有技巧都是从它们推出来的：

1. **没有每连接 goroutine。** 连接是 `taskpool` 里的任务单位，一轮（读 → 解析 → handler → 写）
   在一个 worker 上串行完成，同一连接的各轮互不并发。
2. **handler 跑在 worker 上，worker 是稀缺资源。** 在 handler 里等对端、等锁、等慢 I/O，
   占的是整个 Engine 的 worker，而不是一个廉价的 goroutine。所以 fib 的 API 几乎都是
   "登记回调然后返回"，而不是"阻塞直到好了"。
3. **一轮内的写是 cork 的。** 一轮里所有 `Send` 先进队列，轮末合并成一次 `write`/`writev`。

> 凡是下面写"要注意"的地方，都是真实的坑，不是客套话。

---

## 目录

1. [异步响应：Retain / Release](#1-异步响应retain--release)
2. [大 body 与 OnBody：不缓存、不开 goroutine、自带背压](#2-大-body-与-onbody不缓存不开-goroutine自带背压)
3. [取消与异常：OnCancel / Err / 幂等的收尾](#3-取消与异常oncancel--err--幂等的收尾)
4. [Pipeline 优化：一轮一写](#4-pipeline-优化一轮一写)
5. [Multiplex（HTTP/2、HTTP/3）：handler 池与 `MaxConcurrentHandlers`](#5-multiplexhttp2http3handler-池与-maxconcurrenthandlers)
6. [对象复用：像 fasthttp 一样用 Context](#6-对象复用像-fasthttp-一样用-context)
7. [零拷贝读：`Context.Body` / `Context.Query`](#7-零拷贝读contextbody--contextquery)
8. [读背压：`HoldReads`](#8-读背压holdreads)
9. [写侧：`Cork` / `Flush` / `SendParts` / `SendFile`](#9-写侧cork--flush--sendparts--sendfile)
10. [`Read` 不阻塞、deadline 是关连接](#10-read-不阻塞deadline-是关连接)
11. [WebSocket：按帧接收（OnFrame）](#11-websocket按帧接收onframe)
12. [同一个 Engine 同时当 server 和 client](#12-同一个-engine-同时当-server-和-client)
13. [一个 Handler 同时服务 h1 / h2 / h3，以及 Upgrade / Tunnel](#13-一个-handler-同时服务-h1--h2--h3以及-upgrade--tunnel)
14. [FileCache 与 sendfile：静态文件不碰磁盘、不过用户态](#14-filecache-与-sendfile静态文件不碰磁盘不过用户态)
15. [UDP：`DatagramsHandler` 与 `SendBatch`](#15-udpdatagramshandler-与-sendbatch)
16. [协程池：自己的池、独立的池、不要死锁](#16-协程池自己的池独立的池不要死锁)
17. [进程级技巧：Prefork、IOPollers、GOMAXPROCS](#17-进程级技巧preforkiopollersgomaxprocs)
18. [bufferpool：自己的 buffer 也走同一个池](#18-bufferpool自己的-buffer-也走同一个池)
19. [速查：哪些东西能在哪个 goroutine 上调](#19-速查哪些东西能在哪个-goroutine-上调)
20. [反模式清单](#20-反模式清单)

---

## 1. 异步响应：Retain / Release

**一句话**：响应是引用计数的。处理请求占一个引用，handler 返回时还掉；你多占一个，响应就在你
`Release` 之前不会结束。于是 handler 可以 **立刻返回、把 worker 还给池**，结果出来再回复。

```go
func(c *fibhttp.Context) {
    c.Retain()                       // handler 返回后不自动回写
    go func() {                      // 或者提交给你自己的池 / 回调到别的 reactor
        result := slowQuery(c.Request.URL.Query().Get("q")) // ← 注意：见下面"别碰 r"
        _ = c.Respond(200, "application/json", result)
        c.Release()                  // 引用归零 → 结束响应并交给连接
    }()
}
```

**为什么这是优势**

- 和标准库不同：`net/http` 里 handler 一返回响应就结束，要异步只能 handler 自己阻塞着等，
  占一个 goroutine（标准库每连接本来就有一个，所以它不在乎）。fib 里 handler 阻塞等于占 worker，
  所以必须有"挂起请求"的能力。
- 和 C/C++ 的 epoll 服务端是一回事：请求挂起（pending）、结果回来 `send` 再继续。区别只是
  fib 用引用计数替你管生命周期。
- 同一份 `Context` 可以被多个异步分支各自 `Retain`：扇出 N 个后端调用，各自 `Release`，
  最后一个回来的那个结束响应（"最后一个释放者结束"）。

**扇出聚合的写法**

每个分支各占一个引用，最后一个完成的分支负责写响应。用原子计数决定"谁是最后一个"，
写完再 `Release`；`Release` 才是响应结束的时刻，所以 **响应必须在最后一个 `Release` 之前写好**：

```go
func(c *fibhttp.Context) {
    var (
        mu      sync.Mutex
        parts   [3][]byte
        pending atomic.Int32
    )
    pending.Store(3)
    for i := range parts {
        c.Retain()
        i := i
        backend[i].CallAsync(func(b []byte) { // 你的异步 client，回调在任意 goroutine
            mu.Lock()
            parts[i] = append([]byte(nil), b...)
            mu.Unlock()
            if pending.Add(-1) == 0 { // 最后一个回来的写响应
                _ = c.Respond(200, "application/json", merge(parts[:]))
            }
            c.Release()
        })
    }
    // handler 自己占的那个引用在返回时释放；三个分支的引用各自 Release。
}
```

**配合 fib 自己的异步 client**

`fibhttp.Client`、`websocket.Dialer`、`Engine.Dial` 都是回调式、不阻塞调用方的，和 Retain/Release
天然配对——做反向代理或网关时，上游请求的回调里 `c.Respond`，再 `c.Release()`：

```go
c.Retain()
upstream.Do(req, func(resp *fibhttp.ClientResponse, err error) {
    defer c.Release()
    if err != nil {
        _ = c.Respond(502, "text/plain", []byte(err.Error()))
        return
    }
    _ = c.Respond(resp.StatusCode, resp.Header.Get("Content-Type"), bodyOf(resp))
})
```

**边界与坑**

- **`Retain` / `Release` 必须成对。** 多释放会提前结束响应（因为 handler 自己那一个引用也是一次
  释放）；少释放则连接被占住。没有超时：handler 不释放，就一直占着连接，直到对端断开或读超时。
- **HTTP/1 上，被持有的请求会挡住排在它后面的 pipeline 请求**（响应顺序必须保持），释放之后才
  继续解析。所以在 HTTP/1 上，一个慢的异步请求会让同连接后面的请求排队。HTTP/2、HTTP/3 上
  stream 之间互不影响。
- **别碰 `r`，如果你开了对象复用。** 见 [§6](#6-对象复用像-fasthttp-一样用-context)：
  被 `Retain` 的请求在最后一次 `Release` 之前一直归你，**之后** `*http.Request`、`Header`、
  `URL`、`Context` 都会回收给下一个请求。因此：要么 `Release` 之前用完，要么先拷贝需要的内容。
- 已经写完响应的请求、连接已经没了的请求上，`Retain`/`Release`/`Finish` 都是空操作，
  往上写返回错误而不会写坏连接。可以放心重复调用清理逻辑。
- `Retained()` 可以问当前是否被持有，通常只在测试和诊断里用。
- 可以同时持有至多 65535 个 `Retain`，超了 panic。

---

## 2. 大 body 与 OnBody：不缓存、不开 goroutine、自带背压

默认行为是 body 收齐后才调 handler——上传 1GB 就要在内存里放 1GB。开启
`Config.StreamRequestBody` 后，header 一到就调 handler，body 用 `OnBody` 回调边收边处理：

```go
httpConfig := fibhttp.DefaultConfig()
httpConfig.StreamRequestBody = true
httpConfig.StreamRequestBodyThreshold = 0      // 0：所有 body 都流式；非 0：不超过它的仍整体缓存
httpConfig.MaxStreamedBodyBytes = 64 << 30     // 流式 body 的上限
httpConfig.StreamRequestBodyBuffer = 1 << 20   // 未被读走的 body 攒到这么多就停止读 socket
httpConfig.ReadTimeout = 0                     // 见下面"慢客户端"

handler := fibhttp.NewHandlerWithConfig(httpConfig, fibhttp.HandlerFunc(
    func(c *fibhttp.Context) {
        f, _ := os.Create("upload.bin")
        c.OnBody(func(data []byte, fin bool, err error) {
            if err != nil {            // 连接断了 / 超限 / 帧错误：最后一次回调
                f.Close(); os.Remove("upload.bin")
                return
            }
            f.Write(data)              // data 只在本次调用内有效，要保留先拷贝
            if fin {
                f.Close()
                _ = c.Respond(200, "text/plain", []byte("stored"))
            }
        })
        // OnBody 自动 Retain；最后一次回调返回后自动 Release，响应随即写回。
    },
))
```

完整可运行的例子（含 SHA-256 校验、客户端、1GiB 测试文件生成）：
[`examples/http/upload`](../examples/http/upload)。

**这个设计的几个优势，逐条看**

1. **不缓存整个 body。** 128MB 上传，整体缓存时堆峰值约 470MB，流式时约 4MB。
2. **不开 goroutine。** `OnBody` 只登记回调，从不在内部调用它；已经到达的部分在 handler 返回之后、
   在跑 handler 的 goroutine 上交付，之后的数据由读到它的 worker 交付（开了 IOPollers 或在
   HTTP/2、HTTP/3 上则交给 Engine 的 handler 池）。回调一次一个、保持顺序。
   这和 C 里 `on_body(chunk)` 的回调模型一模一样。
3. **背压是免费的。** 回调慢，body 就积压在 `StreamRequestBodyBuffer` 里；攒满就
   `HoldReads(true)` 停止读 socket，内核缓冲区满，TCP 滑动窗口把对端自然压下来。你不用写任何
   限速代码，**限速的就是回调自己**。HTTP/2、HTTP/3 靠流控窗口（只有消费后才补窗口）实现同样的事，
   并且同一连接上别的请求不受影响。
4. **可以先拒绝再接收。** `Expect: 100-continue` 是惰性的：handler 第一次要 body 时才发
   `100 Continue`。所以可以在客户端还没开始传时就用 413/403 回绝，不用读一个字节：

   ```go
   if c.Request.ContentLength > limit {
       _ = c.Respond(413, "text/plain", []byte("too large"))
       return                    // 不调用 OnBody，客户端不会开始上传
   }
   ```
5. **边收边写盘，也可以边收边转发。** 回调里把 `data` 直接 `Send` 给上游，就是一个流式代理，
   中间不落内存。

**混合写法：先同步读已到的部分，不够再 OnBody**

`Request.Body` 在流式时是 `*fibhttp.BodyStream`，`Read` 不阻塞：

| 返回 | 含义 |
| --- | --- |
| `n > 0` | 已经到的字节 |
| `io.EOF` | 整个 body 读完 |
| `fibhttp.ErrWouldBlock` | 现在没有，后面还会来 → 用 `OnBody` 接管 |
| `ErrBodyAbandoned` | body 已被 `Close`/`OnBody` 接管，或 handler 没 Retain 就返回了 |
| 其它错误 | 后续不会再来（断连、超限、帧错误） |

交接不丢字节：`OnBody` 拿到的正好从上一次 `Read` 停下的地方开始。小请求常常 header 和 body
在同一次读里，同步读到 `io.EOF` 直接回复，省掉一次回调调度。`c.BodyComplete()` 可以先问一句。

**在回调里再异步**

`OnBody` 自动 Retain/Release 只覆盖到最后一次回调返回。要在别处回复（比如写盘是个很慢的操作，
交给你自己的池）：回调里再 `c.Retain()` 一次，处理完 `c.Release()`：

```go
c.OnBody(func(data []byte, fin bool, err error) {
    ...
    if fin {
        c.Retain()
        diskPool.Go(func() { // 你的协程池
            defer c.Release()
            _ = c.Respond(200, "text/plain", []byte(finish(f)))
        })
    }
})
```

**要注意**

- **慢上传**靠 `ReadTimeout` 约束，它从请求第一个字节算起，且 **流式 body 在 handler 运行期间仍在
  到达，所以也受它约束**。上传大文件时要么把它设得足够大/设 0，要么用 `IdleTimeout`。
  请求收全之后 deadline 才会撤掉，所以"慢 handler"不受 `ReadTimeout` 限制。
- 回调里 **不要长时间阻塞**：连接在你的回调里就不读，其它连接不受影响（回调在 worker 上），
  但它占着一个 worker。需要慢操作就换到自己的池 + `HoldReads`（见 [§8](#8-读背压holdreads)）。
- 回调里不能再读 `Request.Body`；关闭 `Body` 会让回调以 `ErrBodyAbandoned` 结束并丢弃余下的 body。
- handler 没读完就返回时：剩余不超过 256KB 会读掉丢弃、连接继续复用；更多或 chunked 则响应后
  关连接。
- 流式请求 **不复用** `Request`/`Header`/`URL`，与复用选项无关。
- 目前 HTTP/1.x 与 HTTP/2、HTTP/3 的流式写法完全一致；gRPC 的双向流需要同一个开关，
  让调用在请求开始到达时就运行。

---

## 3. 取消与异常：OnCancel / Err / 幂等的收尾

挂起请求的最大问题是"对端没了你还在干活"。fib 的处理方式：

- 连接断开、读超时、body 收不完 → 请求被**取消**：不再写出任何东西，剩余引用全部作废，
  你只被告知**一次**。
- 用了 `OnBody` 的 handler 在回调里收到 `err`；只 `Retain`、不用 `OnBody` 的异步 handler 用
  `Context.OnCancel(func(error))`；也可以主动查 `Context.Err()`。
- 通知在 Engine 的 handler 池上执行，不在事件循环上，拖不住服务端。

```go
func(c *fibhttp.Context) {
    ctx, cancel := context.WithCancel(context.Background())
    var once sync.Once
    done := func() { once.Do(c.Release) } // 每次 Retain 恰好对应一次 Release
    c.Retain()
    c.OnCancel(func(err error) {
        cancel()   // 取消上游调用
        done()     // 取消后仍要释放你 Retain 过的引用
    })
    upstream.DoCtx(ctx, req, func(resp *http.Response, err error) {
        if err == nil {
            _ = c.Respond(200, "application/json", bodyOf(resp))
        }
        done()
    })
}
```

**要点**

- 取消之后请求、body、Context 仍然是你的，直到你 `Release` 完每一个 `Retain`；
  之后才回收。**取消不等于可以不 Release**。
- `Release` 在已失败的请求上不会写任何东西，但仍然计数：多释放一次，会提前"释放"掉别的持有者的引用。
  取消回调和正常路径可能都跑到，所以像上面那样用 `sync.Once` 保证每次 `Retain` 只对应一次 `Release`。
- **不要在 `OnData`/HTTP/1 handler 里关自己的连接再等 `OnCancel` 的通知**：`OnClose` 要等本轮返回
  才执行，会死锁（见 guide 的"OnClose 的顺序"）。

---

## 4. Pipeline 优化：一轮一写

HTTP/1 pipeline（一次写 N 个请求、按顺序读 N 个响应）和 WebSocket 一次发多帧是 fib 相对
`net/http` 优势最大的场景之一（HTTP/1 pipeline 是 `net/http` 的约 6.9 倍，内存约十分之一）。
这不是某个 magic flag，而是几个设计合在一起的结果：

1. **读到 short read 为止。** 边沿触发，一轮把 socket 读空，一个 `read` 里往往装着许多请求。
2. **增量解析，原地消费。** HTTP 解析器、WebSocket 帧解析器直接在本轮读 buffer 上工作，
   已解析完的部分不搬动；粘包/拆包只复制残余的那一小段。
3. **cork：一轮内所有回包先排队，轮末一次 `write`/`writev`。** N 个 pipeline 响应合成一次系统调用，
   而不是 N 次。`Send` 先尝试直接写，余量复制进队列；`SendParts` 的两段、多个缓冲的发送队列走
   `writev`。
4. **复用对象，零分配。** 请求、Header、URL、Context 默认都回收复用；Router 每请求零分配，
   在这种每秒千万级请求的场景里，GC 与堆锁才是瓶颈（见 [§6](#6-对象复用像-fasthttp-一样用-context)）。
5. **有上限的积压。** 对端不读响应时，写队列涨到 `WriteBufferHighWatermark` 就停止读它的请求
   （`MaxPendingBytes` 是全服务总量），所以 pipeline 压测中内存稳定在几十 MB，而不是涨到几百 MB。

**你自己写协议时怎么吃到同样的红利**

- 在 `OnData` 里 **循环解析、循环 `Send`**，不要每个请求一次 `Flush`：

  ```go
  Data: func(c *fib.Connection, data []byte) {
      for len(data) > 0 {
          msg, n, ok := parse(data)
          if !ok { break }                 // 半包：自己保存残余，下次拼
          data = data[n:]
          _ = c.Send(handle(msg))          // 先排队；轮末合并成一次写
      }
  }
  ```
- 水位线要大于**一轮读取**产生的回包总量，而不只是单条消息：一轮读取是 corked 的，
  一轮内所有回包会短暂地同时在队列里。一轮最多读 `ReadBufferSize` 字节（默认 16KiB），
  1KiB 的回包配 8KiB 水位线才有 8 倍余量。水位线太小会让 pipeline 触发不必要的暂停读。
- 在异步分支（别的 goroutine）里一次回复该连接的多个请求时，先 `c.Cork()`，回完所有再
  `c.Flush()`，让它们同样合并成一次写（见 [§9](#9-写侧cork--flush--sendparts--sendfile)）。

**HTTP/1 上的 pipeline 与异步**

被 `Retain` 的请求会挡住它之后的 pipeline 请求直到释放。这是**保序**的代价，也是唯一
正确的做法。想要无队头阻塞，就用 HTTP/2、HTTP/3，或者让客户端别 pipeline。

---

## 5. Multiplex（HTTP/2、HTTP/3）：handler 池与 `MaxConcurrentHandlers`

HTTP/2、HTTP/3 的连接是**一个读者、多个请求**：读和解析（分帧、HPACK/QPACK、QUIC 包）在
Engine 的 worker 上，**handler 不在读连接的协程上执行**，而是交给 Engine 的 handler 池
（`Engine.HandlerPool()`，名字 `<Name>-streams`）。结果：

- 同一连接上并发到达的请求并发处理，阻塞的 handler 只拖累它自己。
- 读者永远不会因为某个 handler 慢而停止读（所以 multiplexed 连接上**不能**靠停止读做背压；
  背压改由 stream 流控窗口实现——窗口在 handler 消费 body 之后才补）。

**为什么 streams 池是独立的、而且不是 Engine 的池**：Engine 的 worker 解析出请求后要往 streams
池提交。若两者是同一个池，队列满时所有 worker 都可能卡在提交上，没人取任务，连事件循环也会卡住。
等待只有一个方向（engine worker → 其它池），所以不会互相死锁。

**限制单连接并发**

```go
httpConfig := fibhttp.DefaultConfig()
httpConfig.StreamPool.MaxConcurrentHandlers = 8   // 单连接最多并发 8 个请求
```

第 N 个请求直接在读连接的协程上执行，它返回前该连接不再读新数据——**上限由对端的流控承担，
服务端不需要排队**。N=1（或 `StreamPool.Disable`）是逐个执行的旧行为。这是一个很便宜的
"每连接并发限流器"，适合 handler 很重、或者你想让一个连接内的请求保序的场景。

**多路复用 + Retain**

HTTP/2/3 上 `Retain` 只挂起这一个 stream，其它 stream 照常进出；一个连接就能同时挂着几百个
异步请求，这对网关/聚合类服务是质变：一个连接上 N 个请求并发在途，却没有 N 个 goroutine。

```go
// 不论 h1/h2/h3，写法一样：
c.Retain()
upstream.Do(req, func(resp *fibhttp.ClientResponse, err error) { ...; c.Release() })
```

**其它 multiplex 相关**

- `Config.MaxConcurrentStreams`（默认 250）限制单连接 stream 数，超出的被 `REFUSED_STREAM`。
- 响应写在 stream 自己的 DATA 帧里，遵守对端窗口；窗口不够的 body 暂存，等 `WINDOW_UPDATE`。
- 1xx 中间响应：`c.WriteInterim(103, hdr)` 发 Early Hints，比 server push 更值得用
  （主流浏览器已关闭 push）。
- 客户端：`fibhttp.Client` 在 h2 上一个连接并发多个请求，超时/取消只 `RST_STREAM` 对应 stream，
  GOAWAY / `REFUSED_STREAM` 时自动在新连接上重发服务端没处理的请求。

---

## 6. 对象复用：像 fasthttp 一样用 Context

`Config.ReuseRequests`、`ReuseHeaders`、`ReuseURLs`、`ReuseContexts` 默认全部开启。这是
fib 高并发场景下吞吐的一大来源——请求速率很高、核数很多时，**所有核共用一个堆**，GC 与堆锁
成为瓶颈（HttpArena 64 核：不复用时 baseline 123 万 rps、只用到 36 个核，复用后 163 万；
pipelined 570 万 → 1500 万）。

**代价：规则与 fasthttp 的 `RequestCtx` 一样。**

> 被复用的对象在 handler 用完这个请求之前属于它——handler 已经返回、并且释放了它取得的
> 每一个 `Retain`。之后它就会交给下一个请求（可能在这条连接，也可能在别的连接）。

实践上：

- **不要把 `*http.Request`、`c.Request.Header`、`c.Request.URL`、`*Context`、`Context.Body()` 的切片交给一个活得比
  响应更久的 goroutine**，除非你 `Retain` 了并且在它用完之后才 `Release`。
- 要保留就拷贝：`c.Request.Clone(ctx)`、`c.Request.Header.Clone()`、`append([]byte(nil), body...)`。
- 为 `net/http` 写的、返回后还持有 `*http.Request` 的 handler（例如丢给一个没 `Retain` 的
  goroutine）：关掉 `ReuseRequests`，或者先 `c.Request.Clone(ctx)`。
- 每个选项只管自己那个对象，所以只在乎 `Context` 的 handler 可以保留其余对象的复用。
- 等待下一个请求的 `Context` 处于"已结束"状态：误调用的 `Retain`/`Release`/`Respond`
  是空操作，不会写坏别的请求——**但这也意味着 bug 会被静默吞掉**，在测试中请用
  `-race` 并检查这类调用。

```go
// 想让某个重 handler 安全地异步使用 request 里的数据：
c.Retain()
q := c.Query("q")                       // 零拷贝，是 c.Request.URL.RawQuery 的子串
q = strings.Clone(q)                    // 要跨出 handler，就拷贝
go func() {
    defer c.Release()
    _ = c.Respond(200, "text/plain", search(q))
}()
```

HTTP/3 同样复用请求 stream，`http3.Config.DisableReuse` 关闭。

---

## 7. 零拷贝读：`Context.Body` / `Context.Query`

- `Context.Body()`：非流式 body（handler 运行前已整体读完）直接返回 server 自己的 buffer。
  `io.ReadAll(c.Request.Body)` 要用逐步增长的 buffer 再拷贝一遍。这些字节**在响应结束之前归 handler 所有，
  不可修改，也不可保留**。`Respond`/`Write` 都会拷贝，所以 echo 可以直接
  `c.Respond(200, "application/octet-stream", c.Body())`。流式 body 时返回 nil。
- `Context.Query(name)`：和 `c.Request.URL.Query().Get(name)` 解码方式相同，但不构建 map；
  不需要解码的值就是原始 query 的子串，零分配。
- `Router` 的路由状态挂在 `Context` 上，随 Context 复用：路由一个请求零分配；`c.Param(name)` /
  `c.Params()` / `c.RoutePattern()` 读取。`SetPathValues(true)` 让 `c.Request.PathValue` 也可用，
  代价是每个带参数的请求多一次分配。
- 这里的 "零拷贝" 是**借用**：和 §6 的生命周期规则叠加使用，别带出 handler 之外。

---

## 8. 读背压：`HoldReads`

写方向 fib 自动有两级水位线暂停读；**读方向由应用决定**：

```go
Data: func(c *fib.Connection, data []byte) {
    q.push(append([]byte(nil), data...)) // data 只在本次回调内有效
    if q.size() >= highWater {
        c.HoldReads(true)                // 在 OnData 里调用时，本轮读循环也立即停下
    }
},
// 另一个 goroutine 消费到低水位之后：
//    c.HoldReads(false)                 // 重新注册读事件，socket 里攒下的数据直接投递
```

- 数据留在**内核缓冲区**，对端被 TCP 窗口自然减速；你这一侧的内存不会无限增长。
- 恢复读时 **不需要对端再发任何字节**：重新注册读事件会把已攒下的数据直接投递。
- 可以在任意 goroutine 调用，不嵌套（以最后一次调用为准），`ReadsHeld()` 查状态。
- UDP 连接上不生效（数据报已被事件循环读走）。
- `fibhttp` 的流式 body 就是用它做背压的；你自己的二进制协议、转发代理想要"慢消费者不撑爆内存"，
  就用它。
- 这与"回调里阻塞"根本不同：阻塞会占着 worker；`HoldReads` 让连接**不再被调度**，worker 立即还给池。

排查"明明水位线远大于单条消息却触发了背压"：看 `Engine.Stats()` 的 `ReadsPausedByWatermark`
和 `ReadsPausedByBudget`，后者是 `MaxPendingBytes` 总预算被别的连接耗尽——**一条自己几乎没有积压的
连接也会因此被暂停读**。

---

## 9. 写侧：`Cork` / `Flush` / `SendParts` / `SendFile`

| API | 作用 | 什么时候用 |
| --- | --- | --- |
| `Send(b)` | 先尝试直接写，余量复制进发送队列；永不阻塞 | 默认 |
| `SendParts(a, b)` | 两段数据不拼接，走 `writev`；背压时只复制未发出的后缀 | 帧头 + payload，避免拼接拷贝 |
| `SendFile(f, off, n)` | 排进发送队列，与前后 `Send` 保序；`sendfile(2)` 直发，随对端接收进度读文件 | 大文件，内存不随文件大小增长 |
| `Cork()` / `Flush()` | 手动控制合并写 | 在**本轮之外**回复多个请求；或在 handler 里需要中途把数据交给 socket |

**在 OnData 之外手动 cork**

`OnData` 里的 `Send` 自动被 cork（轮末合并写）。在**别的 goroutine**里，一个连接要回复多条消息时：

```go
c.Cork()
for _, r := range replies {
    _ = c.Send(r)
}
_ = c.Flush()          // ← 必须 Flush：没有别的东西会写 Cork 住的数据
```

> 除非该连接的某一轮恰好结束并顺带 flush 了它。**别依赖这个。**

**流式响应要 `Flush`**

HTTP/1 的 `Context` 实现 `http.Flusher`：`fmt.Fprintf(c, ...)` + `c.Flush()` 立即发出，
不等 handler 返回。SSE 与大文件下载都是这样（`http.ServeFile(c, c.Request, path)` 走 `sendfile`）。

**`SendFile` 的细节**

- 连接复制文件描述符，`f` 可以立刻关闭。
- 文件比声明的短会关连接（对端在等那些字节）。
- TLS 连接无法零拷贝：`SendFile` 返回前读出整段并加密，整段排队在内存里。
- 小于 16KB 的范围在 HTTP 里直接拷贝，比多一次系统调用更省。

---

## 10. `Read` 不阻塞、deadline 是关连接

`*fib.Connection` 实现了 `net.Conn`，但**语义和阻塞 socket 不同**：

- **`Read` 不阻塞**：socket 里没有数据返回 `fib.ErrWouldBlock`。正常数据通过 `OnData` 交付；
  `Read` 是给"想在 `OnData` 里自己把剩余报文读完"的 handler 用的。**不要把它交给 `bufio.Reader` 之类
  期待阻塞语义的代码。**
- **`Write` 不等待**：走 `Send`，拷贝并排队。
- **deadline 不是让调用失败，而是直接关连接**：`SetReadDeadline(t)` 到时关连接，`OnClose` 收到
  `os.ErrDeadlineExceeded`。需要滚动超时就每次收到数据时重设；重设同一时间点不会重建定时器，
  所以每轮都重设没有额外开销。
- 关闭之后 `Read`/`Write` 返回关闭的原因（超时是 `os.ErrDeadlineExceeded`，其余 `net.ErrClosed`）。

**能干的"邪道"**

- **`Read` 用来把 `OnData` 之外还躺在 socket 里的报文取走**：`OnData` 给的是本轮已读到的数据，
  还想继续读就调 `c.Read(buf)`，直到 `ErrWouldBlock`，不必等下一轮调度。半包的残余要自己保存，
  下次 `OnData` 再拼。
- **`Attachment` / `SetAttachment`**：每连接挂任意状态，不用 `map[*Connection]State`
  加锁，也不会泄漏。
- **`c.IsAccepted()` / `c.IsDialed()`**：同一个 handler 同时服务 accept 和 dial 出来的连接时区分两端。
- **`c.CloseWrite()`**：等 `Send` 收下的数据全部交给内核后才 shutdown 写方向，对端读完数据再读到
  EOF——做"回完就半关"的协议很有用。**它在 Layer 之下工作，不会发 TLS 的 close_notify。**
- **`c.CloseRead()`**：之后不再回调 `OnData`，对端 half-close 的 EOF 也不再关连接，连接仍可发送。
  注意 macOS 上 shutdown 读方向后对端再发数据内核会 reset 连接。

---

## 11. WebSocket：按帧接收（OnFrame）

`OnMessage` 收到的是重组后的**完整**消息，大消息要先在内存里拼起来。设置
`HandlerFuncs.Frame` 后消息**按帧交付，不再重组**：

```go
handler := websocket.HandlerFuncs{
    // 设置 Frame 后 Text/Binary 不再回调 Message
    Frame: func(c *websocket.Connection, op websocket.Opcode, fin bool, data []byte) {
        _, _ = file.Write(data) // 边收边处理，连接不持有整条消息
        if fin { _ = file.Close() }
    },
}
```

- 帧之间**不缓存任何 payload**：对端可以发送远大于 `MaxMessageBytes` 的消息，此时它限制的是**单帧**。
- 每帧带消息自己的 opcode（Text/Binary，不会是 Continuation）和 `fin`；未分片消息就是一次 `fin=true`。
- Text 的 UTF-8 仍增量校验（字符可跨帧），非法以 1007 关闭。
- **例外**：压缩消息（permessage-deflate）的各帧是同一个 DEFLATE 流的片段，不能逐帧解压，
  仍然重组、解压后作为一次 `fin=true` 交付。
- 与 `OnBody` 是同一个思路：大载荷边收边处理。适合文件传输、日志上传、流式 ASR 等。

**WebSocket 其它值得知道的**

- permessage-deflate 发送端**不保留上下文**，每条消息从 `sync.Pool` 借 `flate.BestSpeed`
  压缩器，连接本身不持有压缩器——大量连接时内存很小。
- 自定义 `Ping` handler 会**替换**默认的 Pong 回复（和 gorilla 一致），需要回时自己 `c.Pong(payload)`。
- 回调与 `OnMessage` 在同一个 worker 上串行，payload 只在回调期间有效。
- pipeline 的 WebSocket 帧也是原地解析、同轮合并成一次 `writev`（见 [§4](#4-pipeline-优化一轮一写)）。
- 在 HTTP server 里升级：`ws.Upgrade(c, nil)`；h2/h3 上是 Extended CONNECT
  （RFC 8441 / 9220），只有这个 stream 切走，同连接别的请求照常。

---

## 12. 同一个 Engine 同时当 server 和 client

fib 的 client（`Engine.Dial`、`fibhttp.Client`、`websocket.Dialer`、`http3` client、`arpc.Dial`、
`grpc.NewClient`）都是**回调式、不阻塞调用方**的，且和 server 共用同一个事件循环、同一个 worker 池、
同一套背压。

```go
engine, _ := fib.Bind(config, handler)           // server
client := fibhttp.NewClient(engine, fibhttp.DefaultClientConfig()) // 复用 server 的 Engine
```

- 做网关/代理时，上游 client 用**同一个 Engine**，不需要第二套线程/池。
- `Engine.DialWithHandler(...)`：这条连接用**自己的 handler**，Engine 的 handler 收不到——
  同一个 Engine 因此能同时承载使用不同协议的 server 和 client。
- `fib.NewEngine(config, nil)` 创建**不监听**的 Engine，只用于 dial。
- `client.Go(req).Wait()` 是 Future 形式，需要阻塞的地方（比如 `main`、测试）用。**不要在 handler 里
  `Wait()`**——那会占 worker。handler 里请用回调 + `Retain`/`Release`。
- 回调可能在任意 goroutine 上执行（读响应的 worker、定时器 goroutine、`fib-client` 池、调用方
  goroutine），**不要长时间阻塞**。
- client 的 DNS 解析、完成回调在进程级 `fib-client` 池里，TLS 握手在 `fib-tls-handshake`，
  都不占用 Engine 的 worker。
- 关停顺序：先 `client.Close()`，再关 Engine。直接关 Engine 不会通知 client，已发出的请求只能等超时。

---

## 13. 一个 Handler 同时服务 h1 / h2 / h3，以及 Upgrade / Tunnel

- **同一个 `fibhttp.Handler` 同时服务 HTTP/1、HTTP/2（TLS+ALPN、h2c prior knowledge、
  `Upgrade: h2c`）和 HTTP/3**，handler 代码一行都不用改。`Request.Proto` 告诉你是哪个。
  HTTP/3 在同一端口号上用 UDP，TCP 上带 `Alt-Svc` 引导浏览器切换。
- `Config.HTTP2Only`、`Config.DisableHTTP2` 可以强制其一。
- `ResponseWriter` 的 `Write`/`Flush` 在 h1 上即刻发出；在 h2/h3 上响应会先缓存、handler 返回后整体发送。
  超大响应请用 h1 的流式，或 `FileCache` / `SendFile`。
- **`Context.Upgrade(protocol, header, TunnelHandler)`** 回 101（或 h2/h3 上回 200）并把连接/stream
  切换给 `TunnelHandler`，WebSocket 的 `Upgrade` 就是这么实现的。你自己的协议（自定义二进制、
  CONNECT 代理、隧道）可以直接借用：HTTP 上做握手、鉴权、路由，然后无缝切到原始字节流。

---

## 14. FileCache 与 sendfile：静态文件不碰磁盘、不过用户态

```go
files, _ := fibhttp.NewFileCache(fibhttp.FileCacheConfig{Root: "/data/static", Precompressed: true})
files.ServeFile(c, strings.TrimPrefix(c.Request.URL.Path, "/static/"))
```

- 首次请求读入内存，之后**跟随磁盘**：Linux 上 inotify watch 目录，创建/写入/`mv`/删除瞬间让条目失效，
  请求本身不 stat；其它平台每个请求 stat 一次。
- `Precompressed`：按 `Accept-Encoding`（含 q 值）选磁盘上预压缩好的 `name.br` / `name.gz`，
  设置 `Content-Encoding` 与 `Vary`——**压缩的 CPU 在部署时花，不在请求时花**。
- 不带条件、不带 Range 的 GET/HEAD 直接从内存写出，零分配；条件/Range 交给 `net/http.ServeContent`。
- 大于 `MaxFileBytes`（默认 1MB）的文件每次从磁盘发，走 `SendFile`；总量受 `MaxBytes`（默认 64MB）限制。
- 想要纯 `sendfile`：`http.ServeFile(c, c.Request, path)` / `http.FileServer`（`Context` 实现了 `io.ReaderFrom`，
  普通文件和包着文件的 `*io.LimitedReader` 会走 `SendFile`）。

---

## 15. UDP：`DatagramsHandler` 与 `SendBatch`

- 监听 socket 上**每个对端地址是一条 `Connection`**：第一个数据报触发 `OnOpen`，之后同一对端的数据报
  都交给它；`UDPIdleTimeout`（默认 60 秒）后关闭。
- **`DatagramsHandler.OnDatagrams`**：Handler 实现它后，worker 运行时已排队的数据报按序**一次性**交付，
  可以对整批数据**一起回 ACK**，少发数据报。这是 QUIC / HTTP/3 的 ACK 合并所用的技巧，也适合你自己的
  可靠 UDP 协议。
- 数据报来自 `bufferpool`，用完且不再引用时 `bufferpool.Put` 还回去。**装它们的切片不能在
  `OnDatagrams` 返回后持有**。
- **`SendBatch`**：Linux 用 `sendmmsg`、macOS 用 `sendmsg_x`，一次系统调用发一批。
- UDP 发送从不排队：socket 没空间就**丢包**并返回错误，连接保持打开；UDP 不参与写水位。
- Linux 上设置 `ReusePort` 时，每个 poller 自己绑定一个 `SO_REUSEPORT` UDP socket 自己读，
  内核按四元组哈希把数据报交给其中一个，没有惊群；同一对端永远落在同一个 poller 上。

---

## 16. 协程池：自己的池、独立的池、不要死锁

fib 的 worker 池（`taskpool`）可以单独用，也可以替换 Engine 的：

```go
pool := taskpool.NewAdaptive(taskpool.AdaptiveConfig{
    Name: "jobs", MinWorkers: 16, MaxWorkers: 4096, QueueSize: 10000,
})
config.SetTaskPool(pool)          // Engine 用你的池；关闭 Engine 不会停它
```

- **异步 handler 的"工作池"请用独立的池，不要用 Engine 自己的**。Engine 的 worker 要往你的池提交，
  若是同一个池，队列满时所有 worker 都卡在提交上，事件循环也卡住。fib 内部也是这么做的：
  `<Name>-streams`（HTTP/2、HTTP/3 的 handler）、`fib-client`、`fib-tls-handshake` 全是独立的池，
  **等待只有 engine worker → 其它池一个方向**。
- 阻塞型 handler（要等应用自己的 I/O）放在 `<Name>-streams`（上限是 Engine 池的 2 倍）或你自己的池里，
  而不是 Engine 的池里。
- `SharedTaskPool`（默认开）：同进程内**同名**的 Engine 共享 worker 和队列，避免多监听端口重复创建
  大量 goroutine。共享只看名字不看配置：**第一个 Engine 的配置生效**，之后同名 Engine 的相关配置被忽略。
  需要隔离就给不同的 `Name`，或设 `SharedTaskPool=false`。
- `taskpool.ModeInline` 不起 worker、直接在提交者 goroutine 上带 recover 执行，**Engine 不接受它**
  （那样连接的一轮就会跑在事件循环上），但作为独立库功能可用。
- `Resize(min, max)` 运行中调整上下限；`Workers()` 看当前数量。
- `arpc` / `gRPC`：handler 默认在 streams 池异步执行；`Handler.SetAsyncResponse(false)` 或
  `Handle(method, h, false)` 让某个方法在读连接的 worker 上**同步**执行（极短的 handler 省一次调度）；
  `SetTaskPool` / `grpc.TaskPool` 换成自己的池。

---

## 17. 进程级技巧：Prefork、IOPollers、GOMAXPROCS

**Prefork**（Linux）：多进程 + `SO_REUSEPORT`，每个子进程有自己的堆和 GC。

```go
func main() {
    if err := prefork.Run(prefork.Config{}, run); err != nil { log.Fatal(err) }
}
```

- 为什么：一个进程有 64 个 P 时，GC 标记阶段的工作缓冲区、堆锁是所有 P 共用的，分配多的服务大部分时间
  花在这上面而不是请求上。64 核 HttpArena：pipelined 1820 万 → 2540 万，latency-1m 的 p99 从
  1.6ms → 124µs 同时少用四分之一核。
- 默认每个子进程 `GOMAXPROCS=2`（`ProcsPerChild`），子进程数 = master 的 GOMAXPROCS / 2。
- **只有子进程需要的初始化放进 `serve`**；`Run` 之前的代码 master 和每个子进程都会跑一遍。
  子进程之间除了端口什么都不共享，内存里的状态每个子进程一份——
  整机只有一份的资源（如 DB 连接上限）按 `prefork.Children()` 分。
- UDP（HTTP/3）：对端地址变化（NAT 重绑定/迁移）后可能落到另一个子进程，那里没有这条 QUIC 连接。
- 子进程里 fib 的默认值随之变化：CPU 数取 `min(NumCPU, GOMAXPROCS)`，≤4 个 P 不开 IOPollers。

**IOPollers**：把 Engine 拆到多个事件循环（Linux epoll / macOS kqueue）。默认 CPU>4 时开启；
poller 数默认 `max(1, CPU/4)`（32 CPU 以内），更多时 `CPU/2`。poller 只等事件、把变为可执行的连接交给
worker，**多开 poller 会和 worker 抢 P，通常更慢**。poller 自己的工作（accept）成为瓶颈时才划算：
连接频繁建立/关闭的场景（limited-conn）里，Linux 上 poller 自己 `SO_REUSEPORT` accept，从每秒 94 万
请求提到 165 万。

**GOMAXPROCS**：**保持默认（等于核数），不要调高**。早期架构下 2 倍核数更快，现在事件循环停在 Go 的
netpoller 里，这个收益已经不存在，调高反而更慢且内存多 25–40%。`runtime.NumCPU()` 取的是进程的 CPU
亲和性掩码，被 taskset/cpuset 限制时它已经是实际可用核数。

---

## 18. bufferpool：自己的 buffer 也走同一个池

```go
buf := bufferpool.Get(n)                    // len=n, cap=Align(n)；内容是上个使用者留下的，先写后读
defer bufferpool.Put(buf)                   // Put 之后连子切片都不能再用
buf = bufferpool.Append(buf, data)
out := bufferpool.Join(nil, a, b)           // 一次取够，拼几段
```

- **Get/Put 0 分配、约 7ns**；按 2 的幂分档，正好是 Go 分配器的 size class，没有取整浪费。
- 有一份**扛得过 GC** 的储备（`sync.Pool` 每轮 GC 都清空，储备不会），大小是学出来的，上限
  `SetRetainedBytes`（默认 32MiB）；`SetRetainedBytes(0)` 立刻释放，应对内存压力。
- **一个全局池**：fib 的读 buffer、回包拼接、TLS 记录都是同一批 buffer 在各档之间流动。
  你自己的 buffer 也走它，不会额外多一份闲置内存。
- `Put` 对来路不明的 buffer 是安全的：小于 64B 或大于 64MiB 的直接丢弃。

---

## 19. 速查：哪些东西能在哪个 goroutine 上调

| API | 可在哪调 | 备注 |
| --- | --- | --- |
| `Connection.Send` / `SendParts` / `SendFile` / `Close` | 任意 goroutine | 永不阻塞 |
| `Connection.HoldReads` | 任意 goroutine | 在 `OnData` 里调用时本轮读循环立即停 |
| `Connection.Cork` / `Flush` | 任意 goroutine | `Cork` 必须配 `Flush` |
| `Connection.Set*Deadline` | 任意 goroutine | 到期关连接，不是让调用失败 |
| `Context.Respond` / `Write` / `Finish` | 任意 goroutine，**须在最后一个 `Release` 之前** | `Respond` 与 `Write` 不能混用 |
| `Context.Retain` / `Release` | 任意 goroutine | 成对；已结束的请求上是空操作 |
| `Context.OnBody` / `OnCancel` | handler 里或其后（`OnBody` 之后在 handler 池上交付） | 回调不要阻塞 |
| `Context.Conn.Send`（h2/h3） | **不要调** | h2/h3 必须经 `Respond`/`WriteResponse` |
| `OnOpen`（TCP） | 事件循环 | 不能阻塞 |
| `OnData` / `OnClose` | worker | 同一连接串行；`OnClose` 在其之前的 `OnData` 之后 |
| `SyscallConn().Control(f)` 里的 `f` | 持连接锁 | **不要在 f 里再调这个连接的方法**，会死锁 |
| Dial done / Client 回调 | 任意 goroutine（见 §12） | 不要长时间阻塞 |

---

## 20. 反模式清单

1. **在 handler / `OnData` / `OnBody` 回调里阻塞**（`time.Sleep`、同步 RPC、`future.Wait()`、
   `io.ReadAll(远端)`、磁盘慢 I/O）。占的是 worker。改用 `Retain` + 回调，或交给自己的池。
2. **`Retain` 了没 `Release`**（包括出错分支、`OnCancel` 分支、panic 分支）。连接被一直占住。
   用 `defer` 或 `sync.Once`，保证每个 `Retain` 恰好对应一次 `Release`。
3. **`Release` 之后还碰 `r`/`c`/`c.Body()`**，或把它们交给活得更久的 goroutine（对象复用下会读到
   别的请求的数据，且没有 panic）。
4. **在 `Release` 之后才 `Respond`**，响应已经结束，写入返回错误。
5. **把 `data` 存起来**（`OnData`、`OnBody`、`OnFrame`、`Ping/Pong` payload 全都只在回调内有效）。
6. **对 `Connection` 用 `bufio.Reader`**，或期待 `Read` 阻塞。
7. **水位线设得小于一轮读取的回包总量**，pipeline 下触发无谓的暂停读。
8. **开了 `StreamRequestBody` 却保留默认 `ReadTimeout`**，大上传被超时切断。
9. **把异步工作丢进 Engine 的池**，或让同名 Engine 的 `SharedTaskPool` 配置悄悄被第一个 Engine 覆盖。
10. **把 GOMAXPROCS 调大**以为更快。
11. **在 `Close` 自己连接之后，在同一个 `OnData` 里等 `OnClose` 的通知**（比如 `OnCancel`）：死锁。
12. **在 handler 里 `Wait()` 一个 client future**：占 worker 直到上游返回。

---

## 相关文档

- [指南（中文）](guide.zh-CN.md)：各个 API 的完整说明
- [HTTP/1.x](http1.zh-CN.md) · [HTTP/2](http2.zh-CN.md) · [HTTP/3](http3.zh-CN.md)：限制与一致性
- [`examples/http/upload`](../examples/http/upload)：`OnBody` 大文件上传的完整示例
- [`taskpool`](../taskpool)：独立可用的协程池与基准测试

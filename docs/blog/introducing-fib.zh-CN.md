# fib：性能几乎超过以往所有 Go 网络库，我和 Claude 合作完成，代码几乎全部由 Claude 生成

[English](introducing-fib.md) | 简体中文

项目地址：<https://github.com/lesismal/fib>

最近我和 Claude 合作完成了一个新的 Go 网络库，叫 fib。想法和方向是我的，代码几乎全部由 Claude 生成。它支持 TCP/UDP、TLS、HTTP/1.x、HTTP/2、HTTP/3（QUIC）和 WebSocket，HTTP handler 用的是标准库的 `*http.Request` 和 `http.ResponseWriter`。

先说结果。在 GitHub Actions 上自动运行的 benchmark 里，和标准库相比，fib 在一些场景下的吞吐能高出数倍，内存也少得多；和 fasthttp 这类以性能著称的库相比，性能也有提升；和参测的其他 Go 库相比，除了 WebSocket pipeline 还落后 uws 和 fnet，其余场景 fib 都排在第一。和 C/C++/Rust 的框架相比，也已经很接近，有些场景还能超过。这些 Action 所在的仓库里有完整的测试代码，每个数字都可以追溯和复现。

当然，不同的硬件规格、测试参数和框架配置，跑出来的结果可能会不一样。欢迎大家在自己的机器上亲自跑一跑这些 benchmark。

## 先看数据

下面的数据都来自 GitHub Actions 上 `lesismal/go-*-benchmark` 几个仓库的 Docker 压测。runner 是 4 vCPU，服务端 2 核、客户端 2 核，1 万连接，1 KiB payload。每个都是单次运行，共享 runner 有噪声，几个百分点的差距可以当作持平。完整表格（包括 CPU 效率、延迟分位数）请点链接看。

先把几个主要的提升列在前面：

- HTTP/1.1：echo 是 net/http 的 2.3 倍，pipeline 是 net/http 的 6.9 倍、fasthttp 的 1.65 倍，内存只有 net/http 的十分之一
- HTTP/2：echo 是 net/http 的 3.7 倍，multiplex 是 net/http 的 13 倍、Rust h2 的 2.6 倍
- HTTP/3：吞吐是 quic-go 的 3.4 到 5.4 倍，达到 Cloudflare quiche 的九成以上
- TLS：pipeline 是 crypto/tls + net 的 2.9 倍，内存约三分之一
- WebSocket：echo 吞吐在 17 个参测框架里最高，只用 32 MB 内存

**HTTP/1.1**（[Actions run](https://github.com/lesismal/go-http1-benchmark/actions/runs/36839290265)，客户端是 Rust tokio）

| Server | Echo TPS | Echo MEM | Pipeline TPS | Pipeline MEM |
| --- | ---: | ---: | ---: | ---: |
| fib | 194,671 | 32 MB | 1,140,457 | 36 MB |
| axum (Rust) | 236,244 | 227 MB | 301,804 | 490 MB |
| fasthttp | 139,830 | 197 MB | 692,671 | 197 MB |
| net/http | 84,739 | 314 MB | 164,811 | 323 MB |

echo 比 fasthttp 高 39%，内存只有它的六分之一。pipeline 场景差距最大，fib 跑到 114 万 TPS，是 axum 的 3.8 倍，内存始终在 36 MB 左右。在我自己不同连接数的测试里，fib 也基本能达到或超过 fasthttp。

**HTTP/2 h2c**（[Actions run](https://github.com/lesismal/go-http2-benchmark/actions/runs/36839294047)）

| Server | Echo TPS | Echo MEM | Multiplex TPS | Multiplex MEM |
| --- | ---: | ---: | ---: | ---: |
| fib | 129,676 | 119 MB | 333,692 | 432 MB |
| h2 (Rust) | 143,107 | 246 MB | 129,729 | 870 MB |
| net/http | 35,415 | 633 MB | 25,051 | 809 MB |

echo 达到 Rust h2 的九成，内存只有它的一半；multiplex 则反过来领先 h2 2.6 倍。

**HTTP/3**（[Actions run](https://github.com/lesismal/go-http3-benchmark/actions/runs/36839296383)，客户端是 quiche）

| Server | Echo TPS | Echo MEM | Multiplex TPS | Multiplex MEM |
| --- | ---: | ---: | ---: | ---: |
| quiche (Rust) | 72,919 | 387 MB | 101,233 | 881 MB |
| fib | 67,396 | 504 MB | 94,443 | 1.05 GB |
| quic-go | 19,545 | 1.20 GB | 17,505 | 1.80 GB |

这是提升最明显的一组：和 quic-go 相比，吞吐翻了几倍，内存还少了四到六成。和 quiche 比，内存上还多一些，这是接下来要继续做的。

**TLS**（[Actions run](https://github.com/lesismal/go-tls-benchmark/actions/runs/36839299713)，客户端是 C 的 uSockets + BoringSSL）

| Server | TLS 1.3 Echo | TLS 1.3 Pipeline | MEM |
| --- | ---: | ---: | ---: |
| fib | 103,427 | 690,000 | 180 MB |
| crypto/tls + net | 92,494 | 241,106 | 285–518 MB |
| uSockets (C) | 103,972 | 735,000 | 51 MB |
| rustls (Rust) | 107,406 | 195,835 | 110–201 MB |

echo 和 C、Rust 的实现相差不到 4%，pipeline 到了 uSockets 的九成以上，是 rustls 的 3.5 倍。

**WebSocket**（[Actions run](https://github.com/lesismal/go-websocket-benchmark/actions/runs/36999378687)，客户端是 C++ uWebSockets）

| Server | Echo TPS | Echo MEM | Pipeline TPS | Pipeline MEM |
| --- | ---: | ---: | ---: | ---: |
| fib | 138,592 | 32 MB | 984,917 | 32 MB |
| gorilla | 122,296 | 229 MB | 205,224 | 235 MB |
| nbio | 121,398 | 62 MB | 191,091 | 100 MB |
| tokio-tungstenite (Rust) | 83,841 | 105 MB | 748,297 | 286 MB |

pipeline 是 gorilla 的 4.8 倍，内存是它的七分之一，不过还落后 uws 和 fnet 一些。

除了网络层，fib 用的协程池 [`taskpool`](https://github.com/lesismal/fib/tree/main/taskpool) 也可以单独使用。CI 每次 push 都会把它和 nbio、ants、gopool、fnet 在 Linux、macOS、Windows 上对比一遍，[结果](https://github.com/lesismal/fib/actions/runs/37020403709)在事件循环式的提交场景下领先比较明显。

这些提升不是靠某个技巧换来的，而是来自一种和标准库不太一样的架构。要说清楚它，得先从线程说起。

## 从线程说起

传统的 C/C++ 服务端大多是单进程、少量线程的架构。线程很贵：栈要占内存，切换要进内核，开几百上千个就已经很吃力了。线程少，阻塞的代价就高，一个线程卡在一次慢查询上，它负责的所有请求都得跟着等。所以 C/C++ 的网络库基本都走同一条路：epoll/kqueue 做事件驱动，socket 设成非阻塞，业务逻辑拆成回调，耗时的事情丢给线程池。性能是有了，代价是代码被切成一段一段的回调，状态要自己保存，写起来和读起来都累。

C/C++ 和很多其他语言后来也有了协程，C++20 的 coroutine、各种第三方库、Rust 的 async/await 都是。但大多数情况下，协程的生命周期和调度仍然要自己操心：谁来 resume，在哪个线程上跑，帧和栈归谁管，跨过挂起点的引用还有没有效。跟 Erlang 的进程、Go 的协程比起来，这些方案理解和使用的门槛都高不少。

## Go 的协程和它的边界

Go 在这一点上舒服得多。协程比进程、线程轻得多，几千到几万个协程照样跑得很顺。标准库的 `net` 其实也是事件驱动的：runtime 里的 netpoller 用 epoll/kqueue 等事件，事件来了再唤醒阻塞在 `Read` 上的协程。对用户来说接口是同步的，这也是 Go 做服务端这么流行的原因之一。

但标准库的用法比较单一。拿到一个 `net.Conn`，通常至少要有一个协程一直在 `Read` 它。1 万个连接就是 1 万个协程，100 万连接就是 100 万个协程，每个协程有自己的栈，再加上每个连接的 `bufio.Reader`、`bufio.Writer`。连接大部分时间是空闲的，这些内存却一直占着，GC 和调度器的压力也跟着上来。上面表格里 net/http 动辄几百 MB 的内存，主要就是这么来的。也有一些框架用 pre-fork 多进程的办法，把连接分散到多个进程里，减轻单个进程里 runtime 的压力，但这又带来了进程间协作、部署和监控上的麻烦。

## fib 的做法：事件驱动 + 非阻塞接口 + 可配置的协程池

这些年我一直在做的是另一种方案。和 runtime 一样，用 epoll/kqueue/IOCP 做事件驱动，但不给每个连接配一个常驻的读协程。连接可读时，事件循环把它交给协程池里的一个 worker，worker 读数据、解析协议、调用户的 handler、把响应写回去，这一轮做完 worker 就回到池里。空闲的连接只是一个结构体，不占协程，也不占读写缓冲。前面表格里 fib 几十 MB 的内存，就是这么省下来的。

关键在协程池的大小。C/C++ 的线程池一般也就几十个线程，handler 里一阻塞，池子很快就耗尽，其他连接只能干等，所以只能全异步。Go 不一样，即使是限制了大小的协程池，也可以开到几千甚至几万个协程。这个并发度足够让用户在 handler 里继续用同步的方式调用下游的基础设施，比如 RPC、SQL，不用担心几个慢请求把池子占满，让其他连接等很久。

这样一来，连接数由事件驱动来承载，并发度由协程池来承载，业务代码还是同步的写法，连接数和协程数不再绑在一起。

fib 默认的池子上限是 `GOMAXPROCS × 1000` 个 worker，随负载伸缩，闲下来的 worker 会被回收，所以上限设得高并不浪费。如果你的 handler 基本不阻塞，可以调小；如果下游很慢，可以调大；也可以换成自己的池：

```go
config := fib.DefaultConfig()
config.SetPoolSizing(20000, 0) // worker 上限
// 或者 config.SetTaskPool(myPool)
```

## 从 nbio 到 fib：和 Claude 一起写

这个方案我不是第一次做。前几年我手写了 [nbio](https://github.com/lesismal/nbio)，支持 TLS、HTTP/1.x 和 WebSocket，也有不少朋友在生产环境里用。但一个人的精力有限，HTTP/2、HTTP/3、QUIC 这些协议工作量太大，一直没能做。

这一两年 AI 模型进步得很快，于是有了 fib。这次是我和 Claude 合作，代码几乎全部由 Claude 生成。到目前为止，fib 支持：

- TCP、UDP、Unix socket，Linux 上用 epoll，macOS 上用 kqueue，Windows 上用 IOCP
- TLS 1.0–1.3，握手用 crypto/tls，握手之后的记录层加解密由 fib 自己做
- HTTP/1.x、HTTP/2（h2 和 h2c）、HTTP/3，其中 QUIC 和 QPACK 是从头写的，不依赖 quic-go
- WebSocket，可以跑在 HTTP/1.1、HTTP/2 和 HTTP/3 上
- 异步的 HTTP/1、2、3 和 WebSocket 客户端，chi 风格的 Router，常用的中间件

## 用起来和标准库差不多

性能和架构说了这么多，用起来其实很简单。HTTP handler 用的是标准库的 `*http.Request` 和 `http.ResponseWriter`，所以现有的 `net/http` handler、`http.ServeMux`、`http.FileServer`，甚至 chi、gin 的 engine，都可以直接跑在 fib 上：

```go
mux := http.NewServeMux()
mux.HandleFunc("GET /hello/{name}", func(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "hello %s over %s\n", r.PathValue("name"), r.Proto)
})

config := fib.DefaultConfig()
config.Addr = "127.0.0.1:8080"
engine, err := fib.Bind(config, fibhttp.NewHandler(fibhttp.HandlerFunc(
	func(c *fibhttp.Context, r *http.Request) { mux.ServeHTTP(c, r) },
)))
if err != nil {
	panic(err)
}
engine.Run()
```

同一个端口同时服务 HTTP/1.x 和 h2c；换成 TLS 就通过 ALPN 协商 HTTP/2；绑定一个 UDP engine 就是 HTTP/3，handler 不用改。

## 最后

代码主要由 Claude 生成，所以测试格外重要。项目做了大量的测试和 CI：HTTP/1 与 net/http、curl 的一致性测试，h2spec，和 quic-go 的 HTTP/3 互通测试，Autobahn WebSocket 测试，还有 fuzz。我自己的一些服务也已经跑在 fib 上，目前没发现什么问题。

更多内容请看 fib 的文档：[README](https://github.com/lesismal/fib/blob/main/README.zh-CN.md)、[架构](https://github.com/lesismal/fib/blob/main/docs/architecture.zh-CN.html)、[协议流程](https://github.com/lesismal/fib/blob/main/docs/flows.zh-CN.html)，以及 [HTTP/1](https://github.com/lesismal/fib/blob/main/docs/http1.zh-CN.md)、[HTTP/2](https://github.com/lesismal/fib/blob/main/docs/http2.zh-CN.md)、[HTTP/3](https://github.com/lesismal/fib/blob/main/docs/http3.zh-CN.md) 的说明。

希望更多人能试试 fib，有问题、反馈或建议，欢迎提 issue 和 PR。

谢谢！

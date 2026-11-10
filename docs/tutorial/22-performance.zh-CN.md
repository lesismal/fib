# 性能与运维

[English](22-performance.md) | [简体中文](22-performance.zh-CN.md)

fib 服务端默认就很快，但"默认"是一组只针对某一类负载的取值；本章讲怎样为你的负载选容量，并让结果可观测。
内容覆盖任务池的模式、容量与运行时调整，自定义池与共享池，缓冲区与背压，事件循环，`prefork`，`fib/log`，
`pprof` 中间件，引擎计数器，压测活动，以及最后的 CI 一致性套件与生产检查清单。阅读前应已了解
[02-engine.md](02-engine.zh-CN.md)；会改变 handler 形态的技巧见 [21-hacks.md](21-hacks.zh-CN.md)。

## 协程池模型

连接的每一轮读——其中的读、它们触发的 `OnData`、cork 住的回复的 flush 以及 `OnClose`——都是引擎
`taskpool` 上的一个任务；HTTP/2 与 HTTP/3 的请求 handler 跑在另一个池 `Engine.HandlerPool()` 上。各模式的区别只在 worker 的创建与
回收方式，而不在任务本身：

| 模式 | worker |
| --- | --- |
| `ModeAdaptive`（默认） | 任务之间保留 parked worker；所有 worker 都忙且没有正在唤醒的 worker 时扩容，闲置的回收，介于 `MinWorkerCount` 与 `WorkerCount` 之间 |
| `ModeAdaptiveChan` | 同样的策略，队列换成 channel 实现而不是互斥锁保护的栈 |
| `ModeElastic` | 有容量时每次提交都 fork 一个 worker，短暂停留后回收 |
| `ModeInline` | 没有 worker：在提交方的协程上带 recover 地执行每个任务 |

`ModeAdaptive` 还按 P 分片（提交进入提交协程所在 P 的分片，该分片有积压时再看一个随机分片），每个分片是
无锁 MPMC 环形队列，并把热的空闲 worker 放在 LIFO 栈上——这正是 README 的
[Taskpool 一节](../../README.zh-CN.md#taskpool)里它在争用型和事件循环型负载上领先的原因。`ModeInline` 只是
库特性：引擎拒绝它，否则连接的一轮就会跑在事件循环上。

## 池的容量

`WorkerCount` 是上限而不是实际数量：自适应池只在有任务需要时才启动 worker，所以在负载涨到上限之前，
调高上限不花任何代价。`DefaultPoolSizing` 计算
`WorkerCount = max(GOMAXPROCS×1000, NumCPU×20)`，队列为
`clamp(WorkerCount×10, 10000, 100000)`；`DefaultConfig` 装的就是这些值。`MinWorkerCount` 是自适应池
回收到的常驻下限；为零时闲置的池会回收所有 worker。

```go
config.SetPoolSizing(4096, 40960)             // pin the ceiling and the queue
config.MinWorkerCount = 16                    // keep a floor for bursts
config.SetTaskPoolMode(taskpool.ModeAdaptive) // preferred: also moves the sizing to the mode's
```

只有当在本机压测表明默认上限形状不对时才固定容量；仅用 `SetTaskPoolMode` 就足以换策略。通过 `SetTaskPool`
提供的 `*taskpool.TaskPool`（见下一节）可以在引擎运行时用 `Resize(min, max)` 调整自适应池的下限与上限（其他
模式返回 false），而不中断在途任务。

下面的完整程序是一个调优过的引擎：协程池下限、缓冲区、背压、poller 与日志，并在收到 SIGINT 时优雅停止。

```go
package main

import (
	"context"
	"os/signal"
	"syscall"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/taskpool"
)

func main() {
	config := fib.DefaultConfig() // the defaults are already sized for the machine
	config.Name = "gateway"       // labels the pools and the logs

	config.MinWorkerCount = 16                    // resident floor between bursts
	config.SetPoolSizing(4096, 40960)             // ceiling and queue, pinned
	config.SetTaskPoolMode(taskpool.ModeAdaptive) // sizing pinned above is kept

	config.ReadBufferSize = 16 << 10          // bytes one read may take
	config.ReadBufferStartSize = 4 << 10      // small requests keep the small buffer
	config.WriteBufferHighWatermark = 1 << 20 // well above one round of reads
	config.MaxPendingBytes = 1 << 30          // the whole server's queued output
	config.MaxWriteQueueBytes = 4 << 20       // one peer that stops reading
	config.MaxConns = 100000

	config.IOPollers = true
	config.IOPollerCount = 0 // let the machine size the pollers
	config.LogSettings = false

	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(200, "text/plain; charset=utf-8", []byte("ok\n"))
	})
	engine, err := fib.New(config, fib.Binding{
		Addr:    "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(app),
	})
	if err != nil {
		panic(err)
	}
	go func() { _ = engine.Run() }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	engine.Stop()
	_ = engine.Close()
}
```

## 自定义池与共享池

`Config.TaskPool` 接受任何实现了引擎 `TaskPool` 接口的池（`GoTask(taskpool.Task) bool` 与
`GoTasks([]taskpool.Task) int`）；`*taskpool.TaskPool` 就是其中一个，用 `Config.SetTaskPool` 设置。这样提供的池，
引擎永远不会停止它，生命周期由调用方负责，要在所有使用它的引擎关闭后再停止；inline 池（`ModeInline`）会被拒绝。你自己的异步工作一定要用与引擎分开的池：engine worker 会向你的池提交任务，如果是同
一个池，队列满时所有 worker 都可能卡在提交上。

```go
pool := taskpool.NewAdaptive(taskpool.AdaptiveConfig{
	Name: "jobs", MinWorkers: 16, MaxWorkers: 4096, QueueSize: 10000,
})
defer pool.Stop()          // the engine does not stop it
config.SetTaskPool(pool)   // the engine's connections now run on it by "jobs"
pool.Resize(8, 2048)       // move the floor and ceiling while it runs
```

`SharedTaskPool`（默认开）让同名 `Name` 的引擎共享一个 worker 池和一个 handler 池，于是监听两个端口的两个
引擎不会把协程数翻倍。共享只看名字：**第一个引擎的池配置生效**，之后同名引擎的池设置被忽略。要隔离就给
引擎不同的名字，或设置 `SharedTaskPool = false`。

下面的程序提供自己的池，打印它实际运行的模式与 worker 数，并在服务过程中调整容量：

```go
package main

import (
	"fmt"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/taskpool"
)

func main() {
	pool := taskpool.NewAdaptive(taskpool.AdaptiveConfig{
		Name:       "gateway-workers",
		MinWorkers: 8,
		MaxWorkers: 1024,
		QueueSize:  8192,
	})
	defer pool.Stop() // the engine does not stop a pool it was given

	config := fib.DefaultConfig()
	config.SetTaskPool(pool) // connections run on the caller's pool now
	config.LogSettings = false

	engine, err := fib.New(config, fib.Binding{
		Addr: "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
			_ = c.Respond(200, "text/plain", []byte("ok\n"))
		})),
	})
	if err != nil {
		panic(err)
	}
	go func() { _ = engine.Run() }()
	time.Sleep(time.Second) // let the pool take some traffic

	fmt.Println("mode:", pool.Mode(), "workers:", pool.Workers())
	pool.Resize(16, 512) // move the floor and the ceiling while it runs
	fmt.Println("after Resize:", pool.Workers())

	engine.Stop()
	_ = engine.Close()
}
```

`pool.Mode()` 报告创建时的模式，`pool.Workers()` 报告当前 worker 数，`pool.Name()` 是它日志里的名字。除非
安装 `SetPanicHandler`，任务 panic 会被池 recover；`taskpool.SetLogStatus(true)` 让之后新建的池打印它们
解析出的参数。

## 缓冲区与背压

读侧与写侧各自独立定容。`ReadBufferSize` 是一次读 socket 最多取多少字节；`ReadBufferStartSize` 让处理小
请求的连接保持较小的 buffer，而承载大传输的连接从每轮开始就按完整大小读。写侧 `WriteBufferHighWatermark`
在一条连接有这么多字节等待写入 socket 时暂停它的读，好让 TCP 背压追上来（从不读数据的对端不会把队列撑过
`MaxWriteQueueBytes`，超过就以 `ErrSendQueueFull` 关闭连接）。`MaxPendingBytes` 是整个服务端排队输出的
总量，默认关闭。

- watermark 要比**一次读的回复量**大，而不是一条消息：socket 有数据时一轮会以 `ReadBufferSize` 为单位持续
  读，回复在轮末（或每读 256KiB）flush 之前都 cork 在一起，每次读完这一轮都会把排队量与 watermark 比较。`DefaultConfig` 把它定为 4 个读 buffer，并受 64KiB 与 256KiB 限制。
- 当 watermark × 连接数是部署无法承受的内存时，设置 `MaxPendingBytes`；预算耗尽时它会暂停所有连接的读。
- 向可能停止读数据的对端广播时，设置 `MaxWriteQueueBytes`（或 `Connection.SetWriteQueueLimit`）。它是硬
  上限，不是暂停。
- `DefaultConfig` 的 `MaxConns` 是关闭的；公网监听通常应该给一个数值，让洪泛只消耗有界数量的描述符。

### buffer 池

同一个池也可以服务你自己的 buffer：`bufferpool.Get`/`Put` 零分配，大小按 2 的幂分档（正好是 Go
分配器的 size class，不会有取整浪费），而且有一个**能活过 GC** 的储备（与 `sync.Pool` 不同），由
`SetRetainedBytes` 学习并设上限（默认 32MiB；`SetRetainedBytes(0)` 在内存紧张时立即归还）。fib 的读
buffer、回复组装与 TLS 记录都走同一批档位，所以你的 buffer 加入的是同一条循环，而不是多留一份空闲副本。

```go
buf := bufferpool.Get(64 << 10)[:0] // cap 64KiB; contents are the previous user's, write before reading
buf = bufferpool.Append(buf, data)  // moves to a larger class only if data does not fit
defer func() { bufferpool.Put(buf) }() // Put the final buf: Append may have replaced (and Put) the first

joined := bufferpool.Join(nil, a, b) // takes enough at once and joins the parts
```

它适合 handler 的临时 buffer，不适合仍被 `SendShared` 或响应体引用的 buffer：`Put` 把它还给某个档位，
下一次 `Get` 就可能把它交出去。`Append`、`Grow` 与 `Join` 换到更大的 buffer 时会把旧的还给池，所以之后只能
使用它们返回的切片。

完整字段表见 [configuration.zh-CN.md](../configuration.zh-CN.md#缓冲区与背压)。

## Poller、GOMAXPROCS 与 prefork

`IOPollers` 把引擎拆到多个事件循环上（Linux epoll、macOS/BSD kqueue）。poller 只等待事件并把可运行的
连接交给 worker，因此 poller 消耗 P，除非 accept 是瓶颈，否则通常更慢；默认数量随机器而定（4 CPU 及以下
1 个，16 CPU 及以下 2 个，更多则 CPU/8）。`EngineLoop` 让引擎拥有自己的 accept 循环，而不是让 poller 监听。
Windows 上无论怎么设置都只有一个循环。

```go
config.IOPollers = true
config.IOPollerCount = 0  // 0: machine-sized; > 0: exactly this many loops
config.EngineLoop = false // let the pollers accept, which accepts in parallel
```

`GOMAXPROCS` 保持默认值，即核数。调高会更慢，并在
[奇技淫巧指南](../hacks.zh-CN.md#17-进程级技巧preforkiopollersgomaxprocs)的测量中多用 25–40% 内存；
`runtime.NumCPU` 已经会尊重 CPU 亲和掩码或 cgroup 配额。

`prefork`（Linux）是同一问题在进程层面的答案：核数很多时，单个堆的 GC 和锁争用会成为主导，于是 master
启动多个子进程，每个有自己的堆、GC 和几个 P，子进程通过 `SO_REUSEPORT` 共享监听端口。

```go
package main

import (
	"context"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/prefork"
)

func main() {
	// Code before Run runs in the master and in every child; work only the
	// children need (loading data, opening pools) belongs in serve.
	if err := prefork.Run(prefork.Config{ProcsPerChild: 2}, serve); err != nil {
		panic(err)
	}
}

func serve(ctx context.Context) error {
	config := fib.DefaultConfig()
	config.Name = "preforked" // engines of one name in a child share their pools
	config.LogSettings = false

	engine, err := fib.New(config, fib.Binding{
		Addr: "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
			_ = c.Respond(200, "text/plain; charset=utf-8", []byte("ok\n"))
		})),
	})
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done() // SIGINT/SIGTERM, or the master asking the child to stop
		engine.Stop()
	}()
	err = engine.Run()
	_ = engine.Close()
	return err
}
```

子进程默认每个 2 个 P，master 启动的数量是机器 GOMAXPROCS 除以 `ProcsPerChild` 再向上取整，使所有子进程
合计拥有单个进程会有的 P 数；`prefork.Children()` 报告共享这台机器的进程
数，所以"整机唯一"的资源——比如数据库连接上限——要除以它。prefork 需要 Linux，在那里
`SO_REUSEPORT` 才会把连接分摊到多个进程；其他平台上 `prefork.Run` 直接在本进程调用 `serve`，程序保持可移植。
`Config.Children` 可以覆盖子进程数；P 数少于 fib 给 poller 所需数量的子进程，会在引擎自己的循环上服务连接。UDP 连接在对端 NAT rebind 后可能落到另一个子进程，
而内存状态是每个子进程一份，不共享。

## 日志

fib 的每个包都通过 `github.com/lesismal/fib/log` 打日志，因此一次调用就能决定全部日志的形态。
`fiblog.Setup` 接受 `Config`：`Format`（默认 `FormatLine`，另有 `FormatText`、`FormatJSON`）、`Level`
（一个 `slog.Leveler`，所以传 `*slog.LevelVar` 可以在运行时改级别）、`Output` 与 `TimeFormat`。

```go
level := &slog.LevelVar{}
level.Set(slog.LevelInfo)
fiblog.Setup(fiblog.Config{
	Format: fiblog.FormatJSON,
	Level:  level,
	Output: os.Stdout,
})
```

程序里不调用时，也可以由环境变量决定：`FIB_LOG_FORMAT=line|text|json` 与
`FIB_LOG_LEVEL=debug|info|warn|error` 在程序启动时设置 fib 的 logger，
`FIB_LOG_SETTINGS=0` 静默启动时的配置清单而不改动配置。`Engine.LogSettings()` 可随时打印该清单，
`Config.LogStatus` 额外打印一行 `fib: engine started`（监听地址、池、poller、accept 放置方式），`logger` 中间件则通过同一个 logger 记录每个响应。

## pprof

`pprof` 中间件在 `/debug/pprof` 下提供运行时 profile，供 `go tool pprof` 和浏览器使用。一个 profile 可能
要采集数秒，所以每个 profile 在引擎的 handler 池上运行，而不是在跑 handler 的协程上；连接先断开的请求会
停止它的 profile。

```go
handler := fibhttp.NewHandler(middleware.Chain(app, pprof.New()))

// go tool pprof http://localhost:8080/debug/pprof/profile?seconds=10
```

profile 能暴露很多进程信息，所以只在"能访问它的人被允许知道这些信息"的地方提供。下面的完整程序把日志、
pprof 中间件和服务端接在一起：

```go
package main

import (
	"log/slog"
	"os"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	fiblog "github.com/lesismal/fib/log"
	"github.com/lesismal/fib/middleware"
	"github.com/lesismal/fib/middleware/pprof"
)

func main() {
	level := &slog.LevelVar{} // change it while the program runs
	level.Set(slog.LevelInfo)
	fiblog.Setup(fiblog.Config{
		Format: fiblog.FormatJSON, // FormatLine (the default), FormatText or FormatJSON
		Level:  level,
		Output: os.Stdout,
	})

	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(200, "text/plain; charset=utf-8", []byte("ok\n"))
	})
	handler := fibhttp.NewHandler(middleware.Chain(app, pprof.New(pprof.Config{
		Prefix: "/debug/pprof", // the default
	})))

	config := fib.DefaultConfig()
	config.LogSettings = true // print every effective setting when the engine starts
	config.LogStatus = true

	engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:8080", Handler: handler})
	if err != nil {
		fiblog.Error("engine failed", "error", err)
		panic(err)
	}
	defer engine.Close()
	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

## 统计与排障

`Engine.Stats()` 返回能解释变慢原因的计数器：

```go
st := engine.Stats()
fiblog.Info("engine stats",
	"openConns", st.OpenConns, "connsRefused", st.ConnsRefused,
	"pendingBytes", st.PendingBytes,
	"pausedByWatermark", st.ReadsPausedByWatermark,
	"pausedByBudget", st.ReadsPausedByBudget,
	"resumed", st.ReadsResumed)
```

- `ReadsPausedByWatermark` 上升说明对端没有在读回复：watermark 在拦它，过载时这是对的，而当 watermark
  小于一轮输出时这是错的。
- `ReadsPausedByBudget` 变化说明服务端级 `MaxPendingBytes` 被所有连接合计用尽，于是一条自己几乎没积压
  的连接也被暂停。
- `ConnsRefused` 统计因 `MaxConns` 达到上限而在 accept 时关闭的连接；`PendingBytes` 是当前的排队输出量。
- 配置本身：`Engine.Settings().Parameters()` 返回与启动日志相同的行，`Engine.LogSettings()` 按需打印。
  handler 或任务里的 panic 会由池以池名记录，并带堆栈。

## 压测

包级测试每次只驱动一条连接或一个手写帧；`benchmark/loadtest` 下的压测活动则像真正的负载生成器那样驱动
服务端——几百条 stream 分布在少数几条连接上，以及几百条连接各带少量 stream，持续几十秒，并让一部分客户端
停止读取响应：

```text
./benchmark/loadtest/run.sh --mode smoke          # one short case per protocol, about a minute
./benchmark/loadtest/run.sh --mode full --rounds 3 # the full campaign, 30s per case
python3 benchmark/loadtest/report.py /tmp/fib-loadtest/<time>
```

它需要 Go、nghttp2-client 提供的 `h2load`、`python3`、`curl`，Linux 上还需要 `taskset` 与 `flock`；它
故意不是 CI 任务，因为需要一台安静的主机，而且数字跨机器没有意义。压测活动写出 `results.tsv`、
`growth.tsv`，以及每个用例的服务端日志与 `/stats` 快照，因此回归可以从压测产出的同一批文件里读出来。
HTTP/3 驱动器（`h3load`）、停读客户端（`stall`）与交替 A/B 脚本（`ab.sh`）在
[loadtest.zh-CN.md](../loadtest.zh-CN.md)中有文档，那里也记录了压测发现的结果。

## CI 一致性

仓库的 CI 在 Linux、macOS、Windows 上，用 Go 1.27.x 与当前 stable，构建并测试每个 module：

```sh
go build ./...
go test -race -count=1 -timeout 10m -v ./...
```

在此之上还有一致性任务：HTTP/1.x 对 `net/http` 与 curl、HTTP/2 的 h2spec、HTTP/3 的 QUIC Interop
Runner、WebSocket 的 Autobahn、媒体（ffmpeg）、WebRTC（pion）、KCP（kcp-go）与 BitTorrent
（anacrolix、libutp、libtorrent）的互操作对端、fuzzer、覆盖率下限、`gofmt`/`go vet`/`staticcheck`、
`go mod tidy`、漏洞扫描与交叉编译。改动涉及某个协议时，要看的就是对应的任务。
[README 的构建与测试一节](../../README.zh-CN.md#构建与测试)是它的简版。

## 生产检查清单

1. 从 `fib.DefaultConfig()` 出发再改字段：`fib.New` 会拒绝零值 `fib.Config{}`（没有 worker 数），而手工
   构造的配置会悄悄丢掉 `CoalesceWrites`、`IOPollers`、`ReusePort`、`SocketSyscalls`、`UseWritev` 与
   `SharedTaskPool`。
2. 公网监听设置 `MaxConns` 与 `MaxWriteQueueBytes` 上限，让洪泛或停止读数据的对端只消耗有界资源。
3. 只有在清楚它意味着多少内存时才把 `MaxPendingBytes` 从零调开：它替代的是 watermark × 连接数。
4. 按你实际发送的最大消息确定读 buffer，并让写 watermark 明显大于一轮的回复量。
5. `GOMAXPROCS` 保持默认；多核 Linux 上用 `prefork`，而不是更大的堆。
6. 想清楚同名引擎是否应该共享池；不该就设 `SharedTaskPool = false` 或给不同的名字。
7. 在 `fib.New` 之前调用 `fiblog.Setup`（或用 `FIB_LOG_FORMAT`/`FIB_LOG_LEVEL` 环境变量），并且只在
   内部监听上提供 pprof。
8. 持续观察 `Engine.Stats()` 与压测活动：`ReadsPausedByWatermark`、`ConnsRefused`、`PendingBytes`
   能区分背压与饱和。
9. 需要让在途请求跑完时用 `Shutdown(ctx)` 停止，不需要时用 `Stop()`/`Close()`；先关客户端再关引擎。
10. 发布前跑 smoke 压测和对应协议的一致性任务，而不是只跑 `go test`。

## 坑与限制

- **不要为了吞吐调高 `GOMAXPROCS`。** 测量结果是更慢、更费内存；事件循环已经在 Go 的 netpoller 里等待。
- **不要把引擎的 worker 池当成自己的异步池**（与异步 handler 提交的是同一个池）：队列满时所有 worker
  都可能卡在提交上，事件循环随之挂住。
- **`ModeInline` 不是引擎模式。** 引擎会拒绝它；它只用于独立使用。
- **同名引擎里第一个的池配置胜出**，包括容量与模式；之后引擎的设置被静默忽略。
- **固定的 watermark 或读 buffer 是特定负载的。** 照抄另一台机器上的压测数字，是把调优过的服务端变得比
  默认更慢的常见方式。
- **`prefork` 只共享端口。** 内存状态、限流器和连接数都是每个子进程一份；整机级上限要除以
  `prefork.Children()`。
- **profile 与启动日志是信息泄露面。** `/debug/pprof` 与 `LogSettings` 要和你的管理端点遵循同样的访问
  规则。

## 相关文档

- [配置](../configuration.zh-CN.md) —— 每个 `Config` 字段的默认值与容量计算规则
- [压测](../loadtest.zh-CN.md) —— 压测框架、活动与实测结果
- [引擎与配置](02-engine.zh-CN.md) —— `Config`、`Binding` 与关闭语义
- [README 的 Taskpool 一节](../../README.zh-CN.md#taskpool) —— 池模式与基准测试表
- [README 的基准测试一节](../../README.zh-CN.md#基准测试) —— 协议基准与运行方式
- [进阶技巧](21-hacks.zh-CN.md) —— 本章为其定容的写侧与读侧控制

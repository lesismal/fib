# 配置

`fib.Config` 配置一个 Engine：监听、缓冲区、背压、worker 池、事件循环和连接上限。`fib.New` 会复制
这份配置、打开传入的 `Binding`，之后修改这个结构体不会改变已经运行的 Engine。

大多数程序只需要 `fib.DefaultConfig()`（按运行机器 sizing）和一个或多个 Binding：

```go
cfg := fib.DefaultConfig()

engine, err := fib.New(cfg, fib.Binding{
	Addr:    ":8080",
	Handler: handler,
})
if err != nil {
	panic(err)
}
defer engine.Close()
if err := engine.Run(); err != nil {
	panic(err)
}
```

- `DefaultConfig()`：worker 池跟随 CPU，监听 backlog 跟随内核，写水位线按读缓冲、内存和 fd 上限计算。
- `DefaultConfigFor(readBuffer)`：换一个 `ReadBufferSize` 并重新计算水位线；代理或大传输想用 64 KiB
  读缓冲时用它。
- 零值 `fib.Config{}` **不是** `DefaultConfig`：bool 保持 false，池和日志设置也不会被填上。请从
  `DefaultConfig` 出发，只改需要的字段。
- 零值有特殊含义的字段：`WriteBufferHighWatermark`（负值关闭）、`ReadBufferSize`、`MaxEvents`、
  `PollBatchSize`、`Backlog`、`IOPollerCount`、`UDPIdleTimeout`（负值表示永不过期）、`Network`、
  `Name`。其余字段的零值就是字面意思：`MaxPendingBytes`、`MaxWriteQueueBytes`、`MaxConns` 为 0
  表示关闭，bool 按设置值生效。

## `Config` 参考

以下默认值来自原生后端上的 `DefaultConfig()`；标 *动态* 的由机器或内核决定，见
[机器如何计算默认值](#机器如何计算默认值)。

### 标识与日志

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `Name` | `"fib"` | Engine 名。worker 池叫 `<Name>-workers`，HTTP/2、HTTP/3 handler 池叫 `<Name>-streams`。同名 Engine 共享这两个池，见 `SharedTaskPool`。 |
| `LogStatus` | `false` | 启动时打印一行 `fib: engine started`：名字、监听地址、任务池、poller 数和 accept 放置方式。 |
| `LogSettings` | `true` | 启动时逐项打印全部生效配置：值、`default`/`custom` 来源和说明。设为 false 关闭；`Engine.LogSettings()` 仍可随时打印。 |

### 监听与连接

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `Network` | `"tcp"` | Binding 自己没有设置 `Network` 时使用的网络：`tcp`、`tcp4`、`tcp6`、`unix`、`udp`、`udp4`、`udp6`。 |
| `Backlog` | *动态* | TCP 和 Unix 监听队列长度：读得到时用内核的 `somaxconn`，否则用 `SOMAXCONN`。accept 队列溢出会静默丢客户端，所以内核值是合理的默认。 |
| `MaxConns` | `0`（关） | 所有 Binding 加在一起，已 accept 且未关闭的连接上限；超过的连接在 handler 看到之前就被关闭，并计入 `Stats.ConnsRefused`。dial 出去的连接和 UDP peer 不计入。 |
| `ReusePort` | `true` | 监听 socket 设置 `SO_REUSEPORT`，同用户的其他 socket 可以共享地址。Linux 上 `IOPollers` 模式即使不设置，poller 也会各自监听；prefork 子进程总是设置。 |
| `BalanceReusePort` | `false` | Linux 上 `IOPollers` 且没有 `EngineLoop` 时，挂 eBPF selector，把 reuseport 连接按轮询分给 poller，而不是按内核地址哈希。需要多个 poller 和 `CAP_BPF`（Linux 5.8 之前是 `CAP_SYS_ADMIN`）；没有权限时保留内核哈希并打日志。 |
| `AcceptPlacement` | `AcceptFDModulo` | poller 在自己独占的 TCP socket 上 accept 的连接交给哪个 poller：`AcceptFDModulo` 按 fd 取模（默认），`AcceptKernelHash` 交给内核地址哈希选中的、也就是该 socket 所属的 poller。只适用于 Linux 上 `IOPollers` 且没有 `EngineLoop` 的情况。 |
| `UDPIdleTimeout` | `0` → `60s` | UDP peer 这么长时间没有收发就关闭，`OnClose` 收到 `ErrUDPIdleTimeout`。负值表示一直保留；dial 出去的 UDP 连接没有超时。 |

### 缓冲区与背压

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `ReadBufferSize` | `16 KiB` | 一次 read 读多少字节；对代理来说也基本是一次 write 写多少字节。一轮从池里借一块，不是每连接一块。 |
| `WriteBufferHighWatermark` | *动态*（默认读缓冲下 64 KiB） | 单连接待写字节达到它就暂停读；低水位是它的四分之一。它限制用户态缓冲，不限制发送队列。0 表示按 `ReadBufferSize`、机器、预算和连接数计算；负值关闭。 |
| `MaxPendingBytes` | `0`（关） | 所有连接加在一起允许等待的字节数；达到后所有连接暂停读，降到四分之一恢复。0 表示只有每连接水位线；当“水位线 × 连接数”超出内存承受范围时设置它。 |
| `MaxWriteQueueBytes` | `0`（关） | 单连接已经排队、socket 还没取走的字节上限（`Send`、`SendParts`、TLS 留下的；不含 `SendFile` 未读的文件）。超过时这次 send 返回 `ErrSendQueueFull` 并关闭连接。广播最需要它：水位线只暂停读，`MaxPendingBytes` 限制总量而不是单个对端。0 表示不限制；单连接可用 `SetWriteQueueLimit` 自己设。 |

### 写与批量

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `UseWritev` | `true` | 平台支持时用一次 `writev` 发多个缓冲；单缓冲仍用 `write`。 |
| `CoalesceWrites` | `true` | 一轮读取产生的回包先 cork，轮末一起写出，k 个 pipeline 请求只花一次 write。只有原生后端有读取轮；portable 后端忽略。 |
| `SocketSyscalls` | `true` | 用 `recvfrom`、`sendto`、`sendmsg` 而不是 `read`、`write`、`writev`，省掉 VFS 文件权限钩子，尤其是 AppArmor 容器里。只有 Linux 生效，且 386 以及 race、memory、address sanitizer 构建不生效。 |

### Worker 池

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `WorkerCount` | *动态* | worker 数上限：`max(GOMAXPROCS×1000, NumCPU×20)`。池随负载伸缩，上限在负载到达前不产生成本。 |
| `MaxEvents` | *动态* | 池里等待 worker 的任务上限：`clamp(WorkerCount×10, 10,000, 100,000)`。 |
| `TaskPoolMode` | `ModeAdaptive` | 池的伸缩方式：`elastic` 每次提交 fork 一个 worker；`adaptive` 保留 parked worker、按需增长；`adaptive-chan` 通过 channel 做同样的事；`inline` 在提交者 goroutine 上直接跑。优先用 `SetTaskPoolMode`，它会同时调整 sizing。 |
| `MinWorkerCount` | `0` | adaptive 池缩容后保留的常驻 worker 数。0 表示空闲时全部退休，有任务再启动；其他模式忽略。 |
| `SharedTaskPool` | `true` | 同名 Engine 共享一个 worker 池和一个 handler 池。第一个启动的 Engine 按自己的设置创建；之后的同名 Engine 直接使用。要隔离就设 false 或换名字。 |
| `TaskPool` | `nil` | 外部任务池，替代内置池运行连接的轮次，见 `SetTaskPool`。设置后 pool mode、worker 数、共享池和 sizing 都不再作用于它；Engine 不会停止外部池。 |

### 事件循环

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `IOPollers` | `true` | 把 Engine 分到多个等待就绪的事件循环，而不是一个。只有 epoll 和 kqueue 有 poller；Windows 保持一个循环。关闭时 Engine 一定有自己的循环。 |
| `IOPollerCount` | `0` → *动态* | 创建多少个循环：≤4 CPU 为 1，≤16 为 2，更多为 CPU/8（17–23 为 2，24 为 3，32 为 4，64 为 8）。CPU 数是 `min(NumCPU, GOMAXPROCS)`，cgroup quota 会计入。 |
| `EngineLoop` | `false` | 给有 `IOPollers` 的 Engine 一个自己的循环，负责 accept 所有 TCP/Unix 连接并读取 UDP socket。默认由 poller 自己监听和 accept，可以并行 accept。 |

## `Binding` 参考

一个 `Binding` 是一组监听地址、它们共享的网络，以及处理这些地址上连接或 UDP peer 的 handler。
`fib.New` 可以传任意多个 Binding，它们共享 Engine 的循环、池和缓冲区。

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `Network` | `Config.Network`（`"tcp"`） | `tcp`、`tcp4`、`tcp6`、`unix`、`udp`、`udp4`、`udp6`，命名方式同 `net.Listen` 和 `net.ListenPacket`。 |
| `Addr` | `":0"` | `host:port`、Unix 路径，或 `@` 开头的抽象 Unix 地址。端口 0 让内核选，`BindingAddrs` 返回实际地址。 |
| `Addrs` | `nil` | 这组地址的完整列表，共享 `Network`；设置后 `Addr` 被忽略。 |
| `Handler` | 必填 | 服务该 Binding 的 handler：HTTP、TLS server、HTTP/3 server、WebSocket、gRPC、裸 TCP 等等。 |

其余配置——backlog、缓冲区、水位线、poller、`ReusePort`——都属于 Engine，对所有 Binding 生效。
Engine 运行期间可以用 `AddBinding`、`RemoveBinding`、`ShutdownBinding` 增删 Binding。

## 机器如何计算默认值

`DefaultConfig` 启动时读取一次机器信息：

- CPU：`min(runtime.NumCPU(), runtime.GOMAXPROCS(0))`，cgroup CPU quota 会计入。
- 内存：机器物理内存，或更低的 cgroup 限制（v2 `memory.max`，v1 `memory.limit_in_bytes`），
  从进程自己的 cgroup 逐级向上读。
- fd：`RLIMIT_NOFILE` 软限制。

读不到内存时，水位线保持 64 KiB，也不会凭空造一个预算。

单连接水位线的公式：

```text
Bref = 用户设置的 MaxPendingBytes；没有预算时为 内存/2（只作 sizing 参考）
Cref = clamp(min(Bref/16 KiB, 0.8 × RLIMIT_NOFILE, MaxConns 或 100,000), 100, ...)
W    = clamp(min(4 × ReadBufferSize, 4 × Bref / Cref), 64 KiB, 256 KiB)
W    = 向下取整到 16 KiB
```

4× 来自“25% 积压”策略：如果 25% 的参考连接同时满载积压，总量正好到参考预算。`DefaultConfig`
的 `MaxPendingBytes` 仍是 0，所以这里只用来选水位线；需要总量上限的部署自己设置预算。
`Machine.Sizing(readBuffer, pendingBytes, maxConns)` 暴露同样的计算，`DefaultWriteWatermark`
只返回水位线。设置了预算时，`MaxWriteQueueBytes` 还按 `clamp(Bref/8, 8 MiB, 32 MiB)` 推导
（另有半预算上限）；`DefaultConfig` 保持关闭。

默认 16 KiB 读缓冲下，任何机器上水位线都是 64 KiB。换成代理/大传输常用的 64 KiB 读缓冲后：

| 机器内存 | 水位线 |
| --- | --- |
| ≤ 2 GiB | 64 KiB |
| 4 GiB | 80 KiB |
| 8 GiB | 160 KiB |
| ≥ 16 GiB | 256 KiB |

`DefaultConfigFor(64 << 10)` 直接给出这个读缓冲和水位线，通常就是代理网关想要的 sizing。

## 启动日志与生效配置

`LogSettings` 开启时，Engine 启动会打印一个 header 和每个参数一行：

```text
INFO fib: engine config engine=gateway parameters=29
INFO fib: config parameter=readBuffer value="65536 (64 KiB)" source=custom about="bytes one read of a connection's socket takes"
INFO fib: config parameter=writeWatermark value="262144 (256 KiB)" source=default about="bytes a connection may have waiting before its reads pause; zero is sized from readBuffer and the machine"
INFO fib: config parameter=maxPendingBytes value="33255979008 (30.97 GiB)" source=custom about="bytes all connections together may have waiting; zero means no budget"
```

使用 fib/log 的 `line` 格式时（见[日志](#日志)），整个列表是一条记录，参数以对齐的表格写在它下面：

```text
2026-10-27 10:00:00.000 INFO  fib: engine config engine=gateway parameters=29
	settings:
		parameter           value             source   about
		readBuffer          65536 (64 KiB)    custom   bytes one read of a connection's socket takes
		writeWatermark      262144 (256 KiB)  default  bytes a connection may have waiting before its reads pause; ...
		...
```

`source=default` 表示值来自 fib 默认或机器 sizing；`source=custom` 表示调用方改成了不同的值。
`Engine.LogSettings()` 可以随时打印同样的列表，不受 `LogSettings` 开关影响；
`Engine.Settings()` 以结构体返回，`Settings.Parameters()` 返回日志使用的同一组行。

环境变量 `FIB_LOG_SETTINGS=0`（也可以是 `false`、`off`、`no`，不区分大小写）会在不改配置的
情况下关闭启动时的列表。fib 的 CI 就是这样避免测试打印的几千行配置淹没失败的；
`Engine.LogSettings()` 仍可随时打印。

`LogStatus` 是另一个开关，只控制 `fib: engine started` 那一行，默认关闭。

## 日志

fib 的所有包都通过 `github.com/lesismal/fib/log` 打日志：Engine、协程池、arpc、grpc，以及
`logger` 和 `recover` 中间件。没有配置时它写到 `slog.Default()`，所以调用过 `slog.SetDefault` 的
应用会一并收到 fib 的日志。要使用 fib 自己的格式：

```go
import fiblog "github.com/lesismal/fib/log"

fiblog.Setup(fiblog.Config{
	Format: fiblog.FormatJSON, // FormatLine（默认）、FormatText 或 FormatJSON
	Level:  slog.LevelWarn,    // 也可以传 *slog.LevelVar，运行时调整
	Output: os.Stderr,         // 默认值
})
```

```text
line  2026-10-27 10:00:00.000 INFO  fib: engine started engine=fib listen=tcp://[::]:8080
text  time=2026-10-27T10:00:00.000+08:00 level=INFO msg="fib: engine started" engine=fib
json  {"time":"2026-10-27T10:00:00.000+08:00","level":"INFO","msg":"fib: engine started","engine":"fib"}
```

`line` 给人看；`text` 和 `json` 是结构化日志，一条记录一行，便于程序采集。`line` 格式下，panic 的
调用栈写在记录下面的若干行，逐行缩进；其他换行和控制字符一律转义，请求无法伪造出一行日志。
`fiblog.SetLogger` 可以直接传任意 `*slog.Logger`，传 `nil` 恢复 `slog.Default()`。
`arpc.SetLogger` 仍可给 arpc 单独指定 logger。

程序里没有调用时，也可以用环境变量选择：`FIB_LOG_FORMAT`（`line`、`text`、`json`）和
`FIB_LOG_LEVEL`（`debug`、`info`、`warn`、`error`）在程序启动时配置 fib 的 logger。之后调用
`Setup` 或 `SetLogger` 会覆盖它们。

`logger` 中间件把每个响应作为一条记录打出，消息为 `http: access`，字段由 `Config.Fields` 指定（默认
`status`、`latency`、`ip`、`method`、`path`），格式跟随 logger。设置了它的 `Format` 或 `Output` 时，
改为按 `${tag}` 模板把一行直接写到 `Output`，与以前相同。

## 运行时修改

`fib.New` 会复制配置。运行前可以用 `SetPoolSizing`、`SetTaskPoolMode` 调整池，用 `SetTaskPool`
提供外部池（由调用方负责停止）。单连接上，`SetWriteQueueLimit` 限制一个对端的队列，
`SetWriteBackpressure(false)` 让 relay 自己互相压，`HoldReads` 提供应用层读背压。HTTP/1、HTTP/2、
HTTP/3、WebSocket、gRPC 包各有自己的窗口和上限配置；见 [HTTP/1](http1.zh-CN.md)、
[HTTP/2](http2.zh-CN.md)、[HTTP/3](http3.zh-CN.md)、[Hacks](hacks.zh-CN.md) 和 [指南](guide.zh-CN.md)。

## 另见

- [架构](architecture.zh-CN.html)
- [Hacks](hacks.zh-CN.md) — 水位线、cork、批量写背后的设计笔记
- [压测](loadtest.zh-CN.md)

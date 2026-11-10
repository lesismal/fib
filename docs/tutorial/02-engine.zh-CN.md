# 引擎与配置

[English](02-engine.md) | [简体中文](02-engine.zh-CN.md)

引擎就是整个服务器：它拥有事件循环、工作协程池、缓冲区和全部监听器，每个 `fib.Binding` 表示一个（或一组）地址以及服务它的 handler。本章讲 fib 程序的基本结构、实践中真正会改的配置字段、如何用一个引擎同时服务多种协议，以及绑定和引擎如何退出——无论是优雅关闭还是立即关闭。如果还没写过 fib 服务器，请先读 [01-getting-started.zh-CN.md](01-getting-started.zh-CN.md)；完整的字段参考在 [配置](../configuration.zh-CN.md)，本章只链接、不复制。

## fib 程序的结构

每个程序都是同样的五步：从 `fib.DefaultConfig()` 开始，构造 handler，把每个 handler 包进一个 `fib.Binding`，把绑定交给 `fib.New`，然后用 `Run` 服务直到 `Close`：

```go
cfg := fib.DefaultConfig() // machine-sized defaults; a zero fib.Config{} is not the same

engine, err := fib.New(cfg,
	fib.Binding{Addr: "127.0.0.1:8080", Handler: fibhttp.NewHandler(router)},
	fib.Binding{Network: "unix", Addr: sockPath, Handler: adminHandler},
)
if err != nil {
	panic(err) // one address could not be taken: none of the bindings is kept
}
defer engine.Close()

if err := engine.Run(); err != nil { // blocks until Stop, Shutdown or Close ends it
	panic(err)
}
```

`New` 会复制配置，之后再改 `cfg` 不会影响运行中的引擎；它把全部绑定当成一个整体打开：要么每个地址都监听成功，要么一个都不保留并返回让其中一个失败的错误。`Run` 在 `Stop`（或结束 `Shutdown` 的那次停止）之后返回；此时才由调用方执行 `Close`，关闭仍然打开的连接。不传任何绑定而调用 `New` 得到的是只做客户端的引擎，它的连接来自主动拨号。

`DefaultConfig` 是正确起点，因为它只读一次机器情况——CPU、内存和描述符上限——并据此确定工作协程池、监听 backlog 和写水位。它的默认值见[机器如何计算默认值](../configuration.zh-CN.md#机器如何计算默认值)；`DefaultConfigFor(readBuffer)` 与之相同，但换用另一个读缓冲区并据此计算写水位，代理或大流量传输通常需要的正是这套尺寸。

## 真正会改的配置

字段可以分成几组，其中只有少数会改变服务器的行为。先从默认值开始，只设置部署真正需要的：

- **身份与协程池**——`Name` 用于日志中的引擎标识，也决定两个协程池的名字（连接用的 `<Name>-workers`，HTTP/2 和 HTTP/3 handler 用的 `<Name>-streams`）；同名引擎在 `SharedTaskPool` 打开时（默认打开）共享 worker 池，handler 池则总是共享。`WorkerCount` 是池的上限，`MinWorkerCount` 是 adaptive 模式回收后保留的下限，`SetTaskPoolMode` 选择调度器（默认 `taskpool.ModeAdaptive`），并把 `WorkerCount` 和 `MaxEvents` 重置为该模式的尺寸，`SetTaskPool` 可以换成你自己的池。
- **监听器**——`Network` 是绑定自身留空时使用的网络，`Backlog` 是监听队列深度（内核的 `somaxconn`），`MaxConns` 限制全部绑定合计已接受的连接数，`ReusePort` 允许同一用户的其他 socket 共享地址。
- **缓冲区与背压**——`ReadBufferSize` 限制一次读的字节数（默认 16 KiB，每轮复用池中的一个缓冲区，不是每连接一个）；`WriteBufferHighWatermark` 在某连接待写字节达到该值时暂停它的读；`MaxPendingBytes` 对整个服务器做同样的限制；`MaxWriteQueueBytes` 限制单个连接的发送队列，超出的发送不会入队，而是以 `fib.ErrSendQueueFull` 关闭连接，避免不读数据的对端把队列撑大。
- **写入与批量**——`UseWritev` 用一次 `writev` 发送多个缓冲区，`CoalesceWrites` 把一轮内的响应 cork 起来，在本轮结束时一起写出。
- **事件循环**——`IOPollers` 把引擎分散到多个事件循环，`IOPollerCount` 指定数量（0 表示按 CPU 数决定），`EngineLoop` 给引擎一个自己的循环来 accept 和读 UDP，`AcceptPlacement` 决定由哪个 poller 服务它 accept 到的连接。

一个调好的网关配置会把它们组合起来：

```go
package main

import (
	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/taskpool"
)

func main() {
	cfg := fib.DefaultConfig()
	// First: switching the mode resets WorkerCount and MaxEvents to its sizing.
	cfg.SetTaskPoolMode(taskpool.ModeAdaptive)

	cfg.Name = "gateway"             // names the pools "<Name>-workers" and "<Name>-streams"
	cfg.WorkerCount = 4 << 10        // ceiling; adaptive mode retires idle workers
	cfg.MinWorkerCount = 16          // floor the pool retires down to
	cfg.MaxConns = 10_000            // accepted connections, all bindings together
	cfg.MaxPendingBytes = 1 << 30    // server-wide output budget
	cfg.MaxWriteQueueBytes = 8 << 20 // per-connection queue cap
	cfg.ReadBufferSize = 64 << 10    // one read takes at most 64 KiB
	cfg.WriteBufferHighWatermark = 256 << 10
	cfg.IOPollerCount = 2 // two loops; 0 would size them from the CPUs

	handler := fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.String(200, "ok\n")
	}))
	engine, err := fib.New(cfg, fib.Binding{Addr: "127.0.0.1:8080", Handler: handler})
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

启动配置列表（见[启动日志与日志配置](#启动日志与日志配置)）就是检查结果的办法：这里设置的字段显示 `source=custom`，例如 `parameter=workers value=4096 source=custom`；显示 `default` 的，要么被覆盖了，要么没设上，要么设成了与默认值相同的值（比如这里的 `WriteBufferHighWatermark`）。

有两点要注意。零值是特殊标记的字段（`ReadBufferSize`、`Backlog`、`IOPollerCount`、`UDPIdleTimeout`、`Name`、`Network`）表示“取默认值”；其余字段的零值就是字面含义，所以 `MaxConns`、`MaxPendingBytes`、`MaxWriteQueueBytes` 为 0 表示不启用。只调大 `ReadBufferSize` 并不会改变 `DefaultConfig` 已经按旧值选好的写水位：请改用 `DefaultConfigFor`，或者自己设置 `WriteBufferHighWatermark`。

## 一个引擎服务多种协议

绑定很便宜，因为其余一切——事件循环、描述符表、工作协程池和缓冲池——都是共享的。一个引擎可以同时服务 HTTP、HTTPS、HTTP/3、WebSocket、gRPC、原生 TCP 和 Unix socket，各自监听自己的地址，各自使用自己的 handler 栈：

```go
package main

import (
	"fmt"
	"os"
	"path/filepath"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
	"github.com/lesismal/fib/websocket"
)

func main() {
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}

	router := fibhttp.NewRouter()
	router.Get("/", func(c *fibhttp.Context) {
		_ = c.String(200, "hello over %s\n", c.Request.Proto)
	})

	ws := websocket.NewHandler(websocket.HandlerFuncs{
		Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
			_ = c.WriteMessage(op, data) // echo the message back
		},
	})

	sock := filepath.Join(os.TempDir(), "fib-tutorial.sock")
	_ = os.Remove(sock) // a Unix listener wants a path that does not exist yet

	engine, err := fib.New(fib.DefaultConfig(),
		// HTTP/1.x and h2c.
		fib.Binding{Addr: "127.0.0.1:8080", Handler: fibhttp.NewHandler(router)},
		// HTTPS and HTTP/2 over TLS.
		fib.Binding{Addr: "127.0.0.1:8443", Handler: fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), fibhttp.NewHandler(router))},
		// HTTP/3, on UDP; the port number may repeat, the transport differs.
		fib.Binding{Network: "udp", Addr: "127.0.0.1:8443", Handler: http3.NewHandler(serverTLS, router)},
		// A WebSocket endpoint of its own.
		fib.Binding{Addr: "127.0.0.1:8081", Handler: ws},
		// A Unix stream socket for administration.
		fib.Binding{Network: "unix", Addr: sock, Handler: fibhttp.NewHandler(router)},
	)
	if err != nil {
		panic(err) // one address refused: none of the five is kept
	}
	defer engine.Close()

	for i := range 5 {
		addrs, err := engine.BindingAddrs(i) // where each binding really listens; a ":0" port resolves here
		if err != nil {
			panic(err)
		}
		fmt.Println(i, addrs)
	}
	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

每个绑定在自己的地址上应答：

```sh
curl http://127.0.0.1:8080/                           # hello over HTTP/1.1
curl --http2-prior-knowledge http://127.0.0.1:8080/   # hello over HTTP/2.0
curl -k https://127.0.0.1:8443/                       # hello over HTTP/2.0
curl --unix-socket "${TMPDIR:-/tmp}/fib-tutorial.sock" http://localhost/
```

`:8081` 上的 WebSocket 绑定会回显 WebSocket 客户端发来的消息，客户端见 [09-websocket.zh-CN.md](09-websocket.zh-CN.md)；HTTP/3 绑定需要 HTTP/3 客户端，同第 01 章。socket 路径取自 `os.TempDir()`，在 macOS 上是 `$TMPDIR`，在 Linux 上是 `/tmp`。

`Binding` 还有 `Addrs`（一组共享 `Network` 的地址；此时 `Addr` 被忽略）、`MultipathTCP`（监听器属性，所以别的都归引擎，它仍留在绑定上）和 `Filter`，类型为 `func(netip.AddrPort) bool`，在事件循环上快速询问每个新对端——被拒绝的 TCP 连接直接关闭，被拒绝的 UDP 对端的数据报直接丢弃，不产生 `Connection`，也不占用 `MaxConns` 名额。

## 运行时增删绑定

引擎运行期间绑定可以来去。`AddBinding` 多监听一个并返回它的下标；`RemoveBinding` 立即停掉它的监听器，但已接受的连接保持打开；`ShutdownBinding` 按 `Shutdown` 关闭整个引擎的方式排空这个绑定——它的连接 `ShuttingDown` 为真，`ShutdownHandler` 会收到 `OnShutdown`，剩下的连接在 context 结束时关闭：

```go
canary := fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
	_ = c.String(200, "canary\n")
}))
i, err := engine.AddBinding(fib.Binding{Addr: "127.0.0.1:8082", Handler: canary})
if err != nil {
	panic(err)
}

ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
if err := engine.ShutdownBinding(ctx, i); err != nil {
	panic(err) // ctx.Err() if the drain timed out; the binding is gone either way
}
```

下标按 `New` 和 `AddBinding` 的顺序分配，且永不复用：绑定被删除后下标仍属于它，只是不再指向任何绑定。

## 优雅关闭

`Shutdown(ctx)` 立即关闭监听器，此后新来的客户端会被拒绝；然后等待引擎已接受的连接结束，或者等到 `ctx` 到期。如果 context 结束了等待它返回 `ctx.Err()`，否则返回 `nil`，随后停止引擎，使 `Run` 返回；此时仍打开的连接在 `Close` 之前保持打开。引擎主动拨出的连接不等待；UDP 监听器已知的对端要等待。`Stop` 是立即停止的形式：它关闭监听器并让 `Run` 马上返回，所有连接都留给 `Close`。下面是收到 `SIGINT` 或 `SIGTERM` 时排空连接的服务器：

```go
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr: "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
			_ = c.String(200, "hello\n")
		})),
	})
	if err != nil {
		panic(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		// Listeners stop at once; open connections get up to 10s to finish.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := engine.Shutdown(ctx); err != nil {
			_, _ = os.Stderr.WriteString("shutdown: " + err.Error() + "\n")
		}
	}()

	if err := engine.Run(); err != nil {
		panic(err)
	}
	_ = engine.Close() // Run returned: release whatever Shutdown left open
}
```

引擎分辨不出连接是空闲还是在一次交换中间，协议可以。实现了 `fib.ShutdownHandler` 的 handler（在四个 `Handler` 回调之外再加 `OnShutdown(*fib.Connection)`）会在排空开始时收到通知，之后只要连接还开着就按递减的频率继续收到通知——HTTP 服务器正是靠它在排空时立即关闭空闲的 keep-alive 连接、在响应写完后关闭忙的连接，并发送 HTTP/2 的 GOAWAY。普通 handler 里也可以用 `c.Conn.ShuttingDown()` 问同一个问题，在引擎排空时让就绪探针失败。

## 一个进程里的多个引擎

一个进程可以运行多个引擎，而且当它们没有可共享的东西时通常应该这样做：每个协议一个引擎要多一套循环、池和缓冲池，但彼此隔离。同名引擎在 `SharedTaskPool` 打开时（默认打开）跑在同一个工作协程池上：最先启动的引擎按自己的设置建池，其余引擎直接使用。同名引擎还总是共享同一个 handler 池 `<Name>-streams`，与 `SharedTaskPool` 无关；它的上限跟随该名字下最大的工作协程池。把 `SharedTaskPool` 设为 false 可以让引擎拥有自己的工作协程池，再换一个 `Name` 才能把 handler 池也分开。`Name` 保持默认值 `fib` 的引擎两个池都共享。

## 启动日志与日志配置

`DefaultConfig` 会设置 `LogSettings`，因此引擎启动时每个生效设置打印一行，带上值、来源是默认还是调用方设置、以及它的作用。`Engine.LogSettings()` 可以随时打印同一份列表，`Engine.Settings()` 返回这些值，`FIB_LOG_SETTINGS=0` 只关闭启动时的列表而不改配置。`LogStatus` 是另外那条一行的“engine started”横幅。

fib 的所有包都通过 `github.com/lesismal/fib/log` 记录日志；在设置之前，日志写到 `slog.Default()`。要选择 fib 自己的格式：

```go
import (
	"log/slog"
	"os"

	fiblog "github.com/lesismal/fib/log"
)

fiblog.Setup(fiblog.Config{
	Format: fiblog.FormatJSON, // FormatLine (the default), FormatText or FormatJSON
	Level:  slog.LevelWarn,    // or a *slog.LevelVar to change it while the server runs
	Output: os.Stderr,         // the default
})
```

`FIB_LOG_FORMAT=json` 和 `FIB_LOG_LEVEL=warn` 可以在程序做任何配置之前完成同样的选择。

## 坑与限制

- **`fib.Config{}` 不是 `fib.DefaultConfig()`。** 布尔字段保持 false，协程池、缓冲区和日志的尺寸也不会填好；永远从 `DefaultConfig` 出发。
- **`New` 会复制配置。** `New` 返回后再改 `cfg` 对运行中的引擎没有任何影响。
- **一个坏绑定让全部失败。** 任一绑定的任一地址拿不到，`New` 一个都不保留；先确认 Unix socket 路径不存在、端口空闲，再怀疑 handler。
- **`Run` 是阻塞的，`Close` 要排在它之后。** 必须在 `Run` 返回之后再调用 `Close`；运行期间要停止它，调用 `Shutdown` 或 `Stop`。
- **`MaxConns` 只统计已接受的连接。** 主动拨出的连接和 UDP 对端不计入，客户端引擎和 UDP 绑定要自己限制数量。
- **共享池由第一个引擎决定。** 同名且打开 `SharedTaskPool` 的两个引擎不会各自使用自己的尺寸；请改名或关掉共享。`Name` 保持默认值的引擎全都共享同一个池。
- **`SetTaskPoolMode` 会重置池的尺寸。** 先调用它，再设置 `WorkerCount` 和 `MaxEvents`；或者用 `SetPoolSizing`，它固定尺寸，调用顺序无所谓。
- **调大 `ReadBufferSize` 要同时调水位。** `DefaultConfig` 是按 16 KiB 算的写水位；`DefaultConfigFor` 会同时调整两者。
- **运行时的绑定下标不复用。** 保存 `AddBinding` 返回的下标；`RemoveBinding` 之后它不指向任何绑定。
- **`RemoveBinding` 不关闭 TCP 连接。** 它只停止监听；要排空连接请用 `ShutdownBinding`。

## 相关文档

- [配置](../configuration.zh-CN.md)——`Config` 与 `Binding` 的每个字段、默认值、机器尺寸计算、日志
- [快速开始](01-getting-started.zh-CN.md)——第一个服务器，以及同样的五步
- [Handler：同步与异步](03-handlers.zh-CN.md)——这些绑定指定的 handler 到底做什么
- [架构](../architecture.zh-CN.html)——事件循环、工作协程和连接的一轮
- [性能与运维](22-performance.zh-CN.md)——协程池尺寸、`prefork`、`bufferpool`、压测
- [README](../../README.zh-CN.md)——特性总览和本章展开的快速开始

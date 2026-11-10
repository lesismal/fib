# 快速开始

[English](01-getting-started.md) | [简体中文](01-getting-started.zh-CN.md)

本章是从零到一个可运行的 fib 服务的最短路径：安装、写下每个 fib 程序都由之组成的短骨架，
并看到同一个 handler 同时回答 HTTP/1.x、HTTP/2、HTTP/3 和 `net/http` 的 mux。本章也给出后续
章节的地图，读者可以直接跳到需要的协议。

## fib 是什么

fib 是 Go 的事件驱动网络库。几个事件循环等待 I/O 就绪，共享的 worker 池执行每一次读、回调和
写，连接空闲时不占 goroutine。在这个底座之上，它提供 TCP、UDP 与 Unix socket、TLS、HTTP/1.x、
HTTP/2、HTTP/3、WebSocket、arpc、gRPC、SOCKS、KCP、WebRTC、流媒体和 BitTorrent——每种协议都是
一个交给 Engine 运行的 `fib.Handler`。

本教程的每个程序都由三条性质决定：

- **一个 Engine，多个 Binding。** `Engine` 持有事件循环、协程池和 buffer；每个 `Binding` 是一个
  （或一组）地址，加上服务它的 handler。`:80` 上的 HTTP、`:443` 上的 HTTPS、UDP `:443` 上的
  HTTP/3、`:50051` 上的 gRPC 和一个 Unix socket 可以同处一个 Engine。
- **`net/http` 的编程模型。** HTTP handler 收到 `*fibhttp.Context`，它持有标准 `*http.Request`，
  并实现 `http.ResponseWriter`。`net/http` 的 handler、`http.ServeMux`、`http.FileServer` 原样
  跑在 fib 上。
- **不依赖标准库之外的任何包。** fib 只 import 标准库（要求 Go 1.27.3 或更新），所以教程里的
  每个例子都可以写成普通的 `package main`。

## 安装

建一个模块并引入 fib：

```sh
mkdir demo && cd demo
go mod init example.com/demo
go get github.com/lesismal/fib
```

不需要 cgo、C 工具链或配置文件；socket 由 Engine 自己打开。

## 第一个服务

一个 fib 程序就是一份配置、一个或多个 Binding，以及一个 handler：

```go
package main

import (
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte("hello\n"))
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

用 `go run .` 运行，再用 curl 请求它：

```sh
curl -v http://127.0.0.1:8080/
curl --http2-prior-knowledge http://127.0.0.1:8080/
```

第一个请求得到 HTTP/1.1 响应，第二个得到 HTTP/2 响应（h2c，明文 HTTP/2）：同一个 Binding、同一个
handler 同时服务两者，因为 `http` 包能在同一条 TCP 连接上识别 HTTP/1 请求和 HTTP/2 preface。

由内向外读这个程序：

- `fibhttp.HandlerFunc` 把函数适配成 `fibhttp.Handler`（`ServeHTTP(*fibhttp.Context)`）。Context
  既是请求也是响应：它持有 `c.Request`，实现 `http.ResponseWriter`，所以
  `c.Respond(status, contentType, body)` 就是一步到位的应答。
- `fibhttp.NewHandler(app)` 把这个 handler 包进 HTTP 协议处理器——后者解析请求、给响应分帧、
  执行 HTTP 限制，然后把每个请求交给 `app`。
- `fib.Binding` 说明监听哪里（`Addr`）、由哪个 handler 服务这些连接。`Binding.Network` 选择
  `tcp`（默认）、`udp`、`unix` 及其变体。
- `fib.New` 复制配置、打开所有 Binding；只要有一个地址拿不到就整体失败。`engine.Run()` 阻塞到
  Engine 停止；`engine.Close()`（这里 defer）负责收尾。

## 一个 handler，HTTP/2 与 HTTP/3

TLS 不改变 handler，只改变它前面的协议栈。`fib/tlstest` 会为 `localhost` 和 `127.0.0.1` 签发一张
一次性证书，所以例子不需要任何文件：

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
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8",
			[]byte("hello over "+c.Request.Proto+"\n"))
	})

	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}

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
curl -k --http2 https://127.0.0.1:8443/     # hello over HTTP/2.0
curl -k --http1.1 https://127.0.0.1:8443/   # hello over HTTP/1.1
```

需要 `-k`，是因为 `tlstest` 每次进程启动都会新生成一张自签名证书，任何客户端都不信任它。

`fibhttp.ConfigureTLS` 把 `h2` 和 `http/1.1` 加进 ALPN 列表；`fibtls.NewServer` 在连接上完成 TLS
握手，再把连接交给 HTTP handler，所以 `app` 仍然不知道自己在回答哪个协议。TLS 见
[10-tls.zh-CN.md](10-tls.zh-CN.md)，正式证书也在那里讨论；`tlstest` 只用于测试和示例。

HTTP/3 就是再加一个 UDP Binding，handler 不变：

```go
package main

import (
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

func main() {
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8",
			[]byte("hello over "+c.Request.Proto+"\n"))
	})

	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}

	engine, err := fib.New(fib.DefaultConfig(),
		fib.Binding{
			Addr:    "127.0.0.1:8443",
			Handler: fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), fibhttp.NewHandler(app)),
		},
		fib.Binding{
			Network: "udp",
			Addr:    "127.0.0.1:8443",
			Handler: http3.NewHandler(http3.ConfigureTLS(serverTLS), app),
		},
	)
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
curl -k --http3 https://127.0.0.1:8443/    # needs curl built with HTTP/3
```

一份 `tlstest` 配置就能同时用于两个 Binding：`fibhttp.ConfigureTLS` 和 `http3.ConfigureTLS` 都返回
副本，各自带上自己的 ALPN 列表（`h2`/`http/1.1`；以及 `h3`，并要求 TLS 1.3）；而且
`http3.NewHandler` 自己会调用 `http3.ConfigureTLS`，显式调用只是把这个要求写在明面上。很多 curl
（包括 macOS 自带的）不支持 HTTP/3；[08-http3.zh-CN.md](08-http3.zh-CN.md) 里的 fib HTTP/3 客户端能从
同一个服务拿到 `hello over HTTP/3.0`。

TCP Binding 和 UDP Binding 可以共用同一个端口号：它们是不同的传输。浏览器通过 HTTP/1 或 HTTP/2
响应上的 `Alt-Svc` 头发现 HTTP/3 端点，怎么发这个头见 [08-http3.zh-CN.md](08-http3.zh-CN.md)。

## 在 fib 上跑 `net/http` 的 handler

fib 并不要求 fib 风格的 handler。`*fibhttp.Context` 就是 `http.ResponseWriter`，所以一行适配代码
就能服务任何 `http.Handler`，包括标准库的 mux、文件服务器和第三方路由库：

```go
package main

import (
	"fmt"
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	mux := stdhttp.NewServeMux()
	mux.HandleFunc("GET /hello/{name}", func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		fmt.Fprintf(w, "hello %s\n", r.PathValue("name"))
	})
	mux.Handle("/files/", stdhttp.StripPrefix("/files/", stdhttp.FileServer(stdhttp.Dir("."))))

	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		mux.ServeHTTP(c, c.Request) // c is the http.ResponseWriter
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

```sh
curl http://127.0.0.1:8080/hello/fib
curl -OJ http://127.0.0.1:8080/files/go.mod   # the file server serves the working directory
```

哪些行为照旧、哪些不同，[README](../../README.zh-CN.md) 里有摘要，细节在
[06-http1.zh-CN.md](06-http1.zh-CN.md)。简单说：流式响应、flush、文件、trailer 和
`http.ResponseController` 的 deadline 都能用；`Hijack` 不行，因为 fib 用 `Context.Upgrade` 切换
协议（见 [09-websocket.zh-CN.md](09-websocket.zh-CN.md) 和
[13-socks-connect.zh-CN.md](13-socks-connect.zh-CN.md)）。

## 运行、日志与停止

- `go run .` 构建并运行。每个 Engine 启动时逐项打印生效配置；`FIB_LOG_SETTINGS=0` 关闭这份列表，
  `engine.LogSettings()` 随时按需打印。
- `engine.Run()` 阻塞。要在收到信号时退出，就在另一个 goroutine 里调用 `engine.Shutdown(ctx)`
  （优雅停机）或 `engine.Stop()`（立即停止），让 `Run` 返回，然后再 `engine.Close()`；`Close`
  本身只能在 `Run` 返回之后调用。[02-engine.zh-CN.md](02-engine.zh-CN.md) 给出写法。
- 本教程的例子都监听 `127.0.0.1`，不对外暴露。要让所有网卡监听就用 `:8080`。端口 0 让内核挑一个
  空闲端口，`engine.BindingAddrs(i)` 报告实际绑定的是哪个。
- 教程里每个完整程序都由教程自己的测试编译（`go test ./docs/tutorial`），所以这里展示的 API 就是
  当前 checkout 的 API。

## 教程的组织方式

章节按阅读顺序分组，读者可以在需要的那部分停下：

| 部分 | 章节 | 内容 |
| --- | --- | --- |
| 基础 | 02–05 | Engine 与配置、两套 handler 模型（同步与异步）、路由、中间件 |
| HTTP、WebSocket 与 TLS | 06–10 | HTTP/1.x、HTTP/2、HTTP/3、WebSocket、TLS |
| RPC 与代理协议 | 11–14 | gRPC、arpc、SOCKS 与 HTTP CONNECT、KCP |
| 更多协议与模块 | 15–19 | 单端口多协议、WebRTC、流媒体、BitTorrent、原生 TCP/UDP/Unix |
| 客户端、hack 与运维 | 20–22 | 客户端写法、让 fib 更快的技巧、性能与运维 |

其中 02、03、21 是公共章节：[02-engine.zh-CN.md](02-engine.zh-CN.md) 是教程形式的配置参考，
[03-handlers.zh-CN.md](03-handlers.zh-CN.md) 讲两套 handler 模型以及同步/异步之分，
[21-hacks.zh-CN.md](21-hacks.zh-CN.md) 收集了直白 API 之外的各种技巧。

## 坑与限制

- **handler 不能阻塞。** handler 跑在 Engine 的 worker 池上，而不是自己独占的 goroutine。在
  handler 里等对端、等锁或做慢 I/O 会占住一个 worker；fib 的答案是
  [03-handlers.zh-CN.md](03-handlers.zh-CN.md) 里的异步写法，或者自己的 task pool。
- **什么都不写就返回，等于 200 空 body**，与 `net/http` 一致。如果响应在别处产生，先 `Retain`，
  响应完成时再 `Release`。
- **`fib.Config{}` 不是 `fib.DefaultConfig()`。** 零值配置不会填好协程池、buffer 和日志配置；始终从
  `DefaultConfig()` 出发再改字段。
- **一个 Binding 失败，整个 Engine 失败。** `fib.New` 不会保留任何一个地址。
- **`Close` 在 `Run` 之后。** `Close` 用于 `Run` 已经返回的 Engine；要停掉正在运行的 Engine，
  调用 `Shutdown` 或 `Stop`，等 `Run` 返回后再关闭它。
- **让 Engine 活着。** 关掉它就停掉所有 Binding；只做客户端的 Engine 同样要比它的连接活得久。

## 相关文档

- [引擎与配置](02-engine.zh-CN.md) — `Config`、`Binding`、运行时增删 Binding、优雅停机
- [Handler：同步与异步](03-handlers.zh-CN.md) — 执行模型与异步 API
- [配置](../configuration.zh-CN.md) — 全部 `Config`、`Binding` 字段及默认值
- [README](../../README.zh-CN.md) — 特性、架构与基准一览
- [Hack 指南](../hacks.zh-CN.md) — 第 [21-hacks.zh-CN.md](21-hacks.zh-CN.md) 章浓缩的内容
- [fib-examples](https://github.com/lesismal/fib-examples) — 每种协议的可运行示例

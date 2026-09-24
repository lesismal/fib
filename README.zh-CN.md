# fib — Fast In Balance

[![CI](https://github.com/lesismal/fib/actions/workflows/ci.yml/badge.svg)](https://github.com/lesismal/fib/actions/workflows/ci.yml)

[English](README.md) | [简体中文](README.zh-CN.md)

事件驱动的 Go 网络库，附带同架构的 C11 实现。单个 edge-triggered 事件循环收集 I/O
就绪事件，每个 connection 作为一个任务调度到 worker 池：同一 connection 的事件按顺序
执行，任意空闲 worker 都能执行任意 connection，负载按实际任务量均衡，而不是按 fd 数量。

## 特性

- **原生后端**：Linux 用 epoll，macOS 用 kqueue，Windows 用 IOCP，其他系统使用兼容后端
- **connection 级调度**：每个 connection 内 FIFO，不绑定 worker，worker 池自适应伸缩
- **背压**：单连接写水位线，加上整个 server 共享的待发送字节预算
- **零拷贝路径**：`sendfile`、批量 `writev`、池化 buffer
- **协议**：TCP、UDP、Unix socket、TLS、HTTP/1.x、HTTP/2（h2 与 h2c）、基于 QUIC 的 HTTP/3、WebSocket
- **异步 client**：非阻塞 Dial，以及 HTTP/1.x、HTTP/2、HTTP/3 client
- **中间件**：compress、cors、csrf、etag、limiter、logger、pprof、recover、requestid、responsetime

## 快速上手

```sh
go get github.com/lesismal/fib/go
```

```go
package main

import (
	stdhttp "net/http"

	fib "github.com/lesismal/fib/go"
	fibhttp "github.com/lesismal/fib/go/http"
	"github.com/lesismal/fib/go/middleware"
	"github.com/lesismal/fib/go/middleware/logger"
	"github.com/lesismal/fib/go/middleware/recover"
)

func main() {
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context, r *stdhttp.Request) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte("hello\n"))
	})

	config := fib.DefaultConfig()
	config.Addr = "127.0.0.1:8080"
	engine, err := fib.Bind(config, fibhttp.NewHandler(middleware.Chain(app, recover.New(), logger.New())))
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	if err := engine.Run(); err != nil {
		panic(err)
	}
}
```

同一个 `Handler` 在同一端口上同时服务 HTTP/1.x 和 HTTP/2；HTTP/3 见 `examples/http3`。

## 子 package

| Package | 说明 |
| --- | --- |
| [`fib`](go) | 事件循环、connection、TCP / UDP / Unix socket、`SendFile`、异步 Dial |
| [`taskpool`](go/taskpool) | 有界 worker 池：adaptive、cond、elastic 三种模式 |
| [`bufferpool`](go/bufferpool) | 按尺寸档对齐的 buffer 池 |
| [`tls`](go/tls) | 叠加在 connection 上的 TLS，上层协议无需改动 |
| [`http`](go/http) | HTTP/1.x 与 HTTP/2 的 server 和 client |
| [`http3`](go/http3) | HTTP/3、QUIC、QPACK 的 server 和 client |
| [`websocket`](go/websocket) | RFC 6455 server 和 client，支持 permessage-deflate |
| [`middleware`](go/middleware) | HTTP 中间件链与常用中间件 |

## 示例

```sh
cd go
go run ./examples/tcp/nontls/server
go run ./examples/http/nontls/server
go run ./examples/websocket/nontls/server
go run ./examples/http3/tls/server
```

每个 server 旁边都有对应的 `client`。

## 文档

- [Go 使用指南](go/README.zh-CN.md)：配置、API 和各子 package 的用法
- [HTTP/1.x](docs/http1.zh-CN.md)、[HTTP/2](docs/http2.zh-CN.md)、[HTTP/3](docs/http3.zh-CN.md)：支持范围、限制和一致性测试
- [架构文档](docs/architecture.html)：中英文可切换的交互式架构图，涵盖读取调度、写背压、负载均衡和连接关闭回收流程

## C 实现

Go 版所沿用架构的原始 Linux C11 实现位于 [`c/`](c)，接口见
[`c/include/epoll_server.h`](c/include/epoll_server.h)。

```sh
make                      # 构建
./c/echo_server 9000      # 试一下：printf 'hello\n' | nc 127.0.0.1 9000
make test                 # 并发集成测试
```

## 构建与测试

```sh
make go         # go build ./...
make go-test    # go test ./...
```

需要 Go 1.27 或更高版本。

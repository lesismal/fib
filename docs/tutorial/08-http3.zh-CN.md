# HTTP/3

[English](08-http3.md) | [简体中文](08-http3.zh-CN.md)

`fib/http3` 在 UDP 绑定上通过 QUIC 提供 HTTP/3，使用的 `fibhttp.Handler` 和 `*fibhttp.Context` 与 HTTP/1.x、HTTP/2 完全相同——handler 并不知道请求来自哪种协议。QUIC 与 QPACK 由 fib 基于 `crypto/tls.QUICConn` 自行实现，不依赖 quic-go 或 `x/net`；TLS 1.3 与 `h3` ALPN 是强制的：不存在明文 HTTP/3。本章展示只服务 HTTP/3 的服务、同一引擎同时服务三种协议、让浏览器找到 QUIC 端点的 Alt-Svc 通告、客户端，以及 QUIC 带来的限制；协议参考见 [HTTP/3](../http3.zh-CN.md)。

## 模型

HTTP/3 服务就是一个 UDP 绑定，其 handler 是 `http3.NewHandler(tlsConfig, app)`。引擎把每个 datagram 交给其来源地址对应的 `fib.Connection`，每个发来 QUIC Initial 的地址在这条共享的引擎连接上拥有自己的 QUIC 连接；服务端按对端地址解复用，而不是每个连接一个 goroutine。默认情况下握手是引擎 worker 上的状态机（`BlockingTLSHandshake` 改为让 goroutine 停在 `crypto/tls` 里）。完整请求进入引擎的 handler 池，与 HTTP/2 完全一样，所以 handler 并发执行、阻塞只影响自己。响应写成 QUIC 流帧，受连接的拥塞控制与流量控制约束；datagram 成批读取，确认与响应尽量打包进最少的包一起发出。

## 一个 HTTP/3 服务

handler 和其他地方是同一个函数，`c.Request.Proto` 为 `HTTP/3.0`，绑定的 network 是 `udp`：

```go
package main

import (
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
	"github.com/lesismal/fib/tlstest"
)

func main() {
	tlsConfig, _, err := tlstest.Configs() // a throwaway certificate for 127.0.0.1
	if err != nil {
		panic(err)
	}
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.String(stdhttp.StatusOK, "hello over %s\n", c.Request.Proto)
	})
	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Network: "udp",
		Addr:    "127.0.0.1:8443",
		Handler: http3.NewHandler(http3.ConfigureTLS(tlsConfig), app),
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

`http3.ConfigureTLS` 复制配置、把 `h3` 放进 ALPN 列表并要求 TLS 1.3；`NewHandler` 自己会做这一步，上面显式调用只是让这个要求可见。用一个支持 HTTP/3 的客户端验证：

```sh
curl --http3 -k https://127.0.0.1:8443/   # needs curl built with HTTP/3
```

## 一个 handler 服务 h1、h2 与 h3

同一个引擎可以在某端口上用 TCP 服务 HTTP/1.x 和 h2、用同号 UDP 端口服务 HTTP/3——它们是不同的传输层，端口号可以相同。TLS 配置要分开，因为两种栈各有自己的 ALPN：h1/h2 用 `fibhttp.ConfigureTLS`，h3 用 `http3.ConfigureTLS`。handler `app` 是共享的。

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
	tlsConfig, _, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		// Tell h1/h2 clients that this origin also speaks HTTP/3 on 8443.
		if !c.Request.ProtoAtLeast(3, 0) {
			c.Header().Set("Alt-Svc", http3.AltSvc(8443))
		}
		_ = c.String(stdhttp.StatusOK, "hello over %s\n", c.Request.Proto)
	})
	engine, err := fib.New(fib.DefaultConfig(),
		fib.Binding{
			Addr:    "127.0.0.1:8443",
			Handler: fibtls.NewServer(fibhttp.ConfigureTLS(tlsConfig), fibhttp.NewHandler(app)),
		},
		fib.Binding{
			Network: "udp",
			Addr:    "127.0.0.1:8443",
			Handler: http3.NewHandler(tlsConfig, app),
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

HTTP/1.1 或 HTTP/2 客户端会收到 `Alt-Svc: h3=":8443"; ma=86400`，可能把后续请求切换到 QUIC；直接拿到 h3 地址的客户端则不需要这个 header。

## 用 Alt-Svc 通告 HTTP/3

`http3.AltSvc(port)` 只是 header 值——`h3=":<port>"; ma=86400`——所以在一个普通的 h1/h2 响应上设置它就是全部集成工作：

```go
func announce(c *fibhttp.Context) {
	c.Header().Set("Alt-Svc", http3.AltSvc(8443)) // h3=":8443"; ma=86400
	_ = c.String(stdhttp.StatusOK, "this origin also speaks h3\n")
}
```

`http3.AltSvcClient` 是客户端侧的对应物：它包装一个 `*fibhttp.Client` 和一个 `*http3.Client`，跟随这个 header，并在 QUIC 尝试失败时回落到 TCP。第 [20](20-clients.zh-CN.md) 章会讲。

## 配置 QUIC 与 TLS

与 HTTP/1、HTTP/2 服务不同的选项放在 `http3.Config` 里，通过 `NewHandlerWithConfig` 传入。配置里的 `TLSConfig` 是必填的；`NewHandlerWithConfig` 不会从别处补上它。

```go
config := http3.DefaultConfig()
config.TLSConfig = tlsConfig             // required: TLS 1.3 and ALPN h3 are added by the handler
config.MaxConcurrentStreams = 100        // requests a client may have open, the default
config.MaxIdleTimeout = 30 * time.Second // keep it below the engine's UDPIdleTimeout (60s)
config.StreamRequestBody = true          // big uploads stream instead of buffering
config.StreamPool.MaxConcurrentHandlers = 16
handler := http3.NewHandlerWithConfig(config, app)
```

引擎级的 UDP 设置仍然生效：`Config.UDPIdleTimeout`（默认 60 秒）会关闭长期沉默的 UDP 对端，所以 QUIC 的 `MaxIdleTimeout` 必须低于它，否则引擎会先关掉安静的连接。生产环境需要真实证书；`tlstest.Configs()` 只用于示例和测试，证书与 `http3.ConfigureTLS` 的细节见 [TLS](10-tls.zh-CN.md)。

## 客户端

`http3.NewClient(engine, config)` 通过引擎拨出的连接发送请求，每个 host:port 一条多路复用连接，不阻塞；`Do` 走回调，`Go` 返回 `Future`。引擎可以是没有任何绑定的纯客户端引擎。

```go
package main

import (
	"fmt"
	"io"
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/http3"
	"github.com/lesismal/fib/tlstest"
)

func main() {
	engine, err := fib.New(fib.DefaultConfig()) // a client engine has no bindings
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	go engine.Run()

	_, clientTLS, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}
	config := http3.DefaultClientConfig()
	config.TLSConfig = clientTLS
	client := http3.NewClient(engine, config)
	defer client.Close()

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "https://127.0.0.1:8443/", nil)
	if err != nil {
		panic(err)
	}
	resp, err := client.Go(req).Wait() // Future: block instead of taking a callback
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s: %s", resp.Status, body)
}
```

默认情况下响应 body 在回调前已整体缓冲；`ClientConfig.StreamResponseBody` 让大 body 边到边交付。超时或取消只重置自己那条流，而且只有服务端标记为未处理的请求才会重试。这里只是指路——完整的客户端内容见第 [20](20-clients.zh-CN.md) 章。

## 迁移、早期数据与限制

- **跟随 NAT rebinding，但不做主动迁移。** 来自新地址的 1-RTT 包通过 Connection ID 路由，经 `PATH_CHALLENGE` 验证后连接继续（RFC 9000 §9.3）；服务端仍然通告 `disable_active_migration`，所以客户端自行探测新路径不会得到应答。
- **0-RTT 默认关闭。** 服务端用 `http3.Config.EarlyData`、客户端用 `ClientConfig.EarlyData` 打开；以 early data 到达的请求带 `Early-Data: 1`，只有 GET、HEAD、OPTIONS、TRACE 会被服务，其他方法一律 425 Too Early。early data 按设计可被重放，安全方法规则就是防护。
- **没有 server push。** `Context.Push` 在 HTTP/3 上返回 `stdhttp.ErrNotSupported`；请改用 103 Early Hints。
- **没有 DATAGRAM 和 WebTransport。** 对端发来的 DATAGRAM 帧属于未知帧，会关闭连接。
- **内存上限**：整体 body 每连接 `MaxConcurrentStreams × MaxBodyBytes`（默认 100 × 16MB）；流式 body 由其 QUIC 流窗口（1MB）约束。
- **引擎退出**：`Engine.Shutdown` 发送 GOAWAY、等待在途请求完成并以 `H3_NO_ERROR` 关闭；单用 `Engine.Stop`/`Close` 既不发送 GOAWAY 也不发送 CONNECTION_CLOSE，客户端只能等到超时才发现。
- **平台**：与 `fib/http` 一样，`fib/http3` 只在 Linux、macOS、Windows 原生后端上构建；在可移植后端上这个包是空的。
- **互操作**：一致性测试在 CI 中同时把服务端和客户端对着 quic-go、quiche 跑；见 [HTTP/3](../http3.zh-CN.md#一致性测试)。

## 坑与限制

- **不存在明文 HTTP/3。** 没有 TLS 配置的 `udp` 绑定不能提供 HTTP/3：QUIC 建立在 TLS 1.3 之上，`ConfigureTLS`（ALPN `h3`）和证书都必需。
- **`MaxIdleTimeout` 要低于 `UDPIdleTimeout`。** 默认值下 QUIC 的 30 秒低于引擎的 60 秒；把前者调过后者会让引擎先回收安静的连接。
- **不要直接写 `Context.Conn`。** 在 HTTP/3 上它是引擎的 UDP 连接；响应必须走 `Context`，否则流的分帧会被破坏。
- **两个绑定、两种协议，可以同号。** TCP :8443 与 UDP :8443 是两个独立绑定；只改 h1/h2 的端口而不改 Alt-Svc 的值，会把客户端送到没人监听的端口。
- **重试以流为单位。** 超时只取消自己那条流，其他流继续；连接在响应已到达一部分后断开时，请求失败而不是重发。
- **handler 与请求的生命周期规则不变。** HTTP/3 也会复用请求/流对象（除非 `Config.DisableReuse`）；不要把它们留到响应和最后一次 `Release` 之后，也不要在调用 `Respond` 之后修改 body。
- **教程里的证书是测试证书。** `tlstest.Configs()` 只用于示例；浏览器不会信任它，真实部署需要与源站名匹配的证书。

## 相关文档

- [HTTP/3](../http3.zh-CN.md) — 协议参考：QUIC、超时、QPACK、0-RTT、互操作。
- [HTTP/2](07-http2.zh-CN.md) — HTTP/3 共享的多路复用模型，以及 handler 在池上如何运行。
- [HTTP/1.x](06-http1.zh-CN.md) — 本章假定的请求/响应 API。
- [TLS](10-tls.zh-CN.md) — 证书、ALPN 与 `http3.ConfigureTLS` 的深入讲解。
- [客户端](20-clients.zh-CN.md) — HTTP/3 客户端、`AltSvcClient` 与连接管理。
- [配置](../configuration.zh-CN.md) — 引擎级 UDP 设置与所有服务配置字段。

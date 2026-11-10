# TLS

[English](10-tls.md) | [简体中文](10-tls.zh-CN.md)

`fib/tls` 在 fib 的连接之上运行 TLS，密码学部分交给 `crypto/tls`：`fibtls.Handler` 位于普通 `fib.Handler` 之前，完成握手后把明文交给被包裹的 handler——因此 `http`、`websocket`、`grpc` 以及原生 TCP 的 handler 无需修改就能跑在 TLS 上。本章介绍 `fibtls.NewServer` 与 `NewClient`、决定连接上承载哪种协议的 ALPN 栈、客户端的 `DialOptions`、双向认证（mTLS）、密码套件与版本选择，以及 WebRTC 使用的 DTLS。读者应已了解 HTTP 与 WebSocket 相关章节（[06-http1.md](06-http1.zh-CN.md)、[07-http2.md](07-http2.zh-CN.md)、[09-websocket.md](09-websocket.zh-CN.md)）和 `crypto/tls` 的 API。

## 模型

- **两层结构。** `crypto/tls` 负责握手：`Config`、证书、校验、ALPN 以及版本与密码套件的选择——就是你会交给 `tls.Listen` 的同一个 `*crypto/tls.Config`。握手完成之后，对于常见的 AES-GCM（TLS 1.3 与 1.2）和 AES-CBC（TLS 1.2 与 1.1）套件，记录由 fib 自己加封与解封。被包裹的 handler 完全看不到记录：它的 `Send` 会加密，`OnData` 收到的是解密后的字节。
- **握手不阻塞引擎。** 默认情况下，能以状态机方式运行的握手就以状态机运行，等待对端时不占任何协程；需要 `crypto/tls` 的 `Config` 则在独立于引擎 worker 的握手协程池的一个 worker 上运行。`Handler.Blocking` 强制使用后一种方式，`Handler.HandshakeTimeout` 限制握手时间（默认 10 秒；负值表示不限时）。
- **被包裹的 handler 很早就拿到连接。** 它的 `OnOpen` 在握手之前运行，可以立即发送；发出的内容会先被暂存，握手完成后按顺序加密发出。之后 `OnData` 只会收到明文。想得知握手何时完成的 handler 可以实现 `tls.HandshakeHandler`（`OnHandshake(*fib.Connection, tls.ConnectionState)`）；`fibtls.Handshaking` 与 `fibtls.ConnectionState` 可以随时回答同一个问题。
- **`fibtls` 不解释 ALPN。** 协商出的协议保存在连接状态里，由被包裹的 handler 决定它意味着什么。各协议包的 `ConfigureTLS` 辅助函数会加入正确的 `NextProtos`：`http.ConfigureTLS` 加入 `h2` 与 `http/1.1`，`http3.ConfigureTLS` 加入 `h3` 并要求 TLS 1.3，`grpc.ConfigureTLS` 加入 `h2`。

## TLS 回显服务与客户端

`tlstest.Configs()` 会为 `localhost` 与 `127.0.0.1` 签发一张一次性证书，并返回信任它的客户端配置，因此示例不需要任何文件。这些配置是进程内的，所以下面的示例把两端放在同一个程序里：

```go
package main

import (
	"log"
	"time"

	fib "github.com/lesismal/fib"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

func main() {
	serverTLS, clientTLS, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}
	server, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr: "127.0.0.1:8443",
		Handler: fibtls.NewServer(serverTLS, fib.HandlerFuncs{
			Data: func(c *fib.Connection, data []byte) {
				_ = c.Send(data)
			},
		}),
	})
	if err != nil {
		panic(err)
	}
	defer server.Close()
	go func() { _ = server.Run() }()

	client, err := fib.New(fib.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer client.Close()
	go func() { _ = client.Run() }()

	done := make(chan struct{})
	echo := fib.HandlerFuncs{
		Open: func(c *fib.Connection) {
			_ = c.Send([]byte("hello over TLS\n"))
		},
		Data: func(c *fib.Connection, data []byte) {
			log.Printf("echo: %s", data)
			if state, ok := fibtls.ConnectionState(c); ok {
				log.Printf("version 0x%04x, ALPN %q", state.Version, state.NegotiatedProtocol)
			}
			close(done)
		},
	}
	err = fibtls.Dial(client, "tcp", "127.0.0.1:8443", 5*time.Second, clientTLS, echo,
		func(c *fib.Connection, err error) {
			if err != nil {
				log.Printf("dial: %v", err)
			}
		})
	if err != nil {
		panic(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Println("timed out")
	}
}
```

`NewServer(config, handler)` 与 `NewClient(config, handler)` 都只是返回一个 `*fibtls.Handler`，fib 会像绑定其他 handler 一样绑定它；`fibtls.Handler` 把握手的可调项暴露为字段。`fibtls.Dial` 是 `Engine.DialWithHandler` 加一次 TLS 握手：配置里没有指定服务器名时，它会像 `crypto/tls.Dial` 一样用地址中的主机名填好 `ServerName`；它的 `done` 回调在连接建立时触发，**早于**握手完成——从 `done` 或 `OnOpen` 发出的内容都会等待握手，然后按顺序加密发出。

## HTTPS 与 HTTP/2

把 `http` 包的 handler 包进 TLS 就是 HTTPS，而 `http.ConfigureTLS` 加入 ALPN 后，同一个服务还能应答 HTTP/2：

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
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8",
			[]byte("hello over "+c.Request.Proto+"\n"))
	})
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
curl -k --http2 https://127.0.0.1:8443/
```

由客户端的 ALPN 提议决定：提议里有 `h2` 就按 HTTP/2 服务，否则按 HTTP/1.1，而 `app` 对此一无所知——`c.Request.Proto` 会报告实际应答的协议。不加 `http.ConfigureTLS` 时 TLS 栈没有 `NextProtos`，没有客户端会协商 h2，同一个监听器就只是一个仅 HTTP/1.1 的 HTTPS 服务。使用真实证书时，把 `tlstest.Configs()` 换成 `Certificates`（或 `GetCertificate`）里放着证书的配置即可；`tlstest` 只用于测试与示例。

## 仍由 crypto/tls 负责的部分

`fib/tls` 不替代 TLS API，只是调度它。以下仍属于 `crypto/tls`，应在那里配置：

- 证书及其选择：`Certificates`、`GetCertificate`、`GetConfigForClient`；
- 客户端认证：`ClientAuth`、`ClientCAs`，以及客户端的 `Certificates`、`GetClientCertificate`；
- 校验：`RootCAs`、`ServerName`、`InsecureSkipVerify`、`VerifyPeerCertificate`、`VerifyConnection`——回调和校验都运行在握手池的 worker 上，绝不会跑在引擎的事件循环上；
- 版本与套件：`MinVersion`、`MaxVersion`，TLS 1.2 用 `CipherSuites`；TLS 1.3 套件在 `crypto/tls` 里没有配置字段，客户端的提议顺序由 `Handler.TLS13CipherSuites` 决定；
- 会话恢复：`ClientSessionCache` 与 `SessionTicketKey`；握手以状态机方式运行的服务端会用自己的 ticket，密钥属于 `Handler`。

fib 增加的是数据路径——记录的加封与解封、一轮写入合并为一条记录，以及在 Linux 上把大文件 `SendFile` 响应的加封交给内核 TLS（`Handler.KernelTLSMinFile`，用 `NoKernelTLS` 关闭）。

## 感知 ALPN 的 handler

由被包裹的 handler 决定协商出的协议意味着什么。自定义的 `HandshakeHandler` 会收到一次握手结果，可以据此切换连接上的 handler：

```go
type multiplexer struct {
	h2  fib.Handler
	h11 fib.Handler
}

func (m multiplexer) OnOpen(c *fib.Connection) {}

func (m multiplexer) OnData(c *fib.Connection, data []byte) {}

func (m multiplexer) OnPriorityData(c *fib.Connection, data []byte) {}

func (m multiplexer) OnClose(c *fib.Connection, err error) {}

func (m multiplexer) OnHandshake(c *fib.Connection, state stdtls.ConnectionState) {
	if state.NegotiatedProtocol == "h2" {
		c.SetHandler(m.h2)
		return
	}
	c.SetHandler(m.h11)
}
```

`c.SetHandler` 会在任何明文到达之前装好选定的 handler，因为 `OnHandshake` 在握手输出加密之后、`OnData` 收到字节之前运行。这正是 `http` 包使用的机制；大多数程序应当让 `http.ConfigureTLS` 或 `http3.ConfigureTLS` 决定 ALPN 列表，并保持单一 handler 栈。

## 客户端拨号选项

`fibtls.DialOptions` 提供了 `fibtls.Dial` 参数之外的选择：

```go
err := fibtls.DialWithOptions(engine, "tcp", "127.0.0.1:8443", 5*time.Second, clientTLS,
	fibtls.DialOptions{
		HandshakeTimeout: 3 * time.Second,
		Blocking:         true,
	}, handler, func(c *fib.Connection, err error) {
		if err != nil {
			log.Printf("dial: %v", err)
		}
	})
```

- `HandshakeTimeout` 限制握手时间，作用同 `Handler.HandshakeTimeout`。
- `Blocking` 即使握手本可以状态机方式运行，也改用 `crypto/tls` 在握手池的 worker 上运行——用于对比两种方式，或应对在两种方式下行为不同的配置。
- `TLS13CipherSuites` 排列客户端 TLS 1.3 的提议顺序；`SessionCache` 与 `EarlyData` 在以状态机方式运行的握手上开启会话恢复与 0-RTT（服务端的 0-RTT 由 `Handler.EarlyData` 和 `EarlyDataHandler` 控制，`http.Config.EarlyData` 就是这样用的）。
- `Dial` 替换底层拨号，参数与 `Engine.DialWithHandler` 相同：`socks.Dialer` 的 `Dial` 可以让客户端经 SOCKS 代理到达服务端。

`fibtls.NewClient` 是给自行调用 `Engine.DialWithHandler` 的程序使用的底层形式。

## 双向认证

双向认证就是 `crypto/tls` 的配置，fib 只负责调度握手：

```go
clientCAs := x509.NewCertPool()
if !clientCAs.AppendCertsFromPEM(caPEM) {
	log.Fatal("no certificates in the client CA file")
}
serverTLS := &stdtls.Config{
	Certificates: []stdtls.Certificate{serverCert},
	ClientAuth:   stdtls.RequireAndVerifyClientCert,
	ClientCAs:    clientCAs,
	MinVersion:   stdtls.VersionTLS12,
}
clientTLS := &stdtls.Config{
	Certificates: []stdtls.Certificate{clientCert}, // the client's own certificate
	RootCAs:      serverCAs,
	ServerName:   "api.example.com",
}
```

`RequireAndVerifyClientCert` 让服务端用 `ClientCAs` 校验客户端证书链；没有证书、或证书不是该 CA 签发的客户端会握手失败，被包裹的服务端 handler 收到的是带错误的 `OnClose`，而不会收到任何 `OnData`。握手完成后，`fibtls.ConnectionState(c)` 会暴露 `PeerCertificates`、`VerifiedChains` 等字段，因此 handler 可以在 `OnHandshake` 之后（或从第一次 `OnData` 起，那时握手已经完成）读取客户端身份。

## 密码套件与版本

TLS 版本与 TLS 1.2 套件是 `crypto/tls.Config` 的字段；TLS 1.3 套件则属于 `Handler`，因为 `crypto/tls` 没有对应的配置字段：

```go
serverTLS := &stdtls.Config{
	Certificates: []stdtls.Certificate{cert},
	MinVersion:   stdtls.VersionTLS12,
	MaxVersion:   stdtls.VersionTLS13,
	CipherSuites: []uint16{
		stdtls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		stdtls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	},
}
client := fibtls.NewClient(clientTLS, handler)
client.TLS13CipherSuites = []uint16{stdtls.TLS_AES_128_GCM_SHA256}
```

`CipherSuites` 留空即使用 Go 的安全默认值。优先控制 `MinVersion` 而不是枚举套件：默认套件是按本机安全性与速度选好的，而且 TLS 1.3 套件不受 `CipherSuites` 影响。

## WebRTC 用的 DTLS

DTLS 不是流上的 TLS，因此是另一套 API：`fibtls.NewDTLSServer` 与 `fibtls.NewDTLSClient` 返回的 `*DTLSConn` 以 sans-IO 方式运行 DTLS 1.2（RFC 6347）和 DTLS-SRTP（RFC 5764）——数据报由调用方搬运。`webrtc` 包用它承载媒体与数据通道；WebRTC 一侧见 [16-webrtc.md](16-webrtc.zh-CN.md)。普通 TCP 服务请使用 `fibtls.NewServer` 与 `fibtls.Dial`，而不是 DTLS API。

## 坑与限制

- **`tlstest` 是进程内的。** 它的证书与 CA 在每个进程里只签发一次，因此两个分别启动的程序彼此并不信任。它适合单进程测试；真实证书通过 `Config.Certificates`/`RootCAs` 提供。
- **在第一次握手前改好配置。** `Handler` 在首次使用时克隆配置，之后的修改不会被看到；已在使用的 `Config` 本来也不允许修改，这是 `crypto/tls` 的要求。
- **缺少 ALPN 列表会静默退化为 HTTP/1.1。** TLS 依然成功，h2 客户端回落到 HTTP/1.1，而 gRPC 客户端会报协议错误。用对应协议的 `ConfigureTLS` 辅助函数加入 `NextProtos`。
- **不存在明文回退。** handler 为 `fibtls.NewServer` 的绑定只接受 TLS，不会嗅探，也不会“先试 TLS 再退回明文”。一个端口要跑多种协议时请用 `mux` 或 `multiproto` 包，见 [15-multiprotocol.md](15-multiprotocol.zh-CN.md)。
- **`done` 回调不代表握手结束。** 它在 TCP 连接建立时触发；证书错误之类的失败稍后才会出现在 `OnClose` 里。
- **客户端校验必须有一个名字。** 有 `RootCAs` 却没有 `ServerName` 又不设 `InsecureSkipVerify` 的客户端配置会失败；`fibtls.Dial` 会自动填好，`NewClient` 不会。

## 相关文档

- [HTTP/2](07-http2.zh-CN.md) —— ALPN、TLS 上的 h2 与 0-RTT，TLS 记录路径的参考
- [HTTP/3](08-http3.zh-CN.md) —— QUIC 的 TLS 1.3、`http3.ConfigureTLS` 与 h3 ALPN
- [WebSocket](09-websocket.zh-CN.md) —— WebSocket 客户端与服务器上的 `wss://`
- [WebRTC](16-webrtc.zh-CN.md) —— DTLS 1.2 与 DTLS-SRTP，DTLS API 存在的原因
- [客户端](20-clients.zh-CN.md) —— 使用这些拨号选项的 HTTP/2 与 HTTP/3 客户端
- [README](../../README.zh-CN.md) —— TLS 特性概览

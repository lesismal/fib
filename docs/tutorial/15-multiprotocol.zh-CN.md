# 单端口多协议

[English](15-multiprotocol.md) | [简体中文](15-multiprotocol.zh-CN.md)

只要服务端能从客户端发来的最初几个字节判断出它说的是哪种协议，一个端口就能同时提供
HTTPS、h2c、SOCKS 和某种 RPC 协议。`mux` 包做的正是这件事：它维护一张有序的路由表，
持续读取直到某个匹配器认领连接，然后把连接彻底交给该路由的 handler。`multiproto`
包用你指定的协议生成这张表，并补上这些协议额外需要的读取——WebSocket 握手需要读完整的
请求头，gRPC 需要看第一个 HTTP/2 请求头。本章假定你已了解
[02-engine.md](02-engine.zh-CN.md) 中的 `fib.Binding` 和
[06-http1.md](06-http1.zh-CN.md) 中的 HTTP handler。

## 模型

`mux.Handler` 是一个 `fib.Handler`，它只服务连接直到认出协议为止。打开连接时它设置读
超时；每次收到数据时按顺序询问每个匹配器当前字节是什么；某个路由匹配后，它调用
`Connection.SetHandler` 换上该路由的 handler，随即运行后者的 `OnOpen` 并把 mux 读到的
所有字节交给它。此后 engine 直接调用选中的 handler，中间没有任何分发层，`OnHangup` 和
`OnShutdown` 也是问这个 handler 而不是 mux，因此协议 handler 保持正常生命周期，只在
engine 关闭时停止。

匹配器的类型是 `mux.Matcher`，即 `func(input []byte) mux.Result`，返回 `mux.NoMatch`、`mux.NeedMore` 或
`mux.Match`。它运行在连接所属的 worker 上，不能阻塞，也不能保留输入切片——engine 在本轮
结束后会复用它。`NoMatch` 让该路由永久出局；`NeedMore` 让路由等待下一次读取；mux 会把
已收到的全部字节再次交给它，直到某个路由匹配或已到达 `Config.MaxPeek` 字节。

所有匹配器都继承的规则只有一条：**必须由客户端先说话**。TLS、HTTP、SOCKS 和 RPC 协议
都是这样；SMTP、FTP、MySQL 要等服务端先问候，因此没有字节可嗅探。这类协议需要独立的
`fib.Binding`；连到 mux 却一直沉默的对端会在 `Config.Timeout`（默认 10 秒）后断开。

## 用 `mux` 共享一个端口

路由按优先级顺序尝试。TLS 与 HTTP 靠首字节不会混淆，但 TLS 排在最前，因为 ClientHello 的
起始最不易产生歧义；SOCKS 与它们并列；最后一条路由是一个以 `PING` 开头的玩具文本协议。
没有路由识别的连接会以 `mux.ErrNoMatch` 关闭，因为这个 handler 没有配置 `Fallback`：

```go
package main

import (
	"fmt"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/mux"
	"github.com/lesismal/fib/socks"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

func main() {
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}

	web := fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_, _ = fmt.Fprintf(c, "hello over %s\n", c.Request.Proto)
	}))
	pinger := fib.HandlerFuncs{
		Data: func(c *fib.Connection, b []byte) {
			_ = c.Send(append([]byte("PONG "), b...))
		},
	}

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr: "127.0.0.1:8000",
		Handler: mux.New(
			// TLS clients: HTTPS and h2, told apart by ALPN inside.
			mux.Route{Match: mux.TLS(), Handler: fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), web)},
			// Clear HTTP/1.x and h2c, which one fib/http handler serves.
			mux.Route{Match: mux.HTTP(), Handler: web},
			mux.Route{Match: mux.SOCKS(), Handler: socks.NewServer(socks.Config{})},
			mux.Route{Match: mux.Prefix("PING"), Handler: pinger},
		),
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
curl -k https://127.0.0.1:8000/                       # TLS route: hello over HTTP/2.0 (ALPN)
curl http://127.0.0.1:8000/                           # HTTP route: hello over HTTP/1.1
curl --http2-prior-knowledge http://127.0.0.1:8000/   # HTTP route, h2c: hello over HTTP/2.0
curl --socks5 127.0.0.1:8000 https://example.com/     # SOCKS route, out to the internet
printf 'PING' | nc -w 1 127.0.0.1 8000                # Prefix route: PONG PING
printf 'XYZ' | nc -w 1 127.0.0.1 8000                 # no route: closed at once
```

路由内部的 handler 栈就是平常那一套：`fibtls.NewServer` 完成握手后把连接交给 HTTP
handler，`fibhttp.ConfigureTLS` 让 TLS 客户端选择 HTTP/2。mux 对这些一无所知，它只知道
第一个字节是 TLS 记录。如果某个路由的 `Match` 比其 handler 真正支持的协议更宽松，那就是
bug：mux 已经提交，handler 拿到连接后只会握手失败或解析出乱码，而不会回退到后面的路由。

对 mux 来说 TLS 只是一个协议族：`mux.TLS()` 在 ClientHello 的第三个字节就做出判断，
此时 SNI 还没有被解析，因此无法按主机名选择路由。SNI 属于 TLS 层的事务——一个 TLS
handler 可以用 `Config.GetCertificate` 按名字选择证书，`fibhttp.ConfigureTLS` 则按 ALPN
在 HTTP/1.1 与 HTTP/2 之间选择：

```go
serverTLS := &stdtls.Config{
	GetCertificate: func(hello *stdtls.ClientHelloInfo) (*stdtls.Certificate, error) {
		cert, ok := certs[hello.ServerName] // one certificate per host name
		if !ok {
			return nil, fmt.Errorf("no certificate for %q", hello.ServerName)
		}
		return &cert, nil
	},
}
```

如果不同名字需要完全不同的处理栈，就实现 `tls.HandshakeHandler`：它的 `OnHandshake` 在任何
明文到达之前就能拿到协商好的 `stdtls.ConnectionState`（SNI、ALPN），在那里分派内部的
handler。

## 匹配器

`mux` 自带常见共享端口协议的匹配器：

| 匹配器 | 识别的协议 |
| --- | --- |
| `mux.TLS()` | TLS 握手记录，覆盖 TLS 1.0 及以后的版本 |
| `mux.HTTP()` | `mux.HTTP1()` 与 `mux.HTTP2()` |
| `mux.HTTP1()` | 以 HTTP 或 WebDAV 方法开头的请求行 |
| `mux.HTTP2()` | HTTP/2 连接前言，即带 prior knowledge 的 h2c |
| `mux.SOCKS()` | `mux.SOCKS5()` 与 `mux.SOCKS4()` |
| `mux.SOCKS5()` | SOCKS5 greeting：版本号 5，且认证方法数不为 0 |
| `mux.SOCKS4()` | SOCKS4 或 SOCKS4a 的 CONNECT 或 BIND 请求 |
| `mux.Prefix(...)` | 协议的 magic number 或第一条命令；输入仍是某个前缀的开头时返回 `NeedMore` |
| `mux.Any()` | 匹配一切，用于最后一条路由或 fallback |
| `mux.AnyOf(...)` | 其他匹配器的并集 |

有 magic number 的协议通常用 `mux.Prefix` 就够了；如果协议靠第一条可识别的命令区分，
匹配器只需几行：

```go
// resp matches a client that opens with a RESP array, as redis-cli does.
func resp(input []byte) mux.Result {
	if len(input) == 0 {
		return mux.NeedMore
	}
	if input[0] != '*' {
		return mux.NoMatch
	}
	return mux.Match
}
```

把它和内置路由放在一起即可：`mux.Route{Match: resp, Handler: redisServer}`。匹配器要用
真实客户端的字节来测，而不是按 RFC 描述的首字节来测——匹配器看到的就是 socket 送达的
内容，可能一次只有一个字节。

## 回退、上限与交接

`mux.New` 等价于 `mux.NewWithConfig(mux.Config{}, routes...)`。配置里与运行相关的字段有
三个：`Fallback` 是没有路由识别的连接所用的 `fib.Handler`（为 nil 时以 `mux.ErrNoMatch`
关闭连接）；`MaxPeek` 限制判断期间最多缓冲多少字节（默认 64，而 HTTP/2 前言是 24 字节）；
`Timeout` 限制对端沉默多久（默认 10 秒，负值表示不限制）。

```go
app := mux.NewWithConfig(mux.Config{
	Fallback: fib.HandlerFuncs{ // anything else: say so and close
		Open: func(c *fib.Connection) { _ = c.Send([]byte("unknown protocol\n")) },
	},
	MaxPeek: 256, // room for a longer first command
	Timeout: 5 * time.Second,
},
	mux.Route{Match: mux.TLS(), Handler: tlsHandler},
)
```

`mux.Route` 还有一个 `Nudge` 钩子，类型为 `func(c *fib.Connection, input []byte) (done bool)`：
当 mux 因为该路由返回 `NeedMore` 而等待更多字节时，每轮都会调用它，直到它返回 true。这是服务那些必须先等**服务端**开口、之后才肯发够可识别字节的协议的办法：
`multiproto` 用它给 grpc-go 这类要先收到服务端 SETTINGS 才发首请求的 gRPC 客户端发送一个
空的 HTTP/2 SETTINGS 帧。该钩子运行在 worker 上，可以向连接发送数据，但不能阻塞。
判断完成之前 `OnPriorityData` 没有协议可以路由，因此 mux 直接丢弃带外数据。

## 用 `multiproto` 共享一个端口

`multiproto.New` 根据 `multiproto.Config` 中非 nil 的字段，用 `multiproto.Routes`
生成路由，返回一个 `*mux.Handler`。一个协议族一个字段，因此标准组合就是一个字面量：

```go
package main

import (
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/arpc"
	"github.com/lesismal/fib/grpc"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/multiproto"
	"github.com/lesismal/fib/socks"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
	"github.com/lesismal/fib/websocket"
)

func main() {
	serverTLS, _, err := tlstest.Configs()
	if err != nil {
		panic(err)
	}

	web := fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(stdhttp.StatusOK, "text/plain; charset=utf-8", []byte("hello\n"))
	}))
	ws := websocket.NewHandler(websocket.HandlerFuncs{
		Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
			_ = c.WriteMessage(op, data) // echo
		},
	})
	arpcServer := arpc.NewServer()
	arpcServer.Handler.Handle("/echo", func(ctx *arpc.Context) {
		var s string
		if err := ctx.Bind(&s); err != nil {
			_ = ctx.Error(err)
			return
		}
		_ = ctx.Write(s)
	})

	engine, err := fib.New(fib.DefaultConfig(), fib.Binding{
		Addr: "127.0.0.1:8000",
		Handler: multiproto.New(multiproto.Config{
			TLS:       fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), web),
			HTTP1:     web,
			HTTP2:     web,
			WebSocket: ws,
			GRPC:      grpc.NewServer(), // register generated services on it
			SOCKS:     socks.NewServer(socks.Config{}),
			HTTPProxy: fibhttp.NewHandler(fibhttp.NewConnectProxy(fibhttp.ConnectProxyConfig{})),
			ARPC:      arpcServer,
		}),
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
curl -k https://127.0.0.1:8000/                          # TLS: HTTP/2 by ALPN
curl http://127.0.0.1:8000/                              # HTTP1
curl --http2-prior-knowledge http://127.0.0.1:8000/      # HTTP2: h2c that is not gRPC
curl --socks5 127.0.0.1:8000 http://127.0.0.1:8000/      # SOCKS, back into the same port
curl -p -x http://127.0.0.1:8000 http://127.0.0.1:8000/  # HTTPProxy: a CONNECT tunnel
```

handler 为 nil 的协议不提供服务，它的连接交给 `Fallback`，没有 fallback 就关闭。只开
一部分协议就靠这个，而且这样做能如实反映端口的实际能力：`GRPC` 为 nil、`HTTP2: web`
时，所有 h2c 连接都走 HTTP handler。

`fibhttp.NewConnectProxy` 只是 CONNECT 代理。`multiproto` 也会把目标为绝对 URL 的正向代理
请求（不带 `-p` 的 `curl -x`）交给它，而它对这类请求回复 405 Method Not Allowed，除非
`ConnectProxyConfig.Next` 负责处理它们。

## `multiproto` 的嗅探方式

它生成的路由按顺序如下：

- **TLS** —— 一个 ClientHello。TLS 里面是什么由 TLS handler 按 ALPN 区分，
  `fibhttp.ConfigureTLS` 就是这么让 HTTP/1.1 与 HTTP/2 共存的。
- **SOCKS** —— SOCKS5 greeting，或 SOCKS4/4a 请求。
- **HTTP/1.x** —— 先 `HTTPProxy`（`CONNECT`，或目标是绝对 `http://`、`https://` URL 的
  请求），再 `WebSocket`（请求头带 `Upgrade: websocket` 的 `GET`），最后 `HTTP1` 接收
  其余全部。连接的第一个请求就决定了整条连接；这三者中最后被服务的那个接收其他两个没有
  认领的所有 HTTP/1.x 连接。
- **h2c** —— 同时设置 `GRPC` 和 `HTTP2` 时，`GRPC` 会读到第一个请求的 header block 并
  匹配 `content-type: application/grpc...`，由 `NudgeHTTP2` 发出 grpc-go 客户端在等
  的服务端 SETTINGS；`HTTP2` 接收其余连接。只服务其中一个时，它接收所有 h2c 连接。
- **ARPC** —— 对第一个 arpc 头部做启发式匹配。arpc 没有 magic number，所以一旦启用
  ARPC，TLS 和 SOCKS 匹配器就换成更严格的版本（`ClientHello`，以及排除 arpc 的 SOCKS
  匹配），ARPC 路由排在最后。`ARPCMaxBodyLen` 限制被识别时头部可声明的 body 长度；应与
  arpc 服务端设置 `Handler.SetMaxBodyLen` 的值保持一致。

读取量取决于所服务的协议集合：同时有 `WebSocket` 和 `HTTP1` 时要等完整的请求头，只服务
其中一个时看到方法就够。`MaxPeek`（默认 8192）是请求头或 header block 的上限，`Timeout`
是对端沉默的上限；`Routes` 让你用同一张表自己构造 `mux.Handler`，`Config.Routes` 则在
内置路由之后追加自定义路由。

## Binding、`mux` 还是 `multiproto`

- **独立 Binding** 最省也最清晰：一个协议一个端口，不嗅探，各自有连接上限和证书；对于
  服务端先开口的协议，这也是唯一选择。只有 TCP 与 UDP 之间才能共用同一个端口号。
- **`mux`** 适合小而明确的路由表、自定义匹配器，以及首字节易于分类的协议。顺序和 fallback
  由你掌握。
- **`multiproto`** 适合标准协议族：它知道 WebSocket 升级要读多远、如何区分 gRPC 与其他
  h2c、如何避免把 ARPC 当成 TLS。除非你需要它没有的匹配器，否则优先用它。

## 坑与限制

- **必须由客户端先开口。** SMTP、FTP、MySQL 以及其他服务端先问候的协议需要独立 Binding；
  在 mux 上这类对端会在 `Config.Timeout` 后断开，`OnClose` 收到 `os.ErrDeadlineExceeded`。
- **嗅探超时不属于 handler。** 被选中的 handler 启动时没有读超时，由它自己设置；mux 的
  超时只约束嗅探阶段。
- **连接的第一个 HTTP 请求决定整条连接。** 之后连接内的升级或协议切换是所选 handler 的
  职责，不是 mux 的。
- **`MaxPeek` 太小会悄悄分错流。** 放不下的 WebSocket 握手会被判为“不是 WebSocket”，
  落到普通 HTTP 路由；`multiproto` 默认 8192 就是为了容纳浏览器带 Cookie 的请求头。
- **`HTTPProxy` 也会接收绝对 URL 请求。** 发到该端口的普通 `GET http://...` 交给代理
  handler 而不是 `HTTP1`；使用 `NewConnectProxy` 且没有 `Next` 时会得到 405。
- **路由两个字段都必需。** `mux.Route` 的 `Match` 或 `Handler` 为 nil 时 `mux.New` 会
  panic，不存在隐式的“匹配一切”。
- **`Fallback` 只处理“无人匹配”，不处理“匹配后失败”。** TLS 里的 SNI 未知时由 TLS
  handler 应答，不会回退到 fallback。
- **优先级不是朴素的先匹配先得。** 只有前面的路由都排除自己之后某条路由才会匹配，所以把
  宽泛的匹配器放在前面会遮蔽后面的一切。

## 相关文档

- [mux](../../mux) 与 [multiproto](../../multiproto) —— 包文档与全部匹配器
- [SOCKS 与 HTTP CONNECT](13-socks-connect.zh-CN.md) —— 共享端口上使用的代理 handler
- [WebSocket](09-websocket.zh-CN.md) —— `multiproto` 要读完整请求头的握手
- [gRPC](11-grpc.zh-CN.md) —— 需要 `NudgeHTTP2` 才能被识别的 h2c handler
- [引擎与配置](02-engine.zh-CN.md) —— Binding、地址与关闭
- [README](../../README.zh-CN.md) —— `mux` 与 `multiproto` 片段所在的上下文

# SOCKS 与 HTTP CONNECT

[English](13-socks-connect.md) | [简体中文](13-socks-connect.zh-CN.md)

本章讲怎么当“中间人”：让连不到目标的客户端通过 fib 程序到达目标，协议可以是 SOCKS5、
SOCKS4/4a、HTTP CONNECT，或者把两者放在同一个端口上。fib 为此提供三块拼图：`socks.Server`
是普通的 `fib.Handler`，实现 SOCKS 代理；`socks.Dialer` 是非阻塞的客户端拨号器，可以插进 HTTP、
WebSocket、TLS 客户端的拨号钩子；`fibhttp.ConnectProxy` 是给 `curl -x` 和 `HTTPS_PROXY` 用的
HTTP CONNECT 代理。本章沿用第 03 章的 handler 模型和第 06 章的 HTTP Context。

## 协议模型

- **代理就是两条连接加一个中继。** `socks.Server` 是一个 `fib.Handler`：像其他协议 handler 一样
  绑定即可。CONNECT 成功后，任一方向发来的数据都在收到它的回调里直接转给对端，中间没有 goroutine；
  `Config.MaxQueuedBytes` 让较快一侧的读取暂停，等较慢一侧的 socket 消化，于是快发送方被 TCP 流控
  而不是本进程内存限制住。`Config.IdleTimeout` 会结束空闲的中继。
- **客户端一侧从不阻塞。** `socks.ParseURL` 解析 curl 和环境变量所用的代理 URL 形式；得到的
  `Dialer` 的 `Dial` 满足 `fib.Engine.DialWithHandler` 的约定，可直接赋给
  `fibhttp.ClientConfig`、`websocket.DialerConfig` 和 `fibtls.DialOptions` 的 `Dial` 字段。它打开
  的连接就是普通的 fib 连接，`RemoteAddr` 是代理的地址；handler 收发的内容经代理到达目标。
- **CONNECT 是挂在请求上的隧道。** `fibhttp.ConnectProxy` 在 HTTP/1.x、HTTP/2、HTTP/3 上服务
  CONNECT：先 retain 请求、拨号目标，再用 `Context.Connect` 接受该请求，得到 `*fibhttp.Tunnel`。
  中继由隧道自身的流控来节流（`Unsent`、`HoldReads`、`WaitSendable`、`NotifySendable`），而
  `ConnectProxy` 就是这几者的完整示例。
- **一个端口，一个匹配器。** `mux` 看连接发来的头几个字节：SOCKS5 以字节 5 开头，SOCKS4 以字节 4
  开头，HTTP 以请求行开头（CONNECT 也算）。因此一个 `mux.New` 的 Engine 可以在同一端口上同时服务
  SOCKS 和 HTTP CONNECT 代理，不需要看 TLS 内部。

## 带认证的 SOCKS5 服务端

完整程序：一个 HTTP 服务端、挡在它前面的 SOCKS5 代理，以及一个只能通过代理访问该服务端的 HTTP
客户端。代理要求用户名密码，并记录每个请求：

```go
package main

import (
	"io"
	"log"
	nethttp "net/http"
	"time"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/socks"
)

func main() {
	web := fibhttp.NewRouter()
	web.Get("/hello", func(c *fibhttp.Context) {
		_ = c.String(200, "hello through the SOCKS5 proxy")
	})

	srv, err := fib.New(fib.DefaultConfig(),
		fib.Binding{Addr: "127.0.0.1:18080", Handler: fibhttp.NewHandler(web)},
		fib.Binding{Addr: "127.0.0.1:11080", Handler: socks.NewServer(socks.Config{
			Authenticate: func(username, password string) bool {
				return username == "fib" && password == "secret"
			},
			Allow: func(r *socks.Request) error {
				log.Printf("%s %s as %q", r.Version, r.Addr, r.Username)
				return nil
			},
			IdleTimeout: 2 * time.Minute,
		})},
	)
	if err != nil {
		panic(err)
	}
	defer srv.Close()
	go func() { _ = srv.Run() }()

	proxy, err := socks.ParseURL("socks5h://fib:secret@127.0.0.1:11080")
	if err != nil {
		panic(err)
	}
	clientEngine, err := fib.New(fib.DefaultConfig())
	if err != nil {
		panic(err)
	}
	defer clientEngine.Close()
	go func() { _ = clientEngine.Run() }()

	config := fibhttp.DefaultClientConfig()
	config.Dial = proxy.Dial // every request goes through the proxy
	client := fibhttp.NewClient(clientEngine, config)
	defer client.Close()

	req, err := nethttp.NewRequest(nethttp.MethodGet, "http://127.0.0.1:18080/hello", nil)
	if err != nil {
		panic(err)
	}
	rsp, err := client.Go(req).Wait()
	if err != nil {
		panic(err)
	}
	body, err := io.ReadAll(rsp.Body)
	if err != nil {
		panic(err)
	}
	log.Printf("%s: %s", rsp.Proto, body)
}
```

`Authenticate` 会对每个 SOCKS5 客户端执行 RFC 1929 的用户名密码交换；不设置它则放行所有客户端。
`Allow` 在认证之后裁决每个请求：返回 nil 表示允许，返回普通 error 以 “not allowed by ruleset”
拒绝，返回 `*socks.ReplyError` 则以指定的应答码拒绝。`Allow` 看到的是客户端写下的目标，可能是一个
之后解析到别处的域名；针对地址的规则应该放进 `AllowResolved`，它看到代理解析出的每个地址，并且
只在 `Config.Dial` 为 nil 时才会被调用：

```go
server := socks.NewServer(socks.Config{
	Authenticate: func(username, password string) bool {
		return username == "fib" && password == "secret"
	},
	Allow: func(r *socks.Request) error {
		if r.Addr == "169.254.169.254:80" { // a name-free rule, on what the client asked for
			return &socks.ReplyError{Version: r.Version, Code: socks.ReplyNotAllowed}
		}
		return nil
	},
	AllowResolved: func(r *socks.Request, addr netip.AddrPort) error {
		if addr.Addr().IsLoopback() || addr.Addr().IsPrivate() {
			return &socks.ReplyError{Version: r.Version, Code: socks.ReplyNotAllowed}
		}
		return nil
	},
	HandshakeTimeout: 10 * time.Second,
	DialTimeout:      10 * time.Second,
	MaxBinds:         64,
	MaxUDPTargets:    256,
})
```

`Config` 的其余字段都是资源上限：`MaxBinds` 限制等待对端的 BIND 数量，`MaxUDPTargets` 限制一个
UDP 关联同时保持的目标 socket 数，`MaxQueuedBytes` 限制中继每个方向排队的字节数；
`HandshakeTimeout` 与 `DialTimeout` 分别约束握手和到目标的拨号。

## SOCKS4、4a、BIND 与 UDP ASSOCIATE

同一个 `Server` 服务三个版本：它读 greeting 的第一个字节来决定协议。SOCKS4 没有密码交换，因此
配置了 `Config.Authenticate` 时 SOCKS4 客户端会被拒绝；SOCKS4 在客户端解析域名，SOCKS4a 则把
域名交给代理解析。

UDP ASSOCIATE 需要同一个 Engine 上的 UDP binding。服务端会把与客户端到达时同地址族的 UDP binding
地址告诉客户端；没有 UDP binding 的 Server 会拒绝 UDP ASSOCIATE：

```go
server := socks.NewServer(socks.Config{})
engine, err := fib.New(fib.DefaultConfig(),
	fib.Binding{Addr: "127.0.0.1:1080", Handler: server},
	fib.Binding{Network: "udp", Addr: "127.0.0.1:1080", Handler: server}, // UDP ASSOCIATE
)
if err != nil {
	panic(err)
}
```

BIND 要求代理监听客户端指定地址的对端来连接，这正是 FTP 主动模式需要的。`Dialer` 一侧对应
`Dialer.Bind`：它报告代理监听的地址（调用方转告对端），对端连上后再把连接交出来：

```go
if err := proxy.Bind(clientEngine, "127.0.0.1:9000", 2*time.Minute,
	fib.HandlerFuncs{Data: func(c *fib.Connection, data []byte) {
		_ = c.Send(data) // bytes from the peer
	}},
	func(addr net.Addr) {
		log.Printf("tell the peer to connect to %v", addr)
	},
	func(c *fib.Connection, err error) {
		log.Printf("peer connected: %v, %v", c, err)
	}); err != nil {
	panic(err)
}
```

BIND 只接受来自目标 IP（或目标域名解析出的地址之一）的连接；目标写 `0.0.0.0` 或 `::` 这种
未指定地址时才允许任意主机连入。

## 给客户端用的非阻塞拨号器

`socks.ParseURL` 接受 curl 和代理设置常用的 URL 形式；缺省端口是 `socks.DefaultPort`，即 1080。
scheme 同时决定协议和域名解析的一侧：`socks5://` 与 `socks4://` 本地解析，`socks5h://` 与
`socks4a://` 让代理解析：

```go
proxy, err := socks.ParseURL("socks5h://fib:secret@127.0.0.1:1080")
if err != nil {
	panic(err)
}

httpConfig := fibhttp.DefaultClientConfig()
httpConfig.Dial = proxy.Dial // HTTP/1.x and HTTP/2 through the proxy

wsConfig := websocket.DefaultDialerConfig()
wsConfig.Dial = proxy.Dial // wss:// and ws:// through the proxy

tlsOptions := fibtls.DialOptions{Dial: proxy.Dial} // any TLS dial through the proxy
```

`Dialer.Dial` 也正好是 `socks.Config.Dial` 要求的整个 `DialFunc` 约定，所以用 `Forward` 可以把
代理串起来：前端代理通过后端代理到达目标，前端的客户端只看到前端的地址：

```go
front, err := socks.ParseURL("socks5h://127.0.0.1:1081")
back, err := socks.ParseURL("socks5h://127.0.0.1:1080")
front.Forward = back.Dial // front proxies through back
```

在底层，连代理、与代理协商以及任何 DNS 查询都发生在 Engine 的循环和 worker 上，而不是调用方
goroutine 上，所以拨号器在任何地方调用都安全，等待期间 Engine 照常工作：

```go
if err := proxy.Dial(clientEngine, "tcp", "example.com:443", 5*time.Second,
	fib.HandlerFuncs{Data: func(c *fib.Connection, data []byte) {
		log.Printf("from the destination: %s", data)
	}},
	func(conn *fib.Connection, err error) {
		if err != nil {
			log.Print(err)
			return
		}
		_ = conn.Send([]byte("hello through the proxy"))
	}); err != nil {
	panic(err)
}
```

## HTTP CONNECT 代理，以及 SOCKS + CONNECT 同端口

完整程序：一个 Engine 在一个端口上服务 HTTP 站点，在另一个端口上用 `mux` 按首字节把 SOCKS5 和
CONNECT 代理分流。代理要求 Basic 凭据；客户端像 curl `-x` 那样手写 CONNECT，然后在隧道里发一个
普通 GET：

```go
package main

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"net"
	nethttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/mux"
	"github.com/lesismal/fib/socks"
)

func main() {
	auth := func(username, password string) bool {
		return username == "fib" && password == "secret"
	}

	web := fibhttp.NewRouter()
	web.Get("/hello", func(c *fibhttp.Context) {
		_ = c.String(200, "hello through CONNECT")
	})

	proxy := fibhttp.NewConnectProxy(fibhttp.ConnectProxyConfig{
		Authenticate: auth,
		Realm:        "fib",
		Allow: func(r *nethttp.Request) error {
			log.Printf("CONNECT %s", r.Host)
			return nil
		},
	})

	engine, err := fib.New(fib.DefaultConfig(),
		fib.Binding{Addr: "127.0.0.1:18080", Handler: fibhttp.NewHandler(web)},
		fib.Binding{Addr: "127.0.0.1:18082", Handler: mux.New(
			mux.Route{Match: mux.SOCKS(), Handler: socks.NewServer(socks.Config{Authenticate: auth})},
			mux.Route{Match: mux.HTTP(), Handler: fibhttp.NewHandler(proxy)},
		)},
	)
	if err != nil {
		panic(err)
	}
	defer engine.Close()
	go func() { _ = engine.Run() }()

	// The client: CONNECT with Basic credentials, then a request in the tunnel.
	conn, err := net.Dial("tcp", "127.0.0.1:18082")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	credential := base64.StdEncoding.EncodeToString([]byte("fib:secret"))
	if _, err := fmt.Fprintf(conn,
		"CONNECT 127.0.0.1:18080 HTTP/1.1\r\nHost: 127.0.0.1:18080\r\nProxy-Authorization: Basic %s\r\n\r\n",
		credential); err != nil {
		panic(err)
	}
	reader := bufio.NewReader(conn)
	accepted, err := nethttp.ReadResponse(reader, &nethttp.Request{Method: nethttp.MethodConnect})
	if err != nil {
		panic(err)
	}
	if accepted.StatusCode != nethttp.StatusOK {
		panic(accepted.Status)
	}
	if _, err := fmt.Fprintf(conn,
		"GET /hello HTTP/1.1\r\nHost: 127.0.0.1:18080\r\nConnection: close\r\n\r\n"); err != nil {
		panic(err)
	}
	rsp, err := nethttp.ReadResponse(reader, &nethttp.Request{Method: nethttp.MethodGet})
	if err != nil {
		panic(err)
	}
	body, err := io.ReadAll(rsp.Body)
	if err != nil {
		panic(err)
	}
	log.Printf("%s: %s", rsp.Status, body)
}
```

`ConnectProxyConfig.Authenticate` 读取 `Proxy-Authorization: Basic` 头（RFC 9110 §11.7.2），没有
凭据的客户端会收到 `407 Proxy Authentication Required` 和带 `Realm` 的 `Proxy-Authenticate`
质询。其余字段是 `Allow`、`Dial`、`DialTimeout`、`MaxQueuedBytes`、`IdleTimeout` 和 `Next`：
`Next` 服务那些不是“CONNECT 到 host:port”的请求，nil 的 `Next` 会以 `405 Method Not Allowed`
回答它们。把 `Dial` 指向某个 `socks.Dialer` 的 `Dial`，CONNECT 代理就会经 SOCKS 代理出网。

## Context.Connect 与隧道节流

`ConnectProxy` 建立在 `Context.Connect` 之上，任何 handler 都可以用它。它接受 CONNECT 请求，此后
`TunnelHandler` 收到客户端经隧道发来的数据，反方向则由 `Tunnel.Send` 和 `CloseAfterSend` 送出：

```go
type echoTunnel struct{}

func (echoTunnel) OnTunnelOpen(t *fibhttp.Tunnel) { log.Print("tunnel opened") }
func (echoTunnel) OnTunnelData(t *fibhttp.Tunnel, data []byte) {
	_ = t.Send(append([]byte("echo: "), data...)) // Send copies, data is valid for the call only
}
func (echoTunnel) OnTunnelClose(t *fibhttp.Tunnel, err error) {
	log.Printf("tunnel closed: %v", err)
}

router.Connect("/echo-tunnel", func(c *fibhttp.Context) {
	tunnel, err := c.Connect(nil, echoTunnel{})
	if err != nil {
		_ = c.Respond(nethttp.StatusBadRequest, "text/plain; charset=utf-8", []byte(err.Error()))
		return
	}
	_ = tunnel // the tunnel handler drives the rest
})
```

如果中继的一端更慢，就按对端的积压来节流，而不是先缓冲下来。可用的原语就是隧道自己的流控：
`Unsent` 报告还有多少字节没送出去，`HoldReads` 暂停接收对端的数据，
`NotifySendable`/`WaitSendable` 在积压排空后回调或等待：

```go
const limit = 256 << 10 // the relay's Config.MaxQueuedBytes equivalent
tunnel.HoldReads(true)
if tunnel.Unsent() > limit {
	tunnel.NotifySendable(limit, func(error) { tunnel.HoldReads(false) }) // may not wait here
} else {
	tunnel.HoldReads(false)
}
// In a goroutine of your own, never in OnTunnelData, this is the waiting form:
go func() { _ = tunnel.WaitSendable(limit) }()
```

HTTP/1.x 上的隧道会切换整条连接；HTTP/2 和 HTTP/3 上只切换那条流，共享的连接不动。HTTP/2 和
HTTP/3 上，暂停读取期间对端窗口已允许的数据仍会到达，所以 hold 是上界而不是急停。

## curl、curl -x 与 HTTPS_PROXY

代理跑起来后，命令行工具就能用：`curl -x` 给单条命令指定代理，`HTTPS_PROXY`/`ALL_PROXY` 给读
环境变量的程序用：

```sh
# SOCKS5 with the proxy resolving the destination name (socks5h vs socks5)
curl -x socks5h://fib:secret@127.0.0.1:1080 https://example.com/

# HTTP CONNECT with Basic credentials in the URL; curl sends Proxy-Authorization
curl -x http://fib:secret@127.0.0.1:18082 https://example.com/

# The same proxy for programs that read the environment
HTTPS_PROXY=http://fib:secret@127.0.0.1:18082 curl https://example.com/
```

`curl -x` 访问 `https://` URL 时走 CONNECT；访问 `http://` URL 时会把请求以绝对形式整条发给代理，
那属于 `ConnectProxyConfig.Next` 的职责，不是 CONNECT。

## 坑与限制

- **CONNECT 代理只隧道 CONNECT。** 绝对形式的 `http://` 请求不会被 `ConnectProxy` 拨号；要么给
  `Next` 一个转发 handler，要么客户端会看到 `405 Method Not Allowed`。
- **connect proxy 要包一层 `fibhttp.NewHandler`。** `NewConnectProxy` 返回的是请求 handler，不是
  连接 handler；binding 里必须写 `fibhttp.NewHandler(proxy)`。
- **`Authenticate` 会拒绝 SOCKS4 客户端。** SOCKS4 没有密码字段，一个校验密码却仍开放 SOCKS4 的
  服务端就是漏洞；`Server` 会关闭这类客户端。
- **地址规则放在 `AllowResolved` 里。** `Allow` 看到的是客户端发来的名字，等到真正拨号时它可能
  解析到被禁止的地址（DNS rebinding）。设置 `Config.Dial` 时 `AllowResolved` 不会被调用：拨号器
  自己解析域名，串联时要格外小心。
- **UDP ASSOCIATE 需要 UDP binding。** 没有它服务端会拒绝关联，而且 binding 的地址族必须与客户端
  到达服务端时的地址族一致。`SendBatch` 绕过代理的 UDP 分帧，在 UDP 关联连接上不能使用。
- **中继连接的 `RemoteAddr` 是代理的地址。** handler 实际在和目标通信，但它看到的地址属于代理连接。
- **mux 的规则是先说话的一方。** SOCKS 和 HTTP 客户端都先发数据；客户端等服务端先说的协议会在 mux
  超时后被关闭，这类协议要给独立端口。
- **HTTP/1 的 CONNECT 会切换整条连接。** 被拒绝的 CONNECT 用 `Connection: close` 回答，因为客户端
  可能已经把隧道的首批字节发出来了；这条连接上 CONNECT 之后的数据不再是 HTTP。
- **要节流，不要缓冲。** `MaxQueuedBytes` 限制的是中继的一个方向；既没有空闲超时也没有积压上限的
  中继会一直占着内存和 socket。

## 相关文档

- [`socks` 包](../../socks)、[`http` 包](../../http)（`ConnectProxy` 所在）以及它们的包文档。
- 根 README 的 [SOCKS 一节](../../README.zh-CN.md#socks)，里面有单端口 `mux` 的代码片段。
- [第 06 章：HTTP/1.x](06-http1.zh-CN.md) 讲服务端一侧的 CONNECT，[第 07 章：
  HTTP/2](07-http2.zh-CN.md) 讲流上的 CONNECT。
- [第 15 章：单端口多协议](15-multiprotocol.zh-CN.md) 深入讲 `mux` 与 `multiproto`。
- [第 20 章：客户端](20-clients.zh-CN.md) 讲 HTTP、WebSocket、TLS 客户端那些接收
  `socks.Dialer` 的拨号钩子。
- [docs/hacks.zh-CN.md](../hacks.zh-CN.md) 讲连接侧的 `HoldReads`、cork 与背压。

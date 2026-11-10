# 客户端

[English](20-clients.md) | [简体中文](20-clients.zh-CN.md)

fib 的每个协议都有基于同一个引擎构建的客户端，而且都不会阻塞调用方：`Engine.Dial`、
`fibhttp.Client`、`http3` 客户端、`websocket.Dialer`、`fibtls` 与 `socks` 都是"注册回调后立即返回"，
与 [03-handlers.md](03-handlers.zh-CN.md) 中的服务端 Handler 完全一致。本章讲解拨号、HTTP/1.x、HTTP/2 与
HTTP/3 客户端、WebSocket 客户端、TLS 与 SOCKS 选项，以及它们共用的生命周期规则。阅读前应已了解
引擎与配置一章 [02-engine.md](02-engine.zh-CN.md) 和异步 Handler 模型。

## 客户端模型

客户端就是引擎拨出并服务的一组连接，因此纯客户端的程序就是一个没有任何 binding 的引擎：

```go
engine, err := fib.New(fib.DefaultConfig()) // no bindings: dials only, listens nowhere
if err != nil {
	panic(err)
}
go func() { _ = engine.Run() }()
defer func() { engine.Stop(); _ = engine.Close() }()

client := fibhttp.NewClient(engine, fibhttp.DefaultClientConfig())
defer client.Close() // close clients before the engine
```

由此得到三条规则，下面所有示例都依赖它们：

- **一切都不等待。** 拨号、请求或 WebSocket 握手都把结果交给回调，调用方的协程从不会被挂起。唯一的
  阻塞形式（`Future.Wait`、`DialFuture.Wait`）只用于 `main`、测试等允许阻塞的地方——绝不要在
  Handler 里使用。
- **回调运行在拿到结果的那个协程上**：读取响应的引擎 worker、超时时的定时器、处理 DNS 与完成通知的
  进程级 `fib-client` 池，或者请求被直接拒绝时的调用方协程。回调不能长时间阻塞。
- **引擎比客户端活得久。** 客户端会池化连接，关闭客户端即停止它；`client.Close()` 必须在
  `engine.Close()` 之前，后者会直接断开连接且不通知客户端，此时在途请求只能等超时。

默认情况下响应体在回调运行前已完整缓冲，`ClientResponse` 内嵌标准 `*http.Response`。流式方式按客户端
（`StreamResponseBody`）开启，见[流式收发](#流式收发)。

## 异步拨号

`Engine.Dial` 是最底层的形式：打开一条原始连接并交给 Handler，用法见
[19-raw-io.md](19-raw-io.zh-CN.md)。`DialWithHandler` 让这条连接使用它自己的 Handler，这正是同一个引擎能同时
承载服务端与客户端的原因；`DialWith` 还可以指定 socket 绑定的本地地址与端口范围。

```go
// A connection of its own handler, so the engine's server bindings are unaffected.
err = engine.DialWithHandler("tcp", "127.0.0.1:9000", 3*time.Second, myHandler,
	func(c *fib.Connection, err error) {
		if err != nil {
			log.Println("dial:", err)
			return
		}
		_ = c.Send([]byte("ping")) // queued; flushed when the round ends
	})

// Bound to a chosen local address and port range, for a firewall that admits one source.
err = engine.DialWith(fib.DialOptions{
	Network:   "tcp",
	Addr:      "127.0.0.1:9000",
	Timeout:   3 * time.Second,
	Handler:   myHandler,
	LocalAddr: netip.AddrPortFrom(netip.MustParseAddr("192.0.2.10"), 0),
	PortMin:   40000,
	PortMax:   40100, // a port in use is skipped; no free port fails with fib.ErrNoLocalPort
}, func(c *fib.Connection, err error) {
	if err != nil {
		return
	}
	_ = c.Send([]byte("ping"))
})
```

两个调用都只在拨号根本无法开始时返回错误（未知网络、地址格式错误、引擎已停止）；之后的所有失败都通过
`done` 报告，它恰好运行一次。`nil` Handler 表示使用引擎自己的 Handler，即 `Engine.Dial` 的行为。DNS
解析不在调用方协程上做，而是在 `fib-client` 池里完成。

## HTTP/1.x 客户端

`fibhttp.Client` 通过引擎拨出的连接说 HTTP/1.x 与 HTTP/2，在请求之间保持连接，并按 host 把 URL 映射到
池化连接。请从 `DefaultClientConfig` 出发——零值 `ClientConfig` 的超时也是零：

```go
config := fibhttp.DefaultClientConfig() // Timeout 30s, DialTimeout 10s, 64 conns/host
config.Timeout = 5 * time.Second        // one request, from Do to the complete response
config.MaxConnsPerHost = 16
config.MaxIdleConnsPerHost = 8
config.IdleConnTimeout = 90 * time.Second
client := fibhttp.NewClient(engine, config)
defer client.Close()
```

回调形式是 `Do`，请求就是标准 `*http.Request`：

```go
req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://127.0.0.1:8080/", nil)
if err != nil {
	return
}
client.Do(req, func(resp *fibhttp.ClientResponse, err error) {
	if err != nil {
		return // exactly once: the response or the reason there is none
	}
	defer resp.Body.Close()
	fmt.Println(resp.StatusCode, resp.Header.Get("Content-Type"))
})
```

同一个请求也可以等待结果，这在 `main` 中合适，在 Handler 中是错的：

```go
resp, err := client.Go(req).Wait()
if err != nil {
	return
}
defer resp.Body.Close()
```

`Go` 总是等待完整响应，与 `net/http` 一致；在 Handler 的协程上应使用 `Do`。`Start(req, cb)` 是
`Do` 的变体，返回一个 `Call`，无需可取消的 context 就能放弃请求：

```go
call := client.Start(req, func(resp *fibhttp.ClientResponse, err error) {})
call.Cancel(context.Canceled) // callback hears it; a no-op once the request has settled
```

`ContentLength` 不超过 1MB 的请求体（`net/http.NewRequest` 对小的 `bytes.Buffer`、`bytes.Reader`、
`strings.Reader` 就是这么设置的）会被 `Do` 一次读完并随 header 发出。其他请求体由它自己的协程逐段读取，
并被服务端限速（最多 64KB 未发出），因此文件上传是流式的，不会整体读入内存；发完后请求体被关闭。连接
在响应的任何部分到达之前失败时，方法幂等且能通过 `req.GetBody` 重读请求体的请求会在另一条连接上重发一次；
服务端声明未处理的请求（HTTP/2 的 `GOAWAY`、`REFUSED_STREAM`）不论方法都会重发。带 `Expect: 100-continue`
的请求先发 header，收到服务端的 100 后再发 body，或者等过 `ExpectContinueTimeout`（默认 1 秒）后再发。

下面的完整程序在同一个引擎上运行服务端和客户端：第一个请求用 future 形式，第二个用回调形式。

```go
package main

import (
	"fmt"
	"io"
	stdhttp "net/http"
	"strings"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
)

func main() {
	config := fib.DefaultConfig()
	config.LogSettings = false

	engine, err := fib.New(config, fib.Binding{
		Addr: "127.0.0.1:8080",
		Handler: fibhttp.NewHandler(fibhttp.HandlerFunc(func(c *fibhttp.Context) {
			_ = c.Respond(200, "text/plain; charset=utf-8", []byte("hello "+c.Query("name")))
		})),
	})
	if err != nil {
		panic(err)
	}
	go func() { _ = engine.Run() }()
	defer func() { engine.Stop(); _ = engine.Close() }()

	client := fibhttp.NewClient(engine, fibhttp.DefaultClientConfig())
	defer client.Close()

	// Future form: right in main, never in a handler.
	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://127.0.0.1:8080/?name=fib", nil)
	if err != nil {
		panic(err)
	}
	resp, err := client.Go(req).Wait()
	if err != nil {
		panic(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	fmt.Println("sync:", string(body))

	// Callback form: the response arrives on an engine worker.
	req, err = stdhttp.NewRequest(stdhttp.MethodPost, "http://127.0.0.1:8080/upload",
		strings.NewReader("payload"))
	if err != nil {
		panic(err)
	}
	done := make(chan struct{})
	client.Do(req, func(resp *fibhttp.ClientResponse, err error) {
		defer close(done)
		if err != nil {
			fmt.Println("async:", err)
			return
		}
		body, _ := io.ReadAll(resp.Body)
		fmt.Println("async:", resp.StatusCode, string(body))
	})
	<-done
}
```

## 流式收发

两个方向都可以流式。响应方向设置 `StreamResponseBody`，并在回调里用 `OnBody` 取 body；此时回调在 header
（或达到阈值的部分 body）到达后即运行，`data` 只在本次调用内有效：

```go
config := fibhttp.DefaultClientConfig()
config.StreamResponseBody = true
config.StreamResponseBodyThreshold = 64 << 10 // bodies up to 64 KiB are still buffered whole
config.StreamUnknownLengthAtHeader = true     // SSE and long polls hand on as they trickle
client := fibhttp.NewClient(engine, config)

client.Do(req, func(resp *fibhttp.ClientResponse, err error) {
	if err != nil {
		return
	}
	resp.OnBody(func(data []byte, fin bool, err error) {
		if err != nil {
			return // connection gone, timed out or cancelled: the last call
		}
		_, _ = w.Write(data)
		if fin {
			_ = w.Close()
		}
	})
})
```

`OnBody` 不会自己调用传入的函数：已经到达的数据在回调返回后交付，其余部分由读取该连接的 worker 交付，
一次一个、按顺序。函数慢会停止客户端读取这条连接，于是发送方由 TCP 或 HTTP/2 流控限速，而不是由这里的
内存兜底。另一种方式是在回调里读 `resp.Body`（拉取式），没有数据时返回 `fibhttp.ErrWouldBlock`，且不能
与 `OnBody` 混用。

请求方向可以传任意 `io.Reader`，并把 `ContentLength` 留成 `-1` 以流式发送：

```go
pr, pw := io.Pipe()
req, _ := stdhttp.NewRequest(stdhttp.MethodPost, "http://127.0.0.1:8080/upload", pr)
req.ContentLength = -1 // chunked on HTTP/1, DATA frames on HTTP/2

go func() {
	defer pw.Close()
	// write the body in pieces; the client reads it as the server accepts it
}()

client.Do(req, func(resp *fibhttp.ClientResponse, err error) {
	if err != nil {
		return
	}
	_ = resp.Body.Close()
})
```

`http3.Client` 有同样的 `StreamResponseBody` 选项；`ClientResponse.TakeBody` / `BodyBytes` 可以零拷贝
地把完整 body 交出去——这正是 [21-hacks.md](21-hacks.zh-CN.md) 里的反向代理路径。

## HTTP/2 客户端

并没有单独的 HTTP/2 客户端：`fibhttp.Client` 对任何通过 ALPN 选择 HTTP/2 的 `https://` 源站说 HTTP/2，
并在一条连接上承载服务端允许的并发请求。同一条连接被复用、请求被多路复用，因此网关所需的连接数远少于
请求数。

```go
config := fibhttp.DefaultClientConfig()
config.MaxConnsPerHost = 4                 // enough for the fallback to HTTP/1.1
config.ReadIdleTimeout = 30 * time.Second  // PING a silent h2 connection
config.PingTimeout = 15 * time.Second      // and take it for dead if the PING is not answered
config.HTTP2.AdaptiveWindow = true         // grow the flow-control windows to the path's BDP
config.HTTP2.StreamWindow = 4 << 20
client := fibhttp.NewClient(engine, config) // https:// then negotiates h2 by default
```

`UnencryptedHTTP2 = true` 让 `http://` 请求以 h2c 先验知识方式发送（见
[07-http2.md](07-http2.zh-CN.md)），`DisableHTTP2 = true` 让 `https://` 留在 HTTP/1.1。单个请求的超时或取消只会
重置该 stream；服务端声明未处理的请求（`GOAWAY`、`REFUSED_STREAM`）会在新连接上重发。
`ReadIdleTimeout` 与 `SettingsTimeout` 是发现"无声死亡"的连接——忘记它的 NAT、忽然安静的服务端——的两种
方式，而不必等 TCP 自己超时。

## HTTP/3 与 Alt-Svc

HTTP/3 一定加密、一定跑在 UDP socket 的 QUIC 上，因此 `http3.Client` 需要 TLS 1.3 配置和 `https://` URL。
已知 HTTP/3 端点地址时直接拨该源站；未知时（在 TCP 响应上通告 HTTP/3 的 Web 服务器通常如此），用
`http3.AltSvcClient` 包一层 TCP 客户端与 HTTP/3 客户端，它会跟随源站的 `Alt-Svc` header。下面的程序在同一个
引擎上提供源站（TCP 上的 HTTPS 与 UDP 上的 HTTP/3，都在 9443 端口）并发出两个请求，依次打印
`HTTP/2.0 hello` 与 `HTTP/3.0 hello`：

```go
package main

import (
	"fmt"
	"io"
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/http3"
	fibtls "github.com/lesismal/fib/tls"
	"github.com/lesismal/fib/tlstest"
)

func main() {
	serverTLS, clientTLS, err := tlstest.Configs() // a throwaway certificate for localhost
	if err != nil {
		panic(err)
	}

	// The origin: HTTPS on TCP, announcing HTTP/3 on the same port over UDP.
	app := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		if !c.Request.ProtoAtLeast(3, 0) {
			c.Header().Set("Alt-Svc", http3.AltSvc(9443))
		}
		_ = c.String(stdhttp.StatusOK, "hello") // sends Alt-Svc along with it
	})
	config := fib.DefaultConfig()
	config.LogSettings = false
	engine, err := fib.New(config,
		fib.Binding{Addr: "127.0.0.1:9443",
			Handler: fibtls.NewServer(fibhttp.ConfigureTLS(serverTLS), fibhttp.NewHandler(app))},
		fib.Binding{Network: "udp", Addr: "127.0.0.1:9443",
			Handler: http3.NewHandler(http3.ConfigureTLS(serverTLS), app)},
	)
	if err != nil {
		panic(err)
	}
	go func() { _ = engine.Run() }()
	defer func() { engine.Stop(); _ = engine.Close() }()

	tcpConfig := fibhttp.DefaultClientConfig()
	tcpConfig.TLSConfig = clientTLS
	tcp := fibhttp.NewClient(engine, tcpConfig)

	quicConfig := http3.DefaultClientConfig()
	quicConfig.TLSConfig = clientTLS
	quic := http3.NewClient(engine, quicConfig)

	// The first request to an origin goes over TCP; once its response has
	// carried Alt-Svc: h3=..., the next ones take QUIC to that endpoint.
	client := http3.NewAltSvcClient(tcp, quic)
	defer client.Close() // closes both clients, before the engine

	for range 2 {
		req, err := stdhttp.NewRequest(stdhttp.MethodGet, "https://127.0.0.1:9443/", nil)
		if err != nil {
			panic(err)
		}
		resp, err := client.Go(req).Wait()
		if err != nil {
			panic(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		fmt.Println(resp.Proto, string(body))
	}
}
```

服务端见 [08-http3.md](08-http3.zh-CN.md)：由
`http3.NewHandler(http3.ConfigureTLS(tlsConfig), app)` 服务的 UDP binding，以及 TCP 响应 header 里的
`http3.AltSvc(port)`。在作答之前通过 `c.Header()` 设置这个 header 即可；`String`、`Respond`、
`WriteResponse` 和 `Write` 都会发送它。QUIC 失败时，可重复的请求会回退到 TCP，该源站会在 30 秒内留在 TCP 上，因此屏蔽 UDP 的网络
只付一次超时，而不是每个请求都付一次。开启 `quicConfig.EarlyData` 后，`quic.Stats()` 报告会话恢复
（`Resumed`）与 0-RTT（`EarlySent`、`EarlyAccepted`、`EarlyRejected`、`TooEarly`）。

## WebSocket 客户端

`websocket.Dialer` 在引擎连接上完成打开握手，之后用与服务端连接相同的 `HandlerFuncs` 服务客户端连接——
即 [09-websocket.md](09-websocket.zh-CN.md) 里的 `OnMessage`、`OnFrame`、`WriteMessage` 与 `Close`。握手回调
同时拿到连接与服务端的 `*http.Response`。下面的程序拨向它在同一个引擎上运行的 echo 服务端，打印
`open, subprotocol: chat` 与 `got: hello`：

```go
package main

import (
	"fmt"
	stdhttp "net/http"
	"time"

	fib "github.com/lesismal/fib"
	"github.com/lesismal/fib/websocket"
)

func main() {
	// A server to talk to, on the same engine: it echoes, and speaks "chat".
	serverConfig := websocket.DefaultConfig()
	serverConfig.Subprotocols = []string{"chat"}
	serverConfig.EnableCompression = true
	echo := websocket.NewHandlerWithConfig(serverConfig, websocket.HandlerFuncs{
		Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
			_ = c.WriteMessage(op, data)
		},
	})
	config := fib.DefaultConfig()
	config.LogSettings = false
	engine, err := fib.New(config, fib.Binding{Addr: "127.0.0.1:9080", Handler: echo})
	if err != nil {
		panic(err)
	}
	go func() { _ = engine.Run() }()
	defer func() { engine.Stop(); _ = engine.Close() }()

	dialerConfig := websocket.DefaultDialerConfig()
	dialerConfig.HandshakeTimeout = 5 * time.Second
	dialerConfig.Subprotocols = []string{"chat"}
	dialerConfig.EnableCompression = true
	dialer := websocket.NewDialer(engine, dialerConfig)

	closed := make(chan struct{})
	handler := websocket.HandlerFuncs{
		Open: func(c *websocket.Connection, r *stdhttp.Request) {
			fmt.Println("open, subprotocol:", c.Subprotocol())
		},
		Message: func(c *websocket.Connection, op websocket.Opcode, data []byte) {
			fmt.Println("got:", string(data))
			_ = c.Close(websocket.CloseNormal, "")
		},
		Close: func(c *websocket.Connection, code uint16, reason string, err error) {
			close(closed)
		},
	}

	dialer.Dial("ws://127.0.0.1:9080/ws", nil, handler,
		func(conn *websocket.Connection, resp *stdhttp.Response, err error) {
			if err != nil {
				if resp != nil {
					fmt.Println("refused with", resp.StatusCode)
				}
				fmt.Println("dial:", err)
				close(closed)
				return
			}
			_ = conn.WriteText("hello")
		})
	<-closed
}
```

第三个参数是任意 `websocket.Handler`，因此客户端连接可以用 `Frame` 按帧接收、或用 `Message` 接收重组后的
消息；传 `nil` 表示接受连接但忽略其流量。`Dialer.Go(url, header, handler).Wait()` 是给允许阻塞的调用方
准备的同一拨号方式。失败时只有 `done` 运行（服务端有应答时带上它，例如路径错误时的 404）；成功时先运行
handler 的 `Open`，再运行 `done`。若客户端要在自己发起的 HTTP/1.1 upgrade 之后承载原始字节流，HTTP 客户端的
`StartUpgrade` 会在服务端回 101 后把连接变成交给 `TunnelHandler` 的 `*fibhttp.Tunnel`。

## TLS 客户端选项

`wss://` 或 `https://` 客户端都有 `*tls.Config` 字段，fib 在 `crypto/tls` 完成握手后接管记录层。若要自己
拨 TLS——自定义协议、经代理或经 Unix socket 的 TLS——使用 `fibtls.DialWithOptions`，它的 `DialOptions`
就是客户端侧的旋钮：

```go
err = fibtls.DialWithOptions(engine, "tcp", "127.0.0.1:8443", 5*time.Second, tlsConfig,
	fibtls.DialOptions{
		HandshakeTimeout:  5 * time.Second,
		Blocking:          false, // state-machine handshake: no goroutine parked per connection
		EarlyData:         true,  // 0-RTT for a resumable session
		SessionCache:      fibtls.NewQUICSessionCache(64),
		TLS13CipherSuites: []uint16{tls.TLS_AES_128_GCM_SHA256},
		Dial:              proxy.Dial, // a socks.Dialer's Dial reaches the server through it
	},
	myHandler, func(c *fib.Connection, err error) {
		if err != nil {
			return
		}
		_ = c.Send([]byte("secure ping"))
	})
```

`done` 在 TCP 连接建立后、握手之前运行；它发送的内容会等握手完成后加密发出，`myHandler` 只看到明文。
配置没有指定 `ServerName` 时会从地址填入，因此一份配置可以服务多个源站。`Blocking: true` 让握手在
握手池的 worker 上通过 `crypto/tls` 运行，用来与状态机式握手做对比；默认实现等待时不占用协程。mTLS、
会话票据与密码套件选择见 [10-tls.md](10-tls.zh-CN.md)。

## SOCKS 与代理拨号

`fibhttp.Client`、`websocket.Dialer` 与 `fibtls.DialOptions` 都接受一个替代引擎拨号器的 `Dial` 函数，
而 `socks.Dialer.Dial` 恰好就是这个函数。因此只改一个字段，就能让客户端的所有请求都走 SOCKS 代理：

```go
proxy, err := socks.ParseURL("socks5h://user:secret@127.0.0.1:1080") // h: the proxy resolves names
if err != nil {
	panic(err)
}
config := fibhttp.DefaultClientConfig()
config.Dial = proxy.Dial
client := fibhttp.NewClient(engine, config)
```

`socks.Dialer` 可以通过 `Forward` 字段串联到另一个代理；SOCKS 连接就是普通的 fib 连接，因此
`Dialer.Dial` 对 `"udp"` 网络会打开 UDP ASSOCIATE。下面的完整程序在同一个引擎上运行 HTTP 源站、SOCKS5
服务端，以及通过它抓取的客户端：

```go
package main

import (
	"fmt"
	"io"
	stdhttp "net/http"

	fib "github.com/lesismal/fib"
	fibhttp "github.com/lesismal/fib/http"
	"github.com/lesismal/fib/socks"
)

func main() {
	config := fib.DefaultConfig()
	config.LogSettings = false

	origin := fibhttp.HandlerFunc(func(c *fibhttp.Context) {
		_ = c.Respond(200, "text/plain", []byte("through the proxy\n"))
	})
	engine, err := fib.New(config,
		fib.Binding{Addr: "127.0.0.1:8080", Handler: fibhttp.NewHandler(origin)},
		fib.Binding{Addr: "127.0.0.1:1080", Handler: socks.NewServer(socks.Config{})},
	)
	if err != nil {
		panic(err)
	}
	go func() { _ = engine.Run() }()
	defer func() { engine.Stop(); _ = engine.Close() }()

	proxy := &socks.Dialer{Addr: "127.0.0.1:1080"} // Version5, no auth
	clientConfig := fibhttp.DefaultClientConfig()
	clientConfig.Dial = proxy.Dial // every request goes through the proxy
	client := fibhttp.NewClient(engine, clientConfig)
	defer client.Close()

	req, err := stdhttp.NewRequest(stdhttp.MethodGet, "http://127.0.0.1:8080/", nil)
	if err != nil {
		panic(err)
	}
	resp, err := client.Go(req).Wait()
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Print(string(body))
}
```

代理侧——认证、`Allow` 规则、CONNECT 与 UDP ASSOCIATE——见
[13-socks-connect.md](13-socks-connect.zh-CN.md)。

## 坑与限制

- **不要在 Handler 里 `Wait()`。** `Future.Wait`、`DialFuture.Wait` 和 `AltSvcFuture.Wait` 会阻塞调用
  协程；在 Handler 里那就是引擎 worker，这正是 [21-hacks.md](21-hacks.zh-CN.md) 反模式清单的第一条。应使用带
  回调的 `Do`，并在结果到达前 `Retain`/`Release` 请求。
- **先关客户端，再关引擎。** `engine.Close()` 会直接断开客户端的连接且不通知它，在途请求只能等超时。
  先 `client.Close()`，再关引擎。
- **`Do` 返回后不要继续使用该请求。** 客户端可能仍在读取它的 body 与 header；回调运行时才表示它用完这些
  对象。
- **流式响应体必须被取走。** 开启 `StreamResponseBody` 后，回调既没注册 `OnBody` 也没读 `Body`，剩余
  body 会被丢弃（超过阈值时连接还会关闭）。传给 `OnBody` 的 `data` 只在本次调用内有效，要保留必须拷贝。
- **`ClientConfig{}` 不是 `DefaultClientConfig()`。** `NewClient` 只补大小上限与 `PingTimeout`；零
  `Timeout`、`DialTimeout`、`MaxConnsPerHost` 依旧是零，意味着没有超时、没有连接上限；零
  `MaxIdleConnsPerHost` 不保留空闲连接，每个请求都重新拨号。
- **一条 HTTP/1.1 连接同时只承载一个请求**，客户端也不做 pipeline。慢请求只会拖住自己的连接；但一批慢
  请求要设置 `MaxConnsPerHost`，并用 `MaxPendingPerHost` 让一个死掉的 host 只产生有界队列，而不是一直
  增长到超时。
- **回调不是做长耗时工作的地方。** 响应在引擎 worker 上读取，回调返回前一直占着这个 worker；慢回调会拖慢
  客户端，在 HTTP/2 上还会拖住该 worker 读取的所有 stream。

## 相关文档

- [HTTP/1.x](../http1.zh-CN.md#客户端与-expect-100-continue) —— 客户端的请求与响应头、流式请求体、
  `Expect: 100-continue`
- [HTTP/2](../http2.zh-CN.md#客户端) —— 客户端的窗口、健康检查与多路复用
- [HTTP/3](../http3.zh-CN.md#客户端) —— QUIC 客户端、Alt-Svc 与 0-RTT
- [WebSocket](09-websocket.zh-CN.md) —— 客户端 Handler 与服务端共用的连接 API
- [SOCKS 与 HTTP CONNECT](13-socks-connect.zh-CN.md) —— 代理服务端、认证与拨号串联
- [进阶技巧](21-hacks.zh-CN.md) —— 单引擎同时做服务端与客户端，以及回调生命周期规则
- [README](../../README.zh-CN.md) —— 异步客户端概览与基准测试表
